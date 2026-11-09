package com.nuvio.app.features.downloads

import com.nuvio.app.features.player.PlayerPlaybackNetworking
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.RandomAccessFile
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import okhttp3.Call
import okhttp3.Callback
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response

/**
 * HLS (.m3u8) download support for Android.
 *
 * Strategy: fetch the playlist, pick the best variant, download every segment (a few in parallel),
 * decrypt AES-128 segments when needed, and append them in order to the same ".part" file the
 * regular HTTP downloader uses. Progress is stored in a small "<fileName>.hls" sidecar file so a
 * paused/failed download resumes from the last complete segment.
 *
 * Not supported (fails with a clear message): live streams, SAMPLE-AES/DRM, byte-range segments
 * and variants whose audio lives in a separate rendition.
 */

private const val HLS_PARALLEL_SEGMENTS = 4
private const val HLS_MAX_ATTEMPTS = 4
private const val HLS_MAX_PLAYLIST_SNIFF_BYTES = 8L * 1024 * 1024

internal fun isHlsUrl(url: String): Boolean {
    val withoutFragment = url.trim().lowercase().substringBefore('#')
    return withoutFragment.substringBefore('?').endsWith(".m3u8")
}

/** True when a finished "download" is really a (small) HLS playlist text file. */
internal fun looksLikeHlsPlaylist(file: File): Boolean {
    if (!file.isFile) return false
    val size = file.length()
    if (size <= 0L || size > HLS_MAX_PLAYLIST_SNIFF_BYTES) return false
    val head = ByteArray(minOf(size, 64L).toInt())
    val read = file.inputStream().use { it.read(head) }
    if (read <= 0) return false
    return String(head, 0, read, Charsets.UTF_8)
        .trimStart('\uFEFF', ' ', '\n', '\r', '\t')
        .startsWith("#EXTM3U")
}

internal fun hlsStateFile(directory: File, fileName: String): File = File(directory, "$fileName.hls")

// ---------------------------------------------------------------------------------------------
// Playlist parsing
// ---------------------------------------------------------------------------------------------

internal class HlsKey(val uri: String, val iv: ByteArray?)

internal class HlsSegment(
    val url: String,
    val durationSeconds: Double,
    val sequence: Long,
    val key: HlsKey?,
    val initUrl: String?,
)

internal object HlsPlaylistParser {
    private val attributeRegex = Regex("([A-Z0-9-]+)=(\"[^\"]*\"|[^,]*)")

    fun isMasterPlaylist(text: String): Boolean = text.contains("#EXT-X-STREAM-INF")

    fun parseAttributes(line: String): Map<String, String> {
        val body = line.substringAfter(':', "")
        return attributeRegex.findAll(body).associate { match ->
            match.groupValues[1] to match.groupValues[2].trim().trim('"')
        }
    }

    fun resolve(baseUrl: String, reference: String): String =
        baseUrl.toHttpUrl().resolve(reference)?.toString()
            ?: throw IOException("URL invalida na playlist HLS: $reference")

    /** Highest-bandwidth variant that carries its own audio. */
    fun selectVariantUrl(text: String, baseUrl: String): String {
        val lines = text.lines().map { it.trim() }
        val separateAudioGroups = lines
            .filter { it.startsWith("#EXT-X-MEDIA:") }
            .map { parseAttributes(it) }
            .filter { it["TYPE"] == "AUDIO" && !it["URI"].isNullOrBlank() }
            .mapNotNull { it["GROUP-ID"] }
            .toSet()

        var bestBandwidth = -1L
        var bestUrl: String? = null
        var sawVariant = false
        var index = 0
        while (index < lines.size) {
            val line = lines[index]
            if (line.startsWith("#EXT-X-STREAM-INF:")) {
                sawVariant = true
                val attributes = parseAttributes(line)
                var uriIndex = index + 1
                while (uriIndex < lines.size && (lines[uriIndex].isEmpty() || lines[uriIndex].startsWith("#"))) {
                    uriIndex++
                }
                val uri = lines.getOrNull(uriIndex)
                if (uri != null) {
                    val audioGroup = attributes["AUDIO"]
                    val needsSeparateAudio = audioGroup != null && audioGroup in separateAudioGroups
                    if (!needsSeparateAudio) {
                        val bandwidth = attributes["BANDWIDTH"]?.toLongOrNull()
                            ?: attributes["AVERAGE-BANDWIDTH"]?.toLongOrNull()
                            ?: 0L
                        if (bandwidth > bestBandwidth) {
                            bestBandwidth = bandwidth
                            bestUrl = resolve(baseUrl, uri)
                        }
                    }
                }
                index = uriIndex
            }
            index++
        }
        return bestUrl ?: throw DownloadInvalidContentException(
            if (sawVariant) {
                "Este HLS usa audio separado do video, ainda sem suporte para download."
            } else {
                "Playlist HLS sem variantes de video."
            },
        )
    }

    fun parseMedia(text: String, baseUrl: String): List<HlsSegment> {
        var sequence = 0L
        var key: HlsKey? = null
        var initUrl: String? = null
        var pendingDuration: Double? = null
        var ended = false
        val segments = ArrayList<HlsSegment>()

        for (rawLine in text.lineSequence()) {
            val line = rawLine.trim()
            when {
                line.isEmpty() -> Unit
                line.startsWith("#EXT-X-MEDIA-SEQUENCE:") ->
                    sequence = line.substringAfter(':').trim().toLongOrNull() ?: 0L
                line.startsWith("#EXT-X-KEY:") -> {
                    val attributes = parseAttributes(line)
                    when (val method = attributes["METHOD"]) {
                        null, "NONE" -> key = null
                        "AES-128" -> {
                            val uri = attributes["URI"]
                                ?: throw DownloadInvalidContentException("Chave HLS sem URI.")
                            key = HlsKey(resolve(baseUrl, uri), attributes["IV"]?.let(::parseIv))
                        }
                        else -> throw DownloadInvalidContentException(
                            "Este HLS usa criptografia $method, sem suporte para download.",
                        )
                    }
                }
                line.startsWith("#EXT-X-MAP:") -> {
                    val attributes = parseAttributes(line)
                    if (attributes.containsKey("BYTERANGE")) {
                        throw DownloadInvalidContentException("HLS com byte-range ainda sem suporte para download.")
                    }
                    initUrl = attributes["URI"]?.let { resolve(baseUrl, it) }
                }
                line.startsWith("#EXT-X-BYTERANGE:") ->
                    throw DownloadInvalidContentException("HLS com byte-range ainda sem suporte para download.")
                line.startsWith("#EXTINF:") ->
                    pendingDuration = line.substringAfter(':').substringBefore(',').trim().toDoubleOrNull() ?: 0.0
                line.startsWith("#EXT-X-ENDLIST") -> ended = true
                line.startsWith("#") -> Unit
                else -> {
                    segments += HlsSegment(
                        url = resolve(baseUrl, line),
                        durationSeconds = pendingDuration ?: 0.0,
                        sequence = sequence,
                        key = key,
                        initUrl = initUrl,
                    )
                    sequence++
                    pendingDuration = null
                }
            }
        }
        if (!ended) throw DownloadInvalidContentException("Transmissao HLS ao vivo nao pode ser baixada.")
        if (segments.isEmpty()) throw DownloadInvalidContentException("Playlist HLS sem segmentos.")
        return segments
    }

    private fun parseIv(value: String): ByteArray {
        val hex = value.trim().removePrefix("0x").removePrefix("0X").padStart(32, '0').takeLast(32)
        return ByteArray(16) { i -> hex.substring(i * 2, i * 2 + 2).toInt(16).toByte() }
    }
}

// ---------------------------------------------------------------------------------------------
// Networking
// ---------------------------------------------------------------------------------------------

private class HlsFetched(val bytes: ByteArray, val finalUrl: String)

private fun buildHlsRequest(item: DownloadItem, url: String): Request =
    Request.Builder().url(url).apply {
        item.sourceHeaders.forEach { (key, value) ->
            // Accept-Encoding is left to OkHttp so gzip'd playlists are decompressed transparently.
            if (!key.equals("Range", true) && !key.equals("If-Range", true) &&
                !key.equals("Accept-Encoding", true)) header(key, value)
        }
        if (item.sourceHeaders.keys.none { it.equals("User-Agent", true) }) {
            header("User-Agent", PlayerPlaybackNetworking.DEFAULT_USER_AGENT)
        }
    }.build()

private suspend fun OkHttpClient.fetchOnce(item: DownloadItem, url: String): HlsFetched =
    suspendCancellableCoroutine { continuation ->
        val call = newCall(buildHlsRequest(item, url))
        continuation.invokeOnCancellation { call.cancel() }
        call.enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                continuation.resumeWithException(e)
            }

            override fun onResponse(call: Call, response: Response) {
                response.use { res ->
                    try {
                        if (!res.isSuccessful) throw DownloadHttpException(res.code)
                        val body = res.body ?: throw IOException("Resposta HLS vazia")
                        continuation.resume(HlsFetched(body.bytes(), res.request.url.toString()))
                    } catch (error: Exception) {
                        continuation.resumeWithException(error)
                    }
                }
            }
        })
    }

private suspend fun OkHttpClient.fetchWithRetry(item: DownloadItem, url: String): HlsFetched {
    var attempt = 0
    while (true) {
        try {
            return fetchOnce(item, url)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            attempt++
            val retryable = when (error) {
                is DownloadHttpException -> error.statusCode == 429 || error.statusCode >= 500
                is IOException -> true
                else -> false
            }
            if (!retryable || attempt >= HLS_MAX_ATTEMPTS) throw error
            delay(600L * attempt)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Crypto + resume state
// ---------------------------------------------------------------------------------------------

private fun decryptAes128(data: ByteArray, key: ByteArray, iv: ByteArray): ByteArray {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), IvParameterSpec(iv))
    return cipher.doFinal(data)
}

/** Per the HLS spec, a missing IV is the segment's media sequence number as a 128-bit big-endian value. */
private fun sequenceIv(sequence: Long): ByteArray {
    val iv = ByteArray(16)
    for (i in 0..7) iv[15 - i] = (sequence shr (8 * i)).toByte()
    return iv
}

private class HlsResumeState(val segmentCount: Int, val completedSegments: Int, val bytes: Long)

private fun readHlsState(file: File): HlsResumeState? = runCatching {
    val values = file.readLines().mapNotNull { line ->
        val parts = line.split('=')
        if (parts.size == 2) parts[0].trim() to parts[1].trim() else null
    }.toMap()
    HlsResumeState(
        segmentCount = values.getValue("count").toInt(),
        completedSegments = values.getValue("done").toInt(),
        bytes = values.getValue("bytes").toLong(),
    )
}.getOrNull()

private fun writeHlsState(file: File, segmentCount: Int, completedSegments: Int, bytes: Long) {
    runCatching { file.writeText("count=$segmentCount\ndone=$completedSegments\nbytes=$bytes\n") }
}

// ---------------------------------------------------------------------------------------------
// Transfer
// ---------------------------------------------------------------------------------------------

internal suspend fun transferHlsDownload(
    item: DownloadItem,
    directory: File,
    client: OkHttpClient,
    onHeaders: (totalBytes: Long?, validator: String?) -> Unit,
    onProgress: (downloadedBytes: Long, totalBytes: Long?) -> Unit,
): File = withContext(Dispatchers.IO) {
    require(File(item.fileName).name == item.fileName && item.fileName.isNotBlank())
    check(directory.isDirectory || directory.mkdirs()) { "Cannot create downloads directory" }
    val partial = File(directory, "${item.fileName}.part")
    val stateFile = hlsStateFile(directory, item.fileName)

    var playlist = client.fetchWithRetry(item, item.sourceUrl)
    var text = playlist.bytes.decodeToString()
    if (!text.trimStart('\uFEFF', ' ', '\n', '\r', '\t').startsWith("#EXTM3U")) {
        val snippet = text.replace(Regex("\\s+"), " ").take(160)
        throw DownloadInvalidContentException("O servidor nao devolveu uma playlist HLS: $snippet")
    }
    if (HlsPlaylistParser.isMasterPlaylist(text)) {
        val variantUrl = HlsPlaylistParser.selectVariantUrl(text, playlist.finalUrl)
        playlist = client.fetchWithRetry(item, variantUrl)
        text = playlist.bytes.decodeToString()
        if (HlsPlaylistParser.isMasterPlaylist(text)) {
            throw DownloadInvalidContentException("Playlist HLS aninhada nao suportada.")
        }
    }
    val segments = HlsPlaylistParser.parseMedia(text, playlist.finalUrl)

    // Resume from the last fully written segment when the sidecar still matches this playlist.
    val saved = stateFile.takeIf(File::isFile)?.let(::readHlsState)
    var startIndex = 0
    var writtenBytes = 0L
    if (saved != null &&
        saved.segmentCount == segments.size &&
        saved.completedSegments in 0..segments.size &&
        partial.isFile && partial.length() >= saved.bytes
    ) {
        startIndex = saved.completedSegments
        writtenBytes = saved.bytes
        RandomAccessFile(partial, "rw").use { it.setLength(writtenBytes) }
    } else {
        partial.delete()
    }
    writeHlsState(stateFile, segments.size, startIndex, writtenBytes)

    // Progress is reported against an estimated total (bytes so far scaled by playlist duration).
    val totalDuration = segments.sumOf { it.durationSeconds }
    val weights = segments.map { if (totalDuration > 0.0) it.durationSeconds else 1.0 }
    val totalWeight = weights.sum()
    val prefixWeights = DoubleArray(segments.size + 1)
    for (i in segments.indices) prefixWeights[i + 1] = prefixWeights[i] + weights[i]
    fun estimateTotal(written: Long, completed: Int): Long? {
        if (completed <= 0 || prefixWeights[completed] <= 0.0) return null
        return (written / prefixWeights[completed] * totalWeight).toLong().coerceAtLeast(written)
    }

    onHeaders(estimateTotal(writtenBytes, startIndex), null)
    onProgress(writtenBytes, estimateTotal(writtenBytes, startIndex))

    val keyCache = ConcurrentHashMap<String, ByteArray>()

    suspend fun loadSegment(segment: HlsSegment): ByteArray {
        var data = client.fetchWithRetry(item, segment.url).bytes
        val key = segment.key
        if (key != null) {
            val keyBytes = keyCache[key.uri]
                ?: client.fetchWithRetry(item, key.uri).bytes.also { keyCache[key.uri] = it }
            if (keyBytes.size != 16) throw DownloadInvalidContentException("Chave HLS invalida.")
            data = decryptAes128(data, keyBytes, key.iv ?: sequenceIv(segment.sequence))
        }
        return data
    }

    FileOutputStream(partial, true).use { output ->
        coroutineScope {
            val window = ArrayDeque<Deferred<ByteArray>>()
            var nextToLaunch = startIndex
            var nextToWrite = startIndex
            var lastInit: String? = if (startIndex > 0) segments[startIndex - 1].initUrl else null

            while (nextToWrite < segments.size) {
                ensureActive()
                while (window.size < HLS_PARALLEL_SEGMENTS && nextToLaunch < segments.size) {
                    val segment = segments[nextToLaunch++]
                    window.addLast(async { loadSegment(segment) })
                }
                val data = window.removeFirst().await()
                val segment = segments[nextToWrite]

                val init = segment.initUrl
                if (init != null && init != lastInit) {
                    val initBytes = client.fetchWithRetry(item, init).bytes
                    output.write(initBytes)
                    writtenBytes += initBytes.size
                    lastInit = init
                }
                output.write(data)
                writtenBytes += data.size
                output.flush()
                output.fd.sync()
                nextToWrite++
                writeHlsState(stateFile, segments.size, nextToWrite, writtenBytes)
                onProgress(writtenBytes, estimateTotal(writtenBytes, nextToWrite))
            }
        }
    }

    ensureActive()
    stateFile.delete()
    onHeaders(writtenBytes, null)
    onProgress(writtenBytes, writtenBytes)
    partial
}
