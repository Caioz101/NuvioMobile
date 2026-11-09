package com.nuvio.app.features.downloads

import com.nuvio.app.core.network.IPv4FirstDns
import com.nuvio.app.features.p2p.P2pStreamRequest
import com.nuvio.app.features.p2p.P2pStreamingEngine
import com.nuvio.app.features.p2p.P2pStreamingState
import com.nuvio.app.features.player.PlayerPlaybackNetworking
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Request

internal val downloadHttpClient = OkHttpClient.Builder()
    .connectTimeout(60, TimeUnit.SECONDS)
    .readTimeout(60, TimeUnit.SECONDS)
    .writeTimeout(60, TimeUnit.SECONDS)
    .dns(IPv4FirstDns())
    .followRedirects(true)
    .followSslRedirects(true)
    .build()

internal class DownloadHttpException(val statusCode: Int) : IOException("Download failed: HTTP $statusCode")

/** Only one torrent download runs at a time, because the P2P engine serves a single stream. */
private val p2pDownloadMutex = Mutex()

@Volatile
private var ownedP2pLocalUrl: String? = null

private const val P2P_READ_TIMEOUT_MINUTES = 10L

internal suspend fun transferAndroidDownload(
    item: DownloadItem,
    directory: File,
    validator: String?,
    client: OkHttpClient = downloadHttpClient,
    onHeaders: (totalBytes: Long?, validator: String?) -> Unit,
    onProgress: (downloadedBytes: Long, totalBytes: Long?) -> Unit,
): File {
    if (!item.isP2pTorrent) {
        // HLS: known .m3u8 URLs, or a download that already started as HLS (resume sidecar exists).
        if (isHlsUrl(item.sourceUrl) || hlsStateFile(directory, item.fileName).isFile) {
            return transferHlsDownload(item, directory, client, onHeaders, onProgress)
        }
        val file = transferHttpFile(item, item.sourceUrl, directory, validator, client, onHeaders, onProgress)
        // The URL did not look like HLS but the server answered with a playlist (proxies, redirects).
        if (looksLikeHlsPlaylist(file)) {
            file.delete()
            return transferHlsDownload(item, directory, client, onHeaders, onProgress)
        }
        return file
    }
    return p2pDownloadMutex.withLock {
        val localUrl = startP2pDownloadStream(item)
        try {
            // Loopback connection to the torrent engine: do not bind it to a cellular/Wi-Fi network,
            // and allow long reads while pieces are still arriving from peers.
            val p2pClient = downloadHttpClient.newBuilder()
                .readTimeout(P2P_READ_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                .build()
            transferHttpFile(item, localUrl, directory, validator, p2pClient, onHeaders, onProgress)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            val state = P2pStreamingEngine.state.value
            if (state is P2pStreamingState.Streaming && state.localUrl != localUrl) {
                throw DownloadInvalidContentException(
                    "Download por torrent interrompido: outro torrent foi aberto no app. Retome depois.",
                )
            }
            throw error
        } finally {
            stopP2pDownloadStream(localUrl)
        }
    }
}

private suspend fun startP2pDownloadStream(item: DownloadItem): String {
    val infoHash = requireNotNull(item.p2pInfoHash)
    val current = P2pStreamingEngine.state.value
    if (current is P2pStreamingState.Streaming && current.localUrl != ownedP2pLocalUrl) {
        throw DownloadInvalidContentException(
            "Um torrent está sendo reproduzido. Pare a reprodução e retome o download.",
        )
    }
    val localUrl = P2pStreamingEngine.startStream(
        P2pStreamRequest(
            infoHash = infoHash,
            fileIdx = item.p2pFileIdx,
            filename = item.p2pFilename,
            trackers = item.p2pTrackers,
        ),
    )
    ownedP2pLocalUrl = localUrl
    return localUrl
}

private fun stopP2pDownloadStream(localUrl: String) {
    if (ownedP2pLocalUrl == localUrl) ownedP2pLocalUrl = null
    val state = P2pStreamingEngine.state.value
    // Never stop a stream that belongs to the player.
    if (state is P2pStreamingState.Streaming && state.localUrl == localUrl) {
        P2pStreamingEngine.stopStream()
    }
}

private suspend fun transferHttpFile(
    item: DownloadItem,
    sourceUrl: String,
    directory: File,
    validator: String?,
    client: OkHttpClient,
    onHeaders: (totalBytes: Long?, validator: String?) -> Unit,
    onProgress: (downloadedBytes: Long, totalBytes: Long?) -> Unit,
): File = coroutineScope {
    val activeCall = AtomicReference<Call?>()
    val transfer = async(Dispatchers.IO) {
        try {
            require(File(item.fileName).name == item.fileName && item.fileName.isNotBlank())
            check(directory.isDirectory || directory.mkdirs()) { "Cannot create downloads directory" }
            val partial = File(directory, "${item.fileName}.part")
            var offset = partial.takeIf(File::isFile)?.length() ?: 0L
            var restarted = false

            while (true) {
                ensureActive()
                val request = Request.Builder().url(sourceUrl).apply {
                    item.sourceHeaders.forEach { (key, value) ->
                        if (!key.equals("Range", true) && !key.equals("If-Range", true) &&
                            !key.equals("Accept-Encoding", true)) header(key, value)
                    }
                    header("Accept-Encoding", "identity")
                    if (item.sourceHeaders.keys.none { it.equals("User-Agent", true) }) {
                        header("User-Agent", PlayerPlaybackNetworking.DEFAULT_USER_AGENT)
                    }
                    if (offset > 0L) {
                        header("Range", "bytes=$offset-")
                        validator?.let { header("If-Range", it) }
                    }
                }.build()
                val call = client.newCall(request)
                activeCall.set(call)
                ensureActive()
                call.execute().use { response ->
                    if (response.code == 416 && offset > 0L && !restarted) {
                        offset = 0L
                        restarted = true
                        return@use
                    }
                    if (!response.isSuccessful) throw DownloadHttpException(response.code)
                    val range = response.header("Content-Range")?.let(::parseDownloadContentRange)
                    val resumed = response.code == 206
                    if (resumed && (range == null || range.first != offset)) {
                        throw IOException("Server returned an invalid download range")
                    }
                    val startingBytes = if (resumed) offset else 0L
                    val body = response.body ?: throw IOException("Download response is empty")
                    val contentLength = body.contentLength().takeIf { it >= 0L }
                    val totalBytes = range?.second ?: contentLength?.let { startingBytes + it }
                    val responseValidator = response.header("ETag")?.takeUnless { it.startsWith("W/") }
                        ?: response.header("Last-Modified")
                    onHeaders(totalBytes, responseValidator)
                    var downloaded = startingBytes
                    onProgress(downloaded, totalBytes)
                    body.byteStream().use { input ->
                        FileOutputStream(partial, resumed && offset > 0L).use { output ->
                            val buffer = ByteArray(64 * 1024)
                            while (true) {
                                ensureActive()
                                val size = input.read(buffer)
                                if (size == -1) break
                                output.write(buffer, 0, size)
                                downloaded += size
                                onProgress(downloaded, totalBytes)
                            }
                            output.fd.sync()
                        }
                    }
                    ensureActive()
                    if (totalBytes != null && downloaded != totalBytes) {
                        throw IOException("Download ended before all bytes were received")
                    }
                    return@async partial
                }
            }
            @Suppress("UNREACHABLE_CODE")
            partial
        } catch (error: Exception) {
            // Closing a cancelled HTTP call throws IOException from its blocking read.
            ensureActive()
            throw error
        }
    }
    try {
        transfer.await()
    } finally {
        activeCall.get()?.cancel()
    }
}

internal fun parseDownloadContentRange(value: String): Pair<Long, Long?>? {
    val match = Regex("bytes (\\d+)-(\\d+)/(\\d+|\\*)").matchEntire(value.trim()) ?: return null
    val start = match.groupValues[1].toLongOrNull() ?: return null
    val end = match.groupValues[2].toLongOrNull() ?: return null
    val total = match.groupValues[3].toLongOrNull()
    if (end < start || (total != null && end >= total)) return null
    return start to total
}

internal class DownloadInvalidContentException(message: String) : Exception(message)

/**
 * Detects "downloads" that are really a tiny text response (HLS playlist, JSON/HTML error page)
 * instead of a media file. Returns a human-readable reason, or null when the file looks fine.
 */
internal fun detectUnexpectedTextResponse(file: File): String? {
    val size = file.length()
    if (size <= 0L || size > 1_048_576L) return null
    val head = ByteArray(minOf(size, 400L).toInt())
    val read = file.inputStream().use { it.read(head) }
    if (read <= 0) return null
    var printable = 0
    for (i in 0 until read) {
        val b = head[i].toInt()
        if (b == 9 || b == 10 || b == 13 || b in 32..126) printable++
    }
    if (printable.toDouble() / read < 0.85) return null
    val text = String(head, 0, read, Charsets.UTF_8).trim()
    return if (text.startsWith("#EXTM3U")) {
        "Este stream e uma playlist HLS (.m3u8), ainda sem suporte para download."
    } else {
        val snippet = text.replace(Regex("\\s+"), " ").take(160)
        "O servidor nao devolveu um video ($size B): $snippet"
    }
}
