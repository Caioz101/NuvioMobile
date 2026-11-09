package com.nuvio.app.features.p2p

import android.os.SystemClock
import com.nuvio.engine.NuvioEngine
import java.io.File
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

private const val TORRENT_CHECK_CACHE_BYTES = 0L
private const val TORRENT_CHECK_SAMPLE_INTERVAL_MS = 250L

internal actual suspend fun verifyTorrentSource(
    request: P2pStreamRequest,
    timeoutMs: Long,
): P2pTorrentVerificationResult = withContext(Dispatchers.IO) {
    require(timeoutMs > 0L) { "timeoutMs must be positive" }

    val context = P2pStreamingEngine.appContext
        ?: return@withContext P2pTorrentVerificationResult(
            available = false,
            reason = "O motor P2P ainda não foi inicializado",
        )

    val root = File(context.cacheDir, "nuvio-torrent-check/${UUID.randomUUID()}")
    val stateDirectory = File(root, "state")
    val cacheDirectory = File(root, "cache")
    var verifierEngine: NuvioEngine? = null
    var torrentId: String? = null

    try {
        check(stateDirectory.mkdirs() || stateDirectory.isDirectory) {
            "Não foi possível criar o diretório temporário de verificação"
        }
        check(cacheDirectory.mkdirs() || cacheDirectory.isDirectory) {
            "Não foi possível criar o cache temporário de verificação"
        }

        P2pSettingsRepository.ensureLoaded()
        val settings = P2pSettingsRepository.uiState.value
        verifierEngine = NuvioEngine.create(
            buildNuvioEngineConfig(
                stateDirectory = stateDirectory,
                cacheDirectory = cacheDirectory,
                uploadEnabled = false,
                torrentProfile = settings.torrentProfile,
                diskCacheCapacityBytes = TORRENT_CHECK_CACHE_BYTES,
            ),
        )

        val activeEngine = verifierEngine
        val verification = withTimeoutOrNull(timeoutMs) {
            val canonicalHash = canonicalP2pInfoHash(request.infoHash)
            val magnetUri = buildP2pMagnetUri(canonicalHash, request.trackers)
            torrentId = activeEngine.addMagnet(magnetUri)

            val files = activeEngine.files(torrentId!!)
            val selectedFile = selectTorrentVideoFile(
                files = files,
                fileIndex = request.fileIdx,
                filenameHint = request.filename,
            ) ?: return@withTimeoutOrNull P2pTorrentVerificationResult(
                available = false,
                reason = "O torrent não contém o arquivo de vídeo solicitado",
            )

            val startedAt = SystemClock.elapsedRealtime()
            while (SystemClock.elapsedRealtime() - startedAt < timeoutMs) {
                val seedCount = activeEngine.stats.value.connectedSeeds
                if (seedCount > 0) {
                    return@withTimeoutOrNull P2pTorrentVerificationResult(
                        available = true,
                        seeds = seedCount,
                        fileName = selectedFile.path.substringAfterLast('/'),
                        fileSize = selectedFile.size,
                    )
                }
                delay(TORRENT_CHECK_SAMPLE_INTERVAL_MS)
            }

            P2pTorrentVerificationResult(
                available = false,
                seeds = 0,
                fileName = selectedFile.path.substringAfterLast('/'),
                fileSize = selectedFile.size,
                reason = "Nenhum seed conectado dentro do tempo limite",
            )
        }

        verification ?: P2pTorrentVerificationResult(
            available = false,
            reason = "Tempo limite excedido ao obter metadados ou conectar a um seed",
        )
    } catch (error: kotlinx.coroutines.CancellationException) {
        throw error
    } catch (error: Exception) {
        P2pTorrentVerificationResult(
            available = false,
            reason = error.message ?: "Falha ao verificar o torrent",
        )
    } finally {
        withContext(NonCancellable) {
            val activeEngine = verifierEngine
            if (activeEngine != null) {
                torrentId?.let { id ->
                    try {
                        activeEngine.removeTorrent(id)
                    } catch (_: Exception) {
                        // Closing the isolated engine below also releases this torrent.
                    }
                }
                try {
                    activeEngine.shutdown()
                } catch (_: Exception) {
                    // Preserve the verification result while cleaning up best-effort.
                }
            }
            root.deleteRecursively()
        }
    }
}

internal fun selectTorrentVideoFile(
    files: List<com.nuvio.engine.NuvioTorrentFile>,
    fileIndex: Int?,
    filenameHint: String?,
): com.nuvio.engine.NuvioTorrentFile? {
    if (fileIndex != null) {
        return files.firstOrNull { it.index == fileIndex && it.size > 0L && it.isVideoFile() }
    }

    val normalizedHint = filenameHint
        ?.substringAfterLast('/')
        ?.trim()
        ?.takeIf(String::isNotEmpty)
    if (normalizedHint != null) {
        return files.firstOrNull { file ->
            file.size > 0L &&
                file.isVideoFile() &&
                file.path.substringAfterLast('/').equals(normalizedHint, ignoreCase = true)
        }
    }

    return files
        .asSequence()
        .filter { it.size > 0L && it.isVideoFile() }
        .maxByOrNull { it.size }
}

private fun com.nuvio.engine.NuvioTorrentFile.isVideoFile(): Boolean {
    val extension = path.substringAfterLast('.', "").lowercase()
    return extension in setOf("mkv", "mp4", "avi", "mov", "m4v", "ts", "webm", "mpg", "mpeg")
}
