package com.nuvio.app.features.p2p

internal actual suspend fun verifyTorrentSource(
    request: P2pStreamRequest,
    timeoutMs: Long,
): P2pTorrentVerificationResult = P2pTorrentVerificationResult(
    available = false,
    reason = "A verificação automática de torrents está disponível apenas no Android",
)
