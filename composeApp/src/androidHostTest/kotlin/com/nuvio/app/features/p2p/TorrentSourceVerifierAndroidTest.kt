package com.nuvio.app.features.p2p

import com.nuvio.engine.NuvioTorrentFile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class TorrentSourceVerifierAndroidTest {
    @Test
    fun selectsRequestedVideoFileByIndex() {
        val files = listOf(
            torrentFile(0, "Show/cover.jpg", 50_000L),
            torrentFile(1, "Show/episode.mkv", 900_000_000L),
        )

        assertEquals(1, selectTorrentVideoFile(files, 1, null)?.index)
    }

    @Test
    fun selectsRequestedVideoFileByFilename() {
        val files = listOf(
            torrentFile(0, "Show/episode-1.mkv", 800_000_000L),
            torrentFile(1, "Show/episode-2.mkv", 850_000_000L),
        )

        assertEquals(1, selectTorrentVideoFile(files, null, "episode-2.mkv")?.index)
    }

    @Test
    fun doesNotReplaceAnUnmatchedFilenameWithAnotherEpisode() {
        val files = listOf(
            torrentFile(0, "Show/episode-1.mkv", 800_000_000L),
            torrentFile(1, "Show/episode-2.mkv", 850_000_000L),
        )

        assertNull(selectTorrentVideoFile(files, null, "episode-3.mkv"))
    }

    @Test
    fun choosesLargestVideoOnlyWhenNoFileHintExists() {
        val files = listOf(
            torrentFile(0, "Show/poster.jpg", 2_000_000L),
            torrentFile(1, "Show/episode.mkv", 800_000_000L),
            torrentFile(2, "Show/sample.mp4", 20_000_000L),
        )

        assertEquals(1, selectTorrentVideoFile(files, null, null)?.index)
    }

    private fun torrentFile(index: Int, path: String, size: Long) = NuvioTorrentFile(
        index = index,
        offset = 0L,
        size = size,
        path = path,
        pathTruncated = false,
    )
}
