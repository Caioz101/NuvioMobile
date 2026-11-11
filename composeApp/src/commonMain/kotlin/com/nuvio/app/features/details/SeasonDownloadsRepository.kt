package com.nuvio.app.features.details

import com.nuvio.app.core.ui.NuvioToastController
import com.nuvio.app.features.downloads.DownloadEnqueueResult
import com.nuvio.app.features.downloads.DownloadsRepository
import com.nuvio.app.features.debrid.DirectDebridPlayableResult
import com.nuvio.app.features.debrid.DirectDebridPlaybackResolver
import com.nuvio.app.features.streams.StreamsRepository
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

internal data class SeasonDownloadEpisode(
    val videoId: String,
    val seasonNumber: Int,
    val episodeNumber: Int,
    val title: String?,
    val thumbnail: String?,
)

/** Keeps season source discovery alive when the details screen is closed. */
internal object SeasonDownloadsRepository {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun start(
        contentType: String,
        parentMetaId: String,
        title: String,
        logo: String?,
        poster: String?,
        background: String?,
        seasonNumber: Int,
        episodes: List<SeasonDownloadEpisode>,
    ) {
        if (episodes.isEmpty()) {
            NuvioToastController.show("No released episodes to download")
            return
        }

        NuvioToastController.show("Preparing season downloads...")
        scope.launch {
            var queued = 0
            var unavailable = 0
            DownloadsRepository.ensureLoaded()
            for (episode in episodes) {
                    try {
                        val state = StreamsRepository.loadForBatch(
                            type = contentType,
                            videoId = episode.videoId,
                            parentMetaId = parentMetaId,
                            season = episode.seasonNumber,
                            episode = episode.episodeNumber,
                        )
                        val candidates = state?.groups?.flatMap { it.streams }.orEmpty()
                        var episodeQueued = false

                        for (candidate in candidates) {
                            val playable = try {
                                if (DirectDebridPlaybackResolver.shouldResolveToPlayableStream(candidate)) {
                                    when (val resolved = DirectDebridPlaybackResolver.resolveToPlayableStream(
                                        stream = candidate,
                                        season = episode.seasonNumber,
                                        episode = episode.episodeNumber,
                                    )) {
                                        is DirectDebridPlayableResult.Success -> resolved.stream
                                        else -> null
                                    }
                                } else candidate
                            } catch (cancelled: CancellationException) {
                                throw cancelled
                            } catch (_: Exception) {
                                null
                            } ?: continue

                            val result = try {
                                DownloadsRepository.enqueueFromStream(
                                    contentType = contentType,
                                    videoId = episode.videoId,
                                    parentMetaId = parentMetaId,
                                    parentMetaType = contentType,
                                    title = title,
                                    logo = logo,
                                    poster = poster,
                                    background = background,
                                    seasonNumber = episode.seasonNumber,
                                    episodeNumber = episode.episodeNumber,
                                    episodeTitle = episode.title,
                                    episodeThumbnail = episode.thumbnail,
                                    stream = playable,
                                )
                            } catch (cancelled: CancellationException) {
                                throw cancelled
                            } catch (_: Exception) {
                                null
                            }

                            if (result == DownloadEnqueueResult.Started ||
                                result == DownloadEnqueueResult.Replaced
                            ) {
                                queued++
                                episodeQueued = true
                                break
                            }
                        }
                        if (!episodeQueued) unavailable++
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (_: Exception) {
                        unavailable++
                    }
                }

            NuvioToastController.show(
                if (unavailable == 0) "$queued episodes added to downloads"
                else "$queued episodes added; $unavailable unavailable",
            )
        }
    }
}
