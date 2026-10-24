// 文件路径: data/repository/VideoRepository.kt
package com.android.purebilibili.data.repository

import com.android.purebilibili.core.cache.PlayUrlCache
import com.android.purebilibili.core.coroutines.AppScope
import com.android.purebilibili.core.network.AppSignUtils
import com.android.purebilibili.core.network.NetworkModule
import com.android.purebilibili.core.network.WbiKeyManager
import com.android.purebilibili.core.network.WbiUtils
import com.android.purebilibili.core.store.AccountSessionIdentity
import com.android.purebilibili.core.store.SettingsManager
import com.android.purebilibili.core.store.TokenManager
import com.android.purebilibili.core.util.NetworkUtils
import com.android.purebilibili.data.model.response.*
import com.android.purebilibili.feature.video.progress.PbpProgressData
import com.android.purebilibili.feature.video.progress.buildDanmakuDensityValues
import com.android.purebilibili.feature.video.progress.parsePbpProgressData
import com.android.purebilibili.feature.video.subtitle.SubtitleCue
import com.android.purebilibili.feature.video.subtitle.normalizeBilibiliSubtitleUrl
import com.android.purebilibili.feature.video.subtitle.parseBiliSubtitleBody
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import com.android.purebilibili.feature.video.ui.pager.PORTRAIT_PLAYBACK_TARGET_QUALITY
import com.android.purebilibili.feature.video.ui.pager.shouldUsePortraitParallelPlaybackBootstrap
import kotlinx.coroutines.flow.first
import okhttp3.CacheControl
import okhttp3.Request
import java.io.InputStream
import java.security.MessageDigest
import java.util.TreeMap
import java.util.concurrent.ConcurrentHashMap

private const val SUBTITLE_CUE_CACHE_MAX_ENTRIES = 512
private const val SUBTITLE_CUE_CACHE_ENTRY_OVERHEAD_BYTES = 512L
private const val SUBTITLE_CUE_ESTIMATED_BYTES_PER_CUE = 160L

internal fun shouldStartHomePreload(
    hasPreloadedData: Boolean,
    hasActivePreloadTask: Boolean
): Boolean {
    return !hasPreloadedData && !hasActivePreloadTask
}

internal fun shouldPrimeBuvidForHomePreload(feedApiType: SettingsManager.FeedApiType): Boolean {
    return feedApiType == SettingsManager.FeedApiType.MOBILE
}

internal fun shouldReuseInFlightPreloadForHomeRequest(
    idx: Int,
    isPreloading: Boolean,
    hasPreloadedData: Boolean
): Boolean {
    return idx == 0 && isPreloading && !hasPreloadedData
}

internal fun shouldReportHomeDataReadyForSplash(
    hasCompletedPreload: Boolean,
    hasPreloadedData: Boolean
): Boolean {
    return hasCompletedPreload || hasPreloadedData
}

internal data class HomePreloadSession(
    val identity: AccountSessionIdentity,
    val cachedMid: Long? = identity.mid,
    val sessData: String? = null,
    val accessToken: String? = null,
    val accessTokenPlatform: String = TokenManager.ACCESS_TOKEN_PLATFORM_TV,
) {
    val isConsistent: Boolean
        get() = identity.mid == cachedMid?.takeIf { it > 0L }
}

/** A single, single-consumption preload for the current account generation and credentials. */
internal class HomePreloadState(
    private val currentSession: () -> HomePreloadSession,
) {
    private class Entry(val session: HomePreloadSession) {
        lateinit var deferred: Deferred<Result<List<VideoItem>>>
        var cached: Result<List<VideoItem>>? = null
        var completed = false
    }

    private val lock = Any()
    private var entry: Entry? = null

    private fun currentEntryLocked(session: HomePreloadSession): Entry? {
        val previous = entry ?: return null
        if (previous.session != session || !session.isConsistent) {
            entry = null
            previous.deferred.cancel(CancellationException("Home preload account changed"))
            return null
        }
        return previous
    }

    private fun requestedEntryLocked(session: HomePreloadSession): Entry? {
        val current = currentSession()
        val currentEntry = currentEntryLocked(current)
        if (!current.isConsistent || session != current) {
            throw CancellationException("Home request account changed")
        }
        return currentEntry
    }

    fun captureSession(): HomePreloadSession = synchronized(lock) {
        val session = currentSession()
        currentEntryLocked(session)
        if (!session.isConsistent) throw CancellationException("Home account credentials are changing")
        session
    }

    fun ensureCurrent(session: HomePreloadSession) {
        synchronized(lock) { requestedEntryLocked(session) }
    }

    fun isReady(): Boolean = synchronized(lock) {
        val current = currentEntryLocked(currentSession())
        shouldReportHomeDataReadyForSplash(
            hasCompletedPreload = current?.completed == true,
            hasPreloadedData = current?.cached != null,
        )
    }

    fun start(
        scope: CoroutineScope,
        load: suspend () -> Result<List<VideoItem>>,
    ): Boolean {
        val deferred = synchronized(lock) {
            val session = currentSession()
            val previous = currentEntryLocked(session)
            if (!session.isConsistent || previous?.completed == true ||
                !shouldStartHomePreload(
                    hasPreloadedData = previous?.cached != null,
                    hasActivePreloadTask = previous?.deferred?.isCompleted == false,
                )
            ) return false

            val next = Entry(session)
            next.deferred = scope.async(start = CoroutineStart.LAZY) {
                currentCoroutineContext().ensureActive()
                ensureCurrent(session)
                val result = load()
                (result.exceptionOrNull() as? CancellationException)?.let { throw it }
                currentCoroutineContext().ensureActive()
                synchronized(lock) {
                    if (requestedEntryLocked(session) !== next) {
                        throw CancellationException("Home preload was replaced")
                    }
                    next.cached = result
                    next.completed = true
                }
                result
            }
            entry = next
            next.deferred.invokeOnCompletion { cause ->
                if (cause != null) {
                    synchronized(lock) {
                        if (entry === next) entry = null
                    }
                }
            }
            next.deferred
        }
        deferred.start()
        return true
    }

    suspend fun consumeOrAwait(session: HomePreloadSession): Result<List<VideoItem>>? {
        currentCoroutineContext().ensureActive()
        val deferred = synchronized(lock) {
            val current = requestedEntryLocked(session) ?: return null
            current.cached?.let {
                current.cached = null
                return it
            }
            if (!shouldReuseInFlightPreloadForHomeRequest(
                    idx = 0,
                    isPreloading = !current.deferred.isCompleted,
                    hasPreloadedData = false,
                )
            ) return null
            current.deferred
        }
        deferred.await()
        currentCoroutineContext().ensureActive()
        return synchronized(lock) {
            val current = requestedEntryLocked(session)
            if (current == null || current.deferred !== deferred) return null
            current.cached.also { current.cached = null }
        }
    }
}

internal fun resolveHomeFeedWbiKeys(
    cachedKeys: Pair<String, String>?,
    navWbiImg: WbiImg?
): Pair<String, String>? {
    if (cachedKeys != null) return cachedKeys
    val wbiImg = navWbiImg ?: return null
    val imgKey = wbiImg.img_url.substringAfterLast("/").substringBefore(".")
    val subKey = wbiImg.sub_url.substringAfterLast("/").substringBefore(".")
    return if (imgKey.isNotEmpty() && subKey.isNotEmpty()) imgKey to subKey else null
}

internal fun buildSubtitleCueCacheKey(
    bvid: String,
    cid: Long,
    subtitleId: Long,
    subtitleIdStr: String,
    subtitleLan: String,
    normalizedSubtitleUrl: String
): String {
    val urlHash = MessageDigest.getInstance("SHA-1")
        .digest(normalizedSubtitleUrl.toByteArray(Charsets.UTF_8))
        .joinToString(separator = "") { "%02x".format(it) }
    val idPart = subtitleIdStr.takeIf { it.isNotBlank() }
        ?: subtitleId.takeIf { it > 0L }?.toString()
        ?: "no-id"
    return "${bvid.ifBlank { "unknown" }}:${cid.coerceAtLeast(0L)}:${idPart}:${subtitleLan.ifBlank { "unknown" }}:$urlHash"
}

internal fun estimateSubtitleCueCacheBytes(
    entryCount: Int,
    totalCueCount: Int
): Long {
    val normalizedEntryCount = entryCount.coerceAtLeast(0)
    val normalizedCueCount = totalCueCount.coerceAtLeast(0)
    return normalizedEntryCount * SUBTITLE_CUE_CACHE_ENTRY_OVERHEAD_BYTES +
        normalizedCueCount * SUBTITLE_CUE_ESTIMATED_BYTES_PER_CUE
}

data class SubtitleCueCacheStats(
    val entryCount: Int,
    val totalCueCount: Int,
    val estimatedBytes: Long
)

// UP 主卡片模型下沉 core-data（VideoCatalogRepository 迁移），原包名通过 typealias 保留。
typealias CreatorCardStats = com.android.purebilibili.data.model.response.CreatorCardStats

object VideoRepository {
    private data class InitialPlayUrlKey(
        val accountScope: String,
        val bvid: String,
        val cid: Long,
        val quality: Int,
        val audioLang: String?
    )
    private val initialPlayUrlRequests = OnDemandRequestCache<InitialPlayUrlKey, PlayUrlData?>(
        maxEntries = 1,
        maxAgeMs = 0L
    )

    fun cachedVideoCid(bvid: String, aid: Long = 0L): Long =
        SharedContentRepository.cachedDetailCid(bvid, aid)

    fun clearVideoDetailCaches() {
        SharedContentRepository.clearDetailCache()
        VideoCatalogRepository.clearRelatedVideosCache()
    }

    fun invalidateVideoLoadCaches(bvid: String, aid: Long, cid: Long) {
        val cachedInfo = SharedContentRepository.cachedDetailInfo(bvid, aid)
        val canonicalBvid = cachedInfo?.bvid?.takeIf { it.isNotBlank() } ?: bvid
        val resolvedCid = cid.takeIf { it > 0L } ?: cachedInfo?.cid ?: 0L
        SharedContentRepository.invalidateDetail(bvid, aid)
        if (canonicalBvid != bvid) SharedContentRepository.invalidateDetail(canonicalBvid)
        VideoCatalogRepository.invalidateRelatedVideos(canonicalBvid)
        if (resolvedCid > 0L) PlayUrlCache.invalidate(canonicalBvid, resolvedCid)
        initialPlayUrlRequests.invalidateWhere {
            (it.bvid == bvid || it.bvid == canonicalBvid) && (cid <= 0L || it.cid == cid)
        }
    }
    private val api = NetworkModule.api
    // Subtitle cache is shared with TV by SubtitleContentRepository.
    private val verticalVideoCache = ConcurrentHashMap<String, Boolean>()

    private val QUALITY_CHAIN = listOf(120, 116, 112, 80, 74, 64, 32, 16)
    private const val PLAY_URL_RISK_COOLDOWN_MS = 120_000L
    @Volatile
    private var playUrlCooldownUntilMs = 0L
    
    internal fun playbackAccount() = NetworkModule.playbackAccount()

    internal fun isUsingDedicatedPlaybackAccount(): Boolean = playbackAccount() != null

    internal fun hasPlaybackSessionCookie(): Boolean {
        val account = playbackAccount()
        return if (account != null) account.sessData.isNotBlank() else !TokenManager.sessDataCache.isNullOrBlank()
    }

    internal fun playbackAccessToken(): String? {
        val account = playbackAccount()
        return if (account != null) account.accessToken.takeIf { it.isNotBlank() } else TokenManager.accessTokenCache
    }

    internal fun playbackAccessTokenPlatform(): String =
        playbackAccount()?.accessTokenPlatform ?: TokenManager.accessTokenPlatformCache

    internal fun isPlaybackVip(): Boolean =
        playbackAccount()?.isVip ?: TokenManager.isVipCache

    /** 播放会话是否已登录（优先播放账号，其次主账号）。 */
    internal fun isPlaybackLoggedIn(): Boolean =
        resolveVideoPlaybackAuthState(
            hasSessionCookie = hasPlaybackSessionCookie(),
            hasAccessToken = !playbackAccessToken().isNullOrEmpty()
        )

    fun getSubtitleCueCacheStats(): SubtitleCueCacheStats {
        val snapshot = SubtitleContentRepository.cachedCues()
        val entryCount = snapshot.size
        val totalCueCount = snapshot.sumOf { it.size }
        return SubtitleCueCacheStats(
            entryCount = entryCount,
            totalCueCount = totalCueCount,
            estimatedBytes = estimateSubtitleCueCacheBytes(
                entryCount = entryCount,
                totalCueCount = totalCueCount
            )
        )
    }

    fun clearSubtitleCueCache() {
        SubtitleContentRepository.clearCache()
    }

    // 播放器沿用此查询入口读取 Web 播放校验冷却。
    internal fun getAppApiCooldownRemainingMs(nowMs: Long = System.currentTimeMillis()): Long {
        return (playUrlCooldownUntilMs - nowMs).coerceAtLeast(0L)
    }

    internal fun isAppApiCoolingDown(nowMs: Long = System.currentTimeMillis()): Boolean {
        return getAppApiCooldownRemainingMs(nowMs) > 0L
    }

    suspend fun getVideoTitle(
        bvid: String,
        aid: Long = 0L
    ): Result<String> = withContext(Dispatchers.IO) {
        try {
            val lookup = resolveVideoInfoLookupInput(rawBvid = bvid, aid = aid)
                ?: throw Exception("无效的视频标识: bvid=$bvid, aid=$aid")
            val response = if (lookup.bvid.isNotEmpty()) {
                api.getVideoInfo(lookup.bvid)
            } else {
                api.getVideoInfoByAid(lookup.aid)
            }
            val info = response.data ?: throw Exception("视频详情为空: ${response.code}")
            val title = info.title.trim()
            if (title.isEmpty()) throw Exception("视频标题为空")
            Result.success(title)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    
    // buvid 引导已下沉 core-data/BuvidManager；保留原入口供手机既有调用路径使用。
    private suspend fun ensurePlaybackBuvid() {
        com.android.purebilibili.core.network.BuvidManager.ensureBuvid3(playbackAccount())
    }

    /**
     * 公开的 buvid3 初始化函数 - 供其他 Repository 调用
     */
    suspend fun ensureBuvid3() {
        ensurePlaybackBuvid()
    }

    private fun currentHomePreloadSession() = HomePreloadSession(
        identity = TokenManager.accountIdentity.value,
        cachedMid = TokenManager.midCache,
        sessData = TokenManager.sessDataCache,
        accessToken = TokenManager.accessTokenCache,
        accessTokenPlatform = TokenManager.accessTokenPlatformCache,
    )

    private val homePreloadState = HomePreloadState(::currentHomePreloadSession)

    fun isHomeDataReady(): Boolean = homePreloadState.isReady()

    // 等待冷启动凭据恢复后再绑定身份，不将已登录账号的预加载误绑定为匿名会话。
    fun preloadHomeData(scope: CoroutineScope = AppScope.ioScope) {
        scope.launch(Dispatchers.IO) {
            TokenManager.awaitRestore()
            homePreloadState.start(this) {
                com.android.purebilibili.core.util.Logger.d("VideoRepo", "🚀 Starting home data preload...")
                try {
                    val feedApiType = NetworkModule.appContext
                        ?.let { SettingsManager.getFeedApiTypeSync(it) }
                        ?: SettingsManager.FeedApiType.WEB
                    if (shouldPrimeBuvidForHomePreload(feedApiType)) {
                        // 移动端推荐流可能依赖 buvid 会话，保留预热。
                        ensurePlaybackBuvid()
                    } else {
                        com.android.purebilibili.core.util.Logger.d(
                            "VideoRepo",
                            "🚀 Skip buvid warmup for WEB home preload"
                        )
                    }
                    getHomeVideosInternal(idx = 0).also { result ->
                        com.android.purebilibili.core.util.Logger.d(
                            "VideoRepo",
                            "🚀 Home data preload finished. Success=${result.isSuccess}"
                        )
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    com.android.purebilibili.core.util.Logger.e("VideoRepo", "🚀 Home data preload failed", e)
                    Result.failure(e)
                }
            }
        }
    }

    suspend fun getVideoInfoOnly(
        bvid: String,
        aid: Long = 0L,
        requestedCid: Long = 0L
    ): Result<ViewInfo> = SharedContentRepository.detail(bvid, aid, requestedCid)

    suspend fun getInitialPlayUrlData(
        bvid: String,
        cid: Long,
        targetQuality: Int?,
        audioLang: String? = null
    ): PlayUrlData? = withContext(Dispatchers.IO) {
        if (bvid.isBlank() || cid <= 0L) return@withContext null
        TokenManager.awaitRestore()
        val isAutoHighestQuality = targetQuality != null && targetQuality >= 127
        val isVip = isPlaybackVip()
        val startQuality = resolveInitialStartQuality(targetQuality)
        val accountScope = videoPlaybackAccountScope()
        val key = InitialPlayUrlKey(accountScope, bvid, cid, startQuality, audioLang)
        val result = initialPlayUrlRequests.getOrLoad(key) {
            if (!shouldSkipPlayUrlCache(isAutoHighestQuality, isVip, audioLang)) {
                val cached = PlayUrlCache.get(
                    bvid = bvid,
                    cid = cid,
                    requestedQuality = startQuality,
                    audioLang = audioLang,
                    accountScope = accountScope
                )
                if (cached != null) return@getOrLoad cached
            }
            val fetched = fetchPlaybackUrl(
                bvid = bvid,
                cid = cid,
                targetQn = startQuality,
                audioLang = audioLang,
                requestKind = PlayUrlRequestKind.INITIAL
            ) ?: return@getOrLoad null
            if (accountScope != videoPlaybackAccountScope()) {
                throw CancellationException("Playback account changed")
            }
            val discoveredData = if (startQuality <= 32) fetched.data else supplementPlaybackQualities(fetched.data) { missingQuality ->
                if (accountScope != videoPlaybackAccountScope()) {
                    throw CancellationException("Playback account changed")
                }
                // Match PiliPlus: one WBI request for the highest missing tier below the
                // actual maximum. A failed supplement must preserve the first playable result.
                fetchPlayUrlWithWbiInternal(bvid, cid, missingQuality, audioLang)
            }
            if (accountScope != videoPlaybackAccountScope()) {
                throw CancellationException("Playback account changed")
            }
            if (shouldCachePlayUrlResult(
                    source = fetched.source,
                    audioLang = audioLang,
                    requestedQuality = startQuality,
                    returnedQuality = discoveredData.quality,
                    dashVideoIds = discoveredData.dash?.video?.map { it.id }.orEmpty(),
                    requestKind = PlayUrlRequestKind.INITIAL
                )
            ) {
                PlayUrlCache.put(
                    bvid = bvid,
                    cid = cid,
                    data = discoveredData,
                    quality = startQuality,
                    audioLang = audioLang,
                    accountScope = accountScope
                )
            }
            discoveredData
        }
        if (accountScope != videoPlaybackAccountScope()) {
            throw CancellationException("Playback account changed")
        }
        result
    }

    suspend fun getPortraitPlaybackDetails(
        bvid: String,
        aid: Long = 0,
        requestedCid: Long = 0,
        targetQuality: Int = PORTRAIT_PLAYBACK_TARGET_QUALITY,
        audioLang: String? = null
    ): Result<Pair<ViewInfo, PlayUrlData>> = withContext(Dispatchers.IO) {
        try {
            if (shouldUsePortraitParallelPlaybackBootstrap(bvid, requestedCid)) {
                coroutineScope {
                    val infoDeferred = async {
                        getVideoInfoOnly(
                            bvid = bvid,
                            aid = aid,
                            requestedCid = requestedCid
                        )
                    }
                    val playUrlDeferred = async {
                        getInitialPlayUrlData(
                            bvid = bvid,
                            cid = requestedCid,
                            targetQuality = targetQuality,
                            audioLang = audioLang
                        ) ?: throw Exception("无法获取播放地址")
                    }
                    infoDeferred.await().fold(
                        onSuccess = { info ->
                            Result.success(info to playUrlDeferred.await())
                        },
                        onFailure = { error ->
                            Result.failure(error)
                        }
                    )
                }
            } else {
                getVideoDetails(
                    bvid = bvid,
                    aid = aid,
                    requestedCid = requestedCid,
                    targetQuality = targetQuality,
                    audioLang = audioLang
                )
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun preloadPortraitPlayUrl(
        bvid: String,
        cid: Long,
        aid: Long = 0L,
        targetQuality: Int = PORTRAIT_PLAYBACK_TARGET_QUALITY
    ): PlayUrlData? {
        if (bvid.isBlank()) return null
        return withContext(Dispatchers.IO) {
            if (cid <= 0L) {
                val resolved = getPortraitPlaybackDetails(
                    bvid = bvid,
                    aid = aid,
                    requestedCid = 0L,
                    targetQuality = targetQuality
                ).getOrNull()?.second
                if (resolved != null) {
                    com.android.purebilibili.core.util.Logger.d(
                        "VideoRepo",
                        "🚀 Portrait preload resolved cid and playurl: bvid=$bvid"
                    )
                }
                return@withContext resolved
            }
            val cachedPlayData = PlayUrlCache.get(
                bvid = bvid,
                cid = cid,
                requestedQuality = targetQuality
            )
            if (cachedPlayData != null) {
                com.android.purebilibili.core.util.Logger.d(
                    "VideoRepo",
                    "🚀 Portrait preload skip (cached): bvid=$bvid"
                )
                return@withContext cachedPlayData
            }
            val playData = getInitialPlayUrlData(
                bvid = bvid,
                cid = cid,
                targetQuality = targetQuality
            )
            if (playData != null) {
                com.android.purebilibili.core.util.Logger.d(
                    "VideoRepo",
                    "🚀 Portrait preloaded playurl: bvid=$bvid"
                )
            }
            playData
        }
    }

    // 首页仅首次请求可消费同一账号会话的预加载，等待期间切号也必须拒绝旧结果。
    suspend fun getHomeVideos(idx: Int = 0): Result<List<VideoItem>> = withContext(Dispatchers.IO) {
        TokenManager.awaitRestore()
        val session = homePreloadState.captureSession()
        if (idx == 0) {
            val preloaded = homePreloadState.consumeOrAwait(session)
            if (preloaded != null) {
                homePreloadState.ensureCurrent(session)
                com.android.purebilibili.core.util.Logger.d("VideoRepo", "✅ Using preloaded home data!")
                return@withContext preloaded
            }
        }
        getHomeVideosInternal(idx).also {
            currentCoroutineContext().ensureActive()
            homePreloadState.ensureCurrent(session)
        }
    }

    // [重构] 内部加载逻辑
    private suspend fun getHomeVideosInternal(idx: Int): Result<List<VideoItem>> {
        try {
            //  读取推荐流类型设置
            val context = com.android.purebilibili.core.network.NetworkModule.appContext
            val feedApiType = if (context != null) {
                com.android.purebilibili.core.store.SettingsManager.getFeedApiTypeSync(context)
            } else {
                com.android.purebilibili.core.store.SettingsManager.FeedApiType.WEB
            }
            val refreshCount = if (context != null) {
                SettingsManager.getHomeRefreshCountSync(context)
            } else {
                com.android.purebilibili.core.store.DEFAULT_HOME_REFRESH_COUNT
            }
            
            com.android.purebilibili.core.util.Logger.d(
                "VideoRepo",
                " getHomeVideos: feedApiType=$feedApiType, idx=$idx, refreshCount=$refreshCount"
            )
            
            when (feedApiType) {
                com.android.purebilibili.core.store.SettingsManager.FeedApiType.MOBILE -> {
                    // 尝试使用移动端 API
                    val mobileResult = fetchMobileFeed(idx = idx, refreshCount = refreshCount)
                    if (mobileResult.isSuccess && mobileResult.getOrNull()?.isNotEmpty() == true) {
                        return mobileResult
                    } else {
                        // 移动端 API 失败，回退到 Web API
                        com.android.purebilibili.core.util.Logger.d("VideoRepo", " Mobile API failed, fallback to Web API")
                        return fetchWebFeed(idx = idx, refreshCount = refreshCount)
                    }
                }
                com.android.purebilibili.core.store.SettingsManager.FeedApiType.MERGED -> {
                    // 合并模式(参数对齐 PiliNara 原版):
                    // Web 半边用 fresh_type=4/feed_version=V8/brush=idx 等参数, 携带 cookie;
                    // App 半边用 android_hd 身份头取流(剥离 cookie, 已登录时带 access_key),
                    // 并行请求后交错合并、按视频去重
                    return coroutineScope {
                        val webDeferred = async { fetchMergedWebFeed(idx = idx, refreshCount = refreshCount) }
                        val mobileDeferred = async { fetchMergedMobileFeed(idx = idx) }
                        val webResult = webDeferred.await()
                        val mobileResult = mobileDeferred.await()

                        val webList = webResult.getOrNull().orEmpty()
                        val mobileList = mobileResult.getOrNull().orEmpty()

                        if (webList.isEmpty() && mobileList.isEmpty()) {
                            // 两者都失败：优先返回 web 的失败原因，其次移动端
                            val error = webResult.exceptionOrNull() ?: mobileResult.exceptionOrNull()
                                ?: Exception("获取合并推荐流失败")
                            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Merged feed both failed: ${error.message}")
                            Result.failure(error)
                        } else {
                            com.android.purebilibili.core.util.Logger.d(
                                "VideoRepo",
                                " Merged feed: web=${webList.size}, mobile=${mobileList.size}"
                            )
                            Result.success(com.android.purebilibili.feature.home.HomeFeedMergePolicy.mergeFeeds(web = webList, app = mobileList))
                        }
                    }
                }
                else -> return fetchWebFeed(idx = idx, refreshCount = refreshCount)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            e.printStackTrace()
            return Result.failure(e)
        }
    }
    
    //  Web 端推荐流 (WBI 签名)
    private suspend fun fetchWebFeed(idx: Int, refreshCount: Int): Result<List<VideoItem>> {
        try {
            val cachedKeys = WbiKeyManager.getWbiKeys().getOrNull()
            val navWbiImg = if (cachedKeys == null) api.getNavInfo().data?.wbi_img else null
            val resolvedKeys = resolveHomeFeedWbiKeys(
                cachedKeys = cachedKeys,
                navWbiImg = navWbiImg
            ) ?: throw Exception("无法获取 Key")
            val (imgKey, subKey) = resolvedKeys

            val params = mapOf(
                "ps" to refreshCount.toString(), "fresh_type" to "3", "fresh_idx" to idx.toString(),
                "feed_version" to System.currentTimeMillis().toString(), "y_num" to idx.toString()
            )
            val signedParams = WbiUtils.sign(params, imgKey, subKey)
            val feedResp = SharedContentRepository.recommendationResponse(signedParams)
            
            //  [调试] 检查 API 是否返回 dimension 字段
            feedResp.data?.item?.take(3)?.forEachIndexed { index, item ->
                com.android.purebilibili.core.util.Logger.d("VideoRepo", 
                    " 视频[$index]: ${item.title?.take(15)}... dimension=${item.dimension} isVertical=${item.dimension?.isVertical}")
            }
            
            val list = feedResp.data?.item?.filter { it.adInfo == null }?.map { it.toVideoItem() }?.filter { it.bvid.isNotEmpty() } ?: emptyList()
            
            //  [调试] 检查转换后的 VideoItem
            val verticalCount = list.count { it.isVertical }
            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Web推荐: total=${list.size}, vertical=$verticalCount")
            
            return Result.success(list)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            e.printStackTrace()
            return Result.failure(e)
        }
    }
    
    //  移动端推荐流 (appkey + sign 签名)
    private suspend fun fetchMobileFeed(idx: Int, refreshCount: Int): Result<List<VideoItem>> {
        try {
            val accessToken = TokenManager.accessTokenCache
            if (accessToken.isNullOrEmpty()) {
                com.android.purebilibili.core.util.Logger.d("VideoRepo", " No access_token, fallback to Web API")
                return Result.failure(Exception("需要登录才能使用移动端推荐流"))
            }
            
            val params = mapOf(
                "idx" to idx.toString(),
                "pull" to if (idx == 0) "1" else "0",  // 1=刷新, 0=加载更多
                "column" to "4",  // 4列布局
                "flush" to "5",   // 刷新间隔
                "autoplay_card" to "11",
                "ps" to refreshCount.toString(),
                "access_key" to accessToken,
                "appkey" to AppSignUtils.TV_APP_KEY,
                "ts" to AppSignUtils.getTimestamp().toString(),
                "mobi_app" to "android",
                "device" to "android",
                "build" to "8130300"
            )
            
            val signedParams = AppSignUtils.signForTvLogin(params)
            
            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Mobile feed request: idx=$idx")
            val feedResp = api.getMobileFeed(signedParams)
            
            if (feedResp.code != 0) {
                com.android.purebilibili.core.util.Logger.d("VideoRepo", " Mobile feed error: code=${feedResp.code}, msg=${feedResp.message}")
                return Result.failure(Exception(feedResp.message))
            }
            
            val list = feedResp.data?.items
                ?.filter { it.isSupportedRecommendation }
                ?.map { it.toVideoItem() }
                ?.filter { it.bvid.isNotEmpty() }
                ?: emptyList()
            
            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Mobile推荐: total=${list.size}")
            
            return Result.success(list)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Mobile feed exception: ${e.message}")
            return Result.failure(e)
        }
    }
    
    //  [合并模式] Web 端推荐流 - 参数对齐 PiliNara 原版合并模式(携带 cookie)
    //  (feed_version=V8 常量会话、fresh_type=4、brush=idx、version=1、homepage_ver=1)
    //  仅用于 FeedApiType.MERGED, 不影响 Web 单独模式
    private suspend fun fetchMergedWebFeed(idx: Int, refreshCount: Int): Result<List<VideoItem>> {
        try {
            val cachedKeys = WbiKeyManager.getWbiKeys().getOrNull()
            val navWbiImg = if (cachedKeys == null) api.getNavInfo().data?.wbi_img else null
            val resolvedKeys = resolveHomeFeedWbiKeys(
                cachedKeys = cachedKeys,
                navWbiImg = navWbiImg
            ) ?: throw Exception("无法获取 Key")
            val (imgKey, subKey) = resolvedKeys

            val params = mapOf(
                "version" to "1",
                "feed_version" to "V8",
                "homepage_ver" to "1",
                "ps" to refreshCount.toString(),
                "fresh_idx" to idx.toString(),
                "brush" to idx.toString(),
                "fresh_type" to "4"
            )
            val signedParams = WbiUtils.sign(params, imgKey, subKey)
            val feedResp = SharedContentRepository.recommendationResponse(signedParams)

            val list = feedResp.data?.item?.filter { it.adInfo == null }?.map { it.toVideoItem() }?.filter { it.bvid.isNotEmpty() } ?: emptyList()

            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Merged Web推荐: total=${list.size}")

            return Result.success(list)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            e.printStackTrace()
            return Result.failure(e)
        }
    }

    //  [合并模式] App 端推荐流 - 参数对齐 PiliNara 原版合并模式
    //  该 app 端点由 CookieJar 剥离 cookie(见 MergedAppFeedCookiePolicy), 身份由 HD 身份头承担;
    //  已登录时补 access_key。与 App 单独模式(TV appkey + mobi_app=android)完全无关。
    private suspend fun fetchMergedMobileFeed(idx: Int): Result<List<VideoItem>> {
        try {
            // app 取流依赖 buvid 会话, 缺失时先通过 SPI 获取
            // 先等会话备份异步恢复完成，避免启动窗口内误判 buvid 缺失而多打一次 SPI。
            TokenManager.awaitRestore()
            if (TokenManager.buvid3Cache.isNullOrEmpty()) {
                ensurePlaybackBuvid()
            }
            val params = mutableMapOf(
                "build" to "2001100",
                "c_locale" to "zh_CN",
                "channel" to "master",
                "column" to "4",
                "device" to "pad",
                "device_name" to "android",
                "device_type" to "0",
                "disable_rcmd" to "0",
                "flush" to "5",
                "fnval" to "976",
                "fnver" to "0",
                "force_host" to "2",
                "fourk" to "1",
                "guidance" to "0",
                "https_url_req" to "0",
                "idx" to idx.toString(),
                "mobi_app" to "android_hd",
                "network" to "wifi",
                "platform" to "android",
                "player_net" to "1",
                "pull" to if (idx == 0) "true" else "false",  // 首页 true=刷新, 往后 false=加载更多
                "qn" to "32",
                "recsys_mode" to "0",
                "s_locale" to "zh_CN",
                "splash_id" to "",
                "statistics" to "{\"appId\":5,\"platform\":3,\"version\":\"2.0.1\",\"abtest\":\"\"}",
                "ts" to AppSignUtils.getTimestamp().toString(),
                "voice_balance" to "0"
            )
            // 已登录时补 access_key: 对齐 PiliNara 原版(其对 app 端点补 access_key 后再签名)
            TokenManager.accessTokenCache
                ?.takeIf { it.isNotBlank() }
                ?.let { params["access_key"] = it }

            // key/value percent-encode 后拼接签名,
            // 再用 encoded=true 端点原样发送, 保证签名与线上 query 完全一致
            val signedParams = AppSignUtils.signForAndroidHdLogin(params)
            val encodedParams = signedParams.mapValues { (_, value) -> AppSignUtils.percentEncode(value) }

            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Merged Mobile feed request: idx=$idx")
            val feedResp = api.getMobileFeedEncoded(encodedParams)

            if (feedResp.code != 0) {
                com.android.purebilibili.core.util.Logger.d("VideoRepo", " Merged Mobile feed error: code=${feedResp.code}, msg=${feedResp.message}")
                return Result.failure(Exception(feedResp.message))
            }

            val list = feedResp.data?.items
                ?.filter { it.isSupportedRecommendation }
                ?.map { it.toVideoItem() }
                ?.filter { it.bvid.isNotEmpty() }
                ?: emptyList()

            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Merged Mobile推荐: total=${list.size}")

            return Result.success(list)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            com.android.purebilibili.core.util.Logger.d("VideoRepo", " Merged Mobile feed exception: ${e.message}")
            return Result.failure(e)
        }
    }
    
    //  [新增] 热门视频（下沉 core-data/VideoCatalogRepository，两端消费同一实现）
    suspend fun getPopularVideos(page: Int = 1): Result<List<VideoItem>> = VideoCatalogRepository.getPopularVideos(page)

    suspend fun getRankingVideos(rid: Int = 0, type: String = "all"): Result<List<VideoItem>> = VideoCatalogRepository.getRankingVideos(rid, type)

    suspend fun getPreciousVideos(): Result<List<VideoItem>> = VideoCatalogRepository.getPreciousVideos()

    suspend fun getWeeklyPeriods(): Result<List<PopularSeriesPeriod>> = VideoCatalogRepository.getWeeklyPeriods()

    suspend fun getWeeklyPeriod(number: Int): Result<PopularSeriesOneData> = VideoCatalogRepository.getWeeklyPeriod(number)

    suspend fun getWeeklyMustWatchVideos(number: Int? = null): Result<List<VideoItem>> = VideoCatalogRepository.getWeeklyMustWatchVideos(number)

    //  [新增] 分区视频（按分类 ID 获取视频；下沉后两端消费同一实现）
    suspend fun getRegionVideos(tid: Int, page: Int = 1): Result<List<VideoItem>> = VideoCatalogRepository.getRegionVideos(tid, page)
    
    //  [新增] 上报播放心跳（记录到历史记录）
    suspend fun reportPlayHeartbeat(
        bvid: String,
        cid: Long,
        playedTime: Long = 0,
        realPlayedTime: Long = playedTime,
        startTsSec: Long = System.currentTimeMillis() / 1000L,
        aid: Long = 0L,
        epid: Long = 0L,
        sid: Long = 0L,
        videoType: Int = 3,
        subType: Int? = null
    ): Boolean {
        val success = HistoryRepository.reportPlayback(bvid, cid, playedTime, realPlayedTime,
            startTsSec, aid, epid, sid, videoType, subType).isSuccess
        if (success) com.android.purebilibili.core.refresh.HistoryRefreshBus.notifyChanged()
        return success
    }


    suspend fun getNavInfo(): Result<NavData> = withContext(Dispatchers.IO) {
        try {
            val resp = api.getNavInfo()
            if (resp.code == 0 && resp.data != null) {
                val checkedRespData = requireNotNull(resp.data)
                Result.success(checkedRespData)
            } else {
                if (resp.code == -101) {
                    Result.success(NavData(isLogin = false))
                } else {
                    Result.failure(Exception("错误码: ${resp.code}"))
                }
            }
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    /**
     * 按「播放会话」（优先播放账号，其次主账号）查询 nav，用于决定播放画质/会员状态。
     * 与 [getNavInfo]（始终主账号）不同，这里跟随 [NetworkModule.playbackApi] 的账号归属。
     */
    internal suspend fun getPlaybackNavInfo(): Result<NavData> = withContext(Dispatchers.IO) {
        try {
            val resp = NetworkModule.playbackApi().getNavInfo()
            if (resp.code == 0 && resp.data != null) {
                val checkedRespData = requireNotNull(resp.data)
                Result.success(checkedRespData)
            } else {
                if (resp.code == -101) {
                    Result.success(NavData(isLogin = false))
                } else {
                    Result.failure(Exception("错误码: ${resp.code}"))
                }
            }
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun refreshVipStatusForPreferredQualityIfNeeded(
        isLoggedIn: Boolean,
        cachedIsVip: Boolean,
        storedQuality: Int,
        autoHighestEnabled: Boolean
    ): Boolean {
        if (
            !com.android.purebilibili.core.util.shouldRefreshVipStatusBeforeResolvingDefaultQuality(
                storedQuality = storedQuality,
                autoHighestEnabled = autoHighestEnabled,
                isLoggedIn = isLoggedIn,
                cachedIsVip = cachedIsVip
            )
        ) {
            return cachedIsVip
        }

        // 按播放会话刷新：有独立播放账号时查播放账号的 nav，且不污染主账号 VIP 缓存。
        return getPlaybackNavInfo()
            .getOrNull()
            ?.takeIf { it.isLogin }
            ?.let { navData ->
                val isVip = navData.vip.status == 1
                if (!isUsingDedicatedPlaybackAccount()) {
                    TokenManager.isVipCache = isVip
                }
                com.android.purebilibili.core.util.Logger.d(
                    "VideoRepo",
                    " Refreshed VIP status before quality resolution: cached=$cachedIsVip, refreshed=$isVip, storedQuality=$storedQuality, autoHighest=$autoHighestEnabled, dedicatedPlayback=${isUsingDedicatedPlaybackAccount()}"
                )
                isVip
            }
            ?: cachedIsVip
    }

    suspend fun getCreatorCardStats(mid: Long): Result<CreatorCardStats> = VideoCatalogRepository.getCreatorCardStats(mid)

    suspend fun isVerticalVideo(bvid: String, aid: Long = 0L): Boolean = withContext(Dispatchers.IO) {
        val normalizedBvid = bvid.trim()
        if (normalizedBvid.isEmpty() && aid <= 0L) return@withContext false
        val cacheKey = normalizedBvid.ifEmpty { "av$aid" }
        verticalVideoCache[cacheKey]?.let { return@withContext it }
        try {
            val lookup = resolveVideoInfoLookupInput(rawBvid = normalizedBvid, aid = aid)
                ?: return@withContext false
            val viewResp = if (lookup.bvid.isNotEmpty()) {
                api.getVideoInfo(lookup.bvid)
            } else {
                api.getVideoInfoByAid(lookup.aid)
            }
            val isVertical = viewResp.data?.dimension?.isVertical == true
            verticalVideoCache[cacheKey] = isVertical
            isVertical
        } catch (_: Exception) {
            false
        }
    }

    // [修复] 添加 aid 参数支持，修复移动端推荐流视频播放失败问题
    suspend fun getVideoDetails(
        bvid: String,
        aid: Long = 0,
        requestedCid: Long = 0L,
        targetQuality: Int? = null,
        audioLang: String? = null
    ): Result<Pair<ViewInfo, PlayUrlData>> = withContext(Dispatchers.IO) {
        try {
            val info = getVideoInfoOnly(bvid, aid, requestedCid).getOrThrow()
            val playData = getInitialPlayUrlData(
                bvid = info.bvid,
                cid = info.cid,
                targetQuality = targetQuality,
                audioLang = audioLang
            ) ?: error("无法获取播放地址")
            Result.success(info to playData)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            Result.failure(error)
        }
    }

    // [新增] 获取 AI 视频总结
    suspend fun getAiSummary(bvid: String, cid: Long, upMid: Long): Result<AiSummaryResponse> = withContext(Dispatchers.IO) {
        ensurePlaybackBuvid()
        logAiSummaryPreflight(
            bvid = bvid,
            cid = cid,
            upMid = upMid
        )

        try {
            val (imgKey, subKey) = getWbiKeys()
            val params = buildAiSummaryParams(
                bvid = bvid,
                cid = cid,
                upMid = upMid
            )
            val signedParams = WbiUtils.sign(params, imgKey, subKey)

            com.android.purebilibili.core.util.Logger.d(
                "VideoRepo",
                "🤖 AI Summary request: bvid=$bvid cid=$cid upMidPresent=${upMid > 0L}"
            )
            val response = api.getAiConclusion(signedParams)
            val diagnosis = diagnoseAiSummaryResponse(response)
            logAiSummaryResponse(
                bvid = bvid,
                cid = cid,
                diagnosis = diagnosis,
                hasModelResult = response.data?.modelResult != null,
                summaryLength = response.data?.modelResult?.summary?.length ?: 0,
                outlineCount = response.data?.modelResult?.outline?.size ?: 0
            )

            Result.success(response)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            val diagnosis = diagnoseAiSummaryFailure(e)
            if ((e as? retrofit2.HttpException)?.code() == 412 || e.message.orEmpty().contains("412")) {
                wbiKeysCache = null
                wbiKeysTimestamp = 0L
            }
            com.android.purebilibili.core.util.Logger.w(
                "VideoRepo",
                "🤖 AI Summary request failed: bvid=$bvid cid=$cid status=${diagnosis.status} reason=${diagnosis.reason} retryable=${diagnosis.shouldRetryRequest}"
            )
            Result.failure(e)
        }
    }

    private fun buildAiSummaryParams(
        bvid: String,
        cid: Long,
        upMid: Long
    ): Map<String, String> {
        val params = linkedMapOf(
            "bvid" to bvid,
            "cid" to cid.toString()
        )
        if (upMid > 0L) {
            params["up_mid"] = upMid.toString()
        }
        return params
    }

    private fun logAiSummaryPreflight(
        bvid: String,
        cid: Long,
        upMid: Long
    ) {
        val hasSess = !TokenManager.sessDataCache.isNullOrEmpty()
        val hasCsrf = !TokenManager.csrfCache.isNullOrEmpty()
        val hasBuvid = !TokenManager.buvid3Cache.isNullOrEmpty()
        val hasAccessToken = !TokenManager.accessTokenCache.isNullOrEmpty()
        com.android.purebilibili.core.util.Logger.i(
            "VideoRepo",
            "🤖 AI Summary preflight: bvid=$bvid cid=$cid upMidPresent=${upMid > 0L} hasSess=$hasSess hasCsrf=$hasCsrf hasBuvid=$hasBuvid hasAccessToken=$hasAccessToken"
        )
    }

    private fun logAiSummaryResponse(
        bvid: String,
        cid: Long,
        diagnosis: AiSummaryFetchDiagnosis,
        hasModelResult: Boolean,
        summaryLength: Int,
        outlineCount: Int
    ) {
        com.android.purebilibili.core.util.Logger.i(
            "VideoRepo",
            "🤖 AI Summary response: bvid=$bvid cid=$cid status=${diagnosis.status} reason=${diagnosis.reason} rootCode=${diagnosis.rootCode} dataCode=${diagnosis.dataCode} stid=${diagnosis.stid ?: ""} hasModelResult=$hasModelResult summaryLength=$summaryLength outlineCount=$outlineCount retryLater=${diagnosis.shouldRetryLater}"
        )
    }

    //  [优化] WBI Key 缓存
    private var wbiKeysCache: Pair<String, String>? = null
    private var wbiKeysTimestamp: Long = 0
    private const val WBI_CACHE_DURATION = 1000 * 60 * 30 //  优化：30分钟缓存
    
    //  412 错误冷却期（避免过快重试触发风控）
    private var last412Time: Long = 0
    private const val COOLDOWN_412_MS = 5000L // 412 后等待 5 秒

    private suspend fun getWbiKeys(): Pair<String, String> {
        val currentCheck = System.currentTimeMillis()
        val cached = wbiKeysCache
        if (cached != null && (currentCheck - wbiKeysTimestamp < WBI_CACHE_DURATION)) {
            return cached
        }

        //  [优化] 增加重试逻辑，最多 3 次尝试
        val maxRetries = 3
        var lastError: Exception? = null
        
        for (attempt in 1..maxRetries) {
            try {
                val navResp = api.getNavInfo()
                val wbiImg = navResp.data?.wbi_img
                
                if (wbiImg != null) {
                    val imgKey = wbiImg.img_url.substringAfterLast("/").substringBefore(".")
                    val subKey = wbiImg.sub_url.substringAfterLast("/").substringBefore(".")
                    
                    wbiKeysCache = Pair(imgKey, subKey)
                    wbiKeysTimestamp = System.currentTimeMillis()
                    com.android.purebilibili.core.util.Logger.d("VideoRepo", " WBI Keys obtained successfully (attempt $attempt)")
                    return wbiKeysCache!!
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                lastError = e
                android.util.Log.w("VideoRepo", "getWbiKeys attempt $attempt failed: ${e.message}")
                if (attempt < maxRetries) {
                    kotlinx.coroutines.delay(200L * attempt) // 递增延迟
                }
            }
        }
        
        throw Exception("Wbi Keys Error after $maxRetries attempts: ${lastError?.message}")
    }

    suspend fun getPlayUrlData(bvid: String, cid: Long, qn: Int, audioLang: String? = null): PlayUrlData? = withContext(Dispatchers.IO) {
        fetchPlaybackUrl(
            bvid = bvid,
            cid = cid,
            targetQn = qn,
            audioLang = audioLang,
            requestKind = PlayUrlRequestKind.EXPLICIT
        )?.data
    }

    /**
     * Fetch a playable stream while moving between parts of the same video.
     *
     * Unlike a user-requested quality change, a part transition may fall back when the target
     * part does not expose the quality that was playing in the previous part.
     */
    suspend fun getPlayUrlDataForPlaybackTransition(
        bvid: String,
        cid: Long,
        qn: Int,
        audioLang: String? = null
    ): PlayUrlData? = withContext(Dispatchers.IO) {
        getInitialPlayUrlData(bvid, cid, targetQuality = qn, audioLang = audioLang)
    }

    /** 自动升级只查询目标高级画质，沿用当前播放账号的 Web Cookie。 */
    suspend fun getExactPremiumPlayUrl(
        bvid: String,
        cid: Long,
        targetQn: Int,
        audioLang: String? = null
    ): PlayUrlData? = withContext(Dispatchers.IO) {
        if (targetQn !in 125..127 || isAppApiCoolingDown()) return@withContext null
        val payload = try {
            fetchPlayUrlWithWbiInternal(bvid, cid, targetQn, audioLang)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            com.android.purebilibili.core.util.Logger.w("VideoRepo", " Exact quality lookup failed: ${error.message}")
            null
        } ?: return@withContext null
        payload.takeIf { hasExactPlayableRequestedTrack(targetQn, it.dash?.video.orEmpty()) }
    }

    suspend fun getTvCastPlayData(
        aid: Long,
        cid: Long,
        qn: Int
    ): PlayUrlData? = withContext(Dispatchers.IO) {
        if (aid <= 0L || cid <= 0L) return@withContext null

        try {
            val params = buildTvCastPlayUrlParams(
                aid = aid,
                cid = cid,
                qn = qn,
                accessToken = playbackAccessToken()
            )
            val signedParams = AppSignUtils.signForTvLogin(params)
            val response = NetworkModule.playbackApi().getTvPlayUrl(signedParams)
            if (response.code != 0) {
                com.android.purebilibili.core.util.Logger.w(
                    "VideoRepo",
                    " tvPlayUrl failed: code=${response.code}, msg=${response.message}"
                )
                return@withContext null
            }
            response.data
        } catch (e: Exception) {
            com.android.purebilibili.core.util.Logger.w("VideoRepo", " tvPlayUrl exception: ${e.message}")
            null
        }
    }

    suspend fun getTvCastPlayUrl(
        aid: Long,
        cid: Long,
        qn: Int
    ): String? = extractTvCastPlayableUrl(getTvCastPlayData(aid, cid, qn))


    private class EmptyPlayUrlPayloadException(val riskControl: Boolean) :
        IllegalStateException(if (riskControl) "播放接口需要安全校验，请稍后重试" else "播放接口未返回可用地址，请稍后重试")

    private data class PlayUrlFetchResult(
        val data: PlayUrlData,
        val source: PlayUrlSource
    )

    //  [v2 优化] 核心播放地址获取逻辑 - 根据登录状态区分策略
    private suspend fun fetchPlaybackUrl(
        bvid: String,
        cid: Long,
        targetQn: Int,
        audioLang: String? = null,
        requestKind: PlayUrlRequestKind
    ): PlayUrlFetchResult? {
        // 设备标识来自当前账号的持久 Cookie，并仅执行该账号的激活。
        ensurePlaybackBuvid()

        val isLoggedIn = resolveVideoPlaybackAuthState(
            hasSessionCookie = hasPlaybackSessionCookie(),
            hasAccessToken = !playbackAccessToken().isNullOrEmpty()
        )
        com.android.purebilibili.core.util.Logger.d("VideoRepo", " fetchPlaybackUrl: bvid=$bvid, isLoggedIn=$isLoggedIn, targetQn=$targetQn, audioLang=$audioLang")

        return fetchWebPlayback(bvid, cid, targetQn, audioLang, requestKind)
    }

    private fun hasPlayableStreams(data: PlayUrlData?): Boolean {
        if (data == null) return false
        return !data.durl.isNullOrEmpty() || !data.dash?.video.isNullOrEmpty()
    }

    // 普通播放只请求当前账号的 Web/WBI 地址，失败交给调用方展示，不切换鉴权来源。
    private suspend fun fetchWebPlayback(
        bvid: String,
        cid: Long,
        targetQn: Int,
        audioLang: String? = null,
        requestKind: PlayUrlRequestKind
    ): PlayUrlFetchResult? {
        val payload = fetchPlayUrlWithWbiInternal(bvid, cid, targetQn, audioLang) ?: return null
        val dashIds = payload.playableVideoQualityIds()
        if (!shouldAcceptAppApiResultForTargetQuality(requestKind, targetQn, payload.quality, dashIds)) {
            com.android.purebilibili.core.util.Logger.w(
                "VideoRepo", " Requested track unavailable: requested=$targetQn returned=${payload.quality} playable=$dashIds"
            )
            return null
        }
        return PlayUrlFetchResult(payload, PlayUrlSource.DASH)
    }

    /**
     * [新增] 获取预览视频地址 (简单 MP4 URL)
     * 用于首页长按预览播放，优先尝试获取低画质 MP4
     */
    suspend fun getPreviewVideoUrl(bvid: String, cid: Long): String? {
        if (bvid.isBlank()) return null
        // Recommendation cards may omit cid. Resolve it without entering the
        // full player or creating a playback/history session.
        val resolvedCid = if (cid > 0L) cid else getVideoInfoOnly(bvid).getOrThrow().cid
        if (resolvedCid <= 0L) return null
        val data = fetchAsGuestFallback(bvid, resolvedCid, qualities = listOf(16, 32))
        return data?.durl?.firstOrNull()?.let { segment ->
            (listOf(segment.url) + segment.backupUrl.orEmpty()).firstOrNull { it.isNotBlank() }
        }
    }
    
    //  [新增] 以游客身份获取视频（忽略登录凭证）
    //  [修复] 使用 guestApi 确保不携带 SESSDATA/bili_jct
    private suspend fun fetchAsGuestFallback(
        bvid: String,
        cid: Long,
        qualities: List<Int> = buildGuestFallbackQualities(),
    ): PlayUrlData? {
        try {
            com.android.purebilibili.core.util.Logger.d("VideoRepo", " fetchAsGuestFallback: bvid=$bvid, cid=$cid (using guestApi)")
            
            // ✅ 使用 guestApi - 不携带登录凭证
            val guestApi = NetworkModule.guestApi

            for (guestQn in qualities) {
                val legacyResult = guestApi.getPlayUrlLegacy(
                    bvid = bvid,
                    cid = cid,
                    qn = guestQn,
                    fnval = 1, // MP4 格式
                    platform = "html5", // HTML5 平台
                    highQuality = if (guestQn >= 64) 1 else 0
                )

                if (legacyResult.code == 0 && legacyResult.data != null) {
                    val checkedLegacyResultData = requireNotNull(legacyResult.data)
                    val data = checkedLegacyResultData
                    if (!data.durl.isNullOrEmpty()) {
                        com.android.purebilibili.core.util.Logger.d(
                            "VideoRepo",
                            " Guest fallback (Legacy ${guestQn}p) success: actual=${data.quality}"
                        )
                        return data
                    }
                } else {
                    com.android.purebilibili.core.util.Logger.d(
                        "VideoRepo",
                        " Guest fallback ${guestQn}p failed: code=${legacyResult.code}"
                    )
                }
            }
            
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (e: Exception) {
            android.util.Log.w("VideoRepo", "Guest fallback failed: ${e.message}")
        }
        
        return null
    }
    
    //  内部方法：单次请求播放地址 (使用 fnval=4048 获取全部 DASH 流)
    private suspend fun fetchPlayUrlWithWbiInternal(bvid: String, cid: Long, qn: Int, audioLang: String? = null): PlayUrlData? {
        com.android.purebilibili.core.util.Logger.d("VideoRepo", "fetchPlayUrlWithWbiInternal: bvid=$bvid, cid=$cid, qn=$qn, audioLang=$audioLang")
        
        //  使用缓存的 Keys
        if (isAppApiCoolingDown()) {
            throw IllegalStateException("播放接口安全校验冷却中，请稍后重试")
        }
        val (imgKey, subKey) = getWbiKeys()
        val isLoggedIn = resolveVideoPlaybackAuthState(
            hasSessionCookie = hasPlaybackSessionCookie(),
            hasAccessToken = !playbackAccessToken().isNullOrEmpty()
        )
        val auto1080pEnabled = NetworkModule.appContext?.let { context ->
            runCatching { SettingsManager.getAuto1080p(context).first() }.getOrDefault(true)
        } ?: true
        
        val params = buildPlayUrlWbiBaseParams(
            bvid = bvid,
            cid = cid,
            qn = qn,
            audioLang = audioLang,
            tryLook = shouldRequestPlayUrlTryLook(
                isLoggedIn = isLoggedIn,
                auto1080pEnabled = auto1080pEnabled
            )
        )

        val signedParams = WbiUtils.sign(params, imgKey, subKey)
        val response = NetworkModule.playbackApi().getPlayUrl(signedParams)
        
        com.android.purebilibili.core.util.Logger.d("VideoRepo", " PlayUrl response: code=${response.code}, requestedQn=$qn, returnedQuality=${response.data?.quality}")
        com.android.purebilibili.core.util.Logger.d("VideoRepo", " accept_quality=${response.data?.accept_quality}, accept_description=${response.data?.accept_description}")
        //  [调试] 输出 DASH 视频流 ID 列表
        val dashIds = response.data?.dash?.video?.map { it.id }?.distinct()?.sortedDescending()
        com.android.purebilibili.core.util.Logger.d("VideoRepo", " DASH video IDs: $dashIds")
        
        val hasVoucher = !response.data?.vVoucher.isNullOrBlank()
        if (isPlayUrlRiskControlResponse(response.code, hasVoucher)) {
            playUrlCooldownUntilMs = System.currentTimeMillis() + PLAY_URL_RISK_COOLDOWN_MS
            com.android.purebilibili.core.util.Logger.w(
                "VideoRepo",
                " PlayUrl risk response: code=${response.code}, requestedQn=$qn, hasVoucher=$hasVoucher"
            )
            throw EmptyPlayUrlPayloadException(riskControl = true)
        }
        if (response.code == 0) {
            var payload = response.data
            if (hasPlayableStreams(payload)) {
                if (com.android.purebilibili.feature.plugin.HighQualityPlaybackPlugin.enabled &&
                    (!isLoggedIn || !isPlaybackVip())
                ) {
                    val signedUrl = "https://api.bilibili.com/x/player/wbi/playurl?" + signedParams.entries.joinToString("&") { (k, v) ->
                        "$k=${java.net.URLEncoder.encode(v, "UTF-8")}"
                    }
                    payload = com.android.purebilibili.feature.plugin.HighQualityPlaybackPlugin.enhanceUgcPlayUrl(
                        basePayload = payload!!,
                        signedPlayUrl = signedUrl,
                        requestedQn = qn
                    )
                }
                return payload
            }
            com.android.purebilibili.core.util.Logger.w(
                "VideoRepo",
                " PlayUrl success but empty payload: requestedQn=$qn, returnedQuality=${payload?.quality}, dashIds=$dashIds, " +
                    "hasData=${payload != null}, hasVoucher=$hasVoucher, hasSess=$isLoggedIn, " +
                    "hasAccessToken=${!playbackAccessToken().isNullOrBlank()}, webRequest=true"
            )
            throw EmptyPlayUrlPayloadException(riskControl = false)
        }
        
        //  [优化] API 返回错误码分类处理，提供更明确的错误信息
        val errorMessage = classifyPlayUrlError(response.code, response.message)
        android.util.Log.e("VideoRepo", " PlayUrl API error: code=${response.code}, message=${response.message}, classified=$errorMessage")
        // 对于不可重试的错误，抛出明确异常
        if (response.code in listOf(-404, -403, -10403, -62002)) {
            throw Exception(errorMessage)
        }
        return null
    }
    
    fun init(context: android.content.Context) {
        AppScope.ioScope.launch {
            val feedApiType = SettingsManager.getFeedApiTypeSync(context)
            // WEB 推荐流不依赖设备激活预热，冷启动时优先首页请求。
            if (shouldPrimeBuvidForHomePreload(feedApiType)) {
                runCatching { ensurePlaybackBuvid() }
            }
            // 使用共享 WbiKeyManager（磁盘恢复已在启动任务中完成），避免再走私有 nav 链路。
            runCatching { WbiKeyManager.getWbiKeys() }
        }
    }

    /**
     * 获取视频预览图数据 (Videoshot API)
     * 
     * 用于进度条拖动时显示视频缩略图预览
     * @param bvid 视频 BV 号
     * @param cid 视频 CID
     * @return VideoshotData 或 null（如果获取失败）
     */
    suspend fun getVideoshot(bvid: String, cid: Long): VideoshotData? = withContext(Dispatchers.IO) {
        try {
            com.android.purebilibili.core.util.Logger.d("VideoRepo", "🖼️ getVideoshot: bvid=$bvid, cid=$cid")
            val response = api.getVideoshot(bvid = bvid, cid = cid)
            if (response.code == 0 && response.data != null && requireNotNull(response.data).isValid) {
                val checkedResponseData = requireNotNull(response.data)
                com.android.purebilibili.core.util.Logger.d("VideoRepo", "🖼️ Videoshot success: ${checkedResponseData.image.size} images, ${checkedResponseData.index.size} frames")
                checkedResponseData
            } else {
                com.android.purebilibili.core.util.Logger.d("VideoRepo", "🖼️ Videoshot failed: code=${response.code}")
                null
            }
        } catch (e: Exception) {
            android.util.Log.w("VideoRepo", "🖼️ Videoshot exception: ${e.message}")
            null
        }
    }

    // [修复] 获取播放器信息 (BGM/ViewPoints/Etc) — WBI 签名
    suspend fun getPlayerInfo(bvid: String, cid: Long): Result<PlayerInfoData> =
        SubtitleContentRepository.playerInfo(bvid, cid)

    suspend fun getPbpProgressData(
        bvid: String,
        cid: Long,
        aid: Long = 0L
    ): Result<PbpProgressData> = withContext(Dispatchers.IO) {
        try {
            if (cid <= 0L) {
                return@withContext Result.failure(
                    IllegalArgumentException("PBP cid invalid: $cid")
                )
            }
            val body = api.getPbpData(
                cid = cid,
                bvid = bvid.takeIf { it.isNotBlank() },
                aid = aid.takeIf { it > 0L }
            )
            Result.success(parsePbpProgressData(body.string()))
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    /**
     * 本地弹幕密度曲线：拉取全部分段弹幕，按秒桶计数并归一化。
     * 官方 pbp 接口（bvc.bilivideo.com/pbp/data）已 404，此为兜底数据源，
     * 输出结构与 pbp 对齐（秒桶计数，由渲染层归一化），渲染层无需感知差异。
     */
    suspend fun getDanmakuDensityProgressData(
        cid: Long,
        durationSeconds: Long,
    ): Result<PbpProgressData> = withContext(Dispatchers.IO) {
        try {
            if (cid <= 0L || durationSeconds <= 0L) {
                return@withContext Result.failure(
                    IllegalArgumentException("弹幕密度曲线参数无效: cid=$cid duration=${durationSeconds}s")
                )
            }
            val stepSeconds = (durationSeconds / 240).coerceIn(2L, 10L).toInt()
            val segments = DanmakuRepository.getDanmakuSegments(
                cid = cid,
                durationMs = durationSeconds * 1000L
            )
            if (segments.isEmpty()) {
                return@withContext Result.failure(IllegalStateException("弹幕分段为空"))
            }
            val parsed = com.android.purebilibili.danmaku.parser.DanmakuParser.parseProtobuf(segments)
            if (parsed.serverDisabled) {
                return@withContext Result.failure(IllegalStateException("UP主已关闭该视频弹幕"))
            }
            val values = buildDanmakuDensityValues(
                positionsMs = parsed.standardList.map { it.showAtTime },
                durationSeconds = durationSeconds,
                stepSeconds = stepSeconds
            )
            if (values.all { it == 0f }) {
                return@withContext Result.failure(IllegalStateException("该视频弹幕密度为零"))
            }
            Result.success(PbpProgressData(stepSeconds = stepSeconds, values = values))
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun getSubtitleCues(subtitleUrl: String, bvid: String, cid: Long, subtitleId: Long = 0L,
        subtitleIdStr: String = "", subtitleLan: String = ""): Result<List<SubtitleCue>> =
        SubtitleContentRepository.cues(subtitleUrl, bvid, cid, subtitleId, subtitleIdStr, subtitleLan)

    suspend fun getInteractEdgeInfo(
        bvid: String,
        graphVersion: Long,
        edgeId: Long? = null
    ): Result<InteractEdgeInfoData> = withContext(Dispatchers.IO) {
        try {
            val response = api.getInteractEdgeInfo(bvid = bvid, graphVersion = graphVersion, edgeId = edgeId)
            if (response.code == 0 && response.data != null) {
                val checkedResponseData = requireNotNull(response.data)
                Result.success(checkedResponseData)
            } else {
                Result.failure(Exception(response.message.ifBlank { "互动分支信息加载失败(${response.code})" }))
            }
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    
    suspend fun getRelatedVideos(bvid: String): List<RelatedVideo> = VideoCatalogRepository.getRelatedVideos(bvid)

    suspend fun getRelatedVideosResult(bvid: String): Result<List<RelatedVideo>> =
        VideoCatalogRepository.getRelatedVideosResult(bvid)

    suspend fun getCachedRelatedVideosResult(bvid: String): Result<List<RelatedVideo>>? =
        VideoCatalogRepository.getCachedRelatedVideosResult(bvid)


    //  [新增] API 错误码分类，提供用户友好的错误提示
    private fun classifyPlayUrlError(code: Int, message: String?): String {
        return when (code) {
            -404 -> "视频不存在或已被删除"
            -403 -> "视频暂不可用"
            -10403 -> {
                when {
                    message?.contains("地区") == true -> "该视频在当前地区不可用"
                    message?.contains("会员") == true || message?.contains("vip") == true -> "需要大会员才能观看"
                    else -> "视频需要特殊权限才能观看"
                }
            }
            -62002 -> "视频已设为私密"
            -62004 -> "视频正在审核中"
            -62012 -> "视频已下架"
            87008 -> "当前视频可能是专属视频，可能需包月充电观看"
            -400 -> "请求参数错误"
            -101 -> "未登录，请先登录"
            -352 -> "请求频率过高，请稍后再试"
            else -> "获取播放地址失败 (错误码: $code)"
        }
    }
}
