package com.android.purebilibili.feature.plugin

import android.content.Context
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.android.purebilibili.core.plugin.Plugin
import com.android.purebilibili.core.plugin.PluginManager
import com.android.purebilibili.core.network.WbiUtils
import com.android.purebilibili.core.network.WbiKeyManager
import com.android.purebilibili.core.store.TokenManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.util.concurrent.TimeUnit

object HighQualityPlaybackPlugin : Plugin {
    override val id: String = "high_quality_playback"
    override val name: String = "高画质与番剧解析"
    override val description: String = "对接 mxd 远端服务解析大会员高画质与番剧取流"
    override val version: String = "1.0.0"
    override val author: String = "MXD / BiliPai Mod"

    var serverUrl: String = "http://mxd.indevs.in"
    var enabled: Boolean = true
    var maxQuality: Int = 116
    var lastStatus: String by mutableStateOf("尚未取流")

    val qualityOptions = listOf(
        112 to "1080P+",
        116 to "1080P60",
        120 to "4K",
        125 to "HDR",
        126 to "杜比视界",
        127 to "8K"
    )

    private val httpClient = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(25, TimeUnit.SECONDS)
        .build()

    // 严格与浏览器脚本和 PiliPlus 实测能返回 code 0 的 Desktop Chrome UA 保持一致
    private const val PC_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

    private fun getSharedPreferences() =
        PluginManager.getContext().getSharedPreferences("high_quality_playback_prefs", Context.MODE_PRIVATE)

    init {
        runCatching {
            val sp = getSharedPreferences()
            serverUrl = sp.getString("server_url", "http://mxd.indevs.in") ?: "http://mxd.indevs.in"
            enabled = sp.getBoolean("enabled", true)
            maxQuality = sp.getInt("max_quality", 116)
        }
    }

    private fun savePrefs() {
        runCatching {
            getSharedPreferences().edit()
                .putString("server_url", serverUrl)
                .putBoolean("enabled", enabled)
                .putInt("max_quality", maxQuality)
                .apply()
        }
    }

    private fun buildUiPayload(): JsonObject {
        val sessData = TokenManager.sessDataCache.orEmpty()
        val buvid3 = TokenManager.buvid3Cache.orEmpty()
        val csrf = TokenManager.csrfCache.orEmpty()
        var mid = (TokenManager.midCache ?: 0L).toString()
        if (mid == "0" || mid.isBlank()) {
            runCatching {
                val sp = PluginManager.getContext().getSharedPreferences("token_prefs", Context.MODE_PRIVATE)
                val stored = sp.getLong("mid", 0L).takeIf { it > 0 } ?: sp.getString("mid", null)?.toLongOrNull() ?: 0L
                if (stored > 0L) mid = stored.toString()
            }
        }
        val cookieStr = buildString {
            if (mid != "0") append("DedeUserID=$mid; ")
            if (sessData.isNotBlank()) append("SESSDATA=$sessData; ")
            if (buvid3.isNotBlank()) append("buvid3=$buvid3; ")
            if (csrf.isNotBlank()) append("bili_jct=$csrf; ")
        }.trimEnd(' ', ';')

        return buildJsonObject {
            put("ua", PC_UA)
            put("mid", mid)
            put("vip", 0)
            put("level", 6)
            put("money", 0)
            put("cookie", cookieStr)
            put("csrf", csrf)
            put("face", "")
            put("name", "")
        }
    }

    suspend fun resolveUgc(signedPlayUrl: String): String? = withContext(Dispatchers.IO) {
        if (!enabled || serverUrl.isBlank()) return@withContext null
        try {
            val cleanUrl = signedPlayUrl.replace(Regex("dm_.+?&"), "")
            val payload = buildJsonObject {
                put("ui", buildUiPayload())
                put("url", cleanUrl)
            }
            val req = Request.Builder()
                .url("${serverUrl.trimEnd('/')}/api/bzview2")
                .post(payload.toString().toRequestBody("application/json".toMediaType()))
                .header("User-Agent", PC_UA)
                .header("Referer", "https://www.bilibili.com/")
                .build()

            val resp = httpClient.newCall(req).execute()
            val respBody = resp.body?.string() ?: return@withContext null
            val fixed = fixCdnHost(respBody)
            lastStatus = "UGC解析成功 (${fixed.length} 字符)"
            return@withContext fixed
        } catch (e: Exception) {
            android.util.Log.w("HighQualityPlugin", "resolveUgc failed: ${e.message}")
            lastStatus = "UGC取流失败: ${e.message}"
            null
        }
    }

    suspend fun resolvePgcRaw(
        seasonId: Long?,
        episodeId: Long?,
        qn: Int = maxQuality,
        areaLimit: Boolean = false
    ): String? = withContext(Dispatchers.IO) {
        if (!enabled || serverUrl.isBlank()) return@withContext null
        try {
            val csrf = TokenManager.csrfCache.orEmpty()
            val wbiKeys = WbiKeyManager.getWbiKeys().getOrNull() ?: WbiKeyManager.refreshKeys().getOrNull()

            val baseParams = mutableMapOf(
                "x-bili-locale-json" to """{"c_locale":{"language":"zh-Hans"},"always_translate":false}""",
                "x-bili-device-req-json" to """{"platform":"web","device":"pc","spmid":"666.25","mobi_app":"web_cn"}"""
            )
            if (csrf.isNotBlank()) {
                baseParams["csrf"] = csrf
            }

            val signedParams = if (wbiKeys != null && wbiKeys.first.isNotBlank() && wbiKeys.second.isNotBlank()) {
                WbiUtils.sign(baseParams, wbiKeys.first, wbiKeys.second, includeRiskFingerprint = false)
            } else baseParams

            val queryStr = signedParams.entries.joinToString("&") { "${it.key}=${java.net.URLEncoder.encode(it.value, "UTF-8")}" }
            val ogvUrl = "https://api.bilibili.com/ogv/player/playview?$queryStr"

            val ogvBody = buildJsonObject {
                put("scene", "normal")
                putJsonObject("video_index") {
                    put("bvid", JsonNull)
                    put("cid", JsonNull)
                    if (seasonId != null) put("ogv_season_id", seasonId) else put("ogv_season_id", JsonNull)
                    if (episodeId != null) put("ogv_episode_id", episodeId) else put("ogv_episode_id", JsonNull)
                }
                putJsonObject("video_param") {
                    put("qn", qn)
                }
                putJsonObject("player_param") {
                    put("fnver", 0)
                    put("fnval", 4048)
                    put("drm_tech_type", 2)
                    put("version_name", "4.10.1")
                    put("app_id", 100)
                }
                putJsonObject("exp_info") {
                    put("ogv_half_pay", true)
                }
            }.toString()

            val bzview3Payload = buildJsonObject {
                put("iic", areaLimit)
                put("url", ogvUrl)
                put("body", ogvBody)
                put("ui", buildUiPayload())
            }

            val req = Request.Builder()
                .url("${serverUrl.trimEnd('/')}/api/bzview3")
                .post(bzview3Payload.toString().toRequestBody("application/json".toMediaType()))
                .header("User-Agent", PC_UA)
                .header("Referer", "https://www.bilibili.com/")
                .build()

            val resp = httpClient.newCall(req).execute()
            val respBody = resp.body?.string() ?: return@withContext null
            val fixed = fixCdnHost(respBody)
            lastStatus = "番剧解析完成 (${fixed.length} 字符)"
            android.util.Log.d("HighQualityPlugin", "resolvePgcRaw success (${fixed.length} chars)")
            return@withContext fixed
        } catch (e: Exception) {
            android.util.Log.w("HighQualityPlugin", "resolvePgcRaw failed: ${e.message}")
            lastStatus = "番剧取流失败: ${e.message}"
            null
        }
    }

    private fun fixCdnHost(jsonStr: String): String {
        return jsonStr.replace("-mirrorcosov.", "-mirrorcos.")
    }

    @Composable
    override fun SettingsContent() {
        var server by remember { mutableStateOf(serverUrl) }
        var isEnabled by remember { mutableStateOf(enabled) }
        var currentMaxQn by remember { mutableStateOf(maxQuality) }

        Column(modifier = Modifier.padding(16.dp)) {
            Text("高画质与番剧取流服务 (mxd 后端)", style = MaterialTheme.typography.titleMedium)
            Spacer(modifier = Modifier.height(12.dp))
            Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                Text("启用远端解析")
                Switch(checked = isEnabled, onCheckedChange = {
                    isEnabled = it
                    enabled = it
                    savePrefs()
                })
            }
            Spacer(modifier = Modifier.height(8.dp))
            OutlinedTextField(
                value = server,
                onValueChange = {
                    server = it
                    serverUrl = it
                    savePrefs()
                },
                label = { Text("服务地址 (默认 http://mxd.indevs.in)") },
                modifier = Modifier.fillMaxWidth()
            )
            Spacer(modifier = Modifier.height(16.dp))
            Text("目标画质上限", style = MaterialTheme.typography.titleSmall)
            Spacer(modifier = Modifier.height(8.dp))
            Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                qualityOptions.take(3).forEach { (code, label) ->
                    FilterChip(
                        selected = currentMaxQn == code,
                        onClick = {
                            currentMaxQn = code
                            maxQuality = code
                            savePrefs()
                        },
                        label = { Text(label) }
                    )
                }
            }
            Spacer(modifier = Modifier.height(4.dp))
            Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                qualityOptions.drop(3).forEach { (code, label) ->
                    FilterChip(
                        selected = currentMaxQn == code,
                        onClick = {
                            currentMaxQn = code
                            maxQuality = code
                            savePrefs()
                        },
                        label = { Text(label) }
                    )
                }
            }
            Spacer(modifier = Modifier.height(16.dp))
            Text("当前取流状态: $lastStatus", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}
