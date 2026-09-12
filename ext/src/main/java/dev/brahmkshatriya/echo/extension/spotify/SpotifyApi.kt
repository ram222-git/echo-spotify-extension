package dev.brahmkshatriya.echo.extension.spotify

import dev.brahmkshatriya.echo.common.helpers.ClientException
import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.settings.Settings
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.internal.closeQuietly
import spotify.extendedmetadata.metadata.ExtendedMetadataProto
import spotify.extendedmetadata.metadata.ExtendedMetadataProto.BatchedExtensionResponse
import java.net.URLEncoder
import java.security.SecureRandom

class SpotifyApi {
    val json = Json()

    private val webMutex = Mutex()
    val web = TokenManagerDesktop(this)

    @Volatile var settings: Settings? = null
    private var cachedDeviceId: String? = null

    val cookie get() = _cookie
    private var _cookie: String? = null
    fun setCookie(cookie: String?) {
        _cookie = cookie
        synchronized(web) { web.clear() }
        clientTokenManager.clear()
    }

    val isDesktopPersona: Boolean
        get() = web.clientId == DesktopConfig.CLIENT_ID

    fun deviceId(): String {
        cachedDeviceId?.let { return it }
        return synchronized(this) {
            cachedDeviceId?.let { return@synchronized it }
            val stored = settings?.getString(DEVICE_ID_KEY)?.takeIf { it.isNotBlank() }
            val id = stored ?: generateDeviceId().also {
                runCatching { settings?.putString(DEVICE_ID_KEY, it) }
            }
            cachedDeviceId = id
            id
        }
    }

    val clientTokenManager = ClientTokenManager(this)

    val client = OkHttpClient.Builder()
        .addInterceptor { chain ->
            val request = chain.request()
            val builder = request.newBuilder()
            val host = request.url.host

            val desktop = isDesktopPersona
            if (desktop) applyDesktopHeaders(builder) else applyWebHeaders(builder)

            if (request.header("Accept") == null) {
                builder.header("Accept", "application/json")
            }

            val isClientTokenEndpoint = host.contains("clienttoken")
            if (!isClientTokenEndpoint) {
                web.accessToken?.let {
                    if (request.header("Authorization") == null)
                        builder.header("Authorization", "Bearer $it")
                }
                clientTokenManager.clientToken?.let {
                    builder.header("client-token", it)
                }
                builder.header(
                    "spotify-app-version",
                    if (desktop) DesktopConfig.appVersion else WebPlayerConfig.appVersion,
                )
            }

            chain.proceed(builder.build())
        }
        .build()

    /**
     * A dedicated client that always sends Web Player (browser-like) headers regardless of persona.
     * Used for endpoints like storage-resolve that Spotify expects to look like a browser request
     * (product=9, platform=39 = web player). Using Desktop headers on these endpoints causes
     * server errors even when logged in with sp_dc.
     */
    val webPlayerClient = OkHttpClient.Builder()
        .addInterceptor { chain ->
            val request = chain.request()
            val builder = request.newBuilder()

            // Always apply web/browser headers — act like open.spotify.com in a desktop browser
            applyWebHeaders(builder)

            if (request.header("Accept") == null) {
                builder.header("Accept", "application/json")
            }

            web.accessToken?.let {
                if (request.header("Authorization") == null)
                    builder.header("Authorization", "Bearer $it")
            }
            clientTokenManager.clientToken?.let {
                builder.header("client-token", it)
            }
            builder.header("spotify-app-version", WebPlayerConfig.appVersion)

            chain.proceed(builder.build())
        }
        .build()

    private fun applyDesktopHeaders(builder: Request.Builder) {
        builder.header("User-Agent", DesktopConfig.userAgent)
        builder.header("App-Platform", DesktopConfig.PLATFORM_HEADER)
        builder.header("Accept-Language", DesktopConfig.ACCEPT_LANGUAGE)
    }

    private fun applyWebHeaders(builder: Request.Builder) {
        builder.header("User-Agent", WebPlayerConfig.USER_AGENT)
        builder.header("sec-ch-ua", WebPlayerConfig.SEC_CH_UA)
        builder.header("sec-ch-ua-mobile", WebPlayerConfig.SEC_CH_UA_MOBILE)
        builder.header("sec-ch-ua-platform", WebPlayerConfig.SEC_CH_UA_PLATFORM)
        builder.header("Origin", WebPlayerConfig.ORIGIN)
        builder.header("Referer", WebPlayerConfig.REFERER)
        builder.header("Sec-Fetch-Dest", "empty")
        builder.header("Sec-Fetch-Mode", "cors")
        builder.header("Sec-Fetch-Site", "same-site")
        builder.header("Accept-Language", WebPlayerConfig.ACCEPT_LANGUAGE)
        builder.header("App-Platform", WebPlayerConfig.APP_PLATFORM)
    }

    data class Response<T>(
        val json: T,
        val raw: String,
    )

    suspend fun graphCall(
        operationName: String,
        persistedQuery: String,
        variables: JsonObject = buildJsonObject { },
    ): String {
        val req = Request.Builder()
            .url("https://api-partner.spotify.com/pathfinder/v2/query")
            .post(
                buildJsonObject {
                    put("operationName", operationName)
                    put("variables", variables)
                    put("extensions", extensions(persistedQuery))
                }.toString().toRequestBody("application/json".toMediaType())
            )
        return callGetBody(req.build())
    }

    suspend inline fun <reified T> graphQuery(
        operationName: String,
        persistedQuery: String,
        variables: JsonObject = buildJsonObject { },
        print: Boolean = false,
    ): Response<T> {
        val raw = graphCall(operationName, persistedQuery, variables)
        if (print) println(raw)
        return Response(json.decode<T>(raw), raw)
    }

    suspend inline fun <reified T> clientQuery(path: String): Response<T> {
        val raw = callGetBody(
            Request.Builder()
                .url("https://spclient.wg.spotify.com/$path")
                .build()
        )
        return Response(json.decode<T>(raw), raw)
    }

    /**
     * Like [clientQuery] but always uses [webPlayerClient] (browser-like headers).
     * Use this for endpoints that Spotify expects to originate from a web browser,
     * such as storage-resolve (product=9, platform=39).
     */
    suspend inline fun <reified T> clientQueryWeb(path: String): Response<T> {
        ensureTokens()
        val request = Request.Builder()
            .url("https://spclient.wg.spotify.com/$path")
            .build()
        val response = webPlayerClient.newCall(request).await()
        val raw = response.body.string()
        if (!raw.startsWith('{') && !raw.startsWith('[')) {
            throw Exception("Invalid response: $raw")
        }
        return Response(json.decode<T>(raw), raw)
    }

    @PublishedApi
    internal suspend fun ensureTokens() {
        runCatching {
            webMutex.withLock { web.getToken() }
            clientTokenManager.ensureValid()
        }.getOrElse {
            val id = userId
            if (id != null && it is TokenManagerDesktop.Error) throw ClientException.Unauthorized(id)
            throw it
        }
    }

    suspend inline fun <reified T> clientMutate(path: String, data: JsonObject): Response<T> {
        val raw = callGetBody(
            Request.Builder()
                .url("https://spclient.wg.spotify.com/$path")
                .post(data.toString().toRequestBody("application/json".toMediaType()))
                .build()
        )
        return Response(json.decode<T>(raw), raw)
    }

    fun buildExtendedMetadataRequest(
        entityUris: List<String>,
        extensionKinds: List<ExtendedMetadataProto.ExtensionKind>
    ): ByteArray {
        val request = ExtendedMetadataProto.BatchedEntityRequest.newBuilder()
            .setHeader(ExtendedMetadataProto.BatchedEntityRequestHeader.getDefaultInstance())
            .addAllEntityRequest(
                entityUris.map { uri ->
                    ExtendedMetadataProto.EntityRequest.newBuilder()
                        .setEntityUri(uri)
                        .addAllQuery(
                            extensionKinds.map { kind ->
                                ExtendedMetadataProto.ExtensionQuery.newBuilder()
                                    .setExtensionKind(kind)
                                    .build()
                            }
                        )
                        .build()
                }
            )
            .build()

        return request.toByteArray()
    }


    suspend inline fun clientMutateProto(path: String, mediaId: String): BatchedExtensionResponse {
        val requestBytes = buildExtendedMetadataRequest(entityUris = listOf(mediaId), extensionKinds = listOf(
            ExtendedMetadataProto.ExtensionKind.TRACK_V4,
            ExtendedMetadataProto.ExtensionKind.AUDIO_FILES
        ))
        val raw = callGetBodyBytes(
            Request.Builder()
                .url("https://spclient.wg.spotify.com/$path")
                .header("Accept", "application/x-protobuf")
                .header("Content-Type", "application/x-protobuf")
                .post(requestBytes.toRequestBody("application/x-protobuf".toMediaType()))
                .build()
        )

        return BatchedExtensionResponse.parseFrom(raw)
    }

    suspend fun callGetBody(request: Request): String {
        ensureTokens()
        val response = call(request).body.string()
        return if (response.startsWith('{')) response else {
            throw Exception("Invalid response: $response")
        }
    }

    suspend fun callGetBodyBytes(request: Request): ByteArray {
        ensureTokens()
        val response = webPlayerClient.newCall(request).await()
        return if (!response.isSuccessful) {
            throw RuntimeException(
                "Extended metadata request failed: ${response.code} ${response.message}"
            )
        } else response.body.bytes()
    }


    private fun extensions(persistedQuery: String): JsonObject {
        return buildJsonObject {
            putJsonObject("persistedQuery") {
                put("version", 1)
                put("sha256Hash", persistedQuery)
            }
        }
    }

    private suspend fun call(
        request: Request, ignore: Boolean = false, auth: String? = null,
    ) = run {
        val req = if (auth == null) request
        else request.newBuilder().addHeader("Authorization", "Bearer $auth").build()
        val res = client.newCall(req).await()
        if (ignore || res.isSuccessful) res
        else {
            res.closeQuietly()
            throw Exception("${res.code}: Failed to call - ${req.url}")
        }
    }

    suspend fun getWebAccessToken(): String {
        return webMutex.withLock { web.getToken() }
    }

    /**
     * Register this device with Spotify Connect State, linking it to the given
     * Dealer WebSocket connection. After this, the server will push player state
     * (including Widevine manifests) to our WebSocket connection.
     */
    /**
     * Register this device with Spotify Connect State as an observer device (`hobs_$deviceId`),
     * linking it to the Dealer WebSocket connection.
     */
    suspend fun connectStateRegister(connectionId: String, deviceId: String) {
        ensureTokens()
        val body = buildJsonObject {
            put("member_type", "CONNECT_STATE")
            putJsonObject("device") {
                putJsonObject("device_info") {
                    putJsonObject("capabilities") {
                        put("can_be_player", false)
                        put("hidden", true)
                        put("needs_full_player_state", true)
                    }
                }
            }
        }
        val request = Request.Builder()
            .url("https://spclient.wg.spotify.com/connect-state/v1/devices/hobs_$deviceId")
            .header("X-Spotify-Connection-Id", connectionId)
            .header("Accept", "application/json")
            .put(body.toString().toRequestBody("application/json".toMediaType()))
            .build()
        webPlayerClient.newCall(request).await().closeQuietly()

        // Also register player device via track-playback/v1/devices
        runCatching {
            val tpBody = buildJsonObject {
                putJsonObject("device") {
                    put("brand", "spotify")
                    putJsonObject("capabilities") {
                        put("change_volume", true)
                        put("enable_play_token", true)
                        put("supports_file_media_type", true)
                        put("disable_connect", false)
                        put("audio_podcasts", true)
                        put("video_playback", true)
                        putJsonArray("manifest_formats") {
                            add("file_ids_mp4")
                            add("file_ids_mp4_dual")
                            add("file_urls_mp3")
                            add("file_ids_mp3")
                        }
                    }
                    put("device_id", deviceId)
                    put("device_type", "computer")
                    put("model", "web_player")
                    put("name", "Web Player (Chrome)")
                    put("platform_name", "web_player")
                }
                put("connection_id", connectionId)
                put("client_version", "harmony:4.9.0-af0ef98814")
                put("volume", 65535)
            }
            val tpRequest = Request.Builder()
                .url("https://spclient.wg.spotify.com/track-playback/v1/devices")
                .header("Accept", "application/json")
                .post(tpBody.toString().toRequestBody("application/json".toMediaType()))
                .build()
            webPlayerClient.newCall(tpRequest).await().closeQuietly()
        }
    }

    /**
     * Send a "play" player command via Connect State.
     * The server will respond by pushing a replace_state message via Dealer WebSocket
     * that includes the track manifest (file_ids_mp4 with Widevine file IDs).
     */
    suspend fun connectStateLoad(trackUri: String, deviceId: String) {
        ensureTokens()
        val body = buildJsonObject {
            putJsonObject("command") {
                put("endpoint", "play")
                putJsonObject("context") {
                    put("uri", trackUri)
                    put("url", "context://$trackUri")
                    putJsonObject("metadata") {}
                }
                putJsonObject("play_origin") {
                    put("feature_identifier", "harmony")
                    put("feature_version", "4.9.0-af0ef98814")
                }
                putJsonObject("options") {
                    put("license", "on-demand")
                    putJsonObject("skip_to") {}
                    putJsonObject("player_options_override") {}
                }
            }
        }
        val request = Request.Builder()
            .url("https://spclient.wg.spotify.com/connect-state/v1/player/command/from/$deviceId/to/$deviceId")
            .header("Accept", "application/json")
            .post(body.toString().toRequestBody("application/json".toMediaType()))
            .build()
        webPlayerClient.newCall(request).await().closeQuietly()
    }

    var userId: String? = null
    fun setUser(id: String?) {
        userId = id
    }

    companion object {
        private const val DEVICE_ID_KEY = "5911f7cd0d3cb8e1fdf731f0c57303cd353c96d8"

        private fun generateDeviceId(): String {
            val bytes = ByteArray(16)
            SecureRandom().nextBytes(bytes)
            return bytes.joinToString("") { "%02x".format(it) }
        }

        fun urlEncode(data: String): String = URLEncoder.encode(data, "UTF-8")
    }
}
