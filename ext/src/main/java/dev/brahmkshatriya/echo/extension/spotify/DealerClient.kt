package dev.brahmkshatriya.echo.extension.spotify

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Spotify Dealer WebSocket client.
 *
 * Connects to wss://gae2-dealer.g2.spotify.com and registers as a Connect-State device.
 * Used to receive track-playback commands including Widevine MP4 file manifests —
 * Spotify only pushes these via WebSocket, never via REST.
 *
 * Flow:
 *   1. WebSocket → receive Spotify-Connection-Id
 *   2. PUT /connect-state/v1/devices/hobs/{deviceId} (register)
 *   3. PUT /connect-state/v1/player/command (load track)
 *   4. WebSocket push: replace_state → cells[].manifest.file_ids_mp4[format=10].file_id
 */
class DealerClient(private val api: SpotifyApi) {

    private val parser = Json { ignoreUnknownKeys = true }
    private val wsClient = OkHttpClient.Builder()
        .pingInterval(20, java.util.concurrent.TimeUnit.SECONDS)
        .build()

    // --- Connection state ---
    private val connectMutex = Mutex()
    private val socketRef = AtomicReference<WebSocket?>(null)
    private val connectionIdRef = AtomicReference<String?>(null)
    private val connIdDeferred = AtomicReference<CompletableDeferred<String>?>()
    private val registered = AtomicBoolean(false)

    // --- Manifest resolution ---
    // trackUri → deferred MP4_128 fileId
    private val pending = ConcurrentHashMap<String, CompletableDeferred<String>>()
    // trackUri → MP4_128 fileId (persistent per-session cache)
    private val cache = ConcurrentHashMap<String, String>()

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the Widevine MP4_128 file_id for the given track URI.
     *
     * Connects to the Dealer WebSocket if needed, registers as a Connect-State device,
     * sends a load command, and awaits the replace_state push (up to 15 s).
     */
    suspend fun getFileIdForTrack(trackUri: String): String {
        cache[trackUri]?.let {
            SpotifyLog.d("DealerClient: Cache hit for $trackUri -> $it")
            return it
        }

        SpotifyLog.d("DealerClient: Fetching file ID for $trackUri (ensuring WebSocket connected)...")
        val connId = ensureConnected()
        val deviceId = api.deviceId()

        if (registered.compareAndSet(false, true)) {
            try {
                SpotifyLog.d("DealerClient: Registering Connect-State observer (connId=$connId, deviceId=$deviceId)...")
                api.connectStateRegister(connId, deviceId)
                SpotifyLog.d("DealerClient: Connect-State registered successfully!")
            } catch (e: Exception) {
                registered.set(false)
                SpotifyLog.e("DealerClient: Failed to register Connect-State observer", e)
                throw e
            }
        }

        val fresh = CompletableDeferred<String>()
        val deferred = pending.putIfAbsent(trackUri, fresh) ?: run {
            try {
                SpotifyLog.d("DealerClient: Sending connectStateLoad for $trackUri...")
                api.connectStateLoad(trackUri, deviceId)
                SpotifyLog.d("DealerClient: connectStateLoad sent, awaiting WebSocket replace_state...")
            } catch (e: Exception) {
                pending.remove(trackUri, fresh)
                fresh.completeExceptionally(e)
                SpotifyLog.e("DealerClient: Failed to send connectStateLoad", e)
                throw e
            }
            fresh
        }

        return try {
            val fileId = withTimeout(15_000L) { deferred.await() }
            SpotifyLog.d("DealerClient: Successfully obtained fileId=$fileId for $trackUri")
            fileId
        } catch (t: TimeoutCancellationException) {
            SpotifyLog.e("DealerClient: Timed out waiting for Widevine manifest for $trackUri", t)
            throw Exception("Timed out waiting for Widevine manifest for $trackUri")
        } finally {
            pending.remove(trackUri, deferred)
        }
    }

    // -------------------------------------------------------------------------
    // WebSocket management
    // -------------------------------------------------------------------------

    private suspend fun ensureConnected(): String {
        connectionIdRef.get()?.let { return it }
        return connectMutex.withLock {
            connectionIdRef.get() ?: run {
                val deferred = CompletableDeferred<String>()
                connIdDeferred.set(deferred)
                openSocket()
                val id = deferred.await()
                connectionIdRef.set(id)
                id
            }
        }
    }

    private suspend fun openSocket() {
        val token = api.getWebAccessToken()
        val request = Request.Builder()
            .url("wss://gae2-dealer.g2.spotify.com/?access_token=$token")
            .header("Origin", "https://open.spotify.com")
            .build()
        wsClient.newWebSocket(request, Listener())
    }

    private fun reset() {
        socketRef.set(null)
        connectionIdRef.set(null)
        registered.set(false)
    }

    // -------------------------------------------------------------------------
    // WebSocket listener
    // -------------------------------------------------------------------------

    private inner class Listener : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            SpotifyLog.d("DealerClient: WebSocket opened! HTTP status: ${response.code}")
            socketRef.set(webSocket)
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            socketRef.set(webSocket)
            handleMessage(text)
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            SpotifyLog.e("DealerClient: WebSocket failure: ${t.message}, response code: ${response?.code}", t)
            reset()
            connIdDeferred.getAndSet(null)?.completeExceptionally(t)
            val snapshot = pending.values.toList()
            pending.clear()
            snapshot.forEach { it.completeExceptionally(t) }
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            SpotifyLog.d("DealerClient: WebSocket closed: code=$code, reason=$reason")
            reset()
        }
    }

    // -------------------------------------------------------------------------
    // Message parsing
    // -------------------------------------------------------------------------

    private fun handleMessage(text: String) {
        runCatching {
            val msg = parser.parseToJsonElement(text).jsonObject
            val type = msg["type"]?.jsonPrimitive?.content
            val uri  = msg["uri"]?.jsonPrimitive?.content

            when {
                type == "ping" -> {
                    SpotifyLog.d("DealerClient: Received ping, sending pong")
                    socketRef.get()?.send("""{"type":"pong"}""")
                }

                uri?.startsWith("hm://pusher/v1/connections/") == true -> {
                    SpotifyLog.d("DealerClient: Received connection URI: $uri")
                    handleConnectionId(msg)
                }

                uri == "hm://track-playback/v1/command" -> {
                    SpotifyLog.d("DealerClient: Received track-playback command")
                    handleTrackPlaybackCommand(msg)
                }

                else -> {
                    SpotifyLog.d("DealerClient: Received other message (type=$type, uri=$uri): ${text.take(200)}")
                }
            }
        }.onFailure { SpotifyLog.e("DealerClient: Error handling message", it) }
    }

    private fun handleConnectionId(msg: JsonObject) {
        val connId = msg["headers"]?.jsonObject
            ?.get("Spotify-Connection-Id")?.jsonPrimitive?.content ?: return
        connIdDeferred.getAndSet(null)?.complete(connId)
        connectionIdRef.set(connId)
    }

    private fun handleTrackPlaybackCommand(msg: JsonObject) {
        val payloads = msg["payloads"]?.jsonArray ?: return
        for (payload in payloads) {
            val obj = payload.jsonObject
            if (obj["type"]?.jsonPrimitive?.content != "replace_state") continue
            parseReplaceState(obj)
        }
    }

    private fun parseReplaceState(payload: JsonObject) {
        SpotifyLog.d("DealerClient replace_state: $payload")
        val sm     = payload["state_machine"]?.jsonObject ?: return
        val tracks = sm["tracks"]?.jsonArray ?: return

        for (trackElement in tracks) {
            val trackObj = trackElement.jsonObject
            val metadata = trackObj["metadata"]?.jsonObject ?: continue
            val uri = metadata["uri"]?.jsonPrimitive?.content
            val linkedFromUri = metadata["linked_from_uri"]?.jsonPrimitive?.content
            val contextUri = metadata["context_uri"]?.jsonPrimitive?.content
            val manifest = trackObj["manifest"]?.jsonObject ?: continue

            val mp4Array = manifest["file_ids_mp4"]?.jsonArray
                ?: manifest["file_ids_mp4_dual"]?.jsonArray
            if (mp4Array == null || mp4Array.isEmpty()) continue

            val fileId = mp4Array
                .firstOrNull { it.jsonObject["format"]?.jsonPrimitive?.content == "10" }
                ?.jsonObject?.get("file_id")?.jsonPrimitive?.content
                ?: mp4Array.firstOrNull()?.jsonObject?.get("file_id")?.jsonPrimitive?.content

            if (fileId != null) {
                if (uri != null) {
                    cache[uri] = fileId
                    pending.remove(uri)?.complete(fileId)
                }
                if (linkedFromUri != null) {
                    cache[linkedFromUri] = fileId
                    pending.remove(linkedFromUri)?.complete(fileId)
                }
                if (contextUri != null && contextUri.startsWith("spotify:track:")) {
                    cache[contextUri] = fileId
                    pending.remove(contextUri)?.complete(fileId)
                }
            }
        }
    }
}
