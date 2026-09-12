package dev.brahmkshatriya.echo.extension.spotify

/**
 * Universal logger for Spotify Extension.
 * Logs to both Android Logcat (tag "SpotifyEcho") and stdout.
 */
object SpotifyLog {
    private const val TAG = "SpotifyEcho"

    private val androidLogClass = runCatching { Class.forName("android.util.Log") }.getOrNull()
    private val logDMethod = runCatching {
        androidLogClass?.getMethod("d", String::class.java, String::class.java)
    }.getOrNull()
    private val logEMethod = runCatching {
        androidLogClass?.getMethod("e", String::class.java, String::class.java, Throwable::class.java)
    }.getOrNull()

    fun d(message: String) {
        println("[$TAG] $message")
        try {
            logDMethod?.invoke(null, TAG, message)
        } catch (_: Throwable) {}
    }

    fun e(message: String, throwable: Throwable? = null) {
        println("[$TAG] ERROR: $message ${throwable?.message ?: ""}")
        throwable?.printStackTrace()
        try {
            logEMethod?.invoke(null, TAG, message, throwable)
        } catch (_: Throwable) {}
    }
}
