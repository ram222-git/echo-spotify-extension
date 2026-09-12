package dev.brahmkshatriya.echo.extension

import android.annotation.SuppressLint
import android.app.Application
import java.io.File

@Suppress("unused")
class ADSpotifyExtension : SpotifyExtension() {

    @SuppressLint("PrivateApi")
    private fun getApplication(): Application {
        return Class.forName("android.app.ActivityThread").getMethod("currentApplication")
            .invoke(null) as Application
    }

    override val filesDir by lazy { File(getApplication().filesDir, "spotify") }

    // Use Widevine (browser-like) streaming — no PlayPlay/unplayplay needed
    override val showWidevineStreams = true
    override val supportsPlayPlay = false
}