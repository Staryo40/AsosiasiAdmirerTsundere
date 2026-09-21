package com.staryo.plugins

import com.staryo.state.AppState
import com.staryo.state.DelayRange
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.random.Random
import kotlin.random.nextLong

class ResponseDelayConfig {
    lateinit var state: AppState
    var random: Random = Random.Default
}

val ResponseDelay = createApplicationPlugin("ResponseDelay", ::ResponseDelayConfig) {
    val state = pluginConfig.state
    val random = pluginConfig.random

    onCall { call ->
        // A dead server responds immediately
        if (state.outage || isAdminPath(call.request.path())) return@onCall

        val seen = state.changes.value
        val delayMs = pickDelayMs(state.responseDelay, random)
        if (delayMs == 0L) return@onCall

        // Waits for the timer or for a change signal; the coroutine `delay` keeps no thread blocked while waiting.
        val released = withTimeoutOrNull(delayMs) { state.changes.first { it != seen } } != null
        if (released && state.outage) {
            call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse("service is in outage"))
        }
    }
}

fun Application.configureResponseDelay(state: AppState) {
    install(ResponseDelay) { this.state = state }
}

internal fun pickDelayMs(range: DelayRange, random: Random): Long = random.nextLong(range.minMs..range.maxMs)

internal fun isAdminPath(path: String): Boolean =
    path.split("/").firstOrNull { it.isNotEmpty() } == "admin"
