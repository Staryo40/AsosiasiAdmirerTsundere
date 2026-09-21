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

// Simulates PVMBG's slow infrastructure. Runs before authentication, so even a rejected token is answered slowly.
val ResponseDelay = createApplicationPlugin("ResponseDelay", ::ResponseDelayConfig) {
    val state = pluginConfig.state
    val random = pluginConfig.random

    onCall { call ->
        // A dead server does not answer slowly, it does not answer at all: the outage guards reply immediately.
        if (state.outage || isAdminPath(call.request.path())) return@onCall

        // Read the counter before the range: a change after this point releases the wait, one before it is already in the range.
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
