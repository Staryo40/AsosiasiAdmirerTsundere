package com.staryo.plugins

import com.staryo.state.AppState
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

class OutageGuardConfig {
    lateinit var state: AppState
}

val OutageGuard = createRouteScopedPlugin("OutageGuard", ::OutageGuardConfig) {
    val state = pluginConfig.state
    onCall { call ->
        if (state.outage) {
            call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse("service is in outage"))
        }
    }
}

fun Route.outageGuarded(path: String, state: AppState, build: Route.() -> Unit): Route =
    route(path) {
        install(OutageGuard) { this.state = state }
        build()
    }
