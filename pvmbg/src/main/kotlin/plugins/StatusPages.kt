package com.staryo.plugins

import com.staryo.service.InvalidRequestException
import com.staryo.service.ServiceUnavailableException
import com.staryo.service.UnauthorizedException
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import kotlinx.serialization.Serializable

@Serializable
data class ErrorResponse(val error: String)

fun Application.configureSerialization() {
    install(ContentNegotiation) { json() }
}

fun Application.configureStatusPages() {
    install(StatusPages) {
        exception<InvalidRequestException> { call, e ->
            call.respond(HttpStatusCode.BadRequest, ErrorResponse(e.message ?: "bad request"))
        }
        exception<UnauthorizedException> { call, e ->
            call.respond(HttpStatusCode.Unauthorized, ErrorResponse(e.message ?: "unauthorized"))
        }
        exception<ServiceUnavailableException> { call, e ->
            call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse(e.message ?: "unavailable"))
        }
    }
}
