package com.staryo.plugins

import com.staryo.logging.Events
import com.staryo.service.InvalidRequestException
import com.staryo.service.ServiceUnavailableException
import com.staryo.service.TooManyRequestsException
import com.staryo.service.UnauthorizedException
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.defaultExceptionStatusCode
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
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
        exception<TooManyRequestsException> { call, e ->
            call.response.header(HttpHeaders.RetryAfter, e.retryAfterSeconds.toString())
            call.respond(HttpStatusCode.TooManyRequests, ErrorResponse(e.message ?: "too many requests"))
        }
        exception<ServiceUnavailableException> { call, e ->
            call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse(e.message ?: "unavailable"))
        }
        // Last resort, for anything the handlers above do not name.
        exception<Throwable> { call, cause ->
            // Cancellation means the client went away or the server is stopping, so it must keep travelling up.
            if (cause is CancellationException && cause !is TimeoutCancellationException) throw cause

            // Keeps Ktor's own mapping (bad body 400, unsupported media type 415, timeout 504, ...);
            // whatever it has no mapping for is a real failure.
            val status = defaultExceptionStatusCode(cause) ?: HttpStatusCode.InternalServerError
            if (status.value >= 500) {
                call.withCorrelationId {
                    Events.error("unhandled_exception", cause, "status" to status.value, "exception" to cause::class.qualifiedName)
                }
            }
            // The body never carries the exception's own message, which can reveal internal class names and details.
            call.respond(status, ErrorResponse(status.description.lowercase()))
        }
    }
}
