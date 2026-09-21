package com.staryo.plugins

import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.plugins.*
import io.ktor.server.request.*
import io.ktor.util.AttributeKey
import net.logstash.logback.argument.StructuredArguments.entries
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

private val STARTED_AT = AttributeKey<Long>("accessLogStartedAt")
private const val MAX_LOGGED_PATH = 256

// Writes one line per request when its response has been sent, plus a DEBUG line when the request arrives
// (so a request stuck in its delay is visible while it waits). Never logs headers, bodies or query strings.
val AccessLog = createApplicationPlugin("AccessLog") {
    val log = LoggerFactory.getLogger("access")

    onCall { call ->
        call.attributes.put(STARTED_AT, System.nanoTime())
        if (log.isDebugEnabled) {
            val method = call.request.httpMethod.value
            val path = call.request.path().take(MAX_LOGGED_PATH)
            log.atDebug()
                .setMessage("{}")
                .addArgument("request received: $method $path")
                .addArgument(entries(mapOf("event" to "request_received", "method" to method, "path" to path, "client_ip" to call.request.origin.remoteAddress)))
                .log()
        }
    }

    on(ResponseSent) { call ->
        val method = call.request.httpMethod.value
        val path = call.request.path().take(MAX_LOGGED_PATH)
        val status = call.response.status()?.value ?: 0
        val durationMs = (System.nanoTime() - (call.attributes.getOrNull(STARTED_AT) ?: System.nanoTime())) / 1_000_000
        val delayMs = call.attributes.getOrNull(DELAY_MS) ?: 0L

        val fields = linkedMapOf<String, Any>(
            "event" to "http_request",
            "method" to method,
            "path" to path,
            "status" to status,
            "duration_ms" to durationMs,
            "delay_ms" to delayMs,
            "client_ip" to call.request.origin.remoteAddress,
        )
        call.principal<JWTPrincipal>()?.let { principal ->
            fields["client_id"] = principal.payload.subject.orEmpty()
            principal.scope()?.let { fields["scope"] = it.claim }
        }

        log.atLevel(accessLevel(path, status))
            .setMessage("{}")
            .addArgument("$method $path $status ${durationMs}ms")
            .addArgument(entries(fields))
            .log()
    }
}

fun Application.configureAccessLog() {
    install(AccessLog)
}

// A healthy health check is polled all the time, so it is DEBUG. A 503 is the deliberate simulated outage,
// which is a warning; only other 5xx responses are real failures.
internal fun accessLevel(path: String, status: Int): Level = when {
    path == "/health" && status < 400 -> Level.DEBUG
    status == 503 -> Level.WARN
    status >= 500 -> Level.ERROR
    status == 401 || status == 403 || status == 429 -> Level.WARN
    else -> Level.INFO
}
