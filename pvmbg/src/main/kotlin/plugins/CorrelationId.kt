package com.staryo.plugins

import io.ktor.server.application.*
import io.ktor.server.plugins.callid.*
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.MDC
import java.util.UUID

const val CORRELATION_ID_HEADER = "X-Correlation-ID"

// An id from a caller is only trusted if it is short and plain. Anything else is replaced, never logged,
// so a hostile header cannot flood the logs or smuggle odd characters into them.
private val VALID_CORRELATION_ID = Regex("[A-Za-z0-9._-]{1,64}")

fun Application.configureCorrelationId() {
    install(CallId) {
        retrieve { call -> call.request.headers[CORRELATION_ID_HEADER]?.takeIf { VALID_CORRELATION_ID.matches(it) } }
        generate { UUID.randomUUID().toString() }
        // Replaces the plugin's own stricter default check, which would reject the dots this rule allows.
        verify { VALID_CORRELATION_ID.matches(it) }
        replyToHeader(CORRELATION_ID_HEADER)
    }

    // Puts the id in the logging context for the whole request; MDCContext keeps it right across thread switches.
    intercept(ApplicationCallPipeline.Plugins) {
        withContext(MDCContext(mapOf(CORRELATION_ID_MDC_KEY to call.callId.orEmpty()))) { proceed() }
    }
}

internal const val CORRELATION_ID_MDC_KEY = "correlation_id"

// The error handlers run outside the context above (Ktor attaches them before the request's own phases), so
// code there that logs must put the id back for the moment it writes. The previous value is restored afterwards.
internal inline fun <T> ApplicationCall.withCorrelationId(block: () -> T): T {
    val previous = MDC.get(CORRELATION_ID_MDC_KEY)
    MDC.put(CORRELATION_ID_MDC_KEY, callId.orEmpty())
    try {
        return block()
    } finally {
        if (previous == null) MDC.remove(CORRELATION_ID_MDC_KEY) else MDC.put(CORRELATION_ID_MDC_KEY, previous)
    }
}
