package com.staryo.plugins

import io.ktor.server.application.*
import io.ktor.server.plugins.callid.*
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
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
        withContext(MDCContext(mapOf("correlation_id" to call.callId.orEmpty()))) { proceed() }
    }
}
