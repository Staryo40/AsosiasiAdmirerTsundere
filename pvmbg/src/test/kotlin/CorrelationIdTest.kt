package com.staryo

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.staryo.plugins.CORRELATION_ID_HEADER
import com.staryo.plugins.configureCorrelationId
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import kotlin.random.Random
import kotlin.test.*

class CorrelationIdTest {
    private val uuid = Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
    private val logger = LoggerFactory.getLogger("correlation.test") as Logger

    // A small app with just the plugin: one route that logs, and one that waits, logs and reports the id it sees.
    private fun ApplicationTestBuilder.probeApp() {
        application {
            configureCorrelationId()
            routing {
                get("/probe") {
                    logger.info("handling probe")
                    call.respondText("ok")
                }
                get("/slow/{n}") {
                    delay(Random.nextLong(5, 40))
                    logger.info("slow {}", call.parameters["n"])
                    delay(Random.nextLong(5, 40))
                    call.respondText(MDC.get("correlation_id") ?: "missing")
                }
            }
        }
    }

    // Freezes the logging context when the line is written, as the async appender does in production.
    private class CapturingAppender : ListAppender<ILoggingEvent>() {
        override fun append(event: ILoggingEvent) {
            event.prepareForDeferredProcessing()
            super.append(event)
        }
    }

    private inline fun captured(block: (ListAppender<ILoggingEvent>) -> Unit) {
        val appender = CapturingAppender().apply { start() }
        logger.isAdditive = false
        logger.addAppender(appender)
        try {
            block(appender)
        } finally {
            logger.detachAppender(appender)
        }
    }

    @Test
    fun `a valid incoming id is kept and echoed back`() = testApplication {
        probeApp()
        val response = client.get("/probe") { header(CORRELATION_ID_HEADER, "abc-123_X.9") }

        assertEquals("abc-123_X.9", response.headers[CORRELATION_ID_HEADER])
    }

    @Test
    fun `an id is generated when none is sent and every request gets a different one`() = testApplication {
        probeApp()
        val first = client.get("/probe").headers[CORRELATION_ID_HEADER]
        val second = client.get("/probe").headers[CORRELATION_ID_HEADER]

        assertTrue(first != null && uuid.matches(first), "got $first")
        assertTrue(second != null && uuid.matches(second), "got $second")
        assertNotEquals(first, second)
    }

    @Test
    fun `ids of 1 and 64 characters are accepted`() = testApplication {
        probeApp()
        val longest = "a".repeat(64)

        assertEquals("x", client.get("/probe") { header(CORRELATION_ID_HEADER, "x") }.headers[CORRELATION_ID_HEADER])
        assertEquals(longest, client.get("/probe") { header(CORRELATION_ID_HEADER, longest) }.headers[CORRELATION_ID_HEADER])
    }

    @Test
    fun `too long or odd ids are replaced with a generated one`() = testApplication {
        probeApp()
        val bad = listOf("a".repeat(65), "has space", "slash/id", "semi;colon", "quote\"id", "{\"level\":\"FAKE\"}", "")

        for (value in bad) {
            val echoed = client.get("/probe") { header(CORRELATION_ID_HEADER, value) }.headers[CORRELATION_ID_HEADER]
            assertTrue(echoed != null && uuid.matches(echoed), "'$value' was not replaced, got $echoed")
        }
    }

    @Test
    fun `a rejected id never reaches the logs`() = testApplication {
        probeApp()
        captured { appender ->
            client.get("/probe") { header(CORRELATION_ID_HEADER, "evil id with spaces") }

            val ids = appender.list.map { it.mdcPropertyMap["correlation_id"] }
            assertTrue(ids.none { it?.contains("evil") == true }, ids.toString())
        }
    }

    @Test
    fun `the id is in the logging context of lines written during the request`() = testApplication {
        probeApp()
        captured { appender ->
            val echoed = client.get("/probe") { header(CORRELATION_ID_HEADER, "trace-me-42") }.headers[CORRELATION_ID_HEADER]

            assertEquals("trace-me-42", echoed)
            assertEquals(listOf("trace-me-42"), appender.list.map { it.mdcPropertyMap["correlation_id"] })
        }
    }

    @Test
    fun `parallel requests never mix their ids across thread switches`() = testApplication {
        probeApp()
        captured { appender ->
            val results = coroutineScope {
                (1..60).map { n ->
                    async {
                        val response = client.get("/slow/$n") { header(CORRELATION_ID_HEADER, "req-$n") }
                        Triple(n, response.headers[CORRELATION_ID_HEADER], response.bodyAsText())
                    }
                }.awaitAll()
            }

            for ((n, echoed, seenInsideHandler) in results) {
                assertEquals("req-$n", echoed)
                assertEquals("req-$n", seenInsideHandler)
            }
            val logged = appender.list.associate { it.formattedMessage to it.mdcPropertyMap["correlation_id"] }
            for (n in 1..60) assertEquals("req-$n", logged["slow $n"], "line for request $n had the wrong id")
        }
    }

    @Test
    fun `the id is returned on every kind of response of the full app`() = testApplication {
        configure()

        val unauthorized = client.get("/volcanic-reports") { header(CORRELATION_ID_HEADER, "keep-401") }
        val notFound = client.get("/no-such-route") { header(CORRELATION_ID_HEADER, "keep-404") }
        val badLogin = client.post("/auth/login") {
            header(CORRELATION_ID_HEADER, "keep-400")
            contentType(ContentType.Application.Json)
            setBody("not json")
        }
        val ok = client.get("/health") { header(CORRELATION_ID_HEADER, "keep-200") }

        assertEquals(HttpStatusCode.Unauthorized, unauthorized.status)
        assertEquals("keep-401", unauthorized.headers[CORRELATION_ID_HEADER])
        assertEquals("keep-404", notFound.headers[CORRELATION_ID_HEADER])
        assertEquals("keep-400", badLogin.headers[CORRELATION_ID_HEADER])
        assertEquals("keep-200", ok.headers[CORRELATION_ID_HEADER])
    }
}
