package com.staryo

import com.staryo.plugins.*
import com.staryo.service.InvalidRequestException
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.plugins.NotFoundException
import io.ktor.server.plugins.UnsupportedMediaTypeException
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlin.test.*

class UnhandledExceptionTest {
    private val leakedDetail = "secret-detail-xyz"

    // A small app with the plugins that matter here and one route per kind of failure.
    private fun ApplicationTestBuilder.failingApp() {
        application {
            configureCorrelationId()
            configureSerialization()
            configureStatusPages()
            configureAccessLog()
            routing {
                get("/explode") { throw IllegalStateException(leakedDetail) }
                get("/missing") { throw NotFoundException("no such thing") }
                get("/bad") { throw BadRequestException("bad thing $leakedDetail") }
                get("/media") { throw UnsupportedMediaTypeException(ContentType.Text.Plain) }
                get("/timeout") { withTimeout(20) { delay(5000) } }
                get("/invalid") { throw InvalidRequestException("that value is not allowed") }
            }
        }
    }

    // The app starts on the first request; starting it first keeps its startup line out of a capture window.
    private suspend fun ApplicationTestBuilder.boot() {
        configure()
        startApplication()
    }

    private suspend fun ApplicationTestBuilder.postJson(path: String, body: String, correlationId: String? = null) =
        client.post(path) {
            contentType(ContentType.Application.Json)
            correlationId?.let { header(CORRELATION_ID_HEADER, it) }
            setBody(body)
        }

    @Test
    fun `an unexpected exception becomes a clean 500 that leaks nothing`() = testApplication {
        failingApp()
        val response = client.get("/explode") { header(CORRELATION_ID_HEADER, "boom-1") }

        assertEquals(HttpStatusCode.InternalServerError, response.status)
        assertEquals(ContentType.Application.Json, response.contentType()?.withoutParameters())
        assertEquals("""{"error":"internal server error"}""", response.bodyAsText())
        assertFalse(leakedDetail in response.bodyAsText())
        assertEquals("boom-1", response.headers[CORRELATION_ID_HEADER])
    }

    @Test
    fun `an unexpected exception is logged once as an error with its stack trace and the correlation id`() = testApplication {
        failingApp()
        client.get("/")
        JsonCapture("event").use { capture ->
            client.get("/explode") { header(CORRELATION_ID_HEADER, "boom-2") }
            val event = capture.awaitLines(1).single()

            assertEquals("unhandled_exception", event.text("event"))
            assertEquals("ERROR", event.text("level"))
            assertEquals("500", event.text("status"))
            assertEquals("java.lang.IllegalStateException", event.text("exception"))
            assertEquals("boom-2", event.text("correlation_id"))
            assertTrue(leakedDetail in event.text("stack_trace")!!, "the stack trace is for the log, not the client")
            assertEquals(1, capture.raw().lines().count { it.isNotBlank() }, "an error must stay on one line")
        }
    }

    @Test
    fun `the access line of a failed request has the error level and the correlation id`() = testApplication {
        failingApp()
        client.get("/")
        JsonCapture("access").use { capture ->
            client.get("/explode") { header(CORRELATION_ID_HEADER, "boom-3") }
            val line = capture.awaitLines(1).single()

            assertEquals("500", line.text("status"))
            assertEquals("ERROR", line.text("level"))
            assertEquals("boom-3", line.text("correlation_id"))
        }
    }

    @Test
    fun `exceptions Ktor already maps keep their status and are not logged as failures`() = testApplication {
        failingApp()
        client.get("/")
        JsonCapture("event").use { capture ->
            val missing = client.get("/missing")
            val bad = client.get("/bad")
            val media = client.get("/media")

            assertEquals(HttpStatusCode.NotFound, missing.status)
            assertEquals(HttpStatusCode.BadRequest, bad.status)
            assertEquals(HttpStatusCode.UnsupportedMediaType, media.status)
            assertEquals("""{"error":"not found"}""", missing.bodyAsText())
            assertEquals("""{"error":"bad request"}""", bad.bodyAsText())
            assertEquals("""{"error":"unsupported media type"}""", media.bodyAsText())
            assertFalse(leakedDetail in bad.bodyAsText())
            kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { delay(200) }
            assertEquals("", capture.raw())
        }
    }

    @Test
    fun `a timeout is a 504 and is logged as a failure`() = testApplication {
        failingApp()
        client.get("/")
        JsonCapture("event").use { capture ->
            val response = client.get("/timeout")

            assertEquals(HttpStatusCode.GatewayTimeout, response.status)
            val event = capture.awaitLines(1).single()
            assertEquals("unhandled_exception", event.text("event"))
            assertEquals("504", event.text("status"))
        }
    }

    @Test
    fun `handlers that name their exception still win over the catch-all`() = testApplication {
        failingApp()
        val response = client.get("/invalid")

        assertEquals(HttpStatusCode.BadRequest, response.status)
        assertEquals("""{"error":"that value is not allowed"}""", response.bodyAsText())
    }

    @Test
    fun `wrong method and unknown path are unchanged`() = testApplication {
        boot()

        assertEquals(HttpStatusCode.MethodNotAllowed, client.get("/auth/login").status)
        assertEquals(HttpStatusCode.NotFound, client.get("/nope").status)
    }

    @Test
    fun `a malformed login body is a 400 that no longer names an internal class`() = testApplication {
        boot()
        val notJson = postJson("/auth/login", "not json", "cid-400")
        val missing = postJson("/auth/login", "{}", "cid-400b")

        assertEquals(HttpStatusCode.BadRequest, notJson.status)
        assertEquals(HttpStatusCode.BadRequest, missing.status)
        assertEquals("""{"error":"bad request"}""", notJson.bodyAsText())
        assertFalse("com.staryo" in missing.bodyAsText())
        assertEquals("cid-400", notJson.headers[CORRELATION_ID_HEADER])
    }

    @Test
    fun `a wrong content type is a 415 json error`() = testApplication {
        boot()
        val response = client.post("/auth/login") {
            contentType(ContentType.Text.Plain)
            setBody("x")
        }

        assertEquals(HttpStatusCode.UnsupportedMediaType, response.status)
        assertEquals("""{"error":"unsupported media type"}""", response.bodyAsText())
    }

    @Test
    fun `responses produced by the error handlers keep the correlation id in their access lines`() = testApplication {
        boot()
        JsonCapture("access").use { capture ->
            postJson("/auth/login", """{"client_id":"test-reader","client_secret":"wrong"}""", "cid-401")
            postJson("/auth/login", "not json", "cid-400")
            repeat(4) { postJson("/auth/login", """{"client_id":"test-reader","client_secret":"wrong"}""", "cid-fill-$it") }
            postJson("/auth/login", """{"client_id":"test-reader","client_secret":"wrong"}""", "cid-429")
            val lines = capture.awaitLines(7)

            assertEquals(
                listOf("401", "400", "401", "401", "401", "401", "429"),
                lines.map { it.text("status") },
            )
            assertEquals(
                listOf("cid-401", "cid-400", "cid-fill-0", "cid-fill-1", "cid-fill-2", "cid-fill-3", "cid-429"),
                lines.map { it.text("correlation_id") },
            )
        }
    }
}
