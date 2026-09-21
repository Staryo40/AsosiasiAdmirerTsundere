package com.staryo

import ch.qos.logback.classic.Level
import com.staryo.config.AuthConfig
import com.staryo.config.ClientCredentials
import com.staryo.enums.Scope
import com.staryo.plugins.*
import com.staryo.service.AuthService
import com.staryo.state.AppState
import com.staryo.state.DelayRange
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import java.time.Duration
import kotlin.test.*

class AccessLogTest {
    private val authConfig = AuthConfig(
        jwtSecret = "access-log-test-signing-key-access-log-test-key",
        reader = ClientCredentials("reader", "reader-secret"),
        admin = ClientCredentials("admin", "admin-secret"),
        issuer = "pvmbg",
        audience = "pvmbg-api",
        tokenTtl = Duration.ofMinutes(5),
        loginRateLimitPerMinute = 5,
    )
    private val state = AppState()
    private val auth = AuthService(authConfig, state)

    private fun ApplicationTestBuilder.app() {
        application {
            configureCorrelationId()
            configureSerialization()
            configureStatusPages()
            configureAccessLog()
            configureResponseDelay(state)
            configureAuthentication(authConfig)
            routing {
                get("/fast") { call.respondText("ok") }
                outageGuarded("/health", state) { get { call.respondText("OK") } }
                get("/boom") { call.respond(HttpStatusCode.InternalServerError) }
                outageGuarded("/guarded", state) { get { call.respondText("guarded ok") } }
                authorized(Scope.READ) { get("/protected") { call.respondText("protected ok") } }
                authorized(Scope.ADMIN) { get("/admin-only") { call.respondText("admin ok") } }
            }
        }
    }

    private suspend fun ApplicationTestBuilder.warmUp() {
        client.get("/fast")
    }

    @Test
    fun `one line per request with the fixed fields`() = testApplication {
        app()
        warmUp()
        JsonCapture("access").use { capture ->
            val response = client.get("/fast") { header(CORRELATION_ID_HEADER, "trace-1") }
            val line = capture.awaitLines(1).single()

            assertEquals("http_request", line.text("event"))
            assertEquals("GET", line.text("method"))
            assertEquals("/fast", line.text("path"))
            assertEquals("200", line.text("status"))
            assertEquals("INFO", line.text("level"))
            assertEquals("trace-1", line.text("correlation_id"))
            assertEquals("trace-1", response.headers[CORRELATION_ID_HEADER])
            assertTrue(line.number("duration_ms")!! >= 0)
            assertEquals(0L, line.number("delay_ms"))
            assertTrue(!line.text("client_ip").isNullOrBlank())
            assertTrue(line.text("message")!!.startsWith("GET /fast 200 "), line.text("message"))
        }
    }

    @Test
    fun `levels follow the status of the response`() = testApplication {
        app()
        warmUp()
        val reader = auth.login("reader", "reader-secret").access_token
        JsonCapture("access", Level.DEBUG).use { capture ->
            client.get("/fast")
            client.get("/no-such-route")
            client.get("/protected")
            client.get("/admin-only") { bearerAuth(reader) }
            client.get("/boom")
            state.outage = true
            client.get("/guarded")
            client.get("/health")
            val levels = capture.awaitLines(14).filter { it.text("event") == "http_request" }
                .associate { (it.text("path") + " " + it.text("status")) to it.text("level") }

            assertEquals("INFO", levels["/fast 200"])
            assertEquals("INFO", levels["/no-such-route 404"])
            assertEquals("WARN", levels["/protected 401"])
            assertEquals("WARN", levels["/admin-only 403"])
            assertEquals("ERROR", levels["/boom 500"])
            assertEquals("WARN", levels["/guarded 503"])
            assertEquals("WARN", levels["/health 503"])
        }
    }

    @Test
    fun `a healthy health check is only logged at debug`() = testApplication {
        app()
        warmUp()
        JsonCapture("access").use { capture ->
            client.get("/health")
            client.get("/fast")
            val lines = capture.awaitLines(1)
            delayReal(200)

            assertEquals(listOf("/fast"), capture.lines().map { it.text("path") })
            assertEquals(1, lines.size)
        }
        JsonCapture("access", Level.DEBUG).use { capture ->
            client.get("/health")
            val health = capture.awaitLines(2).filter { it.text("event") == "http_request" }.single()

            assertEquals("DEBUG", health.text("level"))
        }
    }

    private suspend fun delayReal(ms: Long) = withContext(Dispatchers.Default) { delay(ms) }

    @Test
    fun `at debug a line is written when the request arrives and again when it ends`() = testApplication {
        app()
        warmUp()
        JsonCapture("access", Level.DEBUG).use { capture ->
            client.get("/fast") { header(CORRELATION_ID_HEADER, "trace-2") }
            val lines = capture.awaitLines(2)

            assertEquals(listOf("request_received", "http_request"), lines.map { it.text("event") })
            assertEquals(listOf("trace-2", "trace-2"), lines.map { it.text("correlation_id") })
        }
    }

    @Test
    fun `at info only the end of the request is written`() = testApplication {
        app()
        warmUp()
        JsonCapture("access").use { capture ->
            client.get("/fast")
            capture.awaitLines(1)
            delayReal(200)

            assertEquals(listOf("http_request"), capture.lines().map { it.text("event") })
        }
    }

    @Test
    fun `a request waiting out its delay is visible at debug before it finishes`() = testApplication {
        app()
        warmUp()
        state.updateResponseDelay(DelayRange(5000, 5000))
        JsonCapture("access", Level.DEBUG).use { capture ->
            coroutineScope {
                val pending = async { client.get("/fast") }
                val seenWhileWaiting = capture.awaitLines(1)

                assertEquals("request_received", seenWhileWaiting.single().text("event"))
                state.updateResponseDelay(DelayRange.NONE)
                pending.await()
            }
        }
    }

    @Test
    fun `delay_ms is the time really waited and duration_ms includes it`() = testApplication {
        app()
        warmUp()
        state.updateResponseDelay(DelayRange(400, 400))
        JsonCapture("access").use { capture ->
            client.get("/fast")
            val line = capture.awaitLines(1).single()

            assertTrue(line.number("delay_ms")!! >= 350, "delay_ms was ${line.number("delay_ms")}")
            assertTrue(line.number("duration_ms")!! >= line.number("delay_ms")!!)
        }
    }

    @Test
    fun `a request released early logs the shorter time it really waited`() = testApplication {
        app()
        warmUp()
        state.updateResponseDelay(DelayRange(5000, 5000))
        JsonCapture("access").use { capture ->
            coroutineScope {
                val pending = async { client.get("/fast") }
                delayReal(300)
                state.updateResponseDelay(DelayRange.NONE)
                pending.await()
            }
            val line = capture.awaitLines(1).single()

            assertTrue(line.number("delay_ms")!! in 250..2500, "delay_ms was ${line.number("delay_ms")}")
        }
    }

    @Test
    fun `a rejected token is still logged with its slow response time`() = testApplication {
        app()
        warmUp()
        state.updateResponseDelay(DelayRange(300, 300))
        JsonCapture("access").use { capture ->
            client.get("/protected") { bearerAuth("garbage") }
            val line = capture.awaitLines(1).single()

            assertEquals("401", line.text("status"))
            assertTrue(line.number("delay_ms")!! >= 250)
        }
    }

    @Test
    fun `the client and scope are logged for a valid token and left out otherwise`() = testApplication {
        app()
        warmUp()
        val reader = auth.login("reader", "reader-secret").access_token
        val admin = auth.login("admin", "admin-secret").access_token
        JsonCapture("access").use { capture ->
            client.get("/protected") { bearerAuth(reader) }
            client.get("/admin-only") { bearerAuth(admin) }
            client.get("/protected")
            client.get("/protected") { bearerAuth("garbage") }
            val lines = capture.awaitLines(4)

            assertEquals("reader", lines[0].text("client_id"))
            assertEquals("read", lines[0].text("scope"))
            assertEquals("admin", lines[1].text("client_id"))
            assertEquals("admin", lines[1].text("scope"))
            assertNull(lines[2]["client_id"])
            assertNull(lines[3]["client_id"])
            assertNull(lines[3]["scope"])
        }
    }

    @Test
    fun `tokens and secrets never appear in the access lines`() = testApplication {
        app()
        warmUp()
        val reader = auth.login("reader", "reader-secret").access_token
        JsonCapture("access", Level.DEBUG).use { capture ->
            client.get("/protected") { bearerAuth(reader) }
            client.get("/protected?client_secret=reader-secret&token=$reader") { header("Authorization", "Bearer $reader") }
            capture.awaitLines(4)
            delayReal(200)

            val raw = capture.raw()
            assertFalse(reader in raw, "the token was logged")
            assertFalse("reader-secret" in raw, "a secret was logged")
            assertFalse("client_secret" in raw, "a query string was logged")
            assertFalse("Bearer" in raw)
        }
    }

    @Test
    fun `a hostile or very long path cannot break or forge a line`() = testApplication {
        app()
        warmUp()
        JsonCapture("access").use { capture ->
            client.get("/x%0A{\"level\":\"FAKE\"}%22")
            client.get("/" + "a".repeat(5000))
            val lines = capture.awaitLines(2)

            assertEquals(2, capture.raw().lines().count { it.isNotBlank() })
            assertTrue(lines.all { it.text("level") == "INFO" })
            assertTrue(lines.all { it.text("path")!!.length <= 256 })
        }
    }

    @Test
    fun `parallel requests each get their own line with their own id and path`() = testApplication {
        app()
        warmUp()
        JsonCapture("access").use { capture ->
            coroutineScope {
                (1..30).map { n -> async { client.get("/fast") { header(CORRELATION_ID_HEADER, "req-$n") } } }.awaitAll()
            }
            val lines = capture.awaitLines(30)

            assertEquals(30, lines.size)
            assertEquals((1..30).map { "req-$it" }.toSet(), lines.map { it.text("correlation_id") }.toSet())
        }
    }

    @Test
    fun `the full app logs a 429 as a warning and a normal read as info`() = testApplication {
        configure()
        JsonCapture("access").use { capture ->
            repeat(6) {
                client.post("/auth/login") {
                    contentType(ContentType.Application.Json)
                    setBody("""{"client_id":"test-reader","client_secret":"wrong"}""")
                }
            }
            val lines = capture.awaitLines(6)

            assertEquals(listOf("401", "401", "401", "401", "401", "429"), lines.map { it.text("status") })
            assertEquals("WARN", lines.last().text("level"))
            assertEquals("/auth/login", lines.last().text("path"))
            assertEquals("POST", lines.last().text("method"))
        }
    }
}
