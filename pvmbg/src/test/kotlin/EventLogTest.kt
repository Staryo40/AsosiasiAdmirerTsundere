package com.staryo

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.staryo.plugins.rejectionReason
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.testing.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.time.Instant
import kotlin.test.*

// Runs the whole app; the ids, secrets and signing key come from the test task in build.gradle.kts.
class EventLogTest {
    private val uuid = Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
    private val signingKey = "test-only-signing-key-test-only-signing-key"

    private suspend fun ApplicationTestBuilder.postJson(path: String, body: String, token: String? = null) =
        client.post(path) {
            contentType(ContentType.Application.Json)
            token?.let { bearerAuth(it) }
            setBody(body)
        }

    private suspend fun ApplicationTestBuilder.login(id: String, secret: String) =
        postJson("/auth/login", buildJsonObject { put("client_id", id); put("client_secret", secret) }.toString())

    private suspend fun ApplicationTestBuilder.tokenFor(id: String, secret: String): String =
        Json.parseToJsonElement(login(id, secret).bodyAsText()).jsonObject["access_token"]!!.jsonPrimitive.content

    private suspend fun ApplicationTestBuilder.adminToken() = tokenFor("test-admin", "test-admin-secret")
    private suspend fun ApplicationTestBuilder.readerToken() = tokenFor("test-reader", "test-reader-secret")

    // The app starts on the first request, and its startup line must not land in a test's capture window.
    private suspend fun ApplicationTestBuilder.boot() {
        configure()
        startApplication()
    }

    private fun List<JsonObject>.named(event: String) = filter { it.text("event") == event }

    private fun forge(expiresAt: Instant = Instant.now().plusSeconds(300), secret: String = signingKey): String =
        JWT.create()
            .withIssuer("pvmbg").withAudience("pvmbg-api").withSubject("test-reader")
            .withClaim("scope", "read").withExpiresAt(expiresAt)
            .sign(Algorithm.HMAC256(secret))

    @Test
    fun `a successful login logs the client scope and token id but never the token`() = testApplication {
        boot()
        JsonCapture("event").use { capture ->
            val token = tokenFor("test-reader", "test-reader-secret")
            val event = capture.awaitLines(1).single()

            assertEquals("login_succeeded", event.text("event"))
            assertEquals("INFO", event.text("level"))
            assertEquals("test-reader", event.text("client_id"))
            assertEquals("read", event.text("scope"))
            assertEquals("300", event.text("expires_in"))
            assertEquals(JWT.decode(token).id, event.text("token_id"))
            assertTrue(uuid.matches(event.text("token_id")!!))
            assertTrue(!event.text("correlation_id").isNullOrBlank())
            assertFalse(token in capture.raw(), "the token was logged")
        }
    }

    @Test
    fun `a failed login says whether the id was unknown or the secret was wrong`() = testApplication {
        boot()
        JsonCapture("event").use { capture ->
            login("nobody", "hunter2-not-logged")
            login("test-reader", "hunter2-not-logged")
            val events = capture.awaitLines(2)

            assertEquals(listOf("login_failed", "login_failed"), events.map { it.text("event") })
            assertEquals(listOf("WARN", "WARN"), events.map { it.text("level") })
            assertEquals(listOf("nobody", "test-reader"), events.map { it.text("client_id") })
            assertEquals(listOf("unknown_client", "wrong_secret"), events.map { it.text("reason") })
            assertEquals("login_failed client_id=nobody reason=unknown_client", events[0].text("message"))
            assertFalse("hunter2-not-logged" in capture.raw(), "the secret that was sent was logged")
        }
    }

    @Test
    fun `the attempted id is logged as sent but capped at 256 characters`() = testApplication {
        boot()
        JsonCapture("event").use { capture ->
            login("someone-typed-their-password", "x")
            login("a".repeat(300), "x")
            val events = capture.awaitLines(2)

            assertEquals("someone-typed-their-password", events[0].text("client_id"))
            assertEquals("a".repeat(256), events[1].text("client_id"))
        }
    }

    @Test
    fun `a hostile id cannot break or forge a log line`() = testApplication {
        boot()
        val hostile = "x\"y\nz {\"level\":\"FAKE\",\"event\":\"login_succeeded\"}"
        JsonCapture("event").use { capture ->
            login(hostile, "x")
            val events = capture.awaitLines(1)

            assertEquals(1, capture.raw().lines().count { it.isNotBlank() })
            assertEquals(hostile, events.single().text("client_id"))
            assertEquals("WARN", events.single().text("level"))
            assertEquals("login_failed", events.single().text("event"))
        }
    }

    @Test
    fun `during an outage a reader login is logged as rejected and the admin login as succeeded`() = testApplication {
        boot()
        val admin = adminToken()
        postJson("/admin/outage", """{"enabled":true}""", admin)
        JsonCapture("event").use { capture ->
            login("test-reader", "test-reader-secret")
            login("test-admin", "test-admin-secret")
            val events = capture.awaitLines(2)

            assertEquals("login_rejected", events[0].text("event"))
            assertEquals("test-reader", events[0].text("client_id"))
            assertEquals("outage", events[0].text("reason"))
            assertEquals("WARN", events[0].text("level"))
            assertEquals("login_succeeded", events[1].text("event"))
            assertEquals("admin", events[1].text("scope"))
        }
    }

    @Test
    fun `hitting the login limit is logged with the key that was limited`() = testApplication {
        boot()
        JsonCapture("event").use { capture ->
            repeat(6) { login("test-reader", "wrong") }
            val limited = capture.awaitLines(6).named("rate_limited").single()

            assertEquals("WARN", limited.text("level"))
            assertEquals("client:test-reader", limited.text("key"))
            assertTrue(limited.number("retry_after_seconds")!! >= 1)
        }
    }

    @Test
    fun `an invented id is limited by IP and the limited key never contains the invented id`() = testApplication {
        boot()
        JsonCapture("event").use { capture ->
            repeat(6) { login("fake-$it", "wrong") }
            val limited = capture.awaitLines(6).named("rate_limited").single()

            assertTrue(limited.text("key")!!.startsWith("ip:"), limited.text("key"))
            assertFalse("fake-" in limited.text("key")!!)
        }
    }

    @Test
    fun `a rejected token is logged with the reason`() = testApplication {
        boot()
        JsonCapture("event").use { capture ->
            client.get("/volcanic-reports")
            client.get("/volcanic-reports") { header(HttpHeaders.Authorization, "Basic dGVzdDp0ZXN0") }
            client.get("/volcanic-reports") { bearerAuth("not.a.jwt") }
            client.get("/volcanic-reports") { bearerAuth(forge(expiresAt = Instant.now().minusSeconds(60))) }
            client.get("/volcanic-reports") { bearerAuth(forge(secret = "some-other-signing-key-some-other-key")) }
            val events = capture.awaitLines(5)

            assertTrue(events.all { it.text("event") == "auth_rejected" && it.text("level") == "WARN" })
            assertEquals(
                listOf("missing_token", "wrong_scheme", "malformed_token", "expired_token", "invalid_token"),
                events.map { it.text("reason") },
            )
            assertTrue(events.all { it["client_id"] == null })
        }
    }

    @Test
    fun `the rejection reason is worked out from the header alone`() {
        assertEquals("missing_token", rejectionReason(null))
        assertEquals("missing_token", rejectionReason("  "))
        assertEquals("missing_token", rejectionReason("Bearer"))
        assertEquals("wrong_scheme", rejectionReason("Basic abc"))
        assertEquals("malformed_token", rejectionReason("Bearer not-a-jwt"))
        assertEquals("expired_token", rejectionReason("bearer " + forge(expiresAt = Instant.now().minusSeconds(5))))
        assertEquals("invalid_token", rejectionReason("Bearer " + forge(secret = "another-key-another-key-another-key-1")))
    }

    @Test
    fun `a reader using an admin route is logged as a denied scope and changes nothing`() = testApplication {
        boot()
        val reader = readerToken()
        JsonCapture("event").use { capture ->
            postJson("/admin/outage", """{"enabled":true}""", reader)
            val events = capture.awaitLines(1)

            assertEquals(listOf("scope_denied"), events.map { it.text("event") })
            assertEquals("WARN", events.single().text("level"))
            assertEquals("test-reader", events.single().text("client_id"))
            assertEquals("admin", events.single().text("required"))
            assertEquals("read", events.single().text("granted"))
            assertEquals(HttpStatusCode.OK, client.get("/health").status)
        }
    }

    @Test
    fun `admin changes are logged with what changed and who changed it`() = testApplication {
        boot()
        val admin = adminToken()
        JsonCapture("event").use { capture ->
            postJson("/admin/outage", """{"enabled":true}""", admin)
            postJson("/admin/outage", """{"enabled":false}""", admin)
            postJson("/admin/schema-version", """{"version":2}""", admin)
            postJson("/admin/response-delay", """{"min_ms":10,"max_ms":20}""", admin)
            val events = capture.awaitLines(4)

            assertEquals(listOf("outage_changed", "outage_changed", "schema_version_changed", "response_delay_changed"), events.map { it.text("event") })
            assertEquals(listOf("true", "false"), events.take(2).map { it.text("enabled") })
            assertEquals("2", events[2].text("version"))
            assertEquals("10", events[3].text("min_ms"))
            assertEquals("20", events[3].text("max_ms"))
            assertTrue(events.all { it.text("client_id") == "test-admin" && it.text("level") == "INFO" })
        }
    }

    @Test
    fun `a rejected admin request does not log a change`() = testApplication {
        boot()
        val admin = adminToken()
        JsonCapture("event").use { capture ->
            postJson("/admin/response-delay", """{"min_ms":3000,"max_ms":500}""", admin)
            postJson("/admin/schema-version", """{"version":9}""", admin)
            postJson("/admin/outage", "not json", admin)
            postJson("/admin/outage", """{"enabled":true}""")
            kotlinx.coroutines.delay(200)

            val names = capture.lines().map { it.text("event") }
            assertEquals(listOf("auth_rejected"), names)
        }
    }

    @Test
    fun `startup writes one summary with the settings and no secrets`() {
        JsonCapture("event").use { capture ->
            testApplication {
                configure()
                startApplication()
            }
            val startup = capture.lines().named("startup").single()

            assertEquals("INFO", startup.text("level"))
            assertEquals("8082", startup.text("port"))
            assertEquals("0", startup.text("delay_min_ms"))
            assertEquals("0", startup.text("delay_max_ms"))
            assertEquals("300", startup.text("token_ttl_seconds"))
            assertEquals("5", startup.text("login_rate_limit_per_minute"))
            assertEquals("INFO", startup.text("log_level"))
            assertEquals("json", startup.text("log_format"))
            assertTrue(startup.number("seed_records")!! > 0)
            assertTrue(startup.number("generator_interval_ms")!! > 0)

            val raw = capture.raw()
            for (secret in listOf(signingKey, "test-reader-secret", "test-admin-secret", "test-reader", "test-admin")) {
                assertFalse(secret in raw, "'$secret' appeared in the startup line")
            }
        }
    }

    @Test
    fun `no event line ever contains a token a secret or the signing key`() = testApplication {
        boot()
        JsonCapture("event").use { capture ->
            val reader = readerToken()
            val admin = adminToken()
            login("test-reader", "hunter2-not-logged")
            client.get("/volcanic-reports") { bearerAuth(forge(secret = "another-key-another-key-another-key-1")) }
            postJson("/admin/outage", """{"enabled":false}""", reader)
            postJson("/admin/outage", """{"enabled":false}""", admin)
            capture.awaitLines(5)

            val raw = capture.raw()
            for (secret in listOf(reader, admin, signingKey, "test-reader-secret", "test-admin-secret", "hunter2-not-logged", "Bearer")) {
                assertFalse(secret in raw, "'${secret.take(12)}...' appeared in the event lines")
            }
        }
    }
}
