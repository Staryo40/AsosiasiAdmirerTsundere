package com.staryo

import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.testing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.*

// These run the whole app from application.yaml; the client ids and secrets come from the test task in build.gradle.kts.
class ServerTest {

    private suspend fun ApplicationTestBuilder.postJson(path: String, body: String, token: String? = null) =
        client.post(path) {
            contentType(ContentType.Application.Json)
            token?.let { bearerAuth(it) }
            setBody(body)
        }

    private suspend fun ApplicationTestBuilder.login(id: String, secret: String) =
        postJson("/auth/login", """{"client_id":"$id","client_secret":"$secret"}""")

    private suspend fun ApplicationTestBuilder.tokenFor(id: String, secret: String): String {
        val response = login(id, secret)
        assertEquals(HttpStatusCode.OK, response.status)
        return Json.parseToJsonElement(response.bodyAsText()).jsonObject["access_token"]!!.jsonPrimitive.content
    }

    private suspend fun ApplicationTestBuilder.readerToken() = tokenFor("test-reader", "test-reader-secret")
    private suspend fun ApplicationTestBuilder.adminToken() = tokenFor("test-admin", "test-admin-secret")

    @Test
    fun `root and health are public`() = testApplication {
        configure()
        assertEquals(HttpStatusCode.OK, client.get("/").status)
        assertEquals(HttpStatusCode.OK, client.get("/health").status)
    }

    @Test
    fun `reports without a token are 401`() = testApplication {
        configure()
        assertEquals(HttpStatusCode.Unauthorized, client.get("/volcanic-reports").status)
    }

    @Test
    fun `reader can read reports with a token from login`() = testApplication {
        configure()
        val response = client.get("/volcanic-reports") { bearerAuth(readerToken()) }
        assertEquals(HttpStatusCode.OK, response.status)
    }

    @Test
    fun `admin can also read reports`() = testApplication {
        configure()
        assertEquals(HttpStatusCode.OK, client.get("/volcanic-reports") { bearerAuth(adminToken()) }.status)
    }

    @Test
    fun `admin routes without a token are 401`() = testApplication {
        configure()
        assertEquals(HttpStatusCode.Unauthorized, postJson("/admin/outage", """{"enabled":true}""").status)
        assertEquals(HttpStatusCode.Unauthorized, postJson("/admin/schema-version", """{"version":2}""").status)
    }

    @Test
    fun `reader is forbidden on admin routes`() = testApplication {
        configure()
        val token = readerToken()
        assertEquals(HttpStatusCode.Forbidden, postJson("/admin/outage", """{"enabled":true}""", token).status)
        assertEquals(HttpStatusCode.Forbidden, postJson("/admin/schema-version", """{"version":2}""", token).status)
    }

    @Test
    fun `admin can change the schema version and confidence_level appears`() = testApplication {
        configure()
        val token = adminToken()
        val before = client.get("/volcanic-reports") { bearerAuth(token) }.bodyAsText()
        assertFalse("confidence_level" in before)

        assertEquals(HttpStatusCode.NoContent, postJson("/admin/schema-version", """{"version":2}""", token).status)

        val after = client.get("/volcanic-reports") { bearerAuth(token) }.bodyAsText()
        assertTrue("confidence_level" in after)
    }

    private suspend fun ApplicationTestBuilder.setOutage(enabled: Boolean, adminToken: String) =
        postJson("/admin/outage", """{"enabled":$enabled}""", adminToken)

    @Test
    fun `outage takes down health and reports whatever the token`() = testApplication {
        configure()
        val reader = readerToken()
        val admin = adminToken()
        assertEquals(HttpStatusCode.NoContent, setOutage(true, admin).status)

        assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/health").status)
        assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/volcanic-reports").status)
        assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/volcanic-reports") { bearerAuth(reader) }.status)
        assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/volcanic-reports") { bearerAuth(admin) }.status)
        assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/volcanic-reports") { bearerAuth("garbage") }.status)
    }

    @Test
    fun `root stays up during an outage`() = testApplication {
        configure()
        setOutage(true, adminToken())
        assertEquals(HttpStatusCode.OK, client.get("/").status)
    }

    @Test
    fun `during an outage reader login is 503 but admin login works`() = testApplication {
        configure()
        val admin = adminToken()
        setOutage(true, admin)

        assertEquals(HttpStatusCode.ServiceUnavailable, login("test-reader", "test-reader-secret").status)
        assertEquals(HttpStatusCode.OK, login("test-admin", "test-admin-secret").status)
        assertEquals(HttpStatusCode.Unauthorized, login("test-reader", "wrong").status)
    }

    @Test
    fun `admin routes still validate the token during an outage`() = testApplication {
        configure()
        val reader = readerToken()
        val admin = adminToken()
        setOutage(true, admin)

        assertEquals(HttpStatusCode.Unauthorized, postJson("/admin/outage", """{"enabled":false}""").status)
        assertEquals(HttpStatusCode.Unauthorized, postJson("/admin/outage", """{"enabled":false}""", "garbage").status)
        assertEquals(HttpStatusCode.Forbidden, postJson("/admin/outage", """{"enabled":false}""", reader).status)
        assertEquals(HttpStatusCode.NoContent, postJson("/admin/outage", """{"enabled":false}""", admin).status)
    }

    @Test
    fun `everything recovers without a restart when the outage is switched off`() = testApplication {
        configure()
        val admin = adminToken()
        setOutage(true, admin)
        setOutage(false, admin)

        assertEquals(HttpStatusCode.OK, client.get("/health").status)
        assertEquals(HttpStatusCode.OK, client.get("/volcanic-reports") { bearerAuth(readerToken()) }.status)
    }

    @Test
    fun `a refused admin request does not change the outage state`() = testApplication {
        configure()
        val reader = readerToken()
        assertEquals(HttpStatusCode.Forbidden, setOutage(true, reader).status)
        assertEquals(HttpStatusCode.Unauthorized, postJson("/admin/outage", """{"enabled":true}""").status)

        assertEquals(HttpStatusCode.OK, client.get("/health").status)
    }

    @Test
    fun `login with wrong secret or unknown id gives the same 401`() = testApplication {
        configure()
        val wrongSecret = login("test-reader", "nope")
        val unknownId = login("nobody", "test-reader-secret")

        assertEquals(HttpStatusCode.Unauthorized, wrongSecret.status)
        assertEquals(HttpStatusCode.Unauthorized, unknownId.status)
        assertEquals(wrongSecret.bodyAsText(), unknownId.bodyAsText())
    }

    @Test
    fun `login response follows the bearer token format`() = testApplication {
        configure()
        val body = Json.parseToJsonElement(login("test-reader", "test-reader-secret").bodyAsText()).jsonObject

        assertEquals("Bearer", body["token_type"]!!.jsonPrimitive.content)
        assertEquals("300", body["expires_in"]!!.jsonPrimitive.content)
    }

    // The limit is 5 per minute in tests (the default). Wrong secrets count just like right ones.
    @Test
    fun `sixth login attempt in a minute is 429 with a retry-after header`() = testApplication {
        configure()
        repeat(5) { assertEquals(HttpStatusCode.Unauthorized, login("test-reader", "wrong").status) }

        val blocked = login("test-reader", "wrong")
        assertEquals(HttpStatusCode.TooManyRequests, blocked.status)
        assertNotNull(blocked.headers[HttpHeaders.RetryAfter]?.toLongOrNull())
    }

    @Test
    fun `a blocked client cannot log in even with the right secret`() = testApplication {
        configure()
        repeat(5) { login("test-reader", "wrong") }

        assertEquals(HttpStatusCode.TooManyRequests, login("test-reader", "test-reader-secret").status)
    }

    @Test
    fun `flooding the reader does not block the admin`() = testApplication {
        configure()
        repeat(6) { login("test-reader", "wrong") }

        assertEquals(HttpStatusCode.OK, login("test-admin", "test-admin-secret").status)
    }

    @Test
    fun `flooding with invented ids is limited too`() = testApplication {
        configure()
        repeat(5) { assertEquals(HttpStatusCode.Unauthorized, login("fake-$it", "wrong").status) }

        assertEquals(HttpStatusCode.TooManyRequests, login("fake-again", "wrong").status)
    }

    @Test
    fun `existing tokens keep working while the login is rate limited`() = testApplication {
        configure()
        val token = readerToken()
        repeat(6) { login("test-reader", "wrong") }

        assertEquals(HttpStatusCode.OK, client.get("/volcanic-reports") { bearerAuth(token) }.status)
    }

    private suspend fun ApplicationTestBuilder.setDelay(min: Long, max: Long, token: String) =
        postJson("/admin/response-delay", """{"min_ms":$min,"max_ms":$max}""", token)

    private suspend fun ApplicationTestBuilder.elapsedMs(path: String): Long {
        val start = System.nanoTime()
        client.get(path)
        return (System.nanoTime() - start) / 1_000_000
    }

    private suspend fun realSleep(ms: Long) = withContext(Dispatchers.Default) { delay(ms) }

    @Test
    fun `response delay route needs an admin token`() = testApplication {
        configure()
        val reader = readerToken()

        assertEquals(HttpStatusCode.Unauthorized, postJson("/admin/response-delay", """{"min_ms":1,"max_ms":2}""").status)
        assertEquals(HttpStatusCode.Forbidden, setDelay(1, 2, reader).status)
        assertEquals(HttpStatusCode.NoContent, setDelay(1, 2, adminToken()).status)
    }

    @Test
    fun `an invalid delay range is 400 and leaves the current delay untouched`() = testApplication {
        configure()
        val admin = adminToken()
        client.get("/health")

        assertEquals(HttpStatusCode.BadRequest, setDelay(3000, 500, admin).status)
        assertEquals(HttpStatusCode.BadRequest, setDelay(-1, 500, admin).status)
        assertEquals(HttpStatusCode.BadRequest, postJson("/admin/response-delay", """{"min_ms":100}""", admin).status)
        assertEquals(HttpStatusCode.BadRequest, postJson("/admin/response-delay", "not json", admin).status)

        assertTrue(elapsedMs("/health") < 300)
    }

    @Test
    fun `the delay set through the admin route takes effect and can be removed again`() = testApplication {
        configure()
        val admin = adminToken()
        client.get("/health")

        assertEquals(HttpStatusCode.NoContent, setDelay(400, 400, admin).status)
        val slow = elapsedMs("/health")
        assertTrue(slow >= 350, "took only $slow ms")

        assertEquals(HttpStatusCode.NoContent, setDelay(0, 0, admin).status)
        assertTrue(elapsedMs("/health") < 300)
    }

    @Test
    fun `the admin route is not slowed down by the delay it configures`() = testApplication {
        configure()
        val admin = adminToken()
        setDelay(3000, 3000, admin)

        val start = System.nanoTime()
        assertEquals(HttpStatusCode.NoContent, setDelay(0, 0, admin).status)
        val elapsed = (System.nanoTime() - start) / 1_000_000

        assertTrue(elapsed < 1000, "took $elapsed ms")
    }

    @Test
    fun `changing the delay releases requests that are already waiting`() = testApplication {
        configure()
        val admin = adminToken()
        client.get("/health")
        setDelay(5000, 5000, admin)

        val start = System.nanoTime()
        coroutineScope {
            val pending = async { client.get("/health") }
            realSleep(300)
            setDelay(0, 0, admin)

            assertEquals(HttpStatusCode.OK, pending.await().status)
            val elapsed = (System.nanoTime() - start) / 1_000_000
            assertTrue(elapsed < 3000, "was not released, took $elapsed ms")
        }
    }

    @Test
    fun `sending the same range again also releases waiting requests`() = testApplication {
        configure()
        val admin = adminToken()
        client.get("/health")
        setDelay(5000, 5000, admin)

        val start = System.nanoTime()
        coroutineScope {
            val pending = async { client.get("/health") }
            realSleep(300)
            setDelay(5000, 5000, admin)

            assertEquals(HttpStatusCode.OK, pending.await().status)
            val elapsed = (System.nanoTime() - start) / 1_000_000
            assertTrue(elapsed < 3000, "was not released, took $elapsed ms")
        }
    }

    @Test
    fun `switching the outage on through the admin route releases waiting requests with 503`() = testApplication {
        configure()
        val admin = adminToken()
        client.get("/health")
        setDelay(5000, 5000, admin)

        val start = System.nanoTime()
        coroutineScope {
            val pending = async { client.get("/health") }
            realSleep(300)
            setOutage(true, admin)

            assertEquals(HttpStatusCode.ServiceUnavailable, pending.await().status)
            val elapsed = (System.nanoTime() - start) / 1_000_000
            assertTrue(elapsed < 3000, "was not released, took $elapsed ms")
        }
    }

    @Test
    fun `login with a malformed body is 400`() = testApplication {
        configure()
        assertEquals(HttpStatusCode.BadRequest, postJson("/auth/login", "not json").status)
        assertEquals(HttpStatusCode.BadRequest, postJson("/auth/login", """{"client_id":"test-reader"}""").status)
    }

    @Test
    fun `another services credentials are rejected`() = testApplication {
        configure()
        val response = client.get("/volcanic-reports") { header("X-BMKG-Key", "some-bmkg-key") }
        assertEquals(HttpStatusCode.Unauthorized, response.status)
    }
}
