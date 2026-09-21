package com.staryo

import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.testing.*
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
