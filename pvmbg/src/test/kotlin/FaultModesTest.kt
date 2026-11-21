package com.staryo

import com.staryo.service.FAULT_NESTING_DEPTH
import com.staryo.service.FAULT_OVERSIZED_BYTES
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.testing.*
import io.ktor.utils.io.*
import kotlinx.serialization.json.*
import kotlin.test.*

class FaultModesTest {

    private suspend fun ApplicationTestBuilder.postJson(path: String, body: String, token: String? = null) =
        client.post(path) {
            contentType(ContentType.Application.Json)
            token?.let { bearerAuth(it) }
            setBody(body)
        }

    private suspend fun ApplicationTestBuilder.tokenFor(id: String, secret: String): String {
        val response = postJson("/auth/login", """{"client_id":"$id","client_secret":"$secret"}""")
        assertEquals(HttpStatusCode.OK, response.status)
        return Json.parseToJsonElement(response.bodyAsText()).jsonObject["access_token"]!!.jsonPrimitive.content
    }

    private suspend fun ApplicationTestBuilder.adminToken() = tokenFor("test-admin", "test-admin-secret")

    private suspend fun ApplicationTestBuilder.reportBytes(token: String): ByteArray =
        client.get("/volcanic-reports") { bearerAuth(token) }.bodyAsBytes()

    private suspend fun ApplicationTestBuilder.reportsAt(version: Int, token: String): JsonArray {
        assertEquals(HttpStatusCode.NoContent, postJson("/admin/schema-version", """{"version":$version}""", token).status)
        val reports = Json.parseToJsonElement(reportBytes(token).decodeToString()).jsonArray
        assertTrue(reports.isNotEmpty(), "the seed must produce reports for these checks")
        return reports
    }

    private suspend fun ApplicationTestBuilder.setFault(mode: String, token: String) =
        postJson("/admin/payload-fault", """{"mode":"$mode"}""", token)

    @Test
    fun `schema 3 renames alert_level to level`() = testApplication {
        configure()
        reportsAt(3, adminToken()).forEach {
            assertFalse("alert_level" in it.jsonObject)
            assertTrue("level" in it.jsonObject)
        }
    }

    @Test
    fun `schema 4 sends eruption_count_24h as a string`() = testApplication {
        configure()
        reportsAt(4, adminToken()).forEach {
            assertTrue(it.jsonObject["eruption_count_24h"]!!.jsonPrimitive.isString)
        }
    }

    @Test
    fun `schema 5 drops reported_at`() = testApplication {
        configure()
        reportsAt(5, adminToken()).forEach { assertFalse("reported_at" in it.jsonObject) }
    }

    @Test
    fun `schema versions outside 1 to 5 are 400`() = testApplication {
        configure()
        val token = adminToken()
        assertEquals(HttpStatusCode.BadRequest, postJson("/admin/schema-version", """{"version":6}""", token).status)
        assertEquals(HttpStatusCode.BadRequest, postJson("/admin/schema-version", """{"version":0}""", token).status)
    }

    @Test
    fun `truncated cuts the body so it no longer parses`() = testApplication {
        configure()
        val token = adminToken()
        val clean = reportBytes(token)
        assertEquals(HttpStatusCode.NoContent, setFault("truncated", token).status)
        val broken = reportBytes(token)
        assertEquals(clean.size / 2, broken.size)
        assertFailsWith<Exception> { Json.parseToJsonElement(broken.decodeToString()) }
    }

    @Test
    fun `deep_nesting prepends one deeply nested element and keeps the real reports`() = testApplication {
        configure()
        val token = adminToken()
        val clean = reportBytes(token)
        setFault("deep_nesting", token)
        val broken = reportBytes(token).decodeToString()
        val nested = "[".repeat(FAULT_NESTING_DEPTH) + "]".repeat(FAULT_NESTING_DEPTH)
        assertEquals("[" + nested + "," + clean.decodeToString().drop(1), broken)
    }

    @Test
    fun `invalid_utf8 prepends an element with an invalid byte sequence and keeps the real reports`() = testApplication {
        configure()
        val token = adminToken()
        val clean = reportBytes(token)
        setFault("invalid_utf8", token)
        val broken = reportBytes(token)
        val element = """{"report_id":"fault-""".toByteArray() + byteArrayOf(0xC3.toByte(), 0x28) + "\"}".toByteArray()
        val expected = "[".toByteArray() + element + ",".toByteArray() + clean.copyOfRange(1, clean.size)
        assertContentEquals(expected, broken)
    }

    @Test
    fun `oversized streams a body above the configured size`() = testApplication {
        configure()
        val token = adminToken()
        setFault("oversized", token)
        val channel = client.get("/volcanic-reports") { bearerAuth(token) }.bodyAsChannel()
        var total = 0L
        val buffer = ByteArray(64 * 1024)
        while (true) {
            val read = channel.readAvailable(buffer)
            if (read < 0) break
            total += read
        }
        assertTrue(total > FAULT_OVERSIZED_BYTES, "body was $total bytes")
    }

    @Test
    fun `none restores the clean body`() = testApplication {
        configure()
        val token = adminToken()
        val clean = reportBytes(token)
        setFault("truncated", token)
        assertEquals(HttpStatusCode.NoContent, setFault("none", token).status)
        assertContentEquals(clean, reportBytes(token))
    }

    @Test
    fun `payload-fault needs an admin token and a known mode`() = testApplication {
        configure()
        val reader = tokenFor("test-reader", "test-reader-secret")
        assertEquals(HttpStatusCode.Unauthorized, setFault("truncated", "").status)
        assertEquals(HttpStatusCode.Forbidden, setFault("truncated", reader).status)
        assertEquals(HttpStatusCode.BadRequest, setFault("melted", adminToken()).status)
    }
}
