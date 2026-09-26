package com.staryo

import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.config.*
import io.ktor.server.testing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.*

class GeneratorConfigTest {

    private fun ApplicationTestBuilder.configureWith(vararg overrides: Pair<String, String>) {
        environment {
            config = ApplicationConfig("application.yaml").mergeWith(MapApplicationConfig(*overrides))
        }
    }

    private fun startupFailureMessage(interval: String): String {
        val error = assertFails {
            testApplication {
                configureWith("pvmbg.generator.interval-seconds" to interval)
                startApplication()
            }
        }
        return generateSequence<Throwable>(error) { it.cause }.mapNotNull { it.message }.joinToString(" | ")
    }

    @Test
    fun `the default interval is 10 seconds`() {
        JsonCapture("event").use { capture ->
            testApplication {
                configure()
                startApplication()
            }

            assertEquals("10000", capture.lines().single { it.text("event") == "startup" }.text("generator_interval_ms"))
        }
    }

    @Test
    fun `a configured interval is used and decimals are allowed`() {
        JsonCapture("event").use { capture ->
            testApplication {
                configureWith("pvmbg.generator.interval-seconds" to "0.5")
                startApplication()
            }

            assertEquals("500", capture.lines().single { it.text("event") == "startup" }.text("generator_interval_ms"))
        }
    }

    @Test
    fun `the smallest allowed interval is accepted`() {
        testApplication {
            configureWith("pvmbg.generator.interval-seconds" to "0.001")
            startApplication()
        }
    }

    @Test
    fun `an interval that is zero negative too small or not a number stops the app from starting`() {
        for (bad in listOf("0", "-1", "0.0001", "abc", "", "NaN", "Infinity")) {
            val message = startupFailureMessage(bad)
            assertTrue("PVMBG_GENERATOR_INTERVAL_SECONDS" in message, "'$bad' gave: $message")
        }
    }

    @Test
    fun `the configured interval really drives the live generator`() = testApplication {
        configureWith("pvmbg.seed.initial" to "3", "pvmbg.generator.interval-seconds" to "0.05")
        startApplication()

        val login = client.post("/auth/login") {
            contentType(ContentType.Application.Json)
            setBody("""{"client_id":"test-reader","client_secret":"test-reader-secret"}""")
        }
        val token = Json.parseToJsonElement(login.bodyAsText()).jsonObject["access_token"]!!.jsonPrimitive.content

        var count = 0
        repeat(150) {
            count = Json.parseToJsonElement(client.get("/volcanic-reports") { bearerAuth(token) }.bodyAsText()).jsonArray.size
            if (count >= 10) return@repeat
            withContext(Dispatchers.Default) { delay(20) }
        }

        assertTrue(count >= 10, "only $count reports after 3 s with 3 seeded and a 50 ms interval")
    }

    @Test
    fun `a slow interval adds nothing in the first moments after the seed`() = testApplication {
        configureWith("pvmbg.seed.initial" to "3")
        startApplication()

        val login = client.post("/auth/login") {
            contentType(ContentType.Application.Json)
            setBody("""{"client_id":"test-reader","client_secret":"test-reader-secret"}""")
        }
        val token = Json.parseToJsonElement(login.bodyAsText()).jsonObject["access_token"]!!.jsonPrimitive.content
        val count = Json.parseToJsonElement(client.get("/volcanic-reports") { bearerAuth(token) }.bodyAsText()).jsonArray.size

        assertEquals(3, count)
    }
}
