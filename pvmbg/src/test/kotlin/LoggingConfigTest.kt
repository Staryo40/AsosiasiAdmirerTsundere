package com.staryo

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.classic.util.LogbackMDCAdapter
import io.ktor.server.config.*
import io.ktor.server.testing.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import kotlin.test.*

// Loads the real src/main/resources/logback.xml into a fresh logger context and captures what it prints.
class LoggingConfigTest {

    private fun capture(
        settings: Map<String, String> = emptyMap(),
        mdc: Map<String, String> = emptyMap(),
        log: (Logger) -> Unit,
    ): List<String> {
        val originalOut = System.out
        val buffer = ByteArrayOutputStream()
        System.setOut(PrintStream(buffer, true, Charsets.UTF_8))

        val context = LoggerContext()
        val mdcAdapter = LogbackMDCAdapter()
        context.mdcAdapter = mdcAdapter
        settings.forEach { (key, value) -> context.putProperty(key, value) }
        try {
            JoranConfigurator().also { it.context = context }.doConfigure(LoggingConfigTest::class.java.getResource("/logback.xml"))
            mdc.forEach { (key, value) -> mdcAdapter.put(key, value) }
            log(context.getLogger("test.logger"))
        } finally {
            context.stop() // drains the async queue before the output is read
            System.setOut(originalOut)
        }
        return buffer.toString(Charsets.UTF_8).lines().filter { it.isNotBlank() }
    }

    private fun jsonLines(lines: List<String>): List<JsonObject> =
        lines.filter { it.startsWith("{") }.map { Json.parseToJsonElement(it).jsonObject }

    private fun JsonObject.text(field: String) = this[field]?.jsonPrimitive?.content

    @Test
    fun `default output is one valid json object per line with the fixed fields`() {
        val lines = capture { it.info("hello world") }

        val event = jsonLines(lines).single()
        assertEquals("hello world", event.text("message"))
        assertEquals("INFO", event.text("level"))
        assertEquals("pvmbg", event.text("service"))
        assertEquals("test.logger", event.text("logger"))
        assertNotNull(event.text("@timestamp"))
        assertNotNull(event.text("thread"))
    }

    @Test
    fun `unneeded logstash fields are left out`() {
        val event = jsonLines(capture { it.info("x") }).single()

        assertNull(event["@version"])
        assertNull(event["level_value"])
        assertNull(event["logger_name"])
        assertNull(event["thread_name"])
    }

    @Test
    fun `context values such as the correlation id become top level fields`() {
        val lines = capture(mdc = mapOf("correlation_id" to "abc-123")) { it.info("with context") }

        assertEquals("abc-123", jsonLines(lines).single().text("correlation_id"))
    }

    @Test
    fun `an exception with a stack trace still stays on a single line`() {
        val lines = capture { it.error("failed", IllegalStateException("boom")) }

        val event = jsonLines(lines).single()
        assertEquals(1, lines.size)
        assertTrue("boom" in event.text("stack_trace")!!)
    }

    @Test
    fun `quotes and line breaks in a message cannot break or forge a line`() {
        val hostile = "a\"b\nc\r\n{\"level\":\"FAKE\"}"

        val lines = capture { it.info(hostile) }

        val event = jsonLines(lines).single()
        assertEquals(1, lines.size)
        assertEquals(hostile, event.text("message"))
        assertEquals("INFO", event.text("level"))
    }

    @Test
    fun `text format is readable and not json`() {
        val lines = capture(settings = mapOf("PVMBG_LOG_FORMAT" to "text"), mdc = mapOf("correlation_id" to "abc-123")) {
            it.info("hello text")
        }

        val line = lines.single()
        assertFalse(line.startsWith("{"))
        assertTrue("INFO" in line && "hello text" in line && "abc-123" in line, line)
    }

    @Test
    fun `text format keeps one event on one line`() {
        val lines = capture(settings = mapOf("PVMBG_LOG_FORMAT" to "text")) { it.info("first\nsecond\r\nthird") }

        assertEquals(1, lines.size)
        assertTrue("first second  third" in lines.single(), lines.single())
    }

    @Test
    fun `text format shows a placeholder when there is no correlation id`() {
        val lines = capture(settings = mapOf("PVMBG_LOG_FORMAT" to "text")) { it.info("no context") }

        assertTrue("no-id" in lines.single())
    }

    @Test
    fun `default level is info so debug is left out`() {
        val lines = capture { logger ->
            logger.debug("debug line")
            logger.info("info line")
        }

        assertEquals(listOf("info line"), jsonLines(lines).map { it.text("message") })
    }

    @Test
    fun `the level can be raised to warn`() {
        val lines = capture(settings = mapOf("PVMBG_LOG_LEVEL" to "WARN")) { logger ->
            logger.info("info line")
            logger.warn("warn line")
        }

        assertEquals(listOf("warn line"), jsonLines(lines).map { it.text("message") })
    }

    @Test
    fun `the level can be lowered to debug`() {
        val lines = capture(settings = mapOf("PVMBG_LOG_LEVEL" to "DEBUG")) { it.debug("debug line") }

        assertEquals(listOf("debug line"), jsonLines(lines).map { it.text("message") })
    }

    @Test
    fun `a burst of lines loses nothing through the async appender`() {
        val lines = capture { logger -> repeat(5000) { logger.info("line {}", it) } }

        assertEquals(5000, jsonLines(lines).size)
    }

    private fun startupFailureMessage(level: String, format: String): String {
        val error = assertFails {
            testApplication {
                environment {
                    config = ApplicationConfig("application.yaml").mergeWith(
                        MapApplicationConfig("pvmbg.logging.level" to level, "pvmbg.logging.format" to format)
                    )
                }
                startApplication()
            }
        }
        return generateSequence<Throwable>(error) { it.cause }.mapNotNull { it.message }.joinToString(" | ")
    }

    @Test
    fun `an unknown log level stops the app from starting`() {
        val message = startupFailureMessage("LOUD", "json")

        assertTrue("PVMBG_LOG_LEVEL" in message, message)
    }

    @Test
    fun `an unknown or uppercase log format stops the app from starting`() {
        assertTrue("PVMBG_LOG_FORMAT" in startupFailureMessage("INFO", "xml"))
        assertTrue("PVMBG_LOG_FORMAT" in startupFailureMessage("INFO", "JSON"))
    }

    @Test
    fun `valid settings are accepted in any letter case for the level`() {
        // Would throw if rejected; the app is only configured, not started.
        testApplication {
            environment {
                config = ApplicationConfig("application.yaml").mergeWith(
                    MapApplicationConfig("pvmbg.logging.level" to "warn", "pvmbg.logging.format" to "text")
                )
            }
            startApplication()
        }
    }
}
