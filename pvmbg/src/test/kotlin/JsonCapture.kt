package com.staryo

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.OutputStreamAppender
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import net.logstash.logback.encoder.LogstashEncoder
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream

internal fun JsonObject.text(field: String) = this[field]?.jsonPrimitive?.content
internal fun JsonObject.number(field: String) = this[field]?.jsonPrimitive?.content?.toLong()

// Renders one logger's events with the same JSON encoder production uses, so tests see the real fields.
internal class JsonCapture(loggerName: String, level: Level? = null) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(loggerName) as Logger
    private val output = ByteArrayOutputStream()
    private val appender: OutputStreamAppender<ILoggingEvent>
    private val previousLevel = logger.level
    private val previousAdditive = logger.isAdditive

    init {
        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        val encoder = LogstashEncoder().apply { this.context = context; start() }
        appender = OutputStreamAppender<ILoggingEvent>().apply {
            this.context = context
            this.encoder = encoder
            outputStream = output
            start()
        }
        logger.addAppender(appender)
        logger.isAdditive = false
        if (level != null) logger.level = level
    }

    fun raw(): String = output.toString(Charsets.UTF_8)

    fun lines(): List<JsonObject> =
        raw().lines().filter { it.isNotBlank() }.map { Json.parseToJsonElement(it).jsonObject }

    // Some lines are written just after the response is sent, so the client can be a moment ahead of them.
    suspend fun awaitLines(count: Int): List<JsonObject> {
        repeat(100) {
            if (lines().size >= count) return lines()
            withContext(Dispatchers.Default) { delay(20) }
        }
        return lines()
    }

    override fun close() {
        logger.detachAppender(appender)
        appender.stop()
        logger.level = previousLevel
        logger.isAdditive = previousAdditive
    }
}
