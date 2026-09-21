package com.staryo.logging

import net.logstash.logback.argument.StructuredArguments.entries
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

object Events {
    const val MAX_FIELD_LENGTH = 256

    private val log = LoggerFactory.getLogger("event")

    fun info(name: String, vararg fields: Pair<String, Any?>) = write(Level.INFO, name, fields)

    fun warn(name: String, vararg fields: Pair<String, Any?>) = write(Level.WARN, name, fields)

    fun error(name: String, cause: Throwable, vararg fields: Pair<String, Any?>) = write(Level.ERROR, name, fields, cause)

    private fun write(level: Level, name: String, fields: Array<out Pair<String, Any?>>, cause: Throwable? = null) {
        val values = linkedMapOf<String, Any>("event" to name)
        for ((key, value) in fields) {
            if (value != null) values[key] = if (value is String) value.take(MAX_FIELD_LENGTH) else value
        }
        val summary = values.entries.drop(1).joinToString(" ") { "${it.key}=${it.value}" }
        val builder = log.atLevel(level)
            .setMessage("{}")
            .addArgument(if (summary.isEmpty()) name else "$name $summary")
            .addArgument(entries(values))
        if (cause != null) builder.setCause(cause)
        builder.log()
    }
}
