package com.staryo.logging

import net.logstash.logback.argument.StructuredArguments.entries
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

// Named things that happened (a login, an admin change, startup), as opposed to the per-request access line.
// The correlation id is added automatically when the event is written during a request.
object Events {
    // Long enough for any real id; stops a huge attacker-supplied value from flooding the log.
    const val MAX_FIELD_LENGTH = 256

    private val log = LoggerFactory.getLogger("event")

    fun info(name: String, vararg fields: Pair<String, Any?>) = write(Level.INFO, name, fields)

    fun warn(name: String, vararg fields: Pair<String, Any?>) = write(Level.WARN, name, fields)

    // The stack trace goes into the line as a single-line field, so an error is still one event.
    fun error(name: String, cause: Throwable, vararg fields: Pair<String, Any?>) = write(Level.ERROR, name, fields, cause)

    private fun write(level: Level, name: String, fields: Array<out Pair<String, Any?>>, cause: Throwable? = null) {
        val values = linkedMapOf<String, Any>("event" to name)
        for ((key, value) in fields) {
            if (value != null) values[key] = if (value is String) value.take(MAX_FIELD_LENGTH) else value
        }
        // The readable message is passed as an argument, never as the format string, so a "{}" in a value is harmless.
        val summary = values.entries.drop(1).joinToString(" ") { "${it.key}=${it.value}" }
        val builder = log.atLevel(level)
            .setMessage("{}")
            .addArgument(if (summary.isEmpty()) name else "$name $summary")
            .addArgument(entries(values))
        if (cause != null) builder.setCause(cause)
        builder.log()
    }
}
