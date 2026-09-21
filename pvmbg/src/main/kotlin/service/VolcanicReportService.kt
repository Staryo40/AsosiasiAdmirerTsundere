package com.staryo.service

import com.staryo.model.*
import com.staryo.repository.VolcanicReportRepository
import com.staryo.state.AppState
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.encodeToJsonElement
import java.time.Instant
import java.time.format.DateTimeParseException

class InvalidRequestException(message: String) : RuntimeException(message)
class ServiceUnavailableException(message: String) : RuntimeException(message)

class VolcanicReportService(
    private val repository: VolcanicReportRepository,
    private val state: AppState,
) {
    private val json = Json

    fun getReports(sinceRaw: String?): JsonElement {
        val reports = repository.findSince(sinceRaw?.let(::parseInstant))
        return when (state.schemaVersion) {
            1 -> json.encodeToJsonElement(reports.map { it.toV1() })
            else -> json.encodeToJsonElement(reports)
        }
    }

    fun setSchemaVersion(version: Int) {
        if (version !in 1..2) throw InvalidRequestException("schema version must be 1 or 2")
        state.schemaVersion = version
    }

    fun setOutage(enabled: Boolean) {
        state.outage = enabled
    }

    private fun parseInstant(raw: String): Instant =
        try {
            Instant.parse(raw)
        } catch (e: DateTimeParseException) {
            throw InvalidRequestException("'since' must be an ISO-8601 timestamp, e.g. 2026-01-01T00:00:00Z")
        }
}
