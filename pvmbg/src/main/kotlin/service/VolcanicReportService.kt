package com.staryo.service

import com.staryo.enums.PayloadFault
import com.staryo.model.*
import com.staryo.repository.VolcanicReportRepository
import com.staryo.state.AppState
import com.staryo.state.DelayRange
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.time.Instant
import java.time.format.DateTimeParseException

class InvalidRequestException(message: String) : RuntimeException(message)
class ServiceUnavailableException(message: String) : RuntimeException(message)

class VolcanicReportService(
    private val repository: VolcanicReportRepository,
    private val state: AppState,
) {
    private val json = Json

    fun getReports(sinceRaw: String?): RenderedReports {
        val reports = repository.findSince(sinceRaw?.let(::parseInstant))
        val body = json.encodeToString(JsonElement.serializer(), shapeReports(reports, state.schemaVersion))
        return RenderedReports(body.toByteArray(), state.payloadFault)
    }

    // 1 and 2 are compatible; 3-5 each break exactly one rule on top of the v1 shape.
    private fun shapeReports(reports: List<VolcanicReportV2>, version: Int): JsonElement = when (version) {
        1 -> json.encodeToJsonElement(reports.map { it.toV1() })
        2 -> json.encodeToJsonElement(reports)
        else -> JsonArray(reports.map { breakSchema(json.encodeToJsonElement(it.toV1()).jsonObject, version) })
    }

    private fun breakSchema(report: JsonObject, version: Int): JsonObject {
        val fields = report.toMutableMap()
        when (version) {
            3 -> fields.remove("alert_level")?.let { fields["level"] = it }
            4 -> fields["eruption_count_24h"]?.let { fields["eruption_count_24h"] = JsonPrimitive(it.jsonPrimitive.content) }
            5 -> fields.remove("reported_at")
        }
        return JsonObject(fields)
    }

    fun setSchemaVersion(version: Int) {
        if (version !in 1..5) throw InvalidRequestException("schema version must be between 1 and 5")
        state.schemaVersion = version
    }

    fun setPayloadFault(fault: PayloadFault) {
        state.payloadFault = fault
    }

    fun setOutage(enabled: Boolean) {
        state.outage = enabled
    }

    // Also releases every request that is currently waiting out its delay, even if the range is unchanged.
    fun setResponseDelay(minMs: Long, maxMs: Long) {
        val range = try {
            DelayRange(minMs, maxMs)
        } catch (e: IllegalArgumentException) {
            throw InvalidRequestException(e.message ?: "invalid delay range")
        }
        state.updateResponseDelay(range)
    }

    private fun parseInstant(raw: String): Instant =
        try {
            Instant.parse(raw)
        } catch (e: DateTimeParseException) {
            throw InvalidRequestException("'since' must be an ISO-8601 timestamp, e.g. 2026-01-01T00:00:00Z")
        }
}
