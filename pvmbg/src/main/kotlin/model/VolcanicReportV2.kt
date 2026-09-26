package com.staryo.model

import kotlinx.serialization.Serializable
import com.staryo.enums.AlertLevel

@Serializable
data class VolcanicReportV2(
    val report_id : String,
    val volcano_id : String,
    val alert_level : AlertLevel,
    val eruption_count_24h : Int,
    val ash_column_height_m : Float,
    val reported_at : String,
    val confidence_level : Float
) {
    fun toV1() = VolcanicReportV1(
        report_id = report_id,
        volcano_id = volcano_id,
        alert_level = alert_level,
        eruption_count_24h = eruption_count_24h,
        ash_column_height_m = ash_column_height_m,
        reported_at = reported_at,
    )
}