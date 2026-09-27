package com.staryo.model

import kotlinx.serialization.Serializable
import com.staryo.enums.AlertLevel

@Serializable
data class VolcanicReportV1(
    val report_id : String,
    val volcano_id : String,
    val alert_level : AlertLevel,
    val eruption_count_24h : Int,
    val ash_column_height_m : Float,
    val reported_at : String
)