package com.staryo.seed

import com.staryo.enums.AlertLevel
import com.staryo.model.VolcanicReportV2
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

class ReportGenerator(randomSeed: Int = 42) {
    private val random = Random(randomSeed)
    private val nextId = AtomicInteger(1)

    @Synchronized
    fun next(volcanoId: String, reportedAt: Instant) = VolcanicReportV2(
        report_id = "rpt-%06d".format(nextId.getAndIncrement()),
        volcano_id = volcanoId,
        alert_level = AlertLevel.entries[random.nextInt(AlertLevel.entries.size)],
        eruption_count_24h = random.nextInt(0, 30),
        ash_column_height_m = random.nextInt(0, 5000).toFloat(),
        reported_at = reportedAt.toString(),
        confidence_level = random.nextFloat(),
    )

    companion object {
        val VOLCANOES = listOf("merapi", "semeru", "bromo", "krakatau", "sinabung")
    }
}
