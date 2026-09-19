package com.staryo.seed

import com.staryo.enums.AlertLevel
import com.staryo.model.VolcanicReportV2
import com.staryo.repository.VolcanicReportRepository
import java.time.Duration
import java.time.Instant
import kotlin.random.Random

class ReportSeeder(private val repository: VolcanicReportRepository) {
    fun run(from: Instant = DEFAULT_START, until: Instant = Instant.now()) {
        val random = Random(42)
        var id = 1

        generateSequence(from) { it + PERIOD }
            .takeWhile { !it.isAfter(until) }
            .forEach { slot ->
                for (volcano in VOLCANOES) {
                    repository.add(
                        VolcanicReportV2(
                            report_id = "rpt-%06d".format(id++),
                            volcano_id = volcano,
                            alert_level = AlertLevel.entries[random.nextInt(AlertLevel.entries.size)],
                            eruption_count_24h = random.nextInt(0, 30),
                            ash_column_height_m = random.nextInt(0, 5000).toFloat(),
                            reported_at = slot.toString(),
                            confidence_level = random.nextFloat(),
                        )
                    )
                }
            }
    }

    private companion object {
        val DEFAULT_START: Instant = Instant.parse("2026-01-01T00:00:00Z")
        val PERIOD: Duration = Duration.ofHours(6)
        val VOLCANOES = listOf("merapi", "semeru", "bromo", "krakatau", "sinabung")
    }
}
