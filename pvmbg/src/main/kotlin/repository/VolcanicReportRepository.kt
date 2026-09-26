package com.staryo.repository

import com.staryo.model.VolcanicReportV2
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

interface VolcanicReportRepository {
    fun add(report: VolcanicReportV2)
    fun findSince(since: Instant?): List<VolcanicReportV2>
}

class InMemoryVolcanicReportRepository : VolcanicReportRepository {
    private val reports = CopyOnWriteArrayList<VolcanicReportV2>()

    override fun add(report: VolcanicReportV2) {
        reports.add(report)
    }

    override fun findSince(since: Instant?): List<VolcanicReportV2> =
        reports
            .filter { since == null || Instant.parse(it.reported_at).isAfter(since) }
            .sortedBy { Instant.parse(it.reported_at) }
}
