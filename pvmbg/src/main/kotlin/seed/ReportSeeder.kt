package com.staryo.seed

import com.staryo.repository.VolcanicReportRepository
import com.staryo.seed.ReportGenerator.Companion.VOLCANOES
import java.time.Duration
import java.time.Instant

class ReportSeeder(
    private val repository: VolcanicReportRepository,
    private val generator: ReportGenerator,
) {
    fun runDefault(from: Instant = DEFAULT_START, until: Instant = Instant.now()) {
        generateSequence(from) { it + PERIOD }
            .takeWhile { !it.isAfter(until) }
            .forEach { slot ->
                VOLCANOES.forEach { repository.add(generator.next(it, slot)) }
            }
    }

    fun run(count: Int, until: Instant = Instant.now()) {
        val lastSlot = Instant.ofEpochSecond(until.epochSecond / PERIOD.seconds * PERIOD.seconds)
        repeat(count) { i ->
            val slotsBack = (count - 1 - i) / VOLCANOES.size
            repository.add(generator.next(VOLCANOES[i % VOLCANOES.size], lastSlot - PERIOD.multipliedBy(slotsBack.toLong())))
        }
    }

    private companion object {
        val DEFAULT_START: Instant = Instant.parse("2026-01-01T00:00:00Z")
        val PERIOD: Duration = Duration.ofHours(6)
    }
}
