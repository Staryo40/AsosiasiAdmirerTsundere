package com.staryo.seed

import com.staryo.repository.VolcanicReportRepository
import com.staryo.seed.ReportGenerator.Companion.VOLCANOES
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class LiveReportGenerator(
    private val repository: VolcanicReportRepository,
    private val generator: ReportGenerator,
    private val interval: Duration,
) {
    fun start(scope: CoroutineScope): Job = scope.launch {
        while (isActive) {
            delay(interval.toMillis())
            repository.add(generator.next(VOLCANOES.random(), Instant.now().truncatedTo(ChronoUnit.SECONDS)))
        }
    }
}
