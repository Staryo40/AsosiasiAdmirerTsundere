package com.staryo

import com.staryo.config.AppConfig
import com.staryo.plugins.configureSerialization
import com.staryo.plugins.configureStatusPages
import com.staryo.repository.InMemoryVolcanicReportRepository
import com.staryo.seed.ReportSeeder
import com.staryo.service.VolcanicReportService
import com.staryo.state.AppState
import io.ktor.server.application.*

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.module() {
    val config = AppConfig.from(this)
    val state = AppState()
    val repository = InMemoryVolcanicReportRepository()
    ReportSeeder(repository).run()
    val service = VolcanicReportService(repository, state)

    configureSerialization()
    configureStatusPages()
    configureRouting(config, service)
}
