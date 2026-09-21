package com.staryo

import com.staryo.config.AppConfig
import com.staryo.plugins.configureAuthentication
import com.staryo.plugins.configureSerialization
import com.staryo.plugins.configureStatusPages
import com.staryo.repository.InMemoryVolcanicReportRepository
import com.staryo.seed.LiveReportGenerator
import com.staryo.seed.ReportGenerator
import com.staryo.seed.ReportSeeder
import com.staryo.service.AuthService
import com.staryo.service.VolcanicReportService
import com.staryo.state.AppState
import io.ktor.server.application.*
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(withSeedOverride(args))
}

// `--args="10"` will make 10 initial records
private fun withSeedOverride(args: Array<String>): Array<String> {
    val (positional, options) = args.partition { !it.startsWith("-") }
    if (positional.isEmpty()) return args

    val count = positional.singleOrNull()?.toIntOrNull()?.takeIf { it >= 0 }
    if (count == null) {
        System.err.println("Invalid seed size '${positional.joinToString(" ")}': expected a single non-negative integer")
        exitProcess(1)
    }
    return (options + "-P:pvmbg.seed.initial=$count").toTypedArray()
}

fun Application.module() {
    val config = AppConfig.from(this)
    val state = AppState()
    val repository = InMemoryVolcanicReportRepository()
    val generator = ReportGenerator()

    val seeder = ReportSeeder(repository, generator)
    config.initialSeedCount?.let(seeder::run) ?: seeder.runDefault()
    LiveReportGenerator(repository, generator, config.generatorInterval).start(this)

    val service = VolcanicReportService(repository, state)

    configureSerialization()
    configureStatusPages()
    configureAuthentication(config.auth)
    configureRouting(config, service, AuthService(config.auth))
}
