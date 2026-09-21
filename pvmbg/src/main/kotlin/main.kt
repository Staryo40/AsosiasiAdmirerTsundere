package com.staryo

import com.staryo.config.AppConfig
import com.staryo.logging.Events
import com.staryo.plugins.configureAccessLog
import com.staryo.plugins.configureAuthentication
import com.staryo.plugins.configureCorrelationId
import com.staryo.plugins.configureResponseDelay
import com.staryo.plugins.configureSerialization
import com.staryo.plugins.configureStatusPages
import com.staryo.repository.InMemoryVolcanicReportRepository
import com.staryo.seed.LiveReportGenerator
import com.staryo.seed.ReportGenerator
import com.staryo.seed.ReportSeeder
import com.staryo.service.AuthService
import com.staryo.service.LoginRateLimiter
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
    configureCorrelationId()
    val config = AppConfig.from(this)
    val state = AppState(config.initialDelay)
    val repository = InMemoryVolcanicReportRepository()
    val generator = ReportGenerator()

    val seeder = ReportSeeder(repository, generator)
    config.initialSeedCount?.let(seeder::run) ?: seeder.runDefault()
    LiveReportGenerator(repository, generator, config.generatorInterval).start(this)

    Events.info(
        "startup",
        "host" to config.host,
        "port" to config.port,
        "seed_records" to repository.findSince(null).size,
        "generator_interval_ms" to config.generatorInterval.toMillis(),
        "delay_min_ms" to config.initialDelay.minMs,
        "delay_max_ms" to config.initialDelay.maxMs,
        "token_ttl_seconds" to config.auth.tokenTtl.seconds,
        "login_rate_limit_per_minute" to config.auth.loginRateLimitPerMinute,
        "log_level" to config.logging.level,
        "log_format" to config.logging.format,
    )

    val service = VolcanicReportService(repository, state)

    configureSerialization()
    configureStatusPages()
    configureAccessLog()
    configureResponseDelay(state)
    configureAuthentication(config.auth)
    configureRouting(config, state, service, AuthService(config.auth, state), LoginRateLimiter(config.auth))
}
