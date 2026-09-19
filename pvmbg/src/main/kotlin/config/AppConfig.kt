package com.staryo.config

import io.ktor.server.application.*
import java.time.Duration

data class AppConfig(
    val host: String,
    val port: Int,
    val initialSeedCount: Int?,
    val generatorInterval: Duration,
) {
    val url: String get() = "http://$host:$port"

    companion object {
        fun from(application: Application): AppConfig {
            val config = application.environment.config
            return AppConfig(
                host = config.propertyOrNull("ktor.deployment.host")?.getString() ?: "localhost",
                port = config.property("ktor.deployment.port").getString().toInt(),
                initialSeedCount = config.propertyOrNull("pvmbg.seed.initial")?.getString()?.toInt(),
                generatorInterval = Duration.ofMillis(
                    (config.property("pvmbg.generator.interval-seconds").getString().toDouble() * 1000).toLong()
                ),
            )
        }
    }
}
