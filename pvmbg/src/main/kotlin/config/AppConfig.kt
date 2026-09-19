package com.staryo.config

import io.ktor.server.application.*

data class AppConfig(
    val host: String,
    val port: Int,
) {
    val url: String get() = "http://$host:$port"

    companion object {
        fun from(application: Application): AppConfig {
            val config = application.environment.config
            return AppConfig(
                host = config.propertyOrNull("ktor.deployment.host")?.getString() ?: "localhost",
                port = config.property("ktor.deployment.port").getString().toInt(),
            )
        }
    }
}
