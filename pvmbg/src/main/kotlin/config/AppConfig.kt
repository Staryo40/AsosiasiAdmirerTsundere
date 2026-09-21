package com.staryo.config

import io.ktor.server.application.*
import java.time.Duration

data class ClientCredentials(val id: String, val secret: String)

data class AuthConfig(
    val jwtSecret: String,
    val reader: ClientCredentials,
    val admin: ClientCredentials,
    val issuer: String,
    val audience: String,
    val tokenTtl: Duration,
    val loginRateLimitPerMinute: Int,
)

data class AppConfig(
    val host: String,
    val port: Int,
    val initialSeedCount: Int?,
    val generatorInterval: Duration,
    val auth: AuthConfig,
) {
    val url: String get() = "http://$host:$port"

    companion object {
        fun from(application: Application): AppConfig {
            val config = application.environment.config
            fun str(path: String) = config.property(path).getString()

            return AppConfig(
                host = config.propertyOrNull("ktor.deployment.host")?.getString() ?: "localhost",
                port = str("ktor.deployment.port").toInt(),
                initialSeedCount = config.propertyOrNull("pvmbg.seed.initial")?.getString()?.toInt(),
                generatorInterval = Duration.ofMillis((str("pvmbg.generator.interval-seconds").toDouble() * 1000).toLong()),
                auth = AuthConfig(
                    jwtSecret = str("pvmbg.auth.jwt-secret"),
                    reader = ClientCredentials(str("pvmbg.auth.reader-client-id"), str("pvmbg.auth.reader-client-secret")),
                    admin = ClientCredentials(str("pvmbg.auth.admin-client-id"), str("pvmbg.auth.admin-client-secret")),
                    issuer = str("pvmbg.auth.issuer"),
                    audience = str("pvmbg.auth.audience"),
                    tokenTtl = Duration.ofSeconds(str("pvmbg.auth.token-ttl-seconds").toLong()),
                    loginRateLimitPerMinute = str("pvmbg.auth.login-rate-limit-per-minute").toInt(),
                ),
            )
        }
    }
}
