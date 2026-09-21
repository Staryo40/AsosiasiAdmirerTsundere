package com.staryo.config

import com.staryo.state.DelayRange
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

// Logback reads these two settings itself, before the app starts. It would silently treat a bad level as DEBUG
// and lose every line on a bad format, so they are checked again here to fail startup on a typo instead.
data class LoggingConfig(val level: String, val format: String) {
    init {
        require(level.uppercase() in LEVELS) { "Invalid log level PVMBG_LOG_LEVEL=$level. Use one of: ${LEVELS.joinToString()}" }
        require(format in FORMATS) { "Invalid log format PVMBG_LOG_FORMAT=$format. Use one of (lowercase): ${FORMATS.joinToString()}" }
    }

    companion object {
        val LEVELS = listOf("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "OFF")
        val FORMATS = listOf("json", "text")
    }
}

data class AppConfig(
    val host: String,
    val port: Int,
    val initialSeedCount: Int?,
    val generatorInterval: Duration,
    val initialDelay: DelayRange,
    val logging: LoggingConfig,
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
                initialDelay = delayRange(str("pvmbg.delay.min-ms"), str("pvmbg.delay.max-ms")),
                logging = LoggingConfig(str("pvmbg.logging.level"), str("pvmbg.logging.format")),
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

        private fun delayRange(min: String, max: String): DelayRange =
            try {
                DelayRange(min.toLong(), max.toLong())
            } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException(
                    "Invalid response delay: PVMBG_RESPONSE_DELAY_MIN_MS=$min, PVMBG_RESPONSE_DELAY_MAX_MS=$max. ${e.message}", e
                )
            }
    }
}
