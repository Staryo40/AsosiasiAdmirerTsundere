package com.staryo.plugins

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.JWTDecodeException
import com.staryo.config.AuthConfig
import com.staryo.enums.Scope
import com.staryo.logging.Events
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.time.Instant

const val PVMBG_JWT = "pvmbg-jwt"

fun JWTPrincipal.scope(): Scope? = Scope.fromClaim(payload.getClaim("scope").asString())

fun ApplicationCall.clientId(): String? = principal<JWTPrincipal>()?.payload?.subject

// Only for the log: the token is decoded without being verified, so an attacker could make a forged token look "expired" here. The request is rejected either way; this just says why.
internal fun rejectionReason(authorization: String?, now: Instant = Instant.now()): String {
    if (authorization.isNullOrBlank()) return "missing_token"
    val parts = authorization.trim().split(Regex("\\s+"), limit = 2)
    if (!parts[0].equals("Bearer", ignoreCase = true)) return "wrong_scheme"
    if (parts.size < 2) return "missing_token"
    val token = try {
        JWT.decode(parts[1])
    } catch (e: JWTDecodeException) {
        return "malformed_token"
    }
    val expiresAt = token.expiresAtAsInstant
    return if (expiresAt != null && expiresAt.isBefore(now)) "expired_token" else "invalid_token"
}

fun Application.configureAuthentication(config: AuthConfig) {
    install(Authentication) {
        jwt(PVMBG_JWT) {
            realm = "pvmbg"
            
            verifier(
                JWT.require(Algorithm.HMAC256(config.jwtSecret))
                    .withIssuer(config.issuer)
                    .withAudience(config.audience)
                    .build()
            )
            
            validate { credential ->
                val principal = JWTPrincipal(credential.payload)
                val allowed = when (credential.payload.subject) {
                    config.reader.id -> principal.scope() == Scope.READ
                    config.admin.id -> principal.scope() == Scope.ADMIN
                    else -> false
                }
                if (allowed) principal else null
            }
            
            challenge { _, realm ->
                Events.warn("auth_rejected", "reason" to rejectionReason(call.request.headers[HttpHeaders.Authorization]))
                call.response.header(HttpHeaders.WWWAuthenticate, "Bearer realm=\"$realm\"")
                call.respond(HttpStatusCode.Unauthorized, ErrorResponse("missing or invalid token"))
            }
        }
    }
}

class ScopeGuardConfig {
    var required: Scope = Scope.READ
}

val ScopeGuard = createRouteScopedPlugin("ScopeGuard", ::ScopeGuardConfig) {
    val required = pluginConfig.required
    on(AuthenticationChecked) { call ->
        val principal = call.principal<JWTPrincipal>() ?: return@on
        val granted = principal.scope()
        if (granted == null || !granted.includes(required)) {
            Events.warn("scope_denied", "client_id" to principal.payload.subject, "required" to required.claim, "granted" to granted?.claim)
            call.respond(HttpStatusCode.Forbidden, ErrorResponse("insufficient scope"))
        }
    }
}

fun Route.authorized(required: Scope, build: Route.() -> Unit): Route =
    authenticate(PVMBG_JWT) {
        install(ScopeGuard) { this.required = required }
        build()
    }
