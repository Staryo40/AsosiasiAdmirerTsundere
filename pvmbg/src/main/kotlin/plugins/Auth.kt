package com.staryo.plugins

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.staryo.config.AuthConfig
import com.staryo.enums.Scope
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

const val PVMBG_JWT = "pvmbg-jwt"

fun JWTPrincipal.scope(): Scope? = Scope.fromClaim(payload.getClaim("scope").asString())

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
        // No principal means authentication already failed and the challenge answered with 401.
        val principal = call.principal<JWTPrincipal>() ?: return@on
        val granted = principal.scope()
        if (granted == null || !granted.includes(required)) {
            call.respond(HttpStatusCode.Forbidden, ErrorResponse("insufficient scope"))
        }
    }
}

// Requires a valid token (401 otherwise) whose scope includes `required` (403 otherwise).
fun Route.authorized(required: Scope, build: Route.() -> Unit): Route =
    authenticate(PVMBG_JWT) {
        install(ScopeGuard) { this.required = required }
        build()
    }
