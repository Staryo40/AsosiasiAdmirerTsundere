package com.staryo.service

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.staryo.config.AuthConfig
import com.staryo.enums.Scope
import com.staryo.logging.Events
import com.staryo.model.TokenResponse
import com.staryo.state.AppState
import java.security.MessageDigest
import java.time.Clock
import java.time.Instant
import java.util.UUID

class UnauthorizedException(message: String) : RuntimeException(message)

class AuthService(
    private val config: AuthConfig,
    private val state: AppState,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val algorithm = Algorithm.HMAC256(config.jwtSecret)

    fun login(clientId: String, clientSecret: String): TokenResponse {
        val attempt = authenticate(clientId, clientSecret)
        val scope = attempt.scope
        if (scope == null) {
            // The caller only ever sees a generic 401; the log says which part was wrong.
            // The id is logged as sent (capped in Events), so a mistyped secret in the id field would show up here.
            Events.warn("login_failed", "client_id" to clientId, "reason" to if (attempt.knownId) "wrong_secret" else "unknown_client")
            throw UnauthorizedException("invalid client credentials")
        }
        // Only the admin can still log in during an outage, otherwise nobody could switch it off again.
        if (state.outage && scope != Scope.ADMIN) {
            Events.warn("login_rejected", "client_id" to clientId, "reason" to "outage")
            throw ServiceUnavailableException("service is in outage")
        }
        return issueToken(clientId, scope)
    }

    private class Attempt(val scope: Scope?, val knownId: Boolean)

    // Every comparison is always made, so timing does not reveal which id exists.
    private fun authenticate(clientId: String, clientSecret: String): Attempt {
        val adminId = constantTimeEquals(clientId, config.admin.id)
        val adminSecret = constantTimeEquals(clientSecret, config.admin.secret)
        val readerId = constantTimeEquals(clientId, config.reader.id)
        val readerSecret = constantTimeEquals(clientSecret, config.reader.secret)
        val scope = when {
            adminId and adminSecret -> Scope.ADMIN
            readerId and readerSecret -> Scope.READ
            else -> null
        }
        return Attempt(scope, knownId = adminId or readerId)
    }

    private fun constantTimeEquals(a: String, b: String): Boolean {
        val sha256 = MessageDigest.getInstance("SHA-256")
        return MessageDigest.isEqual(sha256.digest(a.toByteArray()), sha256.digest(b.toByteArray()))
    }

    private fun issueToken(clientId: String, scope: Scope): TokenResponse {
        val now = Instant.now(clock)
        val tokenId = UUID.randomUUID().toString()
        val token = JWT.create()
            .withIssuer(config.issuer)
            .withAudience(config.audience)
            .withSubject(clientId)
            .withClaim("scope", scope.claim)
            .withJWTId(tokenId)
            .withIssuedAt(now)
            .withExpiresAt(now.plus(config.tokenTtl))
            .sign(algorithm)
        // The token id identifies the token in the logs; the token itself is never logged.
        Events.info("login_succeeded", "client_id" to clientId, "scope" to scope.claim, "token_id" to tokenId, "expires_in" to config.tokenTtl.seconds)
        return TokenResponse(token, "Bearer", config.tokenTtl.seconds)
    }
}
