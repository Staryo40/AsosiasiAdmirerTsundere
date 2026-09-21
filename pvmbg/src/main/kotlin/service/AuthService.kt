package com.staryo.service

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.staryo.config.AuthConfig
import com.staryo.config.ClientCredentials
import com.staryo.enums.Scope
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
        val scope = authenticate(clientId, clientSecret)
            ?: throw UnauthorizedException("invalid client credentials")
        // Only the admin can still log in during an outage, otherwise nobody could switch it off again.
        if (state.outage && scope != Scope.ADMIN) throw ServiceUnavailableException("service is in outage")
        return issueToken(clientId, scope)
    }

    // Both clients are always checked, so timing does not reveal which id exists.
    private fun authenticate(clientId: String, clientSecret: String): Scope? {
        val isAdmin = matches(config.admin, clientId, clientSecret)
        val isReader = matches(config.reader, clientId, clientSecret)
        return when {
            isAdmin -> Scope.ADMIN
            isReader -> Scope.READ
            else -> null
        }
    }

    private fun matches(client: ClientCredentials, clientId: String, clientSecret: String) =
        constantTimeEquals(clientId, client.id) and constantTimeEquals(clientSecret, client.secret)

    private fun constantTimeEquals(a: String, b: String): Boolean {
        val sha256 = MessageDigest.getInstance("SHA-256")
        return MessageDigest.isEqual(sha256.digest(a.toByteArray()), sha256.digest(b.toByteArray()))
    }

    private fun issueToken(clientId: String, scope: Scope): TokenResponse {
        val now = Instant.now(clock)
        val token = JWT.create()
            .withIssuer(config.issuer)
            .withAudience(config.audience)
            .withSubject(clientId)
            .withClaim("scope", scope.claim)
            .withJWTId(UUID.randomUUID().toString())
            .withIssuedAt(now)
            .withExpiresAt(now.plus(config.tokenTtl))
            .sign(algorithm)
        return TokenResponse(token, "Bearer", config.tokenTtl.seconds)
    }
}
