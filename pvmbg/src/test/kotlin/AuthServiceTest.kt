package com.staryo

import com.auth0.jwt.JWT
import com.auth0.jwt.JWTVerifier
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.SignatureVerificationException
import com.staryo.config.AuthConfig
import com.staryo.config.ClientCredentials
import com.staryo.enums.Scope
import com.staryo.service.AuthService
import com.staryo.service.ServiceUnavailableException
import com.staryo.service.UnauthorizedException
import com.staryo.state.AppState
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.*

class AuthServiceTest {
    private val now = Instant.parse("2026-09-21T10:00:00Z")
    private val config = AuthConfig(
        jwtSecret = "unit-test-signing-key-unit-test-signing-key",
        reader = ClientCredentials("reader", "reader-secret"),
        admin = ClientCredentials("admin", "admin-secret"),
        issuer = "pvmbg",
        audience = "pvmbg-api",
        tokenTtl = Duration.ofMinutes(5),
        loginRateLimitPerMinute = 5,
    )
    private val clock = Clock.fixed(now, ZoneOffset.UTC)
    private val state = AppState()
    private val service = AuthService(config, state, clock)

    // Same fixed clock as the service, otherwise the token would be judged against the real time.
    private fun verifier(secret: String = config.jwtSecret): JWTVerifier =
        (JWT.require(Algorithm.HMAC256(secret))
            .withIssuer(config.issuer)
            .withAudience(config.audience) as JWTVerifier.BaseVerification)
            .build(clock)

    @Test
    fun `reader login issues a read token`() {
        val response = service.login("reader", "reader-secret")

        val jwt = verifier().verify(response.access_token)
        assertEquals("reader", jwt.subject)
        assertEquals("read", jwt.getClaim("scope").asString())
        assertEquals("Bearer", response.token_type)
    }

    @Test
    fun `admin login issues an admin token`() {
        val jwt = verifier().verify(service.login("admin", "admin-secret").access_token)

        assertEquals("admin", jwt.subject)
        assertEquals("admin", jwt.getClaim("scope").asString())
    }

    @Test
    fun `token lifetime matches the configured ttl`() {
        val response = service.login("reader", "reader-secret")
        val jwt = verifier().verify(response.access_token)

        assertEquals(now, jwt.issuedAtAsInstant)
        assertEquals(now.plus(Duration.ofMinutes(5)), jwt.expiresAtAsInstant)
        assertEquals(300, response.expires_in)
    }

    @Test
    fun `every token gets a unique id`() {
        val first = verifier().verify(service.login("reader", "reader-secret").access_token)
        val second = verifier().verify(service.login("reader", "reader-secret").access_token)

        assertNotEquals(first.id, second.id)
    }

    @Test
    fun `wrong secret and unknown id fail with the same message`() {
        val wrongSecret = assertFailsWith<UnauthorizedException> { service.login("reader", "nope") }
        val unknownId = assertFailsWith<UnauthorizedException> { service.login("nobody", "reader-secret") }

        assertEquals(wrongSecret.message, unknownId.message)
    }

    @Test
    fun `one clients secret does not work for the other client`() {
        assertFailsWith<UnauthorizedException> { service.login("reader", "admin-secret") }
        assertFailsWith<UnauthorizedException> { service.login("admin", "reader-secret") }
    }

    @Test
    fun `token signed with another key is rejected`() {
        val token = service.login("reader", "reader-secret").access_token

        assertFailsWith<SignatureVerificationException> { verifier("some-other-key-some-other-key-1234").verify(token) }
    }

    @Test
    fun `during an outage the reader cannot log in but the admin can`() {
        state.outage = true

        assertFailsWith<ServiceUnavailableException> { service.login("reader", "reader-secret") }
        assertEquals("admin", verifier().verify(service.login("admin", "admin-secret").access_token).subject)
    }

    @Test
    fun `during an outage wrong credentials are still 401 not 503`() {
        state.outage = true

        assertFailsWith<UnauthorizedException> { service.login("reader", "nope") }
        assertFailsWith<UnauthorizedException> { service.login("nobody", "nope") }
    }

    @Test
    fun `reader can log in again once the outage is over`() {
        state.outage = true
        assertFailsWith<ServiceUnavailableException> { service.login("reader", "reader-secret") }

        state.outage = false
        assertEquals("reader", verifier().verify(service.login("reader", "reader-secret").access_token).subject)
    }

    @Test
    fun `admin scope includes read but read does not include admin`() {
        assertTrue(Scope.ADMIN.includes(Scope.READ))
        assertTrue(Scope.ADMIN.includes(Scope.ADMIN))
        assertTrue(Scope.READ.includes(Scope.READ))
        assertFalse(Scope.READ.includes(Scope.ADMIN))
    }
}
