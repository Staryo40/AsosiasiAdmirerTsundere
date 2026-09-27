package com.staryo

import com.staryo.config.AuthConfig
import com.staryo.config.ClientCredentials
import com.staryo.service.LoginRateLimiter
import com.staryo.service.TooManyRequestsException
import java.time.Duration
import java.time.Instant
import kotlin.test.*

class LoginRateLimiterTest {
    private val clock = MutableClock(Instant.parse("2026-09-21T10:00:00Z"))
    private val config = AuthConfig(
        jwtSecret = "unused-in-this-test",
        reader = ClientCredentials("reader", "reader-secret"),
        admin = ClientCredentials("admin", "admin-secret"),
        issuer = "pvmbg",
        audience = "pvmbg-api",
        tokenTtl = Duration.ofMinutes(5),
        loginRateLimitPerMinute = 3,
    )
    private val limiter = LoginRateLimiter(config, clock)

    private fun exhaust(clientId: String, address: String = "10.0.0.1") = repeat(3) { limiter.check(clientId, address) }

    @Test
    fun `a real client is limited after the configured attempts`() {
        exhaust("reader")
        assertFailsWith<TooManyRequestsException> { limiter.check("reader", "10.0.0.1") }
    }

    @Test
    fun `reader and admin have separate budgets`() {
        exhaust("reader")
        limiter.check("admin", "10.0.0.1")
    }

    @Test
    fun `a real client is limited across different IPs`() {
        exhaust("reader", "10.0.0.1")
        assertFailsWith<TooManyRequestsException> { limiter.check("reader", "10.0.0.2") }
    }

    @Test
    fun `inventing new ids from one IP does not dodge the limit`() {
        repeat(3) { limiter.check("fake-$it", "10.0.0.1") }
        assertFailsWith<TooManyRequestsException> { limiter.check("fake-new", "10.0.0.1") }
    }

    @Test
    fun `unknown ids from one IP do not use up a real clients budget`() {
        repeat(3) { limiter.check("fake-$it", "10.0.0.1") }
        limiter.check("reader", "10.0.0.1")
    }

    @Test
    fun `unknown ids from different IPs have separate budgets`() {
        repeat(3) { limiter.check("fake-$it", "10.0.0.1") }
        limiter.check("fake-x", "10.0.0.2")
    }

    @Test
    fun `retry-after is at least one second and rounds up`() {
        exhaust("reader")
        clock.advance(Duration.ofMillis(59_500))

        assertEquals(1, assertFailsWith<TooManyRequestsException> { limiter.check("reader", "10.0.0.1") }.retryAfterSeconds)
    }

    @Test
    fun `client can log in again after the window`() {
        exhaust("reader")
        clock.advance(Duration.ofMinutes(1))

        limiter.check("reader", "10.0.0.1")
    }
}
