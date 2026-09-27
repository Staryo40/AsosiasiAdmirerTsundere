package com.staryo

import com.staryo.ratelimit.RateLimitDecision
import com.staryo.ratelimit.SlidingWindowLimiter
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.*

class MutableClock(var now: Instant) : Clock() {
    override fun instant(): Instant = now
    override fun getZone(): ZoneId = ZoneOffset.UTC
    override fun withZone(zone: ZoneId): Clock = this
    fun advance(duration: Duration) {
        now = now.plus(duration)
    }
}

class SlidingWindowLimiterTest {
    private val clock = MutableClock(Instant.parse("2026-09-21T10:00:00Z"))
    private val limiter = SlidingWindowLimiter(maxRequests = 3, window = Duration.ofMinutes(1), clock = clock)

    private fun assertAllowed(key: String) = assertEquals(RateLimitDecision.Allowed, limiter.tryAcquire(key))
    private fun limited(key: String) = assertIs<RateLimitDecision.Limited>(limiter.tryAcquire(key))

    @Test
    fun `allows up to the maximum then limits`() {
        repeat(3) { assertAllowed("a") }
        limited("a")
    }

    @Test
    fun `keys have independent budgets`() {
        repeat(3) { assertAllowed("a") }
        limited("a")
        assertAllowed("b")
    }

    @Test
    fun `retry-after is the time until the oldest attempt leaves the window`() {
        assertAllowed("a")
        clock.advance(Duration.ofSeconds(10))
        assertAllowed("a")
        assertAllowed("a")
        clock.advance(Duration.ofSeconds(20))

        assertEquals(Duration.ofSeconds(30), limited("a").retryAfter)
    }

    @Test
    fun `rejected attempts do not extend the block`() {
        repeat(3) { assertAllowed("a") }
        clock.advance(Duration.ofSeconds(30))
        repeat(10) { limited("a") }
        clock.advance(Duration.ofSeconds(30))

        assertAllowed("a")
    }

    @Test
    fun `window slides so old attempts free up one slot at a time`() {
        assertAllowed("a")
        clock.advance(Duration.ofSeconds(30))
        assertAllowed("a")
        assertAllowed("a")
        limited("a")

        clock.advance(Duration.ofSeconds(31))
        assertAllowed("a")
        limited("a")
    }

    @Test
    fun `an attempt is free again exactly one window later`() {
        repeat(3) { assertAllowed("a") }
        clock.advance(Duration.ofMinutes(1))

        assertAllowed("a")
    }

    @Test
    fun `idle keys are forgotten so memory does not grow`() {
        repeat(50) { assertAllowed("key-$it") }
        assertEquals(50, limiter.trackedKeys())

        clock.advance(Duration.ofMinutes(2))
        assertAllowed("fresh")

        assertEquals(1, limiter.trackedKeys())
    }

    @Test
    fun `a maximum below one is rejected`() {
        assertFailsWith<IllegalArgumentException> { SlidingWindowLimiter(0, Duration.ofMinutes(1)) }
    }
}
