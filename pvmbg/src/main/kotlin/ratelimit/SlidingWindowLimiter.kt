package com.staryo.ratelimit

import java.time.Clock
import java.time.Duration
import java.time.Instant

sealed interface RateLimitDecision {
    data object Allowed : RateLimitDecision
    data class Limited(val retryAfter: Duration) : RateLimitDecision
}

// Allows at most `maxRequests` per key within any rolling `window`.
// Rejected attempts are not recorded, so a blocked caller is free again exactly one window after its oldest counted attempt.
class SlidingWindowLimiter(
    private val maxRequests: Int,
    private val window: Duration,
    private val clock: Clock = Clock.systemUTC(),
) {
    init {
        require(maxRequests >= 1) { "maxRequests must be at least 1" }
    }

    private val attempts = HashMap<String, ArrayDeque<Instant>>()
    private var lastSweep = clock.instant()

    @Synchronized
    fun tryAcquire(key: String): RateLimitDecision {
        val now = clock.instant()
        sweepIfDue(now)

        val recent = attempts.getOrPut(key) { ArrayDeque() }
        dropExpired(recent, now)
        if (recent.size >= maxRequests) {
            return RateLimitDecision.Limited(Duration.between(now, recent.first().plus(window)))
        }
        recent.addLast(now)
        return RateLimitDecision.Allowed
    }

    @Synchronized
    internal fun trackedKeys() = attempts.size

    private fun dropExpired(recent: ArrayDeque<Instant>, now: Instant) {
        while (recent.isNotEmpty() && !recent.first().plus(window).isAfter(now)) recent.removeFirst()
    }

    private fun sweepIfDue(now: Instant) {
        if (Duration.between(lastSweep, now) < window) return
        attempts.values.forEach { dropExpired(it, now) }
        attempts.values.removeIf { it.isEmpty() }
        lastSweep = now
    }
}
