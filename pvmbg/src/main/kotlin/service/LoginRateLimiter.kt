package com.staryo.service

import com.staryo.config.AuthConfig
import com.staryo.logging.Events
import com.staryo.ratelimit.RateLimitDecision
import com.staryo.ratelimit.SlidingWindowLimiter
import java.time.Clock
import java.time.Duration
import kotlin.math.max

class TooManyRequestsException(val retryAfterSeconds: Long) : RuntimeException("too many login attempts")

class LoginRateLimiter(
    private val config: AuthConfig,
    clock: Clock = Clock.systemUTC(),
) {
    private val limiter = SlidingWindowLimiter(config.loginRateLimitPerMinute, Duration.ofMinutes(1), clock)

    // every different client id gets a different rate limit budget
    // unknown client id will get flagged by their ip
    fun check(clientId: String, remoteAddress: String) {
        val key = if (clientId == config.reader.id || clientId == config.admin.id) "client:$clientId" else "ip:$remoteAddress"
        val decision = limiter.tryAcquire(key)
        if (decision is RateLimitDecision.Limited) {
            val seconds = max(1, (decision.retryAfter.toMillis() + 999) / 1000)
            // The key is either a real client id or an IP address, never an id the caller invented.
            Events.warn("rate_limited", "key" to key, "retry_after_seconds" to seconds)
            throw TooManyRequestsException(seconds)
        }
    }
}
