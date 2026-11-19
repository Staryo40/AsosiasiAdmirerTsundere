package aggregator

import (
	"sync"
	"time"
)

// circuitBreaker limits outbound calls while a dependency is failing.
type circuitBreaker struct {
	mu              sync.Mutex
	failures        int
	threshold       int
	cooldown        time.Duration
	openUntil       time.Time
	halfOpenAllowed bool
}

func newCircuitBreaker(threshold int, cooldown time.Duration) *circuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	if cooldown < time.Second {
		cooldown = time.Second
	}
	return &circuitBreaker{threshold: threshold, cooldown: cooldown, halfOpenAllowed: true}
}

func (breaker *circuitBreaker) allow(now time.Time) bool {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.openUntil.IsZero() || now.After(breaker.openUntil) {
		return true
	}
	return breaker.halfOpenAllowed
}

func (breaker *circuitBreaker) success() {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	breaker.failures = 0
	breaker.openUntil = time.Time{}
	breaker.halfOpenAllowed = true
}

func (breaker *circuitBreaker) failure(now time.Time) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	breaker.failures++
	if breaker.failures >= breaker.threshold {
		breaker.openUntil = now.Add(breaker.cooldown)
		breaker.failures = 0
		breaker.halfOpenAllowed = false
		return
	}
	breaker.halfOpenAllowed = true
}

func (breaker *circuitBreaker) state(now time.Time) string {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.openUntil.IsZero() || now.After(breaker.openUntil) {
		return "closed"
	}
	if !breaker.halfOpenAllowed {
		return "open"
	}
	return "half_open"
}