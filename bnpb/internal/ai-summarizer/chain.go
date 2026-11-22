package aisummarizer

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	callTimeout   = 10 * time.Second
	chainTimeout  = 30 * time.Second
	quotaCooldown = 10 * time.Minute
	shortCooldown = 30 * time.Second
	templateName  = "template"
)

var summaryRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bnpb_summary_requests_total",
	Help: "Situation-summary attempts per provider and outcome.",
}, []string{"provider", "outcome"})

type attempt struct {
	Provider string `json:"provider"`
	Outcome  string `json:"outcome"`
}

// chain tries each provider in order, skipping any still cooling down after a failure.
type chain struct {
	providers []provider
	now       func() time.Time

	mu       sync.Mutex
	cooldown map[string]time.Time
	fault    string
}

func newChain(providers []provider, fault string) *chain {
	return &chain{providers: providers, now: time.Now, cooldown: map[string]time.Time{}, fault: fault}
}

func (c *chain) setFault(fault string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fault = fault
	// A fault change starts a fresh experiment.
	c.cooldown = map[string]time.Time{}
}

func (c *chain) currentFault() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fault
}

func (c *chain) cooling(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now().Before(c.cooldown[name])
}

func (c *chain) coolDown(name string, period time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cooldown[name] = c.now().Add(period)
}

// summarize always returns a summary: the first accepted reply, or the template.
func (c *chain) summarize(ctx context.Context, digest Digest) (Summary, []attempt) {
	summary := Summary{EventCount: digest.Total, DigestHash: digest.hash(), GeneratedAt: c.now()}
	var attempts []attempt
	if digest.Total > 0 {
		ctx, cancel := context.WithTimeout(ctx, chainTimeout)
		defer cancel()
		fault := c.currentFault()
		for index, candidate := range c.providers {
			if c.cooling(candidate.Name()) {
				attempts = append(attempts, attempt{candidate.Name(), "cooling_down"})
				continue
			}
			text, outcome := c.call(ctx, candidate, digest, index == 0 && fault != "", fault)
			summaryRequests.WithLabelValues(candidate.Name(), outcome).Inc()
			attempts = append(attempts, attempt{candidate.Name(), outcome})
			if outcome == "ok" {
				summary.Text, summary.Provider, summary.Model = text, candidate.Name(), candidate.Model()
				return summary, attempts
			}
		}
	}
	summaryRequests.WithLabelValues(templateName, "ok").Inc()
	summary.Text, summary.Provider, summary.Model = templateSummary(digest), templateName, ""
	return summary, attempts
}

func (c *chain) call(ctx context.Context, candidate provider, digest Digest, faulty bool, fault string) (string, string) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var reply string
	var err error
	switch {
	case faulty && fault == "slow":
		<-ctx.Done()
		err = ctx.Err()
	case faulty && fault == "down":
		err = &callError{Status: http.StatusServiceUnavailable, Err: errors.New("fault injected")}
	case faulty && fault == "wrong":
		// Numbers that are not in the data, as a hallucinating model would write.
		reply = "Tercatat 9999 gempa dengan magnitudo 12,5 di seluruh Indonesia."
	default:
		reply, err = candidate.Complete(ctx, systemPrompt, userPrompt(digest))
	}
	if err == nil {
		if reply, err = checkReply(reply, digest); err == nil {
			return reply, "ok"
		}
		c.coolDown(candidate.Name(), shortCooldown)
		return "", "invalid"
	}
	var failure *callError
	switch {
	case errors.As(err, &failure) && quotaStatus(failure.Status):
		c.coolDown(candidate.Name(), quotaCooldown)
		return "", "quota_or_auth"
	case errors.Is(err, errRefused):
		c.coolDown(candidate.Name(), shortCooldown)
		return "", "refused"
	case errors.Is(err, context.DeadlineExceeded):
		c.coolDown(candidate.Name(), shortCooldown)
		return "", "timeout"
	default:
		c.coolDown(candidate.Name(), shortCooldown)
		return "", "error"
	}
}

func quotaStatus(status int) bool {
	return status == http.StatusUnauthorized || status == http.StatusPaymentRequired ||
		status == http.StatusForbidden || status == http.StatusTooManyRequests
}
