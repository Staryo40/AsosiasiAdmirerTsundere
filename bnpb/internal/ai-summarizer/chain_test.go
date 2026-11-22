package aisummarizer

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

type fakeProvider struct {
	name  string
	reply string
	err   error
	calls int
}

func (p *fakeProvider) Name() string  { return p.name }
func (p *fakeProvider) Model() string { return p.name + "-model" }
func (p *fakeProvider) Complete(context.Context, string, string) (string, error) {
	p.calls++
	return p.reply, p.err
}

func sampleDigest() Digest {
	magnitude := 6.4
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	return buildDigest([]Event{
		{HazardID: "a", HazardType: "SEISMIC", Severity: "AWAS", AreaName: "Palu", Magnitude: &magnitude, ReceivedAt: now.Add(-12 * time.Minute)},
		{HazardID: "b", HazardType: "VOLCANIC", Severity: "SIAGA", AreaName: "Merapi", ReceivedAt: now.Add(-5 * time.Minute)},
	}, time.Hour, now)
}

func TestChainUsesTheFirstProviderThatAnswers(t *testing.T) {
	first := &fakeProvider{name: "anthropic", err: &callError{Status: http.StatusInternalServerError, Err: errors.New("boom")}}
	second := &fakeProvider{name: "openrouter", reply: "Tercatat 2 kejadian, termasuk gempa magnitudo 6,4 di Palu."}
	summary, attempts := newChain([]provider{first, second}, "").summarize(context.Background(), sampleDigest())
	if summary.Provider != "openrouter" || summary.Model != "openrouter-model" {
		t.Fatalf("summary from %q, want openrouter", summary.Provider)
	}
	if len(attempts) != 2 || attempts[0].Outcome != "error" || attempts[1].Outcome != "ok" {
		t.Fatalf("attempts = %+v", attempts)
	}
}

func TestChainFallsBackToTheTemplate(t *testing.T) {
	only := &fakeProvider{name: "anthropic", err: &callError{Status: http.StatusTooManyRequests, Err: errors.New("quota")}}
	summary, attempts := newChain([]provider{only}, "").summarize(context.Background(), sampleDigest())
	if summary.Provider != templateName || summary.Text == "" {
		t.Fatalf("summary = %+v, want the template", summary)
	}
	if attempts[0].Outcome != "quota_or_auth" {
		t.Fatalf("outcome = %q, want quota_or_auth", attempts[0].Outcome)
	}
}

func TestQuotaErrorSkipsTheProviderForTheCooldown(t *testing.T) {
	only := &fakeProvider{name: "anthropic", err: &callError{Status: http.StatusPaymentRequired, Err: errors.New("credit")}}
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	c := newChain([]provider{only}, "")
	c.now = func() time.Time { return now }
	c.summarize(context.Background(), sampleDigest())
	now = now.Add(9 * time.Minute)
	_, attempts := c.summarize(context.Background(), sampleDigest())
	if only.calls != 1 || attempts[0].Outcome != "cooling_down" {
		t.Fatalf("calls = %d, attempts = %+v; want the provider skipped", only.calls, attempts)
	}
	now = now.Add(2 * time.Minute)
	c.summarize(context.Background(), sampleDigest())
	if only.calls != 2 {
		t.Fatalf("calls = %d after the cooldown, want 2", only.calls)
	}
}

func TestReplyWithAnInventedNumberIsRejected(t *testing.T) {
	liar := &fakeProvider{name: "anthropic", reply: "Tercatat 7 gempa di Palu."}
	summary, attempts := newChain([]provider{liar}, "").summarize(context.Background(), sampleDigest())
	if summary.Provider != templateName || attempts[0].Outcome != "invalid" {
		t.Fatalf("summary from %q, attempts %+v; want the reply rejected", summary.Provider, attempts)
	}
}

func TestFaultModesAffectOnlyTheFirstProvider(t *testing.T) {
	for _, fault := range []string{"wrong", "down"} {
		first := &fakeProvider{name: "anthropic", reply: "Tercatat 2 kejadian."}
		second := &fakeProvider{name: "gemini", reply: "Tercatat 2 kejadian."}
		summary, _ := newChain([]provider{first, second}, fault).summarize(context.Background(), sampleDigest())
		if summary.Provider != "gemini" || first.calls != 0 {
			t.Fatalf("fault %s: summary from %q, first called %d times", fault, summary.Provider, first.calls)
		}
	}
}

func TestSlowFaultTimesOut(t *testing.T) {
	first := &fakeProvider{name: "anthropic"}
	c := newChain([]provider{first}, "slow")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	summary, attempts := c.summarize(ctx, sampleDigest())
	if summary.Provider != templateName || attempts[0].Outcome != "timeout" {
		t.Fatalf("summary from %q, attempts %+v", summary.Provider, attempts)
	}
}

func TestNoEventsNeedsNoProvider(t *testing.T) {
	first := &fakeProvider{name: "anthropic", reply: "x"}
	summary, _ := newChain([]provider{first}, "").summarize(context.Background(), buildDigest(nil, time.Hour, time.Now()))
	if first.calls != 0 || summary.Provider != templateName {
		t.Fatalf("calls = %d, provider = %q", first.calls, summary.Provider)
	}
}
