package consumer2

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func latencySamples(t *testing.T, severity, source string) (uint64, float64) {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(notificationLatency)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["severity"] == severity && labels["source"] == source {
				return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum()
			}
		}
	}
	return 0, 0
}

func TestAlertLatencyRunsFromIngestToRecord(t *testing.T) {
	notificationLatency.Reset()
	ingested := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	env := envelope{}
	env.HazardEvent.Source, env.HazardEvent.Severity, env.HazardEvent.IngestedAt = "SENSOR_PANTAI", "AWAS", ingested

	observeAlertLatency(env, ingested.Add(1500*time.Millisecond))
	if count, sum := latencySamples(t, "AWAS", "SENSOR_PANTAI"); count != 1 || sum != 1.5 {
		t.Fatalf("count=%d sum=%v, want one 1.5s sample", count, sum)
	}

	observeAlertLatency(env, ingested.Add(-2*time.Second))
	if count, sum := latencySamples(t, "AWAS", "SENSOR_PANTAI"); count != 2 || sum != 1.5 {
		t.Fatalf("count=%d sum=%v, want a clock-skewed sample recorded as zero", count, sum)
	}

	env.HazardEvent.IngestedAt = time.Time{}
	observeAlertLatency(env, ingested)
	if count, _ := latencySamples(t, "AWAS", "SENSOR_PANTAI"); count != 2 {
		t.Fatalf("count=%d, want a message without ingested_at skipped", count)
	}
}
