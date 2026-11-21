package consumer2

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var notificationLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bnpb_notification_latency_seconds",
	Help:    "From the Aggregator ingesting a hazard to consumer-2 recording its alert.",
	Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30, 60},
}, []string{"severity", "source"})

func observeAlertLatency(env envelope, recordedAt time.Time) {
	if env.HazardEvent.IngestedAt.IsZero() {
		return
	}
	// The two clocks run on different nodes; skew can make the difference negative.
	latency := max(recordedAt.Sub(env.HazardEvent.IngestedAt).Seconds(), 0)
	notificationLatency.WithLabelValues(env.HazardEvent.Severity, env.HazardEvent.Source).Observe(latency)
}
