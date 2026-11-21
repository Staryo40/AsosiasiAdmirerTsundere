package satellite

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	sessionPollInterval = 10 * time.Second
	// An older reading would show a lag that no longer exists.
	sessionStaleAfter = time.Minute
)

var (
	storeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bnpb_satellite_store_latency_seconds",
		Help:    "Time from a feed event arriving to the Aggregator storing the fire change it caused.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
	})
	registerOnce sync.Once

	leadingDesc    = prometheus.NewDesc("bnpb_satellite_leading", "1 when this replica holds the feed lease.", nil, nil)
	connectedDesc  = prometheus.NewDesc("bnpb_satellite_connected", "1 while the feed stream is open.", nil, nil)
	nextOffsetDesc = prometheus.NewDesc("bnpb_satellite_next_offset", "Next feed offset this replica would read.", nil, nil)
	latestDesc     = prometheus.NewDesc("bnpb_satellite_feed_latest_offset", "Newest offset the feed reports in its session.", nil, nil)
	lagDesc        = prometheus.NewDesc("bnpb_satellite_feed_lag_events", "Feed events published but not yet processed.", nil, nil)
	feedEventsDesc = prometheus.NewDesc("bnpb_satellite_feed_events_total", "Feed events processed, by kind.", []string{"kind"}, nil)
	engineDesc     = prometheus.NewDesc("bnpb_satellite_engine_events_total", "Fire engine outcomes, including late data it did not apply.", []string{"outcome"}, nil)
	activeDesc     = prometheus.NewDesc("bnpb_satellite_active_fires", "Fires currently published as active.", nil, nil)
	singletonDesc  = prometheus.NewDesc("bnpb_satellite_singleton_fires", "Single detections not yet confirmed as a fire.", nil, nil)
	watermarkDesc  = prometheus.NewDesc("bnpb_satellite_watermark_timestamp_seconds", "Detection time before which arriving data counts as late.", nil, nil)
	backlogDesc    = prometheus.NewDesc("bnpb_satellite_outbox_backlog", "Fire changes waiting for delivery to the Aggregator.", nil, nil)
	deliveredDesc  = prometheus.NewDesc("bnpb_satellite_delivered_total", "Fire changes the Aggregator answered, by result.", []string{"result"}, nil)
)

type sessionPoller struct {
	cfg    Config
	client *http.Client
	logger *slog.Logger
	latest atomic.Int64
	polled atomic.Int64
}

func newSessionPoller(cfg Config, logger *slog.Logger) *sessionPoller {
	return &sessionPoller{cfg: cfg, client: &http.Client{Timeout: 5 * time.Second}, logger: logger}
}

func (poller *sessionPoller) run(ctx context.Context) {
	defer poller.polled.Store(0)
	failing := false
	ticker := time.NewTicker(sessionPollInterval)
	defer ticker.Stop()
	for {
		err := poller.poll(ctx)
		if ctx.Err() != nil {
			return
		}
		// Log only when the state changes so a long outage does not flood the log.
		if (err != nil) != failing {
			failing = err != nil
			if failing {
				logging.Event(ctx, poller.logger, logging.Record{Level: slog.LevelWarn, Event: "feed_session_unavailable", Message: "feed lag unknown"},
					"category", errorCategory(err))
			} else {
				logging.Event(ctx, poller.logger, logging.Record{Level: slog.LevelInfo, Event: "feed_session_available", Message: "feed lag known again"})
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (poller *sessionPoller) poll(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, poller.cfg.BaseURL.JoinPath("/v1/session").String(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+poller.cfg.Token)
	response, err := poller.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &feedError{status: response.StatusCode}
	}
	var session struct {
		LatestOffset *int64 `json:"latest_offset"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(nil, response.Body, 1<<16)).Decode(&session); err != nil {
		return err
	}
	if session.LatestOffset == nil || *session.LatestOffset < 0 {
		return errors.New("session without a valid latest_offset")
	}
	poller.latest.Store(*session.LatestOffset)
	poller.polled.Store(time.Now().UnixNano())
	return nil
}

func (poller *sessionPoller) reading() (int64, bool) {
	polled := poller.polled.Load()
	if polled == 0 || time.Since(time.Unix(0, polled)) > sessionStaleAfter {
		return 0, false
	}
	return poller.latest.Load(), true
}

type satelliteCollector struct {
	leader    elector
	feed      *streamer
	processor *fireProcessor
	delivery  *sender
	session   *sessionPoller
}

func registerMetrics(collector *satelliteCollector) {
	registerOnce.Do(func() {
		prometheus.MustRegister(storeLatency, collector)
	})
}

func (collector *satelliteCollector) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(collector, descs)
}

func (collector *satelliteCollector) Collect(metrics chan<- prometheus.Metric) {
	gauge := func(desc *prometheus.Desc, value float64, labels ...string) {
		metrics <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
	}
	counter := func(desc *prometheus.Desc, value int64, labels ...string) {
		metrics <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(value), labels...)
	}
	leading := collector.leader.Leading()
	gauge(leadingDesc, boolValue(leading))
	gauge(connectedDesc, boolValue(collector.feed.connected.Load()))

	counts, next, stats := collector.processor.snapshot()
	gauge(nextOffsetDesc, float64(next))
	// A standby reports no lag: the dashboard takes the max across replicas.
	lag := 0.0
	if latest, known := collector.session.reading(); known && leading {
		gauge(latestDesc, float64(latest))
		lag = float64(max(latest+1-next, 0))
	}
	gauge(lagDesc, lag)
	for _, kind := range []string{KindDetection, KindUpdate, KindRetract, KindHeartbeat, KindMalformed, KindUnusable} {
		counter(feedEventsDesc, counts[kind], kind)
	}
	for outcome, value := range map[string]int64{"detection": stats.Detections, "duplicate": stats.Duplicates, "late": stats.Late,
		"update": stats.Updates, "late_update": stats.LateUpdates, "retract": stats.Retracts, "orphan": stats.Orphans,
		"after_retract": stats.AfterRetract} {
		counter(engineDesc, value, outcome)
	}
	gauge(activeDesc, float64(stats.ActiveFires))
	gauge(singletonDesc, float64(stats.Singletons))
	if !stats.Watermark.IsZero() {
		gauge(watermarkDesc, float64(stats.Watermark.Unix()))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if backlog, err := collector.processor.store.Backlog(ctx); err == nil {
		gauge(backlogDesc, float64(backlog))
	}
	counter(deliveredDesc, collector.delivery.sent.Load(), "stored")
	counter(deliveredDesc, collector.delivery.refused.Load(), "refused")
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
