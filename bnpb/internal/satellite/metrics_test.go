package satellite

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func metricsFixture(t *testing.T, session http.HandlerFunc) (*satelliteCollector, *soleElector) {
	t.Helper()
	server := httptest.NewServer(session)
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	store, err := OpenStore(context.Background(), filepath.Join(t.TempDir(), "satellite.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	processor, err := newFireProcessor(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := Config{BaseURL: base, Token: "feed-token", IdleTimeout: time.Second}
	leader := &soleElector{}
	return &satelliteCollector{leader: leader, feed: newStreamer(cfg, processor, logger), processor: processor,
		delivery: newSender(cfg, store, make(chan struct{}), logger), session: newSessionPoller(cfg, logger)}, leader
}

func gathered(t *testing.T, collector prometheus.Collector, name string) []string {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var series []string
	for _, family := range families {
		if family.GetName() == name {
			for _, metric := range family.GetMetric() {
				series = append(series, strings.Join(strings.Fields(metric.String()), ""))
			}
		}
	}
	return series
}

func gauge(t *testing.T, collector prometheus.Collector, name string) string {
	t.Helper()
	if series := gathered(t, collector, name); len(series) > 0 {
		return series[0]
	}
	return ""
}

func TestFeedLagComesFromTheSessionWhileLeading(t *testing.T) {
	collector, leader := metricsFixture(t, func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/session" || request.Header.Get("Authorization") != "Bearer feed-token" {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"profile":"practice","speed":60,"latest_offset":41,"virtual_now":"2027-01-22T17:09:37Z"}`)
	})
	if err := collector.session.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := gauge(t, collector, "bnpb_satellite_feed_lag_events"); got != "gauge:{value:0}" {
		t.Fatalf("a standby reports no lag, got %s", got)
	}
	leader.leading.Store(true)
	if got := gauge(t, collector, "bnpb_satellite_feed_lag_events"); got != "gauge:{value:42}" {
		t.Fatalf("offsets 0..41 are unread, got %s", got)
	}
	if count := len(gathered(t, collector, "bnpb_satellite_feed_events_total")); count != 6 {
		t.Fatalf("every event kind is exported, got %d series", count)
	}
}

func TestARejectedSessionLeavesTheLagUnknown(t *testing.T) {
	collector, leader := metricsFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	leader.leading.Store(true)
	if err := collector.session.poll(context.Background()); err == nil {
		t.Fatal("a 429 must be an error")
	}
	if got := gauge(t, collector, "bnpb_satellite_feed_latest_offset"); got != "" {
		t.Fatalf("no reading means no latest offset, got %s", got)
	}
}
