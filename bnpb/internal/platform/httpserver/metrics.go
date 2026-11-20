package httpserver

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsOnce   sync.Once
	httpRequests  *prometheus.CounterVec
	httpDuration  *prometheus.HistogramVec
	metricsReady  bool
)

// RegisterMetrics exposes GET /metrics and registers HTTP request collectors for service.
func RegisterMetrics(mux *http.ServeMux, service string) {
	metricsOnce.Do(func() {
		labels := prometheus.Labels{"service": service}
		httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "http_requests_total",
			Help:        "Total HTTP requests handled by the service.",
			ConstLabels: labels,
		}, []string{"method", "route", "code"})
		httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "HTTP request latency in seconds.",
			ConstLabels: labels,
			Buckets:     []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "route", "code"})
		prometheus.MustRegister(httpRequests, httpDuration)
		metricsReady = true
	})
	mux.Handle("GET /metrics", promhttp.Handler())
}

func observeRequest(r *http.Request, status int, duration time.Duration) {
	if !metricsReady || r.URL.Path == "/metrics" {
		return
	}
	code := strconv.Itoa(status)
	route := normalizeRoute(r.URL.Path)
	httpRequests.WithLabelValues(r.Method, route, code).Inc()
	httpDuration.WithLabelValues(r.Method, route, code).Observe(duration.Seconds())
}

// normalizeRoute keeps Prometheus cardinality low for path parameters.
func normalizeRoute(path string) string {
	if path == "" {
		return "/"
	}
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == "" {
			continue
		}
		if looksLikeID(part) {
			parts[i] = "{id}"
		}
	}
	return strings.Join(parts, "/")
}

func looksLikeID(part string) bool {
	if part == "" {
		return false
	}
	allDigits := true
	allHexish := true
	hasDigit := false
	hasHyphen := false
	for _, r := range part {
		isDigit := r >= '0' && r <= '9'
		isHex := isDigit || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') || r == '-'
		if isDigit {
			hasDigit = true
		} else {
			allDigits = false
		}
		if r == '-' {
			hasHyphen = true
		}
		if !isHex {
			allHexish = false
		}
	}
	if allDigits && len(part) >= 3 {
		return true
	}
	// Hazard IDs look like hz-7f3a91ab; UUIDs are hex with hyphens.
	if hasHyphen && hasDigit && len(part) >= 6 {
		return true
	}
	return allHexish && len(part) >= 8
}
