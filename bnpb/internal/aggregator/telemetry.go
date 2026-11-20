package aggregator

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var telemetryLatencyBounds = [...]time.Duration{5 * time.Millisecond, 25 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, time.Second, 5 * time.Second}

type telemetryKey struct {
	route  string
	method string
	status int
}

type telemetryValue struct {
	count    uint64
	duration time.Duration
	buckets  [len(telemetryLatencyBounds) + 1]uint64
}

type telemetrySeries struct {
	key    telemetryKey
	sample telemetryValue
}

// Telemetry records bounded-cardinality HTTP route, method, status and latency metrics.
type Telemetry struct {
	mu     sync.Mutex
	mux    *http.ServeMux
	values map[telemetryKey]telemetryValue
}

// NewTelemetry creates the per-server HTTP metrics collector.
func NewTelemetry() *Telemetry {
	return &Telemetry{values: make(map[telemetryKey]telemetryValue)}
}

// Register adds the bounded metrics scrape route and retains mux patterns for route labels.
func (telemetry *Telemetry) Register(mux *http.ServeMux) {
	telemetry.mux = mux
	mux.HandleFunc("GET /metrics", telemetry.metrics)
}

// Wrap records request metrics, including requests rejected before mux dispatch.
// Status is the final HTTP response code, except an aborted handler is a logical 500
// even if no response was sent or a different status was already committed.
func (telemetry *Telemetry) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started := time.Now()
		recorder := &telemetryResponseWriter{ResponseWriter: w}
		status := http.StatusInternalServerError
		defer func() {
			telemetry.observe(request, status, time.Since(started))
		}()
		next.ServeHTTP(recorder, request)
		status = recorder.status
		if status == 0 {
			status = http.StatusOK
		}
	})
}

func (telemetry *Telemetry) observe(request *http.Request, status int, elapsed time.Duration) {
	if status < 100 || status > 599 {
		status = 0
	}
	key := telemetryKey{route: telemetry.metricRoute(request), method: metricMethod(request.Method), status: status}
	telemetry.mu.Lock()
	sample := telemetry.values[key]
	sample.count++
	sample.duration += elapsed
	for index, bound := range telemetryLatencyBounds {
		if elapsed <= bound {
			sample.buckets[index]++
		}
	}
	sample.buckets[len(telemetryLatencyBounds)]++
	telemetry.values[key] = sample
	telemetry.mu.Unlock()
}

func (telemetry *Telemetry) metricRoute(request *http.Request) string {
	route := request.Pattern
	if route == "" && telemetry.mux != nil {
		_, route = telemetry.mux.Handler(request)
	}
	// Aggregator registers method-qualified patterns. ServeMux can return a raw
	// path for a redirect, so only use patterns with a method prefix.
	if route == "" || route[0] == '/' || strings.IndexAny(route, " \t") < 1 {
		return "unmatched"
	}
	return route
}

func metricMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

func (telemetry *Telemetry) metrics(w http.ResponseWriter, _ *http.Request) {
	series := telemetry.snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintln(w, "# HELP bnpb_http_requests_total Requests by final HTTP status (500 for aborted handlers regardless of wire status; 0 for out-of-range codes).")
	_, _ = fmt.Fprintln(w, "# TYPE bnpb_http_requests_total counter")
	for index := range series {
		entry := &series[index]
		_, _ = fmt.Fprintf(w, "bnpb_http_requests_total{route=%q,method=%q,status=\"%d\"} %d\n", entry.key.route, entry.key.method, entry.key.status, entry.sample.count)
	}
	_, _ = fmt.Fprintln(w, "# HELP bnpb_http_request_duration_seconds HTTP request duration by the same status as bnpb_http_requests_total.")
	_, _ = fmt.Fprintln(w, "# TYPE bnpb_http_request_duration_seconds histogram")
	for index := range series {
		series[index].writeHistogram(w)
	}
}

func (telemetry *Telemetry) snapshot() []telemetrySeries {
	telemetry.mu.Lock()
	series := make([]telemetrySeries, 0, len(telemetry.values))
	for key, sample := range telemetry.values {
		series = append(series, telemetrySeries{key: key, sample: sample})
	}
	telemetry.mu.Unlock()
	sort.Slice(series, func(i, j int) bool {
		left, right := series[i].key, series[j].key
		if left.route != right.route {
			return left.route < right.route
		}
		if left.method != right.method {
			return left.method < right.method
		}
		return left.status < right.status
	})
	return series
}

func (entry *telemetrySeries) writeHistogram(w http.ResponseWriter) {
	labels := fmt.Sprintf("route=%q,method=%q,status=\"%d\"", entry.key.route, entry.key.method, entry.key.status)
	for index, bound := range telemetryLatencyBounds {
		_, _ = fmt.Fprintf(w, "bnpb_http_request_duration_seconds_bucket{%s,le=%q} %d\n", labels, strconv.FormatFloat(bound.Seconds(), 'f', -1, 64), entry.sample.buckets[index])
	}
	_, _ = fmt.Fprintf(w, "bnpb_http_request_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels, entry.sample.buckets[len(telemetryLatencyBounds)])
	_, _ = fmt.Fprintf(w, "bnpb_http_request_duration_seconds_sum{%s} %s\n", labels, strconv.FormatFloat(entry.sample.duration.Seconds(), 'f', -1, 64))
	_, _ = fmt.Fprintf(w, "bnpb_http_request_duration_seconds_count{%s} %d\n", labels, entry.sample.count)
}

type telemetryResponseWriter struct {
	http.ResponseWriter
	status int
}

func (writer *telemetryResponseWriter) WriteHeader(status int) {
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		writer.ResponseWriter.WriteHeader(status)
		return
	}
	if writer.status != 0 {
		return
	}
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *telemetryResponseWriter) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(body)
}

func (writer *telemetryResponseWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}
