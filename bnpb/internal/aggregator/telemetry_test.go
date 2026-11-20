package aggregator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
)

func telemetryHandler(mux *http.ServeMux, maxInFlight int) http.Handler {
	telemetry := NewTelemetry()
	telemetry.Register(mux)
	httpserver.RegisterHealth(mux, "bnpb-aggregator")
	return telemetry.Wrap(httpserver.Handler(mux, quietLogger(), maxInFlight))
}

func telemetryResponse(t *testing.T, handler http.Handler, method, path string, want int) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
	if response.Code != want {
		t.Fatalf("%s %s returned %d, want %d", method, path, response.Code, want)
	}
	return response
}

type blockedScrapeWriter struct {
	*httptest.ResponseRecorder
	release <-chan struct{}
}

func (writer blockedScrapeWriter) Write(body []byte) (int, error) {
	<-writer.release
	return writer.ResponseRecorder.Write(body)
}

func TestTelemetryAdmissionPreservesScrapeAndKubeProbeUnderLoad(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var active sync.WaitGroup
		defer func() {
			close(release)
			active.Wait()
		}()
		mux := http.NewServeMux()
		hold := func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Has("hold") {
				<-release
			}
			w.WriteHeader(http.StatusNoContent)
		}
		mux.HandleFunc("GET /internal/hazards/{hazard_id}", hold)
		mux.HandleFunc("GET /ready", hold)
		mux.HandleFunc("GET /readyz", hold)
		handler := telemetryHandler(mux, 1)
		occupy := func(method, path string, w http.ResponseWriter) {
			active.Go(func() { handler.ServeHTTP(w, httptest.NewRequest(method, path, nil)) })
		}
		occupy(http.MethodGet, "/internal/hazards/held?hold", httptest.NewRecorder())
		occupy(http.MethodGet, "/ready?hold", httptest.NewRecorder())
		occupy(http.MethodHead, "/ready?hold", httptest.NewRecorder())
		synctest.Wait()

		telemetryResponse(t, handler, http.MethodGet, "/internal/hazards/rejected-id", http.StatusTooManyRequests)
		telemetryResponse(t, handler, http.MethodGet, "/ready", http.StatusTooManyRequests)
		for _, path := range []string{"/metrics", "/readyz", "/health"} {
			telemetryResponse(t, handler, http.MethodPost, path, http.StatusTooManyRequests)
		}
		scrape := telemetryResponse(t, handler, http.MethodGet, "/metrics", http.StatusOK).Body.String()
		rejection := `bnpb_http_requests_total{route="GET /internal/hazards/{hazard_id}",method="GET",status="429"} 1` + "\n"
		if !strings.Contains(scrape, rejection) || strings.Contains(scrape, "rejected-id") {
			t.Fatalf("scrape did not report the bounded rejection series:\n%s", scrape)
		}
		telemetryResponse(t, handler, http.MethodHead, "/metrics", http.StatusOK)

		scrapeRelease := make(chan struct{})
		finishScrape := sync.OnceFunc(func() { close(scrapeRelease) })
		defer finishScrape()
		occupy(http.MethodGet, "/metrics", blockedScrapeWriter{ResponseRecorder: httptest.NewRecorder(), release: scrapeRelease})
		synctest.Wait()
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			telemetryResponse(t, handler, method, "/metrics", http.StatusTooManyRequests)
			telemetryResponse(t, handler, method, "/readyz", http.StatusNoContent)
			telemetryResponse(t, handler, method, "/health", http.StatusOK)
		}
		occupy(http.MethodGet, "/readyz?hold", httptest.NewRecorder())
		synctest.Wait()
		probe := telemetryResponse(t, handler, http.MethodHead, "/readyz", http.StatusServiceUnavailable)
		if probe.Body.Len() != 0 {
			t.Fatalf("saturated probe returned a body: %s", probe.Body)
		}
		finishScrape()
		synctest.Wait()
		telemetryResponse(t, handler, http.MethodGet, "/metrics", http.StatusOK)
	})
}

func telemetryPanic(handler http.Handler, response http.ResponseWriter) (recovered any) {
	defer func() { recovered = recover() }()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/hazards/private-id", nil))
	return nil
}

func TestTelemetryCountsAbortedRequestOnce(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name  string
		serve http.HandlerFunc
		flush bool
	}{
		{"explicit abort", func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }, false},
		{"panic after flush", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			if err := http.NewResponseController(w).Flush(); err != nil {
				panic(err)
			}
			panic("private panic details")
		}, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("GET /internal/hazards/{hazard_id}", scenario.serve)
			handler := telemetryHandler(mux, 1)
			response := httptest.NewRecorder()
			if got := telemetryPanic(handler, response); got != http.ErrAbortHandler {
				t.Fatalf("panic = %v, want ErrAbortHandler", got)
			}
			if scenario.flush && (!response.Flushed || response.Code != http.StatusAccepted) {
				t.Fatalf("response did not retain the flushed 202: status=%d flushed=%t", response.Code, response.Flushed)
			}
			scrape := telemetryResponse(t, handler, http.MethodGet, "/metrics", http.StatusOK).Body.String()
			labels := `{route="GET /internal/hazards/{hazard_id}",method="GET",status="500"} 1` + "\n"
			for _, metric := range []string{"bnpb_http_requests_total", "bnpb_http_request_duration_seconds_count"} {
				if strings.Count(scrape, metric+"{") != 1 || !strings.Contains(scrape, metric+labels) {
					t.Fatalf("expected one %s sample for the logical 500:\n%s", metric, scrape)
				}
			}
		})
	}
}

func TestTelemetryGroupsMetricFamiliesAndBoundsLabels(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/hazards/{hazard_id}", http.NotFound)
	mux.HandleFunc("CONNECT /redirect/{slug}/{$}", http.NotFound)
	handler := telemetryHandler(mux, 1)
	telemetryResponse(t, handler, http.MethodGet, "/internal/hazards/private-id", http.StatusNotFound)
	telemetryResponse(t, handler, "PRIVATE_METHOD", "/private-path", http.StatusNotFound)
	telemetryResponse(t, handler, http.MethodConnect, "/redirect/private-slug", http.StatusTemporaryRedirect)
	scrape := telemetryResponse(t, handler, http.MethodGet, "/metrics", http.StatusOK).Body.String()
	if strings.Contains(strings.ToLower(scrape), "private") {
		t.Fatalf("scrape leaked a raw method or path:\n%s", scrape)
	}

	series := []string{
		`route="GET /internal/hazards/{hazard_id}",method="GET",status="404"`,
		`route="unmatched",method="OTHER",status="307"`,
		`route="unmatched",method="OTHER",status="404"`,
	}
	lines := strings.Split(strings.TrimSuffix(scrape, "\n"), "\n")
	next := func(prefix string) {
		t.Helper()
		if len(lines) == 0 || !strings.HasPrefix(lines[0], prefix) {
			t.Fatalf("wanted next sample prefixed with %q in:\n%s", prefix, strings.Join(lines, "\n"))
		}
		lines = lines[1:]
	}
	next("# HELP bnpb_http_requests_total ")
	next("# TYPE bnpb_http_requests_total counter")
	for _, labels := range series {
		next("bnpb_http_requests_total{" + labels + "} 1")
	}
	next("# HELP bnpb_http_request_duration_seconds ")
	next("# TYPE bnpb_http_request_duration_seconds histogram")
	for _, labels := range series {
		for _, bound := range []string{"0.005", "0.025", "0.1", "0.5", "1", "5"} {
			next("bnpb_http_request_duration_seconds_bucket{" + labels + `,le="` + bound + `"} `)
		}
		next("bnpb_http_request_duration_seconds_bucket{" + labels + `,le="+Inf"} 1`)
		next("bnpb_http_request_duration_seconds_sum{" + labels + "} ")
		next("bnpb_http_request_duration_seconds_count{" + labels + "} 1")
	}
	if len(lines) != 0 {
		t.Fatalf("unexpected remaining metric samples:\n%s", strings.Join(lines, "\n"))
	}
}
