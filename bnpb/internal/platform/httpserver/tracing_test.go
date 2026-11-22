package httpserver

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

func TestSpansChainAcrossTwoServices(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	logger := logging.New("trace-test", slog.LevelInfo, io.Discard)
	downstreamMux := http.NewServeMux()
	downstreamMux.HandleFunc("GET /internal/items", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	downstream := httptest.NewServer(Handler(downstreamMux, logger, 4))
	defer downstream.Close()

	client := httpclient.New(time.Second, logger)
	defer client.CloseIdleConnections()
	upstreamMux := http.NewServeMux()
	upstreamMux.HandleFunc("GET /v1/items", func(w http.ResponseWriter, r *http.Request) {
		request, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, downstream.URL+"/internal/items", nil)
		response, err := client.Do(request)
		if err != nil {
			t.Error(err)
			return
		}
		_ = response.Body.Close()
		w.WriteHeader(response.StatusCode)
	})
	response := httptest.NewRecorder()
	Handler(upstreamMux, logger, 4).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/items", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status %d", response.Code)
	}

	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		spans[span.Name()] = span
	}
	upstream, client2, server2 := spans["GET /v1/items"], spans["GET "+downstream.Listener.Addr().String()], spans["GET /internal/items"]
	if upstream == nil || client2 == nil || server2 == nil {
		t.Fatalf("missing spans, got %v", keys(spans))
	}
	if upstream.SpanKind() != trace.SpanKindServer || client2.SpanKind() != trace.SpanKindClient || server2.SpanKind() != trace.SpanKindServer {
		t.Fatal("unexpected span kinds")
	}
	traceID := upstream.SpanContext().TraceID()
	if client2.SpanContext().TraceID() != traceID || server2.SpanContext().TraceID() != traceID {
		t.Fatal("spans are not in one trace")
	}
	if client2.Parent().SpanID() != upstream.SpanContext().SpanID() || server2.Parent().SpanID() != client2.SpanContext().SpanID() {
		t.Fatal("spans do not chain server -> client -> server")
	}
}

func keys(spans map[string]sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(spans))
	for name := range spans {
		names = append(names, name)
	}
	return names
}
