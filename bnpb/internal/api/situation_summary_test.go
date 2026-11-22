package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
)

const testSummarizerKey = "ssssssssssssssssssssssssssssssss"

func summaryHandler(t *testing.T, key string, summarizer http.Handler) http.Handler {
	t.Helper()
	authServer := httptest.NewServer(authDependency(t))
	summarizerServer := httptest.NewServer(summarizer)
	t.Cleanup(authServer.Close)
	t.Cleanup(summarizerServer.Close)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	client := httpclient.New(5*time.Second, logger)
	t.Cleanup(client.CloseIdleConnections)
	service, err := newPublicService(clientConfig{
		authBaseURL: dependencyURL(t, authServer.URL), aggregatorBaseURL: dependencyURL(t, authServer.URL),
		authInternalKey: testAuthKey, aggregatorKey: testAggregatorKey,
		summarizerBaseURL: dependencyURL(t, summarizerServer.URL), summarizerKey: key,
	}, client, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	service.registerRoutes(mux)
	return httpserver.Handler(mux, logger, 64)
}

func TestSituationSummaryIsForwardedToEveryRole(t *testing.T) {
	handler := summaryHandler(t, testSummarizerKey, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/internal/situation-summary" || request.Header.Get(aggregatorInternalKeyHeader) != testSummarizerKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `{"summary":"Ringkasan.","provider":"anthropic","ai_generated":true}`)
	}))
	for _, token := range []string{"media-token", "field-token", "ops-token"} {
		got := opsRequest(handler, http.MethodGet, "/v1/situation-summary", token, "")
		if got.Code != http.StatusOK || got.Body.String() != `{"summary":"Ringkasan.","provider":"anthropic","ai_generated":true}` {
			t.Fatalf("%s: HTTP %d %s", token, got.Code, got.Body.String())
		}
	}
	if got := opsRequest(handler, http.MethodGet, "/v1/situation-summary", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous: HTTP %d, want 401", got.Code)
	}
}

func TestSituationSummaryFailuresBecome503(t *testing.T) {
	slow := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	broken := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"code":"summary_not_ready"}}`)
	})
	for name, test := range map[string]struct {
		key     string
		handler http.Handler
	}{"unconfigured": {"", broken}, "not ready": {testSummarizerKey, broken}, "slow": {testSummarizerKey, slow}} {
		started := time.Now()
		got := opsRequest(summaryHandler(t, test.key, test.handler), http.MethodGet, "/v1/situation-summary", "field-token", "")
		if got.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: HTTP %d, want 503", name, got.Code)
		}
		if elapsed := time.Since(started); elapsed > summarizerTimeout+time.Second {
			t.Errorf("%s: answered after %s", name, elapsed)
		}
	}
}
