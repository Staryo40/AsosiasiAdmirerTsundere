package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
)

const (
	testAuthKey       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testAggregatorKey = "gggggggggggggggggggggggggggggggg"
)

func dependencyURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func testPublicHandler(t *testing.T, auth, aggregator http.Handler) http.Handler {
	t.Helper()
	authServer := httptest.NewServer(auth)
	aggregatorServer := httptest.NewServer(aggregator)
	t.Cleanup(authServer.Close)
	t.Cleanup(aggregatorServer.Close)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	client := httpclient.New(time.Second, logger)
	t.Cleanup(client.CloseIdleConnections)
	service, err := newPublicService(clientConfig{
		authBaseURL: dependencyURL(t, authServer.URL), aggregatorBaseURL: dependencyURL(t, aggregatorServer.URL),
		authInternalKey: testAuthKey, aggregatorKey: testAggregatorKey,
	}, client, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	service.registerRoutes(mux)
	return httpserver.Handler(mux, logger, 64)
}

func authDependency(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get(authInternalKeyHeader) != testAuthKey || request.Header.Get(correlation.Header) == "" {
			t.Errorf("Auth credential or correlation missing")
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/internal/auth/login", "/internal/auth/refresh":
			_ = json.NewEncoder(w).Encode(tokenResponse{
				AccessToken: "issued-access", TokenType: "Bearer", ExpiresIn: 60,
				RefreshToken: "issued-refresh", RefreshExpiresIn: 28800,
			})
		case "/internal/auth/introspect":
			var input map[string]string
			_ = json.NewDecoder(request.Body).Decode(&input)
			role := strings.TrimSuffix(input["access_token"], "-token")
			if role != roleMedia && role != roleField && role != roleOps {
				_ = json.NewEncoder(w).Encode(map[string]bool{"active": false})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"active": true, "client_id": role + "-client", "role": role,
				"expires_at": "2026-10-07T12:01:00Z",
			})
		default:
			http.NotFound(w, request)
		}
	})
}

func sampleEvent() map[string]any {
	return map[string]any{
		"hazard_id": "haz-1", "source": "BMKG", "source_ref_id": "evt-1",
		"hazard_type": "SEISMIC", "severity": "AWAS", "area_name": "Palu",
		"latitude": -0.9, "longitude": 119.8,
		"occurred_at": "2026-10-07T11:59:00Z", "ingested_at": "2026-10-07T12:00:00Z",
		"attributes": map[string]any{"magnitude": 7.1},
	}
}

func aggregatorDependency(t *testing.T, calls *atomic.Int32) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get(aggregatorInternalKeyHeader) != testAggregatorKey || request.Header.Get(correlation.Header) == "" {
			t.Errorf("Aggregator credential or correlation missing")
		}
		if request.URL.Query().Has("view") {
			t.Error("public view parameter leaked to Aggregator")
		}
		w.Header().Set("Content-Type", "application/json")
		metadata := map[string]any{"BMKG": map[string]any{"status": "healthy"}}
		if request.URL.Path == "/internal/hazards" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{sampleEvent()}, "next_cursor": nil,
				"sources": metadata, "as_of": "2026-10-07T12:00:01Z",
			})
			return
		}
		if request.URL.Path == "/internal/hazards/haz-1" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"item": sampleEvent(), "sources": metadata, "as_of": "2026-10-07T12:00:01Z",
			})
			return
		}
		http.NotFound(w, request)
	})
}

func publicRequest(handler http.Handler, method, target, bearer, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Header.Set(correlation.Header, "public-test")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestPublicLoginAndRefreshAreProxiedToInternalAuth(t *testing.T) {
	var calls atomic.Int32
	handler := testPublicHandler(t, authDependency(t), aggregatorDependency(t, &calls))
	login := publicRequest(handler, http.MethodPost, "/auth/login", "", `{"client_id":"media","client_secret":"secret"}`)
	if login.Code != http.StatusOK || !strings.Contains(login.Body.String(), `"access_token":"issued-access"`) {
		t.Fatalf("login proxy failed: %d %s", login.Code, login.Body.String())
	}
	refresh := publicRequest(handler, http.MethodPost, "/auth/refresh", "", `{"refresh_token":"issued-refresh"}`)
	if refresh.Code != http.StatusOK || !strings.Contains(refresh.Body.String(), `"refresh_token":"issued-refresh"`) {
		t.Fatalf("refresh proxy failed: %d %s", refresh.Code, refresh.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatal("token routes unexpectedly called Aggregator")
	}
}

func TestMediaGetsSummaryAndCannotRequestFull(t *testing.T) {
	var calls atomic.Int32
	handler := testPublicHandler(t, authDependency(t), aggregatorDependency(t, &calls))
	summary := publicRequest(handler, http.MethodGet, "/v1/hazards?source=BMKG", "media-token", "")
	if summary.Code != http.StatusOK {
		t.Fatalf("summary returned %d: %s", summary.Code, summary.Body.String())
	}
	for _, forbidden := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
		if strings.Contains(summary.Body.String(), `"`+forbidden+`"`) {
			t.Fatalf("raw field %s leaked to Media: %s", forbidden, summary.Body.String())
		}
	}
	full := publicRequest(handler, http.MethodGet, "/v1/hazards?view=full", "media-token", "")
	if full.Code != http.StatusForbidden {
		t.Fatalf("Media full view returned %d", full.Code)
	}
	if calls.Load() != 1 {
		t.Fatalf("forbidden Media request reached Aggregator; calls=%d", calls.Load())
	}
}

func TestFieldAndOpsCanReadFullData(t *testing.T) {
	for _, role := range []string{roleField, roleOps} {
		t.Run(role, func(t *testing.T) {
			var calls atomic.Int32
			handler := testPublicHandler(t, authDependency(t), aggregatorDependency(t, &calls))
			response := publicRequest(handler, http.MethodGet, "/v1/hazards/haz-1?view=full", role+"-token", "")
			if response.Code != http.StatusOK {
				t.Fatalf("full detail returned %d: %s", response.Code, response.Body.String())
			}
			for _, required := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
				if !strings.Contains(response.Body.String(), `"`+required+`"`) {
					t.Fatalf("full response lacks %s: %s", required, response.Body.String())
				}
			}
		})
	}
}

func TestMissingInvalidAndInactiveTokensAreRejected(t *testing.T) {
	var calls atomic.Int32
	handler := testPublicHandler(t, authDependency(t), aggregatorDependency(t, &calls))
	missing := publicRequest(handler, http.MethodGet, "/v1/hazards", "", "")
	if missing.Code != http.StatusUnauthorized || !strings.Contains(missing.Body.String(), "missing_token") {
		t.Fatalf("missing token response: %d %s", missing.Code, missing.Body.String())
	}
	inactive := publicRequest(handler, http.MethodGet, "/v1/hazards", "unknown-token", "")
	if inactive.Code != http.StatusUnauthorized || !strings.Contains(inactive.Body.String(), "invalid_token") {
		t.Fatalf("inactive token response: %d %s", inactive.Code, inactive.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatal("unauthenticated request reached Aggregator")
	}
}

func TestInvalidQueriesAreRejectedBeforeAggregator(t *testing.T) {
	var calls atomic.Int32
	handler := testPublicHandler(t, authDependency(t), aggregatorDependency(t, &calls))
	for _, query := range []string{
		"unknown=x", "view=summary&view=full", "source=BMKG&hazard_type=VOLCANIC",
		"limit=0", "occurred_from=2026-10-08T00:00:00Z&occurred_to=2026-10-07T00:00:00Z",
	} {
		response := publicRequest(handler, http.MethodGet, "/v1/hazards?"+query, "field-token", "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("query %q returned %d: %s", query, response.Code, response.Body.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid query reached Aggregator; calls=%d", calls.Load())
	}
}

func TestAggregatorInternalAuthFailureBecomesPublic503(t *testing.T) {
	aggregator := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":"internal key rejected"}`)
	})
	handler := testPublicHandler(t, authDependency(t), aggregator)
	response := publicRequest(handler, http.MethodGet, "/v1/hazards", "field-token", "")
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "internal key") {
		t.Fatalf("internal failure leaked or mapped incorrectly: %d %s", response.Code, response.Body.String())
	}
}
