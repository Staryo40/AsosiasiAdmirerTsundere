package auth

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
)

func testHandler(t *testing.T) (*Service, http.Handler) {
	t.Helper()
	service, _ := testService(t)
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return service, httpserver.Handler(mux, logger, 64)
}

func authRequest(t *testing.T, handler http.Handler, path, body string, keys ...string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(correlation.Header, "auth-test")
	for _, key := range keys {
		request.Header.Add(InternalKeyHeader, key)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestInternalRoutesAreNotPublicAliases(t *testing.T) {
	_, handler := testHandler(t)
	response := authRequest(t, handler, "/auth/login", `{}`, strings.Repeat("i", 32))
	if response.Code != http.StatusNotFound {
		t.Fatalf("public Auth alias unexpectedly exists: %d", response.Code)
	}
}

func TestInternalCredentialIsRequiredBeforeBodyValidation(t *testing.T) {
	_, handler := testHandler(t)
	missing := authRequest(t, handler, "/internal/auth/login", `{}`)
	if missing.Code != http.StatusUnauthorized {
		t.Fatalf("missing internal key returned %d", missing.Code)
	}
	wrong := authRequest(t, handler, "/internal/auth/login", `{}`, strings.Repeat("x", 32))
	if wrong.Code != http.StatusForbidden {
		t.Fatalf("wrong internal key returned %d", wrong.Code)
	}
	repeated := authRequest(t, handler, "/internal/auth/login", `{}`, strings.Repeat("i", 32), strings.Repeat("i", 32))
	if repeated.Code != http.StatusForbidden {
		t.Fatalf("repeated internal key returned %d", repeated.Code)
	}
}

func TestLoginAndIntrospectionHTTPFlow(t *testing.T) {
	_, handler := testHandler(t)
	loginBody := `{"client_id":"media","client_secret":"` + strings.Repeat("m", 32) + `"}`
	login := authRequest(t, handler, "/internal/auth/login", loginBody, strings.Repeat("i", 32))
	if login.Code != http.StatusOK {
		t.Fatalf("login returned %d: %s", login.Code, login.Body.String())
	}
	if login.Header().Get("Cache-Control") != "no-store" || login.Header().Get(correlation.Header) != "auth-test" {
		t.Fatalf("security or correlation headers missing: %v", login.Header())
	}
	var tokens TokenResponse
	if err := json.NewDecoder(login.Body).Decode(&tokens); err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(introspectRequest{AccessToken: tokens.AccessToken})
	if err != nil {
		t.Fatal(err)
	}
	introspection := authRequest(t, handler, "/internal/auth/introspect", string(payload), strings.Repeat("i", 32))
	if introspection.Code != http.StatusOK {
		t.Fatalf("introspection returned %d: %s", introspection.Code, introspection.Body.String())
	}
	var result Introspection
	if err := json.NewDecoder(introspection.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if !result.Active || result.Role != RoleMedia || result.ClientID != "media" {
		t.Fatalf("unexpected introspection: %+v", result)
	}
}

func TestInvalidLoginIsGenericAndDoesNotLeakSecrets(t *testing.T) {
	_, handler := testHandler(t)
	secret := strings.Repeat("x", 32)
	response := authRequest(t, handler, "/internal/auth/login", `{"client_id":"unknown","client_secret":"`+secret+`"}`, strings.Repeat("i", 32))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid login returned %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "unknown") || strings.Contains(response.Body.String(), secret) {
		t.Fatalf("credential details leaked: %s", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"correlation_id":"auth-test"`) {
		t.Fatalf("correlation ID missing: %s", response.Body.String())
	}
}

func TestHandlersRejectWrongContentTypeAndUnknownJSON(t *testing.T) {
	_, handler := testHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/internal/auth/login", bytes.NewBufferString(`{}`))
	request.Header.Set(InternalKeyHeader, strings.Repeat("i", 32))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("missing content type returned %d", response.Code)
	}

	unknown := authRequest(t, handler, "/internal/auth/refresh", `{"refresh_token":"x","extra":true}`, strings.Repeat("i", 32))
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown JSON field returned %d", unknown.Code)
	}
}
