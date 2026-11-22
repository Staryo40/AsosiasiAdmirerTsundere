package aisummarizer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenRouterSendsTheKeyAndReadsTheReply(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body struct {
			Model    string              `json:"model"`
			Messages []map[string]string `json:"messages"`
		}
		_ = json.NewDecoder(request.Body).Decode(&body)
		if body.Model != "free-model" || len(body.Messages) != 2 || body.Messages[0]["role"] != "system" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"finish_reason":"stop","message":{"content":"Ringkasan."}}]}`))
	}))
	defer server.Close()
	p := &openRouterProvider{model: "free-model", key: "secret", url: server.URL, client: server.Client()}
	reply, err := p.Complete(context.Background(), "system", "user")
	if err != nil || reply != "Ringkasan." {
		t.Fatalf("reply = %q, err = %v", reply, err)
	}
}

func TestGeminiStatusBecomesACallError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/models/flash:generateContent" || request.Header.Get("x-goog-api-key") != "secret" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	p := &geminiProvider{model: "flash", key: "secret", url: server.URL + "/models/", client: server.Client()}
	_, err := p.Complete(context.Background(), "system", "user")
	var failure *callError
	if !errors.As(err, &failure) || failure.Status != http.StatusTooManyRequests {
		t.Fatalf("err = %v, want a 429 callError", err)
	}
}

func TestGeminiSafetyStopIsARefusal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"candidates":[{"finishReason":"SAFETY","content":{"parts":[]}}]}`))
	}))
	defer server.Close()
	p := &geminiProvider{model: "flash", key: "secret", url: server.URL + "/", client: server.Client()}
	if _, err := p.Complete(context.Background(), "system", "user"); !errors.Is(err, errRefused) {
		t.Fatalf("err = %v, want errRefused", err)
	}
}
