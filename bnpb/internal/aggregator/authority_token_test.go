package aggregator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestKubernetesAuthorityReloadsProjectedToken(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("old-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	serverCredential := "old-token"
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer "+serverCredential {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(configMapEnvelope{
				Metadata: struct {
					Name            string `json:"name"`
					Namespace       string `json:"namespace"`
					ResourceVersion string `json:"resourceVersion,omitempty"`
				}{Name: "authority", Namespace: "system", ResourceVersion: "rv-1"},
				Data: map[string]string{authorityConfigMapKey: `{"epoch":0,"owner":"","state":"closed","renewal_seq":0,"grant_set":[]}`},
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"rv-2"}}`))
	}))
	defer server.Close()
	client := &kubernetesAuthorityClient{baseURL: server.URL, namespace: "system", name: "authority", tokenFile: tokenFile, http: server.Client()}

	if _, _, err := client.Read(context.Background()); err != nil {
		t.Fatalf("initial Read: %v", err)
	}
	if err := os.WriteFile(tokenFile, []byte("new-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	serverCredential = "new-token"
	if _, _, err := client.Read(context.Background()); err != nil {
		t.Fatalf("Read after token rotation: %v", err)
	}
	if version, err := client.CompareAndSwap(context.Background(), "rv-1", authorityRecord{}); err != nil || version != "rv-2" {
		t.Fatalf("CAS after token rotation = (%q, %v)", version, err)
	}
	if requests != 3 {
		t.Fatalf("server received %d requests, want 3", requests)
	}
}

func TestKubernetesAuthorityFailsClosedWhenProjectedTokenInvalid(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("cached-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	client := &kubernetesAuthorityClient{baseURL: server.URL, namespace: "system", name: "authority", tokenFile: tokenFile, http: server.Client()}
	if _, _, err := client.Read(context.Background()); err == nil {
		t.Fatal("initial Read unexpectedly succeeded")
	}
	initialRequests := requests

	for _, test := range []struct {
		name    string
		content []byte
		remove  bool
	}{
		{name: "missing", remove: true},
		{name: "empty", content: []byte{}},
		{name: "malformed", content: []byte("not a bearer token")},
		{name: "control character", content: []byte("token\x00bytes")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.remove {
				if err := os.Remove(tokenFile); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(tokenFile, test.content, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := client.Read(context.Background()); err == nil {
				t.Fatal("Read unexpectedly succeeded")
			}
			if _, err := client.CompareAndSwap(context.Background(), "rv-1", authorityRecord{}); err == nil {
				t.Fatal("CompareAndSwap unexpectedly succeeded")
			}
			if requests != initialRequests {
				t.Fatalf("invalid token reached server: request count %d, want %d", requests, initialRequests)
			}
		})
	}
}
