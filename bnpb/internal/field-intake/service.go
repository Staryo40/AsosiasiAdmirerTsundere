package fieldintake

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

const internalKeyHeader = "X-BNPB-Internal-Key"

func Initialize(ctx context.Context, mux *http.ServeMux, _ config.Config, logger *slog.Logger) (service.Shutdown, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	store, err := OpenStore(ctx, cfg.DBPath)
	if err != nil {
		return nil, err
	}
	mux.Handle("GET /internal/field-reports", internalOnly(cfg.InternalKey, pageHandler(store)))

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx, cfg, store, logger)
	}()
	return func(shutdownCtx context.Context) error {
		cancel()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			// Keep store ownership with the live worker if the shutdown budget expires.
			go func() { <-done; _ = store.Close() }()
			return shutdownCtx.Err()
		}
		return store.Close()
	}, nil
}

func internalOnly(key string, next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(key))
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		values := request.Header.Values(internalKeyHeader)
		provided := sha256.Sum256([]byte(request.Header.Get(internalKeyHeader)))
		switch {
		case len(values) == 0:
			writeError(w, http.StatusUnauthorized, "missing_credential")
		case len(values) != 1 || subtle.ConstantTimeCompare(expected[:], provided[:]) != 1:
			writeError(w, http.StatusForbidden, "invalid_credential")
		default:
			next.ServeHTTP(w, request)
		}
	})
}

func pageHandler(store *Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		after, limit := int64(0), 100
		var err error
		if raw := request.URL.Query().Get("after"); raw != "" {
			after, err = strconv.ParseInt(raw, 10, 64)
		}
		if raw := request.URL.Query().Get("limit"); raw != "" && err == nil {
			limit, err = strconv.Atoi(raw)
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_query")
			return
		}
		page, err := store.Page(request.Context(), after, limit)
		switch {
		case err == errInvalidPage:
			writeError(w, http.StatusBadRequest, "invalid_query")
		case err != nil:
			writeError(w, http.StatusServiceUnavailable, "storage_unavailable")
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(page)
		}
	})
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code}})
}
