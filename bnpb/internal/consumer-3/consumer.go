package consumer3

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

// Initialize matches service.Initializer: it is called once at startup with
// the root context (canceled on shutdown signal) and returns a Shutdown that
// joins the consume loop and closes the store.
func Initialize(ctx context.Context, mux *http.ServeMux, _ config.Config, logger *slog.Logger) (service.Shutdown, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	store, err := OpenStore(ctx, cfg.DBPath)
	if err != nil {
		return nil, err
	}

	mux.HandleFunc("GET /stats", statsHandler(store))

	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx, cfg, store, logger)
	}()

	return func(shutdownCtx context.Context) error {
		select {
		case <-done:
		case <-shutdownCtx.Done():
		}
		return store.Close()
	}, nil
}

func statsHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		stats, err := store.Stats(request.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	}
}
