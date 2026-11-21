package satellite

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func Initialize(ctx context.Context, mux *http.ServeMux, _ config.Config, logger *slog.Logger) (service.Shutdown, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	var leader elector = &soleElector{}
	if cfg.Lease.Name != "" {
		if leader, err = newInClusterElector(cfg.Lease, logger); err != nil {
			return nil, err
		}
	}
	store, err := OpenStore(ctx, cfg.DBPath)
	if err != nil {
		return nil, err
	}
	processor := newCountingProcessor(store)
	feed := newStreamer(cfg, processor, logger)
	mux.HandleFunc("GET /ready", readyHandler(leader, feed, processor))

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		leader.Run(ctx, feed.run)
	}()
	return func(shutdownCtx context.Context) error {
		cancel()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			go func() { <-done; _ = store.Close() }()
			return shutdownCtx.Err()
		}
		return store.Close()
	}, nil
}

// Every replica is ready to stand by; the body tells which one holds the feed.
func readyHandler(leader elector, feed *streamer, processor *countingProcessor) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		counts, next := processor.snapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"leading": leader.Leading(), "connected": feed.connected.Load(), "next_offset": next, "events": counts,
		})
	}
}
