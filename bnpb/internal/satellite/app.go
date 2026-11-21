package satellite

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
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
	processor, err := newFireProcessor(ctx, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	feed := newStreamer(cfg, processor, logger)
	delivery := newSender(cfg, store, processor.wake, logger)
	if cfg.IngestToken == "" {
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelWarn, Event: "fire_delivery_disabled", Message: "BNPB_SATELLITE_INGEST_TOKEN is empty; fires stay in the outbox"})
	}
	mux.HandleFunc("GET /ready", readyHandler(leader, feed, processor, delivery))

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		leader.Run(ctx, func(leadCtx context.Context) {
			var group sync.WaitGroup
			group.Go(func() { feed.run(leadCtx) })
			if cfg.IngestToken != "" {
				group.Go(func() { delivery.run(leadCtx) })
			}
			group.Wait()
		})
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

// Every replica is ready to stand by; the body tells which one holds the feed and how far behind delivery is.
func readyHandler(leader elector, feed *streamer, processor *fireProcessor, delivery *sender) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		counts, next, stats := processor.snapshot()
		backlog, _ := processor.store.Backlog(request.Context())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"leading": leader.Leading(), "connected": feed.connected.Load(), "next_offset": next, "events": counts,
			"fires": map[string]any{"active": stats.ActiveFires, "singletons": stats.Singletons, "late": stats.Late,
				"late_updates": stats.LateUpdates, "orphans": stats.Orphans, "duplicates": stats.Duplicates, "watermark": stats.Watermark},
			"delivery": map[string]int64{"backlog": backlog, "sent": delivery.sent.Load(), "refused": delivery.refused.Load()},
		})
	}
}
