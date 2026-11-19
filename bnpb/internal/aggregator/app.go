package aggregator

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func Initialize(ctx context.Context, mux *http.ServeMux, platform config.Config, logger *slog.Logger) (service.Shutdown, error) {
	cfg, err := LoadConfig()
	if err == nil && cfg.ReadMaxInFlight >= platform.MaxInFlight {
		err = errors.New("BNPB_READ_MAX_IN_FLIGHT: must be below BNPB_MAX_IN_FLIGHT")
	}
	if err != nil {
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "configuration_invalid", Message: err.Error()})
		return nil, err
	}
	store, err := OpenStore(ctx, cfg, logger)
	if err != nil {
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "canonical_store_initialization_failed", Message: err.Error()})
		return nil, err
	}
	repair, err := newRepairWorker(store, cfg.ShardRepairCandidates, cfg.Authority.Kubernetes)
	if err != nil {
		store.Close()
		return nil, err
	}
	app := application{sources: newSourceClients(cfg, logger), store: store}
	mux.HandleFunc("GET /ready", app.ready)
	registerSnapshotRoutes(mux, store, cfg)
	registerAuditRoutes4(mux, store, cfg)
	dispatcher := &outboxDispatcher{store: store, publisher: newAMQPPublisher(cfg), cfg: cfg, logger: logger}
	workers, err := app.ownerWorkers(ctx, cfg, logger, dispatcher.run)
	if err != nil {
		app.sources.closeIdleConnections()
		store.Close()
		return nil, err
	}
	if repair != nil {
		workers = append(workers, func(ctx context.Context) { runRepair(ctx, logger, repair) })
	}
	if cfg.Catalog.Token != "" {
		registerCatalogRoutes(mux, store, cfg, store.authority)
	}
	return app.start(ctx, workers...), nil
}

type application struct {
	sources sourceClients
	store   *Store
}

func (app application) ready(w http.ResponseWriter, request *http.Request) {
	if app.store == nil || app.store.Ready(request.Context()) != nil {
		http.Error(w, "canonical store is unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Fix the worker set before shutdown can wait.
func (app application) start(ctx context.Context, workers ...func(context.Context)) service.Shutdown {
	ctx, cancel := context.WithCancel(ctx)
	var group sync.WaitGroup
	for _, worker := range workers {
		group.Go(func() { worker(ctx) })
	}
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		group.Wait()
		app.sources.closeIdleConnections()
		close(done)
	}()
	return func(shutdownCtx context.Context) error {
		cancel()
		select {
		case <-done:
			// The platform invokes cleanup after HTTP drain
			if app.store != nil {
				app.store.Close()
			}
			return nil
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
	}
}

func runRepair(ctx context.Context, logger *slog.Logger, reconcile func(context.Context) error) {
	const interval = 5 * time.Second
	const repairCycleBudget = 20 * time.Second
	for ctx.Err() == nil {
		cycle, cancel := context.WithTimeout(ctx, repairCycleBudget)
		err := reconcile(cycle)
		cancel()
		if err != nil && ctx.Err() == nil {
			logger.WarnContext(ctx, "standby repair reconciliation did not complete", "event", "standby_repair_reconcile_failed")
		}
		if !waitPoll(ctx, interval) {
			return
		}
	}
}
