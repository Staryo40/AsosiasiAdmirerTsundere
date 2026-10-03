package aggregator

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

// Initialize acquires only local resources.
func Initialize(ctx context.Context, mux *http.ServeMux, platform config.Config, logger *slog.Logger) (service.Shutdown, error) {
	cfg, err := LoadConfig()
	if err == nil && cfg.ReadMaxInFlight >= platform.MaxInFlight {
		err = errors.New("BNPB_READ_MAX_IN_FLIGHT: must be below BNPB_MAX_IN_FLIGHT")
	}
	if err != nil {
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "configuration_invalid", Message: err.Error()})
		return nil, err
	}
	app := application{sources: newSourceClients(cfg, logger)}
	mux.HandleFunc("GET /ready", app.ready)
	return app.start(ctx), nil
}

type application struct {
	sources sourceClients
}

func (app application) ready(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "canonical store is not initialized", http.StatusServiceUnavailable)
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
			return nil
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
	}
}
