package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

// Run serves until ctx is canceled, drains active requests, then force-closes on timeout.
func Run(ctx context.Context, server *http.Server, logger *slog.Logger, shutdownTimeout time.Duration) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	logging.Event(ctx, logger, logging.Record{Level: slog.LevelInfo, Event: "server_started", Message: "HTTP server started"})

	select {
	case serveErr := <-serveErrors:
		return normalizedServeError(serveErr)
	case <-ctx.Done():
	}
	return drainServer(server, logger, shutdownTimeout, serveErrors)
}

func drainServer(server *http.Server, logger *slog.Logger, timeout time.Duration, serveErrors <-chan error) error {
	logging.Event(context.Background(), logger, logging.Record{Level: slog.LevelInfo, Event: "shutdown_started", Message: "HTTP server shutdown started"})
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logging.Event(context.Background(), logger, logging.Record{Level: slog.LevelWarn, Event: "shutdown_forced", Message: "graceful HTTP shutdown did not complete"})
		if closeErr := server.Close(); closeErr != nil {
			return fmt.Errorf("force close: %w", closeErr)
		}
	}
	if err := normalizedServeError(<-serveErrors); err != nil {
		return err
	}
	logging.Event(context.Background(), logger, logging.Record{Level: slog.LevelInfo, Event: "shutdown_completed", Message: "HTTP server shutdown completed"})
	return nil
}

func normalizedServeError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve: %w", err)
}
