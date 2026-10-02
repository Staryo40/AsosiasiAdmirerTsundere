package httpserver

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

// Options contains server settings; environment loading belongs to the caller.
type Options struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

// New creates a server with explicit timeouts and structured diagnostics.
func New(options Options, handler http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              options.Addr,
		Handler:           handler,
		ErrorLog:          log.New(serverLogWriter{logger: logger}, "", 0),
		ReadHeaderTimeout: options.ReadHeaderTimeout,
		ReadTimeout:       options.ReadTimeout,
		WriteTimeout:      options.WriteTimeout,
		IdleTimeout:       options.IdleTimeout,
	}
}

type serverLogWriter struct {
	logger *slog.Logger
}

func (w serverLogWriter) Write(message []byte) (int, error) {
	logging.Event(context.Background(), w.logger, logging.Record{Level: slog.LevelError, Event: "http_server_error", Message: "HTTP server diagnostic"})
	return len(message), nil
}
