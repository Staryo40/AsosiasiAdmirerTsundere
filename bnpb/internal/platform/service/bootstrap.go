// Package service wires platform components into a runnable BNPB service.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

// RouteRegistrar adds the application-owned routes and dependencies for a service.
type RouteRegistrar func(*http.ServeMux, config.Config, *slog.Logger)

// Shutdown joins workers and releases resources after HTTP drain, within ctx's remaining deadline.
type Shutdown func(context.Context) error

// Initializer must return cleanup for acquired resources, even on failure.
type Initializer func(context.Context, *http.ServeMux, config.Config, *slog.Logger) (Shutdown, error)

// Bootstrap loads configuration and runs a service or its Docker health probe.
func Bootstrap(service, addressVariable, defaultAddress string, register RouteRegistrar) int {
	return BootstrapApplication(service, addressVariable, defaultAddress, func(_ context.Context, mux *http.ServeMux, cfg config.Config, logger *slog.Logger) (Shutdown, error) {
		register(mux, cfg, logger)
		return nil, nil
	})
}

// BootstrapApplication runs probes before initialization.
func BootstrapApplication(service, addressVariable, defaultAddress string, initialize Initializer) int {
	cfg, err := config.Load(addressVariable, defaultAddress)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if len(os.Args) == 2 && os.Args[1] == "-healthcheck" {
		if err := httpserver.HealthCheck(cfg.HTTPAddr); err != nil {
			return 1
		}
		return 0
	}
	if len(os.Args) != 1 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: service [-healthcheck]")
		return 2
	}
	return runService(service, cfg, initialize)
}

func runService(service string, cfg config.Config, initialize Initializer) int {
	logger := logging.New(service, cfg.LogLevel, os.Stdout)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runApplication(ctx, applicationOptions{service: service, cfg: cfg, logger: logger}, initialize); err != nil {
		// Initializers may wrap driver errors containing credentials or DSNs.
		logging.Event(context.Background(), logger, logging.Record{Level: slog.LevelError, Event: "service_failed", Message: "service initialization, serving or cleanup failed"})
		return 1
	}
	return 0
}

func runApplication(ctx context.Context, options applicationOptions, initialize Initializer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg, logger := options.cfg, options.logger
	mux := http.NewServeMux()
	httpserver.RegisterHealth(mux, options.service)
	shutdown, err := initialize(ctx, mux, cfg, logger)
	if err != nil || ctx.Err() != nil {
		startupErr := errors.Join(err, ctx.Err())
		cancel()
		return errors.Join(startupErr, shutdownApplication(ctx, shutdown, cfg, nil))
	}
	options.shutdown = shutdown
	return serveApplication(ctx, cancel, mux, options)
}

type applicationOptions struct {
	service  string
	cfg      config.Config
	logger   *slog.Logger
	shutdown Shutdown
}

func serveApplication(ctx context.Context, cancel context.CancelFunc, mux *http.ServeMux, options applicationOptions) error {
	server := newApplicationServer(mux, options)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- httpserver.Run(ctx, server, options.logger, options.cfg.ShutdownTimeout) }()
	select {
	case serveErr := <-serveErrors:
		cancel()
		return errors.Join(serveErr, shutdownApplication(ctx, options.shutdown, options.cfg, nil))
	case <-ctx.Done():
		cancel()
		return shutdownApplication(ctx, options.shutdown, options.cfg, serveErrors)
	}
}

func newApplicationServer(mux *http.ServeMux, options applicationOptions) *http.Server {
	cfg, logger := options.cfg, options.logger
	return httpserver.New(httpserver.Options{
		Addr:              cfg.HTTPAddr,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}, httpserver.Handler(mux, logger, cfg.MaxInFlight), logger)
}

func shutdownApplication(ctx context.Context, shutdown Shutdown, cfg config.Config, serveErrors <-chan error) error {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()
	var serveErr error
	if serveErrors != nil {
		serveErr = <-serveErrors
	}
	if shutdown == nil {
		return serveErr
	}
	return errors.Join(serveErr, shutdown(shutdownCtx))
}
