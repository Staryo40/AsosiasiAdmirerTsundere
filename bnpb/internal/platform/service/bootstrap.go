// Package service wires platform components into a runnable BNPB service.
package service

import (
	"context"
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

// Bootstrap loads configuration and runs a service or its Docker health probe.
func Bootstrap(service, addressVariable, defaultAddress string, register RouteRegistrar) int {
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
	return runService(service, cfg, register)
}

func runService(service string, cfg config.Config, register RouteRegistrar) int {
	logger := logging.New(service, cfg.LogLevel, os.Stdout)
	mux := http.NewServeMux()
	httpserver.RegisterHealth(mux, service)
	register(mux, cfg, logger)
	server := httpserver.New(httpserver.Options{
		Addr:              cfg.HTTPAddr,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}, httpserver.Handler(mux, logger, cfg.MaxInFlight), logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := httpserver.Run(ctx, server, logger, cfg.ShutdownTimeout); err != nil {
		logging.Event(context.Background(), logger, logging.Record{Level: slog.LevelError, Event: "server_failed", Message: "HTTP server stopped unexpectedly"})
		return 1
	}
	return 0
}
