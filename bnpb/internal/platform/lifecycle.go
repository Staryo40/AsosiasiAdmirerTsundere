package platform

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// RunServer serves until ctx is canceled, drains active requests, then force-closes on timeout.
func RunServer(ctx context.Context, server *http.Server, logger *slog.Logger, shutdownTimeout time.Duration) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	logEvent(ctx, logger, logRecord{slog.LevelInfo, "server_started", "HTTP server started"})

	select {
	case serveErr := <-serveErrors:
		return normalizedServeError(serveErr)
	case <-ctx.Done():
	}
	return drainServer(server, logger, shutdownTimeout, serveErrors)
}

func drainServer(server *http.Server, logger *slog.Logger, timeout time.Duration, serveErrors <-chan error) error {
	logEvent(context.Background(), logger, logRecord{slog.LevelInfo, "shutdown_started", "HTTP server shutdown started"})
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logEvent(context.Background(), logger, logRecord{slog.LevelWarn, "shutdown_forced", "graceful HTTP shutdown did not complete"})
		if closeErr := server.Close(); closeErr != nil {
			return fmt.Errorf("force close: %w", closeErr)
		}
	}
	if err := normalizedServeError(<-serveErrors); err != nil {
		return err
	}
	logEvent(context.Background(), logger, logRecord{slog.LevelInfo, "shutdown_completed", "HTTP server shutdown completed"})
	return nil
}

func normalizedServeError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve: %w", err)
}

// Adds the application-owned routes and dependencies for each service.
type RouteRegistrar func(*http.ServeMux, Config, *slog.Logger)

// Loads configuration and runs one service or its Docker health probe.
func Bootstrap(service, addressVariable, defaultAddress string, register RouteRegistrar) int {
	config, err := LoadConfig(addressVariable, defaultAddress)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if len(os.Args) == 2 && os.Args[1] == "-healthcheck" {
		if err := HealthCheck(config.HTTPAddr); err != nil {
			return 1
		}
		return 0
	}
	if len(os.Args) != 1 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: service [-healthcheck]")
		return 2
	}
	return runService(service, config, register)
}

func runService(service string, config Config, register RouteRegistrar) int {
	logger := NewLogger(service, config.LogLevel, os.Stdout)
	mux := http.NewServeMux()
	RegisterHealth(mux, service)
	register(mux, config, logger)
	server := newServer(config, HTTPHandler(mux, logger, config.MaxInFlight), logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := RunServer(ctx, server, logger, config.ShutdownTimeout); err != nil {
		logEvent(context.Background(), logger, logRecord{slog.LevelError, "server_failed", "HTTP server stopped unexpectedly"})
		return 1
	}
	return 0
}

func newServer(config Config, handler http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              config.HTTPAddr,
		Handler:           handler,
		ErrorLog:          log.New(serverLogWriter{logger: logger}, "", 0),
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       config.IdleTimeout,
	}
}

func HealthCheck(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	probeHost := "127.0.0.1"
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			probeHost = host
		} else if ip.To4() == nil {
			probeHost = "::1"
		}
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Get("http://" + net.JoinHostPort(probeHost, port) + "/health")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned HTTP %d", response.StatusCode)
	}
	return nil
}
