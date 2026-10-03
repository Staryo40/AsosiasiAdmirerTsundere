package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
)

func testOptions() applicationOptions {
	return applicationOptions{
		service: "test-service",
		cfg:     config.Config{HTTPAddr: "127.0.0.1:0", MaxInFlight: 64, ShutdownTimeout: time.Second},
		logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle did not complete within the test shutdown budget")
	}
}

func TestStartupFailureCancelsAndCleansPartialInitialization(t *testing.T) {
	startupFailure := errors.New("injected initialization failure")
	workerDone := make(chan struct{})
	cleaned := false
	err := runApplication(t.Context(), testOptions(), func(ctx context.Context, _ *http.ServeMux, _ config.Config, _ *slog.Logger) (Shutdown, error) {
		go func() { <-ctx.Done(); close(workerDone) }()
		return func(shutdownCtx context.Context) error {
			if ctx.Err() != context.Canceled || shutdownCtx.Err() != nil {
				t.Error("cleanup must receive a live deadline after root cancellation")
			}
			select {
			case <-workerDone:
				cleaned = true
				return nil
			case <-shutdownCtx.Done():
				return shutdownCtx.Err()
			}
		}, startupFailure
	})
	if !errors.Is(err, startupFailure) || !cleaned {
		t.Fatalf("initialization error or cleanup lost: %v, cleaned=%t", err, cleaned)
	}
	waitDone(t, workerDone)
}

func TestCancellationDuringInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	finished := make(chan error, 1)
	cleaned := make(chan struct{})
	go func() {
		finished <- runApplication(ctx, testOptions(), func(ctx context.Context, _ *http.ServeMux, _ config.Config, _ *slog.Logger) (Shutdown, error) {
			close(started)
			<-ctx.Done()
			return func(context.Context) error { close(cleaned); return nil }, ctx.Err()
		})
	}()
	waitDone(t, started)
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("initialization did not receive root cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initialization survived cancellation")
	}
	waitDone(t, cleaned)
}

func TestListenFailureStopsWorkersAndPropagatesCleanupError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	options := testOptions()
	options.cfg.HTTPAddr = listener.Addr().String()
	workerDone := make(chan struct{})
	cleanupFailure := errors.New("injected cleanup failure")
	err = runApplication(t.Context(), options, func(ctx context.Context, _ *http.ServeMux, _ config.Config, _ *slog.Logger) (Shutdown, error) {
		go func() { <-ctx.Done(); close(workerDone) }()
		return func(shutdownCtx context.Context) error {
			select {
			case <-workerDone:
				return cleanupFailure
			case <-shutdownCtx.Done():
				return shutdownCtx.Err()
			}
		}, nil
	})
	if !errors.Is(err, cleanupFailure) || !strings.Contains(err.Error(), "listen:") {
		t.Fatalf("listen/cleanup errors were lost: %v", err)
	}
	waitDone(t, workerDone)
}

type startEventWriter struct{ started chan struct{} }

func (writer startEventWriter) Write(record []byte) (int, error) {
	if strings.Contains(string(record), `"event":"server_started"`) {
		close(writer.started)
	}
	return len(record), nil
}

func TestServiceShutdownDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	options := testOptions()
	options.cfg.ShutdownTimeout = 100 * time.Millisecond
	options.logger = slog.New(slog.NewJSONHandler(startEventWriter{started}, nil))
	finished := make(chan error, 1)
	go func() {
		finished <- runApplication(ctx, options, func(context.Context, *http.ServeMux, config.Config, *slog.Logger) (Shutdown, error) {
			return func(shutdownCtx context.Context) error {
				<-shutdownCtx.Done()
				return shutdownCtx.Err()
			}, nil
		})
	}()
	waitDone(t, started)
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup ignored shutdown deadline: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup exceeded shutdown budget")
	}
}

func TestServiceDrainsRequestsBeforeResourceCleanup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	options := testOptions()
	options.cfg.HTTPAddr = listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, requestStarted := make(chan struct{}), make(chan struct{})
	requestRelease, requestDone := make(chan struct{}), make(chan struct{})
	releaseRequest := sync.OnceFunc(func() { close(requestRelease) })
	defer releaseRequest()
	cleanupStarted, workerDone := make(chan struct{}), make(chan struct{})
	options.logger = slog.New(slog.NewJSONHandler(startEventWriter{started}, nil))
	finished := make(chan error, 1)
	go func() {
		finished <- runApplication(ctx, options, func(ctx context.Context, mux *http.ServeMux, _ config.Config, _ *slog.Logger) (Shutdown, error) {
			go func() { <-ctx.Done(); close(workerDone) }()
			mux.HandleFunc("GET /active", func(w http.ResponseWriter, _ *http.Request) {
				defer close(requestDone)
				close(requestStarted)
				<-requestRelease
				_, _ = io.WriteString(w, "completed")
			})
			return func(shutdownCtx context.Context) error {
				close(cleanupStarted)
				select {
				case <-requestDone:
				default:
					t.Error("application resources released while a request was active")
				}
				deadline, exists := shutdownCtx.Deadline()
				if !exists || time.Until(deadline) > 950*time.Millisecond {
					t.Error("HTTP drain and resource cleanup must share one shutdown budget")
				}
				return nil
			}, nil
		})
	}()
	waitDone(t, started)
	requestErrors := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		defer client.CloseIdleConnections()
		response, err := client.Get("http://" + options.cfg.HTTPAddr + "/active")
		if err == nil {
			_, err = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		requestErrors <- err
	}()
	waitDone(t, requestStarted)
	cancel()
	waitDone(t, workerDone)
	select {
	case <-cleanupStarted:
		t.Error("resource cleanup began before the HTTP drain completed")
	case <-time.After(100 * time.Millisecond):
	}
	releaseRequest()
	waitDone(t, requestDone)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := <-requestErrors; err != nil {
		t.Fatalf("active request did not drain successfully: %v", err)
	}
	waitDone(t, cleanupStarted)
}

func TestHealthcheckSkipsApplicationInitialization(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/health" {
			t.Errorf("probe called %s", request.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer probe.Close()
	t.Setenv("BNPB_TEST_HTTP_ADDR", probe.Listener.Addr().String())
	t.Setenv("BNPB_CANONICAL_DATABASE_URL", "invalid-private-dsn")
	t.Setenv("BNPB_BMKG_API_KEY", "")
	previousArgs := os.Args
	os.Args = []string{"test-service", "-healthcheck"}
	defer func() { os.Args = previousArgs }()
	exitCode := BootstrapApplication("test-service", "BNPB_TEST_HTTP_ADDR", ":8083", func(context.Context, *http.ServeMux, config.Config, *slog.Logger) (Shutdown, error) {
		t.Fatal("healthcheck initialized business dependencies")
		return nil, nil
	})
	if exitCode != 0 {
		t.Fatalf("probe failed with exit code %d", exitCode)
	}
	if exitCode := Bootstrap("legacy-service", "BNPB_TEST_HTTP_ADDR", ":8083", func(*http.ServeMux, config.Config, *slog.Logger) {
		t.Fatal("legacy probe registered routes")
	}); exitCode != 0 {
		t.Fatalf("legacy probe failed with exit code %d", exitCode)
	}
}
