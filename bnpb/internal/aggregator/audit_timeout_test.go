package aggregator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAuditTimeoutKeepsSocketResponse(t *testing.T) {
	t.Parallel()
	const writeTimeout = time.Second
	for _, test := range []struct {
		name, path string
		status     int
	}{
		{"checksum", "/internal/audit/checksum", http.StatusOK},
		{"events", "/internal/audit/events", http.StatusOK},
		{"location", "/internal/audit/location?hazard_id=fixture", http.StatusOK},
		{"unprocessed", "/internal/audit/unprocessed", http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{DBReadTimeout: time.Minute, ReadMaxInFlight: 1,
				Audit: AuditConfig{Token: "audit-token", ChecksumTimeout: time.Minute}}
			started := make(chan struct{}, 1)
			var store *Store
			if test.status == http.StatusServiceUnavailable {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					started <- struct{}{}
					_, _ = w.Write([]byte(`{"items":[`))
					_ = http.NewResponseController(w).Flush()
					<-r.Context().Done()
				}))
				t.Cleanup(upstream.Close)
				cfg.Audit.UnprocessedURL, cfg.Audit.UnprocessedToken = upstream.URL, "owner-token"
			} else {
				store = stalledAuditStore(t, started)
			}
			mux := http.NewServeMux()
			server := httptest.NewUnstartedServer(mux)
			server.Config.WriteTimeout = writeTimeout
			registerAuditRoutes4(mux, store, cfg, server.Config.WriteTimeout)
			server.Start()
			t.Cleanup(server.Close)

			ctx, cancel := context.WithTimeout(t.Context(), 3*writeTimeout)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+test.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+cfg.Audit.Token)
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatalf("expected JSON before write timeout: %v", err)
			}
			defer response.Body.Close()
			select {
			case <-started:
			default:
				t.Fatal("the configured dependency did not start")
			}
			assertAuditSocketResponse(t, response, test.status)
		})
	}
}

func stalledAuditStore(t *testing.T, started chan<- struct{}) *Store {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://audit@127.0.0.1/audit?sslmode=disable&pool_max_conns=1&pool_min_conns=0&pool_min_idle_conns=0")
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	// pgxpool can keep creating a connection after its caller has canceled Acquire.
	cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		started <- struct{}{}
		<-release
		return context.Canceled
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		pool.Close()
	})
	return &Store{read: pool}
}

func assertAuditSocketResponse(t *testing.T, response *http.Response, status int) {
	t.Helper()
	if response.StatusCode != status || response.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("status=%d content-type=%q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	var body struct {
		Partial          bool             `json:"partial"`
		UnavailableUnits []string         `json:"unavailable_units"`
		Error            auditErrorDetail `json:"error"`
	}
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if status == http.StatusOK {
		if !body.Partial || !slices.Equal(body.UnavailableUnits, []string{"local"}) {
			t.Fatalf("expected partial response for the stalled store: %+v", body)
		}
	} else if body.Error.Code != "unavailable" || body.Error.Message == "" {
		t.Fatalf("expected sanitized unavailable error: %+v", body)
	}
}

func TestAuditReadBudgetReservesRollbackAndEmission(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                      string
		write, work, cleanupLimit time.Duration
	}{
		{"default write deadline", 10 * time.Second, 8 * time.Second, 9 * time.Second},
		{"short write deadline", time.Second, 250 * time.Millisecond, 750 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				handler := withAuditReadBudget(func(_ http.ResponseWriter, request *http.Request) {
					<-request.Context().Done()
					if elapsed := time.Since(start); elapsed != test.work {
						t.Fatalf("work lasted %s, want %s", elapsed, test.work)
					}
					tx := auditRollbackTx{cleanup: func(ctx context.Context) error {
						if ctx.Err() != nil {
							t.Fatal("rollback inherited the expired work context")
						}
						<-ctx.Done()
						return ctx.Err()
					}}
					if err := rollbackTransaction(request.Context(), tx); err == nil {
						t.Fatal("did not report the stalled rollback")
					}
				}, time.Minute, test.write)
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
				if elapsed := time.Since(start); elapsed != test.cleanupLimit {
					t.Fatalf("work and rollback lasted %s, want %s", elapsed, test.cleanupLimit)
				}
			})
		})
	}
}

type auditRollbackTx struct {
	pgx.Tx
	cleanup func(context.Context) error
}

func (tx auditRollbackTx) Rollback(ctx context.Context) error { return tx.cleanup(ctx) }
