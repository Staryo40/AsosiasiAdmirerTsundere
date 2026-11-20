//go:build integration

package aggregator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPollAuthorityGenerationIntegrationRebindsBeforeRestartingWorkers(t *testing.T) {
	cfg, store := openRecoveryFixture(t)
	kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
	owner := openRecoveryAuthority(t, cfg, store, kube, host, "generation-recovery-owner")
	started := make(chan context.Context, 2)
	closed := make(chan error, 1)
	resume := make(chan struct{})
	var running atomic.Int32
	host.guardsClosedForRelease = func() bool {
		return running.Load() == 0 && recoveryGuardsClosed(t.Context(), cfg)
	}
	worker := func(ctx context.Context) {
		running.Add(1)
		defer running.Add(-1)
		started <- ctx
		<-ctx.Done()
	}
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		retryAuthorityRun(ctx, func(ctx context.Context) error {
			err := owner.run(ctx, worker)
			if errors.Is(err, errMutationGenerationChanged) {
				closed <- err
				select {
				case <-resume:
				case <-ctx.Done():
				}
			}
			return err
		}, quietLogger())
	}()
	t.Cleanup(func() { cancel(); waitForCompletion(t, finished) })
	oldContext := generationWorkerContext(t, started)
	staleContext := context.WithoutCancel(oldContext)
	_, oldEpoch, _ := pollAuthorityToken(oldContext)
	oldTokens := recoveryGuardTokens(owner)
	admin, err := pgx.Connect(t.Context(), cfg.ShardURLs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	// Model the live resource changing while the copied guard row and app token remain unchanged.
	_, err = admin.Exec(t.Context(), `CREATE OR REPLACE FUNCTION live_mutation_resource(guard_table regclass)
		RETURNS text LANGUAGE sql SET search_path=pg_catalog AS $$ SELECT 'promoted-test-resource'::text $$`)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.shards[0].beginMutation(staleContext); !errors.Is(err, errStaleMutationAuthority) {
		if tx != nil {
			_ = tx.Rollback(t.Context())
		}
		t.Fatalf("copied guard accepted the old token: %v", err)
	}
	select {
	case err := <-closed:
		if !errors.Is(err, errMutationGenerationChanged) {
			t.Fatalf("generation recovery reason: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("owner did not stop after the live PostgreSQL resource changed")
	}
	if running.Load() != 0 {
		t.Fatal("guard closure completed before workers joined")
	}
	assertRecoveryGuards(t, t.Context(), cfg, nil, "closed", "", oldEpoch)
	host.mu.Lock()
	revocations := append([]recoveryHostRevoke(nil), host.revocations...)
	host.mu.Unlock()
	if len(revocations) != len(recoveryDomains()) {
		t.Fatalf("closed %d host grants, want %d", len(revocations), len(recoveryDomains()))
	}
	for _, revoke := range revocations {
		if revoke.requested != hostRevokeGraceful {
			t.Fatal("confirmed generation change requested a takeover fence of its own owner")
		}
	}
	close(resume)
	newContext := generationWorkerContext(t, started)
	_, newEpoch, _ := pollAuthorityToken(newContext)
	if newEpoch != oldEpoch+1 {
		t.Fatalf("restarted worker epoch=%d, want %d", newEpoch, oldEpoch+1)
	}
	newTokens := recoveryGuardTokens(owner)
	if newTokens[1].generation != oldTokens[1].generation+1 || newTokens[1].resource != "promoted-test-resource" {
		t.Fatalf("worker restarted without a rebound resource token: %+v", newTokens[1])
	}
	if err := recoveryIngest(t, store, staleContext, "old-generation-effect"); !errors.Is(err, errStaleMutationAuthority) {
		t.Fatalf("old epoch accepted after recovery: %v", err)
	}
	if err := recoveryIngest(t, store, newContext, routedShardIdentities(t)[0]); err != nil {
		t.Fatalf("restarted owner could not commit canonical effects: %v", err)
	}
}

func generationWorkerContext(t *testing.T, started <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-started:
		return ctx
	case <-time.After(10 * time.Second):
		t.Fatal("owner workers did not start")
		return nil
	}
}

func TestPollAuthorityGenerationIntegrationClassifiesRenewal(t *testing.T) {
	for _, scenario := range []string{"unchanged", "query timeout", "different owner"} {
		t.Run(scenario, func(t *testing.T) {
			cfg, store := openRecoveryFixture(t)
			kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
			owner := openRecoveryAuthority(t, cfg, store, kube, host, "generation-check-owner")
			active, version, err := owner.activate(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			admin, err := pgx.Connect(t.Context(), cfg.DatabaseURL)
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close(t.Context())
			switch scenario {
			case "query timeout":
				_, err = admin.Exec(t.Context(), `CREATE OR REPLACE FUNCTION live_mutation_resource(guard_table regclass)
					RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog AS $$
					BEGIN PERFORM pg_sleep(1); RETURN 'unobserved'; END $$`)
				store.writeTimeout = 50 * time.Millisecond
			case "different owner":
				_, err = admin.Exec(t.Context(), "UPDATE mutation_guard SET owner='another-owner'")
			}
			if err != nil {
				t.Fatal(err)
			}
			next, _, err := owner.renewOnce(t.Context(), active, version)
			if scenario == "unchanged" {
				if err != nil || next.Epoch != active.Epoch || next.RenewalSeq != active.RenewalSeq+1 {
					t.Fatalf("unchanged generation renewal: %+v, %v", next, err)
				}
			} else if err == nil || errors.Is(err, errMutationGenerationChanged) {
				t.Fatalf("%s was treated as a confirmed PostgreSQL generation change: %v", scenario, err)
			}
			if scenario == "different owner" && !errors.Is(err, errStaleMutationAuthority) {
				t.Fatalf("different guard owner did not remain stale authority: %v", err)
			}
		})
	}
}
