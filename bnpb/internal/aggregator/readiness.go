package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

const routingReadinessTimeout = 750 * time.Millisecond

type readinessUnit struct {
	ID     int16  `json:"id"`
	Unit   string `json:"unit"`
	Status string `json:"status"`
}

type readinessReport struct {
	ProcessStatus string          `json:"process_status"`
	Ready         bool            `json:"ready"`
	LogicalShards []readinessUnit `json:"logical_shards"`
	PollOwner     string          `json:"poll_owner"`
}

func (report readinessReport) err() error {
	if report.Ready {
		return nil
	}
	return errors.New("no verified logical primary is ready")
}

func (store *Store) readiness(ctx context.Context) readinessReport {
	report := readinessReport{ProcessStatus: "ok", LogicalShards: []readinessUnit{}, PollOwner: "unavailable"}
	if store == nil {
		return report
	}
	ctx, cancel := context.WithTimeout(ctx, store.readinessTimeout())
	defer cancel()

	owner := make(chan string)
	go func() { owner <- store.pollOwnerReadiness(ctx) }()

	units := store.logicalReadStores()
	report.LogicalShards = make([]readinessUnit, len(units))
	results := probeLogicalShards(ctx, units)
	for range units {
		unit := <-results
		report.LogicalShards[unit.ID] = unit
		report.Ready = report.Ready || unit.Status == "ready"
	}
	report.PollOwner = <-owner
	return report
}

func (store *Store) readinessTimeout() time.Duration {
	if store.readTimeout <= 0 {
		return time.Second
	}
	return store.readTimeout
}

func probeLogicalShards(ctx context.Context, units []logicalReadStore) <-chan readinessUnit {
	// Each worker can finish after /readyz returns without blocking on its result.
	results := make(chan readinessUnit, len(units))
	for _, unit := range units {
		go func() {
			status := "unavailable"
			if unit.Store != nil && unit.Store.readinessPrimary(ctx, unit.ID, unit.Unit != "local") == nil {
				status = "ready"
			}
			results <- readinessUnit{ID: unit.ID, Unit: unit.Unit, Status: status}
		}()
	}
	return results
}

func anyLogicalShardReady(ctx context.Context, units []logicalReadStore) bool {
	results := probeLogicalShards(ctx, units)
	for range units {
		select {
		case unit := <-results:
			if unit.Status == "ready" {
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
	return false
}

func (store *Store) readinessPrimary(ctx context.Context, id int16, routed bool) error {
	if routed && (store.storeIdentity.role != "shard" || store.storeIdentity.shardID == nil || *store.storeIdentity.shardID != id) {
		return errors.New("logical store identity mismatch")
	}
	if !routed && store.storeIdentity.role != "" {
		return errors.New("local store identity mismatch")
	}
	return store.primarySnapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if !routed {
			var primary bool
			if err := tx.QueryRow(ctx, "SELECT NOT pg_catalog.pg_is_in_recovery()").Scan(&primary); err != nil {
				return err
			}
			if !primary {
				return errors.New("local logical store is not primary")
			}
		}
		version, err := schemaVersion(tx.QueryRow(ctx, schemaHistoryQuery))
		if err == nil && version != currentSchemaVersion {
			err = errSchemaVersion
		}
		return err
	})
}

func (store *Store) pollOwnerReadiness(ctx context.Context) string {
	authority := store.authority
	if authority == nil {
		if store.shards[0] == nil && store.pollLeader != nil {
			if store.pollLeader.Leading() {
				return "leader"
			}
			return "standby"
		}
		if store.shards[0] == nil {
			return "local"
		}
		return "unavailable"
	}
	if authority.kubernetes == nil || authority.host == nil {
		return "unavailable"
	}
	record, _, err := authority.kubernetes.Read(ctx)
	if err != nil || validateAuthorityRecord(record) != nil || record.State != authorityActive {
		return "unavailable"
	}
	current, err := authority.hostGrantsCurrent(ctx, record)
	if err != nil || !current {
		return "unavailable"
	}
	if record.Owner == authority.owner {
		return "owner"
	}
	return "follower"
}

func (app application) ready(w http.ResponseWriter, request *http.Request) {
	report := app.store.readiness(request.Context())
	w.Header().Set("Content-Type", "application/json")
	if !report.Ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(report)
}

func (app application) readyProbe(w http.ResponseWriter, request *http.Request) {
	if app.store != nil {
		ctx, cancel := context.WithTimeout(request.Context(), min(app.store.readinessTimeout(), routingReadinessTimeout))
		defer cancel()
		if anyLogicalShardReady(ctx, app.store.logicalReadStores()) {
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}
