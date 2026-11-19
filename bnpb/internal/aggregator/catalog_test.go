package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCatalogAPIAuthenticationAndBoundedBody(t *testing.T) {
	t.Parallel()
	api := catalogAPI{cfg: Config{Catalog: CatalogConfig{Token: "catalog-secret"}}}
	handler := api.authenticate(http.HandlerFunc(api.ingest))
	cases := []struct {
		name, authorization, body string
		status                    int
	}{
		{name: "missing token", body: `{}`, status: http.StatusUnauthorized},
		{name: "wrong dedicated token", authorization: "Bearer read-secret", body: `{}`, status: http.StatusForbidden},
		{name: "malformed body", authorization: "Bearer catalog-secret", body: `{"input_id":`, status: http.StatusBadRequest},
		{name: "oversized body", authorization: "Bearer catalog-secret", body: strings.Repeat(" ", (4<<20)+1), status: http.StatusBadRequest},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/internal/catalog/seismic", bytes.NewBufferString(test.body))
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d, want %d", response.Code, test.status)
			}
		})
	}
}

func TestCatalogFollowerCannotClaimDurableAcceptance(t *testing.T) {
	t.Parallel()
	api := catalogAPI{cfg: Config{Catalog: CatalogConfig{Token: "catalog-secret"}}, authority: &PollAuthority{owner: "follower", kubernetes: &memoryAuthorityKubernetes{record: authorityRecord{State: authorityClosed}}}}
	handler := api.authenticate(http.HandlerFunc(api.ingest))
	request := httptest.NewRequest(http.MethodPost, "/internal/catalog/seismic", strings.NewReader(`{"input_id":"abc","input_offset":0,"records":[{"event_id":"e1"}]}`))
	request.Header.Set("Authorization", "Bearer catalog-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("follower status=%d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if bytes.Contains(response.Body.Bytes(), []byte(`"accepted"`)) || response.Code == http.StatusAccepted {
		t.Fatalf("follower returned false durable acceptance: %s", response.Body.String())
	}
}

func TestMeasurePoolBatchSharesDeadlineAndKeepsPoolOrder(t *testing.T) {
	const count = 4
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan int, count)
	deadlines := make(chan time.Time, count)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var active, maximum atomic.Int32
	resultsDone := make(chan []poolMeasurementResult, 1)
	go func() {
		resultsDone <- measurePoolBatch(ctx, count, func(ctx context.Context, index int) (storageMeasurement, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
			}
			deadline, _ := ctx.Deadline()
			deadlines <- deadline
			started <- index
			<-release
			return storageMeasurement{ConnectionIndex: index}, nil
		})
	}()
	var sharedDeadline time.Time
	for range count {
		<-started
		deadline := <-deadlines
		if deadline.IsZero() {
			t.Fatal("pool measurement has no shared deadline")
		}
		if sharedDeadline.IsZero() {
			sharedDeadline = deadline
		} else if !deadline.Equal(sharedDeadline) {
			t.Fatalf("pool measurements got different deadlines: %v and %v", sharedDeadline, deadline)
		}
	}
	if got := maximum.Load(); got != count {
		t.Fatalf("maximum concurrent measurements=%d, want %d", got, count)
	}
	releaseOnce.Do(func() { close(release) })
	batchResults := <-resultsDone
	if active.Load() != 0 {
		t.Fatalf("measurements still active after batch returned: %d", active.Load())
	}
	for index, batchResult := range batchResults {
		if batchResult.err != nil || batchResult.measurement.ConnectionIndex != index {
			t.Fatalf("result[%d]=%+v, want successful pool index %d", index, batchResult, index)
		}
	}
}

func TestMeasurementIdentityAndOwnedTableProjection(t *testing.T) {
	measurement := storageMeasurement{}
	setMeasurementIdentity(&measurement, "  ", "")
	if measurement.SettingsComplete || !measurement.Partial || measurement.Node != "" || measurement.Unit != "" {
		t.Fatalf("missing settings fabricated an identity: %+v", measurement)
	}
	setMeasurementIdentity(&measurement, " node-a ", " unit-a ")
	if !measurement.SettingsComplete || measurement.Partial || measurement.Node != "node-a" || measurement.Unit != "unit-a" {
		t.Fatalf("identity was not projected correctly: %+v", measurement)
	}
	for _, name := range []string{"outbox", "handled_inputs"} {
		if !slices.Contains(canonicalMeasurementTables, name) {
			t.Errorf("%q is not included in measured owned tables", name)
		}
	}
	projected, err := json.Marshal(tableMeasurement{Name: "outbox", Rows: 4, Bytes: 8192, Inserted: 5, Updated: 2, StatsApproximate: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"row_count":4`, `"relation_bytes":8192`, `"stats_inserted":5`, `"stats_updated":2`, `"stats_approximate":true`} {
		if !bytes.Contains(projected, []byte(field)) {
			t.Errorf("measurement projection missing %s: %s", field, projected)
		}
	}
}

func TestRoutedMeasurementInventoryRequiresObservedRF2Placement(t *testing.T) {
	t.Parallel()
	placement := []shardPlacement{
		{ShardID: 0, Node: "node-a", Unit: "shard-0-primary", Role: "primary", State: "ready"},
		{ShardID: 0, Node: "node-b", Unit: "shard-0-replica", Role: "replica", State: "ready"},
		{ShardID: 1, Node: "node-b", Unit: "shard-1-primary", Role: "primary", State: "ready"},
		{ShardID: 1, Node: "node-c", Unit: "shard-1-replica", Role: "replica", State: "ready"},
		{ShardID: 2, Node: "node-c", Unit: "shard-2-primary", Role: "primary", State: "ready"},
		{ShardID: 2, Node: "node-a", Unit: "shard-2-replica", Role: "replica", State: "ready"},
	}
	tests := []struct {
		name   string
		change func([]shardPlacement, []storageMeasurement) ([]shardPlacement, []storageMeasurement)
		want   []string
	}{
		{name: "complete"},
		{name: "placement unavailable", change: func(_ []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			return nil, observed
		}, want: []string{unverifiedMeasurementInventory}},
		{name: "copy missing", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			return expected, observed[:5]
		}, want: []string{"shard-2-replica"}},
		{name: "duplicate", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			observed[5] = observed[4]
			return expected, observed
		}, want: []string{"shard-2-primary", "shard-2-replica"}},
		{name: "wrong node", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			observed[5].Node = "node-c"
			return expected, observed
		}, want: []string{"shard-2-replica"}},
		{name: "wrong unit", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			observed[5].Unit = "unplanned-unit"
			return expected, observed
		}, want: []string{"unplanned-unit", "shard-2-replica"}},
		{name: "wrong logical shard", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			observed[5].logicalShardID = 0
			return expected, observed
		}, want: []string{"shard-2-replica"}},
		{name: "live primary instead of replica", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			observed[5].Role, observed[5].InRecovery = "primary", false
			return expected, observed
		}, want: []string{"shard-2-replica"}},
		{name: "same node for both copies", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			expected[5].Node, observed[5].Node = "node-c", "node-c"
			return expected, observed
		}, want: []string{unverifiedMeasurementInventory}},
		{name: "replica not ready", change: func(expected []shardPlacement, observed []storageMeasurement) ([]shardPlacement, []storageMeasurement) {
			expected[5].State = "copying"
			return expected, observed
		}, want: []string{unverifiedMeasurementInventory}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := slices.Clone(placement)
			observed := make([]storageMeasurement, len(placement))
			for index, copy := range placement {
				observed[index] = storageMeasurement{
					Node: copy.Node, Unit: copy.Unit, Role: copy.Role, logicalShardID: copy.ShardID,
					SettingsComplete: true, InRecovery: copy.Role == "replica",
				}
			}
			if test.change != nil {
				expected, observed = test.change(expected, observed)
			}
			response := storageMeasurements{Items: observed}
			response.reconcilePlacement(expected)
			if response.Partial != (len(test.want) != 0) || !slices.Equal(response.UnavailableUnits, test.want) {
				t.Fatalf("partial=%t unavailable=%v, want unavailable=%v", response.Partial, response.UnavailableUnits, test.want)
			}
		})
	}
}

func TestCatalogInputHashesUseReceiptOrdinals(t *testing.T) {
	receipt := catalogReceipt{InputID: "receipt", InputOffset: 12, Records: []json.RawMessage{{}, {}}}
	hashes, expected := catalogInputHashes(receipt)
	if len(hashes) != 2 || len(expected) != 2 {
		t.Fatalf("hashes=%d expected identities=%d, want 2 each", len(hashes), len(expected))
	}
	for index, offset := range []int{12, 13} {
		want := inputHash(IngestChunk{Endpoint: SeismicEndpoint, InputID: receipt.InputID, InputOffset: offset})
		if !bytes.Equal(hashes[index], want) {
			t.Errorf("ordinal %d hash does not match input offset %d", index, offset)
		}
	}
}

func TestClassifyCatalogOutcomesRequiresOneKnownOutcome(t *testing.T) {
	cases := []struct {
		name       string
		statuses   []string
		wantReject bool
		wantErr    bool
	}{
		{name: "all applied", statuses: []string{"applied"}},
		{name: "retained", statuses: []string{"retained"}, wantReject: true},
		{name: "missing", wantErr: true},
		{name: "ambiguous", statuses: []string{"applied", "retained"}, wantErr: true},
		{name: "unknown status", statuses: []string{"pending"}, wantErr: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(map[string][]string)
			if test.statuses != nil {
				outcomes["identity"] = test.statuses
			}
			rejected, err := classifyCatalogOutcomes(outcomes, map[string]struct{}{"identity": {}})
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v, wantErr=%v", err, test.wantErr)
			}
			if rejected != test.wantReject {
				t.Fatalf("rejected=%v, want %v", rejected, test.wantReject)
			}
		})
	}
}
