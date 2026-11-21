package satellite

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/satellite/fire"
)

// The processor owns the checkpoint, so the offset it reports is durable together with its own state.
type Processor interface {
	NextOffset(ctx context.Context) (int64, error)
	Process(ctx context.Context, event Event) error
	Flush(ctx context.Context) error
}

const (
	flushEvery    = 500
	flushInterval = time.Second
)

const KindUnusable = "unusable"

type fireRecordAttributes struct {
	DetectionCount int      `json:"detection_count"`
	Satellites     []string `json:"satellites"`
	MaxConfidence  string   `json:"max_confidence"`
	TotalFRPMW     float64  `json:"total_frp_mw"`
	DetectionKeys  []string `json:"detection_keys"`
}

// The Aggregator's wildfire record contract.
type fireRecord struct {
	SourceRefID  string               `json:"source_ref_id"`
	FeedRevision int64                `json:"feed_revision"`
	Active       bool                 `json:"active"`
	Severity     string               `json:"severity"`
	AreaName     string               `json:"area_name"`
	Latitude     float64              `json:"latitude"`
	Longitude    float64              `json:"longitude"`
	OccurredAt   time.Time            `json:"occurred_at"`
	Attributes   fireRecordAttributes `json:"attributes"`
}

type fireProcessor struct {
	store *Store
	mu    sync.Mutex
	// Rebuilt from the event log whenever a commit fails, because the engine then holds uncommitted changes.
	engine        *fire.Engine
	broken        bool
	next          int64
	pending       []loggedEvent
	firstReceived time.Time
	lastFlush     time.Time
	counts        map[string]int64
	wake          chan struct{}
}

func newFireProcessor(ctx context.Context, store *Store) (*fireProcessor, error) {
	processor := &fireProcessor{store: store, counts: map[string]int64{}, wake: make(chan struct{}, 1)}
	return processor, processor.rebuild(ctx)
}

func (processor *fireProcessor) rebuild(ctx context.Context) error {
	engine := fire.NewEngine()
	err := processor.store.ReplayEvents(ctx, func(logged loggedEvent) error {
		event, err := parseEvent(sseFrame{id: strconv.FormatInt(logged.offset, 10), event: logged.kind, data: logged.body})
		if err != nil {
			return err
		}
		_, err = applyToEngine(engine, event)
		return err
	})
	if err != nil {
		return err
	}
	// Changes from replayed events are already in the outbox.
	engine.Drain()
	next, err := processor.store.NextOffset(ctx)
	if err != nil {
		return err
	}
	processor.engine, processor.next, processor.pending, processor.broken = engine, next, nil, false
	return nil
}

func (processor *fireProcessor) NextOffset(ctx context.Context) (int64, error) {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	if processor.broken {
		if err := processor.rebuild(ctx); err != nil {
			return 0, err
		}
	}
	return processor.next, nil
}

func (processor *fireProcessor) Process(ctx context.Context, event Event) error {
	processor.mu.Lock()
	kind := event.Kind
	if kind == KindDetection || kind == KindUpdate || kind == KindRetract {
		if _, err := applyToEngine(processor.engine, event); err != nil {
			kind = KindUnusable
		} else {
			if len(processor.pending) == 0 {
				processor.firstReceived = time.Now()
			}
			processor.pending = append(processor.pending, loggedEvent{offset: event.Offset, kind: event.Kind, body: event.Raw})
		}
	}
	processor.counts[kind]++
	processor.next = event.Offset + 1
	due := len(processor.pending) >= flushEvery || time.Since(processor.lastFlush) >= flushInterval
	processor.mu.Unlock()
	if due {
		return processor.Flush(ctx)
	}
	return nil
}

// A change's revision is the last offset of its batch, so revisions only grow and replays never outrank newer data.
func (processor *fireProcessor) Flush(ctx context.Context) error {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	if processor.broken {
		return nil
	}
	received := processor.firstReceived
	if received.IsZero() {
		received = time.Now()
	}
	revision := processor.next - 1
	var changes []outboxRow
	for _, changed := range processor.engine.Drain() {
		record, err := json.Marshal(fireRecord{SourceRefID: changed.ID, FeedRevision: revision, Active: changed.Active,
			Severity: changed.Severity, AreaName: changed.AreaName, Latitude: changed.Latitude, Longitude: changed.Longitude,
			OccurredAt: changed.OccurredAt, Attributes: fireRecordAttributes{DetectionCount: changed.DetectionCount,
				Satellites: changed.Satellites, MaxConfidence: changed.MaxConfidence, TotalFRPMW: changed.TotalFRP, DetectionKeys: changed.DetectionKeys}})
		if err != nil {
			processor.broken = true
			return err
		}
		changes = append(changes, outboxRow{fireID: changed.ID, feedRevision: revision, record: record, receivedAt: received})
	}
	if err := processor.store.Commit(ctx, processor.pending, changes, processor.next); err != nil {
		processor.broken = true
		return err
	}
	processor.pending, processor.firstReceived, processor.lastFlush = nil, time.Time{}, time.Now()
	if len(changes) > 0 {
		select {
		case processor.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

func (processor *fireProcessor) snapshot() (map[string]int64, int64, fire.Stats) {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	counts := make(map[string]int64, len(processor.counts))
	for kind, count := range processor.counts {
		counts[kind] = count
	}
	return counts, processor.next, processor.engine.Stats()
}
