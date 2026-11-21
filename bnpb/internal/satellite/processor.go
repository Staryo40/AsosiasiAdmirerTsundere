package satellite

import (
	"context"
	"sync"
	"time"
)

// The processor owns the checkpoint, so the offset it reports is durable together with its own state.
type Processor interface {
	NextOffset(ctx context.Context) (int64, error)
	Process(ctx context.Context, event Event) error
	Flush(ctx context.Context) error
}

const flushEvery = 500

// Counts events and advances the checkpoint; the fire engine replaces it.
type countingProcessor struct {
	store     *Store
	mu        sync.Mutex
	next      int64
	pending   int
	lastFlush time.Time
	counts    map[string]int64
}

func newCountingProcessor(store *Store) *countingProcessor {
	return &countingProcessor{store: store, counts: map[string]int64{}}
}

func (processor *countingProcessor) NextOffset(ctx context.Context) (int64, error) {
	next, err := processor.store.NextOffset(ctx)
	processor.mu.Lock()
	processor.next = next
	processor.mu.Unlock()
	return next, err
}

func (processor *countingProcessor) Process(ctx context.Context, event Event) error {
	processor.mu.Lock()
	processor.counts[event.Kind]++
	processor.next, processor.pending = event.Offset+1, processor.pending+1
	due := processor.pending >= flushEvery || time.Since(processor.lastFlush) > time.Second
	processor.mu.Unlock()
	if due {
		return processor.Flush(ctx)
	}
	return nil
}

func (processor *countingProcessor) Flush(ctx context.Context) error {
	processor.mu.Lock()
	next := processor.next
	processor.mu.Unlock()
	if err := processor.store.SaveNextOffset(ctx, next); err != nil {
		return err
	}
	processor.mu.Lock()
	processor.pending, processor.lastFlush = 0, time.Now()
	processor.mu.Unlock()
	return nil
}

func (processor *countingProcessor) snapshot() (map[string]int64, int64) {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	counts := make(map[string]int64, len(processor.counts))
	for kind, count := range processor.counts {
		counts[kind] = count
	}
	return counts, processor.next
}
