
package aggregator

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type logicalReadStore struct {
	ID    int16
	Unit  string
	Store *Store
}

func (store *Store) logicalReadStores() []logicalReadStore {
	if store.shards == [3]*Store{} {
		return []logicalReadStore{{ID: 0, Unit: "local", Store: store}}
	}
	stores := make([]logicalReadStore, 0, len(store.shards))
	for id, shard := range store.shards {
		stores = append(stores, logicalReadStore{ID: int16(id), Unit: fmt.Sprintf("shard-%d", id), Store: shard})
	}
	return stores
}

type logicalReadResult[T any] struct {
	Unit  logicalReadStore
	Value T
	Err   error
}

// fanoutLogicalReads joins every worker and retains failures by logical unit.
// The caller owns the request deadline shared with all other reads.
func fanoutLogicalReads[T any](ctx context.Context, stores []logicalReadStore,
	read func(context.Context, logicalReadStore) (T, error)) []logicalReadResult[T] {
	results := make([]logicalReadResult[T], len(stores))
	var workers sync.WaitGroup
	workers.Add(len(stores))
	for index, unit := range stores {
		go func(index int, unit logicalReadStore) {
			defer workers.Done()
			value, err := read(ctx, unit)
			results[index] = logicalReadResult[T]{Unit: unit, Value: value, Err: err}
		}(index, unit)
	}
	workers.Wait()
	return results
}

func unavailableReadUnits[T any](results []logicalReadResult[T]) []string {
	units := make([]string, 0)
	for _, result := range results {
		if result.Err != nil {
			units = append(units, result.Unit.Unit)
		}
	}
	sort.Strings(units)
	return units
}
