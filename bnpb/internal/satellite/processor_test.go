package satellite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A synthetic feed in the real wire format: detections around a few hotspots, plus updates and retracts.
func syntheticFrames(seed uint64, count int) []string {
	random := rand.New(rand.NewPCG(seed, seed+1))
	hotspots := [][2]float64{{-2.51, 113.80}, {-0.16, 122.03}, {-3.97, 118.31}, {1.20, 101.40}}
	var keys []string
	frames := make([]string, 0, count)
	clock := time.Date(2026, 11, 28, 0, 0, 0, 0, time.UTC)
	for offset := range count {
		var data, kind string
		switch roll := random.IntN(10); {
		case roll < 7 || len(keys) == 0:
			clock = clock.Add(time.Duration(random.IntN(4)) * time.Minute)
			at := clock.Add(-time.Duration(random.IntN(200)) * time.Minute)
			spot := hotspots[random.IntN(len(hotspots))]
			lat := fmt.Sprintf("%.5f", spot[0]+random.Float64()*0.02)
			lon := fmt.Sprintf("%.5f", spot[1]+random.Float64()*0.02)
			satellite := []string{"N20", "N21", "N"}[random.IntN(3)]
			hhmm := at.Hour()*100 + at.Minute()
			keys = append(keys, fmt.Sprintf("%s|%s|%d|%s|%s", satellite, at.Format("2006-01-02"), hhmm, lat, lon))
			kind = KindDetection
			data = fmt.Sprintf(`{"offset":%d,"detection":{"acq_date":"%s","acq_time":%d,"confidence":"%s","frp":%.2f,"latitude":%s,"longitude":%s,"satellite":"%s"}}`,
				offset, at.Format("2006-01-02"), hhmm, []string{"l", "n", "n", "h"}[random.IntN(4)], random.Float64()*20, lat, lon, satellite)
		case roll < 9:
			kind = KindUpdate
			data = fmt.Sprintf(`{"offset":%d,"key":"%s","changes":{"confidence":"%s","frp":%.2f}}`,
				offset, keys[random.IntN(len(keys))], []string{"l", "n", "h"}[random.IntN(3)], random.Float64()*20)
		default:
			kind = KindRetract
			data = fmt.Sprintf(`{"offset":%d,"key":"%s","reason":"false_positive"}`, offset, keys[random.IntN(len(keys))])
		}
		frames = append(frames, fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", offset, kind, data))
	}
	return frames
}

func framedFeed(t *testing.T, frames []string) *url.URL {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		from, _ := strconv.Atoi(request.URL.Query().Get("from"))
		// Connections end after a while, as the real feed's do.
		for offset := from; offset < len(frames) && offset < from+700; offset++ {
			_, _ = io.WriteString(w, frames[offset])
		}
	}))
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	return base
}

var errCrash = errors.New("simulated crash")

// Dies after a set number of events and never flushes again, like a killed process: its uncommitted batch is lost.
type crashingProcessor struct {
	inner     *fireProcessor
	remaining int
	dead      atomic.Bool
}

func (crash *crashingProcessor) NextOffset(ctx context.Context) (int64, error) {
	return crash.inner.NextOffset(ctx)
}
func (crash *crashingProcessor) Flush(ctx context.Context) error {
	if crash.dead.Load() {
		return errCrash
	}
	return crash.inner.Flush(ctx)
}
func (crash *crashingProcessor) Process(ctx context.Context, event Event) error {
	if crash.dead.Load() {
		return errCrash
	}
	if crash.remaining--; crash.remaining < 0 {
		crash.dead.Store(true)
		return errCrash
	}
	return crash.inner.Process(ctx, event)
}

func streamUntil(t *testing.T, base *url.URL, processor Processor, done func() bool) {
	t.Helper()
	feed := newStreamer(Config{BaseURL: base, Token: "feed-token", IdleTimeout: time.Second}, processor, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	feed.wait = func(ctx context.Context, _ time.Duration) bool { return sleepContext(ctx, time.Millisecond) }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	finished := make(chan struct{})
	go func() { defer close(finished); feed.run(ctx) }()
	for !done() {
		if ctx.Err() != nil {
			t.Fatal("stream did not finish")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-finished
}

// The latest record per fire is what the Aggregator keeps; feed_revision depends on batch timing, so it is left out.
func finalFires(t *testing.T, store *Store) map[string]string {
	t.Helper()
	rows, err := store.db.Query("SELECT fire_id, record FROM outbox ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	final := map[string]string{}
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatal(err)
		}
		var record fireRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		record.FeedRevision = 0
		normalized, _ := json.Marshal(record)
		final[id] = string(normalized)
		if !record.Active {
			delete(final, id)
		}
	}
	return final
}

func runToEnd(t *testing.T, path string, base *url.URL, total int64, crashAfter []int) *Store {
	t.Helper()
	store, err := OpenStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, after := range append(crashAfter, -1) {
		processor, err := newFireProcessor(context.Background(), store)
		if err != nil {
			t.Fatal(err)
		}
		if after < 0 {
			streamUntil(t, base, processor, func() bool { next, _ := store.NextOffset(context.Background()); return next >= total })
			continue
		}
		crash := &crashingProcessor{inner: processor, remaining: after}
		streamUntil(t, base, crash, func() bool { return crash.dead.Load() })
	}
	return store
}

func TestAKilledProcessorRecoversToExactlyTheSameFires(t *testing.T) {
	const total = 4000
	frames := syntheticFrames(7, total)
	base := framedFeed(t, frames)
	reference := finalFires(t, runToEnd(t, filepath.Join(t.TempDir(), "reference.db"), base, total, nil))
	if len(reference) == 0 {
		t.Fatal("the synthetic feed must produce fires")
	}
	for seed := range uint64(5) {
		random := rand.New(rand.NewPCG(seed, 99))
		crashes := []int{random.IntN(300) + 1, random.IntN(1200) + 1, random.IntN(900) + 1, random.IntN(50) + 1}
		recovered := finalFires(t, runToEnd(t, filepath.Join(t.TempDir(), fmt.Sprintf("crash-%d.db", seed)), base, total, crashes))
		if !reflect.DeepEqual(recovered, reference) {
			t.Fatalf("crashes %v: %d fires after recovery, %d without crashes", crashes, len(recovered), len(reference))
		}
	}
}

func TestTheEventLogRebuildsTheEngine(t *testing.T) {
	const total = 1500
	base := framedFeed(t, syntheticFrames(3, total))
	store := runToEnd(t, filepath.Join(t.TempDir(), "log.db"), base, total, nil)
	first, err := newFireProcessor(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newFireProcessor(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	_, nextA, statsA := first.snapshot()
	_, nextB, statsB := second.snapshot()
	if nextA != total || nextA != nextB || statsA != statsB || statsA.ActiveFires == 0 {
		t.Fatalf("rebuilds disagree: %d %+v vs %d %+v", nextA, statsA, nextB, statsB)
	}
}

type fakeAggregator struct {
	mu        sync.Mutex
	failFirst int
	revisions map[string]int64
	received  []string
	status    int
}

func (fake *fakeAggregator) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if request.Header.Get("Authorization") != "Bearer "+strings.Repeat("i", 32) || request.URL.Path != "/internal/satellite/fires" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if fake.failFirst > 0 {
		fake.failFirst--
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if fake.status != 0 {
		w.WriteHeader(fake.status)
		return
	}
	var receipt fireReceipt
	_ = json.NewDecoder(request.Body).Decode(&receipt)
	for _, raw := range receipt.Records {
		var record fireRecord
		_ = json.Unmarshal(raw, &record)
		fake.received = append(fake.received, record.SourceRefID)
		if record.FeedRevision > fake.revisions[record.SourceRefID] {
			fake.revisions[record.SourceRefID] = record.FeedRevision
		}
	}
	w.WriteHeader(http.StatusOK)
}

func TestSenderRetriesUntilTheAggregatorStoresEveryChange(t *testing.T) {
	const total = 1500
	base := framedFeed(t, syntheticFrames(5, total))
	store := runToEnd(t, filepath.Join(t.TempDir(), "send.db"), base, total, nil)
	fake := &fakeAggregator{failFirst: 3, revisions: map[string]int64{}}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	aggregator, _ := url.Parse(server.URL)
	delivery := newSender(Config{AggregatorURL: aggregator, IngestToken: strings.Repeat("i", 32)}, store, make(chan struct{}), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var waits int
	delivery.wait = func(ctx context.Context, _ time.Duration) bool { waits++; return sleepContext(ctx, time.Millisecond) }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	finished := make(chan struct{})
	go func() { defer close(finished); delivery.run(ctx) }()
	for {
		if backlog, _ := store.Backlog(ctx); backlog == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("the outbox never drained")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-finished
	var outboxRows int
	_ = store.db.QueryRow("SELECT count(*) FROM outbox").Scan(&outboxRows)
	final := finalFires(t, store)
	if waits < 3 || len(fake.received) != outboxRows || len(fake.revisions) != len(final) {
		t.Fatalf("waits=%d received=%d outbox=%d fires=%d/%d", waits, len(fake.received), outboxRows, len(fake.revisions), len(final))
	}
}

func TestARefusedBatchIsCountedAndDoesNotBlockLaterChanges(t *testing.T) {
	const total = 600
	base := framedFeed(t, syntheticFrames(9, total))
	store := runToEnd(t, filepath.Join(t.TempDir(), "refused.db"), base, total, nil)
	fake := &fakeAggregator{status: http.StatusBadRequest, revisions: map[string]int64{}}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	aggregator, _ := url.Parse(server.URL)
	delivery := newSender(Config{AggregatorURL: aggregator, IngestToken: strings.Repeat("i", 32)}, store, make(chan struct{}), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	rows, err := store.PendingOutbox(context.Background(), sendBatchRecords, sendBatchBytes)
	if err != nil || len(rows) == 0 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	if err := delivery.send(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if delivery.refused.Load() != int64(len(rows)) {
		t.Fatalf("refused=%d want %d", delivery.refused.Load(), len(rows))
	}
}
