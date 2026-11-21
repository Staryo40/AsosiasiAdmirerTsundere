package satellite

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func jsonNumber(raw string) json.Number { return json.Number(raw) }

type recordingProcessor struct {
	mu      sync.Mutex
	next    int64
	offsets []int64
}

func (processor *recordingProcessor) NextOffset(context.Context) (int64, error) {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	return processor.next, nil
}

func (processor *recordingProcessor) Process(_ context.Context, event Event) error {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	processor.offsets = append(processor.offsets, event.Offset)
	processor.next = event.Offset + 1
	return nil
}

func (processor *recordingProcessor) Flush(context.Context) error { return nil }

func (processor *recordingProcessor) seen() []int64 {
	processor.mu.Lock()
	defer processor.mu.Unlock()
	return append([]int64(nil), processor.offsets...)
}

func writeFrame(w io.Writer, offset int64) {
	fmt.Fprintf(w, "id: %d\nevent: retract\ndata: {\"offset\":%d,\"key\":\"k%d\"}\n\n", offset, offset, offset)
}

type feedHandler func(w http.ResponseWriter, from int64, connection int)

func fakeFeed(t *testing.T, handler feedHandler) (*url.URL, *atomic.Int32) {
	t.Helper()
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer feed-token" || request.URL.Path != "/v1/stream" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		from, _ := strconv.ParseInt(request.URL.Query().Get("from"), 10, 64)
		handler(w, from, int(connections.Add(1)))
	}))
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	return base, &connections
}

func testStreamer(base *url.URL, processor Processor, idle time.Duration) (*streamer, *[]time.Duration, *sync.Mutex) {
	feed := newStreamer(Config{BaseURL: base, Token: "feed-token", IdleTimeout: idle}, processor, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	var waits []time.Duration
	var mu sync.Mutex
	feed.wait = func(ctx context.Context, delay time.Duration) bool {
		mu.Lock()
		waits = append(waits, delay)
		mu.Unlock()
		return sleepContext(ctx, 5*time.Millisecond)
	}
	return feed, &waits, &mu
}

func runUntil(t *testing.T, feed *streamer, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	finished := make(chan struct{})
	go func() { defer close(finished); feed.run(ctx) }()
	for !done() {
		if ctx.Err() != nil {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-finished
}

func assertContiguous(t *testing.T, offsets []int64, total int) {
	t.Helper()
	if len(offsets) < total {
		t.Fatalf("got %d offsets, want %d", len(offsets), total)
	}
	for index, offset := range offsets[:total] {
		if offset != int64(index) {
			t.Fatalf("offset %d at position %d: lost or repeated data", offset, index)
		}
	}
}

func TestResumesFromTheNextOffsetAfterEveryDrop(t *testing.T) {
	const total = 100
	base, connections := fakeFeed(t, func(w http.ResponseWriter, from int64, _ int) {
		// Every connection ends after 7 events, as if the network dropped.
		for offset := from; offset < from+7 && offset < total; offset++ {
			writeFrame(w, offset)
		}
	})
	processor := &recordingProcessor{}
	feed, _, _ := testStreamer(base, processor, time.Second)
	runUntil(t, feed, func() bool { return len(processor.seen()) >= total })
	assertContiguous(t, processor.seen(), total)
	if connections.Load() < total/7 {
		t.Fatalf("expected repeated reconnects, got %d", connections.Load())
	}
}

func TestReplayedOffsetsAreSkippedAndGapsReconnect(t *testing.T) {
	base, _ := fakeFeed(t, func(w http.ResponseWriter, from int64, connection int) {
		switch connection {
		case 1:
			writeFrame(w, 0)
			writeFrame(w, 1)
			writeFrame(w, 3)
		default:
			for _, offset := range []int64{from - 1, from, from + 1, from + 2} {
				writeFrame(w, offset)
			}
		}
	})
	processor := &recordingProcessor{}
	feed, _, _ := testStreamer(base, processor, time.Second)
	runUntil(t, feed, func() bool { return len(processor.seen()) >= 5 })
	assertContiguous(t, processor.seen(), 5)
}

func TestTooManyConnectionsWaitsAtLeastRetryAfter(t *testing.T) {
	base, _ := fakeFeed(t, func(w http.ResponseWriter, from int64, connection int) {
		if connection == 1 {
			w.Header().Set("Retry-After", "4")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeFrame(w, from)
	})
	processor := &recordingProcessor{}
	feed, waits, mu := testStreamer(base, processor, time.Second)
	runUntil(t, feed, func() bool { return len(processor.seen()) >= 1 })
	mu.Lock()
	defer mu.Unlock()
	if len(*waits) == 0 || (*waits)[0] < 4*time.Second {
		t.Fatalf("first wait %v must respect Retry-After", *waits)
	}
}

func TestBackoffGrowsAndUnauthorizedWaitsLong(t *testing.T) {
	if delay := backoff(1, nil); delay < minimumBackoff/2 || delay > minimumBackoff {
		t.Fatalf("first backoff %v", delay)
	}
	if delay := backoff(20, nil); delay > maximumBackoff {
		t.Fatalf("backoff %v exceeds the cap", delay)
	}
	if delay := backoff(1, &feedError{status: http.StatusUnauthorized}); delay != unauthorizedBackoff {
		t.Fatalf("unauthorized backoff %v", delay)
	}
	if delay := backoff(1, &feedError{status: http.StatusServiceUnavailable, retryAfter: 9 * time.Second}); delay < 9*time.Second {
		t.Fatalf("503 backoff %v must respect Retry-After", delay)
	}
}

func TestSilentStreamIsDroppedAndReopened(t *testing.T) {
	base, connections := fakeFeed(t, func(w http.ResponseWriter, from int64, connection int) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		if connection == 1 {
			time.Sleep(2 * time.Second)
			return
		}
		writeFrame(w, from)
	})
	processor := &recordingProcessor{}
	feed, _, _ := testStreamer(base, processor, 100*time.Millisecond)
	start := time.Now()
	runUntil(t, feed, func() bool { return len(processor.seen()) >= 1 })
	if connections.Load() < 2 || time.Since(start) > 1500*time.Millisecond {
		t.Fatalf("silent stream was not abandoned quickly: %d connections in %v", connections.Load(), time.Since(start))
	}
}

func TestMalformedFrameKeepsItsOffset(t *testing.T) {
	base, _ := fakeFeed(t, func(w http.ResponseWriter, from int64, _ int) {
		if from == 0 {
			fmt.Fprint(w, "id: 0\nevent: detection\ndata: {\"offset\":0}\n\n")
		}
		writeFrame(w, max(from, 1))
	})
	processor := &recordingProcessor{}
	feed, _, _ := testStreamer(base, processor, time.Second)
	runUntil(t, feed, func() bool { return len(processor.seen()) >= 2 })
	assertContiguous(t, processor.seen(), 2)
}
