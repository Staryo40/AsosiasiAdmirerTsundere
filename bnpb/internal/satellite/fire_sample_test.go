package satellite

import (
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/satellite/fire"
)

// Replays a recorded stream (set BNPB_TEST_FEED_SAMPLE to an SSE capture) and checks the published fires
// against batch clustering of the detections that should count.
func TestRecordedFeedFiresEqualBatchClustering(t *testing.T) {
	path := os.Getenv("BNPB_TEST_FEED_SAMPLE")
	if path == "" {
		t.Skip("set BNPB_TEST_FEED_SAMPLE to a captured feed stream")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	engine := fire.NewEngine()
	reader := newSSEReader(file)
	published := map[string]fire.Fire{}
	accepted := newAcceptedDetections()
	events, started := 0, time.Now()
	for {
		frame, err := reader.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		event, err := parseEvent(frame)
		if err != nil {
			t.Fatalf("frame %s: %v", frame.id, err)
		}
		outcome, err := applyToEngine(engine, event)
		if err != nil {
			t.Fatalf("offset %d: %v", event.Offset, err)
		}
		events++
		accepted.record(event, outcome)
		if events%500 == 0 {
			drainActive(engine, published)
		}
	}
	drainActive(engine, published)
	elapsed := time.Since(started)

	var got [][]string
	for _, published := range published {
		got = append(got, published.DetectionKeys)
	}
	sort.Slice(got, func(i, j int) bool { return strings.Join(got[i], ",") < strings.Join(got[j], ",") })
	want := accepted.batch()
	stats := engine.Stats()
	t.Logf("%d events in %v (%.0f events/s): %d fires, %d singletons, %d late, %d late updates, %d orphans, %d duplicates",
		events, elapsed.Round(time.Millisecond), float64(events)/elapsed.Seconds(), len(got), stats.Singletons, stats.Late,
		stats.LateUpdates, stats.Orphans, stats.Duplicates)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("published fires (%d) differ from batch clustering (%d)", len(got), len(want))
	}
}
