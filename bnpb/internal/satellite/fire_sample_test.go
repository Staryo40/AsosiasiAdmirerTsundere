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
	final := map[string]fire.Detection{}
	gone := map[string]bool{}
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
		// Mirror what the engine accepted, so the batch input is exactly the detections that count.
		switch {
		case event.Kind == KindDetection && outcome == fire.Added:
			detection, _ := engineDetection(*event.Detection)
			final[detection.Key] = detection
		case event.Kind == KindRetract && outcome == fire.Applied:
			gone[event.Key] = true
		case event.Kind == KindUpdate && outcome == fire.Applied:
			if detection, ok := final[event.Key]; ok {
				var updated fire.Detection
				updated = detection
				if raw, present := event.Changes["confidence"]; present {
					updated.Confidence = strings.Trim(string(raw), `"`)
				}
				final[event.Key] = updated
			}
		}
		if events%500 == 0 {
			drain(engine, published)
		}
	}
	drain(engine, published)
	elapsed := time.Since(started)

	var counted []fire.Detection
	for key, detection := range final {
		if !gone[key] && detection.Confidence != "l" {
			counted = append(counted, detection)
		}
	}
	var got [][]string
	for _, published := range published {
		got = append(got, published.DetectionKeys)
	}
	sort.Slice(got, func(i, j int) bool { return strings.Join(got[i], ",") < strings.Join(got[j], ",") })
	want := fire.Batch(counted)
	stats := engine.Stats()
	t.Logf("%d events in %v (%.0f events/s): %d fires, %d singletons, %d late, %d late updates, %d orphans, %d duplicates",
		events, elapsed.Round(time.Millisecond), float64(events)/elapsed.Seconds(), len(got), stats.Singletons, stats.Late,
		stats.LateUpdates, stats.Orphans, stats.Duplicates)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("published fires (%d) differ from batch clustering (%d)", len(got), len(want))
	}
}

func drain(engine *fire.Engine, published map[string]fire.Fire) {
	for _, change := range engine.Drain() {
		if change.Active {
			published[change.ID] = change
		} else {
			delete(published, change.ID)
		}
	}
}
