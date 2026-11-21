package satellite

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// Frames copied from the live practice feed.
const feedSample = `: comment lines are ignored
id: 0
event: detection
data: {"offset":0,"detection":{"version":"2.0NRT","scan":0.43,"acq_date":"2026-11-28","acq_time":2350,"bright_ti4":335.67,"bright_ti5":295.97,"confidence":"l","daynight":"D","frp":9.65,"instrument":"VIIRS","latitude":-3.97749,"longitude":118.31730,"satellite":"N20","track":0.43}}

id: 11
event: update
data: {"offset":11,"key":"N21|2026-11-28|2356|-0.15865|122.03404","changes":{"bright_ti4":304.99,"confidence":"l","frp":7.26}}

id: 50
event: retract
data: {"offset":50,"reason":"false_positive","key":"N20|2026-11-28|2357|-0.16571|122.03576"}

id: 450
event: heartbeat
data: {"offset":450,"virtual_now":"2026-11-29T01:45:00Z"}

`

func readAll(t *testing.T, raw string) []Event {
	t.Helper()
	reader := newSSEReader(strings.NewReader(raw))
	var events []Event
	for {
		frame, err := reader.next()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatal(err)
		}
		event, err := parseEvent(frame)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
}

func TestParsesEveryFeedEventKind(t *testing.T) {
	events := readAll(t, feedSample)
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}
	detection := events[0]
	if detection.Kind != KindDetection || detection.Offset != 0 || detection.Detection.Confidence != "l" {
		t.Fatalf("detection: %+v", detection)
	}
	// Trailing zeros and the unpadded acq_time must survive exactly as sent.
	if key := detection.Detection.Key(); key != "N20|2026-11-28|2350|-3.97749|118.31730" {
		t.Fatalf("detection key %q", key)
	}
	if events[1].Kind != KindUpdate || events[1].Key != "N21|2026-11-28|2356|-0.15865|122.03404" || string(events[1].Changes["confidence"]) != `"l"` {
		t.Fatalf("update: %+v", events[1])
	}
	if events[2].Kind != KindRetract || events[2].Reason != "false_positive" {
		t.Fatalf("retract: %+v", events[2])
	}
	if events[3].Kind != KindHeartbeat || !events[3].VirtualNow.Equal(time.Date(2026, 11, 29, 1, 45, 0, 0, time.UTC)) {
		t.Fatalf("heartbeat: %+v", events[3])
	}
}

func TestSSEJoinsDataLinesAndAcceptsCRLF(t *testing.T) {
	raw := "id: 7\r\nevent: retract\r\ndata: {\"offset\":7,\r\ndata: \"key\":\"k\"}\r\n\r\n"
	events := readAll(t, raw)
	if len(events) != 1 || events[0].Offset != 7 || events[0].Key != "k" {
		t.Fatalf("events %+v", events)
	}
}

func TestDetectionTimeIsHHMMInUTC(t *testing.T) {
	for raw, want := range map[string]time.Time{
		"2":    time.Date(2026, 11, 29, 0, 2, 0, 0, time.UTC),
		"2350": time.Date(2026, 11, 29, 23, 50, 0, 0, time.UTC),
	} {
		detection := Detection{AcqDate: "2026-11-29", AcqTime: jsonNumber(raw)}
		if got, err := detection.DetectedAt(); err != nil || !got.Equal(want) {
			t.Errorf("acq_time %s: %s %v", raw, got, err)
		}
	}
	for _, raw := range []string{"2360", "2400", "-1", "12.5"} {
		if _, err := (Detection{AcqDate: "2026-11-29", AcqTime: jsonNumber(raw)}).DetectedAt(); err == nil {
			t.Errorf("acq_time %s must be rejected", raw)
		}
	}
}

func TestMalformedFramesAreRejected(t *testing.T) {
	for _, frame := range []sseFrame{
		{id: "1", event: KindDetection, data: []byte(`{"offset":1}`)},
		{id: "2", event: KindDetection, data: []byte(`{"offset":3,"detection":{"satellite":"N20"}}`)},
		{id: "4", event: KindUpdate, data: []byte(`{"offset":4,"key":"k"}`)},
		{id: "5", event: KindHeartbeat, data: []byte(`{"offset":5,"virtual_now":"yesterday"}`)},
		{id: "6", event: KindDetection, data: []byte(`{"offset":6,"detection":`)},
	} {
		if _, err := parseEvent(frame); !errors.Is(err, errMalformedEvent) {
			t.Errorf("frame %s must be malformed, got %v", frame.id, err)
		}
	}
}

func TestOversizedFramesStopTheReader(t *testing.T) {
	reader := newSSEReader(strings.NewReader("data: " + strings.Repeat("x", 70<<10) + "\n\n"))
	if _, err := reader.next(); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("expected frame too large, got %v", err)
	}
}
