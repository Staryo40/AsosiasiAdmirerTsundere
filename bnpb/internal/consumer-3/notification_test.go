package consumer3

import (
	"encoding/json"
	"testing"
)

func TestUnsupportedNotificationsDoNotConsumeIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   int
		revision  int64
		eventType string
	}{
		{"missing version", 0, 1, "hazard.created"},
		{"future version", 2, 2, "hazard.updated"},
		{"retraction", 1, 2, "hazard.retracted"},
		{"created revision mismatch", 1, 2, "hazard.created"},
		{"updated revision mismatch", 1, 1, "hazard.updated"},
		{"zero revision", 1, 0, "hazard.created"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t)
			worker := &consumer{store: store}
			env := enumEnvelope("SATELIT", "WILDFIRE")
			raw, err := json.Marshal(map[string]any{
				"schema_version": test.version, "revision": test.revision, "event_type": test.eventType,
				"hazard_id": env.HazardID, "hazard_event": env.HazardEvent,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatal(err)
			}
			if err := worker.applyEnvelope(t.Context(), "same-message", env); err == nil {
				t.Fatal("unsupported notification was durably accepted")
			}
			stats, err := store.Stats(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if stats.MessagesSeen != 0 || stats.FeedEntries != 0 {
				t.Fatal("invalid notification produced a durable effect")
			}
			if err := worker.applyEnvelope(t.Context(), "same-message", enumEnvelope("SATELIT", "WILDFIRE")); err != nil {
				t.Fatalf("valid retry lost its identity: %v", err)
			}
		})
	}
}

func TestSupportedUpdateProducesDurableEffect(t *testing.T) {
	store := openTestStore(t)
	env := enumEnvelope("SATELIT", "WILDFIRE")
	env.Revision, env.EventType = 2, "hazard.updated"
	if err := (&consumer{store: store}).applyEnvelope(t.Context(), "updated-message", env); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(t.Context())
	if err != nil || stats.FeedEntries != 1 || stats.MessagesSeen != 1 {
		t.Fatalf("durable updated feed: %+v, %v", stats, err)
	}
}
