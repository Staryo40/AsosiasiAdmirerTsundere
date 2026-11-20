package consumer2

import (
	"testing"
	"time"
)

var validHazardPairs = []struct {
	name       string
	source     string
	hazardType string
}{
	{name: "BMKG seismic", source: "BMKG", hazardType: "SEISMIC"},
	{name: "PVMBG volcanic", source: "PVMBG", hazardType: "VOLCANIC"},
	{name: "SATELIT wildfire", source: "SATELIT", hazardType: "WILDFIRE"},
	{name: "SENSOR_PANTAI coastal", source: "SENSOR_PANTAI", hazardType: "COASTAL"},
}

var invalidHazardPairs = []struct {
	name       string
	source     string
	hazardType string
}{
	{name: "BMKG volcanic", source: "BMKG", hazardType: "VOLCANIC"},
	{name: "BMKG wildfire", source: "BMKG", hazardType: "WILDFIRE"},
	{name: "BMKG coastal", source: "BMKG", hazardType: "COASTAL"},
	{name: "PVMBG seismic", source: "PVMBG", hazardType: "SEISMIC"},
	{name: "PVMBG wildfire", source: "PVMBG", hazardType: "WILDFIRE"},
	{name: "PVMBG coastal", source: "PVMBG", hazardType: "COASTAL"},
	{name: "SATELIT seismic", source: "SATELIT", hazardType: "SEISMIC"},
	{name: "SATELIT volcanic", source: "SATELIT", hazardType: "VOLCANIC"},
	{name: "SATELIT coastal", source: "SATELIT", hazardType: "COASTAL"},
	{name: "SENSOR_PANTAI seismic", source: "SENSOR_PANTAI", hazardType: "SEISMIC"},
	{name: "SENSOR_PANTAI volcanic", source: "SENSOR_PANTAI", hazardType: "VOLCANIC"},
	{name: "SENSOR_PANTAI wildfire", source: "SENSOR_PANTAI", hazardType: "WILDFIRE"},
	{name: "unknown source", source: "UNKNOWN", hazardType: "SEISMIC"},
	{name: "unknown type", source: "BMKG", hazardType: "UNKNOWN"},
	{name: "TSUNAMI", source: "BMKG", hazardType: "TSUNAMI"},
}

func enumEnvelope(source, hazardType string) envelope {
	var env envelope
	env.SchemaVersion = 1
	env.Revision = 1
	env.EventType = "hazard.created"
	env.HazardID = "hz-enum"
	env.HazardEvent.Source = source
	env.HazardEvent.HazardType = hazardType
	env.HazardEvent.Severity = "SIAGA"
	env.HazardEvent.AreaName = "Test Area"
	env.HazardEvent.OccurredAt = time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
	return env
}

func TestApplyEnvelopeAcceptsValidSourceTypePairs(t *testing.T) {
	for _, pair := range validHazardPairs {
		t.Run(pair.name, func(t *testing.T) {
			store := openTestStore(t)
			result, err := (&consumer{store: store}).applyEnvelope(t.Context(), "valid-message", enumEnvelope(pair.source, pair.hazardType))
			if err != nil {
				t.Fatal(err)
			}
			if !result.AlertSent {
				t.Fatal("valid SIAGA event did not create an alert")
			}
			stats, err := store.Stats(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if stats.AlertsSent != 1 || stats.MessagesSeen != 1 {
				t.Fatalf("valid event was not durably applied: %+v", stats)
			}
		})
	}
}

func TestApplyEnvelopeRejectsInvalidSourceTypePairsBeforeDedupe(t *testing.T) {
	for _, pair := range invalidHazardPairs {
		t.Run(pair.name, func(t *testing.T) {
			store := openTestStore(t)
			worker := &consumer{store: store}
			if _, err := worker.applyEnvelope(t.Context(), "retry-message", enumEnvelope(pair.source, pair.hazardType)); err == nil {
				t.Fatal("invalid source/type pair was accepted")
			}
			stats, err := store.Stats(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if stats.MessagesSeen != 0 || stats.AlertsSent != 0 {
				t.Fatalf("invalid event had a durable effect: %+v", stats)
			}
			result, err := worker.applyEnvelope(t.Context(), "retry-message", enumEnvelope("BMKG", "SEISMIC"))
			if err != nil {
				t.Fatalf("valid retry with same message ID failed: %v", err)
			}
			if !result.AlertSent {
				t.Fatal("valid retry did not create an alert")
			}
		})
	}
}
