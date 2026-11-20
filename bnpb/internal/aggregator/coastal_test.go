package aggregator

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMapCoastalSeverity(t *testing.T) {
	raw := json.RawMessage(`{
		"observation_id":"obs-1","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:00Z",
		"sea_level_anomaly_m":0.1,"wave_height_m":1.0,"quality":"ok","critical":false}`)
	station := pantaiStation{StationID: "stn-padang-01", Name: "Padang", Latitude: -0.9, Longitude: 100.3}
	event, err := MapCoastal(raw, station, time.Date(2026, 11, 12, 2, 43, 5, 0, time.UTC), time.Hour, newReceipt(time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	if event.HazardType != Coastal || event.Source != SensorPantai || event.Severity != Normal {
		t.Fatalf("unexpected canonical mapping: %+v", event)
	}
}

func TestMapCoastalRejectsBadQuality(t *testing.T) {
	raw := json.RawMessage(`{
		"observation_id":"obs-2","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:00Z",
		"sea_level_anomaly_m":0.1,"wave_height_m":1.0,"quality":"bad","critical":false}`)
	station := pantaiStation{Name: "Padang"}
	_, err := MapCoastal(raw, station, time.Now().UTC(), time.Hour, newReceipt(time.Now().UTC()))
	if err == nil {
		t.Fatal("expected invalid quality to be rejected")
	}
}