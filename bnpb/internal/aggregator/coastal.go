package aggregator

import (
	"encoding/json"
	"math"
	"time"
)

var pantaiQualities = map[string]struct{}{"ok": {}, "degraded": {}}

// MapCoastal maps a Sensor Pantai observation to the canonical HazardEvent.
func MapCoastal(raw json.RawMessage, station pantaiStation, generatedAt time.Time, staleAfter time.Duration, receipt Receipt) (HazardEvent, error) {
	fields, err := mappingFields(raw, receipt)
	if err != nil {
		return HazardEvent{}, err
	}
	hazard, err := coastalCore(fields, station, generatedAt, staleAfter)
	if err != nil {
		return HazardEvent{}, err
	}
	if err := coastalMeasurements(fields); err != nil {
		return HazardEvent{}, err
	}
	removeMappedFields(fields, "observation_id", "station_id", "observed_at", "sea_level_anomaly_m", "wave_height_m", "quality", "critical")
	if err := namespaceSourceExtras(fields, "source_extras"); err != nil {
		return HazardEvent{}, err
	}
	hazard.Attributes = fields
	hazard.HazardID, hazard.IngestedAt = receipt.HazardID, receipt.IngestedAt.UTC()
	return hazard, nil
}

func coastalCore(fields rawObject, station pantaiStation, generatedAt time.Time, staleAfter time.Duration) (HazardEvent, error) {
	hazard := HazardEvent{Source: SensorPantai, HazardType: Coastal}
	var err error
	if hazard.SourceRefID, err = textField(fields, "observation_id"); err != nil {
		return HazardEvent{}, err
	}
	if _, err = textField(fields, "station_id"); err != nil {
		return HazardEvent{}, err
	}
	if hazard.OccurredAt, err = timeField(fields, "observed_at"); err != nil {
		return HazardEvent{}, err
	}
	if staleAfter > 0 && generatedAt.Sub(hazard.OccurredAt) > staleAfter {
		return HazardEvent{}, invalidField("observed_at", InvalidValue)
	}
	quality, err := textField(fields, "quality")
	if err != nil {
		return HazardEvent{}, err
	}
	if _, allowed := pantaiQualities[quality]; !allowed {
		return HazardEvent{}, invalidField("quality", InvalidValue)
	}
	critical, err := required[bool](fields, "critical")
	if err != nil {
		return HazardEvent{}, err
	}
	hazard.AreaName = station.Name
	if hazard.AreaName == "" {
		hazard.AreaName = station.Province
	}
	hazard.Latitude = station.Latitude
	hazard.Longitude = station.Longitude
	anomaly, err := numberField(fields, "sea_level_anomaly_m", -math.MaxFloat64, math.MaxFloat64)
	if err != nil {
		return HazardEvent{}, err
	}
	wave, err := numberField(fields, "wave_height_m", 0, math.MaxFloat64)
	if err != nil {
		return HazardEvent{}, err
	}
	hazard.Severity = coastalSeverity(critical, anomaly, wave)
	return hazard, nil
}

func coastalMeasurements(fields rawObject) error {
	if _, err := numberField(fields, "sea_level_anomaly_m", -math.MaxFloat64, math.MaxFloat64); err != nil {
		return err
	}
	if _, err := numberField(fields, "wave_height_m", 0, math.MaxFloat64); err != nil {
		return err
	}
	return nil
}

func coastalSeverity(critical bool, anomaly, wave float64) Severity {
	if critical {
		return Awas
	}
	if anomaly >= 0.5 || wave >= 3.0 {
		return Awas
	}
	if anomaly >= 0.3 || wave >= 2.0 {
		return Siaga
	}
	if anomaly >= 0.15 || wave >= 1.5 {
		return Waspada
	}
	return Normal
}