package aggregator

import (
	"encoding/json"
	"math"
)

var volcanicSeverity = map[string]Severity{"Normal": Normal, "Waspada": Waspada, "Siaga": Siaga, "Awas": Awas}

// MapVolcanic supports V1/V2 coexistence; absent confidence stays absent.
func MapVolcanic(raw json.RawMessage, receipt Receipt) (HazardEvent, error) {
	fields, err := mappingFields(raw, receipt)
	if err != nil {
		return HazardEvent{}, err
	}
	hazard, err := volcanicCore(fields)
	if err != nil {
		return HazardEvent{}, err
	}
	if err := volcanicMeasurements(fields); err != nil {
		return HazardEvent{}, err
	}
	removeMappedFields(fields, "report_id", "reported_at", "alert_level")
	if err := namespaceSourceExtras(fields, "source_extras"); err != nil {
		return HazardEvent{}, err
	}
	hazard.Attributes = fields
	hazard.HazardID, hazard.IngestedAt = receipt.HazardID, receipt.IngestedAt.UTC()
	return hazard, nil
}

func volcanicCore(fields rawObject) (HazardEvent, error) {
	hazard := HazardEvent{Source: PVMBG, HazardType: Volcanic}
	var err error
	if hazard.SourceRefID, err = textField(fields, "report_id"); err != nil {
		return HazardEvent{}, err
	}
	if hazard.OccurredAt, err = timeField(fields, "reported_at"); err != nil {
		return HazardEvent{}, err
	}
	if hazard.Severity, err = severityField(fields, "alert_level", volcanicSeverity); err != nil {
		return HazardEvent{}, err
	}
	volcanoID, err := textField(fields, "volcano_id")
	if err != nil {
		return HazardEvent{}, err
	}
	volcano, found := volcanoReference[volcanoID]
	if !found {
		return HazardEvent{}, invalidField("volcano_id", UnknownVolcano)
	}
	hazard.AreaName, hazard.Latitude, hazard.Longitude = volcano.Name, volcano.Latitude, volcano.Longitude
	return hazard, nil
}

func volcanicMeasurements(fields rawObject) error {
	count, err := required[int32](fields, "eruption_count_24h")
	if err != nil {
		return err
	}
	if count < 0 {
		return invalidField("eruption_count_24h", InvalidValue)
	}
	if _, err := numberField(fields, "ash_column_height_m", 0, math.MaxFloat64); err != nil {
		return err
	}
	if _, present := fields["confidence_level"]; present {
		_, err = numberField(fields, "confidence_level", 0, 1)
	}
	return err
}
