package aggregator

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
)

func MapSeismic(raw json.RawMessage, warnings []json.RawMessage, receipt Receipt) (HazardEvent, error) {
	fields, err := mappingFields(raw, receipt)
	if err != nil {
		return HazardEvent{}, err
	}
	hazard, err := seismicCore(fields)
	if err != nil {
		return HazardEvent{}, err
	}
	matched, err := relatedWarnings(warnings, hazard.SourceRefID)
	if err != nil {
		return HazardEvent{}, err
	}
	hazard.Severity = seismicSeverity(fields["magnitude"], matched)
	if hazard.Attributes, err = seismicAttributes(fields, matched); err != nil {
		return HazardEvent{}, err
	}
	hazard.HazardID, hazard.IngestedAt = receipt.HazardID, receipt.IngestedAt.UTC()
	return hazard, nil
}

func seismicCore(fields rawObject) (HazardEvent, error) {
	hazard := HazardEvent{Source: BMKG, HazardType: Seismic}
	var err error
	if hazard.SourceRefID, err = textField(fields, "event_id"); err != nil {
		return HazardEvent{}, err
	}
	if hazard.AreaName, err = textField(fields, "region_name"); err != nil {
		return HazardEvent{}, err
	}
	if hazard.Latitude, err = numberField(fields, "epicenter_lat", -90, 90); err != nil {
		return HazardEvent{}, err
	}
	if hazard.Longitude, err = numberField(fields, "epicenter_lon", -180, 180); err != nil {
		return HazardEvent{}, err
	}
	if hazard.OccurredAt, err = timeField(fields, "occurred_at"); err != nil {
		return HazardEvent{}, err
	}
	if _, err = numberField(fields, "magnitude", -math.MaxFloat64, math.MaxFloat64); err != nil {
		return HazardEvent{}, err
	}
	if _, err = numberField(fields, "depth_km", 0, math.MaxFloat64); err != nil {
		return HazardEvent{}, err
	}
	if _, err = required[bool](fields, "potential_tsunami"); err != nil {
		return HazardEvent{}, err
	}
	return hazard, nil
}

func magnitudeSeverity(raw json.RawMessage) Severity {
	if decimalIdentity(json.Number(bytes.TrimSpace(raw))) == "0" {
		return Normal
	}
	magnitude, _ := new(big.Rat).SetString(string(bytes.TrimSpace(raw)))
	if magnitude.Cmp(big.NewRat(5, 1)) < 0 {
		return Normal
	}
	if magnitude.Cmp(big.NewRat(13, 2)) < 0 {
		return Waspada
	}
	return Siaga
}

func seismicSeverity(magnitude json.RawMessage, warnings []tsunamiWarning) Severity {
	if len(warnings) == 0 {
		return magnitudeSeverity(magnitude)
	}
	severity := Waspada
	for _, warning := range warnings {
		if warning.severity == Awas || (warning.severity == Siaga && severity != Awas) {
			severity = warning.severity
		}
	}
	return severity
}

func seismicAttributes(fields rawObject, warnings []tsunamiWarning) (map[string]json.RawMessage, error) {
	if len(warnings) > 0 && string(bytes.TrimSpace(fields["potential_tsunami"])) != "true" {
		return nil, invalidField("potential_tsunami", InvalidValue)
	}
	removeMappedFields(fields, "event_id", "region_name", "epicenter_lat", "epicenter_lon", "occurred_at")
	if err := namespaceSourceExtras(fields, "source_extras", "tsunami_warnings"); err != nil {
		return nil, err
	}
	if len(warnings) == 0 {
		return fields, nil
	}
	raw, err := json.Marshal(warningPayloads(warnings))
	if err != nil {
		return nil, err
	}
	fields["tsunami_warnings"] = raw
	return fields, nil
}
