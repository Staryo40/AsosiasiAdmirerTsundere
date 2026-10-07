package aggregator

import (
	"encoding/json"
	"slices"
	"strings"
	"time"
)

type tsunamiWarning struct {
	id, eventID string
	issuedAt    time.Time
	severity    Severity
	fields      rawObject
}

var warningSeverity = map[string]Severity{"Waspada": Waspada, "Siaga": Siaga, "Awas": Awas}

func parseWarning(raw json.RawMessage) (tsunamiWarning, error) {
	fields, err := parseObject(raw)
	if err != nil {
		return tsunamiWarning{}, err
	}
	warning := tsunamiWarning{fields: fields}
	if warning.id, err = textField(fields, "warning_id"); err != nil {
		return tsunamiWarning{}, err
	}
	if warning.eventID, err = textField(fields, "related_event_id"); err != nil {
		return tsunamiWarning{}, err
	}
	if warning.issuedAt, err = timeField(fields, "issued_at"); err != nil {
		return tsunamiWarning{}, err
	}
	if _, err = timeField(fields, "estimated_arrival"); err != nil {
		return tsunamiWarning{}, err
	}
	if warning.severity, err = severityField(fields, "threat_level", warningSeverity); err != nil {
		return tsunamiWarning{}, err
	}
	if err := warningZones(fields); err != nil {
		return tsunamiWarning{}, err
	}
	return warning, nil
}

func warningZones(fields rawObject) error {
	zones, err := required[[]json.RawMessage](fields, "affected_zones")
	if err != nil {
		return err
	}
	for _, raw := range zones {
		if _, err := textField(rawObject{"affected_zones": raw}, "affected_zones"); err != nil {
			return err
		}
	}
	return nil
}

func relatedWarnings(records []json.RawMessage, eventID string) ([]tsunamiWarning, error) {
	warnings := make([]tsunamiWarning, 0, len(records))
	seen := make(map[string]bool)
	for _, raw := range records {
		warning, err := parseWarning(raw)
		if err != nil {
			return nil, err
		}
		if warning.eventID != eventID {
			continue
		}
		if seen[warning.id] {
			return nil, invalidField("warning_id", InvalidValue)
		}
		seen[warning.id] = true
		warnings = append(warnings, warning)
	}
	slices.SortFunc(warnings, compareWarnings)
	return warnings, nil
}

func compareWarnings(left, right tsunamiWarning) int {
	if order := left.issuedAt.Compare(right.issuedAt); order != 0 {
		return order
	}
	return strings.Compare(left.id, right.id)
}

func warningPayloads(warnings []tsunamiWarning) []rawObject {
	payloads := make([]rawObject, 0, len(warnings))
	for _, warning := range warnings {
		payloads = append(payloads, warning.fields)
	}
	return payloads
}
