package aggregator

import (
	"encoding/json"
	"time"
)

type Source string
type HazardType string
type Severity string

const (
	BMKG     Source     = "BMKG"
	PVMBG    Source     = "PVMBG"
	Seismic  HazardType = "SEISMIC"
	Volcanic HazardType = "VOLCANIC"
	Normal   Severity   = "NORMAL"
	Waspada  Severity   = "WASPADA"
	Siaga    Severity   = "SIAGA"
	Awas     Severity   = "AWAS"
)

// HazardEvent contains only the public canonical fields; revisions belong to storage.
type HazardEvent struct {
	HazardID    string                     `json:"hazard_id"`
	Source      Source                     `json:"source"`
	SourceRefID string                     `json:"source_ref_id"`
	HazardType  HazardType                 `json:"hazard_type"`
	Severity    Severity                   `json:"severity"`
	AreaName    string                     `json:"area_name"`
	Latitude    float64                    `json:"latitude"`
	Longitude   float64                    `json:"longitude"`
	OccurredAt  time.Time                  `json:"occurred_at"`
	IngestedAt  time.Time                  `json:"ingested_at"`
	Attributes  map[string]json.RawMessage `json:"attributes"`
}

// Receipt supplies the persisted identity and first receipt time, keeping mapping pure.
type Receipt struct {
	HazardID   string
	IngestedAt time.Time
}

type ValidationCode string

const (
	MissingField   ValidationCode = "missing_field"
	NullField      ValidationCode = "null_field"
	WrongType      ValidationCode = "wrong_type"
	InvalidValue   ValidationCode = "invalid_value"
	InvalidJSON    ValidationCode = "invalid_json"
	DuplicateKey   ValidationCode = "duplicate_key"
	UnknownVolcano ValidationCode = "unknown_volcano"
)

// ValidationError is safe to log: neither source values nor unknown key names appear.
type ValidationError struct {
	Field string
	Code  ValidationCode
}

func (err *ValidationError) Error() string { return "mapping: " + err.Field + ": " + string(err.Code) }

func invalidField(field string, code ValidationCode) error {
	return &ValidationError{Field: field, Code: code}
}

func (receipt Receipt) validate() error {
	if !validText(receipt.HazardID) {
		return invalidField("hazard_id", InvalidValue)
	}
	if !validInstant(receipt.IngestedAt) {
		return invalidField("ingested_at", InvalidValue)
	}
	return nil
}
