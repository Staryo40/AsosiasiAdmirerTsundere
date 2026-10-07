package api

import (
	"encoding/json"
	"errors"
	"time"
)

type hazardEvent struct {
	HazardID    string          `json:"hazard_id"`
	Source      string          `json:"source"`
	SourceRefID string          `json:"source_ref_id"`
	HazardType  string          `json:"hazard_type"`
	Severity    string          `json:"severity"`
	AreaName    string          `json:"area_name"`
	Latitude    float64         `json:"latitude"`
	Longitude   float64         `json:"longitude"`
	OccurredAt  string          `json:"occurred_at"`
	IngestedAt  string          `json:"ingested_at"`
	Attributes  json.RawMessage `json:"attributes"`
}

type hazardSummary struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
	IngestedAt string `json:"ingested_at"`
}

type listEnvelope struct {
	Items      []hazardEvent   `json:"items"`
	NextCursor *string         `json:"next_cursor"`
	Sources    json.RawMessage `json:"sources"`
	AsOf       string          `json:"as_of"`
}

type detailEnvelope struct {
	Item    hazardEvent     `json:"item"`
	Sources json.RawMessage `json:"sources"`
	AsOf    string          `json:"as_of"`
}

func projectList(payload []byte, view string) (any, error) {
	var envelope listEnvelope
	if err := decodeStrictBytes(payload, &envelope); err != nil {
		return nil, err
	}
	if envelope.Items == nil || !validMetadata(envelope.Sources, envelope.AsOf) {
		return nil, errors.New("invalid list envelope")
	}
	for _, item := range envelope.Items {
		if err := validateHazard(item); err != nil {
			return nil, err
		}
	}
	if view == viewFull {
		return envelope, nil
	}
	items := make([]hazardSummary, 0, len(envelope.Items))
	for _, item := range envelope.Items {
		items = append(items, summarize(item))
	}
	return struct {
		Items      []hazardSummary `json:"items"`
		NextCursor *string         `json:"next_cursor"`
		Sources    json.RawMessage `json:"sources"`
		AsOf       string          `json:"as_of"`
	}{items, envelope.NextCursor, envelope.Sources, envelope.AsOf}, nil
}

func projectDetail(payload []byte, view string) (any, error) {
	var envelope detailEnvelope
	if err := decodeStrictBytes(payload, &envelope); err != nil {
		return nil, err
	}
	if err := validateHazard(envelope.Item); err != nil || !validMetadata(envelope.Sources, envelope.AsOf) {
		return nil, errors.New("invalid detail envelope")
	}
	if view == viewFull {
		return envelope, nil
	}
	return struct {
		Item    hazardSummary   `json:"item"`
		Sources json.RawMessage `json:"sources"`
		AsOf    string          `json:"as_of"`
	}{summarize(envelope.Item), envelope.Sources, envelope.AsOf}, nil
}

func summarize(event hazardEvent) hazardSummary {
	return hazardSummary{
		HazardID: event.HazardID, Source: event.Source, HazardType: event.HazardType,
		Severity: event.Severity, AreaName: event.AreaName,
		OccurredAt: event.OccurredAt, IngestedAt: event.IngestedAt,
	}
}

func validateHazard(event hazardEvent) error {
	if event.HazardID == "" || event.SourceRefID == "" || event.AreaName == "" {
		return errors.New("hazard identity or area is empty")
	}
	if event.Source != "BMKG" && event.Source != "PVMBG" {
		return errors.New("hazard source is invalid")
	}
	if event.Source == "BMKG" && event.HazardType != "SEISMIC" || event.Source == "PVMBG" && event.HazardType != "VOLCANIC" {
		return errors.New("hazard source and type are inconsistent")
	}
	if event.Severity != "NORMAL" && event.Severity != "WASPADA" && event.Severity != "SIAGA" && event.Severity != "AWAS" {
		return errors.New("hazard severity is invalid")
	}
	if event.Latitude < -90 || event.Latitude > 90 || event.Longitude < -180 || event.Longitude > 180 {
		return errors.New("hazard coordinates are invalid")
	}
	if !validUTCTime(event.OccurredAt) || !validUTCTime(event.IngestedAt) {
		return errors.New("hazard timestamps are invalid")
	}
	var attributes map[string]json.RawMessage
	if err := decodeStrictBytes(event.Attributes, &attributes); err != nil || attributes == nil {
		return errors.New("hazard attributes are invalid")
	}
	return nil
}

func validMetadata(sources json.RawMessage, asOf string) bool {
	var object map[string]json.RawMessage
	return decodeStrictBytes(sources, &object) == nil && len(object) > 0 && validUTCTime(asOf)
}

func validUTCTime(value string) bool {
	if len(value) == 0 || value[len(value)-1] != 'Z' {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}
