package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
)

// The canonical PVMBG v1/v2 field names an adaptation may target.
var pvmbgFields = []string{"report_id", "volcano_id", "alert_level", "eruption_count_24h", "ash_column_height_m", "reported_at", "confidence_level"}

var pvmbgNumericFields = []string{"eruption_count_24h", "ash_column_height_m", "confidence_level"}

var plainDecimal = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// Ops-set rewrites applied to a PVMBG record before mapping; the stored original bytes never change.
type pvmbgAdaptation struct {
	aliases        map[string]string
	numericStrings []string
	timeFallback   bool
}

func (adaptation pvmbgAdaptation) empty() bool {
	return len(adaptation.aliases) == 0 && len(adaptation.numericStrings) == 0 && !adaptation.timeFallback
}

func loadPVMBGAdaptation(lookup func(string) (string, bool)) (pvmbgAdaptation, error) {
	adaptation := pvmbgAdaptation{}
	aliases, err := settingList(lookup, "BNPB_PVMBG_FIELD_ALIASES")
	if err != nil {
		return adaptation, err
	}
	targets := map[string]bool{}
	for _, pair := range aliases {
		source, target, found := strings.Cut(pair, ":")
		switch {
		case !found || !fieldName(source) || !fieldName(target) || source == target:
			return adaptation, fmt.Errorf("BNPB_PVMBG_FIELD_ALIASES: each entry must be source:target with two different field names")
		case slices.Contains(pvmbgFields, source):
			return adaptation, fmt.Errorf("BNPB_PVMBG_FIELD_ALIASES: %s is already a PVMBG field and cannot be an alias", source)
		case !slices.Contains(pvmbgFields, target):
			return adaptation, fmt.Errorf("BNPB_PVMBG_FIELD_ALIASES: %s is not a PVMBG field", target)
		case adaptation.aliases[source] != "" || targets[target]:
			return adaptation, fmt.Errorf("BNPB_PVMBG_FIELD_ALIASES: each source and target may appear once")
		}
		if adaptation.aliases == nil {
			adaptation.aliases = map[string]string{}
		}
		adaptation.aliases[source], targets[target] = target, true
	}
	numeric, err := settingList(lookup, "BNPB_PVMBG_NUMERIC_STRINGS")
	if err != nil {
		return adaptation, err
	}
	for _, field := range numeric {
		if !slices.Contains(pvmbgNumericFields, field) || slices.Contains(adaptation.numericStrings, field) {
			return adaptation, fmt.Errorf("BNPB_PVMBG_NUMERIC_STRINGS: entries must be distinct names from %s", strings.Join(pvmbgNumericFields, ", "))
		}
		adaptation.numericStrings = append(adaptation.numericStrings, field)
	}
	switch fallback, _ := lookup("BNPB_PVMBG_MISSING_TIME_FALLBACK"); strings.TrimSpace(fallback) {
	case "":
	case "received_at":
		adaptation.timeFallback = true
	default:
		return adaptation, errors.New("BNPB_PVMBG_MISSING_TIME_FALLBACK: must be empty or received_at")
	}
	return adaptation, nil
}

func settingList(lookup func(string) (string, bool), name string) ([]string, error) {
	configured, _ := lookup(name)
	if strings.TrimSpace(configured) == "" {
		return nil, nil
	}
	entries := strings.Split(configured, ",")
	for index, entry := range entries {
		entries[index] = strings.TrimSpace(entry)
		if entries[index] == "" {
			return nil, fmt.Errorf("%s: empty entry", name)
		}
	}
	return entries, nil
}

func fieldName(name string) bool {
	return name != "" && len(name) <= 64 && strings.IndexFunc(name, func(r rune) bool {
		return !(unicode.IsLower(r) || unicode.IsDigit(r) || r == '_') || r > unicode.MaxASCII
	}) < 0
}

// Unparseable records are returned unchanged so the mapping reports their real error.
func (adaptation pvmbgAdaptation) rewrite(raw json.RawMessage) (rawObject, error) {
	fields, err := parseObject(raw)
	if err != nil {
		return nil, nil
	}
	for source, target := range adaptation.aliases {
		value, present := fields[source]
		if !present {
			continue
		}
		if _, clash := fields[target]; clash {
			return nil, invalidField(target, AmbiguousField)
		}
		fields[target] = value
		delete(fields, source)
	}
	for _, field := range adaptation.numericStrings {
		value := bytes.TrimSpace(fields[field])
		if len(value) == 0 || value[0] != '"' {
			continue
		}
		var text string
		if json.Unmarshal(value, &text) == nil && plainDecimal.MatchString(text) {
			fields[field] = json.RawMessage(text)
		}
	}
	return fields, nil
}

func withFallbackTime(fields rawObject, occurredAt time.Time) {
	stamp, _ := json.Marshal(occurredAt.UTC().Format(time.RFC3339Nano))
	fields["reported_at"] = stamp
	fields["occurred_at_inferred"] = json.RawMessage("true")
}

func (store *Store) adaptVolcanic(ctx context.Context, tx pgx.Tx, chunk IngestChunk, raw json.RawMessage) (json.RawMessage, error) {
	if store.pvmbg.empty() {
		return raw, nil
	}
	fields, err := store.pvmbg.rewrite(raw)
	if err != nil || fields == nil {
		return raw, err
	}
	if _, present := fields["reported_at"]; store.pvmbg.timeFallback && !present {
		occurredAt, err := fallbackOccurrence(ctx, tx, fields, chunk.ReceivedAt)
		if err != nil {
			return nil, err
		}
		withFallbackTime(fields, occurredAt)
	}
	return json.Marshal(fields)
}

// Reuse the stored time so a re-sent record maps to the same occurred_at instead of each new receipt time.
func fallbackOccurrence(ctx context.Context, tx pgx.Tx, fields rawObject, receivedAt time.Time) (time.Time, error) {
	reportID, err := textField(fields, "report_id")
	if err != nil {
		return databaseTime(receivedAt), nil
	}
	var stored time.Time
	err = tx.QueryRow(ctx, "SELECT occurred_at FROM hazard_events WHERE source = $1 AND source_ref_id = $2",
		string(PVMBG), reportID).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return databaseTime(receivedAt), nil
	}
	return stored, err
}
