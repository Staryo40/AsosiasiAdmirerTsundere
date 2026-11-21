package aggregator

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func volcanicV1(t *testing.T, edit func(map[string]any)) json.RawMessage {
	t.Helper()
	record := map[string]any{"report_id": "rpt-adapt-1", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 3, "ash_column_height_m": 1200.5, "reported_at": "2026-11-21T01:02:03Z"}
	if edit != nil {
		edit(record)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func adaptationFrom(t *testing.T, env map[string]string) pvmbgAdaptation {
	t.Helper()
	adaptation, err := loadPVMBGAdaptation(func(name string) (string, bool) { value, ok := env[name]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	return adaptation
}

func validationCode(err error) ValidationCode {
	var validation *ValidationError
	if errors.As(err, &validation) {
		return validation.Code
	}
	return ""
}

func adaptAndMap(t *testing.T, adaptation pvmbgAdaptation, raw json.RawMessage) (HazardEvent, error) {
	t.Helper()
	fields, err := adaptation.rewrite(raw)
	if err != nil {
		return HazardEvent{}, err
	}
	adapted, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return MapVolcanic(adapted, p1Receipt())
}

func TestWithinDepth(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want bool
	}{
		{strings.Repeat("[", 32) + strings.Repeat("]", 32), true},
		{strings.Repeat("[", 33) + strings.Repeat("]", 33), false},
		{`{"a":{"b":[1,2,{"c":3}]}}`, true},
		{`{"text":"` + strings.Repeat("[", 100) + `"}`, true},
		{`{"text":"\"` + strings.Repeat("{", 100) + `"}`, true},
		{strings.Repeat("{\"a\":", 40) + "1" + strings.Repeat("}", 40), false},
	} {
		if got := withinDepth([]byte(test.raw), 32); got != test.want {
			t.Errorf("withinDepth(%.40q) = %v, want %v", test.raw, got, test.want)
		}
	}
}

func TestValidateJSONRejectsDeepRecordsAsTooDeep(t *testing.T) {
	// The same element the PVMBG deep_nesting fault prepends.
	deep := strings.Repeat("[", 5000) + strings.Repeat("]", 5000)
	if code := validationCode(validateJSON(json.RawMessage(deep))); code != TooDeep {
		t.Fatalf("expected too_deep, got %q", code)
	}
	if err := validateJSON(volcanicV1(t, nil)); err != nil {
		t.Fatalf("a normal record must pass: %v", err)
	}
}

func TestIncompatibleSchemasAreRejectedWithoutAdaptation(t *testing.T) {
	for name, test := range map[string]struct {
		edit func(map[string]any)
		want ValidationCode
	}{
		"v3 rename":       {func(r map[string]any) { r["level"] = r["alert_level"]; delete(r, "alert_level") }, MissingField},
		"v4 type change":  {func(r map[string]any) { r["eruption_count_24h"] = "3" }, WrongType},
		"v5 missing time": {func(r map[string]any) { delete(r, "reported_at") }, MissingField},
	} {
		_, err := MapVolcanic(volcanicV1(t, test.edit), p1Receipt())
		if code := validationCode(err); code != test.want {
			t.Errorf("%s: expected %q, got %v", name, test.want, err)
		}
	}
}

func TestAdaptationMapsRenamedAndRetypedFieldsLikeV1(t *testing.T) {
	adaptation := adaptationFrom(t, map[string]string{
		"BNPB_PVMBG_FIELD_ALIASES":   "level:alert_level",
		"BNPB_PVMBG_NUMERIC_STRINGS": "eruption_count_24h, ash_column_height_m",
	})
	want, err := MapVolcanic(volcanicV1(t, nil), p1Receipt())
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(map[string]any){
		"v3": func(r map[string]any) { r["level"] = r["alert_level"]; delete(r, "alert_level") },
		"v4": func(r map[string]any) { r["eruption_count_24h"] = "3"; r["ash_column_height_m"] = "1200.5" },
		"v1": nil,
	} {
		got, err := adaptAndMap(t, adaptation, volcanicV1(t, edit))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Severity != want.Severity || !got.OccurredAt.Equal(want.OccurredAt) || got.SourceRefID != want.SourceRefID ||
			string(got.Attributes["eruption_count_24h"]) != "3" {
			t.Errorf("%s: mapped %+v, want %+v", name, got, want)
		}
	}
}

func TestAdaptationRefusesAmbiguousAliases(t *testing.T) {
	adaptation := adaptationFrom(t, map[string]string{"BNPB_PVMBG_FIELD_ALIASES": "level:alert_level"})
	_, err := adaptAndMap(t, adaptation, volcanicV1(t, func(r map[string]any) { r["level"] = "Awas" }))
	if code := validationCode(err); code != AmbiguousField {
		t.Fatalf("expected ambiguous_field, got %v", err)
	}
}

func TestNumericStringsAcceptOnlyPlainDecimals(t *testing.T) {
	adaptation := adaptationFrom(t, map[string]string{"BNPB_PVMBG_NUMERIC_STRINGS": "eruption_count_24h"})
	for _, text := range []string{"03", "3 ", "1e2", "+3", "", "three"} {
		_, err := adaptAndMap(t, adaptation, volcanicV1(t, func(r map[string]any) { r["eruption_count_24h"] = text }))
		if code := validationCode(err); code != WrongType {
			t.Errorf("%q: expected wrong_type, got %v", text, err)
		}
	}
}

func TestFallbackTimeMarksTheRecordAsInferred(t *testing.T) {
	fields, err := pvmbgAdaptation{timeFallback: true}.rewrite(volcanicV1(t, func(r map[string]any) { delete(r, "reported_at") }))
	if err != nil {
		t.Fatal(err)
	}
	received := time.Date(2026, 11, 21, 4, 5, 6, 0, time.UTC)
	withFallbackTime(fields, received)
	adapted, _ := json.Marshal(fields)
	hazard, err := MapVolcanic(adapted, p1Receipt())
	if err != nil {
		t.Fatal(err)
	}
	if !hazard.OccurredAt.Equal(received) || string(hazard.Attributes["occurred_at_inferred"]) != "true" {
		t.Fatalf("occurred_at=%s attributes=%s", hazard.OccurredAt, hazard.Attributes)
	}
}

func TestLoadPVMBGAdaptationRefusesBadSettings(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unknown target":    {"BNPB_PVMBG_FIELD_ALIASES": "level:alert"},
		"alias is a field":  {"BNPB_PVMBG_FIELD_ALIASES": "volcano_id:alert_level"},
		"missing colon":     {"BNPB_PVMBG_FIELD_ALIASES": "level"},
		"same name":         {"BNPB_PVMBG_FIELD_ALIASES": "alert_level:alert_level"},
		"repeated target":   {"BNPB_PVMBG_FIELD_ALIASES": "level:alert_level,lvl:alert_level"},
		"bad characters":    {"BNPB_PVMBG_FIELD_ALIASES": "Level:alert_level"},
		"empty entry":       {"BNPB_PVMBG_NUMERIC_STRINGS": "eruption_count_24h,,"},
		"non numeric field": {"BNPB_PVMBG_NUMERIC_STRINGS": "volcano_id"},
		"repeated numeric":  {"BNPB_PVMBG_NUMERIC_STRINGS": "eruption_count_24h,eruption_count_24h"},
		"unknown fallback":  {"BNPB_PVMBG_MISSING_TIME_FALLBACK": "now"},
	} {
		if _, err := loadPVMBGAdaptation(func(key string) (string, bool) { value, ok := env[key]; return value, ok }); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if !adaptationFrom(t, map[string]string{"BNPB_PVMBG_FIELD_ALIASES": " ", "BNPB_PVMBG_MISSING_TIME_FALLBACK": ""}).empty() {
		t.Fatal("blank settings must mean no adaptation")
	}
}
