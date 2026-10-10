package aggregator

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type p1MappingFixture struct {
	Name     string            `json:"name"`
	Source   Source            `json:"source"`
	Input    json.RawMessage   `json:"input"`
	Warnings []json.RawMessage `json:"warnings"`
	Expected json.RawMessage   `json:"expected"`
}

func p1Fixtures(t *testing.T) []p1MappingFixture {
	t.Helper()
	payload, err := os.ReadFile("testdata/mapping.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []p1MappingFixture
	if err := json.Unmarshal(payload, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 4 {
		t.Fatalf("expected four contract fixtures, got %d", len(fixtures))
	}
	return fixtures
}

func p1Receipt() Receipt {
	return Receipt{HazardID: "hazard-test", IngestedAt: time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)}
}

func p1Map(fixture p1MappingFixture) (HazardEvent, error) {
	if fixture.Source == BMKG {
		return MapSeismic(fixture.Input, fixture.Warnings, p1Receipt())
	}
	return MapVolcanic(fixture.Input, p1Receipt())
}

func p1JSON(t *testing.T, payload []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	return document
}

func p1EqualJSON(t *testing.T, actual, expected []byte) {
	t.Helper()
	if !reflect.DeepEqual(p1JSON(t, actual), p1JSON(t, expected)) {
		t.Fatalf("canonical payload differs\nactual: %s\nexpected: %s", actual, expected)
	}
}

func p1Encode(t *testing.T, document any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func p1WithField(t *testing.T, raw json.RawMessage, field string, replacement json.RawMessage) json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if replacement == nil {
		delete(fields, field)
	} else {
		fields[field] = replacement
	}
	return p1Encode(t, fields)
}

func TestP1MappingContractFixtures(t *testing.T) {
	for _, fixture := range p1Fixtures(t) {
		t.Run(fixture.Name, func(t *testing.T) {
			hazard, err := p1Map(fixture)
			if err != nil {
				t.Fatal(err)
			}
			p1EqualJSON(t, p1Encode(t, hazard), fixture.Expected)
		})
	}
}

func TestP1MagnitudeBoundaries(t *testing.T) {
	// Cases constrain each side of both severity thresholds, including exact decimals.
	for _, test := range []struct {
		magnitude string
		expected  Severity
	}{
		{"4.99", Normal}, {"5.0", Waspada}, {"6.49", Waspada}, {"6.5", Siaga},
		{"4.999999999999999999", Normal}, {"6.499999999999999999", Waspada},
	} {
		t.Run(test.magnitude, func(t *testing.T) {
			fixture := p1Fixtures(t)[0]
			fixture.Input = p1WithField(t, fixture.Input, "magnitude", json.RawMessage(test.magnitude))
			hazard, err := p1Map(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if hazard.Severity != test.expected {
				t.Fatalf("severity=%s, want %s", hazard.Severity, test.expected)
			}
		})
	}
}

func TestP1WarningOverridesMagnitude(t *testing.T) {
	for _, test := range []struct {
		level    string
		expected Severity
	}{
		{"Waspada", Waspada}, {"Siaga", Siaga}, {"Awas", Awas},
	} {
		t.Run(test.level, func(t *testing.T) {
			fixture := p1Fixtures(t)[1]
			fixture.Warnings = []json.RawMessage{p1WithField(t, fixture.Warnings[0], "threat_level", p1Encode(t, test.level))}
			hazard, err := p1Map(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if hazard.Severity != test.expected {
				t.Fatalf("warning must replace magnitude severity: %s", hazard.Severity)
			}
		})
	}
	fixture := p1Fixtures(t)[1]
	fixture.Warnings[1] = p1WithField(t, fixture.Warnings[1], "threat_level", p1Encode(t, "Awas"))
	hazard, err := p1Map(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if hazard.Severity != Awas {
		t.Fatal("related Awas must win over other warnings")
	}
}

func TestP1VolcanicLevelsAndConfidence(t *testing.T) {
	// Missing confidence differs from zero; null, boolean and out-of-range are invalid.
	for _, level := range []string{"Normal", "Waspada", "Siaga", "Awas"} {
		for _, confidence := range []string{"absent", "0", "1"} {
			t.Run(level+"/"+confidence, func(t *testing.T) {
				fixture := p1Fixtures(t)[2]
				fixture.Input = p1WithField(t, fixture.Input, "alert_level", p1Encode(t, level))
				if confidence != "absent" {
					fixture.Input = p1WithField(t, fixture.Input, "confidence_level", json.RawMessage(confidence))
				}
				hazard, err := p1Map(fixture)
				if err != nil {
					t.Fatal(err)
				}
				if string(hazard.Severity) != strings.ToUpper(level) {
					t.Fatalf("incorrect alert mapping: %s", hazard.Severity)
				}
				actual, present := hazard.Attributes["confidence_level"]
				if present != (confidence != "absent") {
					t.Fatal("absent confidence became a value")
				}
				if present {
					p1EqualJSON(t, actual, []byte(confidence))
				}
			})
		}
	}
}

func TestP1InvalidCoreFields(t *testing.T) {
	for _, fixture := range []p1MappingFixture{p1Fixtures(t)[0], p1Fixtures(t)[2]} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(fixture.Input, &fields); err != nil {
			t.Fatal(err)
		}
		for field := range fields {
			if field == "custom" || field == "extra" || field == "source_extras" || field == "tsunami_warnings" {
				continue
			}
			for _, replacement := range []struct {
				name string
				raw  json.RawMessage
				code ValidationCode
			}{
				{"missing", nil, MissingField}, {"null", json.RawMessage("null"), NullField},
				{"object", json.RawMessage(`{"unexpected":true}`), WrongType},
			} {
				t.Run(string(fixture.Source)+"/"+field+"/"+replacement.name, func(t *testing.T) {
					changed := fixture
					changed.Input = p1WithField(t, fixture.Input, field, replacement.raw)
					_, err := p1Map(changed)
					p1Validation(t, err, replacement.code)
				})
			}
		}
	}
}

func p1Validation(t *testing.T, err error, code ValidationCode) {
	t.Helper()
	var validation *ValidationError
	if !errors.As(err, &validation) || validation.Code != code {
		t.Fatalf("expected validation code %s, got %v", code, err)
	}
}

func TestP1RejectsUnsupportedChanges(t *testing.T) {
	for _, test := range []struct {
		field, raw string
		code       ValidationCode
	}{
		{"confidence_level", "null", NullField}, {"confidence_level", "true", WrongType},
		{"confidence_level", `"0.5"`, WrongType}, {"confidence_level", "-0.01", InvalidValue},
		{"confidence_level", "1.01", InvalidValue}, {"alert_level", `"Unknown"`, InvalidValue},
		{"volcano_id", `"unknown-volcano"`, UnknownVolcano}, {"eruption_count_24h", "1.5", WrongType},
		{"eruption_count_24h", "-1", InvalidValue}, {"reported_at", `"2026-10-03"`, InvalidValue},
	} {
		t.Run(test.field+"/"+test.raw, func(t *testing.T) {
			fixture := p1Fixtures(t)[2]
			fixture.Input = p1WithField(t, fixture.Input, test.field, json.RawMessage(test.raw))
			_, err := p1Map(fixture)
			p1Validation(t, err, test.code)
		})
	}
	for _, test := range []struct {
		raw  string
		code ValidationCode
	}{
		{`{"report_id":"a","report_id":"b"}`, DuplicateKey},
		{`{"extra":{"x":1,"x":2}}`, DuplicateKey}, {`{"report_id":`, InvalidJSON},
	} {
		t.Run(test.raw, func(t *testing.T) {
			_, err := MapVolcanic(json.RawMessage(test.raw), p1Receipt())
			p1Validation(t, err, test.code)
		})
	}
}

func TestP1PollingBoundaries(t *testing.T) {
	started := p1Receipt().IngestedAt
	for _, endpoint := range []Endpoint{SeismicEndpoint, WarningEndpoint, VolcanicEndpoint} {
		t.Run(string(endpoint), func(t *testing.T) {
			if pollQuery(nil, time.Second, started, endpoint) != nil {
				t.Fatal("first poll must request complete history")
			}
			future := started.Add(time.Hour)
			boundary := future
			if endpoint == WarningEndpoint {
				boundary = started
			}
			query := pollQuery(&future, time.Second, started, endpoint)
			if query.Get("since") != boundary.Add(-time.Second).Format(time.RFC3339Nano) {
				t.Fatalf("wrong overlap boundary: %v", query)
			}
		})
	}
}

func TestP1TiedTimestampOrdering(t *testing.T) {
	fixture := p1Fixtures(t)[1]
	raw := p1WithField(t, fixture.Input, "event_id", p1Encode(t, "a"))
	ordered, hold := orderRecords([]json.RawMessage{fixture.Input, raw}, SeismicEndpoint)
	if hold || len(ordered) != 2 || ordered[0].identity != "a" {
		t.Fatal("timestamp ties must retain both records and order by identity")
	}
	broken := p1WithField(t, raw, "occurred_at", nil)
	_, hold = orderRecords([]json.RawMessage{raw, broken}, SeismicEndpoint)
	if !hold {
		t.Fatal("unusable timestamp must hold checkpoint")
	}
}
