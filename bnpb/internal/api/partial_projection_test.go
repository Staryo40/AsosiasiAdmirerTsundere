package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestPartialProjectionSurvivesSummaryAndFullResponses(t *testing.T) {
	const unavailableUnit = "shard-2"
	var aggregator http.Handler = http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		event := sampleEvent()
		event["attributes"] = map[string]any{
			"magnitude":        7.1,
			"source_ref_id":    "must-not-leak-through-summary",
			"raw_payload":      map[string]any{"secret": "must-not-leak-through-summary"},
			"future_attribute": []string{"additive", "data"},
		}
		switch request.URL.Path {
		case "/internal/hazards":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{event}, "next_cursor": "retry-boundary",
				"sources": map[string]any{"BMKG": map[string]any{"status": "partial"}},
				"as_of":   "2026-10-07T12:00:01Z", "partial": true,
				"unavailable_units": []string{unavailableUnit},
			})
		case "/internal/hazards/haz-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"item":    event,
				"sources": map[string]any{"BMKG": map[string]any{"status": "partial"}},
				"as_of":   "2026-10-07T12:00:01Z", "partial": true,
				"unavailable_units": []string{unavailableUnit},
			})
		default:
			http.NotFound(w, request)
		}
	})

	for _, test := range []struct {
		name, role, target string
		full               bool
	}{
		{name: "media list", role: "media", target: "/v1/hazards"},
		{name: "media detail", role: "media", target: "/v1/hazards/haz-1"},
		{name: "field list", role: "field", target: "/v1/hazards?view=full", full: true},
		{name: "field detail", role: "field", target: "/v1/hazards/haz-1?view=full", full: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := testPublicHandler(t, authDependency(t), aggregator)
			response := publicRequest(handler, http.MethodGet, test.target, test.role+"-token", "")
			if response.Code != http.StatusOK {
				t.Fatalf("response returned %d: %s", response.Code, response.Body.String())
			}
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			var partial bool
			var unavailable []string
			if err := json.Unmarshal(envelope["partial"], &partial); err != nil || !partial {
				t.Fatalf("partial metadata was not preserved: %s", response.Body.String())
			}
			if err := json.Unmarshal(envelope["unavailable_units"], &unavailable); err != nil || len(unavailable) != 1 || unavailable[0] != unavailableUnit {
				t.Fatalf("unavailable units were not preserved: %s", response.Body.String())
			}
			projectionKey := "items"
			if strings.Contains(test.target, "haz-1") {
				projectionKey = "item"
			}
			if test.full {
				var fullEvent map[string]json.RawMessage
				if projectionKey == "items" {
					var fullEvents []map[string]json.RawMessage
					if err := json.Unmarshal(envelope[projectionKey], &fullEvents); err != nil || len(fullEvents) != 1 {
						t.Fatalf("invalid full list response: %v", err)
					}
					fullEvent = fullEvents[0]
				} else if err := json.Unmarshal(envelope[projectionKey], &fullEvent); err != nil {
					t.Fatalf("invalid full detail response: %v", err)
				}
				if _, exists := fullEvent["attributes"]; !exists {
					t.Errorf("full response lost event attributes: %s", response.Body.String())
				}
				return
			}
			for _, raw := range []string{"source_ref_id", "raw_payload", "future_attribute", "secret", "latitude", "longitude", "attributes"} {
				if strings.Contains(response.Body.String(), `"`+raw+`"`) {
					t.Errorf("Media response leaked raw/additive field %q: %s", raw, response.Body.String())
				}
			}
			var projections []map[string]json.RawMessage
			if projectionKey == "items" {
				if err := json.Unmarshal(envelope[projectionKey], &projections); err != nil || len(projections) != 1 {
					t.Fatalf("invalid list projection: %v", err)
				}
			} else {
				var projected map[string]json.RawMessage
				if err := json.Unmarshal(envelope[projectionKey], &projected); err != nil {
					t.Fatalf("invalid detail projection: %v", err)
				}
				projections = append(projections, projected)
			}
			allowed := map[string]bool{"hazard_id": true, "source": true, "hazard_type": true, "severity": true, "area_name": true, "occurred_at": true, "ingested_at": true}
			for field := range projections[0] {
				if !allowed[field] {
					t.Errorf("Media projection contains unexpected field %q", field)
				}
			}
			fullTarget := test.target + "?view=full"
			if response := publicRequest(handler, http.MethodGet, fullTarget, "media-token", ""); response.Code != http.StatusForbidden {
				t.Errorf("Media full view returned %d: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestPartialProjectionRejectsMalformedMetadata(t *testing.T) {
	validList := `{"items":[],"next_cursor":null,"sources":{"BMKG":{}},"as_of":"2026-10-07T12:00:01Z"}`
	for _, payload := range []string{
		`{"items":[],"next_cursor":null,"sources":{"BMKG":{}},"as_of":"2026-10-07T12:00:01Z","partial":null}`,
		`{"items":[],"next_cursor":null,"sources":{"BMKG":{}},"as_of":"2026-10-07T12:00:01Z","partial":true}`,
		`{"items":[],"next_cursor":null,"sources":{"BMKG":{}},"as_of":"2026-10-07T12:00:01Z","unavailable_units":null}`,
		`{"items":[],"next_cursor":null,"sources":{"BMKG":{}},"as_of":"2026-10-07T12:00:01Z","partial":false,"unavailable_units":["shard-2"]}`,
	} {
		if _, err := projectList([]byte(payload), viewSummary); err == nil {
			t.Errorf("malformed partial metadata accepted: %s", payload)
		}
	}
	projected, err := projectList([]byte(validList), viewSummary)
	if err != nil {
		t.Fatalf("legacy envelope without partial fields was rejected: %v", err)
	}
	encoded, err := json.Marshal(projected)
	if err != nil || !strings.Contains(string(encoded), `"partial":false`) || !strings.Contains(string(encoded), `"unavailable_units":[]`) {
		t.Fatalf("legacy envelope defaults were not represented as healthy: %s (%v)", encoded, err)
	}
}
