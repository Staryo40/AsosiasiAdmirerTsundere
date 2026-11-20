package aggregator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadAuditConfig(t *testing.T) {
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	}
	cfg, err := loadAuditConfig(lookup(nil))
	if err != nil || cfg.Token != "" {
		t.Fatalf("optional config: %#v %v", cfg, err)
	}
	cfg, err = loadAuditConfig(lookup(map[string]string{"BNPB_AUDIT_TOKEN": "secret", "BNPB_UNPROCESSED_API_URL": "https://member3.example/api", "BNPB_UNPROCESSED_API_TOKEN": "service", "BNPB_STORAGE_COPY_DATABASE_URLS": "[\"postgres://audit@db.example/copy\"]"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "secret" || cfg.UnprocessedURL != "https://member3.example/api" || cfg.UnprocessedToken != "service" || !reflect.DeepEqual(cfg.CopyURLs, []string{"postgres://audit@db.example/copy"}) {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	for _, values := range []map[string]string{{"BNPB_UNPROCESSED_API_URL": "https://m3.example"}, {"BNPB_AUDIT_TOKEN": ""}, {"BNPB_AUDIT_TOKEN": "secret", "BNPB_STORAGE_COPY_DATABASE_URLS": "{\"url\":\"postgres://db/copy\"}"}, {"BNPB_AUDIT_TOKEN": "secret", "BNPB_UNPROCESSED_API_TOKEN": "orphan"}} {
		if _, err := loadAuditConfig(lookup(values)); err == nil {
			t.Errorf("accepted invalid configuration keys %v", keys(values))
		}
	}
}
func keys(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	return result
}

func TestParseAuditQueryDefaultsAndBounds(t *testing.T) {
	query, err := parseAuditQuery("", true)
	if err != nil {
		t.Fatal(err)
	}
	if query.Limit != 100 || query.Source != nil || query.Since != nil || query.Cursor != nil {
		t.Fatalf("unexpected defaults: %#v", query)
	}
	query, err = parseAuditQuery("limit=1000&source=SATELIT&since=2026-11-12T02%3A43%3A00Z", true)
	if err != nil {
		t.Fatal(err)
	}
	if query.Limit != 1000 || query.Source == nil || *query.Source != Satelit || query.Since == nil || !query.Since.Equal(time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC)) {
		t.Fatalf("query parse mismatch: %#v", query)
	}
	for _, raw := range []string{"source=", "source=UNKNOWN", "limit=0", "limit=1001", "limit=01&limit=2", "cursor=x", "since=not-a-date", "extra=1", "since=2026-11-12"} {
		if _, err := parseAuditQuery(raw, true); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	for _, raw := range []string{"limit=1000", "cursor=x", "since=2026-11-12T00%3A00%3A00Z"} {
		if _, err := parseAuditQuery(raw, false); err == nil {
			t.Errorf("checksum accepted %q", raw)
		}
	}
	if _, err := parseAuditQuery("source=BMKG", false); err != nil {
		t.Errorf("checksum rejected source: %v", err)
	}
}

func TestAuditCursorWireAndOrdering(t *testing.T) {
	start := encodeAuditStartCursor()
	parsed, err := parseAuditCursor(start)
	if err != nil || !parsed.Start {
		t.Fatalf("start cursor: %#v %v", parsed, err)
	}
	stamp := time.Date(2026, 11, 12, 2, 51, 8, 0, time.FixedZone("UTC+7", 7*3600))
	cursor := encodeAuditCursor(HazardEvent{HazardID: "hz-a", IngestedAt: stamp})
	parsed, err = parseAuditCursor(cursor)
	if err != nil || parsed.Start || parsed.HazardID != "hz-a" || !parsed.IngestedAt.Equal(stamp) {
		t.Fatalf("key cursor: %#v %v", parsed, err)
	}
	if !auditEventLess(HazardEvent{IngestedAt: stamp, HazardID: "a"}, HazardEvent{IngestedAt: stamp, HazardID: "b"}) {
		t.Fatal("hazard ID byte-order tie break is not ascending")
	}
	if auditEventLess(HazardEvent{IngestedAt: stamp.Add(time.Second)}, HazardEvent{IngestedAt: stamp}) {
		t.Fatal("timestamp ordering is not ascending")
	}
}

func TestAuditAuthenticationAndErrorEnvelope(t *testing.T) {
	cfg := Config{ReadMaxInFlight: 1}
	cfg.Audit.Token = "correct"
	mux := http.NewServeMux()
	registerAuditRoutes4(mux, nil, cfg, 0)
	cases := []struct {
		name, authorization string
		status              int
		code                string
	}{
		{"missing", "", http.StatusUnauthorized, "unauthorized"},
		{"wrong", "Bearer incorrect", http.StatusForbidden, "forbidden"},
		{"wrong-scheme", "Basic correct", http.StatusForbidden, "forbidden"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/internal/audit/events?bad=%", nil)
			if test.authorization != "" {
				request.Header.Add("Authorization", test.authorization)
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			assertAuditError(t, response, test.status, test.code)
		})
	}
	request := httptest.NewRequest(http.MethodGet, "/internal/audit/events?bad=%", nil)
	request.Header.Set("Authorization", "Bearer correct")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	assertAuditError(t, response, http.StatusBadRequest, "invalid_query")
	cfg.Audit.Token = ""
	mux = http.NewServeMux()
	registerAuditRoutes4(mux, nil, cfg, 0)
	request = httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
	request.Header.Set("Authorization", "Bearer ")
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	assertAuditError(t, response, http.StatusForbidden, "forbidden")
	cfg.Audit.Token = "correct"
	api := auditAPI{cfg: cfg, slots: make(chan struct{}, 1)}
	api.slots <- struct{}{}
	handler := api.authenticate(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	request = httptest.NewRequest(http.MethodGet, "/internal/audit/events?bad=%", nil)
	request.Header.Set("Authorization", "Bearer correct")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertAuditError(t, response, http.StatusTooManyRequests, "too_many_requests")
	mux = http.NewServeMux()
	registerAuditRoutes4(mux, nil, cfg, 0)
	request = httptest.NewRequest(http.MethodGet, "/internal/audit/unprocessed", nil)
	request.Header.Set("Authorization", "Bearer correct")
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	assertAuditErrorStatus(t, response, http.StatusServiceUnavailable)
}
func assertAuditErrorStatus(t *testing.T, response *httptest.ResponseRecorder, status int) {
	t.Helper()
	if response.Code != status || !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}
	var body map[string]map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || len(body) != 1 || body["error"]["code"] == "" || body["error"]["message"] == "" || len(body["error"]) != 2 {
		t.Fatalf("unexpected error envelope: %s", response.Body)
	}
}
func assertAuditError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
	if !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("content type=%q", response.Header().Get("Content-Type"))
	}
	var body map[string]map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body["error"]["code"] != code || body["error"]["message"] == "" || len(body["error"]) != 2 {
		t.Fatalf("unexpected error envelope: %#v", body)
	}
}

func TestAuditLocationDropsConflictingUnitsAndSortsVerifiedLocations(t *testing.T) {
	primary := auditLocation{Node: "node-a", Unit: "unit-a", Role: "primary"}
	replica := auditLocation{Node: "node-z", Unit: "unit-z", Role: "replica"}
	conflict := auditLocation{Node: "node-b", Unit: "unit-conflict", Role: "replica"}
	other := conflict
	other.Node = "node-c"
	observed := []locationProbeResult{
		{location: replica, shardID: 1, found: true},
		{location: conflict, shardID: 1, found: true},
		{location: primary, shardID: 1, found: true},
		{location: other, shardID: 1, found: true},
		{location: replica, shardID: 1, found: true},
	}
	locations, unavailable := reconcileLocationPlacement(nil, observed)
	if !reflect.DeepEqual(unavailable, []string{conflict.Unit}) || !reflect.DeepEqual(locations, []auditLocation{primary, replica}) {
		t.Fatalf("locations=%+v unavailable=%v", locations, unavailable)
	}
}

func TestAuditLocationKeepsPromotedCopyWhileReportingRoleDrift(t *testing.T) {
	promoted := auditLocation{Node: "node-b", Unit: "s0-b", Role: "primary"}
	expected := []locationPlacementCopy{
		{shardPlacement: shardPlacement{ShardID: 0, Unit: "s0-a", Node: "node-a", Role: "primary", State: "ready"}},
		{shardPlacement: shardPlacement{ShardID: 0, Unit: promoted.Unit, Node: promoted.Node, Role: "replica", State: "ready"}},
	}
	observed := []locationProbeResult{{location: promoted, shardID: 0, found: true}}
	locations, unavailable := reconcileLocationPlacement(expected, observed)
	promoted.RoleDrift = true
	if !reflect.DeepEqual(locations, []auditLocation{promoted}) || !reflect.DeepEqual(unavailable, []string{"s0-b", "s0-a"}) {
		t.Fatalf("locations=%+v unavailable=%v", locations, unavailable)
	}
	observed = append(observed, locationProbeResult{
		location: auditLocation{Node: promoted.Node, Unit: promoted.Unit, Role: "replica"}, shardID: 0, found: true,
	})
	locations, unavailable = reconcileLocationPlacement(expected, observed)
	if !reflect.DeepEqual(locations, []auditLocation{promoted}) || len(unavailable) == 0 {
		t.Fatalf("conflicting roles erased the positive probe: locations=%+v unavailable=%v", locations, unavailable)
	}
}

func TestAuditLocationRequiresRetirementProofForMissingCopies(t *testing.T) {
	primary := auditLocation{Node: "node-a", Unit: "s0-a", Role: "primary"}
	replacement := auditLocation{Node: "node-d", Unit: "s0-d", Role: "replica"}
	lost := auditLocation{Node: "node-b", Unit: "s0-b", Role: "replica"}
	expected := []locationPlacementCopy{
		{shardPlacement: shardPlacement{ShardID: 0, Unit: primary.Unit, Node: primary.Node, Role: primary.Role, State: "ready"}},
		{shardPlacement: shardPlacement{ShardID: 0, Unit: replacement.Unit, Node: replacement.Node, Role: replacement.Role, State: "ready"}},
		{shardPlacement: shardPlacement{ShardID: 0, Unit: lost.Unit, Node: lost.Node, Role: lost.Role, State: "retiring"}},
	}
	observed := []locationProbeResult{
		{location: primary, shardID: 0, found: true},
		{location: replacement, shardID: 0, found: true},
		{probeUnit: "copy-1", boundUnit: lost.Unit, partial: true},
	}
	locations, unavailable := reconcileLocationPlacement(expected, observed[:2])
	if !reflect.DeepEqual(locations, []auditLocation{primary, replacement}) || !reflect.DeepEqual(unavailable, []string{lost.Unit}) {
		t.Fatalf("unproven loss: locations=%+v unavailable=%v", locations, unavailable)
	}
	expected[2].retired = true
	locations, unavailable = reconcileLocationPlacement(expected, observed)
	if !reflect.DeepEqual(locations, []auditLocation{primary, replacement}) || len(unavailable) != 0 {
		t.Fatalf("proven loss: locations=%+v unavailable=%v", locations, unavailable)
	}
	observed[2].boundUnit = ""
	_, unavailable = reconcileLocationPlacement(expected, observed)
	if !reflect.DeepEqual(unavailable, []string{"copy-1"}) {
		t.Fatalf("unbound failure was masked: unavailable=%v", unavailable)
	}
	observed[2] = locationProbeResult{location: lost, shardID: 0, found: true}
	locations, unavailable = reconcileLocationPlacement(expected, observed)
	if !reflect.DeepEqual(locations, []auditLocation{primary, lost, replacement}) || !reflect.DeepEqual(unavailable, []string{lost.Unit}) {
		t.Fatalf("contradicted proof: locations=%+v unavailable=%v", locations, unavailable)
	}
	observed[2].found = false
	locations, unavailable = reconcileLocationPlacement(expected, observed)
	if !reflect.DeepEqual(locations, []auditLocation{primary, replacement}) || !reflect.DeepEqual(unavailable, []string{lost.Unit}) {
		t.Fatalf("retired identity still serving: locations=%+v unavailable=%v", locations, unavailable)
	}
}

func TestAuditLocationUnavailableDirectProbeRequiresExplicitBinding(t *testing.T) {
	const unit = "s0-b"
	for _, test := range []struct{ name, application, boundUnit string }{
		{name: "bound", application: auditCopyApplicationPrefix + unit, boundUnit: unit},
		{name: "ordinary application name", application: unit},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := mustTestPool(t, "postgres://127.0.0.1:1/copy?application_name="+url.QueryEscape(test.application))
			defer pool.Close()
			ctx, cancel := context.WithDeadline(t.Context(), time.Time{})
			defer cancel()
			probe := (locationProbeJob{pool: pool, routed: true, unit: "copy-1", shardID: noAuditShard}).probe(ctx, "missing")
			if !probe.partial || probe.boundUnit != test.boundUnit {
				t.Fatalf("probe=%+v, want partial with binding %q", probe, test.boundUnit)
			}
		})
	}
}

func TestAuditQueryRejectsMalformedEncoding(t *testing.T) {
	if _, err := url.ParseQuery("source=%zz"); err == nil {
		t.Fatal("fixture should be malformed")
	}
	if _, err := parseAuditQuery("source=%zz", true); err == nil {
		t.Fatal("accepted malformed query escape")
	}
}
