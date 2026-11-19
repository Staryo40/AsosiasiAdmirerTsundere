//go:build integration

package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestAnalyticsMatchesAuditActiveEventsAndMarksMissingShard(t *testing.T) {
	children := make([]*Store, logicalShardCount)
	owners := make([]context.Context, logicalShardCount)
	for shard := range children {
		cfg := m2IsolatedConfig(t)
		children[shard] = m2OpenStore(t, cfg)
		owners[shard] = activateM2ShardFixtureOwner(t, cfg, children[shard])
	}
	parent := &Store{readTimeout: children[0].readTimeout, shards: [logicalShardCount]*Store{children[0], children[1], children[2]}}
	from := time.Time{}
	to := from.Add(24 * time.Hour)
	longArea := "A" + strings.Repeat("é", 513)
	fixtures := []struct {
		name, ref, area string
		source          Source
		kind            HazardType
		severity        Severity
		occurred        time.Time
	}{
		{name: "earthquake", source: BMKG, kind: Seismic, severity: Normal, area: longArea, occurred: from.Add(time.Nanosecond)},
		{name: "late-tsunami", source: BMKG, kind: Seismic, severity: Waspada, area: "B-area", occurred: from.Add(3 * time.Hour)},
		{name: "volcanic", source: PVMBG, kind: Volcanic, severity: Siaga, area: "C-area", occurred: from.Add(2 * time.Hour)},
		{name: "wildfire", source: Satelit, kind: Wildfire, severity: Awas, area: "D-area", occurred: from.Add(time.Hour)},
		{name: "coastal", source: SensorPantai, kind: Coastal, severity: Normal, area: "D-area", occurred: from.Add(time.Hour)},
		{name: "to-boundary", source: BMKG, kind: Seismic, severity: Normal, area: "outside", occurred: to},
	}
	for index := range fixtures {
		fixture := &fixtures[index]
		fixture.ref = analyticsFixtureReference(t, fixture.source, fixture.name)
		shard, err := logicalShard(fixture.source, fixture.ref)
		if err != nil {
			t.Fatal(err)
		}
		child := children[shard]
		if err := m2ExecFixtureMutation(owners[shard], child, `INSERT INTO hazard_events
			(hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision,updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,0,0,$7,$7,'{}',1,$7)`, "analytics-"+fixture.name, string(fixture.source), fixture.ref,
			string(fixture.kind), string(fixture.severity), fixture.area, fixture.occurred); err != nil {
			t.Fatal(err)
		}
	}

	cfg := testConfig(t)
	cfg.Audit.Token = "analytics-audit-secret"
	cfg.Audit.ChecksumTimeout = 3 * time.Second
	cfg.DBReadTimeout = 3 * time.Second
	cfg.ReadMaxInFlight = 2
	mux := http.NewServeMux()
	registerAuditRoutes4(mux, parent, cfg)
	registerSnapshotRoutes(mux, parent, cfg, 10*time.Second)
	fetchAnalytics := func(from, to time.Time) analyticsResponse {
		return fetchAnalyticsWindow(t, mux, cfg.InternalKey, analyticsQuery{From: from, To: to, Bucket: "hour"})
	}
	assertReconcilesAudit := func() analyticsResponse {
		t.Helper()
		var auditItems []HazardEvent
		cursor := ""
		for {
			path := "/internal/audit/events?limit=2"
			if cursor != "" {
				path += "&cursor=" + url.QueryEscape(cursor)
			}
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("Authorization", "Bearer "+cfg.Audit.Token)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			var page auditEventsResponse
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || page.Partial {
				t.Fatalf("Audit page status=%d body=%s", response.Code, response.Body)
			}
			for _, event := range page.Items {
				if !event.OccurredAt.Before(from) && event.OccurredAt.Before(to) {
					auditItems = append(auditItems, event)
				}
			}
			if page.NextCursor == nil {
				break
			}
			cursor = *page.NextCursor
		}
		analytics := fetchAnalytics(from, to)
		if analytics.Partial || analytics.Total != int64(len(auditItems)) {
			t.Fatalf("analytics/Audit mismatch: total=%d audit=%d partial=%t", analytics.Total, len(auditItems), analytics.Partial)
		}
		assertAnalyticsDimensions(t, analytics, auditItems)
		return analytics
	}

	strictFrom := fetchAnalytics(from.Add(time.Nanosecond), to)
	if strictFrom.Total != 4 {
		t.Fatalf("sub-microsecond lower bound counted a row before from: total=%d", strictFrom.Total)
	}
	strictTo := fetchAnalytics(from, to.Add(time.Nanosecond))
	if strictTo.Total != 6 {
		t.Fatalf("sub-microsecond upper bound omitted a row before to: total=%d", strictTo.Total)
	}
	initial := assertReconcilesAudit()
	if initial.Total != 5 || initial.Severity["NORMAL"] != 2 || initial.Severity["WASPADA"] != 1 ||
		initial.Severity["SIAGA"] != 1 || initial.Severity["AWAS"] != 1 {
		t.Fatalf("initial category/severity grouping: %+v", initial)
	}
	initialCategories := map[string]bool{}
	for _, item := range initial.Series {
		initialCategories[item.Category] = true
	}
	for _, category := range []string{"EARTHQUAKE", "VOLCANIC", "WILDFIRE", "COASTAL"} {
		if !initialCategories[category] {
			t.Errorf("initial analytics missing category %s", category)
		}
	}

	late := fixtures[1]
	shard, _ := logicalShard(late.source, late.ref)
	if err := m2ExecFixtureMutation(owners[shard], children[shard], `INSERT INTO source_warnings (warning_id,related_event_id,issued_at,payload)
		VALUES ($1,$2,$3,'{}')`, "analytics-late-warning", late.ref, to.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	rawOnly := assertReconcilesAudit()
	rawCategories := analyticsCategoryCounts(rawOnly.Series)
	if rawCategories["EARTHQUAKE"] != 2 || rawCategories["TSUNAMI"] != 0 {
		t.Fatalf("raw warning without canonical attributes changed category: %+v", rawCategories)
	}
	attributes, err := json.Marshal(map[string]any{
		"potential_tsunami": true,
		"tsunami_warnings": []map[string]any{{
			"warning_id": "analytics-late-warning", "related_event_id": late.ref,
			"threat_level": "Waspada", "affected_zones": []string{},
			"estimated_arrival": to.Add(2 * time.Hour).Format(time.RFC3339),
			"issued_at":         to.Add(time.Hour).Format(time.RFC3339),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2ExecFixtureMutation(owners[shard], children[shard], `UPDATE hazard_events SET attributes=$2,revision=revision+1 WHERE hazard_id=$1`,
		"analytics-late-tsunami", attributes); err != nil {
		t.Fatal(err)
	}
	withWarning := assertReconcilesAudit()
	categories := analyticsCategoryCounts(withWarning.Series)
	if categories["EARTHQUAKE"] != 1 || categories["TSUNAMI"] != 1 || categories["VOLCANIC"] != 1 ||
		categories["WILDFIRE"] != 1 || categories["COASTAL"] != 1 {
		t.Fatalf("canonical related warning classification: %+v", categories)
	}

	wildfire := fixtures[3]
	shard, _ = logicalShard(wildfire.source, wildfire.ref)
	if err := m2ExecFixtureMutation(owners[shard], children[shard], `UPDATE hazard_events SET area_name='Z-updated',severity='SIAGA',revision=revision+1 WHERE hazard_id=$1`, "analytics-wildfire"); err != nil {
		t.Fatal(err)
	}
	updated := assertReconcilesAudit()
	if updated.Severity["AWAS"] != 0 || updated.Severity["SIAGA"] != 2 || len(updated.Areas) == 0 || updated.Areas[len(updated.Areas)-1].AreaName != "Z-updated" {
		t.Fatalf("updated active row not reflected: %+v", updated)
	}

	coastal := fixtures[4]
	shard, _ = logicalShard(coastal.source, coastal.ref)
	if err := m2ExecFixtureMutation(owners[shard], children[shard], `DELETE FROM hazard_events WHERE hazard_id=$1`, "analytics-coastal"); err != nil {
		t.Fatal(err)
	}
	removed := assertReconcilesAudit()
	if removed.Total != 4 {
		t.Fatalf("removed active row still counted: total=%d", removed.Total)
	}

	earthquake := fixtures[0]
	earthquakeShard, _ := logicalShard(earthquake.source, earthquake.ref)
	missingShard := (int(earthquakeShard) + 1) % logicalShardCount
	children[missingShard].Close()
	partial := fetchAnalytics(from, to)
	if !partial.Partial || len(partial.UnavailableUnits) != 1 || partial.UnavailableUnits[0] != fmt.Sprintf("shard-%d", missingShard) {
		t.Fatalf("missing shard not disclosed: %+v", partial)
	}
	if !slices.Contains(partial.Areas, analyticsAreaItem{AreaName: longArea, Count: 1}) {
		t.Fatalf("available shard lost the long canonical area: %+v", partial.Areas)
	}
	tinyBudget := cfg
	tinyBudget.MaxResponseBytes = int64(len(longArea) - 1)
	tinyBudgetMux := http.NewServeMux()
	registerSnapshotRoutes(tinyBudgetMux, parent, tinyBudget, 10*time.Second)
	capped := requestAnalyticsWindow(tinyBudgetMux, cfg.InternalKey, analyticsQuery{From: from, To: to, Bucket: "hour"})
	var failure readErrorEnvelope
	if capped.Code != http.StatusServiceUnavailable || json.Unmarshal(capped.Body.Bytes(), &failure) != nil ||
		failure.Error.Code != "analytics_response_too_large" {
		t.Fatalf("oversized partial result returned HTTP %d: %s", capped.Code, capped.Body)
	}
	for shard, child := range children {
		if shard != missingShard {
			child.Close()
		}
	}
	unavailable := fetchAnalytics(from, to)
	if !unavailable.Partial || len(unavailable.UnavailableUnits) != logicalShardCount || unavailable.Total != 0 ||
		unavailable.Series == nil || len(unavailable.Series) != 0 || unavailable.Areas == nil || len(unavailable.Areas) != 0 {
		t.Fatalf("all-shard outage did not return explicit empty partial figures: %+v", unavailable)
	}
}

func assertAnalyticsDimensions(t *testing.T, analytics analyticsResponse, auditItems []HazardEvent) {
	t.Helper()
	wantSeries := make(map[analyticsSeriesKey]int64)
	wantSeverity := map[string]int64{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}
	wantAreas := make(map[string]int64)
	for _, event := range auditItems {
		hour := event.OccurredAt.UTC().Truncate(time.Hour)
		category := auditAnalyticsCategory(t, event)
		wantSeries[analyticsSeriesKey{Start: hour, Category: category}]++
		wantSeverity[string(event.Severity)]++
		wantAreas[event.AreaName]++
	}
	gotSeries := make(map[analyticsSeriesKey]int64)
	for _, point := range analytics.Series {
		gotSeries[analyticsSeriesKey{Start: point.Start, Category: point.Category}] = point.Count
	}
	if len(analytics.Series) != len(wantSeries) || !maps.Equal(gotSeries, wantSeries) {
		t.Errorf("analytics series = %v, Audit aggregation = %v", analytics.Series, wantSeries)
	}
	if !maps.Equal(analytics.Severity, wantSeverity) {
		t.Errorf("analytics severity = %v, Audit aggregation = %v", analytics.Severity, wantSeverity)
	}
	gotAreas := make(map[string]int64)
	for _, area := range analytics.Areas {
		gotAreas[area.AreaName] = area.Count
	}
	if len(analytics.Areas) != len(wantAreas) || !maps.Equal(gotAreas, wantAreas) {
		t.Errorf("analytics areas = %v, Audit aggregation = %v", analytics.Areas, wantAreas)
	}
}

func auditAnalyticsCategory(t *testing.T, event HazardEvent) string {
	t.Helper()
	if event.HazardType != Seismic {
		return string(event.HazardType)
	}
	if raw, ok := event.Attributes["tsunami_warnings"]; ok {
		var warnings []struct {
			RelatedEventID string `json:"related_event_id"`
		}
		if err := json.Unmarshal(raw, &warnings); err != nil {
			t.Fatalf("decode Audit event warnings: %v", err)
		}
		for _, warning := range warnings {
			if warning.RelatedEventID == event.SourceRefID {
				return "TSUNAMI"
			}
		}
	}
	return "EARTHQUAKE"
}

func analyticsFixtureReference(t *testing.T, source Source, prefix string) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		reference := fmt.Sprintf("%s-%d", prefix, index)
		if _, err := logicalShard(source, reference); err == nil {
			return reference
		}
	}
	t.Fatal("could not generate analytics fixture reference")
	return ""
}
func fetchAnalyticsWindow(t *testing.T, mux *http.ServeMux, internalKey string, query analyticsQuery) analyticsResponse {
	t.Helper()
	response := requestAnalyticsWindow(mux, internalKey, query)
	var parsed analyticsResponse
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &parsed) != nil {
		t.Fatalf("analytics status=%d body=%s", response.Code, response.Body)
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("invalid analytics response headers: %v", response.Header())
	}
	return parsed
}

func requestAnalyticsWindow(mux *http.ServeMux, internalKey string, query analyticsQuery) *httptest.ResponseRecorder {
	values := url.Values{
		"from":   {query.From.Format(time.RFC3339Nano)},
		"to":     {query.To.Format(time.RFC3339Nano)},
		"bucket": {query.Bucket},
	}
	request := httptest.NewRequest(http.MethodGet, "/internal/analytics?"+values.Encode(), nil)
	request.Header.Set(internalKeyHeader, internalKey)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

func analyticsCategoryCounts(series []analyticsSeriesItem) map[string]int64 {
	counts := make(map[string]int64)
	for _, item := range series {
		counts[item.Category] += item.Count
	}
	return counts
}
