//go:build integration

package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestAuditCrossLogicalShardPagesAndChecksum(t *testing.T) {
	children := make([]*Store, 3)
	for id := range children {
		children[id] = m2OpenStore(t, m2IsolatedConfig(t))
	}
	parent := &Store{readTimeout: children[0].readTimeout, shards: [3]*Store{children[0], children[1], children[2]}}
	stamps := []time.Time{time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC), time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC), time.Date(2026, 11, 12, 2, 0, 1, 0, time.UTC), time.Date(2026, 11, 12, 2, 0, 2, 0, time.UTC), time.Date(2026, 11, 12, 2, 0, 3, 0, time.UTC)}
	sources := []Source{BMKG, PVMBG, Satelit, SensorPantai, BMKG}
	references := []string{"same", "same", "z-ref", "a-ref", "m-ref"}
	for id, child := range children {
		for n := id; n < len(stamps); n += len(children) {
			_, err := child.write.Exec(t.Context(), `INSERT INTO hazard_events
			(hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision,updated_at)
			VALUES ($1,$2,$3,$4,'SIAGA','Fixture',-2.51230,113.80440,$5,$6,$7,1,$6)`,
				[]string{"hz-a", "hz-b", "hz-c", "hz-d", "hz-e"}[n], string(sources[n]), references[n], string(hazardTypeForSource(sources[n])), stamps[n], stamps[n], []byte(`{"detection_keys":["N20|2026-11-12|243|-2.51230|113.80440"]}`))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	cfg := testConfig(t)
	cfg.Audit.Token = "audit-secret"
	cfg.DBReadTimeout = 2 * time.Second
	cfg.ReadMaxInFlight = 2
	mux := http.NewServeMux()
	registerAuditRoutes4(mux, parent, cfg, 0)
	fetch := func(path string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer audit-secret")
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body)
		}
		return response
	}
	var all auditEventsResponse
	cursor := ""
	for {
		path := "/internal/audit/events?limit=2"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		response := fetch(path)
		var page auditEventsResponse
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		if page.Partial {
			t.Fatal("all configured logical shards were available")
		}
		all.Items = append(all.Items, page.Items...)
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
		if len(all.Items) > len(stamps) {
			t.Fatal("cursor did not advance")
		}
	}
	if len(all.Items) != len(stamps) {
		t.Fatalf("events=%d want %d", len(all.Items), len(stamps))
	}
	for i := 1; i < len(all.Items); i++ {
		if auditEventLess(all.Items[i], all.Items[i-1]) {
			t.Fatalf("global order regressed at %d", i)
		}
	}
	if string(all.Items[2].Attributes["detection_keys"]) != `["N20|2026-11-12|243|-2.51230|113.80440"]` {
		t.Fatalf("raw detection key changed: %s", all.Items[2].Attributes["detection_keys"])
	}
	var since auditEventsResponse
	if err := json.Unmarshal(fetch("/internal/audit/events?since=2026-11-12T02%3A00%3A01Z").Body.Bytes(), &since); err != nil {
		t.Fatal(err)
	}
	if len(since.Items) != 3 || since.Items[0].HazardID != "hz-c" {
		t.Fatalf("since is not inclusive on ingested_at: %#v", since.Items)
	}
	var afterStoredTime auditEventsResponse
	if err := json.Unmarshal(fetch("/internal/audit/events?since=2026-11-12T02%3A00%3A00.000000001Z").Body.Bytes(), &afterStoredTime); err != nil {
		t.Fatal(err)
	}
	if len(afterStoredTime.Items) != 3 || afterStoredTime.Items[0].HazardID != "hz-c" {
		t.Fatalf("since one nanosecond after stored microsecond included the stored rows: %#v", afterStoredTime.Items)
	}
	var checksum auditChecksumResponse
	if err := json.Unmarshal(fetch("/internal/audit/checksum").Body.Bytes(), &checksum); err != nil {
		t.Fatal(err)
	}
	refs := []string{"a-ref", "m-ref", "same", "same", "z-ref"}
	expected := sha256.Sum256([]byte(strings.Join(refs, "\n")))
	if checksum.Count != int64(len(all.Items)) || checksum.Checksum != hex.EncodeToString(expected[:]) || checksum.Source != nil || checksum.Algorithm != "sha256" || checksum.AsOf.IsZero() {
		t.Fatalf("checksum mismatch: %#v", checksum)
	}
	var selected auditChecksumResponse
	if err := json.Unmarshal(fetch("/internal/audit/checksum?source=BMKG").Body.Bytes(), &selected); err != nil {
		t.Fatal(err)
	}
	selectedExpected := sha256.Sum256([]byte(strings.Join([]string{"m-ref", "same"}, "\n")))
	if selected.Count != 2 || selected.Checksum != hex.EncodeToString(selectedExpected[:]) || selected.Source == nil || *selected.Source != BMKG {
		t.Fatalf("source checksum mismatch: %#v", selected)
	}
	children[1].Close()
	request := httptest.NewRequest(http.MethodGet, "/internal/audit/events?limit=1", nil)
	request.Header.Set("Authorization", "Bearer audit-secret")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	var partialPage auditEventsResponse
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &partialPage) != nil || !partialPage.Partial || len(partialPage.UnavailableUnits) != 1 || partialPage.NextCursor == nil {
		t.Fatalf("partial first page did not preserve start boundary: status=%d body=%s", response.Code, response.Body)
	}
	firstBoundary := *partialPage.NextCursor
	request = httptest.NewRequest(http.MethodGet, "/internal/audit/events?limit=1&cursor="+url.QueryEscape(firstBoundary), nil)
	request.Header.Set("Authorization", "Bearer audit-secret")
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	var retryPage auditEventsResponse
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &retryPage) != nil || !retryPage.Partial || retryPage.NextCursor == nil || *retryPage.NextCursor != firstBoundary {
		t.Fatalf("partial page advanced its cursor: status=%d body=%s", response.Code, response.Body)
	}
	request = httptest.NewRequest(http.MethodGet, "/internal/audit/checksum", nil)
	request.Header.Set("Authorization", "Bearer audit-secret")
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	var partialChecksum auditChecksumResponse
	reachableRefs := []string{"a-ref", "same", "z-ref"}
	reachableDigest := sha256.Sum256([]byte(strings.Join(reachableRefs, "\n")))
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &partialChecksum) != nil ||
		!partialChecksum.Partial || len(partialChecksum.UnavailableUnits) != 1 ||
		partialChecksum.Count != int64(len(reachableRefs)) ||
		partialChecksum.Checksum != hex.EncodeToString(reachableDigest[:]) {
		t.Fatalf("checksum did not preserve reachable rows and disclose unavailable shard: status=%d body=%s", response.Code, response.Body)
	}
}

func TestAuditRejectsChangedStoreIdentity(t *testing.T) {
	cfg := routedM2Config(t)
	cfg.DBReadTimeout = 2 * time.Second
	cfg.DBReadMaxConns = 1
	cfg.ReadMaxInFlight = 1
	cfg.Audit.Token = "audit-secret"
	store := openRoutedM2Store(t, cfg)
	ownerCtx := activateFixtureOwner(t, cfg, store, fixtureMutationOwner, fixtureMutationEpoch)
	stamp := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	var refs [logicalShardCount]string
	for shardID, id := range []string{"audit-a", "audit-b", "audit-c"} {
		refs[shardID] = globalReadReference(t, shardID, id)
		insertGlobalReadFixture(t, ownerCtx, store.shards[shardID], id, refs[shardID], stamp)
	}
	mux := http.NewServeMux()
	registerAuditRoutes4(mux, store, cfg, 0)
	fetch := func(path string, body any) {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+cfg.Audit.Token)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), body) != nil {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body)
		}
	}

	const eventsPath = "/internal/audit/events?limit=1"
	const checksumPath = "/internal/audit/checksum"
	var first auditEventsResponse
	fetch(eventsPath, &first)
	if first.Partial || first.NextCursor == nil || len(first.Items) != 1 || first.Items[0].HazardID != "audit-a" {
		t.Fatalf("initial Audit page: %+v", first)
	}
	continuation := eventsPath + "&cursor=" + url.QueryEscape(*first.NextCursor)

	target := store.shards[1]
	setFixtureStoreIdentity(t, target, storeIdentity{role: "control"})
	var partialPage auditEventsResponse
	fetch(continuation, &partialPage)
	if !partialPage.Partial || len(partialPage.UnavailableUnits) != 1 || partialPage.UnavailableUnits[0] != "shard-1" ||
		partialPage.NextCursor == nil || *partialPage.NextCursor != *first.NextCursor ||
		len(partialPage.Items) != 1 || partialPage.Items[0].HazardID != "audit-c" {
		t.Fatalf("mismatched route did not preserve a partial cursor: %+v", partialPage)
	}
	var partialChecksum auditChecksumResponse
	fetch(checksumPath, &partialChecksum)
	reachable := sha256.Sum256([]byte(refs[0] + "\n" + refs[2]))
	if !partialChecksum.Partial || len(partialChecksum.UnavailableUnits) != 1 || partialChecksum.UnavailableUnits[0] != "shard-1" ||
		partialChecksum.Count != 2 || partialChecksum.Checksum != hex.EncodeToString(reachable[:]) {
		t.Fatalf("mismatched route contributed to checksum: %+v", partialChecksum)
	}

	setFixtureStoreIdentity(t, target, target.storeIdentity)
	var retry auditEventsResponse
	fetch(continuation, &retry)
	if retry.Partial || retry.NextCursor == nil || *retry.NextCursor == *first.NextCursor ||
		len(retry.Items) != 1 || retry.Items[0].HazardID != "audit-b" {
		t.Fatalf("recovered route did not restore continuation: %+v", retry)
	}
	var recovered auditChecksumResponse
	fetch(checksumPath, &recovered)
	all := sha256.Sum256([]byte(strings.Join(refs[:], "\n")))
	if recovered.Partial || len(recovered.UnavailableUnits) != 0 || recovered.Count != int64(len(refs)) ||
		recovered.Checksum != hex.EncodeToString(all[:]) {
		t.Fatalf("recovered route did not restore complete checksum: %+v", recovered)
	}
}

func TestAuditEventsPageLimitsAndEmptyChecksum(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	_, err := store.write.Exec(t.Context(), `INSERT INTO hazard_events
		(hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude,
		 occurred_at, ingested_at, attributes, revision, updated_at)
		SELECT 'limit-' || n, 'BMKG', 'ref-' || n, 'SEISMIC', 'NORMAL', 'fixture', 0, 0,
			now(), now() + n * interval '1 microsecond', '{}'::jsonb, 1, now()
		FROM generate_series(1, 1001) AS n`)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	cfg.Audit.Token = "audit-secret"
	mux := http.NewServeMux()
	registerAuditRoutes4(mux, store, cfg, 0)
	fetch := func(path string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer audit-secret")
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body)
		}
		return response
	}
	var defaultPage auditEventsResponse
	if err := json.Unmarshal(fetch("/internal/audit/events").Body.Bytes(), &defaultPage); err != nil {
		t.Fatal(err)
	}
	if len(defaultPage.Items) != 100 || defaultPage.NextCursor == nil || defaultPage.Partial {
		t.Fatalf("default page was not 100 events with continuation: items=%d next=%v partial=%v", len(defaultPage.Items), defaultPage.NextCursor, defaultPage.Partial)
	}
	var maximumPage auditEventsResponse
	if err := json.Unmarshal(fetch("/internal/audit/events?limit=1000").Body.Bytes(), &maximumPage); err != nil {
		t.Fatal(err)
	}
	if len(maximumPage.Items) != 1000 || maximumPage.NextCursor == nil || maximumPage.Partial {
		t.Fatalf("maximum page was not 1000 events with continuation: items=%d next=%v partial=%v", len(maximumPage.Items), maximumPage.NextCursor, maximumPage.Partial)
	}
	empty := m2OpenStore(t, m2IsolatedConfig(t))
	emptyCfg := testConfig(t)
	emptyCfg.Audit.Token = "audit-secret"
	emptyMux := http.NewServeMux()
	registerAuditRoutes4(emptyMux, empty, emptyCfg, 0)
	request := httptest.NewRequest(http.MethodGet, "/internal/audit/checksum", nil)
	request.Header.Set("Authorization", "Bearer audit-secret")
	response := httptest.NewRecorder()
	emptyMux.ServeHTTP(response, request)
	var checksum map[string]json.RawMessage
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &checksum) != nil ||
		string(checksum["source"]) != "null" || string(checksum["count"]) != "0" ||
		string(checksum["checksum"]) != `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"` {
		t.Fatalf("empty checksum wire response: status=%d body=%s", response.Code, response.Body)
	}
}
