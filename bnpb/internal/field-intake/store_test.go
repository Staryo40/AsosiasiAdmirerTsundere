package fieldintake

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(context.Background(), filepath.Join(t.TempDir(), "intake.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func sampleReport(reportID string) Report {
	return Report{ClientID: "field-client", ReportID: reportID, ReportedAt: time.Date(2026, 11, 21, 3, 0, 0, 0, time.UTC),
		Latitude: -2.51, Longitude: 113.8, HazardType: "WILDFIRE", Description: "Api terlihat",
		AcceptedAt: time.Date(2026, 11, 21, 3, 0, 1, 0, time.UTC)}
}

func TestApplyStoresOnceAndCountsRedeliveriesAndRetries(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	report := sampleReport("fr-1")
	retry := report
	retry.AcceptedAt = report.AcceptedAt.Add(30 * time.Second)
	for index, candidate := range []Report{report, report, retry} {
		outcome, err := store.Apply(ctx, "fr-msg", candidate, []byte("{}"))
		want := Duplicate
		if index == 0 {
			want = Stored
		}
		if err != nil || outcome != want {
			t.Fatalf("copy %d: outcome=%s err=%v", index, outcome, err)
		}
	}
	page, err := store.Page(ctx, 0, 10)
	if err != nil || page.Stored != 1 || len(page.Items) != 1 || page.Items[0].Deliveries != 3 || page.Conflicts != 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

func TestApplyKeepsTheFirstContentAndRecordsAConflict(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	first := sampleReport("fr-1")
	changed := first
	changed.Description = "Api sudah padam"
	if _, err := store.Apply(ctx, "fr-msg", first, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if outcome, err := store.Apply(ctx, "fr-msg", changed, []byte(`{"changed":true}`)); err != nil || outcome != Conflict {
		t.Fatalf("outcome=%s err=%v", outcome, err)
	}
	other := first
	other.ClientID = "another-team"
	if outcome, err := store.Apply(ctx, "fr-msg-2", other, []byte("{}")); err != nil || outcome != Stored {
		t.Fatalf("a second client reusing the report_id must be stored: outcome=%s err=%v", outcome, err)
	}
	page, err := store.Page(ctx, 0, 10)
	if err != nil || page.Stored != 2 || page.Conflicts != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

func TestPageWalksStoredReportsInOrder(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if _, err := store.Apply(ctx, "msg-"+id, sampleReport(id), []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	after := int64(0)
	for {
		page, err := store.Page(ctx, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			seen = append(seen, item.ReportID)
		}
		if page.NextAfter == nil {
			break
		}
		after = *page.NextAfter
	}
	if strings.Join(seen, "") != "abcde" {
		t.Fatalf("walked %v", seen)
	}
	if _, err := store.Page(ctx, 0, 1001); err != errInvalidPage {
		t.Fatalf("expected invalid page, got %v", err)
	}
}

func TestConfigNeedsBrokerIdentityAndAStrongKey(t *testing.T) {
	base := map[string]string{"BNPB_FIELD_INTAKE_RABBITMQ_USER": "field-intake", "BNPB_FIELD_INTAKE_RABBITMQ_PASSWORD": "p@ss",
		"BNPB_FIELD_INTAKE_INTERNAL_KEY": strings.Repeat("k", 32)}
	lookup := func(env map[string]string) func(string) (string, bool) {
		return func(name string) (string, bool) { value, ok := env[name]; return value, ok }
	}
	cfg, err := loadConfig(lookup(base))
	if err != nil || cfg.BrokerURL != "amqp://field-intake:p%40ss@rabbitmq:5672/" || cfg.DBPath != defaultDBPath {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	for _, broken := range []map[string]string{
		{"BNPB_FIELD_INTAKE_RABBITMQ_USER": ""},
		{"BNPB_FIELD_INTAKE_INTERNAL_KEY": "short"},
		{"BNPB_FIELD_INTAKE_INTERNAL_KEY": " " + strings.Repeat("k", 32)},
	} {
		env := map[string]string{}
		for key, value := range base {
			env[key] = value
		}
		for key, value := range broken {
			env[key] = value
		}
		if _, err := loadConfig(lookup(env)); err == nil {
			t.Errorf("expected an error for %v", broken)
		}
	}
}

func TestFieldReportListingNeedsTheInternalKey(t *testing.T) {
	handler := internalOnly(strings.Repeat("k", 32), pageHandler(testStore(t)))
	for header, want := range map[string]int{"": http.StatusUnauthorized, "wrong": http.StatusForbidden, strings.Repeat("k", 32): http.StatusOK} {
		request := httptest.NewRequest(http.MethodGet, "/internal/field-reports", nil)
		if header != "" {
			request.Header.Set(internalKeyHeader, header)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != want {
			t.Errorf("key %q: HTTP %d, want %d", header, recorder.Code, want)
		}
	}
}
