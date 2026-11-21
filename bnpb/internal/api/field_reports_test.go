package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
)

type fakePublisher struct {
	mu        sync.Mutex
	published []amqp.Publishing
	err       error
}

func (fake *fakePublisher) Publish(_ context.Context, message amqp.Publishing) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.err != nil {
		return fake.err
	}
	fake.published = append(fake.published, message)
	return nil
}

func (fake *fakePublisher) Close() {}

func fieldReportHandler(t *testing.T, publisher reportPublisher) http.Handler {
	t.Helper()
	authServer := httptest.NewServer(authDependency(t))
	t.Cleanup(authServer.Close)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	service, err := newPublicService(clientConfig{authBaseURL: dependencyURL(t, authServer.URL),
		aggregatorBaseURL: dependencyURL(t, "http://127.0.0.1:1"), authInternalKey: testAuthKey, aggregatorKey: testAggregatorKey},
		httpclient.New(time.Second, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	service.fieldReports = publisher
	mux := http.NewServeMux()
	service.registerRoutes(mux)
	return httpserver.Handler(mux, logger, 64)
}

func validReportBody(reportID string) string {
	return `{"report_id":"` + reportID + `","reported_at":"2026-11-21T03:00:00Z","latitude":-2.51,"longitude":113.8,` +
		`"hazard_type":"WILDFIRE","description":"Api terlihat di sisi timur desa","related_hazard_id":null}`
}

func postReport(handler http.Handler, token, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/field-reports", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestFieldReportIsAcceptedOnlyAfterThePublisherConfirms(t *testing.T) {
	publisher := &fakePublisher{}
	handler := fieldReportHandler(t, publisher)
	got := postReport(handler, "field-token", validReportBody("fr-1"))
	if got.Code != http.StatusAccepted {
		t.Fatalf("HTTP %d %s", got.Code, got.Body)
	}
	var reply map[string]string
	if json.Unmarshal(got.Body.Bytes(), &reply) != nil || reply["report_id"] != "fr-1" || reply["accepted_at"] == "" {
		t.Fatalf("unexpected reply %s", got.Body)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("expected one publication, got %d", len(publisher.published))
	}
	message := publisher.published[0]
	var queued fieldReportMessage
	if err := json.Unmarshal(message.Body, &queued); err != nil {
		t.Fatal(err)
	}
	if message.DeliveryMode != amqp.Persistent || message.CorrelationId == "" || queued.ClientID != "field-client" ||
		queued.ReportID != "fr-1" || queued.Latitude != -2.51 || queued.AcceptedAt.IsZero() {
		t.Fatalf("unexpected publication %+v %+v", message, queued)
	}
}

func TestFieldReportMessageIDIsStablePerClientAndReport(t *testing.T) {
	publisher := &fakePublisher{}
	handler := fieldReportHandler(t, publisher)
	postReport(handler, "field-token", validReportBody("fr-same"))
	postReport(handler, "field-token", validReportBody("fr-same"))
	postReport(handler, "field-token", validReportBody("fr-other"))
	ids := []string{publisher.published[0].MessageId, publisher.published[1].MessageId, publisher.published[2].MessageId}
	if ids[0] != ids[1] || ids[0] == ids[2] || !strings.HasPrefix(ids[0], "fr-") {
		t.Fatalf("message ids %v", ids)
	}
	other := fieldReportMessageID(fieldReportMessage{ClientID: "another-team", ReportID: "fr-same"})
	if other == ids[0] {
		t.Fatal("two clients reusing a report_id must not share a message id")
	}
}

func TestFieldReportsAreForFieldTeamsOnly(t *testing.T) {
	publisher := &fakePublisher{}
	handler := fieldReportHandler(t, publisher)
	for token, want := range map[string]int{"": http.StatusUnauthorized, "media-token": http.StatusForbidden, "ops-token": http.StatusForbidden} {
		if got := postReport(handler, token, validReportBody("fr-1")); got.Code != want {
			t.Errorf("token %q: HTTP %d, want %d", token, got.Code, want)
		}
	}
	if len(publisher.published) != 0 {
		t.Fatal("a refused caller must not publish")
	}
}

func TestFieldReportValidation(t *testing.T) {
	publisher := &fakePublisher{}
	handler := fieldReportHandler(t, publisher)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	for name, body := range map[string]string{
		"bad report_id":   validReportBody("fr 1"),
		"future time":     strings.Replace(validReportBody("fr-1"), "2026-11-21T03:00:00Z", future, 1),
		"missing lat":     strings.Replace(validReportBody("fr-1"), `"latitude":-2.51,`, "", 1),
		"bad longitude":   strings.Replace(validReportBody("fr-1"), "113.8", "181", 1),
		"unknown hazard":  strings.Replace(validReportBody("fr-1"), "WILDFIRE", "TSUNAMI", 1),
		"empty text":      strings.Replace(validReportBody("fr-1"), "Api terlihat di sisi timur desa", " ", 1),
		"control char":    strings.Replace(validReportBody("fr-1"), "Api terlihat", `Api\u0007terlihat`, 1),
		"long text":       strings.Replace(validReportBody("fr-1"), "Api terlihat di sisi timur desa", strings.Repeat("a", 2001), 1),
		"unknown field":   strings.Replace(validReportBody("fr-1"), `"related_hazard_id":null`, `"priority":1`, 1),
		"empty related":   strings.Replace(validReportBody("fr-1"), `"related_hazard_id":null`, `"related_hazard_id":""`, 1),
		"trailing object": validReportBody("fr-1") + `{}`,
	} {
		if got := postReport(handler, "field-token", body); got.Code != http.StatusBadRequest {
			t.Errorf("%s: HTTP %d %s", name, got.Code, got.Body)
		}
	}
	if len(publisher.published) != 0 {
		t.Fatalf("invalid reports were published: %d", len(publisher.published))
	}
}

func TestFieldReportIsRefusedWhenTheBrokerDoesNotConfirm(t *testing.T) {
	for name, publisher := range map[string]reportPublisher{
		"broker down":  &fakePublisher{err: errors.New("publication failed: connect")},
		"unconfigured": unconfiguredPublisher{},
	} {
		got := postReport(fieldReportHandler(t, publisher), "field-token", validReportBody("fr-1"))
		if got.Code != http.StatusServiceUnavailable || got.Header().Get("Retry-After") == "" ||
			!strings.Contains(got.Body.String(), "intake_unavailable") {
			t.Errorf("%s: HTTP %d %s", name, got.Code, got.Body)
		}
	}
}
