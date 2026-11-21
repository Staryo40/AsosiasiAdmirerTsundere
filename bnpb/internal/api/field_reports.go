package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

var reportIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

var fieldHazardTypes = map[string]bool{"SEISMIC": true, "VOLCANIC": true, "WILDFIRE": true, "COASTAL": true, "OTHER": true}

type fieldReportInput struct {
	ReportID        string   `json:"report_id"`
	ReportedAt      string   `json:"reported_at"`
	Latitude        *float64 `json:"latitude"`
	Longitude       *float64 `json:"longitude"`
	HazardType      string   `json:"hazard_type"`
	Description     string   `json:"description"`
	RelatedHazardID *string  `json:"related_hazard_id"`
}

// The queued message: the validated report plus who sent it and when BNPB accepted it.
type fieldReportMessage struct {
	ClientID        string    `json:"client_id"`
	ReportID        string    `json:"report_id"`
	ReportedAt      time.Time `json:"reported_at"`
	Latitude        float64   `json:"latitude"`
	Longitude       float64   `json:"longitude"`
	HazardType      string    `json:"hazard_type"`
	Description     string    `json:"description"`
	RelatedHazardID *string   `json:"related_hazard_id"`
	AcceptedAt      time.Time `json:"accepted_at"`
}

func (service *publicService) registerFieldReportRoute(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/field-reports", service.requireAccess(requireField(service.withAdmission(service.handleFieldReport))))
}

func requireField(next func(http.ResponseWriter, *http.Request, principal)) func(http.ResponseWriter, *http.Request, principal) {
	return func(w http.ResponseWriter, request *http.Request, identity principal) {
		preventCaching(w)
		if identity.role != roleField {
			writePublicError(w, request, http.StatusForbidden, "forbidden", "only field teams may submit field reports")
			return
		}
		next(w, request, identity)
	}
}

func (service *publicService) handleFieldReport(w http.ResponseWriter, request *http.Request, identity principal) {
	var input fieldReportInput
	if err := decodePublicJSON(w, request, &input); err != nil {
		writeDecodeError(w, request, err, "request body must be one field report object")
		return
	}
	message, err := input.validate(identity.clientID, time.Now().UTC())
	if err != nil {
		writePublicError(w, request, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	body, err := json.Marshal(message)
	if err != nil {
		writePublicError(w, request, http.StatusBadRequest, "invalid_request", "field report is invalid")
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), fieldReportPublishBy)
	defer cancel()
	err = service.fieldReports.Publish(ctx, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: fieldReportMessageID(message),
		CorrelationId: correlation.ID(request.Context()), Timestamp: message.AcceptedAt, AppId: "bnpb-api",
		Type: "field_report.submitted", Body: body,
	})
	if err != nil {
		// Not confirmed by the broker, so nothing is promised; the client retries with the same report_id.
		logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelWarn, Event: "field_report_not_queued", Message: "broker did not confirm the field report"},
			"report_id", message.ReportID, "category", publishCategory(err))
		w.Header().Set("Retry-After", "2")
		writePublicError(w, request, http.StatusServiceUnavailable, "intake_unavailable", "field report was not accepted; retry with the same report_id")
		return
	}
	logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelInfo, Event: "field_report_queued", Message: "field report durably queued"},
		"report_id", message.ReportID, "client_id", message.ClientID)
	writePublicJSON(w, http.StatusAccepted, map[string]string{"report_id": message.ReportID, "accepted_at": message.AcceptedAt.Format(time.RFC3339Nano)})
}

// Scoped per client so two field teams reusing a report_id never collide.
func fieldReportMessageID(message fieldReportMessage) string {
	digest := sha256.Sum256([]byte(message.ClientID + "\x00" + message.ReportID))
	return "fr-" + hex.EncodeToString(digest[:16])
}

func publishCategory(err error) string {
	if errors.Is(err, errIntakeUnconfigured) {
		return "unconfigured"
	}
	return "broker"
}

func (input fieldReportInput) validate(clientID string, now time.Time) (fieldReportMessage, error) {
	message := fieldReportMessage{ClientID: clientID, ReportID: input.ReportID, HazardType: input.HazardType,
		Description: input.Description, RelatedHazardID: input.RelatedHazardID, AcceptedAt: now}
	if !reportIDPattern.MatchString(input.ReportID) {
		return message, errors.New("report_id must be 1-128 letters, digits, dot, underscore or hyphen")
	}
	reportedAt, err := time.Parse(time.RFC3339Nano, input.ReportedAt)
	if err != nil || reportedAt.Year() < 2000 || reportedAt.After(now.Add(5*time.Minute)) {
		return message, errors.New("reported_at must be an RFC 3339 time that is not in the future")
	}
	message.ReportedAt = reportedAt.UTC()
	if input.Latitude == nil || input.Longitude == nil || !inRange(*input.Latitude, 90) || !inRange(*input.Longitude, 180) {
		return message, errors.New("latitude and longitude are required and must be valid coordinates")
	}
	message.Latitude, message.Longitude = *input.Latitude, *input.Longitude
	if !fieldHazardTypes[input.HazardType] {
		return message, errors.New("hazard_type must be SEISMIC, VOLCANIC, WILDFIRE, COASTAL or OTHER")
	}
	if !validDescription(input.Description) {
		return message, errors.New("description must be 1-" + strconv.Itoa(maxDescriptionRunes) + " characters of text")
	}
	if id := input.RelatedHazardID; id != nil && (*id == "" || len(*id) > 256 || strings.ContainsFunc(*id, unicode.IsControl)) {
		return message, errors.New("related_hazard_id must be a hazard_id or null")
	}
	return message, nil
}

const maxDescriptionRunes = 2000

func validDescription(text string) bool {
	if strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > maxDescriptionRunes {
		return false
	}
	return !strings.ContainsFunc(text, func(r rune) bool { return unicode.IsControl(r) && r != '\n' && r != '\t' })
}

func inRange(value, limit float64) bool {
	return !math.IsNaN(value) && value >= -limit && value <= limit
}
