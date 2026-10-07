package auth

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const (
	maximumAuthBodyBytes = 16 * 1024
	InternalKeyHeader    = "X-BNPB-Auth-Key"
)

type loginRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type introspectRequest struct {
	AccessToken string `json:"access_token"`
}

type errorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlation_id"`
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

// RegisterRoutes adds the three internal Auth endpoints to mux.
func (service *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/auth/login", service.requireInternalKey(service.handleLogin))
	mux.HandleFunc("POST /internal/auth/refresh", service.requireInternalKey(service.handleRefresh))
	mux.HandleFunc("POST /internal/auth/introspect", service.requireInternalKey(service.handleIntrospect))
}

func (service *Service) requireInternalKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		values := request.Header.Values(InternalKeyHeader)
		if len(values) == 0 {
			writeError(w, request, http.StatusUnauthorized, "missing_credential", "missing internal credential")
			return
		}
		if len(values) != 1 || values[0] == "" || !service.ValidInternalKey(values[0]) {
			writeError(w, request, http.StatusForbidden, "invalid_credential", "invalid internal credential")
			return
		}
		next(w, request)
	}
}

func (service *Service) handleLogin(w http.ResponseWriter, request *http.Request) {
	preventTokenCaching(w)
	var input loginRequest
	if err := decodeStrictJSON(w, request, &input); err != nil || input.ClientID == "" || input.ClientSecret == "" {
		if errors.Is(err, errUnsupportedMediaType) {
			writeError(w, request, http.StatusUnsupportedMediaType, "invalid_request", "content type must be application/json")
			return
		}
		writeError(w, request, http.StatusBadRequest, "invalid_request", "request body must contain client_id and client_secret")
		return
	}
	response, err := service.Login(input.ClientID, input.ClientSecret)
	if err != nil {
		if errors.Is(err, ErrInvalidCredentials) {
			logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelWarn, Event: "auth_rejected", Message: "client login rejected"}, "reason", "invalid_credentials")
			writeError(w, request, http.StatusUnauthorized, "invalid_credentials", "invalid client credentials")
			return
		}
		logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelError, Event: "token_issue_failed", Message: "access or refresh token issuance failed"})
		writeError(w, request, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelInfo, Event: "auth_login_succeeded", Message: "client login succeeded"}, "client_id", input.ClientID)
	writeJSON(w, http.StatusOK, response)
}

func (service *Service) handleRefresh(w http.ResponseWriter, request *http.Request) {
	preventTokenCaching(w)
	var input refreshRequest
	if err := decodeStrictJSON(w, request, &input); err != nil || input.RefreshToken == "" {
		if errors.Is(err, errUnsupportedMediaType) {
			writeError(w, request, http.StatusUnsupportedMediaType, "invalid_request", "content type must be application/json")
			return
		}
		writeError(w, request, http.StatusBadRequest, "invalid_request", "request body must contain refresh_token")
		return
	}
	response, err := service.Refresh(input.RefreshToken)
	if err != nil {
		if errors.Is(err, ErrInvalidRefresh) {
			logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelWarn, Event: "auth_rejected", Message: "refresh rejected"}, "reason", "invalid_refresh_token")
			writeError(w, request, http.StatusUnauthorized, "invalid_refresh_token", "invalid or expired refresh token")
			return
		}
		logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelError, Event: "token_issue_failed", Message: "access or refresh token issuance failed"})
		writeError(w, request, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	logging.Event(request.Context(), service.logger, logging.Record{Level: slog.LevelInfo, Event: "auth_refresh_succeeded", Message: "client token refresh succeeded"})
	writeJSON(w, http.StatusOK, response)
}

func (service *Service) handleIntrospect(w http.ResponseWriter, request *http.Request) {
	preventTokenCaching(w)
	var input introspectRequest
	if err := decodeStrictJSON(w, request, &input); err != nil || input.AccessToken == "" {
		if errors.Is(err, errUnsupportedMediaType) {
			writeError(w, request, http.StatusUnsupportedMediaType, "invalid_request", "content type must be application/json")
			return
		}
		writeError(w, request, http.StatusBadRequest, "invalid_request", "request body must contain access_token")
		return
	}
	writeJSON(w, http.StatusOK, service.Introspect(input.AccessToken))
}

var errUnsupportedMediaType = errors.New("unsupported media type")

func decodeStrictJSON(w http.ResponseWriter, request *http.Request, destination any) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errUnsupportedMediaType
	}
	request.Body = http.MaxBytesReader(w, request.Body, maximumAuthBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func preventTokenCaching(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

func writeError(w http.ResponseWriter, request *http.Request, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: errorDetail{
		Code:          code,
		Message:       message,
		CorrelationID: correlation.ID(request.Context()),
	}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
