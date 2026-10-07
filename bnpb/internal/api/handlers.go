package api

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

const maximumPublicAuthBodyBytes = 16 * 1024

type loginPayload struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type refreshPayload struct {
	RefreshToken string `json:"refresh_token"`
}

type tokenResponse struct {
	AccessToken      string `json:"access_token"`
	TokenType        string `json:"token_type"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
}

type publicErrorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlation_id"`
}

type publicError struct {
	Error publicErrorDetail `json:"error"`
}

func (service *publicService) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /auth/login", service.handleLogin)
	mux.HandleFunc("POST /auth/refresh", service.handleRefresh)
	mux.HandleFunc("GET /v1/hazards", service.requireAccess(service.handleList))
	mux.HandleFunc("GET /v1/hazards/{hazard_id}", service.requireAccess(service.handleDetail))
}

func (service *publicService) handleLogin(w http.ResponseWriter, request *http.Request) {
	preventCaching(w)
	var input loginPayload
	if err := decodePublicJSON(w, request, &input); err != nil || input.ClientID == "" || input.ClientSecret == "" {
		writeDecodeError(w, request, err, "request body must contain client_id and client_secret")
		return
	}
	var output tokenResponse
	status, err := service.authCall(request.Context(), "/internal/auth/login", input, &output)
	if err != nil {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "authentication service is unavailable")
		return
	}
	if status == http.StatusUnauthorized {
		writePublicError(w, request, status, "invalid_credentials", "invalid client credentials")
		return
	}
	if status != http.StatusOK || !validTokenResponse(output) {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "authentication service is unavailable")
		return
	}
	writePublicJSON(w, http.StatusOK, output)
}

func (service *publicService) handleRefresh(w http.ResponseWriter, request *http.Request) {
	preventCaching(w)
	var input refreshPayload
	if err := decodePublicJSON(w, request, &input); err != nil || input.RefreshToken == "" {
		writeDecodeError(w, request, err, "request body must contain refresh_token")
		return
	}
	var output tokenResponse
	status, err := service.authCall(request.Context(), "/internal/auth/refresh", input, &output)
	if err != nil {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "authentication service is unavailable")
		return
	}
	if status == http.StatusUnauthorized {
		writePublicError(w, request, status, "invalid_refresh_token", "invalid or expired refresh token")
		return
	}
	if status != http.StatusOK || !validTokenResponse(output) {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "authentication service is unavailable")
		return
	}
	writePublicJSON(w, http.StatusOK, output)
}

func (service *publicService) requireAccess(next func(http.ResponseWriter, *http.Request, principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		token, missing := bearerToken(request.Header.Values("Authorization"))
		if missing {
			writePublicError(w, request, http.StatusUnauthorized, "missing_token", "missing bearer access token")
			return
		}
		if token == "" {
			writePublicError(w, request, http.StatusUnauthorized, "invalid_token", "invalid or expired access token")
			return
		}
		identity, err := service.introspect(request.Context(), token)
		if errors.Is(err, errInvalidToken) {
			writePublicError(w, request, http.StatusUnauthorized, "invalid_token", "invalid or expired access token")
			return
		}
		if err != nil {
			writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "authentication service is unavailable")
			return
		}
		next(w, request, identity)
	}
}

func (service *publicService) handleList(w http.ResponseWriter, request *http.Request, identity principal) {
	query, err := parseListQuery(request.URL.RawQuery)
	if err != nil {
		writePublicError(w, request, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	if identity.role == roleMedia && query.view == viewFull {
		writePublicError(w, request, http.StatusForbidden, "forbidden", "requested view is not allowed")
		return
	}
	status, payload, err := service.aggregatorCall(request.Context(), "/internal/hazards", query.upstream)
	service.writeAggregatorResult(w, request, status, payload, err, query.view, false)
}

func (service *publicService) handleDetail(w http.ResponseWriter, request *http.Request, identity principal) {
	query, err := parseDetailQuery(request.URL.RawQuery)
	if err != nil {
		writePublicError(w, request, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	if identity.role == roleMedia && query.view == viewFull {
		writePublicError(w, request, http.StatusForbidden, "forbidden", "requested view is not allowed")
		return
	}
	hazardID := request.PathValue("hazard_id")
	if hazardID == "" || len(hazardID) > 256 {
		writePublicError(w, request, http.StatusBadRequest, "invalid_query", "hazard_id is invalid")
		return
	}
	status, payload, err := service.aggregatorCall(request.Context(), "/internal/hazards/"+hazardID, "")
	service.writeAggregatorResult(w, request, status, payload, err, query.view, true)
}

func (service *publicService) writeAggregatorResult(w http.ResponseWriter, request *http.Request, status int, payload []byte, callErr error, view string, detail bool) {
	if callErr != nil {
		writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "hazard data service is unavailable")
		return
	}
	if status == http.StatusOK {
		var projected any
		var err error
		if detail {
			projected, err = projectDetail(payload, view)
		} else {
			projected, err = projectList(payload, view)
		}
		if err != nil {
			writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "hazard data service returned an invalid response")
			return
		}
		writePublicJSON(w, http.StatusOK, projected)
		return
	}
	if status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
		if !json.Valid(payload) {
			writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "hazard data service returned an invalid response")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(payload)
		return
	}
	writePublicError(w, request, http.StatusServiceUnavailable, "dependency_unavailable", "hazard data service is unavailable")
}

func bearerToken(values []string) (token string, missing bool) {
	if len(values) == 0 {
		return "", true
	}
	if len(values) != 1 {
		return "", false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], false
}

func validTokenResponse(response tokenResponse) bool {
	return response.AccessToken != "" && response.RefreshToken != "" && response.TokenType == "Bearer" && response.ExpiresIn > 0 && response.RefreshExpiresIn > 0
}

var errUnsupportedMediaType = errors.New("unsupported media type")

func decodePublicJSON(w http.ResponseWriter, request *http.Request, destination any) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errUnsupportedMediaType
	}
	request.Body = http.MaxBytesReader(w, request.Body, maximumPublicAuthBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request contains trailing JSON")
	}
	return nil
}

func writeDecodeError(w http.ResponseWriter, request *http.Request, err error, message string) {
	if errors.Is(err, errUnsupportedMediaType) {
		writePublicError(w, request, http.StatusUnsupportedMediaType, "invalid_request", "content type must be application/json")
		return
	}
	writePublicError(w, request, http.StatusBadRequest, "invalid_request", message)
}

func preventCaching(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

func writePublicError(w http.ResponseWriter, request *http.Request, status int, code, message string) {
	writePublicJSON(w, status, publicError{Error: publicErrorDetail{
		Code: code, Message: message, CorrelationID: correlation.ID(request.Context()),
	}})
}

func writePublicJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
