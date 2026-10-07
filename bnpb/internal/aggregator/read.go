package aggregator

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

const internalKeyHeader = "X-BNPB-Internal-Key"

type snapshotAPI struct {
	store *Store
	cfg   Config
	slots chan struct{}
}

type applicationError struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlation_id"`
}

type readErrorEnvelope struct {
	Error   applicationError       `json:"error"`
	Sources map[Source]sourceState `json:"sources,omitempty"`
	AsOf    *time.Time             `json:"as_of,omitempty"`
}

func registerSnapshotRoutes(mux *http.ServeMux, store *Store, cfg Config) {
	api := snapshotAPI{store: store, cfg: cfg, slots: make(chan struct{}, cfg.ReadMaxInFlight)}
	mux.Handle("GET /internal/hazards", api.authenticate(http.HandlerFunc(api.list)))
	mux.Handle("GET /internal/hazards/{hazard_id}", api.authenticate(http.HandlerFunc(api.detail)))
}

func (api snapshotAPI) authenticate(next http.Handler) http.Handler {
	expectedDigest := sha256.Sum256([]byte(api.cfg.InternalKey))
	admitted := api.limitSnapshotReads(next)
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if failureCode := api.credentialErrorCode(request, expectedDigest); failureCode != "" {
			status := http.StatusForbidden
			if failureCode == "missing_credential" {
				status = http.StatusUnauthorized
			}
			writeReadError(w, request, status, failureCode)
			return
		}
		admitted.ServeHTTP(w, request)
	})
}

func (api snapshotAPI) credentialErrorCode(request *http.Request, expectedDigest [sha256.Size]byte) string {
	credentials := request.Header.Values(internalKeyHeader)
	if len(credentials) == 0 {
		return "missing_credential"
	}
	providedDigest := sha256.Sum256([]byte(credentials[0]))
	if len(credentials) != 1 || api.cfg.InternalKey == "" || subtle.ConstantTimeCompare(expectedDigest[:], providedDigest[:]) != 1 {
		return "invalid_credential"
	}
	return ""
}

func (api snapshotAPI) limitSnapshotReads(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		select {
		case api.slots <- struct{}{}:
			defer func() { <-api.slots }()
			next.ServeHTTP(w, request)
		default:
			writeReadJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many requests"})
		}
	})
}

func (api snapshotAPI) list(w http.ResponseWriter, request *http.Request) {
	query, err := parseHazardQuery(request.URL.RawQuery, api.cfg)
	if err != nil {
		writeReadError(w, request, http.StatusBadRequest, "invalid_query")
		return
	}
	page, err := api.store.listHazards(request.Context(), query, api.cfg)
	if err != nil {
		writeReadError(w, request, http.StatusServiceUnavailable, "store_unavailable")
		return
	}
	if sourcesUnavailable(page.Sources) {
		failure := readErrorEnvelope{Error: readFailure(request, "source_unavailable"), Sources: page.Sources, AsOf: &page.AsOf}
		writeReadJSON(w, http.StatusServiceUnavailable, failure)
		return
	}
	writeReadJSON(w, http.StatusOK, page)
}

func (api snapshotAPI) detail(w http.ResponseWriter, request *http.Request) {
	hazardID := request.PathValue("hazard_id")
	if request.URL.RawQuery != "" || request.URL.ForceQuery || !utf8.ValidString(hazardID) || !validText(hazardID) || len(hazardID) > 2048 {
		writeReadError(w, request, http.StatusBadRequest, "invalid_query")
		return
	}
	detail, err := api.store.hazardDetail(request.Context(), hazardID, api.cfg)
	if err == errHazardNotFound {
		writeReadError(w, request, http.StatusNotFound, "not_found")
		return
	}
	if err != nil {
		writeReadError(w, request, http.StatusServiceUnavailable, "store_unavailable")
		return
	}
	writeReadJSON(w, http.StatusOK, detail)
}

func readFailure(request *http.Request, code string) applicationError {
	messages := map[string]string{
		"invalid_query": "invalid query parameters", "missing_credential": "internal credential is required",
		"invalid_credential": "invalid internal credential", "not_found": "hazard not found",
		"source_unavailable": "selected sources are unavailable", "store_unavailable": "canonical store is unavailable",
	}
	return applicationError{code, messages[code], correlation.ID(request.Context())}
}

func writeReadError(w http.ResponseWriter, request *http.Request, status int, code string) {
	writeReadJSON(w, status, readErrorEnvelope{Error: readFailure(request, code)})
}

func writeReadJSON(w http.ResponseWriter, status int, response any) {
	encoded, err := json.Marshal(response)
	if err != nil {
		status, encoded = http.StatusInternalServerError, []byte(`{"error":"internal server error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}
