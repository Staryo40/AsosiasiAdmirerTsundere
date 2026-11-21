package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

const wildfireForwardedHeader = "X-BNPB-Wildfire-Forwarded"

type wildfireAPI struct {
	store     *Store
	cfg       Config
	authority *PollAuthority
}

func loadSatelliteIngestToken(lookup func(string) (string, bool)) (string, error) {
	token, configured := lookup("BNPB_SATELLITE_INGEST_TOKEN")
	if !configured || token == "" {
		return "", nil
	}
	if len(token) < 32 || strings.TrimSpace(token) != token || strings.IndexFunc(token, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("BNPB_SATELLITE_INGEST_TOKEN: must contain at least 32 bytes without whitespace or control characters")
	}
	return token, nil
}

// The satellite service's only way into the Canonical Store; it never gets the internal read key.
func registerWildfireRoute(mux *http.ServeMux, store *Store, cfg Config, authority *PollAuthority) {
	if cfg.SatelliteIngestToken == "" {
		return
	}
	api := wildfireAPI{store: store, cfg: cfg, authority: authority}
	mux.Handle("POST /internal/satellite/fires", api.authenticate(http.HandlerFunc(api.ingest)))
}

func (api wildfireAPI) authenticate(next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(api.cfg.SatelliteIngestToken))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		values := r.Header.Values("Authorization")
		token, found := "", false
		if len(values) == 1 {
			token, found = strings.CutPrefix(values[0], "Bearer ")
		}
		provided := sha256.Sum256([]byte(token))
		switch {
		case len(values) == 0:
			writeReadError(w, r, http.StatusUnauthorized, "missing_credential")
		case !found || subtle.ConstantTimeCompare(expected[:], provided[:]) != 1:
			writeReadError(w, r, http.StatusForbidden, "invalid_credential")
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func (api wildfireAPI) ingest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, catalogMaxBodyBytes))
	if err != nil {
		writeReadError(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	ctx, local := api.ownerContext(w, r, body)
	if !local {
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	receipt, err := decodeCatalogReceipt(w, r)
	if err != nil {
		writeReadError(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := api.ingestReceipt(ctx, receipt); err != nil {
		code, status := "storage_unavailable", http.StatusServiceUnavailable
		switch {
		case errors.Is(err, errReceiptConflict):
			code, status = "input_conflict", http.StatusConflict
		case errors.Is(err, errAuthorityUnavailable) || errors.Is(err, errStaleMutationAuthority):
			code = "authority_unavailable"
		}
		writeReadError(w, r, status, code)
		return
	}
	// Records the store rejected are kept as failed messages; resending them would not change that.
	writeReadJSON(w, http.StatusOK, map[string]any{"accepted": len(receipt.Records), "input_id": receipt.InputID})
}

func (api wildfireAPI) ingestReceipt(ctx context.Context, receipt catalogReceipt) error {
	batch := IngestChunk{Endpoint: WildfireEndpoint, Records: receipt.Records, ReceivedAt: receipt.ReceivedAt,
		InputID: receipt.InputID, InputOffset: receipt.InputOffset}
	policy := checkpointPolicy{omitPollState: true}
	size := api.cfg.IngestChunkRecords
	if size <= 0 {
		return errors.New("wildfire ingest chunk size is invalid")
	}
	routed := api.store.shards[0] != nil
	for start := 0; start < len(batch.Records); start += size {
		chunk := batch
		end := min(start+size, len(batch.Records))
		chunk.Records, chunk.InputOffset = batch.Records[start:end], batch.InputOffset+start
		if routed {
			if err := api.store.validateChunk(chunk); err != nil {
				return err
			}
		} else if err := api.store.ingestChunk(ctx, chunk, policy); err != nil {
			return err
		}
	}
	if routed {
		return api.store.ingestShardChunk(ctx, batch, policy)
	}
	return nil
}

// Writes need the poll owner's mutation authority; any other instance forwards the original bytes once.
func (api wildfireAPI) ownerContext(w http.ResponseWriter, r *http.Request, body []byte) (context.Context, bool) {
	if api.authority == nil {
		return r.Context(), true
	}
	ctx, err := api.authority.currentContext(r.Context())
	if err == nil {
		return ctx, true
	}
	endpoint, ownerErr := api.authority.activeOwnerEndpoint(r.Context())
	if r.Header.Get(wildfireForwardedHeader) != "" || ownerErr != nil || endpoint == "" {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return nil, false
	}
	forward, err := http.NewRequestWithContext(r.Context(), http.MethodPost, strings.TrimRight(endpoint, "/")+"/internal/satellite/fires", bytes.NewReader(body))
	if err != nil {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return nil, false
	}
	forward.Header.Set("Authorization", r.Header.Get("Authorization"))
	forward.Header.Set("Content-Type", "application/json")
	forward.Header.Set(correlation.Header, correlation.ID(r.Context()))
	correlation.InjectTraceHeaders(r.Context(), forward.Header)
	forward.Header.Set(wildfireForwardedHeader, "1")
	catalogAPI{}.relayCatalog(w, r, forward)
	return nil, false
}
