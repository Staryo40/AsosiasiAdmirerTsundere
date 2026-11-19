package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/jackc/pgx/v5/pgxpool"
)

const catalogTokenHeader = "Authorization"

const catalogForwardedHeader = "X-BNPB-Catalog-Forwarded"

type catalogAPI struct {
	store     *Store
	cfg       Config
	authority *PollAuthority
}

type catalogReceipt struct {
	InputID     string            `json:"input_id"`
	InputOffset int               `json:"input_offset"`
	ReceivedAt  time.Time         `json:"received_at"`
	Records     []json.RawMessage `json:"records"`
}

func registerCatalogRoutes(mux *http.ServeMux, store *Store, cfg Config, authority *PollAuthority) {
	api := catalogAPI{store: store, cfg: cfg, authority: authority}
	mux.Handle("POST /internal/catalog/seismic", api.authenticate(http.HandlerFunc(api.ingest)))
	mux.Handle("GET /internal/storage/measurements", api.authenticate(http.HandlerFunc(api.measurements)))
}

func (api catalogAPI) authenticate(next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(api.cfg.Catalog.Token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		values := r.Header.Values(catalogTokenHeader)
		missing := len(values) == 0
		valid := len(values) == 1 && strings.HasPrefix(values[0], "Bearer ") && api.cfg.Catalog.Token != ""
		if valid {
			provided := sha256.Sum256([]byte(strings.TrimPrefix(values[0], "Bearer ")))
			valid = subtle.ConstantTimeCompare(expected[:], provided[:]) == 1
		}
		if !valid {
			status, code := http.StatusForbidden, "invalid_credential"
			if missing {
				status, code = http.StatusUnauthorized, "missing_credential"
			}
			writeReadError(w, r, status, code)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (api catalogAPI) ingest(w http.ResponseWriter, r *http.Request) {
	ctx, localOwner := api.ownerContext(w, r)
	if !localOwner {
		return
	}
	receipt, err := decodeCatalogReceipt(w, r)
	if err != nil {
		writeReadError(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := api.ingestCatalogChunks(ctx, receipt); err != nil {
		code := "storage_unavailable"
		if errors.Is(err, errAuthorityUnavailable) || errors.Is(err, errStaleMutationAuthority) {
			code = "authority_unavailable"
		}
		writeReadError(w, r, http.StatusServiceUnavailable, code)
		return
	}
	retained, err := api.store.catalogReceiptOutcomes(r.Context(), receipt)
	if err != nil {
		writeReadError(w, r, http.StatusServiceUnavailable, "storage_unavailable")
		return
	}
	if retained {
		writeReadError(w, r, http.StatusConflict, "catalogue_input_rejected")
		return
	}
	writeReadJSON(w, http.StatusOK, map[string]any{"accepted": len(receipt.Records), "input_id": receipt.InputID, "input_offset": receipt.InputOffset})
}

func (api catalogAPI) ownerContext(w http.ResponseWriter, r *http.Request) (context.Context, bool) {
	if api.authority == nil {
		return r.Context(), true
	}
	ctx, err := api.authority.currentContext(r.Context())
	if err == nil {
		return ctx, true
	}
	if r.Header.Get(catalogForwardedHeader) != "" {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return nil, false
	}
	endpoint, err := api.authority.activeOwnerEndpoint(r.Context())
	if err != nil || endpoint == "" {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return nil, false
	}
	api.forwardCatalog(w, r, endpoint)
	return nil, false
}

func decodeCatalogReceipt(w http.ResponseWriter, r *http.Request) (catalogReceipt, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, catalogMaxBodyBytes))
	if err != nil {
		return catalogReceipt{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var receipt catalogReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return catalogReceipt{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return catalogReceipt{}, errors.New("catalogue receipt has trailing JSON")
	}
	if receipt.InputID == "" || len(receipt.InputID) > 128 || receipt.InputOffset < 0 || !validInstant(receipt.ReceivedAt) || len(receipt.Records) == 0 || len(receipt.Records) > 1000 {
		return catalogReceipt{}, errors.New("catalogue receipt fields are invalid")
	}
	for _, record := range receipt.Records {
		trimmed := bytes.TrimSpace(record)
		if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
			return catalogReceipt{}, errors.New("catalogue record is not a JSON object")
		}
	}
	return receipt, nil
}

func (api catalogAPI) ingestCatalogChunks(ctx context.Context, receipt catalogReceipt) error {
	chunkSize := api.cfg.IngestChunkRecords
	if chunkSize <= 0 {
		return errors.New("catalogue ingest chunk size is invalid")
	}
	for start := 0; start < len(receipt.Records); start += chunkSize {
		end := min(start+chunkSize, len(receipt.Records))
		chunk := IngestChunk{
			Endpoint: SeismicEndpoint, Records: receipt.Records[start:end], ReceivedAt: receipt.ReceivedAt,
			InputID: receipt.InputID, InputOffset: receipt.InputOffset + start,
		}
		if err := api.store.ingestChunk(ctx, chunk, checkpointPolicy{omitPollState: true}); err != nil {
			return err
		}
	}
	return nil
}

const catalogMaxBodyBytes = 4 << 20

func (api catalogAPI) forwardCatalog(w http.ResponseWriter, r *http.Request, endpoint string) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, catalogMaxBodyBytes))
	if err != nil {
		writeReadError(w, r, http.StatusBadRequest, "invalid_request")
		return
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, strings.TrimRight(endpoint, "/")+"/internal/catalog/seismic", bytes.NewReader(body))
	if err != nil {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return
	}
	request.Header.Set(catalogTokenHeader, r.Header.Get(catalogTokenHeader))
	request.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	request.Header.Set(correlation.Header, correlation.ID(r.Context()))
	correlation.InjectTraceHeaders(r.Context(), request.Header)
	request.Header.Set(catalogForwardedHeader, "1")
	api.relayCatalog(w, r, request)
}

func (api catalogAPI) relayCatalog(w http.ResponseWriter, r *http.Request, request *http.Request) {
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return
	}
	defer response.Body.Close()
	const maxResponse = 4 << 20
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
	if err != nil || int64(len(responseBody)) > maxResponse {
		writeReadError(w, r, http.StatusServiceUnavailable, "authority_unavailable")
		return
	}
	w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(responseBody)
}

type catalogOutcomePool struct {
	pool *pgxpool.Pool
}

type catalogOutcomeResult struct {
	identities map[string][]string
	err        error
}

// catalogReceiptOutcomes verifies that every receipt ordinal has exactly one
// durable handled-input outcome without reading any retained payload.
func (store *Store) catalogReceiptOutcomes(parent context.Context, receipt catalogReceipt) (bool, error) {
	hashes, expected := catalogInputHashes(receipt)
	pools, err := catalogOutcomePools(store)
	if err != nil {
		return false, err
	}
	timeout := store.readTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	outcomes, err := queryCatalogOutcomePools(ctx, pools, hashes, expected)
	if err != nil {
		return false, err
	}
	return classifyCatalogOutcomes(outcomes, expected)
}

func catalogInputHashes(receipt catalogReceipt) ([][]byte, map[string]struct{}) {
	hashes := make([][]byte, len(receipt.Records))
	expected := make(map[string]struct{}, len(receipt.Records))
	chunk := IngestChunk{Endpoint: SeismicEndpoint, InputID: receipt.InputID, InputOffset: receipt.InputOffset}
	for index := range receipt.Records {
		chunk.InputOffset = receipt.InputOffset + index
		hash := inputHash(chunk)
		hashes[index] = hash
		expected[string(hash)] = struct{}{}
	}
	return hashes, expected
}

func catalogOutcomePools(store *Store) ([]catalogOutcomePool, error) {
	routed := false
	for _, shard := range store.shards {
		routed = routed || shard != nil
	}
	pools := make([]catalogOutcomePool, 0, len(store.shards)+1)
	if store.read == nil {
		return nil, errors.New("catalogue metadata unavailable")
	}
	if !routed {
		return append(pools, catalogOutcomePool{pool: store.read}), nil
	}
	pools = append(pools, catalogOutcomePool{pool: store.read})
	for _, shard := range store.shards {
		if shard == nil || shard.read == nil {
			return nil, errors.New("catalogue shard metadata unavailable")
		}
		pools = append(pools, catalogOutcomePool{pool: shard.read})
	}
	return pools, nil
}

func queryCatalogOutcomePools(ctx context.Context, pools []catalogOutcomePool, hashes [][]byte, expected map[string]struct{}) (map[string][]string, error) {
	results := make([]catalogOutcomeResult, len(pools))
	var wait sync.WaitGroup
	wait.Add(len(pools))
	for index, resource := range pools {
		go func(index int, pool *pgxpool.Pool) {
			defer wait.Done()
			results[index].identities, results[index].err = queryCatalogOutcomes(ctx, pool, hashes, expected)
		}(index, resource.pool)
	}
	wait.Wait()
	outcomes := make(map[string][]string, len(expected))
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}
		for key, statuses := range result.identities {
			outcomes[key] = append(outcomes[key], statuses...)
		}
	}
	return outcomes, nil
}

func queryCatalogOutcomes(ctx context.Context, pool *pgxpool.Pool, hashes [][]byte, expected map[string]struct{}) (map[string][]string, error) {
	rows, err := pool.Query(ctx, `SELECT endpoint,input_hash,status FROM handled_inputs
		WHERE endpoint=$1 AND input_hash=ANY($2::bytea[])`, string(SeismicEndpoint), hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	outcomes := make(map[string][]string)
	for rows.Next() {
		var endpoint, status string
		var hash []byte
		if err := rows.Scan(&endpoint, &hash, &status); err != nil {
			return nil, err
		}
		key := string(hash)
		if endpoint != string(SeismicEndpoint) {
			return nil, errors.New("catalogue metadata endpoint mismatch")
		}
		if _, ok := expected[key]; !ok {
			return nil, errors.New("unexpected catalogue metadata identity")
		}
		outcomes[key] = append(outcomes[key], status)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return outcomes, nil
}

func classifyCatalogOutcomes(outcomes map[string][]string, expected map[string]struct{}) (bool, error) {
	retained := false
	for key := range expected {
		statuses := outcomes[key]
		if len(statuses) != 1 {
			return false, errors.New("catalogue metadata outcome missing or ambiguous")
		}
		switch statuses[0] {
		case "applied":
		case "retained":
			retained = true
		default:
			return false, errors.New("catalogue metadata status unavailable")
		}
	}
	return retained, nil
}
