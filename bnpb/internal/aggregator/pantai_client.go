package aggregator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
)

type pantaiStation struct {
	StationID string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type pantaiObservations struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Items       []json.RawMessage `json:"items"`
	NextCursor  string            `json:"next_cursor"`
}

type pantaiClient struct {
	http       *http.Client
	origin     *url.URL
	token      string
	maxBytes   int64
	breaker    *circuitBreaker
	inFlight   chan struct{}
	stations   map[string]pantaiStation
	stationsAt time.Time
}

func newPantaiClient(cfg Config, logger *slog.Logger) *pantaiClient {
	origin, _ := url.Parse(cfg.Pantai.BaseURL)
	slots := cfg.PantaiResilience.MaxInFlightCalls
	if slots < 1 {
		slots = 1
	}
	return &pantaiClient{
		http:     httpclient.New(cfg.Pantai.HTTPTimeout, logger),
		origin:   origin,
		token:    cfg.PantaiAuthToken,
		maxBytes: cfg.MaxResponseBytes,
		breaker:  newCircuitBreaker(cfg.PantaiResilience.BreakerFailures, cfg.PantaiResilience.BreakerCooldown),
		inFlight: make(chan struct{}, slots),
		stations: make(map[string]pantaiStation),
	}
}

func (client *pantaiClient) closeIdleConnections() {
	client.http.CloseIdleConnections()
}

func (client *pantaiClient) observations(ctx context.Context, cursor string) (pantaiObservations, error) {
	now := time.Now().UTC()
	if !client.breaker.allow(now) {
		return pantaiObservations{}, &sourceError{category: "circuit_open"}
	}
	select {
	case client.inFlight <- struct{}{}:
		defer func() { <-client.inFlight }()
	default:
		return pantaiObservations{}, &sourceError{category: "bulkhead_rejected"}
	}
	query := url.Values{}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	query.Set("limit", "100")
	var payload pantaiObservations
	err := client.getJSON(ctx, "/v1/observations", query, &payload)
	if err != nil {
		client.breaker.failure(now)
		return pantaiObservations{}, err
	}
	client.breaker.success()
	return payload, nil
}

func (client *pantaiClient) station(ctx context.Context, stationID string) (pantaiStation, error) {
	if station, ok := client.stations[stationID]; ok && time.Since(client.stationsAt) < time.Hour {
		return station, nil
	}
	if err := client.refreshStations(ctx); err != nil {
		return pantaiStation{}, err
	}
	station, ok := client.stations[stationID]
	if !ok {
		return pantaiStation{}, invalidField("station_id", InvalidValue)
	}
	return station, nil
}

type pantaiStations struct {
	Items []pantaiStation `json:"items"`
}

func (client *pantaiClient) refreshStations(ctx context.Context) error {
	var payload pantaiStations
	if err := client.getJSON(ctx, "/v1/stations", nil, &payload); err != nil {
		return err
	}
	next := make(map[string]pantaiStation, len(payload.Items))
	for _, station := range payload.Items {
		if station.StationID == "" {
			continue
		}
		next[station.StationID] = station
	}
	client.stations = next
	client.stationsAt = time.Now().UTC()
	return nil
}

func (client *pantaiClient) getJSON(ctx context.Context, path string, query url.Values, target any) error {
	ctx, cancel := context.WithTimeout(ctx, client.http.Timeout)
	defer cancel()
	address := *client.origin
	address.Path = path
	address.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address.String(), nil)
	if err != nil {
		return &sourceError{category: "invalid_request"}
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+client.token)
	response, err := client.http.Do(request)
	if err != nil {
		return safeSourceFailure("transport_error", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &sourceError{category: "http_status", statusCode: response.StatusCode,
			retryAfter: retryAfter(response.Header.Get("Retry-After"), time.Now().UTC())}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, client.maxBytes+1))
	if err != nil {
		return safeSourceFailure("read_error", err)
	}
	if int64(len(body)) > client.maxBytes {
		return &sourceError{category: "response_too_large"}
	}
	if err := json.Unmarshal(body, target); err != nil {
		return &sourceError{category: "invalid_json"}
	}
	return nil
}

func (client *pantaiClient) breakerState() string {
	return client.breaker.state(time.Now().UTC())
}

func (client *pantaiClient) configured() bool {
	return client != nil && client.token != "" && client.origin != nil
}
