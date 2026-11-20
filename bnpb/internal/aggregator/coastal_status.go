package aggregator

import (
	"net/http"
	"time"
)

type coastalStatus struct {
	Source      Source          `json:"source"`
	Status      string          `json:"status"`
	Breaker     string          `json:"breaker_state"`
	Poll        pollObservation `json:"poll"`
	LastHazard  *HazardEvent    `json:"last_hazard,omitempty"`
	AsOf        time.Time       `json:"as_of"`
	PartialData bool            `json:"partial_data"`
}

func (api snapshotAPI) coastalStatus(w http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	asOf := time.Now().UTC()
	position, err := api.store.pollPosition(ctx, CoastalEndpoint)
	if err != nil {
		writeReadError(w, request, http.StatusServiceUnavailable, "store_unavailable")
		return
	}
	snapshot := endpointSnapshot{endpoint: CoastalEndpoint, pollObservation: position.observation}
	observation := endpointObservation(snapshot, asOf, api.cfg)
	breaker := "unknown"
	if api.cfg.PantaiAuthToken != "" {
		breaker = api.store.coastalBreakerState()
	}
	last, err := api.store.latestCoastalHazard(ctx)
	if err != nil {
		writeReadError(w, request, http.StatusServiceUnavailable, "store_unavailable")
		return
	}
	partial := observation.Status != "healthy" && last.HazardID != ""
	payload := coastalStatus{
		Source: SensorPantai, Status: observation.Status, Breaker: breaker, Poll: observation,
		AsOf: asOf, PartialData: partial,
	}
	if last.HazardID != "" {
		payload.LastHazard = &last
	}
	writeReadJSON(w, http.StatusOK, payload)
}

func registerCoastalRoutes(mux *http.ServeMux, api snapshotAPI) {
	mux.Handle("GET /internal/coastal/status", api.authenticate(http.HandlerFunc(api.coastalStatus)))
}