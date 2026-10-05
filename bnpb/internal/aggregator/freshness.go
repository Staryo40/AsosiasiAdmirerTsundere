package aggregator

import "time"

type pollObservation struct {
	Status             string     `json:"status"`
	LastSuccessfulPoll *time.Time `json:"last_successful_poll"`
	StaleSince         *time.Time `json:"stale_since"`
	LastAttemptAt      *time.Time `json:"last_attempt_at"`
	ErrorCategory      *string    `json:"error_category"`
}

type sourceState struct {
	pollObservation
	HasCachedData bool                       `json:"has_cached_data"`
	Endpoints     map[string]pollObservation `json:"endpoints"`
}

type endpointSnapshot struct {
	endpoint Endpoint
	pollObservation
	cached bool
}

func endpointObservation(snapshot endpointSnapshot, asOf time.Time, cfg Config) pollObservation {
	observation := snapshot.pollObservation
	observation.Status = "healthy"
	if observation.LastSuccessfulPoll == nil {
		observation.Status = "unavailable"
	}
	if observation.ErrorCategory != nil || observation.StaleSince != nil {
		if observation.LastSuccessfulPoll != nil {
			observation.Status = "stale"
		}
		observation.ErrorCategory = safePollCategory(observation.ErrorCategory)
		observation.StaleSince = earliestTime(observation.StaleSince, observation.LastAttemptAt)
		return observation
	}
	return overdueObservation(observation, asOf, endpointThreshold(snapshot.endpoint, cfg))
}

func endpointThreshold(endpoint Endpoint, cfg Config) time.Duration {
	source := cfg.BMKG
	if endpoint == VolcanicEndpoint {
		source = cfg.PVMBG
		return max(3*source.PollInterval, cfg.PVMBGCycleTimeout+source.PollInterval)
	}
	return max(3*source.PollInterval, source.HTTPTimeout+source.PollInterval)
}

func overdueObservation(observation pollObservation, asOf time.Time, threshold time.Duration) pollObservation {
	boundary := observation.LastAttemptAt
	if observation.LastSuccessfulPoll != nil {
		stamp := observation.LastSuccessfulPoll.Add(threshold)
		boundary = &stamp
	} else if boundary == nil || asOf.Before(boundary.Add(threshold)) {
		return observation
	}
	if boundary != nil && !asOf.Before(*boundary) {
		observation.StaleSince = boundary
		category := "timeout"
		observation.ErrorCategory = &category
		if observation.LastSuccessfulPoll != nil {
			observation.Status = "stale"
		}
	}
	return observation
}

func safePollCategory(category *string) *string {
	if category == nil {
		return nil
	}
	switch *category {
	case "outage", "auth", "rate_limited", "timeout", "network", "invalid_response", "store":
		return category
	default:
		redacted := "invalid_response"
		return &redacted
	}
}

func sourceFreshness(snapshots []endpointSnapshot, asOf time.Time, cfg Config) sourceState {
	state := sourceState{pollObservation: pollObservation{Status: "healthy"}, Endpoints: make(map[string]pollObservation)}
	allSuccessful := true
	for _, snapshot := range snapshots {
		observation := endpointObservation(snapshot, asOf, cfg)
		allSuccessful = allSuccessful && observation.LastSuccessfulPoll != nil
		state.addEndpointObservation(snapshot, observation)
	}
	if !allSuccessful {
		state.LastSuccessfulPoll = nil
	}
	if snapshots[0].LastSuccessfulPoll == nil && !state.HasCachedData {
		state.Status = "unavailable"
	}
	return state
}

func (state *sourceState) addEndpointObservation(snapshot endpointSnapshot, observation pollObservation) {
	state.Endpoints[publicEndpoint(snapshot.endpoint)] = observation
	state.HasCachedData = state.HasCachedData || snapshot.cached
	state.LastSuccessfulPoll = earliestTime(state.LastSuccessfulPoll, observation.LastSuccessfulPoll)
	state.LastAttemptAt = latestTime(state.LastAttemptAt, observation.LastAttemptAt)
	state.StaleSince = earliestTime(state.StaleSince, observation.StaleSince)
	if state.ErrorCategory == nil {
		state.ErrorCategory = observation.ErrorCategory
	}
	if observation.Status != "healthy" {
		state.Status = "stale"
	}
}

func publicEndpoint(endpoint Endpoint) string {
	switch endpoint {
	case SeismicEndpoint:
		return "seismic_events"
	case WarningEndpoint:
		return "tsunami_warnings"
	default:
		return "volcanic_reports"
	}
}

func earliestTime(first, second *time.Time) *time.Time {
	if first == nil || (second != nil && second.Before(*first)) {
		return second
	}
	return first
}

func latestTime(first, second *time.Time) *time.Time {
	if first == nil || (second != nil && second.After(*first)) {
		return second
	}
	return first
}

func sourcesUnavailable(sources map[Source]sourceState) bool {
	for _, state := range sources {
		if state.Status != "unavailable" || state.HasCachedData {
			return false
		}
	}
	return true
}
