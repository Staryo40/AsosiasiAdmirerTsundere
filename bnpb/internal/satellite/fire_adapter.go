package satellite

import (
	"encoding/json"
	"errors"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/satellite/fire"
)

var errUnusableDetection = errors.New("detection fields cannot be read")

func engineDetection(detection Detection) (fire.Detection, error) {
	at, err := detection.DetectedAt()
	if err != nil {
		return fire.Detection{}, err
	}
	latitude, latErr := detection.Latitude.Float64()
	longitude, lonErr := detection.Longitude.Float64()
	frp, frpErr := detection.FRP.Float64()
	if latErr != nil || lonErr != nil || frpErr != nil || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
		return fire.Detection{}, errUnusableDetection
	}
	return fire.Detection{Key: detection.Key(), Satellite: detection.Satellite, Time: at, Latitude: latitude,
		Longitude: longitude, Confidence: detection.Confidence, FRP: frp}, nil
}

// Heartbeats and malformed frames carry nothing for the engine; they still advance the checkpoint.
func applyToEngine(engine *fire.Engine, event Event) (fire.Outcome, error) {
	switch event.Kind {
	case KindDetection:
		detection, err := engineDetection(*event.Detection)
		if err != nil {
			return "", err
		}
		return engine.AddDetection(detection), nil
	case KindUpdate:
		var confidence *string
		var frp *float64
		if raw, present := event.Changes["confidence"]; present {
			var value string
			if json.Unmarshal(raw, &value) != nil {
				return "", errUnusableDetection
			}
			confidence = &value
		}
		if raw, present := event.Changes["frp"]; present {
			var value float64
			if json.Unmarshal(raw, &value) != nil {
				return "", errUnusableDetection
			}
			frp = &value
		}
		return engine.Update(event.Key, confidence, frp), nil
	case KindRetract:
		return engine.Retract(event.Key), nil
	}
	return "", nil
}
