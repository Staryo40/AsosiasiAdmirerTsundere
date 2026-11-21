package satellite

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	KindDetection = "detection"
	KindUpdate    = "update"
	KindRetract   = "retract"
	KindHeartbeat = "heartbeat"
)

// Numbers stay json.Number so detection keys keep the feed's exact text, e.g. 113.80440 and acq_time 2.
type Detection struct {
	Satellite  string      `json:"satellite"`
	AcqDate    string      `json:"acq_date"`
	AcqTime    json.Number `json:"acq_time"`
	Latitude   json.Number `json:"latitude"`
	Longitude  json.Number `json:"longitude"`
	Confidence string      `json:"confidence"`
	FRP        json.Number `json:"frp"`
}

func (detection Detection) Key() string {
	return detection.Satellite + "|" + detection.AcqDate + "|" + detection.AcqTime.String() + "|" +
		detection.Latitude.String() + "|" + detection.Longitude.String()
}

// acq_time is HHMM without zero padding, in UTC.
func (detection Detection) DetectedAt() (time.Time, error) {
	day, err := time.Parse("2006-01-02", detection.AcqDate)
	if err != nil {
		return time.Time{}, err
	}
	clock, err := detection.AcqTime.Int64()
	if err != nil || clock < 0 || clock > 2359 || clock%100 > 59 {
		return time.Time{}, fmt.Errorf("acq_time %q is not HHMM", detection.AcqTime)
	}
	return day.Add(time.Duration(clock/100)*time.Hour + time.Duration(clock%100)*time.Minute), nil
}

type Event struct {
	Offset     int64
	Kind       string
	Detection  *Detection
	Key        string
	Changes    map[string]json.RawMessage
	Reason     string
	VirtualNow time.Time
}

type envelope struct {
	Offset     *int64                     `json:"offset"`
	Detection  *Detection                 `json:"detection"`
	Key        string                     `json:"key"`
	Changes    map[string]json.RawMessage `json:"changes"`
	Reason     string                     `json:"reason"`
	VirtualNow string                     `json:"virtual_now"`
}

var errMalformedEvent = errors.New("malformed feed event")

func parseEvent(frame sseFrame) (Event, error) {
	var body envelope
	decoder := json.NewDecoder(bytes.NewReader(frame.data))
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil || body.Offset == nil {
		return Event{}, errMalformedEvent
	}
	event := Event{Offset: *body.Offset, Kind: frame.event}
	if frame.id != "" && frame.id != fmt.Sprint(event.Offset) {
		return Event{}, fmt.Errorf("%w: id %s does not match offset %d", errMalformedEvent, frame.id, event.Offset)
	}
	switch frame.event {
	case KindDetection:
		if body.Detection == nil || body.Detection.Satellite == "" {
			return Event{}, errMalformedEvent
		}
		event.Detection = body.Detection
	case KindUpdate:
		if body.Key == "" || len(body.Changes) == 0 {
			return Event{}, errMalformedEvent
		}
		event.Key, event.Changes = body.Key, body.Changes
	case KindRetract:
		if body.Key == "" {
			return Event{}, errMalformedEvent
		}
		event.Key, event.Reason = body.Key, body.Reason
	case KindHeartbeat:
		now, err := time.Parse(time.RFC3339Nano, body.VirtualNow)
		if err != nil {
			return Event{}, errMalformedEvent
		}
		event.VirtualNow = now.UTC()
	}
	return event, nil
}
