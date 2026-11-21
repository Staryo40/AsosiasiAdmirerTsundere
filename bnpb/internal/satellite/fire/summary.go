package fire

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"time"
)

type Fire struct {
	ID             string
	Active         bool
	Latitude       float64
	Longitude      float64
	AreaName       string
	OccurredAt     time.Time
	Severity       string
	DetectionCount int
	Satellites     []string
	MaxConfidence  string
	TotalFRP       float64
	DetectionKeys  []string
}

// Stable for the anchor detection, so reprocessing the same stream names every fire the same way.
func fireID(anchorKey string) string {
	digest := sha256.Sum256([]byte(anchorKey))
	return "fire-" + hex.EncodeToString(digest[:8])
}

func severity(count int, totalFRP float64) string {
	switch {
	case count >= 20 || totalFRP >= 100:
		return "AWAS"
	case count >= 6 || totalFRP >= 30:
		return "SIAGA"
	default:
		return "WASPADA"
	}
}

var confidenceRank = map[string]int{"l": 0, "n": 1, "h": 2}

// Sums run in key order so floating-point results do not depend on map order.
func summarize(state *fireState) Fire {
	keys := make([]string, 0, len(state.members))
	for key := range state.members {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	summary := Fire{ID: state.id, Active: true, DetectionCount: len(keys), DetectionKeys: keys, MaxConfidence: "l"}
	satellites := map[string]bool{}
	var latitude, longitude, frp float64
	for index, key := range keys {
		member := state.members[key]
		latitude += member.Latitude
		longitude += member.Longitude
		frp += member.FRP
		satellites[member.Satellite] = true
		if confidenceRank[member.Confidence] > confidenceRank[summary.MaxConfidence] {
			summary.MaxConfidence = member.Confidence
		}
		if index == 0 || member.Time.Before(summary.OccurredAt) {
			summary.OccurredAt = member.Time.UTC()
		}
	}
	for satellite := range satellites {
		summary.Satellites = append(summary.Satellites, satellite)
	}
	sort.Strings(summary.Satellites)
	count := float64(len(keys))
	summary.Latitude, summary.Longitude = latitude/count, longitude/count
	summary.TotalFRP = math.Round(frp*100) / 100
	summary.Severity = severity(summary.DetectionCount, summary.TotalFRP)
	summary.AreaName = nearestProvince(summary.Latitude, summary.Longitude)
	return summary
}
