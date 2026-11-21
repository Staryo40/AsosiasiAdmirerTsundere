package fire

import (
	_ "embed"
	"encoding/json"
	"math"
)

const (
	earthRadiusKM  = 6371.0
	linkDistanceKM = 1.0
	linkMinutes    = 360
	// 0.012 degrees is over 1 km in both axes for |latitude| below 25, so 3x3 cells hold every candidate.
	cellDegrees = 0.012
)

//go:embed provinces.json
var provinceData []byte

type province struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

var provinces = func() []province {
	var table struct {
		Provinces []province `json:"provinces"`
	}
	if err := json.Unmarshal(provinceData, &table); err != nil || len(table.Provinces) != 38 {
		panic("fire: provinces.json must hold 38 provinces")
	}
	return table.Provinces
}()

func distanceKM(latA, lonA, latB, lonB float64) float64 {
	toRadians := math.Pi / 180
	dLat, dLon := (latB-latA)*toRadians, (lonB-lonA)*toRadians
	h := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(latA*toRadians)*math.Cos(latB*toRadians)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKM * math.Asin(math.Min(1, math.Sqrt(h)))
}

func nearestProvince(latitude, longitude float64) string {
	best, bestDistance := "", math.Inf(1)
	for _, candidate := range provinces {
		if distance := distanceKM(latitude, longitude, candidate.Latitude, candidate.Longitude); distance < bestDistance {
			best, bestDistance = candidate.Name, distance
		}
	}
	return best
}

type cell struct{ lat, lon int32 }

func cellOf(latitude, longitude float64) cell {
	return cell{int32(math.Floor(latitude / cellDegrees)), int32(math.Floor(longitude / cellDegrees))}
}

func linked(a, b *node) bool {
	gap := a.minute - b.minute
	if gap < 0 {
		gap = -gap
	}
	return gap <= linkMinutes && distanceKM(a.Latitude, a.Longitude, b.Latitude, b.Longitude) <= linkDistanceKM
}
