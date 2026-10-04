package aggregator

import (
	_ "embed"
	"encoding/json"
)

type volcanoLocation struct {
	Name           string  `json:"name"`
	Latitude       float64 `json:"latitude"`
	Longitude      float64 `json:"longitude"`
	SourceURL      string  `json:"source_url"`
	Interpretation string  `json:"interpretation"`
	VerifiedOn     string  `json:"verified_on"`
}

//go:embed reference/volcanoes.json
var volcanoData []byte

var volcanoReference = loadVolcanoReference()

func loadVolcanoReference() map[string]volcanoLocation {
	var reference map[string]volcanoLocation
	if err := json.Unmarshal(volcanoData, &reference); err != nil {
		panic("invalid bundled BNPB volcano reference")
	}
	return reference
}
