// Package placementcompare compares analytical placement policies over the same
// raw seismic-event JSONL input. It does not route writes or model replicas.
package placementcompare

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"
)

const shardCount = 3

// SeismicEvent is the raw BMKG catalogue shape used by the Aggregator mapper.
type SeismicEvent struct {
	EventID   string  `json:"event_id"`
	Region    string  `json:"region_name"`
	Longitude float64 `json:"epicenter_lon"`
}

type shardStats struct {
	LogicalRows int64 `json:"logical_rows"`
	InputBytes  int64 `json:"input_bytes"`
	Writes      int64 `json:"writes"`
}

type skew struct {
	Max       float64 `json:"max"`
	Mean      float64 `json:"mean"`
	MaxToMean float64 `json:"max_to_mean"`
}

type metricSkew struct {
	LogicalRows skew `json:"logical_rows"`
	InputBytes  skew `json:"input_bytes"`
	Writes      skew `json:"writes"`
}

type scenario struct {
	Rows   int64         `json:"logical_rows"`
	Shards [3]shardStats `json:"shards"`
	Skew   metricSkew    `json:"skew"`
}

type StrategyComparison struct {
	FixedHash     scenario `json:"fixed_hash"`
	LongitudeBand scenario `json:"longitude_band"`
}

type Result struct {
	Algorithm             string              `json:"algorithm"`
	InputSHA256           string              `json:"input_sha256"`
	InputRows             int64               `json:"input_rows"`
	ResourcesSHA256       string              `json:"resources_sha256"`
	BurstArea             string              `json:"burst_area,omitempty"`
	Natural               StrategyComparison  `json:"natural"`
	RegionBurst           *StrategyComparison `json:"region_burst,omitempty"`
	CountersAreAnalytical bool                `json:"counters_are_analytical"`
	CounterDisclosure     string              `json:"counter_disclosure"`
}

// CompareJSONL reads raw JSONL once, hashes the exact input bytes, and computes
// natural-catalogue and optional same-region subset distributions. A row is
// counted once per scenario; no synthetic IDs, writes, or records are added.
func CompareJSONL(input io.Reader, resourceManifest []byte, burstArea string) (Result, error) {
	if input == nil {
		return Result{}, errors.New("input is required")
	}
	if len(resourceManifest) == 0 {
		return Result{}, errors.New("resource manifest is required")
	}
	resourceHash := sha256.Sum256(resourceManifest)
	inputHash := sha256.New()
	reader := bufio.NewReader(io.TeeReader(input, inputHash))
	result := Result{
		Algorithm:             "sha256-32be-v1; longitude-bands-120deg-v1",
		ResourcesSHA256:       hex.EncodeToString(resourceHash[:]),
		BurstArea:             burstArea,
		CountersAreAnalytical: true,
		CounterDisclosure:     "Logical rows, JSONL record bytes (excluding line delimiters), and writes are analytical placement counters, not runtime node bytes or write latency; no physical routing or replicas are modeled.",
	}
	var burst StrategyComparison
	lineNumber := 0
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) != 0 {
			lineNumber++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) != 0 {
				var event SeismicEvent
				if decodeErr := json.Unmarshal(trimmed, &event); decodeErr != nil {
					return Result{}, fmt.Errorf("invalid JSONL record at line %d", lineNumber)
				}
				if !utf8.ValidString(event.EventID) || strings.TrimSpace(event.EventID) == "" || len(event.EventID) > 2048 || !utf8.ValidString(event.Region) || strings.TrimSpace(event.Region) == "" || math.IsNaN(event.Longitude) || math.IsInf(event.Longitude, 0) || event.Longitude < -180 || event.Longitude > 180 {
					return Result{}, fmt.Errorf("invalid seismic event at line %d", lineNumber)
				}
				rowBytes := int64(len(bytes.TrimRight(line, "\r\n")))
				fixed := fixedHashShard("BMKG", event.EventID)
				geo := longitudeShard(event.Longitude)
				add(&result.Natural.FixedHash, fixed, rowBytes)
				add(&result.Natural.LongitudeBand, geo, rowBytes)
				result.InputRows++
				if burstArea != "" && event.Region == burstArea {
					add(&burst.FixedHash, fixed, rowBytes)
					add(&burst.LongitudeBand, geo, rowBytes)
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return Result{}, errors.New("failed reading input JSONL")
		}
	}
	result.InputSHA256 = hex.EncodeToString(inputHash.Sum(nil))
	finalize(&result.Natural.FixedHash)
	finalize(&result.Natural.LongitudeBand)
	if burstArea != "" {
		finalize(&burst.FixedHash)
		finalize(&burst.LongitudeBand)
		result.RegionBurst = &burst
	}
	return result, nil
}

func add(target *scenario, shard int, bytes int64) {
	target.Rows++
	target.Shards[shard].LogicalRows++
	target.Shards[shard].InputBytes += bytes
	target.Shards[shard].Writes++
}

func finalize(target *scenario) {
	target.Skew.LogicalRows = summarize(target.Shards, func(s shardStats) float64 { return float64(s.LogicalRows) })
	target.Skew.InputBytes = summarize(target.Shards, func(s shardStats) float64 { return float64(s.InputBytes) })
	target.Skew.Writes = summarize(target.Shards, func(s shardStats) float64 { return float64(s.Writes) })
}

func summarize(shards [3]shardStats, value func(shardStats) float64) skew {
	var sum float64
	var maximum float64
	for _, shard := range shards {
		current := value(shard)
		sum += current
		maximum = math.Max(maximum, current)
	}
	mean := sum / shardCount
	ratio := 0.0
	if mean != 0 {
		ratio = maximum / mean
	}
	return skew{Max: maximum, Mean: mean, MaxToMean: ratio}
}

// fixedHashShard mirrors sha256-32be-v1 locally; this command intentionally
// does not import Aggregator business logic across service boundaries.
func fixedHashShard(source, reference string) int {
	var key [13 + 1 + 2048]byte
	length := copy(key[:], source)
	length++
	length += copy(key[length:], reference)
	digest := sha256.Sum256(key[:length])
	return int(binary.BigEndian.Uint32(digest[:4]) % shardCount)
}

// Longitude bands divide [-180,180] into three 120-degree ranges. Boundary
// values belong to the band on their east side, except +180 which is last.
func longitudeShard(longitude float64) int {
	if longitude < -60 {
		return 0
	}
	if longitude < 60 {
		return 1
	}
	return 2
}
