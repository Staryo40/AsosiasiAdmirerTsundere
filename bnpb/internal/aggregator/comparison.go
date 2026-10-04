package aggregator

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"
)

// SameMaterialContent ignores BNPB-generated identity/receipt metadata.
// Arrays retain their order, matching PostgreSQL JSONB equality semantics.
func SameMaterialContent(left, right HazardEvent) (bool, error) {
	if left.Source != right.Source ||
		left.SourceRefID != right.SourceRefID ||
		left.HazardType != right.HazardType ||
		left.Severity != right.Severity ||
		left.AreaName != right.AreaName ||
		left.Latitude != right.Latitude ||
		left.Longitude != right.Longitude ||
		!left.OccurredAt.Equal(right.OccurredAt) {
		return false, nil
	}
	leftJSON, err := semanticAttributes(left.Attributes)
	if err != nil {
		return false, err
	}
	rightJSON, err := semanticAttributes(right.Attributes)
	if err != nil {
		return false, err
	}
	return equalJSON(leftJSON, rightJSON), nil
}

func semanticAttributes(attributes map[string]json.RawMessage) (any, error) {
	raw, err := json.Marshal(attributes)
	if err != nil {
		return nil, invalidField("attributes", InvalidJSON)
	}
	if err := validateJSON(raw); err != nil {
		return nil, invalidField("attributes", InvalidJSON)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, invalidField("attributes", InvalidJSON)
	}
	return decoded, nil
}

func equalJSON(left, right any) bool {
	switch left := left.(type) {
	case map[string]any:
		right, ok := right.(map[string]any)
		return ok && equalJSONObjects(left, right)
	case []any:
		right, ok := right.([]any)
		return ok && equalJSONArrays(left, right)
	case json.Number:
		right, ok := right.(json.Number)
		return ok && decimalIdentity(left) == decimalIdentity(right)
	default:
		return left == right
	}
}

func equalJSONObjects(left, right map[string]any) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftJSON := range left {
		rightJSON, present := right[key]
		if !present || !equalJSON(leftJSON, rightJSON) {
			return false
		}
	}
	return true
}

func equalJSONArrays(left, right []any) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !equalJSON(left[index], right[index]) {
			return false
		}
	}
	return true
}

// Normalize coefficient/exponent without expanding the exponent or rounding integers.
func decimalIdentity(number json.Number) string {
	mantissa, exponent := string(number), new(big.Int)
	if index := strings.IndexAny(mantissa, "eE"); index >= 0 {
		exponent.SetString(mantissa[index+1:], 10)
		mantissa = mantissa[:index]
	}
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		exponent.Sub(exponent, big.NewInt(int64(len(mantissa)-index-1)))
		mantissa = mantissa[:index] + mantissa[index+1:]
	}
	negative := strings.HasPrefix(mantissa, "-")
	coefficient := strings.TrimLeft(strings.TrimPrefix(mantissa, "-"), "0")
	if coefficient == "" {
		return "0"
	}
	trimmed := strings.TrimRight(coefficient, "0")
	exponent.Add(exponent, big.NewInt(int64(len(coefficient)-len(trimmed))))
	if negative {
		trimmed = "-" + trimmed
	}
	return trimmed + "e" + exponent.String()
}
