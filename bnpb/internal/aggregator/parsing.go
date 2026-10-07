package aggregator

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

type rawObject map[string]json.RawMessage

func mappingFields(raw json.RawMessage, receipt Receipt) (rawObject, error) {
	if err := receipt.validate(); err != nil {
		return nil, err
	}
	return parseObject(raw)
}

func parseObject(raw json.RawMessage) (rawObject, error) {
	if err := validateJSON(raw); err != nil {
		return nil, err
	}
	return required[rawObject](rawObject{"record": raw}, "record")
}

func validateJSON(raw json.RawMessage) error {
	if !utf8.Valid(raw) || !json.Valid(raw) || !validUnicodeEscapes(raw) {
		return invalidField("record", InvalidJSON)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return uniqueJSONKeys(decoder)
}

// Syntax is already validated; reject surrogate repair that would alter source identities or keys.
func validUnicodeEscapes(raw []byte) bool {
	for index := 0; index < len(raw); index++ {
		if raw[index] != '\\' {
			continue
		}
		index++
		if raw[index] != 'u' {
			continue
		}
		first := escapedRune(raw[index+1 : index+5])
		index += 4
		if utf16.IsSurrogate(first) {
			if !validSurrogatePair(first, raw[index+1:]) {
				return false
			}
			index += 6
		}
	}
	return true
}

func escapedRune(hex []byte) rune {
	codePoint, _ := strconv.ParseUint(string(hex), 16, 16)
	return rune(codePoint)
}

func validSurrogatePair(first rune, following []byte) bool {
	return len(following) >= 6 && bytes.HasPrefix(following, []byte(`\u`)) &&
		utf16.DecodeRune(first, escapedRune(following[2:6])) != unicode.ReplacementChar
}

func uniqueJSONKeys(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return invalidField("record", InvalidJSON)
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return nil
	}
	if delimiter == '{' {
		return uniqueObjectKeys(decoder)
	}
	for decoder.More() {
		if err := uniqueJSONKeys(decoder); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

func uniqueObjectKeys(decoder *json.Decoder) error {
	seen := make(map[string]bool)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return invalidField("record", InvalidJSON)
		}
		key := token.(string)
		if seen[key] {
			return invalidField("record", DuplicateKey)
		}
		seen[key] = true
		if err := uniqueJSONKeys(decoder); err != nil {
			return err
		}
	}
	_, err := decoder.Token()
	return err
}

func required[T any](fields rawObject, key string) (T, error) {
	var decoded T
	raw, present := fields[key]
	if !present {
		return decoded, invalidField(key, MissingField)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return decoded, invalidField(key, NullField)
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return decoded, invalidField(key, WrongType)
	}
	return decoded, nil
}

func validText(text string) bool {
	return strings.TrimSpace(text) != "" && !strings.ContainsFunc(text, unicode.IsControl)
}

func textField(fields rawObject, key string) (string, error) {
	text, err := required[string](fields, key)
	if err == nil && !validText(text) {
		err = invalidField(key, InvalidValue)
	}
	return text, err
}

func numberField(fields rawObject, key string, minimum, maximum float64) (float64, error) {
	number, err := required[json.Number](fields, key)
	if err != nil {
		return 0, err
	}
	if bytes.TrimSpace(fields[key])[0] == '"' {
		return 0, invalidField(key, WrongType)
	}
	approximation, err := number.Float64()
	if err != nil || math.IsInf(approximation, 0) {
		return 0, invalidField(key, InvalidValue)
	}
	if approximation == 0 {
		if decimalIdentity(number) != "0" {
			return 0, invalidField(key, InvalidValue)
		}
		return 0, nil
	}
	exact, valid := new(big.Rat).SetString(string(bytes.TrimSpace(fields[key])))
	if !valid || exact.Cmp(new(big.Rat).SetFloat64(minimum)) < 0 || exact.Cmp(new(big.Rat).SetFloat64(maximum)) > 0 {
		return 0, invalidField(key, InvalidValue)
	}
	return approximation, nil
}

// Reject forms time.Parse tolerates but RFC 3339 does not, and precision it would truncate.
var sourceTimestamp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-](0\d|1\d|2[0-3]):[0-5]\d)$`)

func validInstant(instant time.Time) bool {
	return !instant.IsZero() && instant.UTC().Year() >= 1 && instant.UTC().Year() <= 9999
}

func timeField(fields rawObject, key string) (time.Time, error) {
	text, err := required[string](fields, key)
	if err != nil {
		return time.Time{}, err
	}
	instant, err := time.Parse(time.RFC3339Nano, text)
	if err != nil || !sourceTimestamp.MatchString(text) || !validInstant(instant) {
		return time.Time{}, invalidField(key, InvalidValue)
	}
	return instant.UTC(), nil
}

func removeMappedFields(fields rawObject, mapped ...string) {
	for _, key := range mapped {
		delete(fields, key)
	}
}

func namespaceSourceExtras(fields rawObject, reserved ...string) error {
	extras := make(rawObject)
	for _, key := range reserved {
		if raw, present := fields[key]; present {
			extras[key] = raw
			delete(fields, key)
		}
	}
	if len(extras) == 0 {
		return nil
	}
	raw, err := json.Marshal(extras)
	if err == nil {
		fields["source_extras"] = raw
	}
	return err
}

func severityField(fields rawObject, key string, levels map[string]Severity) (Severity, error) {
	level, err := required[string](fields, key)
	if err != nil {
		return "", err
	}
	severity, valid := levels[level]
	if !valid {
		return "", invalidField(key, InvalidValue)
	}
	return severity, nil
}
