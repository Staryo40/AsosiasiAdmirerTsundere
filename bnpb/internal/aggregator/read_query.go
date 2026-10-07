package aggregator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"time"
)

var errInvalidQuery = errors.New("invalid query")

type hazardFilters struct {
	Source       *Source     `json:"source"`
	HazardType   *HazardType `json:"hazard_type"`
	Severity     *Severity   `json:"severity"`
	OccurredFrom *time.Time  `json:"occurred_from"`
	OccurredTo   *time.Time  `json:"occurred_to"`
}

type hazardCursor struct {
	Version    int           `json:"v"`
	OccurredAt time.Time     `json:"occurred_at"`
	HazardID   string        `json:"hazard_id"`
	Filters    hazardFilters `json:"filters"`
}

type hazardQuery struct {
	filters hazardFilters
	limit   int
	cursor  *hazardCursor
}

func parseHazardQuery(rawQuery string, cfg Config) (hazardQuery, error) {
	query, err := url.ParseQuery(rawQuery)
	if err != nil || !validQueryKeys(query) {
		return hazardQuery{}, errInvalidQuery
	}
	filters, err := parseHazardFilters(query)
	if err != nil {
		return hazardQuery{}, err
	}
	page := hazardQuery{filters: filters, limit: cfg.ListDefaultLimit}
	if query.Has("limit") {
		page.limit, err = strconv.Atoi(query.Get("limit"))
		if err != nil || page.limit < 1 || page.limit > cfg.ListMaxLimit {
			return hazardQuery{}, errInvalidQuery
		}
	}
	if query.Has("cursor") {
		page.cursor, err = parseHazardCursor(query.Get("cursor"), filters)
	}
	return page, err
}

func validQueryKeys(query url.Values) bool {
	for key, entries := range query {
		if !slices.Contains([]string{"source", "hazard_type", "severity", "occurred_from", "occurred_to", "limit", "cursor"}, key) || len(entries) != 1 || entries[0] == "" {
			return false
		}
	}
	return true
}

func parseHazardFilters(query url.Values) (hazardFilters, error) {
	filters := hazardFilters{}
	var err error
	if filters.Source, err = enumFilter(query.Get("source"), []Source{BMKG, PVMBG}); err != nil {
		return filters, err
	}
	if filters.HazardType, err = enumFilter(query.Get("hazard_type"), []HazardType{Seismic, Volcanic}); err != nil {
		return filters, err
	}
	if filters.Severity, err = enumFilter(query.Get("severity"), []Severity{Normal, Waspada, Siaga, Awas}); err != nil {
		return filters, err
	}
	if filters.OccurredFrom, err = timeFilter(query.Get("occurred_from")); err != nil {
		return filters, err
	}
	if filters.OccurredTo, err = timeFilter(query.Get("occurred_to")); err != nil {
		return filters, err
	}
	if !filters.consistent() {
		return filters, errInvalidQuery
	}
	return filters, nil
}

func enumFilter[T ~string](text string, allowed []T) (*T, error) {
	if text == "" {
		return nil, nil
	}
	selection := T(text)
	if !slices.Contains(allowed, selection) {
		return nil, errInvalidQuery
	}
	return &selection, nil
}

func timeFilter(text string) (*time.Time, error) {
	if text == "" {
		return nil, nil
	}
	stamp, err := time.Parse(time.RFC3339Nano, text)
	if err != nil || !sourceTimestamp.MatchString(text) || !validInstant(stamp) {
		return nil, errInvalidQuery
	}
	stamp = stamp.UTC()
	return &stamp, nil
}

func (filters hazardFilters) consistent() bool {
	if filters.Source != nil && filters.HazardType != nil &&
		((*filters.Source == BMKG) != (*filters.HazardType == Seismic)) {
		return false
	}
	return filters.OccurredFrom == nil || filters.OccurredTo == nil || !filters.OccurredFrom.After(*filters.OccurredTo)
}

func (filters hazardFilters) selectedSources() []Source {
	if filters.Source != nil {
		return []Source{*filters.Source}
	}
	if filters.HazardType != nil {
		if *filters.HazardType == Seismic {
			return []Source{BMKG}
		}
		return []Source{PVMBG}
	}
	return []Source{BMKG, PVMBG}
}

func parseHazardCursor(token string, filters hazardFilters) (*hazardCursor, error) {
	if len(token) > 2048 {
		return nil, errInvalidQuery
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != token || !validCursorKeys(raw) {
		return nil, errInvalidQuery
	}
	var cursor hazardCursor
	if err := json.Unmarshal(raw, &cursor); err != nil || cursor.Version != 1 ||
		!validInstant(cursor.OccurredAt) || !validText(cursor.HazardID) || len(cursor.HazardID) > 2048 {
		return nil, errInvalidQuery
	}
	expected, expectedErr := json.Marshal(filters)
	actual, actualErr := json.Marshal(cursor.Filters)
	if expectedErr != nil || actualErr != nil || !bytes.Equal(expected, actual) {
		return nil, errInvalidQuery
	}
	cursor.OccurredAt = cursor.OccurredAt.UTC()
	return &cursor, nil
}

func validCursorKeys(raw []byte) bool {
	fields, err := parseObject(raw)
	if err != nil || !exactKeys(fields, []string{"v", "occurred_at", "hazard_id", "filters"}) {
		return false
	}
	stamp, err := timeField(fields, "occurred_at")
	if err != nil || !stamp.Equal(databaseTime(stamp)) {
		return false
	}
	filters, err := parseObject(fields["filters"])
	return err == nil && exactKeys(filters, []string{"source", "hazard_type", "severity", "occurred_from", "occurred_to"})
}

func exactKeys(fields rawObject, keys []string) bool {
	if len(fields) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, exists := fields[key]; !exists {
			return false
		}
	}
	return true
}

func nextHazardCursor(hazard HazardEvent, filters hazardFilters) (string, error) {
	raw, err := json.Marshal(hazardCursor{1, hazard.OccurredAt, hazard.HazardID, filters})
	return base64.RawURLEncoding.EncodeToString(raw), err
}
