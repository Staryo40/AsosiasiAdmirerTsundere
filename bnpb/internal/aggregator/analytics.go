package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

const analyticsMaxBuckets = 1000

var (
	errInvalidAnalyticsQuery  = errors.New("invalid analytics query")
	errAnalyticsGroupLimit    = errors.New("analytics group limit exceeded")
	errAnalyticsResponseLimit = errors.New("analytics response byte limit exceeded")
)

type analyticsQuery struct {
	From   time.Time
	To     time.Time
	Bucket string
}

type analyticsSeriesItem struct {
	Start    time.Time `json:"start"`
	Category string    `json:"category"`
	Count    int64     `json:"count"`
}

type analyticsAreaItem struct {
	AreaName string `json:"area_name"`
	Count    int64  `json:"count"`
}

type analyticsResponse struct {
	analyticsSummary
	Areas []analyticsAreaItem `json:"areas"`
	analyticsAvailability
}

type analyticsSummary struct {
	From     time.Time             `json:"from"`
	To       time.Time             `json:"to"`
	Bucket   string                `json:"bucket"`
	AsOf     time.Time             `json:"as_of"`
	Total    int64                 `json:"total"`
	Series   []analyticsSeriesItem `json:"series"`
	Severity map[string]int64      `json:"severity"`
}

type analyticsAvailability struct {
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
}

type analyticsGroup struct {
	Bucket   time.Time
	Category string
	Severity string
	AreaName string
	Count    int64
	Kind     int
}
type analyticsCounts struct {
	Total            int64
	Series           map[analyticsSeriesKey]int64
	Severity         map[string]int64
	Areas            map[string]int64
	UnavailableUnits []string
}

func parseAnalyticsQuery(raw string) (analyticsQuery, error) {
	values, err := url.ParseQuery(raw)
	if err != nil || validateAnalyticsParams(values) != nil {
		return analyticsQuery{}, errInvalidAnalyticsQuery
	}
	from, err := parseQueryTimestamp(values.Get("from"))
	if err != nil {
		return analyticsQuery{}, errInvalidAnalyticsQuery
	}
	to, err := parseQueryTimestamp(values.Get("to"))
	if err != nil || !to.After(from) || to.Sub(from) > 366*24*time.Hour {
		return analyticsQuery{}, errInvalidAnalyticsQuery
	}
	bucket, err := parseAnalyticsBucket(values)
	if err != nil || analyticsBucketCount(from, to, bucket) > analyticsMaxBuckets {
		return analyticsQuery{}, errInvalidAnalyticsQuery
	}
	return analyticsQuery{From: from, To: to, Bucket: bucket}, nil
}

func validateAnalyticsParams(values url.Values) error {
	for key, entries := range values {
		if (key != "from" && key != "to" && key != "bucket") || len(entries) != 1 {
			return errInvalidAnalyticsQuery
		}
	}
	return nil
}

func parseAnalyticsBucket(values url.Values) (string, error) {
	bucket := "day"
	if values.Has("bucket") {
		bucket = values.Get("bucket")
	}
	if bucket != "hour" && bucket != "day" && bucket != "week" {
		return "", errInvalidAnalyticsQuery
	}
	return bucket, nil
}

func analyticsBucketCount(from, to time.Time, bucket string) int64 {
	start := analyticsBucketStart(from, bucket)
	last := analyticsBucketStart(to.Add(-time.Nanosecond), bucket)
	unit := time.Hour
	if bucket == "day" {
		unit = 24 * time.Hour
	} else if bucket == "week" {
		unit = 7 * 24 * time.Hour
	}
	return int64(last.Sub(start)/unit) + 1
}

func analyticsBucketStart(stamp time.Time, bucket string) time.Time {
	stamp = stamp.UTC()
	switch bucket {
	case "hour":
		return time.Date(stamp.Year(), stamp.Month(), stamp.Day(), stamp.Hour(), 0, 0, 0, time.UTC)
	case "week":
		day := time.Date(stamp.Year(), stamp.Month(), stamp.Day(), 0, 0, 0, 0, time.UTC)
		offset := (int(day.Weekday()) + 6) % 7
		return day.AddDate(0, 0, -offset)
	default:
		return time.Date(stamp.Year(), stamp.Month(), stamp.Day(), 0, 0, 0, 0, time.UTC)
	}
}

func (api snapshotAPI) analytics(w http.ResponseWriter, request *http.Request) {
	query, err := parseAnalyticsQuery(request.URL.RawQuery)
	if err != nil {
		writeReadError(w, request, http.StatusBadRequest, "invalid_query")
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), api.analyticsBudget)
	defer cancel()
	asOf := time.Now().UTC()
	results := fanoutLogicalReads(ctx, api.store.logicalReadStores(), func(ctx context.Context, unit logicalReadStore) ([]analyticsGroup, error) {
		return unit.Store.readAnalytics(ctx, query, api.cfg.MaxResponseBytes)
	})
	counts, err := aggregateAnalyticsGroups(results)
	if err != nil {
		writeAnalyticsAggregateError(w, request, err)
		return
	}
	response := analyticsResponseFrom(query, counts, asOf)
	writeAnalyticsResponse(w, request, api.cfg.MaxResponseBytes, response)
}

func analyticsReadBudget(cfg Config, writeTimeout time.Duration) time.Duration {
	budget := cfg.Audit.ChecksumTimeout
	if budget <= 0 {
		budget = cfg.DBReadTimeout
	}
	return readWorkBudget(budget, writeTimeout)
}

func aggregateAnalyticsGroups(results []logicalReadResult[[]analyticsGroup]) (analyticsCounts, error) {
	counts := analyticsCounts{
		Series:           make(map[analyticsSeriesKey]int64),
		Severity:         map[string]int64{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0},
		Areas:            make(map[string]int64),
		UnavailableUnits: unavailableReadUnits(results),
	}
	for _, result := range results {
		if err := aggregateAnalyticsResult(&counts, result); err != nil {
			return analyticsCounts{}, err
		}
	}
	return counts, nil
}

func aggregateAnalyticsResult(counts *analyticsCounts, result logicalReadResult[[]analyticsGroup]) error {
	if result.Err != nil {
		if errors.Is(result.Err, errAnalyticsGroupLimit) || errors.Is(result.Err, errAnalyticsResponseLimit) {
			return result.Err
		}
		return nil
	}
	for _, group := range result.Value {
		switch group.Kind {
		case analyticsSeriesGroup:
			counts.Series[analyticsSeriesKey{Start: group.Bucket, Category: group.Category}] += group.Count
			counts.Total += group.Count
		case analyticsSeverityGroup:
			counts.Severity[group.Severity] += group.Count
		case analyticsAreaGroup:
			counts.Areas[group.AreaName] += group.Count
		}
	}
	return nil
}

func analyticsResponseFrom(query analyticsQuery, counts analyticsCounts, asOf time.Time) analyticsResponse {
	return analyticsResponse{
		analyticsSummary: analyticsSummary{
			From: query.From, To: query.To, Bucket: query.Bucket, AsOf: asOf, Total: counts.Total,
			Series: analyticsSeriesItems(counts.Series), Severity: counts.Severity,
		},
		Areas: analyticsAreaItems(counts.Areas),
		analyticsAvailability: analyticsAvailability{
			Partial: len(counts.UnavailableUnits) != 0, UnavailableUnits: counts.UnavailableUnits,
		},
	}
}

func analyticsSeriesItems(counts map[analyticsSeriesKey]int64) []analyticsSeriesItem {
	items := make([]analyticsSeriesItem, 0, len(counts))
	for key, count := range counts {
		items = append(items, analyticsSeriesItem{Start: key.Start, Category: key.Category, Count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Start.Equal(items[j].Start) {
			return items[i].Category < items[j].Category
		}
		return items[i].Start.Before(items[j].Start)
	})
	return items
}

func analyticsAreaItems(counts map[string]int64) []analyticsAreaItem {
	items := make([]analyticsAreaItem, 0, len(counts))
	for name, count := range counts {
		items = append(items, analyticsAreaItem{AreaName: name, Count: count})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].AreaName < items[j].AreaName })
	return items
}

func writeAnalyticsResponse(w http.ResponseWriter, request *http.Request, maxBytes int64, response analyticsResponse) {
	body, err := marshalAnalyticsResponse(response, maxBytes)
	if err != nil {
		writeAnalyticsAggregateError(w, request, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func marshalAnalyticsResponse(response analyticsResponse, maxBytes int64) ([]byte, error) {
	summary, err := json.Marshal(response.analyticsSummary)
	if err != nil {
		return nil, err
	}
	availability, err := json.Marshal(response.analyticsAvailability)
	if err != nil {
		return nil, err
	}
	const areasPrefix = `,"areas":[`
	summary = summary[:len(summary)-1]
	availability[0] = ','
	remaining := maxBytes - int64(len(summary)+len(areasPrefix)+1+len(availability))
	if remaining < 0 {
		return nil, errAnalyticsResponseLimit
	}
	body, err := appendAnalyticsAreas(append(summary, areasPrefix...), response.Areas, remaining)
	if err != nil {
		return nil, err
	}
	return append(append(body, ']'), availability...), nil
}

func appendAnalyticsAreas(body []byte, areas []analyticsAreaItem, remaining int64) ([]byte, error) {
	var scratch bytes.Buffer
	encoder := json.NewEncoder(&scratch)
	for index, area := range areas {
		if err := encoder.Encode(area); err != nil {
			return nil, err
		}
		encoded := scratch.Bytes()[:scratch.Len()-1] // omit the encoder's final newline
		remaining -= int64(len(encoded))
		if index != 0 {
			remaining--
		}
		if remaining < 0 {
			return nil, errAnalyticsResponseLimit
		}
		if index != 0 {
			body = append(body, ',')
		}
		body = append(body, encoded...)
		scratch.Reset()
	}
	return body, nil
}

func writeAnalyticsAggregateError(w http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, errAnalyticsGroupLimit):
		writeAnalyticsError(w, request, http.StatusServiceUnavailable, "analytics_result_limit", "analytics grouped result exceeds the 10000-row per-store limit")
	case errors.Is(err, errAnalyticsResponseLimit):
		writeAnalyticsError(w, request, http.StatusServiceUnavailable, "analytics_response_too_large", "analytics response exceeds the configured size limit")
	default:
		writeAnalyticsError(w, request, http.StatusServiceUnavailable, "analytics_unavailable", "analytics results are unavailable")
	}
}

type analyticsSeriesKey struct {
	Start    time.Time
	Category string
}

const (
	analyticsSeriesGroup = iota
	analyticsSeverityGroup
	analyticsAreaGroup
)

func analyticsBucketExpression(bucket string) string {
	switch bucket {
	case "hour":
		return `date_bin('1 hour', occurred_at, '2000-01-01 00:00:00+00'::timestamptz)`
	case "week":
		return `date_bin('1 week', occurred_at, '2000-01-03 00:00:00+00'::timestamptz)`
	default:
		return `date_bin('1 day', occurred_at, '2000-01-01 00:00:00+00'::timestamptz)`
	}
}
func writeAnalyticsError(w http.ResponseWriter, request *http.Request, status int, code, message string) {
	writeReadJSON(w, status, readErrorEnvelope{Error: applicationError{
		Code: code, Message: message, CorrelationID: correlation.ID(request.Context()),
	}})
}
