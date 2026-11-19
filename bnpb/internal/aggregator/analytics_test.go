package aggregator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParseAnalyticsQueryAndBucketBounds(t *testing.T) {
	query, err := parseAnalyticsQuery("from=2026-11-16T23%3A30%3A00-05%3A00&to=2026-11-17T01%3A00%3A00-05%3A00")
	if err != nil || query.Bucket != "day" || query.From.Format(time.RFC3339) != "2026-11-17T04:30:00Z" {
		t.Fatalf("parse default day query: %+v, %v", query, err)
	}
	if _, err := parseAnalyticsQuery("from=2026-11-16T00%3A00%3A00Z&to=2026-12-27T16%3A00%3A00Z&bucket=hour"); err != nil {
		t.Fatalf("exactly 1000 hourly buckets: %v", err)
	}
	tests := []struct {
		name string
		raw  string
	}{
		{name: "missing to", raw: "from=2026-11-16T00%3A00%3A00Z"},
		{name: "missing from", raw: "to=2026-11-17T00%3A00%3A00Z"},
		{name: "missing zone", raw: "from=2026-11-16T00%3A00%3A00&to=2026-11-17T00%3A00%3A00Z"},
		{name: "malformed timestamp", raw: "from=bad&to=2026-11-17T00%3A00%3A00Z"},
		{name: "empty from", raw: "from=&to=2026-11-17T00%3A00%3A00Z"},
		{name: "empty to", raw: "from=2026-11-16T00%3A00%3A00Z&to="},
		{name: "unknown bucket", raw: "from=2026-11-16T00%3A00%3A00Z&to=2026-11-17T00%3A00%3A00Z&bucket=month"},
		{name: "repeated from", raw: "from=2026-11-16T00%3A00%3A00Z&from=2026-11-16T00%3A00%3A00Z&to=2026-11-17T00%3A00%3A00Z"},
		{name: "unknown parameter", raw: "from=2026-11-16T00%3A00%3A00Z&to=2026-11-17T00%3A00%3A00Z&unexpected=x"},
		{name: "repeated bucket", raw: "from=2026-11-16T00%3A00%3A00Z&to=2026-11-17T00%3A00%3A00Z&bucket=hour&bucket=day"},
		{name: "range exceeds year", raw: "from=2026-11-16T00%3A00%3A00Z&to=2027-11-18T00%3A00%3A00Z"},
		{name: "reversed range", raw: "from=2026-11-16T00%3A00%3A00Z&to=2026-11-15T00%3A00%3A00Z"},
		{name: "too many hourly buckets", raw: "from=2026-11-16T00%3A00%3A00Z&to=2026-12-29T00%3A00%3A00Z&bucket=hour"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseAnalyticsQuery(test.raw); err == nil {
				t.Fatalf("accepted invalid analytics query %q", test.raw)
			}
		})
	}
}

func TestAnalyticsRequiresStrictRFC3339(t *testing.T) {
	tests := []struct {
		name, from, to string
	}{
		{name: "tenth fractional digit", from: "2026-11-16T00:00:00.1234567890Z"},
		{name: "decimal comma", from: "2026-11-16T00:00:00,1Z"},
		{name: "single-digit hour", from: "2026-11-16T0:00:00Z"},
		{name: "zone hour out of range", from: "2026-11-16T00:00:00+24:00"},
		{name: "zone minute out of range", from: "2026-11-16T00:00:00+00:60"},
		{name: "UTC year below range", from: "0001-01-01T00:00:00+00:01", to: "0001-01-02T00:00:00Z"},
		{name: "UTC year above range", from: "9999-12-31T00:00:00Z", to: "9999-12-31T23:59:59-00:01"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			to := test.to
			if to == "" {
				to = "2026-11-17T00:00:00Z"
			}
			values := url.Values{"from": {test.from}, "to": {to}}
			if _, err := parseAnalyticsQuery(values.Encode()); err == nil {
				t.Fatalf("accepted invalid timestamps %s", values.Encode())
			}
		})
	}
}

func TestAnalyticsWeekAtEarliestUTCYear(t *testing.T) {
	values := url.Values{
		"from":   {"0001-01-01T02:00:00+02:00"},
		"to":     {"0001-01-02T00:00:00Z"},
		"bucket": {"week"},
	}
	query, err := parseAnalyticsQuery(values.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := query.From.Format(time.RFC3339Nano), "0001-01-01T00:00:00Z"; got != want {
		t.Fatalf("UTC from = %s, want %s", got, want)
	}
	if got, want := analyticsBucketStart(query.From, query.Bucket).Format(time.RFC3339), "0001-01-01T00:00:00Z"; got != want {
		t.Fatalf("first Monday bucket = %s, want %s", got, want)
	}
}

func TestAnalyticsBucketStartsUseUTCAndMondayWeeks(t *testing.T) {
	stamp := time.Date(2026, 11, 22, 23, 59, 0, 0, time.FixedZone("west", -8*60*60))
	if got, want := analyticsBucketStart(stamp, "hour"), time.Date(2026, 11, 23, 7, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("hour bucket = %s, want %s", got, want)
	}
	if got, want := analyticsBucketStart(stamp, "day"), time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("day bucket = %s, want %s", got, want)
	}
	if got, want := analyticsBucketStart(stamp, "week"), time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("week bucket = %s, want Monday %s", got, want)
	}
	from := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	to := from.Add(1000 * time.Hour)
	if got := analyticsBucketCount(from, to, "hour"); got != 1000 {
		t.Fatalf("hour buckets = %d, want 1000", got)
	}
	if got := analyticsBucketCount(from, to.Add(time.Nanosecond), "hour"); got != 1001 {
		t.Fatalf("hour buckets = %d, want 1001", got)
	}
}
func TestAnalyticsDatabaseBoundsCeilToMicroseconds(t *testing.T) {
	bound := time.Date(2026, 11, 17, 0, 0, 0, 123456789, time.UTC)
	if got, want := analyticsCeilMicrosecond(bound), time.Date(2026, 11, 17, 0, 0, 0, 123457000, time.UTC); !got.Equal(want) {
		t.Fatalf("ceil bound = %s, want %s", got, want)
	}
	aligned := time.Date(2026, 11, 17, 0, 0, 0, 123456000, time.UTC)
	if got := analyticsCeilMicrosecond(aligned); !got.Equal(aligned) {
		t.Fatalf("aligned bound changed: got %s, want %s", got, aligned)
	}
}

func TestAnalyticsResponseByteCap(t *testing.T) {
	start := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	query := analyticsQuery{From: start, To: start.Add(time.Hour), Bucket: "hour"}
	for _, test := range []struct {
		name  string
		areas map[string]int64
	}{
		{name: "empty arrays"},
		{name: "escaped names and separators", areas: map[string]int64{
			"Z<&> é": 12, strings.Repeat(`"\`+"\n", 300): 1, "A": 2,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := analyticsResponseFrom(query, analyticsCounts{Areas: test.areas, UnavailableUnits: []string{}}, start)
			want, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "/internal/analytics", nil)
			for _, boundary := range []struct {
				name   string
				cap    int64
				status int
			}{
				{name: "exact byte limit", cap: int64(len(want)), status: http.StatusOK},
				{name: "one byte too small", cap: int64(len(want) - 1), status: http.StatusServiceUnavailable},
			} {
				t.Run(boundary.name, func(t *testing.T) {
					recorder := httptest.NewRecorder()
					writeAnalyticsResponse(recorder, request, boundary.cap, response)
					if recorder.Code != boundary.status {
						t.Fatalf("cap=%d status=%d body=%s", boundary.cap, recorder.Code, recorder.Body)
					}
					if boundary.status == http.StatusOK {
						if !bytes.Equal(recorder.Body.Bytes(), want) || !strings.Contains(recorder.Body.String(), `"series":[]`) ||
							(test.areas == nil && !strings.Contains(recorder.Body.String(), `"areas":[]`)) {
							t.Fatalf("invalid compact analytics envelope: %s", recorder.Body)
						}
						return
					}
					var failure readErrorEnvelope
					if err := json.Unmarshal(recorder.Body.Bytes(), &failure); err != nil || failure.Error.Code != "analytics_response_too_large" {
						t.Fatalf("invalid size-limit error: %s", recorder.Body)
					}
				})
			}
		})
	}
}

func TestAnalyticsReadBudgetReservesWriteHeadroom(t *testing.T) {
	for _, test := range []struct {
		name                                string
		checksumTimeout, writeTimeout, want time.Duration
	}{
		{name: "configured budget fits", checksumTimeout: 5 * time.Second, writeTimeout: 10 * time.Second, want: 5 * time.Second},
		{name: "reserves rollback and emission", checksumTimeout: time.Minute, writeTimeout: 10 * time.Second, want: 8 * time.Second},
		{name: "short write deadline", checksumTimeout: time.Minute, writeTimeout: 2 * time.Second, want: 500 * time.Millisecond},
		{name: "falls back to DB read timeout", writeTimeout: 10 * time.Second, want: 200 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{DBReadTimeout: 200 * time.Millisecond, Audit: AuditConfig{ChecksumTimeout: test.checksumTimeout}}
			if got := analyticsReadBudget(cfg, test.writeTimeout); got != test.want {
				t.Fatalf("analytics budget = %s, want %s", got, test.want)
			}
		})
	}
}
