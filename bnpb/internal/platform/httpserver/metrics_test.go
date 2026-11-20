package httpserver

import "testing"

func TestNormalizeRoute(t *testing.T) {
	cases := map[string]string{
		"/health":                         "/health",
		"/v1/hazards":                     "/v1/hazards",
		"/v1/hazards/hz-7f3a91ab":         "/v1/hazards/{id}",
		"/v1/hazards/12345":               "/v1/hazards/{id}",
		"/auth/login":                     "/auth/login",
		"/internal/audit/events":          "/internal/audit/events",
		"/v1/hazards/not-an-id":           "/v1/hazards/not-an-id",
		"/v1/hazards/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee": "/v1/hazards/{id}",
	}
	for path, want := range cases {
		if got := normalizeRoute(path); got != want {
			t.Fatalf("normalizeRoute(%q)=%q, want %q", path, got, want)
		}
	}
}
