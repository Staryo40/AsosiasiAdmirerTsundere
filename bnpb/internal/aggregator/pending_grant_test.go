package aggregator

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestPendingGrantCompletionPreservesNativeHighWater(t *testing.T) {
	closed := authorityGrant{DomainUUID: authorityTestDomain, HostBootID: "boot-1", Epoch: 7, RenewalSeq: 1}
	live := closed
	live.DeadlineBootTimeNS = 100
	older := closed
	older.Epoch = 6
	newer := closed
	newer.Epoch = 8
	wrongDomain := closed
	wrongDomain.DomainUUID = "00000000-0000-4000-8000-000000000002"
	wrongSequence := closed
	wrongSequence.RenewalSeq = 0
	for _, test := range []struct {
		name       string
		status     int
		current    authorityGrant
		wantGrants int
		wantError  bool
	}{
		{name: "closed exact epoch is not reopened", status: http.StatusNotFound, current: closed},
		{name: "expired exact epoch is not reopened", status: http.StatusGone, current: live},
		{name: "live exact epoch is reused", status: http.StatusOK, current: live},
		{name: "never armed epoch is completed", status: http.StatusNotFound, wantGrants: 1},
		{name: "older closed epoch is advanced", status: http.StatusNotFound, current: older, wantGrants: 1},
		{name: "newer epoch conflicts", status: http.StatusNotFound, current: newer, wantError: true},
		{name: "wrong domain conflicts", status: http.StatusNotFound, current: wrongDomain, wantError: true},
		{name: "incomplete high water conflicts", status: http.StatusNotFound, current: wrongSequence, wantError: true},
		{name: "unavailable host is not expiry", status: http.StatusServiceUnavailable, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var grants atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.WriteHeader(test.status)
					_ = json.NewEncoder(w).Encode(test.current)
					return
				}
				grants.Add(1)
				if test.wantGrants == 0 {
					w.WriteHeader(http.StatusConflict)
					return
				}
				var identity hostGrantRequest
				if err := json.NewDecoder(r.Body).Decode(&identity); err != nil || identity.Epoch != 7 || identity.Owner != "crashed-owner" {
					t.Errorf("unexpected grant identity: %+v, %v", identity, err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(live)
			}))
			defer server.Close()
			authority := &PollAuthority{
				domains: []string{authorityTestDomain},
				host:    &hostFenceClient{baseURL: server.URL, token: "test-token", http: server.Client()},
			}
			record := authorityRecord{Epoch: 7, Owner: "crashed-owner", OwnerDomainUUID: authorityTestDomain, State: authorityClosing, RenewalSeq: 1}
			err := authority.completePendingGrants(t.Context(), record)
			if (err != nil) != test.wantError || int(grants.Load()) != test.wantGrants {
				t.Fatalf("completion error=%v, grants=%d; want error=%t, grants=%d", err, grants.Load(), test.wantError, test.wantGrants)
			}
			if test.current.Epoch > record.Epoch && !errors.Is(err, errAuthorityConflict) {
				t.Fatalf("newer high water did not remain a conflict: %v", err)
			}
		})
	}
}
