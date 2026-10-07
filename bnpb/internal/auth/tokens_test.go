package auth

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testStart = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func testService(t *testing.T) (*Service, *time.Time) {
	t.Helper()
	config, err := loadConfig(lookupFrom(validEnvironment()))
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(config, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	now := testStart
	service.now = func() time.Time { return now }
	return service, &now
}

func TestAllThreeIdentitiesReceiveServerAssignedRoles(t *testing.T) {
	service, _ := testService(t)
	tests := []struct {
		id     string
		secret string
		role   Role
	}{
		{"media", strings.Repeat("m", 32), RoleMedia},
		{"field-team", strings.Repeat("f", 32), RoleField},
		{"internal-ops", strings.Repeat("o", 32), RoleOps},
	}
	for _, test := range tests {
		t.Run(string(test.role), func(t *testing.T) {
			response, err := service.Login(test.id, test.secret)
			if err != nil {
				t.Fatal(err)
			}
			if response.TokenType != "Bearer" || response.ExpiresIn != 60 || response.RefreshExpiresIn != 8*60*60 {
				t.Fatalf("unexpected response metadata: %+v", response)
			}
			if strings.Contains(response.AccessToken, ".") {
				t.Fatal("opaque access token unexpectedly looks like a JWT")
			}
			result := service.Introspect(response.AccessToken)
			if !result.Active || result.ClientID != test.id || result.Role != test.role || result.ExpiresAt == nil {
				t.Fatalf("unexpected introspection: %+v", result)
			}
		})
	}
}

func TestLoginUsesOneGenericCredentialFailure(t *testing.T) {
	service, _ := testService(t)
	for _, credentials := range [][2]string{
		{"unknown", strings.Repeat("x", 32)},
		{"media", strings.Repeat("x", 32)},
		{"unknown", strings.Repeat("m", 32)},
	} {
		if _, err := service.Login(credentials[0], credentials[1]); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("unexpected credential error: %v", err)
		}
	}
}

func TestAccessTokenExpiresNaturally(t *testing.T) {
	service, now := testService(t)
	response, err := service.Login("field-team", strings.Repeat("f", 32))
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(59 * time.Second)
	if !service.Introspect(response.AccessToken).Active {
		t.Fatal("token expired before its 60-second TTL")
	}
	*now = now.Add(time.Second)
	if service.Introspect(response.AccessToken).Active {
		t.Fatal("token remained active at its expiration instant")
	}
}

func TestRefreshRotatesBothTokensAndRevokesOldAccess(t *testing.T) {
	service, now := testService(t)
	first, err := service.Login("field-team", strings.Repeat("f", 32))
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(30 * time.Second)
	second, err := service.Refresh(first.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if first.AccessToken == second.AccessToken || first.RefreshToken == second.RefreshToken {
		t.Fatal("refresh did not rotate both tokens")
	}
	if service.Introspect(first.AccessToken).Active {
		t.Fatal("old access token remained active after refresh")
	}
	if !service.Introspect(second.AccessToken).Active {
		t.Fatal("new access token is not active")
	}
	if _, err := service.Refresh(first.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("old refresh token was reusable: %v", err)
	}
	if second.RefreshExpiresIn != 8*60*60-30 {
		t.Fatalf("refresh extended or changed the absolute deadline: %d", second.RefreshExpiresIn)
	}
}

func TestAbsoluteSessionDeadlineClampsLastAccessToken(t *testing.T) {
	service, now := testService(t)
	first, err := service.Login("media", strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(8*time.Hour - 30*time.Second)
	last, err := service.Refresh(first.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if last.ExpiresIn != 30 || last.RefreshExpiresIn != 30 {
		t.Fatalf("tokens were not clamped to session deadline: %+v", last)
	}
	*now = now.Add(30 * time.Second)
	if service.Introspect(last.AccessToken).Active {
		t.Fatal("last access token survived the absolute deadline")
	}
	if _, err := service.Refresh(last.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("refresh survived the absolute deadline: %v", err)
	}
}

func TestOnlyOneConcurrentRefreshCanSucceed(t *testing.T) {
	service, _ := testService(t)
	first, err := service.Login("internal-ops", strings.Repeat("o", 32))
	if err != nil {
		t.Fatal(err)
	}

	var successes atomic.Int32
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := service.Refresh(first.RefreshToken); err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrInvalidRefresh) {
				t.Errorf("unexpected refresh error: %v", err)
			}
		}()
	}
	group.Wait()
	if successes.Load() != 1 {
		t.Fatalf("expected exactly one successful refresh, got %d", successes.Load())
	}
}

func TestInternalKeyComparison(t *testing.T) {
	service, _ := testService(t)
	if !service.ValidInternalKey(strings.Repeat("i", 32)) {
		t.Fatal("configured internal key was rejected")
	}
	if service.ValidInternalKey(strings.Repeat("x", 32)) {
		t.Fatal("foreign internal key was accepted")
	}
}
