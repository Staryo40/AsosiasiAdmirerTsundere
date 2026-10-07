package authservice

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

const tokenBytes = 32

var (
	// ErrInvalidCredentials deliberately covers both the client ID and secret.
	ErrInvalidCredentials = errors.New("invalid client credentials")
	// ErrInvalidRefresh deliberately covers unknown, used and expired tokens.
	ErrInvalidRefresh = errors.New("invalid or expired refresh token")
)

// TokenResponse is returned by both login and refresh.
type TokenResponse struct {
	AccessToken      string `json:"access_token"`
	TokenType        string `json:"token_type"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
}

// Introspection is the Auth-to-API contract for one opaque access token.
type Introspection struct {
	Active    bool       `json:"active"`
	ClientID  string     `json:"client_id,omitempty"`
	Role      Role       `json:"role,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type accessGrant struct {
	clientID string
	role     Role
	expires  time.Time
}

type refreshSession struct {
	clientID   string
	role       Role
	expires    time.Time
	accessHash [sha256.Size]byte
}

// Service issues opaque tokens and owns their in-memory state.
type Service struct {
	config Config
	logger *slog.Logger
	now    func() time.Time
	random io.Reader

	mu            sync.Mutex
	accessTokens  map[[sha256.Size]byte]accessGrant
	refreshTokens map[[sha256.Size]byte]refreshSession
}

// New creates an Auth Service using cryptographically secure randomness.
func New(config Config, logger *slog.Logger) (*Service, error) {
	if len(config.clients) != 3 {
		return nil, fmt.Errorf("exactly three downstream clients are required")
	}
	return &Service{
		config:        config,
		logger:        logger,
		now:           time.Now,
		random:        rand.Reader,
		accessTokens:  make(map[[sha256.Size]byte]accessGrant),
		refreshTokens: make(map[[sha256.Size]byte]refreshSession),
	}, nil
}

// Login exchanges one configured client credential for an opaque token pair.
func (service *Service) Login(clientID, clientSecret string) (TokenResponse, error) {
	client, valid := service.authenticate(clientID, clientSecret)
	if !valid {
		return TokenResponse{}, ErrInvalidCredentials
	}

	now := service.now().UTC()
	session := refreshSession{clientID: client.id, role: client.role, expires: now.Add(service.config.RefreshSessionTTL)}
	service.mu.Lock()
	defer service.mu.Unlock()
	service.deleteExpiredLocked(now)
	return service.issuePairLocked(now, session)
}

// Refresh atomically consumes a refresh token, revokes its access token, and
// rotates both tokens without extending the absolute session deadline.
func (service *Service) Refresh(refreshToken string) (TokenResponse, error) {
	if refreshToken == "" {
		return TokenResponse{}, ErrInvalidRefresh
	}
	hash := sha256.Sum256([]byte(refreshToken))
	now := service.now().UTC()

	service.mu.Lock()
	defer service.mu.Unlock()
	service.deleteExpiredLocked(now)
	session, exists := service.refreshTokens[hash]
	if !exists || !now.Before(session.expires) {
		return TokenResponse{}, ErrInvalidRefresh
	}

	oldAccessHash := session.accessHash
	response, err := service.issuePairLocked(now, session)
	if err != nil {
		return TokenResponse{}, err
	}
	delete(service.refreshTokens, hash)
	delete(service.accessTokens, oldAccessHash)
	return response, nil
}

// Introspect reports only active, unexpired access tokens.
func (service *Service) Introspect(accessToken string) Introspection {
	if accessToken == "" {
		return Introspection{Active: false}
	}
	hash := sha256.Sum256([]byte(accessToken))
	now := service.now().UTC()

	service.mu.Lock()
	defer service.mu.Unlock()
	service.deleteExpiredLocked(now)
	grant, exists := service.accessTokens[hash]
	if !exists || !now.Before(grant.expires) {
		return Introspection{Active: false}
	}
	expires := grant.expires
	return Introspection{Active: true, ClientID: grant.clientID, Role: grant.role, ExpiresAt: &expires}
}

// ValidInternalKey authenticates the single Client-API-to-Auth credential.
func (service *Service) ValidInternalKey(provided string) bool {
	providedHash := sha256.Sum256([]byte(provided))
	return subtle.ConstantTimeCompare(providedHash[:], service.config.internalKeyHash[:]) == 1
}

func (service *Service) issuePairLocked(now time.Time, session refreshSession) (TokenResponse, error) {
	accessToken, accessHash, err := uniqueToken(service.random, service.accessTokens)
	if err != nil {
		return TokenResponse{}, fmt.Errorf("generate access token: %w", err)
	}
	refreshToken, refreshHash, err := uniqueToken(service.random, service.refreshTokens)
	if err != nil {
		return TokenResponse{}, fmt.Errorf("generate refresh token: %w", err)
	}

	accessExpiry := now.Add(service.config.AccessTokenTTL)
	if accessExpiry.After(session.expires) {
		accessExpiry = session.expires
	}
	session.accessHash = accessHash
	service.accessTokens[accessHash] = accessGrant{clientID: session.clientID, role: session.role, expires: accessExpiry}
	service.refreshTokens[refreshHash] = session

	return TokenResponse{
		AccessToken:      accessToken,
		TokenType:        "Bearer",
		ExpiresIn:        wholeSeconds(accessExpiry.Sub(now)),
		RefreshToken:     refreshToken,
		RefreshExpiresIn: wholeSeconds(session.expires.Sub(now)),
	}, nil
}

func uniqueToken[T any](source io.Reader, tokens map[[sha256.Size]byte]T) (string, [sha256.Size]byte, error) {
	for {
		token, err := randomToken(source)
		if err != nil {
			return "", [sha256.Size]byte{}, err
		}
		hash := sha256.Sum256([]byte(token))
		if _, collision := tokens[hash]; !collision {
			return token, hash, nil
		}
	}
}

func (service *Service) authenticate(id, secret string) (clientCredential, bool) {
	idHash := sha256.Sum256([]byte(id))
	secretHash := sha256.Sum256([]byte(secret))
	var selected clientCredential
	matched := 0
	for _, candidate := range service.config.clients {
		match := subtle.ConstantTimeCompare(idHash[:], candidate.idDigest[:]) &
			subtle.ConstantTimeCompare(secretHash[:], candidate.secretHash[:])
		if match == 1 {
			selected = candidate
		}
		matched |= match
	}
	return selected, matched == 1
}

func (service *Service) deleteExpiredLocked(now time.Time) {
	for hash, grant := range service.accessTokens {
		if !now.Before(grant.expires) {
			delete(service.accessTokens, hash)
		}
	}
	for hash, session := range service.refreshTokens {
		if !now.Before(session.expires) {
			delete(service.refreshTokens, hash)
			delete(service.accessTokens, session.accessHash)
		}
	}
}

func randomToken(source io.Reader) (string, error) {
	random := make([]byte, tokenBytes)
	if _, err := io.ReadFull(source, random); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(random), nil
}

func wholeSeconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return int64(duration / time.Second)
}
