// Package correlation carries request identifiers across service boundaries.
package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
)

// Header is the HTTP header used to propagate correlation identifiers.
const Header = "X-Correlation-ID"

type correlationKey struct{}

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// WithID returns a child context carrying id.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// ID returns the correlation identifier in ctx, or an empty string.
func ID(ctx context.Context) string {
	correlationID, _ := ctx.Value(correlationKey{}).(string)
	return correlationID
}

// NewID generates a random correlation identifier.
func NewID() string {
	random := make([]byte, 16)
	_, _ = rand.Read(random)
	return hex.EncodeToString(random)
}

// ValidID reports whether id is safe to accept from an incoming HTTP header.
func ValidID(id string) bool {
	return correlationPattern.MatchString(id)
}

// MessageContext prefers the transport identity; legacy envelopes can supply a valid fallback.
func MessageContext(ctx context.Context, transportID, envelopeID string) context.Context {
	if ValidID(transportID) {
		return WithID(ctx, transportID)
	}
	if ValidID(envelopeID) {
		return WithID(ctx, envelopeID)
	}
	return WithID(ctx, NewID())
}
