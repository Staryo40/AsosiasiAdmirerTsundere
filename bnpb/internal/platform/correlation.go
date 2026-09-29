package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
)

const CorrelationHeader = "X-Correlation-ID"

type correlationKey struct{}

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// Returns a child context carrying id.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	correlationID, _ := ctx.Value(correlationKey{}).(string)
	return correlationID
}

func newCorrelationID() string {
	random := make([]byte, 16)
	_, _ = rand.Read(random)
	return hex.EncodeToString(random)
}
