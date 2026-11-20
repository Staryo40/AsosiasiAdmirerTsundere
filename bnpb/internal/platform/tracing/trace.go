// Package tracing propagates W3C trace context alongside correlation IDs.
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
)

const Header = "traceparent"

type traceKey struct{}

// WithTrace returns ctx carrying a W3C traceparent value.
func WithTrace(ctx context.Context, traceparent string) context.Context {
	return context.WithValue(ctx, traceKey{}, traceparent)
}

// Parent returns the traceparent stored in ctx, if any.
func Parent(ctx context.Context) string {
	value, _ := ctx.Value(traceKey{}).(string)
	return value
}

// Ensure incoming requests carry a traceparent for cross-service correlation.
func Ensure(request *http.Request) (context.Context, string) {
	values := request.Header.Values(Header)
	if len(values) == 1 && validTraceparent(values[0]) {
		return WithTrace(request.Context(), values[0]), values[0]
	}
	generated := NewTraceparent()
	ctx := WithTrace(request.Context(), generated)
	return ctx, generated
}

func validTraceparent(value string) bool {
	if len(value) < 55 || value[0:2] != "00" {
		return false
	}
	return value[2] == '-' && value[35] == '-'
}

// NewTraceparent creates a random root span traceparent (sampled).
func NewTraceparent() string {
	identifier := make([]byte, 16)
	_, _ = rand.Read(identifier)
	span := make([]byte, 8)
	_, _ = rand.Read(span)
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(identifier), hex.EncodeToString(span))
}