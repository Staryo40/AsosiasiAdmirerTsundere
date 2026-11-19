package correlation

import (
	"context"
	"net/http"
	"strings"
)

const traceParentHeader = "Traceparent"

type traceContextKey struct{}

// WithTrace links a request to a valid W3C v00 trace. Baggage and vendor
// tracestate do not cross the service trust boundary.
func WithTrace(ctx context.Context, header http.Header) context.Context {
	values := header.Values(traceParentHeader)
	if len(values) != 1 || !validTraceParent(values[0]) {
		return ctx
	}
	return context.WithValue(ctx, traceContextKey{}, values[0])
}

func validTraceParent(parent string) bool {
	if len(parent) != 55 || !strings.HasPrefix(parent, "00-") || parent[35] != '-' || parent[52] != '-' {
		return false
	}
	return validTraceHex(parent[3:35]) && validTraceHex(parent[36:52]) &&
		validTraceHex(parent[53:]) && parent[3:35] != "00000000000000000000000000000000" &&
		parent[36:52] != "0000000000000000"
}

func validTraceHex(value string) bool {
	for index := range len(value) {
		c := value[index]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// TraceID returns the validated incoming trace identifier, or an empty string.
func TraceID(ctx context.Context) string {
	parent, ok := ctx.Value(traceContextKey{}).(string)
	if !ok {
		return ""
	}
	return parent[3:35]
}

// InjectTraceHeaders forwards only the validated traceparent. It preserves the
// parent ID because propagation alone does not create a span.
func InjectTraceHeaders(ctx context.Context, header http.Header) {
	if parent, ok := ctx.Value(traceContextKey{}).(string); ok {
		header.Set(traceParentHeader, parent)
	}
}
