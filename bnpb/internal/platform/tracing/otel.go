package tracing

import (
	"context"
	"net/http"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	// EndpointVariable is the standard OpenTelemetry setting; without it no span is exported.
	EndpointVariable = "OTEL_EXPORTER_OTLP_ENDPOINT"
	tracerName       = "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb"
)

var propagator = propagation.TraceContext{}

// Start installs an OTLP HTTP exporter when the endpoint is configured; otherwise spans stay no-ops.
func Start(ctx context.Context, service string) (func(context.Context) error, error) {
	if os.Getenv(EndpointVariable) == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagator)
	return provider.Shutdown, nil
}

// Probes and scrapes would bury the request traces.
var untraced = map[string]bool{"/health": true, "/ready": true, "/readyz": true, "/metrics": true}

// StartServer opens the span for one incoming request, continuing the caller's trace.
// The returned context carries the span's own traceparent, so outbound calls become its children.
func StartServer(request *http.Request) (context.Context, trace.Span) {
	ctx := propagator.Extract(request.Context(), propagation.HeaderCarrier(request.Header))
	span := trace.SpanFromContext(ctx)
	if !untraced[request.URL.Path] {
		ctx, span = otel.Tracer(tracerName).Start(ctx, request.Method, trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", request.Method), attribute.String("url.path", request.URL.Path)))
	}
	if !span.SpanContext().IsValid() || !span.IsRecording() {
		ctx, _ = Ensure(request.WithContext(ctx))
		return ctx, span
	}
	return WithTrace(ctx, traceparentOf(span.SpanContext())), span
}

// EndServer names the span after the matched route, so traces group by endpoint, not by ID.
func EndServer(span trace.Span, method, pattern string, status int) {
	if !span.IsRecording() {
		return
	}
	if pattern != "" {
		span.SetName(pattern)
	} else {
		span.SetName(method)
	}
	span.SetAttributes(attribute.Int("http.response.status_code", status))
	if status >= http.StatusInternalServerError {
		span.SetStatus(codes.Error, http.StatusText(status))
	}
	span.End()
}

// StartClient opens a span for one outbound call and points the request's traceparent at it.
func StartClient(request *http.Request) (*http.Request, trace.Span) {
	ctx := propagator.Extract(request.Context(), propagation.HeaderCarrier(request.Header))
	if parent := Parent(request.Context()); parent != "" && !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = propagator.Extract(ctx, propagation.HeaderCarrier(http.Header{"Traceparent": {parent}}))
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, request.Method+" "+request.URL.Host, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("http.request.method", request.Method), attribute.String("server.address", request.URL.Host),
			attribute.String("url.path", request.URL.Path)))
	if span.IsRecording() {
		request = request.WithContext(ctx)
		request.Header.Set(Header, traceparentOf(span.SpanContext()))
	}
	return request, span
}

// EndClient records the outcome of an outbound call.
func EndClient(span trace.Span, status int, err error) {
	if !span.IsRecording() {
		return
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	}
	span.End()
}

func traceparentOf(spanContext trace.SpanContext) string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(trace.ContextWithSpanContext(context.Background(), spanContext), carrier)
	return carrier["traceparent"]
}
