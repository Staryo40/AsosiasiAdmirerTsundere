package platform

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"
)

// NewHTTPClient returns an isolated, reusable client with correlation and latency logging.
func NewHTTPClient(timeout time.Duration, logger *slog.Logger) *http.Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return &http.Client{
		Transport: instrumentedTransport{next: transport, logger: logger},
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type instrumentedTransport struct {
	next   *http.Transport
	logger *slog.Logger
}

func (t instrumentedTransport) CloseIdleConnections() {
	t.next.CloseIdleConnections()
}

func (t instrumentedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	outbound := requestWithCorrelation(request)
	start := time.Now()
	response, err := t.next.RoundTrip(outbound)
	elapsed := time.Since(start)
	if err != nil {
		t.logFailure(outbound, err, elapsed)
		return nil, err
	}
	t.logSuccess(outbound, response, elapsed)
	return response, nil
}

func requestWithCorrelation(request *http.Request) *http.Request {
	outbound := request.Clone(request.Context())
	correlationID := CorrelationID(request.Context())
	if correlationID == "" {
		correlationID = newCorrelationID()
		outbound = outbound.WithContext(WithCorrelationID(outbound.Context(), correlationID))
	}
	outbound.Header.Set(CorrelationHeader, correlationID)
	return outbound
}

func (t instrumentedTransport) logFailure(request *http.Request, err error, elapsed time.Duration) {
	attrs := outboundLogAttrs(request, elapsed)
	attrs = append(attrs, "failure", transportFailure(request.Context(), err))
	logEvent(request.Context(), t.logger, logRecord{slog.LevelError, "outbound_http", "outbound HTTP request failed"}, attrs...)
}

func (t instrumentedTransport) logSuccess(request *http.Request, response *http.Response, elapsed time.Duration) {
	attrs := append(outboundLogAttrs(request, elapsed), "status", response.StatusCode)
	logEvent(request.Context(), t.logger, logRecord{accessLevel("", response.StatusCode), "outbound_http", "outbound HTTP response headers received"}, attrs...)
}

func outboundLogAttrs(request *http.Request, elapsed time.Duration) []any {
	return []any{
		"method", request.Method,
		"url", safeURL(request.URL),
		"duration_ms", elapsed.Milliseconds(),
	}
}

func safeURL(address *url.URL) string {
	return (&url.URL{Scheme: address.Scheme, Host: address.Host, Path: address.Path, RawPath: address.RawPath}).String()
}

func transportFailure(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "transport_error"
}
