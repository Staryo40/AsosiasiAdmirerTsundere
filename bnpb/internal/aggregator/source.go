package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
)

type pollRequestMetadataKey struct{}

type pollRequestMetadata struct {
	endpoint         Endpoint
	period           string
	authority        *PollAuthority
	authorityTimeout time.Duration
}
type sourceClients struct {
	bmkg, pvmbg *sourceClient
}

type sourceClient struct {
	client   *http.Client
	origin   *url.URL
	apiKey   string
	maxBytes int64
	routed   bool
}

type sourceRequest struct {
	method, path string
	query        url.Values
	header       http.Header
	body         []byte
}

// Error messages omit source URLs, credentials and bodies.
type sourceError struct {
	category   string
	statusCode int
	cause      error
	retryAfter time.Duration
}

func (err *sourceError) Error() string {
	if err.statusCode != 0 {
		return fmt.Sprintf("source request: HTTP %d", err.statusCode)
	}
	return "source request: " + err.category
}

func (err *sourceError) Unwrap() error { return err.cause }

// Configuration is validated before parsing these URLs.
func newSourceClients(cfg Config, logger *slog.Logger) sourceClients {
	bmkgURL, _ := url.Parse(cfg.BMKG.BaseURL)
	pvmbgURL, _ := url.Parse(cfg.PVMBG.BaseURL)
	newClient := httpclient.New
	routed := cfg.ShardURLs[0] != ""
	if routed {
		newClient = httpclient.NewDirectSingleRequest
	}
	return sourceClients{
		bmkg: &sourceClient{
			client: newClient(cfg.BMKG.HTTPTimeout, logger), origin: bmkgURL,
			apiKey: cfg.BMKGAPIKey, maxBytes: cfg.MaxResponseBytes, routed: routed,
		},
		pvmbg: &sourceClient{
			client: newClient(cfg.PVMBG.HTTPTimeout, logger), origin: pvmbgURL,
			maxBytes: cfg.MaxResponseBytes, routed: routed,
		},
	}
}

func (clients sourceClients) closeIdleConnections() {
	clients.bmkg.client.CloseIdleConnections()
	clients.pvmbg.client.CloseIdleConnections()
}

func (client *sourceClient) request(ctx context.Context, request sourceRequest) (*http.Request, error) {
	if !strings.HasPrefix(request.path, "/") || strings.HasPrefix(request.path, "//") || strings.ContainsAny(request.path, "?#\\") {
		return nil, &sourceError{category: "invalid_request"}
	}
	address := *client.origin
	address.Path = request.path
	address.RawQuery = request.query.Encode()
	outbound, err := http.NewRequestWithContext(ctx, request.method, address.String(), bytes.NewReader(request.body))
	if err != nil {
		return nil, &sourceError{category: "invalid_request"}
	}
	outbound.Header = client.headers(request)
	if owner, epoch, ok := pollAuthorityToken(ctx); ok {
		outbound.Header.Set("X-BNPB-Owner", owner)
		outbound.Header.Set("X-BNPB-Epoch", strconv.FormatInt(epoch, 10))
		if metadata, ok := ctx.Value(pollRequestMetadataKey{}).(pollRequestMetadata); ok {
			outbound.Header.Set("X-BNPB-Endpoint", string(metadata.endpoint))
			outbound.Header.Set("X-BNPB-Period", metadata.period)
		}
	}
	return outbound, nil
}

func (client *sourceClient) headers(request sourceRequest) http.Header {
	headers := request.header.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Accept", "application/json")
	if request.body != nil {
		headers.Set("Content-Type", "application/json")
	}
	if client.apiKey != "" {
		headers.Set("X-BMKG-Key", client.apiKey)
	}
	return headers
}

func verifyPollAuthority(ctx context.Context) error {
	metadata, _ := ctx.Value(pollRequestMetadataKey{}).(pollRequestMetadata)
	if metadata.authority == nil {
		return nil
	}
	owner, epoch, ok := pollAuthorityToken(ctx)
	if !ok {
		return errStaleMutationAuthority
	}
	checkCtx, cancel := context.WithTimeout(ctx, metadata.authorityTimeout)
	defer cancel()
	live, err := metadata.authority.currentContext(checkCtx)
	if err != nil {
		return err
	}
	if currentOwner, currentEpoch, _ := pollAuthorityToken(live); currentOwner != owner || currentEpoch != epoch {
		return errStaleMutationAuthority
	}
	return nil
}

func (client *sourceClient) doJSON(ctx context.Context, request sourceRequest, target any) error {
	// PVMBG login may have taken time; validate ownership before dispatching the data GET.
	if client.routed && request.method == http.MethodGet {
		if err := verifyPollAuthority(ctx); err != nil {
			return err
		}
	}
	// Bound body reads through the instrumented transport.
	ctx, cancel := context.WithTimeout(ctx, client.client.Timeout)
	defer cancel()
	outbound, err := client.request(ctx, request)
	if err != nil {
		return err
	}
	response, err := client.client.Do(outbound)
	if err != nil {
		return safeSourceFailure("transport_error", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &sourceError{category: "http_status", statusCode: response.StatusCode,
			retryAfter: retryAfter(response.Header.Get("Retry-After"), time.Now())}
	}
	return client.decode(response, target)
}

func retryAfter(header string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(header), 10, 64); err == nil {
		if seconds > 0 {
			return time.Duration(min(seconds, int64((1<<63-1)/time.Second))) * time.Second
		}
		return 0
	}
	if stamp, err := http.ParseTime(header); err == nil {
		return max(0, stamp.Sub(now))
	}
	return 0
}

func (client *sourceClient) decode(response *http.Response, target any) error {
	if response.ContentLength > client.maxBytes {
		return &sourceError{category: "response_too_large"}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, client.maxBytes+1))
	if err != nil {
		return safeSourceFailure("read_error", err)
	}
	if int64(len(body)) > client.maxBytes {
		return &sourceError{category: "response_too_large"}
	}
	if err := json.Unmarshal(body, target); err != nil {
		return &sourceError{category: "invalid_json"}
	}
	return nil
}

func safeSourceFailure(category string, err error) error {
	var addressErr *url.Error
	if errors.As(err, &addressErr) {
		err = addressErr.Err
	}
	var networkErr net.Error
	switch {
	case errors.Is(err, context.Canceled):
		category = "canceled"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &networkErr) && networkErr.Timeout():
		category = "timeout"
	}
	return &sourceError{category: category, cause: err}
}
