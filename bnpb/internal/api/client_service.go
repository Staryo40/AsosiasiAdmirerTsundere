package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
)

const (
	authInternalKeyHeader       = "X-BNPB-Auth-Key"
	aggregatorInternalKeyHeader = "X-BNPB-Internal-Key"
	maximumAuthResponseBytes    = 1 << 20
	maximumAggregatorBytes      = 32 << 20
)

var (
	errInvalidToken          = errors.New("invalid access token")
	errDependencyUnavailable = errors.New("dependency unavailable")
)

type principal struct {
	clientID string
	role     string
}

type authIntrospection struct {
	Active    bool   `json:"active"`
	ClientID  string `json:"client_id"`
	Role      string `json:"role"`
	ExpiresAt string `json:"expires_at"`
}

type publicService struct {
	config clientConfig
	client *http.Client
	logger *slog.Logger
}

func newPublicService(config clientConfig, client *http.Client, logger *slog.Logger) (*publicService, error) {
	if client == nil || logger == nil {
		return nil, errors.New("HTTP client and logger are required")
	}
	return &publicService{config: config, client: client, logger: logger}, nil
}

func (service *publicService) authCall(ctx context.Context, path string, input, output any) (int, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return 0, err
	}
	endpoint := joinedURL(service.config.authBaseURL, path, "")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(authInternalKeyHeader, service.config.authInternalKey)
	response, err := service.client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("auth request: %w", err)
	}
	defer response.Body.Close()
	payload, err := readBounded(response.Body, maximumAuthResponseBytes)
	if err != nil {
		return response.StatusCode, fmt.Errorf("auth response: %w", err)
	}
	if response.StatusCode == http.StatusOK && output != nil {
		if err := decodeStrictBytes(payload, output); err != nil {
			return response.StatusCode, fmt.Errorf("auth response: %w", err)
		}
	}
	return response.StatusCode, nil
}

func (service *publicService) introspect(ctx context.Context, accessToken string) (principal, error) {
	var result authIntrospection
	status, err := service.authCall(ctx, "/internal/auth/introspect", map[string]string{"access_token": accessToken}, &result)
	if err != nil || status != http.StatusOK {
		return principal{}, errDependencyUnavailable
	}
	if !result.Active {
		return principal{}, errInvalidToken
	}
	if result.ClientID == "" || (result.Role != roleMedia && result.Role != roleField && result.Role != roleOps) || result.ExpiresAt == "" {
		return principal{}, errDependencyUnavailable
	}
	return principal{clientID: result.ClientID, role: result.Role}, nil
}

func (service *publicService) aggregatorCall(ctx context.Context, path, rawQuery string) (int, []byte, error) {
	endpoint := joinedURL(service.config.aggregatorBaseURL, path, rawQuery)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set(aggregatorInternalKeyHeader, service.config.aggregatorKey)
	response, err := service.client.Do(request)
	if err != nil {
		return 0, nil, fmt.Errorf("aggregator request: %w", err)
	}
	defer response.Body.Close()
	payload, err := readBounded(response.Body, maximumAggregatorBytes)
	if err != nil {
		return response.StatusCode, nil, fmt.Errorf("aggregator response: %w", err)
	}
	return response.StatusCode, payload, nil
}

func joinedURL(base *url.URL, path, rawQuery string) string {
	endpoint := *base
	endpoint.Path = path
	endpoint.RawQuery = rawQuery
	return endpoint.String()
}

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > maximum {
		return nil, errors.New("response body exceeds configured limit")
	}
	return payload, nil
}

func decodeStrictBytes(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("response contains trailing JSON")
	}
	return nil
}
