package api

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

const (
	defaultAuthBaseURL       = "http://auth:8084"
	defaultAggregatorBaseURL = "http://aggregator:8083"
)

// clientConfig contains dependency locations and dedicated credentials. Keys
// remain private to prevent accidental formatting or logging.
type clientConfig struct {
	authBaseURL       *url.URL
	aggregatorBaseURL *url.URL
	authInternalKey   string
	aggregatorKey     string
}

func loadClientConfig() (clientConfig, error) {
	return loadClientConfigFrom(os.LookupEnv)
}

func loadClientConfigFrom(lookup func(string) (string, bool)) (clientConfig, error) {
	authURL, err := baseURLSetting(lookup, "BNPB_AUTH_BASE_URL", defaultAuthBaseURL)
	if err != nil {
		return clientConfig{}, err
	}
	aggregatorURL, err := baseURLSetting(lookup, "BNPB_AGGREGATOR_BASE_URL", defaultAggregatorBaseURL)
	if err != nil {
		return clientConfig{}, err
	}
	authKey, err := secretSetting(lookup, "BNPB_AUTH_INTERNAL_KEY")
	if err != nil {
		return clientConfig{}, err
	}
	aggregatorKey, err := secretSetting(lookup, "BNPB_AGGREGATOR_INTERNAL_KEY")
	if err != nil {
		return clientConfig{}, err
	}
	return clientConfig{
		authBaseURL: authURL, aggregatorBaseURL: aggregatorURL,
		authInternalKey: authKey, aggregatorKey: aggregatorKey,
	}, nil
}

func baseURLSetting(lookup func(string) (string, bool), name, fallback string) (*url.URL, error) {
	configured := fallback
	if value, exists := lookup(name); exists {
		configured = value
	}
	parsed, err := url.Parse(configured)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%s: must be an absolute HTTP(S) origin without credentials, path, query or fragment", name)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return nil, fmt.Errorf("%s: base URL must not contain a path", name)
	}
	parsed.Path = ""
	return parsed, nil
}

func secretSetting(lookup func(string) (string, bool), name string) (string, error) {
	value, exists := lookup(name)
	if !exists || value == "" {
		return "", fmt.Errorf("%s: required environment variable is missing or empty", name)
	}
	if len(value) < 32 || strings.TrimSpace(value) != value {
		return "", fmt.Errorf("%s: must contain at least 32 bytes without surrounding whitespace", name)
	}
	return value, nil
}
