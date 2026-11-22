// Package aisummarizer keeps the last hour of hazard events from the
// hazard.events stream and writes a short Indonesian situation summary with
// an LLM, falling back across providers and finally to a template. It is off
// the alert path: nothing else waits on it. Its broker code is its own copy,
// since services must not share a business-logic library.
package aisummarizer

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultBrokerAddr     = "rabbitmq:5672"
	defaultDBPath         = "/data/ai-summarizer.db"
	defaultProviders      = "anthropic,openrouter,gemini"
	defaultAnthropicModel = "claude-haiku-5-5"
	queueRole             = "ai-summarizer"
)

type providerConfig struct {
	Name  string
	Key   string
	Model string
}

type Config struct {
	BrokerURL   string
	Queue       string
	DBPath      string
	InternalKey string
	Providers   []providerConfig
	Fault       string
	Interval    time.Duration
	Window      time.Duration
}

func LoadConfig() (Config, error) {
	return loadConfig(os.LookupEnv)
}

func loadConfig(lookup func(string) (string, bool)) (Config, error) {
	user, err := required(lookup, "BNPB_AI_SUMMARIZER_RABBITMQ_USER")
	if err != nil {
		return Config{}, err
	}
	password, err := required(lookup, "BNPB_AI_SUMMARIZER_RABBITMQ_PASSWORD")
	if err != nil {
		return Config{}, err
	}
	internalKey, err := required(lookup, "BNPB_AI_SUMMARIZER_INTERNAL_KEY")
	if err != nil {
		return Config{}, err
	}
	interval, err := duration(lookup, "BNPB_AI_SUMMARIZER_INTERVAL", time.Minute)
	if err != nil {
		return Config{}, err
	}
	window, err := duration(lookup, "BNPB_AI_SUMMARIZER_WINDOW", time.Hour)
	if err != nil {
		return Config{}, err
	}
	fault := envOrDefault(lookup, "BNPB_AI_SUMMARIZER_FAULT", "")
	if !validFault(fault) {
		return Config{}, fmt.Errorf("BNPB_AI_SUMMARIZER_FAULT: must be empty, slow, wrong, or down")
	}
	providers, err := loadProviders(lookup)
	if err != nil {
		return Config{}, err
	}
	return Config{
		BrokerURL:   fmt.Sprintf("amqp://%s@%s/", url.UserPassword(user, password).String(), envOrDefault(lookup, "BNPB_RABBITMQ_ADDR", defaultBrokerAddr)),
		Queue:       "hazard.events." + queueRole,
		DBPath:      envOrDefault(lookup, "BNPB_AI_SUMMARIZER_DB_PATH", defaultDBPath),
		InternalKey: internalKey,
		Providers:   providers,
		Fault:       fault,
		Interval:    interval,
		Window:      window,
	}, nil
}

// loadProviders keeps the configured order; a provider without a key or model is left out.
func loadProviders(lookup func(string) (string, bool)) ([]providerConfig, error) {
	var providers []providerConfig
	for _, raw := range strings.Split(envOrDefault(lookup, "BNPB_AI_SUMMARIZER_PROVIDERS", defaultProviders), ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if name != "anthropic" && name != "openrouter" && name != "gemini" {
			return nil, fmt.Errorf("BNPB_AI_SUMMARIZER_PROVIDERS: unknown provider %q", name)
		}
		prefix := "BNPB_AI_SUMMARIZER_" + strings.ToUpper(name)
		model := envOrDefault(lookup, prefix+"_MODEL", "")
		if name == "anthropic" && model == "" {
			model = defaultAnthropicModel
		}
		providers = append(providers, providerConfig{Name: name, Key: envOrDefault(lookup, prefix+"_KEY", ""), Model: model})
	}
	return providers, nil
}

func validFault(fault string) bool {
	return fault == "" || fault == "slow" || fault == "wrong" || fault == "down"
}

func duration(lookup func(string) (string, bool), name string, fallback time.Duration) (time.Duration, error) {
	value := envOrDefault(lookup, name, "")
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s: must be a positive duration such as 60s", name)
	}
	return parsed, nil
}

func envOrDefault(lookup func(string) (string, bool), name, fallback string) string {
	if value, exists := lookup(name); exists && value != "" {
		return value
	}
	return fallback
}

func required(lookup func(string) (string, bool), name string) (string, error) {
	value, exists := lookup(name)
	if !exists || value == "" {
		return "", fmt.Errorf("%s: required environment variable is missing or empty", name)
	}
	return value, nil
}
