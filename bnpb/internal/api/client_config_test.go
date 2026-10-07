package api

import (
	"strings"
	"testing"
)

func clientEnvironment() map[string]string {
	return map[string]string{
		"BNPB_AUTH_BASE_URL":           "http://auth:8084",
		"BNPB_AGGREGATOR_BASE_URL":     "http://aggregator:8083",
		"BNPB_AUTH_INTERNAL_KEY":       strings.Repeat("a", 32),
		"BNPB_AGGREGATOR_INTERNAL_KEY": strings.Repeat("g", 32),
	}
}

func clientLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	}
}

func TestClientConfigLoadsSafeOriginsAndKeys(t *testing.T) {
	config, err := loadClientConfigFrom(clientLookup(clientEnvironment()))
	if err != nil {
		t.Fatal(err)
	}
	if config.authBaseURL.String() != "http://auth:8084" || config.aggregatorBaseURL.String() != "http://aggregator:8083" {
		t.Fatalf("unexpected dependency URLs: %s %s", config.authBaseURL, config.aggregatorBaseURL)
	}
}

func TestClientConfigRejectsUnsafeValues(t *testing.T) {
	for _, scenario := range []struct{ name, variable, value string }{
		{"auth credentials", "BNPB_AUTH_BASE_URL", "http://user:secret@auth:8084"},
		{"aggregator path", "BNPB_AGGREGATOR_BASE_URL", "http://aggregator:8083/internal"},
		{"short auth key", "BNPB_AUTH_INTERNAL_KEY", "short"},
		{"short aggregator key", "BNPB_AGGREGATOR_INTERNAL_KEY", "short"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			values := clientEnvironment()
			values[scenario.variable] = scenario.value
			_, err := loadClientConfigFrom(clientLookup(values))
			if err == nil || strings.Contains(err.Error(), "user:secret") {
				t.Fatalf("unsafe value was accepted or echoed: %v", err)
			}
		})
	}
}
