package aisummarizer

import (
	"testing"
	"time"
)

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func baseEnv() map[string]string {
	return map[string]string{
		"BNPB_AI_SUMMARIZER_RABBITMQ_USER":     "ai-summarizer",
		"BNPB_AI_SUMMARIZER_RABBITMQ_PASSWORD": "pw",
		"BNPB_AI_SUMMARIZER_INTERNAL_KEY":      "key",
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(lookupFrom(baseEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queue != "hazard.events.ai-summarizer" || cfg.Interval != time.Minute || cfg.Window != time.Hour {
		t.Fatalf("cfg = %+v", cfg)
	}
	if len(cfg.Providers) != 3 || cfg.Providers[0].Name != "anthropic" || cfg.Providers[0].Model != defaultAnthropicModel {
		t.Fatalf("providers = %+v", cfg.Providers)
	}
}

func TestConfigRejectsUnknownProviderAndFault(t *testing.T) {
	env := baseEnv()
	env["BNPB_AI_SUMMARIZER_PROVIDERS"] = "anthropic,other"
	if _, err := loadConfig(lookupFrom(env)); err == nil {
		t.Fatal("accepted an unknown provider")
	}
	env = baseEnv()
	env["BNPB_AI_SUMMARIZER_FAULT"] = "broken"
	if _, err := loadConfig(lookupFrom(env)); err == nil {
		t.Fatal("accepted an unknown fault")
	}
}

func TestConfigRequiresTheInternalKey(t *testing.T) {
	env := baseEnv()
	delete(env, "BNPB_AI_SUMMARIZER_INTERNAL_KEY")
	if _, err := loadConfig(lookupFrom(env)); err == nil {
		t.Fatal("started without an internal key")
	}
}
