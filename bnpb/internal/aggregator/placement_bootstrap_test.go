package aggregator

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
)

func bootstrapJSON(t *testing.T, revision int64, copies []shardPlacement) string {
	t.Helper()
	raw, err := json.Marshal(placementSnapshot{Revision: revision, Copies: copies})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestPlacementBootstrapConfiguration(t *testing.T) {
	configured := bootstrapJSON(t, 0, rotatingPlacement())
	env := requiredEnvironment()
	env["BNPB_SHARD_PLACEMENT"] = configured
	cfg, err := configFromEnvironment(env)
	if err != nil || cfg.ShardPlacement != configured {
		t.Fatalf("placement configuration not loaded: %v", err)
	}
	parsed, err := parsePlacementBootstrap(cfg.ShardPlacement)
	if err != nil || parsed == nil || len(parsed.Copies) != 6 || parsed.Copies[4].Unit != "s2-a" {
		t.Fatalf("placement not decoded/canonically ordered: %+v, %v", parsed, err)
	}
	if !replicatedBootstrap(parsed.Copies) {
		t.Fatal("normal RF2 bootstrap not recognized")
	}
	unconfigured, err := parsePlacementBootstrap("")
	if err != nil || unconfigured != nil {
		t.Fatalf("omission should preserve existing storage: %+v, %v", unconfigured, err)
	}
	padded := strings.Repeat(" ", maxPlacementBootstrapBytes-len(configured)) + configured
	if _, err := parsePlacementBootstrap(padded); err != nil {
		t.Fatalf("exact byte boundary rejected: %v", err)
	}
	oneCopy := []shardPlacement{parsed.Copies[0], parsed.Copies[2], parsed.Copies[5]}
	if replicatedBootstrap(oneCopy) {
		t.Fatal("one-copy initialization advertised as RF2")
	}
}

func TestPlacementBootstrapRejectsAmbiguousInput(t *testing.T) {
	valid := bootstrapJSON(t, 0, rotatingPlacement())
	duplicate := append(slices.Clone(rotatingPlacement()), rotatingPlacement()[0])
	fullNode := append(slices.Clone(rotatingPlacement()), shardPlacement{1, "s1-a", "node-a", "replica", "copying"})
	for _, test := range []struct{ name, raw string }{
		{"whitespace", " "}, {"null", "null"}, {"array", "[]"}, {"missing copies", `{"revision":0}`},
		{"null revision", strings.Replace(valid, `"revision":0`, `"revision":null`, 1)},
		{"negative revision", strings.Replace(valid, `"revision":0`, `"revision":-1`, 1)},
		{"wrong revision type", strings.Replace(valid, `"revision":0`, `"revision":"private"`, 1)},
		{"duplicate revision", strings.Replace(valid, `"revision":0`, `"revision":0,"revision":1`, 1)},
		{"unknown root key", strings.Replace(valid, `"copies":`, `"private":true,"copies":`, 1)},
		{"unknown copy key", strings.Replace(valid, `"unit":`, `"private":true,"unit":`, 1)},
		{"missing shard key", strings.Replace(valid, `"shard_id":0,`, "", 1)},
		{"null shard key", strings.Replace(valid, `"shard_id":0`, `"shard_id":null`, 1)},
		{"duplicate copy key", strings.Replace(valid, `"shard_id":0`, `"shard_id":0,"shard_id":1`, 1)},
		{"trailing JSON", valid + `{}`}, {"duplicate unit", bootstrapJSON(t, 0, duplicate)},
		{"third shard on node", bootstrapJSON(t, 0, fullNode)},
		{"oversized", strings.Repeat(" ", maxPlacementBootstrapBytes+1)},
		{"invalid UTF8", string([]byte{0xff})},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parsePlacementBootstrap(test.raw); !errors.Is(err, errPlacementBootstrap) || strings.Contains(err.Error(), "private") {
				t.Fatalf("invalid config accepted or leaked: %v", err)
			}
		})
	}
}

func TestPlacementBootstrapRejectsBeforeConnecting(t *testing.T) {
	cfg := testConfig(t)
	cfg.ShardPlacement = `{"revision":0,"copies":null}`
	cfg.DatabaseURL = "not a database address"
	store, err := OpenStore(t.Context(), cfg, quietLogger())
	if store != nil || !errors.Is(err, errPlacementBootstrap) {
		t.Fatalf("invalid config reached database initialization: %v", err)
	}
}
