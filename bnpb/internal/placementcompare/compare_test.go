package placementcompare

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

func TestCompareJSONLDeterministicAndReportsHandCalculatedSkew(t *testing.T) {
	input := []byte("{\"event_id\":\"synthetic-a\",\"region_name\":\"Region A\",\"epicenter_lon\":-100}\n" +
		"{\"event_id\":\"synthetic-b\",\"region_name\":\"Region A\",\"epicenter_lon\":-80}\n" +
		"{\"event_id\":\"synthetic-c\",\"region_name\":\"Region B\",\"epicenter_lon\":0}")
	resources := []byte("{\"nodes\":[\"synthetic-node-a\",\"synthetic-node-b\",\"synthetic-node-c\"]}\n")
	first, err := CompareJSONL(bytes.NewReader(input), resources, "Region A")
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompareJSONL(bytes.NewReader(input), resources, "Region A")
	if err != nil {
		t.Fatal(err)
	}
	if first.InputSHA256 != second.InputSHA256 || first.Natural != second.Natural || first.RegionBurst == nil || second.RegionBurst == nil {
		t.Fatal("identical inputs did not produce deterministic comparison")
	}
	inputHash := sha256.Sum256(input)
	resourceHash := sha256.Sum256(resources)
	if first.InputSHA256 != hex.EncodeToString(inputHash[:]) || first.ResourcesSHA256 != hex.EncodeToString(resourceHash[:]) {
		t.Fatal("input or resource hash does not cover the exact supplied bytes")
	}
	geo := first.Natural.LongitudeBand
	if geo.Rows != 3 || geo.Shards[0].LogicalRows != 2 || geo.Shards[1].LogicalRows != 1 || geo.Shards[2].LogicalRows != 0 {
		t.Fatalf("unexpected hand-calculated longitude placement: %+v", geo)
	}
	if geo.Skew.LogicalRows.Max != 2 || geo.Skew.LogicalRows.Mean != 1 || geo.Skew.LogicalRows.MaxToMean != 2 {
		t.Fatalf("unexpected row skew: %+v", geo.Skew.LogicalRows)
	}
	var expectedBytes int64
	for _, row := range bytes.Split(input, []byte("\n")) {
		expectedBytes += int64(len(row))
	}
	if got := geo.Shards[0].InputBytes + geo.Shards[1].InputBytes + geo.Shards[2].InputBytes; got != expectedBytes {
		t.Fatalf("input bytes counted %d, want exact raw size %d", got, expectedBytes)
	}
	burst := first.RegionBurst.LongitudeBand
	if burst.Rows != 2 || burst.Shards[0].LogicalRows != 2 || burst.Shards[1].LogicalRows != 0 || burst.Shards[2].LogicalRows != 0 {
		t.Fatalf("region burst changed identity or included other regions: %+v", burst)
	}
	if first.RegionBurst.FixedHash.Rows != 2 || totalWrites(first.RegionBurst.FixedHash) != 2 || totalWrites(first.RegionBurst.LongitudeBand) != 2 {
		t.Fatal("region burst must count only the two original matching identities, without synthetic writes")
	}
}

func TestCheckedInSyntheticCatalogue(t *testing.T) {
	input, err := os.ReadFile("testdata/catalog.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	result, err := CompareJSONL(bytes.NewReader(input), []byte("synthetic resource manifest"), "Kabupaten Contoh")
	if err != nil {
		t.Fatal(err)
	}
	if result.InputRows != 6 || result.RegionBurst == nil || result.RegionBurst.FixedHash.Rows != 2 || result.RegionBurst.LongitudeBand.Rows != 2 {
		t.Fatalf("unexpected synthetic fixture summary: rows=%d burst=%+v", result.InputRows, result.RegionBurst)
	}
}

func totalWrites(value scenario) int64 {
	var total int64
	for _, shard := range value.Shards {
		total += shard.Writes
	}
	return total
}
