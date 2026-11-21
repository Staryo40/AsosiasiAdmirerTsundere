package satellite

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestVerifyAgreesAfterCrashes(t *testing.T) {
	const total = 3000
	base := framedFeed(t, syntheticFrames(11, total))
	path := filepath.Join(t.TempDir(), "satellite.db")
	runToEnd(t, path, base, total, []int{40, 700, 5})
	var out, errs bytes.Buffer
	if code := RunVerify([]string{path}, &out, &errs); code != 0 {
		t.Fatalf("verify exit %d: %s %s", code, errs.String(), out.String())
	}
	var report verifyReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.NextOffset != total || report.Published == 0 || report.Published != report.Batch || len(report.Fires) != report.Published {
		t.Fatalf("unexpected report: next=%d published=%d batch=%d fires=%d", report.NextOffset, report.Published, report.Batch, len(report.Fires))
	}
}

func TestVerifyFlagsAnOutboxThatLostAChange(t *testing.T) {
	const total = 1500
	path := filepath.Join(t.TempDir(), "satellite.db")
	store := runToEnd(t, path, framedFeed(t, syntheticFrames(3, total)), total, nil)
	if _, err := store.db.ExecContext(context.Background(), "DELETE FROM outbox WHERE seq = (SELECT max(seq) FROM outbox)"); err != nil {
		t.Fatal(err)
	}
	report, err := verifyDatabase(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if report.PublishedRebuilt {
		t.Fatal("a missing outbox change must show as a difference from the rebuild")
	}
}
