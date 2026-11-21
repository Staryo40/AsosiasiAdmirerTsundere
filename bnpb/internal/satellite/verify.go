package satellite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/satellite/fire"
)

// Mirrors what the engine accepted, so batch clustering sees exactly the detections that count.
type acceptedDetections struct {
	final map[string]fire.Detection
	gone  map[string]bool
}

func newAcceptedDetections() *acceptedDetections {
	return &acceptedDetections{final: map[string]fire.Detection{}, gone: map[string]bool{}}
}

func (accepted *acceptedDetections) record(event Event, outcome fire.Outcome) {
	switch {
	case event.Kind == KindDetection && outcome == fire.Added:
		detection, _ := engineDetection(*event.Detection)
		accepted.final[detection.Key] = detection
	case event.Kind == KindRetract && outcome == fire.Applied:
		accepted.gone[event.Key] = true
	case event.Kind == KindUpdate && outcome == fire.Applied:
		if detection, ok := accepted.final[event.Key]; ok {
			if raw, present := event.Changes["confidence"]; present {
				detection.Confidence = strings.Trim(string(raw), `"`)
			}
			accepted.final[event.Key] = detection
		}
	}
}

func (accepted *acceptedDetections) batch() [][]string {
	var counted []fire.Detection
	for key, detection := range accepted.final {
		if !accepted.gone[key] && detection.Confidence != "l" {
			counted = append(counted, detection)
		}
	}
	return fire.Batch(counted)
}

type verifiedFire struct {
	ID            string   `json:"id"`
	Severity      string   `json:"severity"`
	AreaName      string   `json:"area_name"`
	OccurredAt    string   `json:"occurred_at"`
	FeedRevision  int64    `json:"feed_revision"`
	DetectionKeys []string `json:"detection_keys"`
}

type verifyReport struct {
	NextOffset       int64          `json:"next_offset"`
	LoggedEvents     int            `json:"logged_events"`
	Stats            fire.Stats     `json:"stats"`
	Published        int            `json:"published_active"`
	Rebuilt          int            `json:"rebuilt_active"`
	Batch            int            `json:"batch_fires"`
	Undelivered      int64          `json:"undelivered_changes"`
	PublishedRebuilt bool           `json:"published_equals_rebuild"`
	PublishedBatch   bool           `json:"published_equals_batch"`
	OnlyPublished    []string       `json:"only_published"`
	OnlyRebuilt      []string       `json:"only_rebuilt"`
	Fires            []verifiedFire `json:"fires"`
	Database         string         `json:"database"`
}

// RunVerify compares what this replica published with a fresh replay of its own event log and with batch clustering.
func RunVerify(args []string, stdout, stderr io.Writer) int {
	path := os.Getenv("BNPB_SATELLITE_DB_PATH")
	if path == "" {
		path = defaultDBPath
	}
	if len(args) == 1 {
		path = args[0]
	} else if len(args) > 1 {
		_, _ = fmt.Fprintln(stderr, "usage: bnpb-satellite -verify [database path]")
		return 2
	}
	report, err := verifyDatabase(context.Background(), path)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "verify:", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return 1
	}
	if !report.PublishedRebuilt || !report.PublishedBatch {
		return 1
	}
	return 0
}

func verifyDatabase(ctx context.Context, path string) (verifyReport, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return verifyReport{}, err
	}
	defer db.Close()
	// One read transaction so the log, outbox and checkpoint describe the same moment.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return verifyReport{}, err
	}
	defer func() { _ = tx.Rollback() }()
	report := verifyReport{Database: path}
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE((SELECT next_offset FROM checkpoint WHERE id = 1), 0)").Scan(&report.NextOffset); err != nil {
		return report, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM outbox WHERE sent = 0").Scan(&report.Undelivered); err != nil {
		return report, err
	}
	published, err := publishedFires(ctx, tx)
	if err != nil {
		return report, err
	}
	engine, accepted := fire.NewEngine(), newAcceptedDetections()
	rebuilt := map[string]fire.Fire{}
	rows, err := tx.QueryContext(ctx, "SELECT offset_id, kind, body FROM events ORDER BY offset_id")
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var logged loggedEvent
		if err := rows.Scan(&logged.offset, &logged.kind, &logged.body); err != nil {
			rows.Close()
			return report, err
		}
		event, err := parseEvent(sseFrame{id: strconv.FormatInt(logged.offset, 10), event: logged.kind, data: logged.body})
		if err != nil {
			rows.Close()
			return report, fmt.Errorf("logged offset %d: %w", logged.offset, err)
		}
		outcome, err := applyToEngine(engine, event)
		if err != nil {
			rows.Close()
			return report, fmt.Errorf("logged offset %d: %w", logged.offset, err)
		}
		accepted.record(event, outcome)
		report.LoggedEvents++
		if report.LoggedEvents%flushEvery == 0 {
			drainActive(engine, rebuilt)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return report, err
	}
	drainActive(engine, rebuilt)
	report.Stats = engine.Stats()

	publishedKeys := map[string]string{}
	for id, published := range published {
		publishedKeys[id] = strings.Join(published.DetectionKeys, ",")
		report.Fires = append(report.Fires, published)
	}
	sort.Slice(report.Fires, func(i, j int) bool { return report.Fires[i].ID < report.Fires[j].ID })
	rebuiltKeys := map[string]string{}
	for id, change := range rebuilt {
		keys := append([]string(nil), change.DetectionKeys...)
		sort.Strings(keys)
		rebuiltKeys[id] = strings.Join(keys, ",")
	}
	for id, keys := range publishedKeys {
		if rebuiltKeys[id] != keys {
			report.OnlyPublished = append(report.OnlyPublished, id)
		}
	}
	for id, keys := range rebuiltKeys {
		if publishedKeys[id] != keys {
			report.OnlyRebuilt = append(report.OnlyRebuilt, id)
		}
	}
	sort.Strings(report.OnlyPublished)
	sort.Strings(report.OnlyRebuilt)
	report.Published, report.Rebuilt = len(published), len(rebuilt)
	report.PublishedRebuilt = len(report.OnlyPublished) == 0 && len(report.OnlyRebuilt) == 0

	var groups [][]string
	for _, published := range published {
		groups = append(groups, published.DetectionKeys)
	}
	sort.Slice(groups, func(i, j int) bool { return strings.Join(groups[i], ",") < strings.Join(groups[j], ",") })
	want := accepted.batch()
	report.Batch = len(want)
	report.PublishedBatch = reflect.DeepEqual(groups, want) || (len(groups) == 0 && len(want) == 0)
	return report, nil
}

// The newest outbox row per fire is what the Aggregator ends up with once delivery catches up.
func publishedFires(ctx context.Context, tx *sql.Tx) (map[string]verifiedFire, error) {
	rows, err := tx.QueryContext(ctx, `SELECT o.fire_id, o.record FROM outbox o
		JOIN (SELECT fire_id, max(seq) AS seq FROM outbox GROUP BY fire_id) latest ON latest.seq = o.seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fires := map[string]verifiedFire{}
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		var record fireRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, fmt.Errorf("outbox record for %s: %w", id, err)
		}
		if record.Active {
			keys := append([]string(nil), record.Attributes.DetectionKeys...)
			sort.Strings(keys)
			fires[id] = verifiedFire{ID: id, Severity: record.Severity, AreaName: record.AreaName,
				OccurredAt: record.OccurredAt.UTC().Format("2006-01-02T15:04:05Z"), FeedRevision: record.FeedRevision, DetectionKeys: keys}
		}
	}
	return fires, rows.Err()
}

func drainActive(engine *fire.Engine, active map[string]fire.Fire) {
	for _, change := range engine.Drain() {
		if change.Active {
			active[change.ID] = change
		} else {
			delete(active, change.ID)
		}
	}
}
