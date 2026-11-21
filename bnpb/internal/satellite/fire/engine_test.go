package fire

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

var start = time.Date(2026, 11, 28, 0, 0, 0, 0, time.UTC)

// Moves a point due east by the given distance; at these latitudes that is accurate to well under a metre.
func east(lat, lon, km float64) float64 {
	return lon + km/(earthRadiusKM*math.Cos(lat*math.Pi/180))*180/math.Pi
}

func detection(key string, minutes int, lat, lon float64, confidence string, frp float64) Detection {
	return Detection{Key: key, Satellite: strings.SplitN(key, "-", 2)[0], Time: start.Add(time.Duration(minutes) * time.Minute),
		Latitude: lat, Longitude: lon, Confidence: confidence, FRP: frp}
}

// Collects drained changes into the currently published fires, as the Aggregator would see them.
type published map[string]Fire

func (fires published) apply(changes []Fire) {
	for _, change := range changes {
		if change.Active {
			fires[change.ID] = change
		} else {
			delete(fires, change.ID)
		}
	}
}

func (fires published) memberships() [][]string {
	var groups [][]string
	for _, fire := range fires {
		groups = append(groups, fire.DetectionKeys)
	}
	sort.Slice(groups, func(i, j int) bool { return strings.Join(groups[i], ",") < strings.Join(groups[j], ",") })
	return groups
}

func drainInto(engine *Engine, fires published) published {
	fires.apply(engine.Drain())
	return fires
}

func TestChainedDetectionsFormOneFire(t *testing.T) {
	engine := NewEngine()
	lat, lon := -2.5, 113.8
	engine.AddDetection(detection("N20-a", 0, lat, lon, "n", 1))
	engine.AddDetection(detection("N20-c", 10, lat, east(lat, lon, 1.6), "n", 1))
	if fires := drainInto(engine, published{}); len(fires) != 0 || engine.Stats().Singletons != 2 {
		t.Fatalf("two detections 1.6 km apart are not a fire: %v", fires)
	}
	engine.AddDetection(detection("N21-b", 20, lat, east(lat, lon, 0.8), "h", 1))
	fires := drainInto(engine, published{})
	if len(fires) != 1 || engine.Stats().Singletons != 0 {
		t.Fatalf("A-B-C must chain into one fire, got %v", fires)
	}
	for _, fire := range fires {
		if fire.DetectionCount != 3 || !reflect.DeepEqual(fire.Satellites, []string{"N20", "N21"}) || fire.MaxConfidence != "h" ||
			!fire.OccurredAt.Equal(start) || fire.AreaName != "Kalimantan Tengah" {
			t.Fatalf("summary %+v", fire)
		}
	}
}

func TestLinkBoundariesAreInclusive(t *testing.T) {
	for name, test := range map[string]struct {
		km      float64
		minutes int
		linked  bool
	}{
		"0.999 km": {0.999, 0, true}, "1.001 km": {1.001, 0, false},
		"360 min": {0.1, 360, true}, "361 min": {0.1, 361, false},
	} {
		engine := NewEngine()
		engine.AddDetection(detection("N20-a", 0, 0.5, 110, "n", 1))
		engine.AddDetection(detection("N20-b", test.minutes, 0.5, east(0.5, 110, test.km), "n", 1))
		if got := len(drainInto(engine, published{})) == 1; got != test.linked {
			t.Errorf("%s: linked=%v want %v", name, got, test.linked)
		}
	}
}

func TestLowConfidenceAndDuplicatesNeverCount(t *testing.T) {
	engine := NewEngine()
	engine.AddDetection(detection("N20-a", 0, 0.5, 110, "n", 1))
	engine.AddDetection(detection("N20-b", 1, 0.5, 110.001, "l", 1))
	if engine.AddDetection(detection("N20-a", 0, 0.5, 110, "n", 1)) != Duplicate {
		t.Fatal("a repeated key is a duplicate")
	}
	if fires := drainInto(engine, published{}); len(fires) != 0 {
		t.Fatalf("a low-confidence neighbour must not form a fire: %v", fires)
	}
	if stats := engine.Stats(); stats.Duplicates != 1 || stats.Singletons != 1 {
		t.Fatalf("stats %+v", stats)
	}
}

func TestBridgeMergesAndRemovalSplitsKeepingLineage(t *testing.T) {
	engine := NewEngine()
	lat := 0.5
	west, eastSide := 110.0, east(lat, 110, 1.8)
	engine.AddDetection(detection("N20-w1", 0, lat, west, "n", 1))
	engine.AddDetection(detection("N20-w2", 1, lat, east(lat, west, 0.1), "n", 1))
	engine.AddDetection(detection("N21-e1", 2, lat, eastSide, "n", 1))
	engine.AddDetection(detection("N21-e2", 3, lat, east(lat, eastSide, 0.1), "n", 1))
	fires := drainInto(engine, published{})
	if len(fires) != 2 {
		t.Fatalf("expected two fires, got %d", len(fires))
	}
	westID, eastID := fireID("N20-w1"), fireID("N21-e1")
	survivor, absorbed := min(westID, eastID), max(westID, eastID)

	engine.AddDetection(detection("N20-bridge", 4, lat, east(lat, west, 0.95), "n", 1))
	changes := engine.Drain()
	fires.apply(changes)
	if len(fires) != 1 || fires[survivor].DetectionCount != 5 {
		t.Fatalf("the bridge must merge into the smaller ID: %v", fires)
	}
	if !hasWithdrawal(changes, absorbed) {
		t.Fatal("the absorbed fire must be withdrawn")
	}

	engine.Retract("N20-bridge")
	fires.apply(engine.Drain())
	if len(fires) != 2 || fires[westID].DetectionCount != 2 || fires[eastID].DetectionCount != 2 {
		t.Fatalf("removing the bridge must split back into both lineages: %v", fires)
	}
}

func hasWithdrawal(changes []Fire, id string) bool {
	for _, change := range changes {
		if change.ID == id && !change.Active {
			return true
		}
	}
	return false
}

func TestAFireShrinksAwayAndComesBackUnderTheSameID(t *testing.T) {
	engine := NewEngine()
	engine.AddDetection(detection("N20-a", 0, 0.5, 110, "n", 10))
	engine.AddDetection(detection("N20-b", 1, 0.5, 110.001, "n", 10))
	fires := drainInto(engine, published{})
	id := fireID("N20-a")
	low, normal := "l", "n"
	engine.Update("N20-b", &low, nil)
	changes := engine.Drain()
	fires.apply(changes)
	if len(fires) != 0 || !hasWithdrawal(changes, id) {
		t.Fatalf("a fire below two detections is withdrawn: %v", fires)
	}
	engine.Update("N20-b", &normal, nil)
	fires.apply(engine.Drain())
	if fires[id].DetectionCount != 2 {
		t.Fatalf("restoring confidence must revive the same fire: %v", fires)
	}
}

func TestUpdatesChangeTheExistingFire(t *testing.T) {
	engine := NewEngine()
	engine.AddDetection(detection("N20-a", 0, 0.5, 110, "n", 10))
	engine.AddDetection(detection("N20-b", 1, 0.5, 110.001, "n", 10.005))
	fires := drainInto(engine, published{})
	if fires[fireID("N20-a")].TotalFRP != 20.01 || fires[fireID("N20-a")].Severity != "WASPADA" {
		t.Fatalf("summary %+v", fires)
	}
	frp := 95.0
	engine.Update("N20-b", nil, &frp)
	fires.apply(engine.Drain())
	if len(fires) != 1 || fires[fireID("N20-a")].TotalFRP != 105 || fires[fireID("N20-a")].Severity != "AWAS" {
		t.Fatalf("an FRP update must change the same fire: %+v", fires)
	}
}

func TestSeverityThresholds(t *testing.T) {
	for _, test := range []struct {
		count int
		frp   float64
		want  string
	}{{2, 1, "WASPADA"}, {6, 1, "SIAGA"}, {2, 30, "SIAGA"}, {20, 1, "AWAS"}, {2, 100, "AWAS"}, {5, 29.99, "WASPADA"}} {
		if got := severity(test.count, test.frp); got != test.want {
			t.Errorf("%d detections, %.2f MW: %s want %s", test.count, test.frp, got, test.want)
		}
	}
}

// Real feed keys carry their own detection time, which is how an update to a forgotten detection is recognised.
func feedKey(satellite string, minutes int) string {
	at := start.Add(time.Duration(minutes) * time.Minute)
	return fmt.Sprintf("%s|%s|%d|0.5|110", satellite, at.Format("2006-01-02"), at.Hour()*100+at.Minute())
}

func TestLateDetectionsAndTouchesAreCountedNotClustered(t *testing.T) {
	engine := NewEngine()
	first, second, newest := feedKey("N20", 0), feedKey("N21", 1), feedKey("N20", 800)
	engine.AddDetection(detection(first, 0, 0.5, 110, "n", 1))
	engine.AddDetection(detection(newest, 800, 3, 100, "n", 1))
	if engine.AddDetection(detection(second, 1, 0.5, 110.001, "n", 1)) != Late {
		t.Fatal("a detection 799 minutes behind the newest is late")
	}
	low := "l"
	if engine.Update(first, &low, nil) != LateTouch {
		t.Fatal("an update to a detection that left memory is late")
	}
	if engine.Update(second, &low, nil) != LateTouch || engine.Retract("N99|2026-12-30|0|1|1") != Orphan {
		t.Fatal("late and unknown targets must be classified")
	}
	engine.Retract(newest)
	if engine.Retract(newest) != Ignored {
		t.Fatal("a second retract is ignored")
	}
	if fires := drainInto(engine, published{}); len(fires) != 0 {
		t.Fatalf("late data must not form fires: %v", fires)
	}
	if stats := engine.Stats(); stats.Late != 1 || stats.LateUpdates != 2 || stats.Orphans != 1 || stats.AfterRetract != 1 {
		t.Fatalf("stats %+v", stats)
	}
}

func TestOldStateLeavesMemoryAndItsFireStaysFinal(t *testing.T) {
	engine := NewEngine()
	engine.AddDetection(detection("N20-a", 0, 0.5, 110, "n", 1))
	engine.AddDetection(detection("N20-b", 1, 0.5, 110.001, "n", 1))
	fires := drainInto(engine, published{})
	engine.AddDetection(detection("N20-later", 722, 3, 100, "n", 1))
	if len(engine.nodes) != 1 || len(engine.fires) != 0 || len(drainInto(engine, fires)) != 1 {
		t.Fatalf("old detections must be evicted while the published fire stays: nodes=%d fires=%d", len(engine.nodes), len(engine.fires))
	}
}

func TestProvinceLookup(t *testing.T) {
	for _, test := range []struct {
		lat, lon float64
		want     string
	}{{-2.51234, 113.80450, "Kalimantan Tengah"}, {-1.0, 120.5, "Sulawesi Tengah"}, {-10.56, 120.70, "Nusa Tenggara Timur"}} {
		if got := nearestProvince(test.lat, test.lon); got != test.want {
			t.Errorf("%.2f,%.2f: %s want %s", test.lat, test.lon, got, test.want)
		}
	}
}

type modelDetection struct {
	Detection
	late, retracted bool
}

// Random streams in a small area, so fires constantly grow, merge, shrink and split.
func randomStream(seed uint64, events int) []func(*Engine, map[string]*modelDetection, *int) {
	random := rand.New(rand.NewPCG(seed, seed*7+1))
	var steps []func(*Engine, map[string]*modelDetection, *int)
	var keys []string
	sent := map[string]Detection{}
	clock := 0
	for index := range events {
		switch roll := random.IntN(10); {
		case roll < 6 || len(keys) == 0:
			clock += random.IntN(40)
			when := clock - random.IntN(300)
			if random.IntN(25) == 0 {
				when = clock - 400 - random.IntN(300)
			}
			conf := []string{"l", "n", "n", "h"}[random.IntN(4)]
			found := detection(fmt.Sprintf("N%d-%d", 20+random.IntN(2), index), when, random.Float64()*0.05, 110+random.Float64()*0.05, conf, float64(random.IntN(2000))/100)
			// The feed repeats a detection byte for byte; its key embeds the time, so a repeat never moves.
			if random.IntN(20) == 0 && len(keys) > 0 {
				found = sent[keys[random.IntN(len(keys))]]
			}
			sent[found.Key] = found
			keys = append(keys, found.Key)
			steps = append(steps, func(engine *Engine, model map[string]*modelDetection, newest *int) {
				minute := int(floorMinute(found.Time) - floorMinute(start))
				if model[found.Key] != nil {
					engine.AddDetection(found)
					return
				}
				late := *newest != math.MinInt && minute < *newest-linkMinutes
				model[found.Key] = &modelDetection{Detection: found, late: late}
				if !late && minute > *newest {
					*newest = minute
				}
				engine.AddDetection(found)
			})
		default:
			key := keys[random.IntN(len(keys))]
			retract := random.IntN(3) == 0
			conf := []string{"l", "n", "h"}[random.IntN(3)]
			frp := float64(random.IntN(2000)) / 100
			steps = append(steps, func(engine *Engine, model map[string]*modelDetection, newest *int) {
				target := model[key]
				tooLate := target == nil || target.late || int(floorMinute(target.Time)-floorMinute(start)) < *newest-linkMinutes
				if !tooLate && !target.retracted {
					if retract {
						target.retracted = true
					} else {
						target.Confidence, target.FRP = conf, frp
					}
				}
				if retract {
					engine.Retract(key)
				} else {
					engine.Update(key, &conf, &frp)
				}
			})
		}
	}
	return steps
}

func runStream(steps []func(*Engine, map[string]*modelDetection, *int), drainEvery int) (published, map[string]*modelDetection, [][]Fire) {
	engine, model, newest := NewEngine(), map[string]*modelDetection{}, math.MinInt
	fires := published{}
	var drains [][]Fire
	for index, step := range steps {
		step(engine, model, &newest)
		if index%drainEvery == 0 {
			changes := engine.Drain()
			drains = append(drains, changes)
			fires.apply(changes)
		}
	}
	changes := engine.Drain()
	fires.apply(changes)
	return fires, model, append(drains, changes)
}

func TestIncrementalFiresEqualBatchClustering(t *testing.T) {
	for seed := range uint64(300) {
		steps := randomStream(seed, 400)
		// Revivals and withdrawals inside one drain window are where publication can go stale.
		fires, model, _ := runStream(steps, []int{1, 7, 50}[seed%3])
		var final []Detection
		for _, candidate := range model {
			if !candidate.late && !candidate.retracted && candidate.Confidence != "l" {
				final = append(final, candidate.Detection)
			}
		}
		if want, got := Batch(final), fires.memberships(); !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: incremental fires differ from batch clustering\n only incremental %v\n only batch       %v",
				seed, missingFrom(got, want), missingFrom(want, got))
		}
		for _, fire := range fires {
			if fire.DetectionCount != len(fire.DetectionKeys) || fire.Severity != severity(fire.DetectionCount, fire.TotalFRP) {
				t.Fatalf("seed %d: inconsistent summary %+v", seed, fire)
			}
		}
	}
}

func missingFrom(groups, other [][]string) []string {
	known := map[string]bool{}
	for _, group := range other {
		known[strings.Join(group, ",")] = true
	}
	var missing []string
	for _, group := range groups {
		if joined := strings.Join(group, ","); !known[joined] {
			missing = append(missing, joined)
		}
	}
	return missing
}

func TestReplayingTheSameStreamGivesIdenticalChanges(t *testing.T) {
	for seed := range uint64(50) {
		_, _, first := runStream(randomStream(seed, 400), 7)
		_, _, second := runStream(randomStream(seed, 400), 7)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("seed %d: two runs over the same stream disagree", seed)
		}
	}
}
