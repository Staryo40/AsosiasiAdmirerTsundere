// Package fire recognises fires from hotspot detections, following the spec's Definisi Kebakaran.
// Every decision depends only on the order of calls, so replaying the same events rebuilds the same fires.
package fire

import (
	"container/heap"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Detections older than the newest detection minus this can no longer link or be updated, so they leave memory.
const retainMinutes = 2 * linkMinutes

type Detection struct {
	Key        string
	Satellite  string
	Time       time.Time
	Latitude   float64
	Longitude  float64
	Confidence string
	FRP        float64
}

type Outcome string

const (
	Added     Outcome = "added"
	Duplicate Outcome = "duplicate"
	Late      Outcome = "late"
	Applied   Outcome = "applied"
	Orphan    Outcome = "orphan"
	LateTouch Outcome = "late_update"
	Ignored   Outcome = "after_retract"
)

type node struct {
	Detection
	minute    int64
	retracted bool
	inGrid    bool
	fire      *fireState
}

func (n *node) clustered() bool { return !n.retracted && n.Confidence != "l" }

func earlier(a, b *node) bool {
	if a.minute != b.minute {
		return a.minute < b.minute
	}
	return a.Key < b.Key
}

type fireState struct {
	id        string
	anchor    string
	members   map[string]*node
	maxMinute int64
	emitted   bool
	last      Fire
}

type Stats struct {
	Detections, Duplicates, Late, LateUpdates, Orphans, AfterRetract int64
	Updates, Retracts, Singletons, ActiveFires                       int64
	Watermark                                                        time.Time
}

type Engine struct {
	nodes        map[string]*node
	grid         map[cell]map[string]*node
	fires        map[string]*fireState
	late         map[string]bool
	buckets      map[int64][]*node
	minutes      minuteHeap
	maxMinute    int64
	hasMax       bool
	evictedBelow int64
	dirty        map[string]*fireState
	retired      map[string]Fire
	stats        Stats
}

func NewEngine() *Engine {
	return &Engine{nodes: map[string]*node{}, grid: map[cell]map[string]*node{}, fires: map[string]*fireState{},
		late: map[string]bool{}, buckets: map[int64][]*node{}, dirty: map[string]*fireState{}, retired: map[string]Fire{}}
}

func (engine *Engine) AddDetection(detection Detection) Outcome {
	if engine.nodes[detection.Key] != nil || engine.late[detection.Key] {
		engine.stats.Duplicates++
		return Duplicate
	}
	minute := floorMinute(detection.Time)
	if engine.hasMax && minute < engine.maxMinute-linkMinutes {
		engine.late[detection.Key] = true
		engine.stats.Late++
		return Late
	}
	engine.stats.Detections++
	current := &node{Detection: detection, minute: minute}
	engine.nodes[detection.Key] = current
	if engine.buckets[minute] == nil {
		heap.Push(&engine.minutes, minute)
	}
	engine.buckets[minute] = append(engine.buckets[minute], current)
	if current.clustered() {
		engine.insert(current)
	}
	if !engine.hasMax || minute > engine.maxMinute {
		engine.maxMinute, engine.hasMax = minute, true
		engine.evict()
	}
	return Added
}

func (engine *Engine) Update(key string, confidence *string, frp *float64) Outcome {
	current, outcome := engine.lookup(key)
	if current == nil {
		return outcome
	}
	engine.stats.Updates++
	was := current.clustered()
	if frp != nil {
		current.FRP = *frp
		if current.fire != nil {
			engine.dirty[current.fire.id] = current.fire
		}
	}
	if confidence != nil {
		current.Confidence = *confidence
	}
	switch now := current.clustered(); {
	case was && !now:
		engine.remove(current)
	case !was && now:
		engine.insert(current)
	}
	return Applied
}

func (engine *Engine) Retract(key string) Outcome {
	current, outcome := engine.lookup(key)
	if current == nil {
		return outcome
	}
	engine.stats.Retracts++
	was := current.clustered()
	current.retracted = true
	if was {
		engine.remove(current)
	}
	return Applied
}

func (engine *Engine) lookup(key string) (*node, Outcome) {
	current := engine.nodes[key]
	switch {
	case current != nil && current.retracted:
		engine.stats.AfterRetract++
		return nil, Ignored
	// The same lateness bound as detections; it also keeps every possible neighbour of a change in the grid.
	case current != nil && current.minute < engine.maxMinute-linkMinutes:
		engine.stats.LateUpdates++
		return nil, LateTouch
	case current != nil:
		return current, Applied
	case engine.late[key]:
		engine.stats.LateUpdates++
		return nil, LateTouch
	}
	if minute, ok := keyMinute(key); ok && minute < engine.evictedBelow {
		engine.stats.LateUpdates++
		return nil, LateTouch
	}
	engine.stats.Orphans++
	return nil, Orphan
}

func (engine *Engine) insert(current *node) {
	place := cellOf(current.Latitude, current.Longitude)
	if engine.grid[place] == nil {
		engine.grid[place] = map[string]*node{}
	}
	engine.grid[place][current.Key] = current
	current.inGrid = true
	engine.stats.Singletons++
	neighbours := engine.neighbours(current)
	if len(neighbours) == 0 {
		return
	}
	var survivor *fireState
	var singles []*node
	merged := map[string]*fireState{}
	for _, neighbour := range neighbours {
		if neighbour.fire == nil {
			singles = append(singles, neighbour)
			continue
		}
		merged[neighbour.fire.id] = neighbour.fire
		if survivor == nil || neighbour.fire.id < survivor.id {
			survivor = neighbour.fire
		}
	}
	if survivor == nil {
		engine.newFire(append(singles, current))
		return
	}
	for id, other := range merged {
		if id == survivor.id {
			continue
		}
		for _, member := range other.members {
			engine.join(survivor, member)
		}
		engine.retire(other)
	}
	for _, member := range append(singles, current) {
		engine.join(survivor, member)
	}
	engine.dirty[survivor.id] = survivor
}

func (engine *Engine) neighbours(current *node) []*node {
	var found []*node
	place := cellOf(current.Latitude, current.Longitude)
	for dLat := int32(-1); dLat <= 1; dLat++ {
		for dLon := int32(-1); dLon <= 1; dLon++ {
			for _, candidate := range engine.grid[cell{place.lat + dLat, place.lon + dLon}] {
				if candidate != current && linked(candidate, current) {
					found = append(found, candidate)
				}
			}
		}
	}
	return found
}

func (engine *Engine) join(target *fireState, member *node) {
	if member.fire == nil {
		engine.stats.Singletons--
	} else if member.fire != target {
		delete(member.fire.members, member.Key)
	}
	member.fire = target
	target.members[member.Key] = member
	target.maxMinute = max(target.maxMinute, member.minute)
}

func (engine *Engine) newFire(members []*node) *fireState {
	anchor := members[0]
	for _, member := range members[1:] {
		if earlier(member, anchor) {
			anchor = member
		}
	}
	// A fire keeps its ID after its anchor leaves, so that detection can later found a second fire with the same hash.
	id := fireID(anchor.Key)
	for attempt := 1; engine.fires[id] != nil; attempt++ {
		id = fireID(anchor.Key + "#" + strconv.Itoa(attempt))
	}
	created := &fireState{id: id, anchor: anchor.Key, members: map[string]*node{}, maxMinute: anchor.minute}
	// An ID can come back after a merge or split retired it; the fire is then the same lineage again.
	// Consumers still hold the version published before, so a later retirement must still withdraw it.
	if withdrawn, pending := engine.retired[id]; pending {
		created.emitted, created.last = true, withdrawn
		delete(engine.retired, id)
	}
	engine.fires[created.id] = created
	for _, member := range members {
		engine.join(created, member)
	}
	engine.dirty[created.id] = created
	return created
}

func (engine *Engine) retire(gone *fireState) {
	delete(engine.fires, gone.id)
	delete(engine.dirty, gone.id)
	if gone.emitted {
		withdrawn := gone.last
		withdrawn.Active = false
		engine.retired[gone.id] = withdrawn
	}
}

func (engine *Engine) remove(current *node) {
	if current.inGrid {
		place := cellOf(current.Latitude, current.Longitude)
		delete(engine.grid[place], current.Key)
		if len(engine.grid[place]) == 0 {
			delete(engine.grid, place)
		}
		current.inGrid = false
	}
	owner := current.fire
	if owner == nil {
		engine.stats.Singletons--
		return
	}
	delete(owner.members, current.Key)
	current.fire = nil
	engine.split(owner)
}

// The piece holding the anchor keeps the ID; if the anchor left, the piece with the earliest member does.
func (engine *Engine) split(owner *fireState) {
	if len(owner.members) == 0 {
		engine.retire(owner)
		return
	}
	pieces := components(owner.members)
	keeper := 0
	if anchor := owner.members[owner.anchor]; anchor != nil {
		keeper = pieceOf(pieces, anchor)
	} else {
		earliest := (*node)(nil)
		for _, member := range owner.members {
			if earliest == nil || earlier(member, earliest) {
				earliest = member
			}
		}
		keeper, owner.anchor = pieceOf(pieces, earliest), earliest.Key
	}
	kept := pieces[keeper]
	owner.members, owner.maxMinute = map[string]*node{}, 0
	for _, member := range kept {
		owner.members[member.Key] = member
		owner.maxMinute = max(owner.maxMinute, member.minute)
	}
	if len(kept) < 2 {
		engine.retire(owner)
		engine.release(kept)
	} else {
		engine.dirty[owner.id] = owner
	}
	for index, piece := range pieces {
		switch {
		case index == keeper:
		case len(piece) >= 2:
			for _, member := range piece {
				member.fire = nil
				engine.stats.Singletons++
			}
			engine.newFire(piece)
		default:
			engine.release(piece)
		}
	}
}

func (engine *Engine) release(members []*node) {
	for _, member := range members {
		member.fire = nil
		engine.stats.Singletons++
	}
}

func pieceOf(pieces [][]*node, wanted *node) int {
	for index, piece := range pieces {
		for _, member := range piece {
			if member == wanted {
				return index
			}
		}
	}
	return 0
}

// Connected pieces of one fire's members, using a local grid so detections that left memory still link.
func components(members map[string]*node) [][]*node {
	local := map[cell][]*node{}
	keys := make([]string, 0, len(members))
	for key, member := range members {
		place := cellOf(member.Latitude, member.Longitude)
		local[place] = append(local[place], member)
		keys = append(keys, key)
	}
	sort.Strings(keys)
	seen := map[*node]bool{}
	var pieces [][]*node
	for _, key := range keys {
		start := members[key]
		if seen[start] {
			continue
		}
		seen[start] = true
		piece, queue := []*node{start}, []*node{start}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			place := cellOf(current.Latitude, current.Longitude)
			for dLat := int32(-1); dLat <= 1; dLat++ {
				for dLon := int32(-1); dLon <= 1; dLon++ {
					for _, candidate := range local[cell{place.lat + dLat, place.lon + dLon}] {
						if !seen[candidate] && linked(candidate, current) {
							seen[candidate] = true
							piece, queue = append(piece, candidate), append(queue, candidate)
						}
					}
				}
			}
		}
		pieces = append(pieces, piece)
	}
	return pieces
}

// A fire whose newest member left memory can no longer change, so its state is dropped after it is drained.
func (engine *Engine) evict() {
	horizon := engine.maxMinute - retainMinutes
	for engine.minutes.Len() > 0 && engine.minutes[0] < horizon {
		minute := heap.Pop(&engine.minutes).(int64)
		for _, old := range engine.buckets[minute] {
			delete(engine.nodes, old.Key)
			if old.inGrid {
				place := cellOf(old.Latitude, old.Longitude)
				delete(engine.grid[place], old.Key)
				if len(engine.grid[place]) == 0 {
					delete(engine.grid, place)
				}
				old.inGrid = false
			}
			if old.fire != nil && old.fire.maxMinute < horizon {
				delete(engine.fires, old.fire.id)
			}
		}
		delete(engine.buckets, minute)
	}
	engine.evictedBelow = max(engine.evictedBelow, horizon)
}

func (engine *Engine) Stats() Stats {
	stats := engine.stats
	stats.ActiveFires = int64(len(engine.fires))
	if engine.hasMax {
		stats.Watermark = time.Unix((engine.maxMinute-linkMinutes)*60, 0).UTC()
	}
	return stats
}

// Changed and withdrawn fires since the last drain, sorted by ID.
func (engine *Engine) Drain() []Fire {
	changes := make([]Fire, 0, len(engine.dirty)+len(engine.retired))
	for _, changed := range engine.dirty {
		changed.last, changed.emitted = summarize(changed), true
		changes = append(changes, changed.last)
	}
	for _, withdrawn := range engine.retired {
		changes = append(changes, withdrawn)
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].ID < changes[j].ID })
	engine.dirty, engine.retired = map[string]*fireState{}, map[string]Fire{}
	return changes
}

func floorMinute(at time.Time) int64 {
	seconds := at.Unix()
	if seconds < 0 && seconds%60 != 0 {
		return seconds/60 - 1
	}
	return seconds / 60
}

// Keys are satellite|acq_date|acq_time|latitude|longitude as the feed sends them.
func keyMinute(key string) (int64, bool) {
	parts := strings.Split(key, "|")
	if len(parts) != 5 {
		return 0, false
	}
	day, err := time.Parse("2006-01-02", parts[1])
	if err != nil {
		return 0, false
	}
	var clock int
	for _, digit := range parts[2] {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		clock = clock*10 + int(digit-'0')
	}
	return floorMinute(day.Add(time.Duration(clock/100)*time.Hour + time.Duration(clock%100)*time.Minute)), true
}

type minuteHeap []int64

func (h minuteHeap) Len() int           { return len(h) }
func (h minuteHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h minuteHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minuteHeap) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *minuteHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}
