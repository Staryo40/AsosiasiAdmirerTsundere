package fire

import (
	"sort"
	"strings"
)

// Batch clusters a final set of detections with union-find, independently of the incremental engine.
// Callers pass only detections that count: deduplicated, not low, not retracted, not late.
func Batch(detections []Detection) [][]string {
	nodes := make([]*node, len(detections))
	parent := make([]int, len(detections))
	index := map[cell][]int{}
	for position, detection := range detections {
		nodes[position] = &node{Detection: detection, minute: floorMinute(detection.Time)}
		parent[position] = position
		place := cellOf(detection.Latitude, detection.Longitude)
		index[place] = append(index[place], position)
	}
	var find func(int) int
	find = func(position int) int {
		for parent[position] != position {
			parent[position] = parent[parent[position]]
			position = parent[position]
		}
		return position
	}
	for position, current := range nodes {
		place := cellOf(current.Latitude, current.Longitude)
		for dLat := int32(-1); dLat <= 1; dLat++ {
			for dLon := int32(-1); dLon <= 1; dLon++ {
				for _, other := range index[cell{place.lat + dLat, place.lon + dLon}] {
					if other > position && linked(current, nodes[other]) {
						parent[find(other)] = find(position)
					}
				}
			}
		}
	}
	groups := map[int][]string{}
	for position, current := range nodes {
		root := find(position)
		groups[root] = append(groups[root], current.Key)
	}
	var fires [][]string
	for _, keys := range groups {
		if len(keys) >= 2 {
			sort.Strings(keys)
			fires = append(fires, keys)
		}
	}
	sort.Slice(fires, func(i, j int) bool { return strings.Join(fires[i], ",") < strings.Join(fires[j], ",") })
	return fires
}
