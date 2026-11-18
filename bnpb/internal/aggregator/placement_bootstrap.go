package aggregator

// AI-assisted implementation; configuration does not provision or promote physical copies.
import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

const maxPlacementBootstrapBytes = 64 << 10

var errPlacementBootstrap = errors.New("BNPB_SHARD_PLACEMENT: invalid placement configuration")

func parsePlacementBootstrap(configured string) (*placementSnapshot, error) {
	if configured == "" {
		return nil, nil
	}
	if len(configured) > maxPlacementBootstrapBytes {
		return nil, errPlacementBootstrap
	}
	fields, err := parseObject(json.RawMessage(configured))
	if err != nil || !exactKeys(fields, []string{"revision", "copies"}) {
		return nil, errPlacementBootstrap
	}
	revision, err := required[int64](fields, "revision")
	if err != nil || revision < 0 {
		return nil, errPlacementBootstrap
	}
	copies, err := bootstrapCopies(fields)
	if err != nil {
		return nil, errPlacementBootstrap
	}
	return &placementSnapshot{Revision: revision, Copies: copies}, nil
}

func bootstrapCopies(fields rawObject) ([]shardPlacement, error) {
	rawCopies, err := required[[]json.RawMessage](fields, "copies")
	if err != nil {
		return nil, err
	}
	copies := make([]shardPlacement, 0, len(rawCopies))
	for _, raw := range rawCopies {
		copy, err := bootstrapCopy(raw)
		if err != nil {
			return nil, err
		}
		copies = append(copies, copy)
	}
	if err := validatePlacement(copies); err != nil {
		return nil, err
	}
	slices.SortFunc(copies, func(left, right shardPlacement) int {
		if left.ShardID < right.ShardID {
			return -1
		}
		if left.ShardID > right.ShardID {
			return 1
		}
		return strings.Compare(left.Unit, right.Unit)
	})
	return copies, nil
}

func bootstrapCopy(raw json.RawMessage) (shardPlacement, error) {
	fields, err := required[rawObject](rawObject{"copy": raw}, "copy")
	if err != nil || !exactKeys(fields, []string{"shard_id", "unit", "node", "role", "state"}) {
		return shardPlacement{}, errPlacementBootstrap
	}
	if _, err := required[int16](fields, "shard_id"); err != nil {
		return shardPlacement{}, errPlacementBootstrap
	}
	var copy shardPlacement
	if err := json.Unmarshal(raw, &copy); err != nil {
		return copy, errPlacementBootstrap
	}
	return copy, nil
}

func bootstrapPlacement(ctx context.Context, tx pgx.Tx, proposed placementSnapshot) error {
	current, err := lockedPlacement(ctx, tx)
	if err != nil {
		return err
	}
	// A restarted/competing replica may carry the already-applied configuration.
	// Never rewrite it or reset revision merely because the input revision is stale.
	if proposed.Revision <= current.Revision && slices.Equal(proposed.Copies, current.Copies) {
		return nil
	}
	if len(current.Copies) == 0 && !replicatedBootstrap(proposed.Copies) {
		return errInvalidPlacement
	}
	return recordPlacementChange(ctx, tx, proposed, current)
}

func replicatedBootstrap(copies []shardPlacement) bool {
	var ready [logicalShardCount]int
	for _, copy := range copies {
		if copy.State == "ready" {
			ready[copy.ShardID]++
		}
	}
	for _, count := range ready {
		if count < 2 {
			return false
		}
	}
	return true
}
