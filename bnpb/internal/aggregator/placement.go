package aggregator

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

var errPlacementChanged = errors.New("placement revision changed")
var errInvalidPlacement = errors.New("invalid shard placement")

type shardPlacement struct {
	ShardID int16  `json:"shard_id"`
	Unit    string `json:"unit"`
	Node    string `json:"node"`
	Role    string `json:"role"`
	State   string `json:"state"`
}

type placementSnapshot struct {
	Revision int64            `json:"revision"`
	Copies   []shardPlacement `json:"copies"`
}

func (store *Store) placement(ctx context.Context) (placementSnapshot, error) {
	var placement placementSnapshot
	err := store.snapshot(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT revision FROM placement_revision WHERE singleton").Scan(&placement.Revision); err != nil {
			return storeError("read placement revision", err)
		}
		var err error
		placement.Copies, err = readPlacementCopies(ctx, tx)
		return err
	})
	return placement, err
}

// Metadata admission only: this does not provision, promote, or route a PostgreSQL instance.
func (store *Store) recordPlacement(ctx context.Context, proposed placementSnapshot) (err error) {
	if err := validatePlacement(proposed.Copies); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, store.writeTimeout)
	defer cancel()
	tx, err := store.write.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return storeError("begin placement", err)
	}
	defer func() { err = errors.Join(err, rollbackTransaction(ctx, tx)) }()
	if err := recordPlacementTransaction(ctx, tx, proposed); err != nil {
		return err
	}
	return storeError("commit placement", tx.Commit(ctx))
}

func recordPlacementTransaction(ctx context.Context, tx pgx.Tx, proposed placementSnapshot) error {
	current, err := lockedPlacement(ctx, tx)
	if err != nil {
		return err
	}
	return recordPlacementChange(ctx, tx, proposed, current)
}

func recordPlacementChange(ctx context.Context, tx pgx.Tx, proposed, current placementSnapshot) error {
	if proposed.Revision != current.Revision {
		return errPlacementChanged
	}
	if !retainsPhysicalCopies(current.Copies, proposed.Copies) {
		return errInvalidPlacement
	}
	return writePlacementCopies(ctx, tx, proposed.Copies)
}

func lockedPlacement(ctx context.Context, tx pgx.Tx) (placementSnapshot, error) {
	var current placementSnapshot
	if err := tx.QueryRow(ctx, "SELECT revision FROM placement_revision WHERE singleton FOR UPDATE").Scan(&current.Revision); err != nil {
		return current, storeError("lock placement revision", err)
	}
	var err error
	current.Copies, err = readPlacementCopies(ctx, tx)
	return current, err
}

func readPlacementCopies(ctx context.Context, tx pgx.Tx) ([]shardPlacement, error) {
	rows, err := tx.Query(ctx, "SELECT shard_id, unit, node, role, state FROM shard_placements ORDER BY shard_id, unit COLLATE \"C\"")
	if err != nil {
		return nil, storeError("read placement copies", err)
	}
	defer rows.Close()
	copies := []shardPlacement{}
	for rows.Next() {
		var copy shardPlacement
		if err := rows.Scan(&copy.ShardID, &copy.Unit, &copy.Node, &copy.Role, &copy.State); err != nil {
			return nil, storeError("scan placement copy", err)
		}
		copies = append(copies, copy)
	}
	return copies, storeError("read placement copies", rows.Err())
}

func validatePlacement(copies []shardPlacement) error {
	units := make(map[string]bool, len(copies))
	nodes := make(map[string]uint8)
	var primaries [logicalShardCount]int
	for _, copy := range copies {
		if !validPlacementCopy(copy) || units[copy.Unit] {
			return errInvalidPlacement
		}
		units[copy.Unit] = true
		bit := uint8(1 << copy.ShardID)
		if nodes[copy.Node]&bit != 0 {
			return errInvalidPlacement
		}
		nodes[copy.Node] |= bit
		if nodes[copy.Node] == (1<<logicalShardCount)-1 {
			return errInvalidPlacement
		}
		if copy.Role == "primary" && copy.State == "ready" {
			primaries[copy.ShardID]++
		}
	}
	for _, count := range primaries {
		if count != 1 {
			return errInvalidPlacement
		}
	}
	return nil
}

func validPlacementCopy(copy shardPlacement) bool {
	return copy.ShardID >= 0 && copy.ShardID < logicalShardCount &&
		validText(copy.Unit) && utf8.ValidString(copy.Unit) && validText(copy.Node) && utf8.ValidString(copy.Node) &&
		(copy.Role == "primary" || copy.Role == "replica") &&
		(copy.State == "ready" || copy.State == "copying" || copy.State == "retiring") &&
		(copy.Role != "primary" || copy.State != "copying")
}

// A route change cannot prove physical erasure. Step 4 deliberately cannot forget old copies.
func retainsPhysicalCopies(previous, proposed []shardPlacement) bool {
	byUnit := make(map[string]shardPlacement, len(proposed))
	for _, copy := range proposed {
		byUnit[copy.Unit] = copy
	}
	for _, old := range previous {
		copy, exists := byUnit[old.Unit]
		if !exists || copy.ShardID != old.ShardID || copy.Node != old.Node {
			return false
		}
	}
	return true
}

func writePlacementCopies(ctx context.Context, tx pgx.Tx, copies []shardPlacement) error {
	// Demotions precede promotions to respect the unique ready-primary index.
	for _, primaryPass := range []bool{false, true} {
		for _, copy := range copies {
			if (copy.Role == "primary" && copy.State == "ready") != primaryPass {
				continue
			}
			_, err := tx.Exec(ctx, `INSERT INTO shard_placements (shard_id, unit, node, role, state)
                VALUES ($1, $2, $3, $4, $5) ON CONFLICT (unit) DO UPDATE
                SET role = EXCLUDED.role, state = EXCLUDED.state`, copy.ShardID, copy.Unit, copy.Node, copy.Role, copy.State)
			if err != nil {
				return storeError("record placement copy", err)
			}
		}
	}
	return nil
}
