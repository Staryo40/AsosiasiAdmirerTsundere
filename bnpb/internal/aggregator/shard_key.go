package aggregator

import (
	"crypto/sha256"
	"encoding/binary"
	"unicode/utf8"
)

const logicalShardCount = 3

// Physical placement is deliberately absent from the identity hash.
func logicalShard(source Source, sourceRefID string) (int16, error) {
	if hazardTypeForSource(source) == "" {
		return 0, invalidField("source", InvalidValue)
	}
	if len(sourceRefID) > 2048 || !utf8.ValidString(sourceRefID) || !validText(sourceRefID) {
		return 0, invalidField("source_ref_id", InvalidValue)
	}
	// The longest source is SENSOR_PANTAI (13 bytes); the delimiter remains NUL.
	var key [13 + 1 + 2048]byte
	length := copy(key[:], source)
	length++
	length += copy(key[length:], sourceRefID)
	digest := sha256.Sum256(key[:length])
	return int16(binary.BigEndian.Uint32(digest[:4]) % logicalShardCount), nil
}
