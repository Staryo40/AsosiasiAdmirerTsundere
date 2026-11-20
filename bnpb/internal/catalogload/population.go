package catalogload

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

type populationIdentity struct {
	Count    int64  `json:"count"`
	Checksum string `json:"checksum"`
}

const emptyPopulationChecksum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func expectedPopulation(snapshot io.Reader, baselinePath string) (populationIdentity, string, error) {
	refs := make(map[string]struct{})
	var baselineSHA256 string
	if baselinePath != "" {
		var err error
		baselineSHA256, err = readBaselineRefs(baselinePath, refs)
		if err != nil {
			return populationIdentity{}, "", fmt.Errorf("read baseline artifact: %w", err)
		}
	}
	if err := addArtifactRefs(snapshot, refs); err != nil {
		return populationIdentity{}, "", err
	}
	return checksumPopulation(refs), baselineSHA256, nil
}

func readBaselineRefs(path string, refs map[string]struct{}) (string, error) {
	file, _, err := openArtifact(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if err := addArtifactRefs(io.TeeReader(file, digest), refs); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func addArtifactRefs(artifact io.Reader, refs map[string]struct{}) error {
	rows := 0
	err := walkArtifact(artifact, func(record json.RawMessage) error {
		rows++
		ref, err := eventReference(record)
		if err != nil {
			return fmt.Errorf("artifact row %d: %w", rows, err)
		}
		// BMKG preserves event_id, and upserts one HazardEvent per (source, source_ref_id).
		refs[ref] = struct{}{}
		return nil
	})
	if err == nil && rows == 0 {
		return errors.New("artifact contains no seismic events")
	}
	return err
}

func eventReference(record json.RawMessage) (string, error) {
	var fields map[string]json.RawMessage
	var ref string
	if !utf8.Valid(record) || json.Unmarshal(record, &fields) != nil ||
		json.Unmarshal(fields["event_id"], &ref) != nil || strings.TrimSpace(ref) == "" ||
		len(ref) > 2048 || strings.ContainsFunc(ref, unicode.IsControl) {
		return "", errors.New("invalid BMKG event_id")
	}
	return ref, nil
}

func checksumPopulation(refs map[string]struct{}) populationIdentity {
	ordered := make([]string, 0, len(refs))
	for ref := range refs {
		ordered = append(ordered, ref)
	}
	slices.Sort(ordered)
	digest := sha256.New()
	for index, ref := range ordered {
		if index > 0 {
			_, _ = digest.Write([]byte{'\n'})
		}
		_, _ = digest.Write([]byte(ref))
	}
	return populationIdentity{Count: int64(len(ordered)), Checksum: hex.EncodeToString(digest.Sum(nil))}
}
