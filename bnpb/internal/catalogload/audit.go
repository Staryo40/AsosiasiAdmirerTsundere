package catalogload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type auditChecksum struct {
	Source           string   `json:"source"`
	Count            int64    `json:"count"`
	Checksum         string   `json:"checksum"`
	Algorithm        string   `json:"algorithm"`
	AsOf             string   `json:"as_of"`
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
}

var utcTimestamp = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|\+00:00)$`)

func readAuditChecksum(ctx context.Context, client *http.Client, base *url.URL, token string) (auditChecksum, error) {
	raw, err := getRaw(ctx, client, endpoint(base, "/internal/audit/checksum?source=BMKG"), token)
	if err != nil {
		return auditChecksum{}, err
	}
	return parseAuditChecksum(raw)
}

func parseAuditChecksum(raw []byte) (auditChecksum, error) {
	var audit auditChecksum
	var count *int64
	var partial *bool
	if err := decodeAuditEnvelope(raw, map[string]any{
		"source": &audit.Source, "count": &count, "checksum": &audit.Checksum,
		"algorithm": &audit.Algorithm, "as_of": &audit.AsOf,
		"partial": &partial, "unavailable_units": &audit.UnavailableUnits,
	}); err != nil {
		return auditChecksum{}, err
	}
	if partial == nil || *partial || audit.UnavailableUnits == nil || len(audit.UnavailableUnits) != 0 {
		return auditChecksum{}, errors.New("Audit checksum requires explicit partial=false and an empty unavailable_units array")
	}
	if count == nil || *count < 0 || audit.Source != "BMKG" || audit.Algorithm != "sha256" ||
		!validDigest(audit.Checksum) || strings.ToLower(audit.Checksum) != audit.Checksum ||
		(*count == 0) != (audit.Checksum == emptyPopulationChecksum) {
		return auditChecksum{}, errors.New("malformed BMKG Audit checksum metadata")
	}
	if _, err := time.Parse(time.RFC3339Nano, audit.AsOf); err != nil || !utcTimestamp.MatchString(audit.AsOf) {
		return auditChecksum{}, errors.New("Audit checksum as_of must be an ISO 8601 UTC timestamp")
	}
	audit.Count = *count
	return audit, nil
}

func decodeAuditEnvelope(raw []byte, fields map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return errors.New("Audit checksum envelope must be a JSON object")
	}
	for decoder.More() {
		token, err := decoder.Token()
		key, isKey := token.(string)
		field, found := fields[key]
		if err != nil || !isKey || !found || decoder.Decode(field) != nil {
			return errors.New("Audit checksum envelope contains an invalid, duplicate or unexpected field")
		}
		delete(fields, key)
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(fields) != 0 {
		return errors.New("Audit checksum envelope is incomplete")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after Audit checksum envelope")
	}
	return nil
}
