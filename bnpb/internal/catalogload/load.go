package catalogload

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	maxRecordBytes       = 128 << 10
	batchRecords         = 20
	maxWireBodyBytes     = 4 << 20
	batchEnvelopeReserve = 1 << 10
	maxBatchRecordBytes  = maxWireBodyBytes - batchEnvelopeReserve
)

type Options struct {
	Artifact, BaselineArtifact, BaseURL, TokenFile, AuditTokenFile string
	ExpectedChecksum, ResourcesManifest, RunOutput                 string
	Client                                                         *http.Client
}

type resourceManifest struct {
	ReferenceChecksum string                     `json:"reference_checksum"`
	Resources         map[string]json.RawMessage `json:"resources"`
}

type Report struct {
	Artifact               string             `json:"artifact"`
	ArtifactSHA256         string             `json:"artifact_sha256"`
	InputFormat            string             `json:"input_format"`
	Rows                   int64              `json:"rows"`
	BaselineArtifactSHA256 string             `json:"baseline_artifact_sha256,omitempty"`
	ExpectedPopulation     populationIdentity `json:"expected_population"`
	AuditBefore            auditChecksum      `json:"audit_before"`
	AuditAfter             auditChecksum      `json:"audit_after"`
	ReferenceChecksum      string             `json:"reference_checksum"`
	Resources              json.RawMessage    `json:"resources_manifest"`
	MeasurementsBefore     json.RawMessage    `json:"measurements_before"`
	MeasurementsAfter      json.RawMessage    `json:"measurements_after"`
	SubmissionStartedAt    time.Time          `json:"submission_started_at"`
	SubmissionFinishedAt   time.Time          `json:"submission_finished_at"`
	DatasetAvailable       bool               `json:"dataset_available"`
}

type batchRequest struct {
	InputID     string            `json:"input_id"`
	InputOffset int               `json:"input_offset"`
	ReceivedAt  time.Time         `json:"received_at"`
	Records     []json.RawMessage `json:"records"`
}

func Run(ctx context.Context, options Options) (Report, error) {
	var report Report
	if options.Artifact == "" || options.BaseURL == "" || options.TokenFile == "" || options.AuditTokenFile == "" || options.RunOutput == "" {
		return report, errors.New("artifact, base URL, catalog token file, audit token file, and run output are required")
	}
	manifest, manifestBytes, err := readManifest(options.ResourcesManifest)
	if err != nil {
		return report, err
	}
	reference := strings.ToLower(strings.TrimSpace(options.ExpectedChecksum))
	manifestReference := strings.ToLower(strings.TrimSpace(manifest.ReferenceChecksum))
	if reference != "" && manifestReference != "" && reference != manifestReference {
		return report, errors.New("command-line checksum disagrees with resources manifest")
	}
	if reference == "" {
		reference = manifestReference
	}
	if !validDigest(reference) {
		return report, errors.New("a 64-character reference checksum is required")
	}
	catalogToken, err := readToken(options.TokenFile)
	if err != nil {
		return report, fmt.Errorf("read catalog token: %w", err)
	}
	auditToken, err := readToken(options.AuditTokenFile)
	if err != nil {
		return report, fmt.Errorf("read audit token: %w", err)
	}
	if catalogToken == auditToken {
		return report, errors.New("catalog and Audit credentials must be distinct")
	}
	snapshot, err := os.CreateTemp("", "bnpb-catalog-*")
	if err != nil {
		return report, fmt.Errorf("create artifact snapshot: %w", err)
	}
	defer os.Remove(snapshot.Name())
	defer snapshot.Close()
	artifactSHA256, err := snapshotArtifact(options.Artifact, snapshot)
	if err != nil {
		return report, err
	}
	expected, baselineSHA256, err := expectedPopulation(snapshot, options.BaselineArtifact)
	if err != nil {
		return report, err
	}
	if expected.Checksum != reference {
		return report, errors.New("expected artifact population checksum does not match supplied reference")
	}
	if _, err := snapshot.Seek(0, io.SeekStart); err != nil {
		return report, err
	}
	client := options.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	base, err := url.Parse(options.BaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") {
		return report, errors.New("invalid aggregator base URL")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	base.RawQuery, base.Fragment = "", ""
	report = Report{
		Artifact: options.Artifact, ArtifactSHA256: artifactSHA256,
		InputFormat:            "JSONL or JSON array of raw seismic-event objects",
		BaselineArtifactSHA256: baselineSHA256, ExpectedPopulation: expected,
		ReferenceChecksum: reference, Resources: manifestBytes,
	}
	before, err := readAuditChecksum(ctx, client, base, auditToken)
	if err != nil {
		return report, fmt.Errorf("read complete pre-load Audit checksum: %w", err)
	}
	report.AuditBefore = before
	measureBefore, err := getRaw(ctx, client, endpoint(base, "/internal/storage/measurements"), catalogToken)
	if err != nil {
		return report, fmt.Errorf("pre-load measurements unavailable: %w", err)
	}
	if isPartial(measureBefore) {
		return report, errors.New("pre-load measurements are partial")
	}
	report.MeasurementsBefore = measureBefore
	report.SubmissionStartedAt = time.Now().UTC()
	err = loadArtifact(ctx, snapshot, client, base, catalogToken, report.ArtifactSHA256, &report.Rows)
	report.SubmissionFinishedAt = time.Now().UTC()
	if err != nil {
		return report, err
	}
	after, err := readAuditChecksum(ctx, client, base, auditToken)
	if err != nil {
		return report, fmt.Errorf("read complete post-load Audit checksum: %w", err)
	}
	report.AuditAfter = after
	if after.Count != expected.Count || after.Checksum != expected.Checksum {
		return report, fmt.Errorf("post-load Audit population does not match expected artifact count and checksum (count %d, expected %d)",
			after.Count, expected.Count)
	}
	measureAfter, err := getRaw(ctx, client, endpoint(base, "/internal/storage/measurements"), catalogToken)
	if err != nil {
		return report, fmt.Errorf("post-load measurements unavailable: %w", err)
	}
	if isPartial(measureAfter) {
		return report, errors.New("post-load measurements are partial")
	}
	report.MeasurementsAfter = measureAfter
	report.DatasetAvailable = true
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, err
	}
	if err := os.WriteFile(options.RunOutput, append(encoded, '\n'), 0600); err != nil {
		return report, err
	}
	return report, nil
}

func snapshotArtifact(path string, snapshot *os.File) (string, error) {
	source, info, err := openArtifact(path)
	if err != nil {
		return "", err
	}
	defer source.Close()
	hash := sha256.New()
	// Copy at most one byte past the original size to detect growth as well as truncation.
	copied, err := io.Copy(io.MultiWriter(snapshot, hash), io.LimitReader(source, info.Size()+1))
	if err != nil {
		return "", err
	}
	if copied != info.Size() {
		return "", errors.New("artifact changed while computing its checksum")
	}
	if _, err := snapshot.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func openArtifact(path string) (*os.File, os.FileInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err == nil && info.Mode().IsRegular() {
		return file, info, nil
	}
	file.Close()
	if err != nil {
		return nil, nil, err
	}
	return nil, nil, errors.New("artifact must be a regular file")
}

func readManifest(path string) (resourceManifest, json.RawMessage, error) {
	if path == "" {
		return resourceManifest{}, nil, errors.New("resources manifest is required")
	}
	manifestBytes, err := readBoundedFile(path, 1<<20)
	if err != nil {
		return resourceManifest{}, nil, err
	}
	var manifest resourceManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return manifest, nil, fmt.Errorf("decode resources manifest: %w", err)
	}
	if len(manifest.Resources) == 0 {
		return manifest, nil, errors.New("resources manifest must record measured resource metadata")
	}
	return manifest, append(json.RawMessage(nil), manifestBytes...), nil
}

func readToken(path string) (string, error) {
	tokenBytes, err := readBoundedFile(path, 4096)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", errors.New("token file must contain one nonempty token")
	}
	return token, nil
}

func readBoundedFile(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(contents)) > maximum {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, maximum)
	}
	return contents, nil
}
func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func endpoint(base *url.URL, path string) string {
	copy := *base
	suffix, err := url.Parse(path)
	if err != nil {
		return ""
	}
	copy.Path += suffix.Path
	copy.RawQuery = suffix.RawQuery
	return copy.String()
}

func loadArtifact(ctx context.Context, file io.Reader, client *http.Client, base *url.URL, token, inputID string, total *int64) error {
	batch := make([]json.RawMessage, 0, batchRecords)
	batchBytes := 0
	flush := func() error {
		if err := sendBatch(ctx, client, base, token, inputID, *total, batch); err != nil {
			return err
		}
		*total += int64(len(batch))
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	if err := walkArtifact(file, func(record json.RawMessage) error {
		recordBytes := conservativeRecordBytes(record)
		if len(batch) > 0 && batchBytes+1+recordBytes > maxBatchRecordBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		if len(batch) > 0 {
			batchBytes++
		}
		batch = append(batch, record)
		batchBytes += recordBytes
		if len(batch) == batchRecords {
			return flush()
		}
		return nil
	}); err != nil {
		return err
	}
	if len(batch) > 0 {
		return flush()
	}
	if *total == 0 {
		return errors.New("artifact contains no seismic events")
	}
	return nil
}

func walkArtifact(file io.Reader, visit func(json.RawMessage) error) error {
	br := bufio.NewReaderSize(file, 64<<10)
	first, err := firstNonSpace(br)
	if errors.Is(err, io.EOF) {
		return errors.New("artifact contains no seismic events")
	}
	if err != nil {
		return err
	}
	if first == '[' {
		return walkArray(br, visit)
	}
	if first != '{' {
		return errors.New("unsupported artifact format; only JSONL or JSON arrays of raw seismic-event objects are accepted (CSV is unsupported)")
	}
	return walkJSONL(br, visit)
}

func firstNonSpace(reader *bufio.Reader) (byte, error) {
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
			if err := reader.UnreadByte(); err != nil {
				return 0, err
			}
			return b, nil
		}
	}
}

func walkJSONL(br *bufio.Reader, visit func(json.RawMessage) error) error {
	for {
		line, err := readBoundedLine(br)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			if line[0] != '{' {
				return errors.New("JSONL input must contain one JSON object per line")
			}
			if err := visit(line); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func readBoundedLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > maxRecordBytes {
			return nil, errors.New("JSONL record exceeds 128 KiB")
		}
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

// RawMessage encoding can expand each input byte to at most six JSON bytes
// (for example, an escaped control character); reserve that upper bound.
func conservativeRecordBytes(record json.RawMessage) int {
	if len(record) > maxBatchRecordBytes/6 {
		return maxBatchRecordBytes + 1
	}
	return len(record) * 6
}

func walkArray(reader *bufio.Reader, visit func(json.RawMessage) error) error {
	decoder := json.NewDecoder(reader)
	tokenValue, err := decoder.Token()
	if err != nil || tokenValue != json.Delim('[') {
		return errors.New("invalid JSON array")
	}
	for decoder.More() {
		var record json.RawMessage
		if err := decoder.Decode(&record); err != nil {
			return fmt.Errorf("decode JSON array record: %w", err)
		}
		if len(record) == 0 || len(record) > maxRecordBytes || record[0] != '{' {
			return errors.New("JSON array must contain objects no larger than 128 KiB")
		}
		if err := visit(record); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after JSON array")
	}
	return nil
}

func sendBatch(ctx context.Context, client *http.Client, base *url.URL, token, inputID string, offset int64, records []json.RawMessage) error {
	if offset > int64(^uint(0)>>1) {
		return errors.New("artifact row offset exceeds platform integer range")
	}
	bodyEstimate := batchEnvelopeReserve
	for index, record := range records {
		if index > 0 {
			bodyEstimate++
		}
		recordBytes := conservativeRecordBytes(record)
		if recordBytes > maxWireBodyBytes-bodyEstimate {
			return errors.New("catalog batch exceeds request limit")
		}
		bodyEstimate += recordBytes
	}
	body, err := json.Marshal(batchRequest{InputID: inputID, InputOffset: int(offset), ReceivedAt: time.Now().UTC(), Records: records})
	if err != nil {
		return err
	}
	if len(body) > maxWireBodyBytes {
		return errors.New("catalog batch exceeds request limit")
	}
	requestURL := endpoint(base, "/internal/catalog/seismic")
	var lastErr error
	for attempt := range 5 {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err == nil {
			err = checkBatchResponse(response, inputID, int(offset), len(records))
		}
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < 4 {
			timer := time.NewTimer(time.Duration(attempt+1) * 250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return fmt.Errorf("catalog batch failed after bounded retries: %w", lastErr)
}

func checkBatchResponse(response *http.Response, inputID string, offset, count int) error {
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(payload) > 1<<20 {
		return errors.New("catalog response exceeds 1 MiB")
	}
	if response.StatusCode == http.StatusOK {
		var acknowledgement struct {
			Accepted    int    `json:"accepted"`
			InputID     string `json:"input_id"`
			InputOffset int    `json:"input_offset"`
		}
		if json.Unmarshal(payload, &acknowledgement) != nil || acknowledgement.Accepted != count || acknowledgement.InputID != inputID || acknowledgement.InputOffset != offset {
			return errors.New("catalog response did not confirm durable batch identity")
		}
		return nil
	}
	if response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests {
		return fmt.Errorf("catalog API rejected batch with HTTP %d", response.StatusCode)
	}
	return fmt.Errorf("catalog API returned retryable HTTP %d", response.StatusCode)
}

func getRaw(ctx context.Context, client *http.Client, address, token string) (json.RawMessage, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 4<<20 {
		return nil, errors.New("aggregator response exceeds 4 MiB")
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("aggregator API returned HTTP %d", response.StatusCode)
	}
	return raw, nil
}

func isPartial(raw json.RawMessage) bool {
	var snapshot struct {
		Partial          *bool    `json:"partial"`
		MissingSettings  []string `json:"missing_settings"`
		UnavailableUnits []string `json:"unavailable_units"`
		Items            []struct {
			Unit             string `json:"unit"`
			Node             string `json:"node"`
			Role             string `json:"role"`
			SettingsComplete bool   `json:"settings_complete"`
			Tables           []struct {
				Name     string `json:"name"`
				Rows     *int64 `json:"row_count"`
				Bytes    *int64 `json:"relation_bytes"`
				Inserted *int64 `json:"stats_inserted"`
				Updated  *int64 `json:"stats_updated"`
			} `json:"tables"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil || snapshot.Partial == nil || *snapshot.Partial || len(snapshot.Items) == 0 || len(snapshot.MissingSettings) != 0 || len(snapshot.UnavailableUnits) != 0 {
		return true
	}
	for _, physical := range snapshot.Items {
		if !physical.SettingsComplete || strings.TrimSpace(physical.Unit) == "" || strings.TrimSpace(physical.Node) == "" || (physical.Role != "primary" && physical.Role != "replica") {
			return true
		}
		var seen [len(ownedTables)]bool
		for _, table := range physical.Tables {
			if table.Name == "" || table.Rows == nil || table.Bytes == nil || table.Inserted == nil || table.Updated == nil ||
				*table.Rows < 0 || *table.Bytes < 0 || *table.Inserted < 0 || *table.Updated < 0 {
				return true
			}
			for index, required := range ownedTables {
				if table.Name == required {
					if seen[index] {
						return true
					}
					seen[index] = true
				}
			}
		}
		for _, present := range seen {
			if !present {
				return true
			}
		}
	}
	return false
}

var ownedTables = [...]string{"hazard_events", "source_seismic", "source_warnings", "outbox", "handled_inputs"}
