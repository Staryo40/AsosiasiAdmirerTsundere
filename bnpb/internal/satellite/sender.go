package satellite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const (
	sendBatchRecords = 100
	sendBatchBytes   = 2 << 20
	sendIdlePoll     = 2 * time.Second
)

type sender struct {
	aggregator *url.URL
	token      string
	store      *Store
	client     *http.Client
	logger     *slog.Logger
	wake       <-chan struct{}
	wait       func(context.Context, time.Duration) bool
	sent       atomic.Int64
	refused    atomic.Int64
}

func newSender(cfg Config, store *Store, wake <-chan struct{}, logger *slog.Logger) *sender {
	return &sender{aggregator: cfg.AggregatorURL, token: cfg.IngestToken, store: store, logger: logger, wake: wake,
		client: &http.Client{Timeout: 10 * time.Second}, wait: sleepContext}
}

type fireReceipt struct {
	InputID     string            `json:"input_id"`
	InputOffset int               `json:"input_offset"`
	ReceivedAt  time.Time         `json:"received_at"`
	Records     []json.RawMessage `json:"records"`
}

// At-least-once delivery: a row is marked sent only after the Aggregator answers, and it drops stale revisions.
func (s *sender) run(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		rows, err := s.store.PendingOutbox(ctx, sendBatchRecords, sendBatchBytes)
		if err == nil && len(rows) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
			case <-time.After(sendIdlePoll):
			}
			continue
		}
		if err == nil {
			err = s.send(ctx, rows)
		}
		if err == nil {
			failures = 0
			continue
		}
		failures++
		delay := backoff(failures, err)
		logging.Event(ctx, s.logger, logging.Record{Level: slog.LevelWarn, Event: "fire_delivery_failed", Message: "fire changes not delivered; retrying"},
			"category", errorCategory(err), "retry_in_ms", delay.Milliseconds())
		if !s.wait(ctx, delay) {
			return
		}
	}
}

func (s *sender) send(ctx context.Context, rows []outboxRow) error {
	first, last := rows[0].seq, rows[len(rows)-1].seq
	receipt := fireReceipt{InputID: fmt.Sprintf("sat-%d-%d", first, last), ReceivedAt: time.Now().UTC()}
	for _, row := range rows {
		receipt.Records = append(receipt.Records, row.record)
	}
	body, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	ctx = correlation.WithID(ctx, correlation.NewID())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.aggregator.JoinPath("/internal/satellite/fires").String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+s.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(correlation.Header, correlation.ID(ctx))
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	switch {
	case response.StatusCode == http.StatusOK:
		s.sent.Add(int64(len(rows)))
	case response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusConflict:
		// The same bytes would be refused again; record it loudly instead of blocking every later change.
		s.refused.Add(int64(len(rows)))
		logging.Event(ctx, s.logger, logging.Record{Level: slog.LevelError, Event: "fire_delivery_refused", Message: "the Aggregator refused a fire batch"},
			"status", response.StatusCode, "first_seq", first, "last_seq", last)
	default:
		return &feedError{status: response.StatusCode, retryAfter: parseRetryAfter(response.Header.Get("Retry-After"))}
	}
	if err := s.store.MarkSent(ctx, first, last); err != nil {
		return err
	}
	lag := time.Since(rows[0].receivedAt)
	logging.Event(ctx, s.logger, logging.Record{Level: slog.LevelInfo, Event: "fire_delivered", Message: "fire changes stored by the Aggregator"},
		"records", len(rows), "first_seq", first, "last_seq", last, "oldest_received_to_stored_ms", lag.Milliseconds())
	return nil
}
