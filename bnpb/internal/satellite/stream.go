package satellite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const (
	KindMalformed  = "malformed"
	minimumBackoff = time.Second
	maximumBackoff = 30 * time.Second
	// A rejected token will not fix itself; wait long instead of hammering the feed.
	unauthorizedBackoff = 60 * time.Second
)

type feedError struct {
	status     int
	retryAfter time.Duration
}

func (err *feedError) Error() string { return fmt.Sprintf("feed answered HTTP %d", err.status) }

var errOffsetGap = errors.New("feed skipped an offset")

type streamer struct {
	cfg       Config
	client    *http.Client
	processor Processor
	logger    *slog.Logger
	connected atomic.Bool
	wait      func(context.Context, time.Duration) bool
}

func newStreamer(cfg Config, processor Processor, logger *slog.Logger) *streamer {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true,
		DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second, IdleConnTimeout: 30 * time.Second}
	return &streamer{cfg: cfg, client: &http.Client{Transport: transport}, processor: processor, logger: logger, wait: sleepContext}
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Runs while this replica leads; every reconnect resumes from the processor's durable checkpoint.
func (s *streamer) run(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		delivered, err := s.once(ctx)
		if ctx.Err() != nil {
			return
		}
		if delivered > 0 {
			failures = 0
		}
		failures++
		delay := backoff(failures, err)
		logging.Event(ctx, s.logger, logging.Record{Level: slog.LevelWarn, Event: "feed_disconnected", Message: "feed stream ended; reconnecting"},
			"events", delivered, "category", errorCategory(err), "retry_in_ms", delay.Milliseconds())
		if !s.wait(ctx, delay) {
			return
		}
	}
}

func backoff(failures int, err error) time.Duration {
	var failure *feedError
	if errors.As(err, &failure) && failure.status == http.StatusUnauthorized {
		return unauthorizedBackoff
	}
	ceiling := minimumBackoff << min(failures-1, 5)
	ceiling = min(ceiling, maximumBackoff)
	delay := ceiling/2 + time.Duration(rand.Int64N(int64(ceiling/2)+1))
	if errors.As(err, &failure) {
		delay = max(delay, failure.retryAfter)
	}
	return delay
}

func errorCategory(err error) string {
	var failure *feedError
	switch {
	case err == nil:
		return "end_of_stream"
	case errors.As(err, &failure):
		return "http_" + strconv.Itoa(failure.status)
	case errors.Is(err, errOffsetGap):
		return "offset_gap"
	case errors.Is(err, errIdle):
		return "idle"
	default:
		return "transport"
	}
}

var errIdle = errors.New("feed was silent past the idle timeout")

func (s *streamer) once(ctx context.Context) (delivered int, err error) {
	ctx = correlation.WithID(ctx, correlation.NewID())
	next, err := s.processor.NextOffset(ctx)
	if err != nil {
		return 0, err
	}
	streamCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	request, err := http.NewRequestWithContext(streamCtx, http.MethodGet, s.cfg.BaseURL.JoinPath("/v1/stream").String()+"?from="+strconv.FormatInt(next, 10), nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set(correlation.Header, correlation.ID(ctx))
	response, err := s.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, &feedError{status: response.StatusCode, retryAfter: parseRetryAfter(response.Header.Get("Retry-After"))}
	}
	s.connected.Store(true)
	defer s.connected.Store(false)
	logging.Event(ctx, s.logger, logging.Record{Level: slog.LevelInfo, Event: "feed_connected", Message: "streaming from the feed"}, "from", next)

	idle := time.AfterFunc(s.cfg.IdleTimeout, func() { cancel(errIdle) })
	defer idle.Stop()
	defer func() {
		if flushErr := s.processor.Flush(context.WithoutCancel(ctx)); err == nil {
			err = flushErr
		}
		if cause := context.Cause(streamCtx); errors.Is(cause, errIdle) {
			err = errIdle
		}
	}()
	reader := newSSEReader(response.Body)
	for {
		frame, err := reader.next()
		if err != nil {
			return delivered, err
		}
		idle.Reset(s.cfg.IdleTimeout)
		event, parseErr := parseEvent(frame)
		if parseErr != nil {
			// Keep the position from the frame id so one bad frame cannot stall the stream.
			offset, idErr := strconv.ParseInt(frame.id, 10, 64)
			if idErr != nil {
				return delivered, parseErr
			}
			event = Event{Offset: offset, Kind: KindMalformed}
		}
		switch {
		case event.Offset < next:
			continue
		case event.Offset > next:
			return delivered, fmt.Errorf("%w: expected %d, got %d", errOffsetGap, next, event.Offset)
		}
		if err := s.processor.Process(ctx, event); err != nil {
			return delivered, err
		}
		next, delivered = next+1, delivered+1
	}
}

func parseRetryAfter(header string) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds > 0 {
		return min(time.Duration(seconds)*time.Second, 10*time.Minute)
	}
	if at, err := http.ParseTime(header); err == nil {
		return min(max(time.Until(at), 0), 10*time.Minute)
	}
	return 0
}
