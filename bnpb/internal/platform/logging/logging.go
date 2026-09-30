// Package logging defines structured, correlation-aware service logging.
package logging

import (
	"context"
	"io"
	"log/slog"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
)

// New creates a JSON logger with a service name and minimum log level.
func New(service string, level slog.Level, output io.Writer) *slog.Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With("service", service)
}

// WithContext enriches logger with the correlation identifier in ctx.
func WithContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if id := correlation.ID(ctx); id != "" {
		return logger.With("correlation_id", id)
	}
	return logger
}

// Record names an event and its severity independently of request attributes.
type Record struct {
	Level   slog.Level
	Event   string
	Message string
}

// Event logs a named event and any additional structured attributes.
func Event(ctx context.Context, logger *slog.Logger, record Record, attrs ...any) {
	args := make([]any, 0, len(attrs)+2)
	args = append(args, "event", record.Event)
	args = append(args, attrs...)
	WithContext(ctx, logger).Log(ctx, record.Level, record.Message, args...)
}

// HTTPLevel maps HTTP response statuses to the shared service log severity.
func HTTPLevel(status int) slog.Level {
	switch {
	case status >= 500:
		return slog.LevelError
	case status >= 400:
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}
