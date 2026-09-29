package platform

import (
	"context"
	"io"
	"log/slog"
)

// Constructor
func NewLogger(service string, level slog.Level, output io.Writer) *slog.Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With("service", service)
}

// Enriches logger with the correlation identifier in ctx.
func LoggerWithContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if correlationID := CorrelationID(ctx); correlationID != "" {
		return logger.With("correlation_id", correlationID)
	}
	return logger
}

type logRecord struct {
	level   slog.Level
	event   string
	message string
}

func logEvent(ctx context.Context, logger *slog.Logger, record logRecord, attrs ...any) {
	args := make([]any, 0, len(attrs)+2)
	args = append(args, "event", record.event)
	args = append(args, attrs...)
	LoggerWithContext(ctx, logger).Log(ctx, record.level, record.message, args...)
}

type serverLogWriter struct {
	logger *slog.Logger
}

func (w serverLogWriter) Write(message []byte) (int, error) {
	logEvent(context.Background(), w.logger, logRecord{slog.LevelError, "http_server_error", "HTTP server diagnostic"})
	return len(message), nil
}
