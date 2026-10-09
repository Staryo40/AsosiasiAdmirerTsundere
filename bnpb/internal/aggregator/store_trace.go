package aggregator

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

type databaseTracer struct{ logger *slog.Logger }
type databaseTraceKey struct{}

func databaseTraceContext(ctx context.Context) context.Context {
	// Background connection/query operations get their own identity, never a fabricated message CID.
	if correlation.ID(ctx) == "" {
		ctx = correlation.WithID(ctx, correlation.NewID())
	}
	return context.WithValue(ctx, databaseTraceKey{}, time.Now())
}

func (tracer databaseTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return databaseTraceContext(ctx)
}

func (tracer databaseTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, completion pgx.TraceQueryEndData) {
	tracer.recordCompletion(ctx, "query", completion.Err)
}

func (tracer databaseTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	return databaseTraceContext(ctx)
}

func (tracer databaseTracer) TraceConnectEnd(ctx context.Context, completion pgx.TraceConnectEndData) {
	tracer.recordCompletion(ctx, "connect", completion.Err)
}

func (tracer databaseTracer) recordCompletion(ctx context.Context, operation string, err error) {
	started := ctx.Value(databaseTraceKey{}).(time.Time)
	level, category := databaseStatus(err)
	// SQL text, arguments, config and driver error messages can expose source payloads or credentials.
	logging.Event(ctx, tracer.logger, logging.Record{Level: level, Event: "outbound_database", Message: "database operation completed"},
		"operation", operation, "category", category, "sqlstate", databaseSQLState(err), "duration_ms", time.Since(started).Seconds()*1000)
}

func databaseStatus(err error) (slog.Level, string) {
	if err == nil {
		return slog.LevelInfo, "success"
	}
	if errors.Is(err, context.Canceled) {
		return slog.LevelWarn, "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return slog.LevelWarn, "timeout"
	}
	return slog.LevelWarn, "database"
}

func databaseSQLState(err error) string {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return postgresError.Code
	}
	return ""
}
