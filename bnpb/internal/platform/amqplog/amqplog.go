// Package amqplog records AMQP call latency without broker replies or credential-bearing URLs.
package amqplog

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

func Start(ctx context.Context, logger *slog.Logger, operation string) func(error) {
	started := time.Now()
	return func(err error) {
		level, category := callStatus(err)
		logging.Event(ctx, logger, logging.Record{Level: level, Event: "outbound_amqp", Message: "AMQP operation completed"},
			"operation", operation, "category", category, "duration_ms", time.Since(started).Seconds()*1000)
	}
}

func callStatus(err error) (slog.Level, string) {
	if err == nil {
		return slog.LevelInfo, "success"
	}
	if errors.Is(err, context.Canceled) {
		return slog.LevelWarn, "canceled"
	}
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkError) && networkError.Timeout()) {
		return slog.LevelWarn, "timeout"
	}
	return slog.LevelWarn, "broker"
}
