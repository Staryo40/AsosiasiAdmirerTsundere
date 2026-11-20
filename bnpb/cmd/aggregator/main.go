package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/aggregator"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	telemetry := aggregator.NewTelemetry()
	initialize := func(ctx context.Context, mux *http.ServeMux, cfg config.Config, logger *slog.Logger) (service.Shutdown, error) {
		telemetry.Register(mux)
		return aggregator.Initialize(ctx, mux, cfg, logger)
	}
	os.Exit(service.BootstrapApplication("bnpb-aggregator", "BNPB_AGGREGATOR_HTTP_ADDR", ":8083", initialize, telemetry.Wrap))
}
