package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.Bootstrap("bnpb-aggregator", "BNPB_AGGREGATOR_HTTP_ADDR", ":8083", registerRoutes))
}

func registerRoutes(_ *http.ServeMux, _ config.Config, _ *slog.Logger) {
	// Polling, canonical mapping, storage, and publishing are registered here.
}
