package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform"
)

func main() {
	os.Exit(platform.Bootstrap("bnpb-aggregator", "BNPB_AGGREGATOR_HTTP_ADDR", ":8083", registerRoutes))
}

func registerRoutes(_ *http.ServeMux, _ platform.Config, _ *slog.Logger) {
	// Polling, canonical mapping, storage, and publishing are registered here.
}
