package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.Bootstrap("bnpb-api", "BNPB_API_HTTP_ADDR", ":8080", registerRoutes))
}

func registerRoutes(_ *http.ServeMux, _ config.Config, _ *slog.Logger) {
	// Application routes are registered here as BNPB API capabilities are implemented.
}
