package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform"
)

func main() {
	os.Exit(platform.Bootstrap("bnpb-api", "BNPB_API_HTTP_ADDR", ":8080", registerRoutes))
}

func registerRoutes(_ *http.ServeMux, _ platform.Config, _ *slog.Logger) {
	// Application routes are registered here as BNPB API capabilities are implemented.
}
