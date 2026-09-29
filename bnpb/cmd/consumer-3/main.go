package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform"
)

func main() {
	os.Exit(platform.Bootstrap("bnpb-consumer-3", "BNPB_CONSUMER_3_HTTP_ADDR", ":8087", registerRoutes))
}

func registerRoutes(_ *http.ServeMux, _ platform.Config, _ *slog.Logger) {
	// Broker consumption is started here when the demo consumer is implemented.
}
