package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.Bootstrap("bnpb-consumer-2", "BNPB_CONSUMER_2_HTTP_ADDR", ":8086", registerRoutes))
}

func registerRoutes(_ *http.ServeMux, _ config.Config, _ *slog.Logger) {
	// Broker consumption is started here when the consumer is implemented.
}
