package main

import (
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/api"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-api", "BNPB_API_HTTP_ADDR", ":8080", api.Initialize))
}
