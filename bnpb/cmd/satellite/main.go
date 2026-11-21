package main

import (
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
	satellite "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/satellite"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-satellite", "BNPB_SATELLITE_HTTP_ADDR", ":8089", satellite.Initialize))
}
