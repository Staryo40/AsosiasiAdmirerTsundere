package main

import (
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
	satellite "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/satellite"
)

func main() {
	// Runs beside a live replica: kubectl exec satellite-0 -- /bnpb-satellite -verify
	if len(os.Args) >= 2 && os.Args[1] == "-verify" {
		os.Exit(satellite.RunVerify(os.Args[2:], os.Stdout, os.Stderr))
	}
	os.Exit(service.BootstrapApplication("bnpb-satellite", "BNPB_SATELLITE_HTTP_ADDR", ":8089", satellite.Initialize))
}
