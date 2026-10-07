package main

import (
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/aggregator"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-aggregator", "BNPB_AGGREGATOR_HTTP_ADDR", ":8083", aggregator.Initialize))
}
