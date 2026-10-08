package main

import (
	"os"

	consumer3 "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/consumer-3"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-consumer-3", "BNPB_CONSUMER_3_HTTP_ADDR", ":8087", consumer3.Initialize))
}
