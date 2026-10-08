package main

import (
	"os"

	consumer2 "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/consumer-2"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-consumer-2", "BNPB_CONSUMER_2_HTTP_ADDR", ":8086", consumer2.Initialize))
}
