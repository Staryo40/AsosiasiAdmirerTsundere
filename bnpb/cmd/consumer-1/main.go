package main

import (
	"os"

	consumer1 "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/consumer-1"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-consumer-1", "BNPB_CONSUMER_1_HTTP_ADDR", ":8085", consumer1.Initialize))
}
