package main

import (
	"os"

	fieldintake "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/field-intake"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-field-intake", "BNPB_FIELD_INTAKE_HTTP_ADDR", ":8088", fieldintake.Initialize))
}
