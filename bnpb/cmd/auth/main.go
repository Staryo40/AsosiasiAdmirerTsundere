package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/auth"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-auth", "BNPB_AUTH_HTTP_ADDR", ":8084", initialize))
}

func initialize(_ context.Context, mux *http.ServeMux, _ config.Config, logger *slog.Logger) (service.Shutdown, error) {
	authConfig, err := auth.LoadConfig()
	if err != nil {
		return nil, err
	}
	authService, err := auth.New(authConfig, logger)
	if err != nil {
		return nil, err
	}
	authService.RegisterRoutes(mux)
	return nil, nil
}
