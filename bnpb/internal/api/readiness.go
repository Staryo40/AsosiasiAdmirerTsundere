// Package api owns the client-facing service's dependency checks.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpclient"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func Initialize(_ context.Context, mux *http.ServeMux, cfg config.Config, logger *slog.Logger) (service.Shutdown, error) {
	address, err := aggregatorReadyURL()
	if err != nil {
		return nil, err
	}
	client := httpclient.New(cfg.OutboundHTTPTimeout, logger)
	mux.HandleFunc("GET /ready", dependencyReadiness(client, address))
	return func(context.Context) error {
		client.CloseIdleConnections()
		return nil
	}, nil
}

func aggregatorReadyURL() (string, error) {
	base, present := os.LookupEnv("BNPB_AGGREGATOR_BASE_URL")
	if !present {
		base = "http://aggregator:8083"
	}
	address, err := url.Parse(base)
	if err != nil || (address.Scheme != "http" && address.Scheme != "https") || address.Hostname() == "" ||
		address.User != nil || address.RawQuery != "" || address.ForceQuery || address.Fragment != "" || strings.Trim(address.Path, "/") != "" {
		return "", errors.New("BNPB_AGGREGATOR_BASE_URL: expected HTTP(S) origin without credentials, path, query or fragment")
	}
	address.Path = "/ready"
	return address.String(), nil
}

func dependencyReadiness(client *http.Client, address string) http.HandlerFunc {
	return func(w http.ResponseWriter, incoming *http.Request) {
		if !aggregatorAvailable(incoming.Context(), client, address) {
			http.Error(w, "aggregator is unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func aggregatorAvailable(ctx context.Context, client *http.Client, address string) bool {
	outgoing, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return false
	}
	response, err := client.Do(outgoing)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	return response.StatusCode == http.StatusOK
}
