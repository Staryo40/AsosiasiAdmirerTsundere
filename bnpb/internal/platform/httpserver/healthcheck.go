package httpserver

import (
	"fmt"
	"net"
	"net/http"
	"time"
)

// HealthCheck probes the local service health endpoint without following redirects.
func HealthCheck(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	probeHost := "127.0.0.1"
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			probeHost = host
		} else if ip.To4() == nil {
			probeHost = "::1"
		}
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Get("http://" + net.JoinHostPort(probeHost, port) + "/health")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned HTTP %d", response.StatusCode)
	}
	return nil
}
