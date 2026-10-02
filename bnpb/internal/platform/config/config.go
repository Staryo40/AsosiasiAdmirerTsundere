// Package config loads and validates shared BNPB service settings.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultLogLevel          = "INFO"
	defaultMaxInFlight       = 64
	defaultOutboundTimeout   = 5 * time.Second
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	defaultShutdownTimeout   = 5 * time.Second
)

// Config contains technical settings shared by every BNPB service.
type Config struct {
	HTTPAddr            string
	LogLevel            slog.Level
	MaxInFlight         int
	OutboundHTTPTimeout time.Duration
	ReadHeaderTimeout   time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	IdleTimeout         time.Duration
	ShutdownTimeout     time.Duration
}

// Load reads and validates the shared settings plus one service address.
func Load(httpAddrVar, defaultAddr string) (Config, error) {
	addr, err := envAddress(httpAddrVar, defaultAddr)
	if err != nil {
		return Config{}, err
	}
	level, err := envLogLevel("BNPB_LOG_LEVEL", defaultLogLevel)
	if err != nil {
		return Config{}, err
	}
	maxInFlight, err := envPositiveInt("BNPB_MAX_IN_FLIGHT", defaultMaxInFlight)
	if err != nil {
		return Config{}, err
	}
	timeouts, err := loadTimeouts()
	if err != nil {
		return Config{}, err
	}

	return Config{
		HTTPAddr:            addr,
		LogLevel:            level,
		MaxInFlight:         maxInFlight,
		OutboundHTTPTimeout: timeouts.outbound,
		ReadHeaderTimeout:   timeouts.readHeader,
		ReadTimeout:         timeouts.read,
		WriteTimeout:        timeouts.write,
		IdleTimeout:         timeouts.idle,
		ShutdownTimeout:     timeouts.shutdown,
	}, nil
}

type timeoutConfig struct {
	outbound   time.Duration
	readHeader time.Duration
	read       time.Duration
	write      time.Duration
	idle       time.Duration
	shutdown   time.Duration
}

func loadTimeouts() (timeoutConfig, error) {
	timeouts := timeoutConfig{}
	settings := []struct {
		name     string
		fallback time.Duration
		target   *time.Duration
	}{
		{"BNPB_OUTBOUND_HTTP_TIMEOUT", defaultOutboundTimeout, &timeouts.outbound},
		{"BNPB_READ_HEADER_TIMEOUT", defaultReadHeaderTimeout, &timeouts.readHeader},
		{"BNPB_READ_TIMEOUT", defaultReadTimeout, &timeouts.read},
		{"BNPB_WRITE_TIMEOUT", defaultWriteTimeout, &timeouts.write},
		{"BNPB_IDLE_TIMEOUT", defaultIdleTimeout, &timeouts.idle},
		{"BNPB_SHUTDOWN_TIMEOUT", defaultShutdownTimeout, &timeouts.shutdown},
	}
	for _, setting := range settings {
		configured, err := envPositiveDuration(setting.name, setting.fallback)
		if err != nil {
			return timeoutConfig{}, err
		}
		*setting.target = configured
	}
	return timeouts, nil
}

func envOrDefault(name, fallback string) string {
	if configured, ok := os.LookupEnv(name); ok {
		return configured
	}
	return fallback
}

func envAddress(name, fallback string) (string, error) {
	configured := envOrDefault(name, fallback)
	host, portText, err := net.SplitHostPort(configured)
	if err != nil || strings.ContainsAny(host, " /\\\t\r\n") {
		return "", fmt.Errorf("%s: invalid HTTP address", name)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("%s: invalid HTTP address", name)
	}
	return configured, nil
}

func envLogLevel(name, fallback string) (slog.Level, error) {
	switch strings.ToUpper(envOrDefault(name, fallback)) {
	case "DEBUG":
		return slog.LevelDebug, nil
	case "INFO":
		return slog.LevelInfo, nil
	case "WARN":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%s: invalid log level", name)
	}
}

func envPositiveInt(name string, fallback int) (int, error) {
	configured, err := strconv.Atoi(envOrDefault(name, strconv.Itoa(fallback)))
	if err != nil || configured <= 0 {
		return 0, fmt.Errorf("%s: must be a positive integer", name)
	}
	return configured, nil
}

func envPositiveDuration(name string, fallback time.Duration) (time.Duration, error) {
	configured, err := time.ParseDuration(envOrDefault(name, fallback.String()))
	if err != nil || configured <= 0 {
		return 0, fmt.Errorf("%s: must be a positive duration", name)
	}
	return configured, nil
}
