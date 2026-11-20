package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/fencehost"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("expected serve, init, or fence-expire subcommand")
	}
	switch args[0] {
	case "init":
		flags := flag.NewFlagSet("init", flag.ContinueOnError)
		stateDir := flags.String("state-dir", "", "durable state directory")
		domains := flags.String("domains", "", "comma-separated allowlisted domain UUIDs")
		resourcesFile := flags.String("resources-file", "", "bounded JSON domain resource bindings")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *stateDir == "" || *domains == "" || *resourcesFile == "" {
			return errors.New("--state-dir, --domains, and --resources-file are required")
		}
		allowed := splitDomains(*domains)
		bindings, err := fencehost.LoadResourceBindings(*resourcesFile, allowed)
		if err != nil {
			return err
		}
		return fencehost.InitializeWithResources(*stateDir, allowed, bindings)
	case "recover":
		flags := flag.NewFlagSet("recover", flag.ContinueOnError)
		stateDir := flags.String("state-dir", "", "durable state directory")
		domains := flags.String("domains", "", "comma-separated allowlisted libvirt domain UUIDs")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *stateDir == "" || *domains == "" {
			return errors.New("--state-dir and --domains are required")
		}
		return fencehost.Recover(*stateDir, splitDomains(*domains))
	case "fence-expire":
		flags := flag.NewFlagSet("fence-expire", flag.ContinueOnError)
		stateDir := flags.String("state-dir", "", "durable state directory")
		domain := flags.String("domain", "", "libvirt domain UUID")
		epoch := flags.Uint64("epoch", 0, "lease epoch")
		seq := flags.Uint64("seq", 0, "renewal sequence")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *stateDir == "" || *domain == "" || *epoch == 0 || *seq == 0 {
			return errors.New("state-dir, domain, epoch, and seq are required")
		}
		return fencehost.Expire(*stateDir, *domain, *epoch, *seq)
	case "serve":
		return serve(args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	stateDir := flags.String("state-dir", "", "durable state directory")
	listen := flags.String("listen", "127.0.0.1:9443", "HTTPS listen address")
	tokenFile := flags.String("token-file", "", "file containing the bearer credential")
	cert := flags.String("tls-cert", "", "TLS certificate file")
	key := flags.String("tls-key", "", "TLS private key file")
	domains := flags.String("domains", "", "comma-separated allowlisted libvirt domain UUIDs")
	resourcesFile := flags.String("resources-file", "", "bounded JSON domain resource bindings")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" || *tokenFile == "" || *cert == "" || *key == "" || *domains == "" || *resourcesFile == "" {
		return errors.New("state directory, token, TLS cert/key, domains, and resource bindings are required")
	}
	allowedDomains := splitDomains(*domains)
	bindings, err := fencehost.LoadResourceBindings(*resourcesFile, allowedDomains)
	if err != nil {
		_ = fencehost.FailClosedDomains(allowedDomains)
		return err
	}
	tokenBytes, err := os.ReadFile(*tokenFile)
	if err != nil {
		return errors.New("read bearer credential file")
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" || len(token) > 4096 {
		return errors.New("bearer credential file is empty or too large")
	}
	host, err := fencehost.Open(*stateDir, token, allowedDomains, bindings)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *listen, Handler: host, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.ListenAndServeTLS(*cert, *key) }()
	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("HTTPS server: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("HTTPS shutdown: %w", err)
		}
		return <-serveErrors
	}
}

func splitDomains(value string) []string {
	domains := strings.Split(value, ",")
	for i := range domains {
		domains[i] = strings.TrimSpace(domains[i])
	}
	return domains
}
