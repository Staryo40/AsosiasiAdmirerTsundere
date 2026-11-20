package fencehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxResourcesBytes = 64 << 10

var schemaPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

type GuardTarget struct {
	DatabaseURL string `json:"database_url"`
	Schema      string `json:"schema"`
}

type DomainResources struct {
	DomainUUID string        `json:"domain_uuid"`
	AppOnly    bool          `json:"app_only,omitempty"`
	Guards     []GuardTarget `json:"guards,omitempty"`
}

type ResourceBindings struct {
	Domains []DomainResources `json:"domains"`
}

func LoadResourceBindings(path string, allowed []string) (ResourceBindings, error) {
	file, err := os.Open(path)
	if err != nil {
		return ResourceBindings{}, errors.New("resource bindings are unavailable")
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maxResourcesBytes+1))
	if err != nil || len(contents) > maxResourcesBytes {
		return ResourceBindings{}, errors.New("resource bindings are invalid or too large")
	}
	dec := json.NewDecoder(bytes.NewReader(contents))
	dec.DisallowUnknownFields()
	var bindings ResourceBindings
	if dec.Decode(&bindings) != nil || dec.Decode(new(any)) != io.EOF {
		return ResourceBindings{}, errors.New("resource bindings are invalid JSON")
	}
	if err := bindings.Validate(allowed); err != nil {
		return ResourceBindings{}, err
	}
	return bindings, nil
}

func (b ResourceBindings) Validate(allowed []string) error {
	allow := make(map[string]struct{}, len(allowed))
	for _, domain := range allowed {
		if !domainPattern.MatchString(domain) {
			return errors.New("allowlisted domain UUID is invalid")
		}
		allow[strings.ToLower(domain)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(b.Domains))
	for _, d := range b.Domains {
		key := strings.ToLower(d.DomainUUID)
		if !domainPattern.MatchString(d.DomainUUID) {
			return errors.New("resource binding domain UUID is invalid")
		}
		if _, ok := allow[key]; !ok {
			return errors.New("resource binding contains a non-allowlisted domain")
		}
		if _, ok := seen[key]; ok {
			return errors.New("resource binding domain is duplicated")
		}
		seen[key] = struct{}{}
		if d.AppOnly {
			if len(d.Guards) != 0 {
				return errors.New("app-only domain cannot configure guards")
			}
			continue
		}
		if len(d.Guards) == 0 {
			return errors.New("non-app-only domain requires PostgreSQL guards")
		}
		for _, g := range d.Guards {
			u, err := url.Parse(g.DatabaseURL)
			if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" || g.Schema == "" || !schemaPattern.MatchString(g.Schema) {
				return errors.New("PostgreSQL guard target is invalid")
			}
		}
	}
	if len(seen) != len(allow) {
		return errors.New("resource bindings do not cover every allowed domain")
	}
	return nil
}

func persistResourceBindings(path string, b ResourceBindings) error {
	body, err := json.Marshal(b)
	if err != nil {
		return errors.New("encode resource bindings")
	}
	if existing, err := os.ReadFile(path); err == nil {
		var old ResourceBindings
		if json.Unmarshal(existing, &old) != nil {
			return errors.New("persisted resource bindings are corrupt")
		}
		oldBytes, _ := json.Marshal(old)
		if !bytes.Equal(oldBytes, body) {
			return errors.New("resource bindings changed; explicit fail-closed recovery required")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("persisted resource bindings are unavailable")
	}
	tmp, err := os.CreateTemp(filepathDir(path), ".resources-*")
	if err != nil {
		return errors.New("persist resource bindings")
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return errors.New("secure resource bindings")
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return errors.New("persist resource bindings")
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return errors.New("persist resource bindings")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("persist resource bindings")
	}
	if err := os.Rename(name, path); err != nil {
		return errors.New("persist resource bindings")
	}
	return nil
}

func filepathDir(path string) string {
	i := strings.LastIndexAny(path, `/\\`)
	if i < 0 {
		return "."
	}
	if i == 0 {
		return path[:1]
	}
	return path[:i]
}

func (h *Host) revokeResources(ctx context.Context, domain string, identity Identity) error {
	binding, ok := h.bindings[strings.ToLower(domain)]
	if !ok {
		return errors.New("resource binding is unavailable")
	}
	if binding.AppOnly {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, operationLimit)
	defer cancel()
	errs := make(chan error, len(binding.Guards))
	var wg sync.WaitGroup
	for _, target := range binding.Guards {
		target := target
		wg.Add(1)
		go func() { defer wg.Done(); errs <- h.ops.closeGuard(ctx, target, identity) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return errors.New("one or more PostgreSQL guards could not be verified closed")
		}
	}
	return nil
}

func (nativeOps) closeGuard(ctx context.Context, target GuardTarget, identity Identity) error {
	pool, err := pgxpool.New(ctx, target.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	qualified := pgx.Identifier{target.Schema, "mutation_guard"}.Sanitize()
	var recovery bool
	if err := pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&recovery); err != nil {
		return err
	}
	if !recovery {
		_, err = pool.Exec(ctx, "UPDATE "+qualified+" SET state='closed', owner='' WHERE singleton AND owner=$1 AND epoch=$2 AND state='active'", identity.Owner, int64(identity.Epoch))
		if err != nil {
			return err
		}
	}
	deadline := time.NewTicker(100 * time.Millisecond)
	defer deadline.Stop()
	for {
		var state, owner string
		var epoch int64
		err = pool.QueryRow(ctx, "SELECT state, owner, epoch FROM "+qualified+" WHERE singleton").Scan(&state, &owner, &epoch)
		if err == nil {
			if epoch == int64(identity.Epoch) && state == "closed" && owner == "" {
				return nil
			}
			if owner != identity.Owner || epoch != int64(identity.Epoch) {
				return errors.New("guard identity mismatch")
			}
			if state != "active" {
				return errors.New("guard state is uncertain")
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
		}
	}
}
