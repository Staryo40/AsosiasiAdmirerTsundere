// Poll owner supervision follows bnpb/contracts/v1/README.md.
package aggregator

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const authorityRetryMaximum = 5 * time.Second

func (app application) ownerWorkers(ctx context.Context, cfg Config, logger *slog.Logger, dispatch func(context.Context)) ([]func(context.Context), error) {
	routed, err := validateShardURLs(cfg)
	if err != nil {
		return nil, err
	}
	if !routed {
		return append(app.pollWorkers(cfg, logger), dispatch), nil
	}
	stores := []*Store{app.store, app.store.shards[0], app.store.shards[1], app.store.shards[2]}
	authorityPools := make([]*pgxpool.Pool, 0, len(stores))
	for _, store := range stores {
		authorityPools = append(authorityPools, store.authorityPools...)
	}
	kubernetes, err := NewKubernetesAuthorityClient(cfg.Authority.Kubernetes)
	if err != nil {
		return nil, err
	}
	host, err := NewHostFenceClient(cfg.Authority.HostFence)
	if err != nil {
		return nil, err
	}
	constructorCtx, cancel := context.WithTimeout(ctx, cfg.DBWriteTimeout)
	defer cancel()
	authority, err := NewPollAuthority(constructorCtx, PollAuthorityConfig{
		Owner: uuid.NewString(), OwnerDomain: cfg.Authority.OwnerDomain, OwnerEndpoint: cfg.Authority.OwnerEndpoint, Domains: cfg.Authority.Domains, Stores: stores,
		AuthorityPools: authorityPools, Kubernetes: kubernetes, Host: host,
		RenewalInterval: cfg.Authority.RenewalInterval,
	})
	if err != nil {
		return nil, err
	}
	app.store.authority = authority
	activeWorkers := append(app.pollWorkers(cfg, logger), dispatch)
	return []func(context.Context){func(ctx context.Context) {
		retryAuthorityRun(ctx, func(ctx context.Context) error {
			return authority.run(ctx, activeWorkers...)
		}, logger)
	}}, nil
}

func retryAuthorityRun(ctx context.Context, run func(context.Context) error, logger *slog.Logger) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := run(ctx); ctx.Err() != nil {
			return
		} else if err != nil {
			logger.WarnContext(ctx, "poll authority run ended; retrying after bounded backoff")
		}
		if !waitPoll(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, authorityRetryMaximum)
	}
}
