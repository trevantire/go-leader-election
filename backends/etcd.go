// Package backends provides etcd and Consul implementations of the Backend interface.
package backends

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

// EtcdBackend implements Backend using etcd distributed key-value store.
// It uses etcd's built-in concurrency primitives for safe leader election.
type EtcdBackend struct {
	client  *clientv3.Client
	session *concurrency.Session
	logger  *zap.Logger
}

// EtcdConfig holds configuration for the etcd backend.
type EtcdConfig struct {
	Endpoints []string
	Username  string
	Password  string
	TTL       int // session TTL in seconds; defaults to 15
	Logger    *zap.Logger
}

// NewEtcdBackend creates a new etcd-backed election backend.
// It establishes a connection and creates an etcd session with the given TTL.
func NewEtcdBackend(cfg EtcdConfig) (*EtcdBackend, error) {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 15
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: cfg.Endpoints,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd client: %w", err)
	}

	session, err := concurrency.NewSession(cli, concurrency.WithTTL(cfg.TTL))
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("etcd session: %w", err)
	}

	return &EtcdBackend{
		client:  cli,
		session: session,
		logger:  cfg.Logger,
	}, nil
}

// Campaign attempts to acquire leadership using etcd elections.
// It creates an election on the given key and attempts to become leader.
// The campaign blocks until leadership is acquired or ctx is cancelled.
func (e *EtcdBackend) Campaign(ctx context.Context, key, value, _ string) (bool, error) {
	election := concurrency.NewElection(e.session, key)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- election.Campaign(ctx, value)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return false, fmt.Errorf("etcd campaign: %w", err)
		}
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Observe returns the current leader value from the etcd election.
func (e *EtcdBackend) Observe(ctx context.Context, key string) (string, error) {
	election := concurrency.NewElection(e.session, key)

	resp, err := election.Leader(ctx)
	if err != nil {
		if err == concurrency.ErrElectionNoLeader {
			return "", nil
		}
		return "", fmt.Errorf("etcd observe: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// Resign releases leadership by resigning from the etcd election.
func (e *EtcdBackend) Resign(ctx context.Context, key string) error {
	election := concurrency.NewElection(e.session, key)
	if err := election.Resign(ctx); err != nil {
		return fmt.Errorf("etcd resign: %w", err)
	}
	return nil
}

// Watch returns a channel that emits leader values on changes.
func (e *EtcdBackend) Watch(ctx context.Context, key string) (<-chan string, error) {
	election := concurrency.NewElection(e.session, key)
	ch := election.Observe(ctx)

	out := make(chan string, 1)
	go func() {
		defer close(out)
		for {
			select {
			case resp, ok := <-ch:
				if !ok {
					return
				}
				if len(resp.Kvs) > 0 {
					select {
					case out <- string(resp.Kvs[0].Value):
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// Close releases the etcd session and client resources.
func (e *EtcdBackend) Close() error {
	if err := e.session.Close(); err != nil {
		e.logger.Warn("etcd session close error", zap.Error(err))
	}
	return e.client.Close()
}
