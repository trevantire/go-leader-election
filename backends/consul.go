package backends

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// ConsulBackend implements Backend using HashiCorp Consul's key-value store.
// It leverages Consul sessions for leader election with lock-delay semantics.
type ConsulBackend struct {
	client    *api.Client
	sessionID string
	keyPrefix string
	logger    *zap.Logger
}

// ConsulConfig holds configuration for the Consul backend.
type ConsulConfig struct {
	Address   string
	Token     string
	Datacenter string
	KeyPrefix string
	TTL       string // session TTL, e.g. "15s"
	Logger    *zap.Logger
}

// NewConsulBackend creates a new Consul-backed election backend.
func NewConsulBackend(cfg ConsulConfig) (*ConsulBackend, error) {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "leader-election/"
	}
	if cfg.TTL == "" {
		cfg.TTL = "15s"
	}

	conf := api.DefaultConfig()
	if cfg.Address != "" {
		conf.Address = cfg.Address
	}
	if cfg.Token != "" {
		conf.Token = cfg.Token
	}
	if cfg.Datacenter != "" {
		conf.Datacenter = cfg.Datacenter
	}

	client, err := api.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("consul client: %w", err)
	}

	sessionID, _, err := client.Session().Create(&api.SessionEntry{
		TTL:      cfg.TTL,
		Behavior: "delete", // delete keys when session expires
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("consul session: %w", err)
	}

	// Start renewal goroutine
	go func() {
		ttl, _ := time.ParseDuration(cfg.TTL)
		ticker := time.NewTicker(ttl / 2)
		defer ticker.Stop()
		for range ticker.C {
			_, _, err := client.Session().Renew(sessionID, nil)
			if err != nil {
				cfg.Logger.Warn("consul session renew failed", zap.Error(err))
				return
			}
		}
	}()

	return &ConsulBackend{
		client:    client,
		sessionID: sessionID,
		keyPrefix: cfg.KeyPrefix,
		logger:    cfg.Logger,
	}, nil
}

// Campaign attempts to acquire leadership using Consul's KV with session locking.
// It uses Consul's Check-And-Set (CAS) for atomic leader acquisition.
func (c *ConsulBackend) Campaign(ctx context.Context, key, value, _ string) (bool, error) {
	fullKey := c.keyPrefix + key

	// Try to acquire the lock by setting the key with our session
	kv := &api.KVPair{
		Key:     fullKey,
		Value:   []byte(value),
		Session: c.sessionID,
	}

	// First, try to acquire (CAS with index 0 = new key)
	success, _, err := c.client.KV().Acquire(kv, nil)
	if err != nil {
		return false, fmt.Errorf("consul acquire: %w", err)
	}

	if success {
		return true, nil
	}

	// Key exists — try CAS with current modify index
	pair, _, err := c.client.KV().Get(fullKey, nil)
	if err != nil {
		return false, fmt.Errorf("consul get: %w", err)
	}
	if pair == nil {
		// Key was deleted between attempts, retry
		return false, nil
	}

	kv.ModifyIndex = pair.ModifyIndex
	success, _, err = c.client.KV().Acquire(kv, nil)
	if err != nil {
		return false, fmt.Errorf("consul acquire CAS: %w", err)
	}

	return success, nil
}

// Observe returns the current leader value from Consul KV.
func (c *ConsulBackend) Observe(ctx context.Context, key string) (string, error) {
	fullKey := c.keyPrefix + key
	pair, _, err := c.client.KV().Get(fullKey, nil)
	if err != nil {
		return "", fmt.Errorf("consul observe: %w", err)
	}
	if pair == nil {
		return "", nil
	}
	return string(pair.Value), nil
}

// Resign releases leadership by releasing the Consul session lock.
func (c *ConsulBackend) Resign(ctx context.Context, key string) error {
	fullKey := c.keyPrefix + key
	pair, _, err := c.client.KV().Get(fullKey, nil)
	if err != nil {
		return fmt.Errorf("consul get for resign: %w", err)
	}
	if pair == nil {
		return nil
	}

	kv := &api.KVPair{
		Key:     fullKey,
		Value:   pair.Value,
		Session: c.sessionID,
	}
	_, _, err = c.client.KV().Release(kv, nil)
	if err != nil {
		return fmt.Errorf("consul release: %w", err)
	}
	return nil
}

// Watch returns a channel that emits leader values on changes via Consul blocking queries.
func (c *ConsulBackend) Watch(ctx context.Context, key string) (<-chan string, error) {
	fullKey := c.keyPrefix + key
	out := make(chan string, 1)

	go func() {
		defer close(out)
		var lastIndex uint64

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pair, meta, err := c.client.KV().Get(fullKey, &api.QueryOptions{
				WaitIndex: lastIndex,
				WaitTime:  30 * time.Second,
			})
			if err != nil {
				c.logger.Warn("consul watch error", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			if meta.LastIndex <= lastIndex {
				continue
			}
			lastIndex = meta.LastIndex

			if pair != nil {
				select {
				case out <- string(pair.Value):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

// Close destroys the Consul session, releasing all held locks.
func (c *ConsulBackend) Close() error {
	_, err := c.client.Session().Destroy(c.sessionID, nil)
	if err != nil {
		return fmt.Errorf("consul session destroy: %w", err)
	}
	return nil
}
