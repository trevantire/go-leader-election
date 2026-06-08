// Package election provides a Raft-based leader election library for Go.
// It supports pluggable backends (etcd, Consul) and includes a built-in
// Raft node implementation for standalone clusters.
//
// Usage:
//
//	import "github.com/trevantire/go-leader-election"
//
//	func main() {
//	    backend, _ := backends.NewEtcdBackend(backends.EtcdConfig{
//	        Endpoints: []string{"localhost:2379"},
//	    })
//
//	    elect, _ := election.New(election.Config{
//	        Key:     "my-service/leader",
//	        Value:   "node-1",
//	        Backend: backend,
//	    })
//
//	    ctx, cancel := context.WithCancel(context.Background())
//	    defer cancel()
//
//	    elect.Start(ctx)
//	    defer elect.Stop()
//
//	    if elect.IsLeader() {
//	        fmt.Println("I am the leader!")
//	    }
//	}
package election

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trevantire/go-leader-election/backends"
	"go.uber.org/zap"
)

// Elect manages the leader election lifecycle.
// It coordinates between a backend (etcd/Consul) and the local Raft node
// to provide reliable leader election with automatic failover.
type Elect struct {
	key     string
	value   string
	backend backends.Backend
	raft    *RaftNode
	logger  *zap.Logger

	isLeader   atomic.Bool
	leaderID   string
	cancelFunc context.CancelFunc
	done       chan struct{}
	mu         sync.RWMutex
}

// Config holds the configuration for creating a new Elect instance.
type Config struct {
	// Key is the election key in the backend store.
	Key string

	// Value is the unique identifier for this node (e.g., hostname, UUID).
	Value string

	// Backend is the distributed store backend (etcd or Consul).
	Backend backends.Backend

	// RaftConfig optionally configures the local Raft node.
	// If nil, a default configuration is used.
	RaftConfig *RaftConfig

	// Logger for structured logging. Defaults to no-op logger.
	Logger *zap.Logger

	// CampaignInterval is the interval between campaign attempts when
	// leadership is lost. Defaults to 5 seconds.
	CampaignInterval time.Duration

	// OnLeaderChange is called when leadership status changes.
	// The bool parameter is true if this node became leader.
	OnLeaderChange func(isLeader bool)
}

// New creates a new Elect instance with the given configuration.
func New(cfg Config) (*Elect, error) {
	if cfg.Key == "" {
		return nil, fmt.Errorf("election key is required")
	}
	if cfg.Value == "" {
		return nil, fmt.Errorf("election value (node ID) is required")
	}
	if cfg.Backend == nil {
		return nil, fmt.Errorf("backend is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.CampaignInterval == 0 {
		cfg.CampaignInterval = 5 * time.Second
	}

	raftCfg := RaftConfig{
		NodeID: cfg.Value,
		Logger: cfg.Logger,
	}
	if cfg.RaftConfig != nil {
		raftCfg = *cfg.RaftConfig
		raftCfg.NodeID = cfg.Value
		if raftCfg.Logger == nil {
			raftCfg.Logger = cfg.Logger
		}
	}

	return &Elect{
		key:     cfg.Key,
		value:   cfg.Value,
		backend: cfg.Backend,
		raft:    NewRaftNode(raftCfg),
		logger:  cfg.Logger,
		done:    make(chan struct{}),
	}, nil
}

// Start begins the leader election process. It starts the local Raft node
// and begins campaigning for leadership in the background.
// The provided context controls the lifecycle of the election.
func (e *Elect) Start(ctx context.Context) {
	ctx, e.cancelFunc = context.WithCancel(ctx)

	e.raft.Start()
	go e.campaignLoop(ctx)
	go e.watchLeader(ctx)

	e.logger.Info("election started",
		zap.String("key", e.key),
		zap.String("value", e.value))
}

// Stop gracefully stops the election process and resigns leadership.
func (e *Elect) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.raft.Stop()

	if e.isLeader.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.backend.Resign(ctx, e.key); err != nil {
			e.logger.Warn("failed to resign leadership", zap.Error(err))
		}
		e.isLeader.Store(false)
	}

	close(e.done)
	e.logger.Info("election stopped", zap.String("value", e.value))
}

// IsLeader returns true if this node is currently the leader.
func (e *Elect) IsLeader() bool {
	return e.isLeader.Load()
}

// LeaderID returns the current leader's identifier, or empty string if unknown.
func (e *Elect) LeaderID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leaderID
}

// Done returns a channel that is closed when the election is stopped.
func (e *Elect) Done() <-chan struct{} {
	return e.done
}

// campaignLoop continuously attempts to acquire leadership.
func (e *Elect) campaignLoop(ctx context.Context) {
	// Initial campaign
	e.tryCampaign(ctx)

	ticker := time.NewTicker(e.getCampaignInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !e.isLeader.Load() {
				e.tryCampaign(ctx)
			}
		}
	}
}

// tryCampaign attempts to acquire leadership through the backend.
func (e *Elect) tryCampaign(ctx context.Context) {
	// Check current leader first
	current, err := e.backend.Observe(ctx, e.key)
	if err != nil {
		e.logger.Warn("failed to observe leader", zap.Error(err))
		return
	}

	if current == e.value {
		// We are already the leader
		e.setLeader(true)
		return
	}

	if current != "" {
		// Someone else is leader
		e.setLeaderID(current)
		e.setLeader(false)
		return
	}

	// No leader — campaign
	acquired, err := e.backend.Campaign(ctx, e.key, e.value, current)
	if err != nil {
		e.logger.Warn("campaign failed", zap.Error(err))
		return
	}

	if acquired {
		e.logger.Info("acquired leadership",
			zap.String("key", e.key),
			zap.String("value", e.value))
		e.setLeader(true)
		e.setLeaderID(e.value)
	} else {
		e.setLeader(false)
	}
}

// watchLeader watches for leader changes from the backend.
func (e *Elect) watchLeader(ctx context.Context) {
	ch, err := e.backend.Watch(ctx, e.key)
	if err != nil {
		e.logger.Warn("failed to watch leader", zap.Error(err))
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case leader, ok := <-ch:
			if !ok {
				return
			}

			isUs := leader == e.value
			e.setLeader(isUs)
			e.setLeaderID(leader)

			e.logger.Info("leader changed",
				zap.String("leader", leader),
				zap.Bool("isLeader", isUs))
		}
	}
}

func (e *Elect) setLeader(isLeader bool) {
	wasLeader := e.isLeader.Swap(isLeader)
	if wasLeader != isLeader {
		e.logger.Info("leadership status changed",
			zap.String("value", e.value),
			zap.Bool("isLeader", isLeader))

		// Notify raft node
		if isLeader {
			e.raft.Heartbeat()
		}
	}
}

func (e *Elect) setLeaderID(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.leaderID = id
}

func (e *Elect) getCampaignInterval() time.Duration {
	// Add jitter to prevent thundering herd
	return e.getCampaignIntervalWithJitter()
}

func (e *Elect) getCampaignIntervalWithJitter() time.Duration {
	base := 5 * time.Second
	jitter := time.Duration(rand.Int63n(int64(base / 2)))
	return base + jitter
}
