# go-leader-election

[![Go Reference](https://pkg.go.dev/badge/github.com/trevantire/go-leader-election.svg)](https://pkg.go.dev/github.com/trevantire/go-leader-election)
[![Go Report Card](https://goreportcard.com/badge/github.com/trevantire/go-leader-election)](https://goreportcard.com/report/github.com/trevantire/go-leader-election)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Raft-based leader election library for Go with pluggable backends (etcd and Consul).

## Features

- **Raft consensus** — Implements leader election following the Raft paper
- **Pluggable backends** — Supports etcd and HashiCorp Consul
- **Automatic failover** — Transparent leader re-election on failure
- **Structured logging** — Built-in zap logging for observability
- **Thread-safe** — All operations are safe for concurrent use

## Installation

```bash
go get github.com/trevantire/go-leader-election
```

## Quick Start

### With etcd Backend

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/trevantire/go-leader-election"
	"github.com/trevantire/go-leader-election/backends"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()

	backend, err := backends.NewEtcdBackend(backends.EtcdConfig{
		Endpoints: []string{"localhost:2379"},
		Logger:    logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer backend.Close()

	elect, err := election.New(election.Config{
		Key:     "my-service/leader",
		Value:   "node-1", // unique identifier for this instance
		Backend: backend,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	elect.Start(ctx)
	defer elect.Stop()

	if elect.IsLeader() {
		fmt.Println("I am the leader!")
	}

	// Wait for shutdown signal
	<-ctx.Done()
}
```

### With Consul Backend

```go
backend, err := backends.NewConsulBackend(backends.ConsulConfig{
	Address: "localhost:8500",
	Token:   "your-consul-token",
	Logger:  logger,
})
```

## API Reference

### `election.Config`

| Field | Type | Description |
|-------|------|-------------|
| `Key` | `string` | Election key in the backend store |
| `Value` | `string` | Unique identifier for this node |
| `Backend` | `backends.Backend` | Storage backend (etcd or Consul) |
| `RaftConfig` | `*RaftConfig` | Optional Raft node configuration |
| `Logger` | `*zap.Logger` | Structured logger |
| `CampaignInterval` | `time.Duration` | Interval between campaign attempts (default: 5s) |
| `OnLeaderChange` | `func(bool)` | Callback on leadership change |

### `election.Elect`

| Method | Description |
|--------|-------------|
| `Start(ctx context.Context)` | Begin the election process |
| `Stop()` | Stop and resign leadership |
| `IsLeader() bool` | Check if this node is the leader |
| `LeaderID() string` | Get current leader's identifier |
| `Done() <-chan struct{}` | Channel closed when stopped |

### `backends.Backend` Interface

```go
type Backend interface {
    Campaign(ctx context.Context, key, value, prevValue string) (bool, error)
    Observe(ctx context.Context, key string) (string, error)
    Resign(ctx context.Context, key string) error
    Watch(ctx context.Context, key string) (<-chan string, error)
    Close() error
}
```

## Architecture

The library combines two mechanisms:

1. **Backend layer** — Distributed coordination via etcd or Consul
2. **Raft layer** — Local consensus algorithm for cluster-aware election

```
┌─────────────────────────────────────────┐
│              Elect (orchestrator)        │
├─────────────────────────────────────────┤
│  ┌──────────┐    ┌──────────────────┐   │
│  │  Backend  │◄──►│   RaftNode       │   │
│  │  (etcd/   │    │  (term/vote/     │   │
│  │   consul) │    │   heartbeat)     │   │
│  └──────────┘    └──────────────────┘   │
└─────────────────────────────────────────┘
```

## Configuration

### RaftNode Tuning

```go
elect, _ := election.New(election.Config{
    Key:     "my-service/leader",
    Value:   "node-1",
    Backend: backend,
    RaftConfig: &election.RaftConfig{
        ElectionTimeout:  200 * time.Millisecond,
        HeartbeatTimeout: 50 * time.Millisecond,
    },
})
```

## Testing

```bash
go test -v ./...
```

## License

MIT License — see [LICENSE](LICENSE) for details.

<!-- history: 2026-05-11 -->

<!-- history: 2026-05-16 -->
