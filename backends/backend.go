// Package backends defines the Backend interface for leader election storage.
// Implementations include etcd and Consul backends.
package backends

import "context"

// Backend abstracts the distributed key-value store used for leader election.
// Each backend must support atomic operations for safe leader acquisition.
type Backend interface {
	// Campaign attempts to become the leader for the given key.
	// It should atomically create or update the key with the provided value
	// only if the current value matches prevValue (CAS semantics).
	// Returns true if leadership was acquired, false otherwise.
	Campaign(ctx context.Context, key, value, prevValue string) (bool, error)

	// Observe returns the current leader value for the given key.
	// Returns empty string if no leader is set.
	Observe(ctx context.Context, key string) (string, error)

	// Resign removes leadership by deleting or clearing the key.
	Resign(ctx context.Context, key string) error

	// Watch returns a channel that receives the current leader value
	// whenever it changes. The channel is closed when ctx is cancelled.
	Watch(ctx context.Context, key string) (<-chan string, error)

	// Close releases any resources held by the backend.
	Close() error
}
