package election

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// mockBackend implements backends.Backend for testing.
type mockBackend struct {
	leader    string
	campaignOK bool
	watchCh   chan string
}

func newMockBackend() *mockBackend {
	return &mockBackend{
		watchCh: make(chan string, 10),
	}
}

func (m *mockBackend) Campaign(_ context.Context, _, value, _ string) (bool, error) {
	if m.campaignOK || m.leader == "" {
		m.leader = value
		m.watchCh <- value
		return true, nil
	}
	return false, nil
}

func (m *mockBackend) Observe(_ context.Context, _ string) (string, error) {
	return m.leader, nil
}

func (m *mockBackend) Resign(_ context.Context, _ string) error {
	m.leader = ""
	return nil
}

func (m *mockBackend) Watch(_ context.Context, _ string) (<-chan string, error) {
	return m.watchCh, nil
}

func (m *mockBackend) Close() error {
	close(m.watchCh)
	return nil
}

func TestNewElect_ValidatesConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "missing key",
			cfg:     Config{Value: "node-1", Backend: newMockBackend()},
			wantErr: true,
		},
		{
			name:    "missing value",
			cfg:     Config{Key: "test", Backend: newMockBackend()},
			wantErr: true,
		},
		{
			name:    "missing backend",
			cfg:     Config{Key: "test", Value: "node-1"},
			wantErr: true,
		},
		{
			name: "valid config",
			cfg: Config{
				Key:     "test",
				Value:   "node-1",
				Backend: newMockBackend(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestElect_StartStop(t *testing.T) {
	backend := newMockBackend()
	backend.campaignOK = true

	elect, err := New(Config{
		Key:     "test/leader",
		Value:   "node-1",
		Backend: backend,
		Logger:  zap.NewNop(),
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	elect.Start(ctx)

	// Give it time to campaign
	time.Sleep(100 * time.Millisecond)

	if !elect.IsLeader() {
		t.Error("expected node to become leader with mock backend")
	}

	cancel()
	elect.Stop()

	select {
	case <-elect.Done():
	case <-time.After(2 * time.Second):
		t.Error("Done channel not closed after Stop()")
	}
}

func TestElect_IsLeader(t *testing.T) {
	backend := newMockBackend()
	backend.campaignOK = true

	elect, err := New(Config{
		Key:     "test/leader",
		Value:   "node-1",
		Backend: backend,
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if elect.IsLeader() {
		t.Error("should not be leader before Start()")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	elect.Start(ctx)
	defer elect.Stop()

	time.Sleep(100 * time.Millisecond)

	if elect.LeaderID() != "node-1" {
		t.Errorf("expected leader ID 'node-1', got '%s'", elect.LeaderID())
	}
}

func TestRaftNode_States(t *testing.T) {
	node := NewRaftNode(RaftConfig{
		NodeID: "test-node",
		Logger: zap.NewNop(),
	})

	if node.State() != Follower {
		t.Errorf("expected initial state Follower, got %v", node.State())
	}

	if node.IsLeader() {
		t.Error("should not be leader initially")
	}

	node.Start()
	defer node.Stop()

	// Give it time to start
	time.Sleep(10 * time.Millisecond)

	if node.CurrentTerm() != 0 {
		t.Errorf("expected initial term 0, got %d", node.CurrentTerm())
	}
}

func TestRaftNode_VoteRequest(t *testing.T) {
	node := NewRaftNode(RaftConfig{
		NodeID: "test-node",
		Logger: zap.NewNop(),
	})
	node.Start()
	defer node.Stop()

	// First vote should be granted
	granted := node.HandleVoteRequest(1, "candidate-1")
	if !granted {
		t.Error("expected first vote to be granted")
	}

	// Same term, different candidate should be denied
	granted = node.HandleVoteRequest(1, "candidate-2")
	if granted {
		t.Error("expected second vote for same term to be denied")
	}

	// Higher term should be granted
	granted = node.HandleVoteRequest(2, "candidate-2")
	if !granted {
		t.Error("expected vote for higher term to be granted")
	}
}
