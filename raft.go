package election

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// NodeState represents the current state of a Raft node.
type NodeState int

const (
	// Follower is the initial state and the state after a leader is detected.
	Follower NodeState = iota
	// Candidate is the state when a node starts an election.
	Candidate
	// Leader is the state when a node wins an election.
	Leader
)

// String returns the string representation of a NodeState.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RaftNode implements the core Raft consensus algorithm for leader election.
// It manages terms, votes, and heartbeats as described in the Raft paper.
type RaftNode struct {
	mu sync.RWMutex

	// Persistent state on all servers
	currentTerm uint64
	votedFor    string
	nodeID      string

	// Volatile state
	state     NodeState
	leaderID  string
	peers     []string
	lastHeard time.Time

	// Configuration
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	// Channels
	voteCh        chan voteRequest
	heartbeatCh   chan struct{}
	leaderChangeCh chan string
	stopCh        chan struct{}
	stopped       atomic.Bool

	logger *zap.Logger
}

// RaftConfig holds configuration for a RaftNode.
type RaftConfig struct {
	NodeID           string
	Peers            []string
	ElectionTimeout  time.Duration // randomized 150-300ms by default
	HeartbeatTimeout time.Duration // defaults to 50ms
	Logger           *zap.Logger
}

// voteRequest represents a request for a vote from a candidate.
type voteRequest struct {
	Term         uint64
	CandidateID  string
	ResponseChan chan bool
}

// NewRaftNode creates a new Raft node with the given configuration.
func NewRaftNode(cfg RaftConfig) *RaftNode {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 150 * time.Millisecond
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 50 * time.Millisecond
	}

	return &RaftNode{
		nodeID:           cfg.NodeID,
		peers:            cfg.Peers,
		state:            Follower,
		currentTerm:      0,
		votedFor:         "",
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		voteCh:           make(chan voteRequest, 100),
		heartbeatCh:      make(chan struct{}, 1),
		leaderChangeCh:   make(chan string, 1),
		stopCh:           make(chan struct{}),
		logger:           cfg.Logger,
	}
}

// Start begins the Raft node's main event loop.
func (r *RaftNode) Start() {
	r.lastHeard = time.Now()
	go r.run()
}

// Stop halts the Raft node and releases resources.
func (r *RaftNode) Stop() {
	if r.stopped.CompareAndSwap(false, true) {
		close(r.stopCh)
	}
}

// LeaderChange returns a channel that emits the leader node ID whenever leadership changes.
func (r *RaftNode) LeaderChange() <-chan string {
	return r.leaderChangeCh
}

// IsLeader returns true if this node is currently the leader.
func (r *RaftNode) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state == Leader
}

// CurrentTerm returns the current Raft term.
func (r *RaftNode) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// State returns the current state of the node.
func (r *RaftNode) State() NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// HandleVoteRequest processes an incoming vote request from a candidate.
// Returns true if the vote is granted.
func (r *RaftNode) HandleVoteRequest(term uint64, candidateID string) bool {
	respCh := make(chan bool, 1)
	r.voteCh <- voteRequest{
		Term:         term,
		CandidateID:  candidateID,
		ResponseChan: respCh,
	}
	return <-respCh
}

// Heartbeat signals that a heartbeat was received from the current leader.
func (r *RaftNode) Heartbeat() {
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}
}

// run is the main event loop for the Raft node.
func (r *RaftNode) run() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func (r *RaftNode) runFollower() {
	timeout := r.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-timer.C:
			// Election timeout — become candidate
			r.logger.Info("election timeout, becoming candidate",
				zap.String("node", r.nodeID))
			r.setState(Candidate)
			return
		case <-r.heartbeatCh:
			// Reset election timer on heartbeat
			r.lastHeard = time.Now()
			timer.Reset(r.randomElectionTimeout())
		case req := <-r.voteCh:
			r.handleVote(req)
			timer.Reset(r.randomElectionTimeout())
		}
	}
}

func (r *RaftNode) runCandidate() {
	r.mu.Lock()
	r.currentTerm++
	r.votedFor = r.nodeID
	term := r.currentTerm
	r.mu.Unlock()

	r.logger.Info("starting election",
		zap.String("node", r.nodeID),
		zap.Uint64("term", term))

	votesReceived := 1 // vote for self
	votesNeeded := (len(r.peers)+1)/2 + 1

	// Request votes from peers
	for _, peer := range r.peers {
		go func(peerID string) {
			granted := r.requestVote(peerID, term)
			if granted {
				r.voteCh <- voteRequest{Term: term, CandidateID: r.nodeID, ResponseChan: make(chan bool, 1)}
			}
		}(peer)
	}

	timeout := r.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-timer.C:
			// Election timeout — restart election
			return
		case req := <-r.voteCh:
			if req.Term > term {
				// Discovered higher term — revert to follower
				r.mu.Lock()
				r.currentTerm = req.Term
				r.votedFor = ""
				r.setState(Follower)
				r.mu.Unlock()
				req.ResponseChan <- true
				return
			}
			if req.Term == term && req.CandidateID == r.nodeID {
				votesReceived++
				if votesReceived >= votesNeeded {
					r.setState(Leader)
					return
				}
			}
		case <-r.heartbeatCh:
			// Leader already exists for this term
			r.setState(Follower)
			return
		}
	}
}

func (r *RaftNode) runLeader() {
	r.mu.Lock()
	r.leaderID = r.nodeID
	r.mu.Unlock()

	// Notify leader change
	select {
	case r.leaderChangeCh <- r.nodeID:
	default:
	}

	r.logger.Info("became leader",
		zap.String("node", r.nodeID),
		zap.Uint64("term", r.CurrentTerm()))

	ticker := time.NewTicker(r.heartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.sendHeartbeats()
		case req := <-r.voteCh:
			if req.Term > r.CurrentTerm() {
				r.mu.Lock()
				r.currentTerm = req.Term
				r.votedFor = ""
				r.setState(Follower)
				r.mu.Unlock()
				req.ResponseChan <- true
				return
			}
			req.ResponseChan <- false
		case <-r.heartbeatCh:
			// Another node claims leadership at same term — step down
			r.setState(Follower)
			return
		}
	}
}

func (r *RaftNode) handleVote(req voteRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	granted := false
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = ""
		r.state = Follower
	}

	if req.Term >= r.currentTerm && (r.votedFor == "" || r.votedFor == req.CandidateID) {
		r.votedFor = req.CandidateID
		granted = true
	}

	req.ResponseChan <- granted
}

func (r *RaftNode) requestVote(peerID string, term uint64) bool {
	// In a real implementation, this would make an RPC call to the peer.
	// For this library, we simulate vote requests via the HandleVoteRequest method.
	r.logger.Debug("requesting vote",
		zap.String("from", r.nodeID),
		zap.String("to", peerID),
		zap.Uint64("term", term))
	return false // actual RPC would return the result
}

func (r *RaftNode) sendHeartbeats() {
	// In a real implementation, this would send RPC heartbeats to all peers.
	r.logger.Debug("sending heartbeats",
		zap.String("leader", r.nodeID),
		zap.Uint64("term", r.CurrentTerm()))
}

func (r *RaftNode) getState() NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *RaftNode) setState(state NodeState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
}

func (r *RaftNode) randomElectionTimeout() time.Duration {
	// Randomized timeout between [electionTimeout, 2*electionTimeout)
	jitter := time.Duration(rand.Int63n(int64(r.electionTimeout)))
	return r.electionTimeout + jitter
}
