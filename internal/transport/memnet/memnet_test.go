package memnet

import (
	"context"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/transport"
)

// echoServer is a trivial transport.Server test double: it records how many
// times each handler was called and answers with a term derived from the args
// so callers can assert that the *right* peer answered.
type echoServer struct {
	id        int
	voteCalls int
	apCalls   int
	snapCalls int
}

func (e *echoServer) HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply {
	e.voteCalls++
	return &raft.RequestVoteReply{Term: e.id, VoteGranted: true}
}

func (e *echoServer) HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	e.apCalls++
	return &raft.AppendEntriesReply{Term: e.id, Success: true}
}

func (e *echoServer) HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	e.snapCalls++
	return &raft.InstallSnapshotReply{Term: e.id}
}

var _ transport.Server = (*echoServer)(nil)

// newTestNet wires up a fully-connected, reliable network of the given node ids
// using a MockClock, and returns the network plus the echo servers by id.
func newTestNet(t *testing.T, seed int64, ids ...int) (*Network, map[int]*echoServer) {
	t.Helper()
	clk := clock.NewMockClock(time.Unix(0, 0))
	net := New(seed, clk)
	servers := make(map[int]*echoServer, len(ids))
	for _, id := range ids {
		s := &echoServer{id: id}
		servers[id] = s
		net.AddNode(id, s)
	}
	return net, servers
}

func TestDeliversRPCToPeer(t *testing.T) {
	net, servers := newTestNet(t, 1, 0, 1)
	tr := net.Transport(0)

	reply, err := tr.SendRequestVote(context.Background(), 1, &raft.RequestVoteArgs{Term: 5})
	if err != nil {
		t.Fatalf("SendRequestVote: unexpected error %v", err)
	}
	if reply.Term != 1 {
		t.Errorf("reply came from wrong peer: got term %d, want 1", reply.Term)
	}
	if servers[1].voteCalls != 1 {
		t.Errorf("peer 1 handler called %d times, want 1", servers[1].voteCalls)
	}
	if servers[0].voteCalls != 0 {
		t.Errorf("sender's own handler was called %d times, want 0", servers[0].voteCalls)
	}
}

func TestSendToUnknownPeerErrors(t *testing.T) {
	net, _ := newTestNet(t, 1, 0)
	tr := net.Transport(0)

	if _, err := tr.SendAppendEntries(context.Background(), 99, &raft.AppendEntriesArgs{}); err == nil {
		t.Fatal("expected error sending to unknown peer, got nil")
	}
}

func TestCrashedPeerIsUnreachable(t *testing.T) {
	net, servers := newTestNet(t, 1, 0, 1)
	tr := net.Transport(0)

	net.Crash(1)
	if _, err := tr.SendRequestVote(context.Background(), 1, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("expected error sending to crashed peer, got nil")
	}
	if servers[1].voteCalls != 0 {
		t.Errorf("crashed peer handler called %d times, want 0", servers[1].voteCalls)
	}
}

func TestRestartRestoresReachability(t *testing.T) {
	net, _ := newTestNet(t, 1, 0, 1)
	tr := net.Transport(0)

	net.Crash(1)
	restarted := &echoServer{id: 1}
	net.Restart(1, restarted)

	reply, err := tr.SendRequestVote(context.Background(), 1, &raft.RequestVoteArgs{})
	if err != nil {
		t.Fatalf("after restart: unexpected error %v", err)
	}
	if reply.Term != 1 {
		t.Errorf("after restart: reply term %d, want 1", reply.Term)
	}
	if restarted.voteCalls != 1 {
		t.Errorf("restarted handler called %d times, want 1", restarted.voteCalls)
	}
}

func TestDisconnectAndConnect(t *testing.T) {
	net, _ := newTestNet(t, 1, 0, 1)
	tr := net.Transport(0)

	net.Disconnect(1)
	if _, err := tr.SendRequestVote(context.Background(), 1, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("expected error to disconnected peer, got nil")
	}

	net.Connect(1)
	if _, err := tr.SendRequestVote(context.Background(), 1, &raft.RequestVoteArgs{}); err != nil {
		t.Fatalf("after reconnect: unexpected error %v", err)
	}
}

func TestPartitionBlocksCrossGroupAndAllowsIntraGroup(t *testing.T) {
	net, _ := newTestNet(t, 1, 0, 1, 2, 3, 4)

	// Majority {0,1,2} | minority {3,4}.
	net.Partition([]int{0, 1, 2}, []int{3, 4})

	// Intra-group succeeds.
	if _, err := net.Transport(0).SendRequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err != nil {
		t.Errorf("0 -> 2 within group should succeed, got %v", err)
	}
	if _, err := net.Transport(3).SendRequestVote(context.Background(), 4, &raft.RequestVoteArgs{}); err != nil {
		t.Errorf("3 -> 4 within group should succeed, got %v", err)
	}
	// Cross-group fails both directions.
	if _, err := net.Transport(2).SendRequestVote(context.Background(), 3, &raft.RequestVoteArgs{}); err == nil {
		t.Error("2 -> 3 across groups should fail")
	}
	if _, err := net.Transport(3).SendRequestVote(context.Background(), 0, &raft.RequestVoteArgs{}); err == nil {
		t.Error("3 -> 0 across groups should fail")
	}
}

func TestNodeOmittedFromAllGroupsIsUnreachable(t *testing.T) {
	net, _ := newTestNet(t, 1, 0, 1, 2)
	net.Partition([]int{0, 1}) // node 2 is in no group

	if _, err := net.Transport(0).SendRequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err == nil {
		t.Error("expected node omitted from all partition groups to be unreachable")
	}
}

func TestHealRestoresFullConnectivity(t *testing.T) {
	net, _ := newTestNet(t, 1, 0, 1, 2)
	net.Partition([]int{0}, []int{1, 2})
	net.Heal()

	if _, err := net.Transport(0).SendRequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err != nil {
		t.Errorf("after Heal, 0 -> 2 should succeed, got %v", err)
	}
}

// pumpClock keeps a MockClock advancing in a background goroutine so that
// latency injected by an unreliable network resolves. It returns a stop func.
func pumpClock(clk *clock.MockClock) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-done:
				return
			default:
				clk.Advance(time.Millisecond)
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	return func() { close(done); <-finished }
}

// deliveredOverUnreliableNet sends n RequestVotes from 0 to 1 over an unreliable
// network seeded with seed and returns how many were delivered (not dropped).
func deliveredOverUnreliableNet(seed int64, n int) int {
	clk := clock.NewMockClock(time.Unix(0, 0))
	net := New(seed, clk)
	net.AddNode(0, &echoServer{id: 0})
	net.AddNode(1, &echoServer{id: 1})
	net.SetReliable(false)
	stop := pumpClock(clk)
	defer stop()

	tr := net.Transport(0)
	delivered := 0
	for i := 0; i < n; i++ {
		if _, err := tr.SendRequestVote(context.Background(), 1, &raft.RequestVoteArgs{Term: i}); err == nil {
			delivered++
		}
	}
	return delivered
}

func TestUnreliableDropPatternIsReproducibleFromSeed(t *testing.T) {
	const n = 300
	a := deliveredOverUnreliableNet(42, n)
	b := deliveredOverUnreliableNet(42, n)
	if a != b {
		t.Fatalf("same seed produced different drop patterns: %d vs %d", a, b)
	}
	if a == 0 || a == n {
		t.Fatalf("unreliable net delivered %d/%d - expected some drops but not all", a, n)
	}
}

func TestCrashedNodeCannotSend(t *testing.T) {
	net, _ := newTestNet(t, 1, 0, 1)
	net.Crash(0)

	if _, err := net.Transport(0).SendRequestVote(context.Background(), 1, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("a crashed node should not be able to send RPCs")
	}
}
