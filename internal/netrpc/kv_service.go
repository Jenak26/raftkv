package netrpc

import (
	"context"
	"errors"
	"net/rpc"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/kv"
)

// KVArgs and KVReply are the wire types for the client-facing KV RPC. The
// command's outcome travels in Result; a server-side error (not the leader, timed
// out) travels as a string in Err so net/rpc delivers it inside the reply rather
// than as a transport-level ServerError, letting the client map it back to a
// typed, retryable error.
type KVArgs struct{ Cmd kv.Command }
type KVReply struct {
	Result kv.Result
	Err    string
}

// kvService adapts a *kv.Server to net/rpc and is registered under "KV". It bounds
// each Submit with its own timeout so a request that cannot commit (no quorum)
// returns rather than blocking the RPC forever; the client then retries elsewhere.
type kvService struct {
	srv     *kv.Server
	timeout time.Duration
}

func (s *kvService) Submit(args *KVArgs, reply *KVReply) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	res, err := s.srv.Submit(ctx, args.Cmd)
	reply.Result = res
	if err != nil {
		reply.Err = err.Error()
	}
	return nil
}

// KVClient is a client stub for one server's KV service. It implements kv.KV, so a
// Clerk built over a slice of KVClients behaves identically to one built over
// in-process servers in tests.
type KVClient struct {
	addr    string
	timeout time.Duration

	mu     sync.Mutex
	client *rpc.Client
}

// DialKV returns a lazy client for the KV service at addr; the connection is
// established on first use and re-established after an error.
func DialKV(addr string, timeout time.Duration) *KVClient {
	return &KVClient{addr: addr, timeout: timeout}
}

func (k *KVClient) Submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	client, err := k.clientConn()
	if err != nil {
		return kv.Result{}, err
	}

	args := &KVArgs{Cmd: cmd}
	reply := &KVReply{}
	done := make(chan *rpc.Call, 1)
	client.Go("KV.Submit", args, reply, done)

	var timeout <-chan time.Time
	if k.timeout > 0 {
		timer := time.NewTimer(k.timeout)
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-timeout:
		k.drop()
		return kv.Result{}, errors.New("netrpc: KV.Submit timed out")
	case c := <-done:
		if c.Error != nil {
			k.drop()
			return kv.Result{}, c.Error
		}
		if reply.Err != "" {
			return reply.Result, decodeKVErr(reply.Err)
		}
		return reply.Result, nil
	}
}

// decodeKVErr maps the server's error string back to the typed kv sentinels so a
// caller can distinguish them; any unknown string becomes a plain error. The Clerk
// treats every non-nil error as "retry elsewhere", so exact identity is a nicety,
// not a correctness requirement.
func decodeKVErr(s string) error {
	switch s {
	case kv.ErrNotLeader.Error():
		return kv.ErrNotLeader
	case kv.ErrLostLeader.Error():
		return kv.ErrLostLeader
	case kv.ErrTimeout.Error():
		return kv.ErrTimeout
	default:
		return errors.New(s)
	}
}

func (k *KVClient) clientConn() (*rpc.Client, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.client != nil {
		return k.client, nil
	}
	c, err := rpc.Dial("tcp", k.addr)
	if err != nil {
		return nil, err
	}
	k.client = c
	return c, nil
}

func (k *KVClient) drop() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.client != nil {
		k.client.Close()
		k.client = nil
	}
}

// Close releases the underlying connection.
func (k *KVClient) Close() { k.drop() }

// Compile-time assertion that KVClient satisfies the Clerk's endpoint interface.
var _ kv.KV = (*KVClient)(nil)
