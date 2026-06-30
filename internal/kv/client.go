package kv

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// KV is the client-facing surface of a single server: submit a command and get its
// result. In tests the Clerk holds in-process *Server values; in the live binary it
// holds net/rpc stubs. Both satisfy this one method, so the Clerk's leader-finding
// and retry logic is identical in both worlds.
type KV interface {
	Submit(ctx context.Context, cmd Command) (Result, error)
}

// Clerk is the client. It hides three realities from the caller: it does not know
// which server is the leader, the leader can change mid-operation, and any request
// may be lost. It discovers the leader by trying, follows leadership changes, and
// retries - reusing one SeqNum across all retries of a logical operation so the
// server's dedup table collapses duplicates into exactly-once.
type Clerk struct {
	servers []KV
	timeout time.Duration // per-attempt deadline
	retry   time.Duration // pause between full sweeps when no server accepts

	mu       sync.Mutex
	clientID int64
	seq      int64
	leader   int // index into servers of the last server that accepted
}

// ClerkOption configures a Clerk.
type ClerkOption func(*Clerk)

// WithTimeout sets the per-attempt commit deadline (default 1s).
func WithTimeout(d time.Duration) ClerkOption { return func(c *Clerk) { c.timeout = d } }

// WithRetryInterval sets the pause between full server sweeps (default 10ms).
func WithRetryInterval(d time.Duration) ClerkOption { return func(c *Clerk) { c.retry = d } }

// NewClerk builds a client over the given servers. Each Clerk gets a random
// ClientID so its sessions never collide with another client's.
func NewClerk(servers []KV, opts ...ClerkOption) *Clerk {
	c := &Clerk{
		servers:  servers,
		timeout:  time.Second,
		retry:    10 * time.Millisecond,
		clientID: rand.Int63(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get returns the value for key and whether it existed. It is a linearizable read:
// it reflects every write that completed before this call began.
func (c *Clerk) Get(ctx context.Context, key string) (value string, ok bool, err error) {
	res, err := c.call(ctx, Command{Kind: OpGet, Key: key})
	return res.Value, res.Ok, err
}

// GetStale returns the value for key from whatever node answers first, without a
// leadership round. It is fast but may return slightly stale data (it is not
// linearizable) - the read-consistency trade-off this knob exists to demonstrate.
func (c *Clerk) GetStale(ctx context.Context, key string) (value string, ok bool, err error) {
	res, err := c.call(ctx, Command{Kind: OpGet, Key: key, ReadStale: true})
	return res.Value, res.Ok, err
}

// Put sets key to value.
func (c *Clerk) Put(ctx context.Context, key, value string) error {
	_, err := c.call(ctx, Command{Kind: OpPut, Key: key, Value: value})
	return err
}

// Delete removes key and reports whether it existed.
func (c *Clerk) Delete(ctx context.Context, key string) (existed bool, err error) {
	res, err := c.call(ctx, Command{Kind: OpDelete, Key: key})
	return res.Ok, err
}

// CAS sets key to value only if it currently equals expected; it reports whether
// the swap happened.
func (c *Clerk) CAS(ctx context.Context, key, expected, value string) (swapped bool, err error) {
	res, err := c.call(ctx, Command{Kind: OpCAS, Key: key, Expected: expected, Value: value})
	return res.Ok, err
}

// call stamps cmd with this client's identity and the next SeqNum, then retries it
// across servers until one commits it or ctx expires. The SeqNum is allocated once
// and reused on every retry, which is what makes a lost-reply re-submit
// exactly-once at the server.
func (c *Clerk) call(ctx context.Context, cmd Command) (Result, error) {
	c.mu.Lock()
	c.seq++
	cmd.ClientID = c.clientID
	cmd.SeqNum = c.seq
	server := c.leader
	c.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		for i := 0; i < len(c.servers); i++ {
			idx := (server + i) % len(c.servers)
			attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
			res, err := c.servers[idx].Submit(attemptCtx, cmd)
			cancel()
			if err == nil {
				c.mu.Lock()
				c.leader = idx // remember the leader for next time
				c.mu.Unlock()
				return res, nil
			}
		}
		// No server accepted this sweep (election in progress, partition, etc.).
		// Pause briefly, then sweep again from where we left off.
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(c.retry):
		}
	}
}
