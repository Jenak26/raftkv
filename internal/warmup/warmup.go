// Package warmup holds small concurrency exercises whose only purpose is to
// validate the Phase 0 toolchain — that `go test -race` is wired up and that
// the build/lint/CI pipeline catches data races. It is intentionally throwaway
// and may be deleted once Raft development begins.
package warmup

import "sync"

// Counter is a goroutine-safe integer counter. Removing the mutex makes the
// race detector fail TestCounterConcurrent — a quick proof the tooling works.
type Counter struct {
	mu sync.Mutex
	n  int64
}

// Add atomically adds delta to the counter.
func (c *Counter) Add(delta int64) {
	c.mu.Lock()
	c.n += delta
	c.mu.Unlock()
}

// Value returns the current count.
func (c *Counter) Value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// ParallelSum computes the sum of fn(i) for i in [0,n) using a fixed pool of
// worker goroutines. It demonstrates the fan-out/fan-in channel pattern that
// recurs throughout the Raft implementation (per-peer replicator goroutines
// feeding results back to the leader).
func ParallelSum(n, workers int, fn func(int) int64) int64 {
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan int)
	results := make(chan int64)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local int64
			for i := range jobs {
				local += fn(i)
			}
			results <- local
		}()
	}

	go func() {
		for i := 0; i < n; i++ {
			jobs <- i
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var total int64
	for partial := range results {
		total += partial
	}
	return total
}
