package warmup

import (
	"sync"
	"testing"
)

// TestCounterConcurrent hammers the counter from many goroutines. Run with
// `go test -race` - this is the canary that proves the race detector is active.
func TestCounterConcurrent(t *testing.T) {
	var c Counter
	const goroutines, perGoroutine = 100, 1000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				c.Add(1)
			}
		}()
	}
	wg.Wait()

	if got, want := c.Value(), int64(goroutines*perGoroutine); got != want {
		t.Fatalf("Counter.Value() = %d, want %d", got, want)
	}
}

func TestParallelSum(t *testing.T) {
	// Sum of 0..999 = 999*1000/2 = 499500.
	got := ParallelSum(1000, 8, func(i int) int64 { return int64(i) })
	if want := int64(499500); got != want {
		t.Fatalf("ParallelSum = %d, want %d", got, want)
	}
}

func TestParallelSumDefaultsToOneWorker(t *testing.T) {
	got := ParallelSum(10, 0, func(int) int64 { return 1 })
	if got != 10 {
		t.Fatalf("ParallelSum with 0 workers = %d, want 10", got)
	}
}
