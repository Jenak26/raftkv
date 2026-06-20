package clock

import (
	"testing"
	"time"
)

func TestRealClockNowMovesForward(t *testing.T) {
	c := NewRealClock()
	t0 := c.Now()
	c.Sleep(time.Millisecond)
	if !c.Now().After(t0) {
		t.Fatal("RealClock.Now did not advance after Sleep")
	}
}

func TestMockClockNowAndAdvance(t *testing.T) {
	start := time.Unix(0, 0)
	c := NewMockClock(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now() = %v, want %v", c.Now(), start)
	}
	c.Advance(5 * time.Second)
	if got, want := c.Now(), start.Add(5*time.Second); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v", got, want)
	}
}

func TestMockClockAfterDoesNotFireEarly(t *testing.T) {
	c := NewMockClock(time.Unix(0, 0))
	ch := c.After(100 * time.Millisecond)

	c.Advance(99 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("timer fired before its deadline")
	default:
	}

	c.Advance(time.Millisecond) // crosses the deadline exactly
	select {
	case got := <-ch:
		if want := time.Unix(0, 0).Add(100 * time.Millisecond); !got.Equal(want) {
			t.Fatalf("timer delivered %v, want %v", got, want)
		}
	default:
		t.Fatal("timer did not fire at its deadline")
	}
}

func TestMockClockAfterNonPositiveFiresImmediately(t *testing.T) {
	c := NewMockClock(time.Unix(0, 0))
	select {
	case <-c.After(0):
	default:
		t.Fatal("After(0) should fire immediately")
	}
}

// TestMockClockSleepUnblocksConcurrently exercises the concurrent path (a
// goroutine sleeping while another advances time) so that `go test -race`
// validates the locking.
func TestMockClockSleepUnblocksConcurrently(t *testing.T) {
	c := NewMockClock(time.Unix(0, 0))
	done := make(chan struct{})
	go func() {
		c.Sleep(50 * time.Millisecond)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-done:
			return
		case <-deadline:
			t.Fatal("Sleep did not unblock after advancing past its deadline")
		default:
			c.Advance(10 * time.Millisecond)
			time.Sleep(time.Millisecond) // let the sleeper register/wake
		}
	}
}
