// Package clock abstracts the passage of time so that the rest of the system
// never calls time.Now or time.After directly.
//
// This indirection is the foundation of Deterministic Simulation Testing (see
// DIFFERENTIATION.md): in production we use RealClock, but in simulation tests
// we use MockClock, whose time only advances when the test explicitly calls
// Advance. That lets a test compress hours of election timeouts and heartbeats
// into microseconds and, crucially, makes timing reproducible from a seed.
package clock

import (
	"sync"
	"time"
)

// Clock is the minimal time interface the system depends on. Any code that
// needs the current time, a timer, or to sleep takes a Clock rather than
// touching the time package directly.
type Clock interface {
	// Now returns the clock's current time.
	Now() time.Time
	// After returns a channel that delivers the time once d has elapsed
	// according to this clock. For MockClock the channel only fires when
	// simulated time is advanced past the deadline.
	After(d time.Duration) <-chan time.Time
	// Sleep blocks until d has elapsed according to this clock.
	Sleep(d time.Duration)
}

// RealClock is the production Clock, backed by the operating system.
type RealClock struct{}

// NewRealClock returns a Clock backed by the real OS clock.
func NewRealClock() RealClock { return RealClock{} }

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }

// waiter is a pending After/Sleep that fires once simulated time reaches deadline.
type waiter struct {
	deadline time.Time
	ch       chan time.Time
}

// MockClock is a deterministic Clock whose time only moves forward when Advance
// is called. It is safe for concurrent use: many goroutines may register timers
// via After/Sleep while another goroutine (typically the simulation scheduler)
// drives time with Advance.
type MockClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

// NewMockClock returns a MockClock whose current time is start.
func NewMockClock(start time.Time) *MockClock {
	return &MockClock{now: start}
}

// Now returns the current simulated time.
func (m *MockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// After registers a timer for d from now. The returned channel is buffered, so
// firing it never blocks Advance.
func (m *MockClock) After(d time.Duration) <-chan time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- m.now
		return ch
	}
	m.waiters = append(m.waiters, &waiter{deadline: m.now.Add(d), ch: ch})
	return ch
}

// Sleep blocks until simulated time has advanced by d. It must be called from a
// goroutine other than the one driving Advance, otherwise it deadlocks.
func (m *MockClock) Sleep(d time.Duration) {
	<-m.After(d)
}

// Advance moves simulated time forward by d and fires every waiter whose
// deadline now lies at or before the new time.
func (m *MockClock) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	now := m.now
	var fired []*waiter
	remaining := m.waiters[:0]
	for _, w := range m.waiters {
		if w.deadline.After(now) {
			remaining = append(remaining, w)
		} else {
			fired = append(fired, w)
		}
	}
	m.waiters = remaining
	m.mu.Unlock()

	// Fire outside the lock; channels are buffered so this never blocks.
	for _, w := range fired {
		w.ch <- now
	}
}
