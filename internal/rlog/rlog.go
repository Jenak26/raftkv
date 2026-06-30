// Package rlog is the project's structured logging convention. Every node tags
// each line with its seed, the simulated time elapsed since startup, and its
// [node, term, role] identity, so that a log captured during a seeded
// simulation reads as a precise, replayable timeline of the cluster.
//
// Time comes from an injected clock.Clock (a clock.MockClock in simulation), so
// timestamps advance with simulated - not wall-clock - time and are identical on
// every replay of the same seed. That is what makes "grep the log for the bug"
// a reliable debugging workflow in this project.
package rlog

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
)

// Logger writes seed- and time-stamped log lines for the nodes of one cluster.
// It is safe for concurrent use by multiple node goroutines.
type Logger struct {
	seed  int64
	clk   clock.Clock
	start time.Time

	mu sync.Mutex
	w  io.Writer
}

// New returns a Logger that stamps every line with seed and writes to w. The
// current clk time is captured as the simulation start, so logged timestamps are
// reported relative to it (e.g. t=+500ms).
func New(seed int64, clk clock.Clock, w io.Writer) *Logger {
	return &Logger{seed: seed, clk: clk, start: clk.Now(), w: w}
}

// Logf writes one line describing an event at node id, which currently believes
// it is in the given term and role. The message is formatted like fmt.Printf.
func (l *Logger) Logf(id, term int, role raft.Role, format string, a ...any) {
	elapsed := l.clk.Now().Sub(l.start)
	msg := fmt.Sprintf(format, a...)
	line := fmt.Sprintf("seed=%d t=+%s n=%d term=%d role=%s | %s\n",
		l.seed, elapsed, id, term, role, msg)

	l.mu.Lock()
	defer l.mu.Unlock()
	io.WriteString(l.w, line)
}
