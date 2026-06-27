package rlog

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
)

func TestLogLineCarriesSeedTimeNodeTermRole(t *testing.T) {
	clk := clock.NewMockClock(time.Unix(0, 0))
	var buf bytes.Buffer
	lg := New(42, clk, &buf)

	clk.Advance(500 * time.Millisecond)
	lg.Logf(2, 5, raft.Leader, "appended %d entries", 3)

	line := buf.String()
	for _, want := range []string{"seed=42", "t=+500ms", "n=2", "term=5", "role=Leader", "appended 3 entries"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line missing %q\ngot: %s", want, line)
		}
	}
	if !strings.HasSuffix(line, "\n") {
		t.Errorf("log line should end in a newline, got %q", line)
	}
}

func TestTimestampIsRelativeToSimulationStart(t *testing.T) {
	clk := clock.NewMockClock(time.Unix(1000, 0)) // non-zero start
	var buf bytes.Buffer
	lg := New(7, clk, &buf)

	clk.Advance(1500 * time.Millisecond)
	lg.Logf(0, 1, raft.Follower, "tick")

	if !strings.Contains(buf.String(), "t=+1.5s") {
		t.Errorf("timestamp should be relative to start (want t=+1.5s)\ngot: %s", buf.String())
	}
}
