// Command raftviz is a live, interactive visualizer for the Raft cluster. It runs
// a 5-node cluster in-process over the simulated network (real clock, so elections
// happen in real time) and serves a web UI that shows each node's role, term,
// commit index, and log length, with buttons to propose entries, crash/restart
// nodes, and isolate/rejoin them. Kill the leader and watch a new one get elected.
//
// It is the project's public live demo (deployable as a single container, e.g. a
// Hugging Face Docker Space). It listens on $PORT (default 7860).
package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
	"github.com/janak/raftkv/internal/transport/memnet"
)

//go:embed web/index.html
var webFS embed.FS

const numNodes = 5

// viz owns the in-process cluster and the per-node liveness/connectivity it tracks.
type viz struct {
	mu         sync.Mutex
	net        *memnet.Network
	clk        clock.Clock
	persisters map[int]storage.Persister
	nodes      map[int]*raft.Raft
	stops      map[int]chan struct{} // per-node applyCh drain stoppers
	up         map[int]bool
	connected  map[int]bool
	proposed   int
}

func newViz() *viz {
	v := &viz{
		clk:        clock.NewRealClock(),
		persisters: map[int]storage.Persister{},
		nodes:      map[int]*raft.Raft{},
		stops:      map[int]chan struct{}{},
		up:         map[int]bool{},
		connected:  map[int]bool{},
	}
	v.net = memnet.New(time.Now().UnixNano(), v.clk)
	for id := 0; id < numNodes; id++ {
		v.persisters[id] = storage.NewInMemoryPersister()
		v.start(id)
	}
	return v
}

func peerIDs() []int {
	ids := make([]int, numNodes)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

// start (re)creates node id from its persister and attaches it to the network.
// Must hold v.mu (or be called during construction).
func (v *viz) start(id int) {
	ch := make(chan raft.ApplyMsg, 512)
	rf := raft.Make(raft.Config{
		ID: id, Peers: peerIDs(), Transport: v.net.Transport(id),
		Persister: v.persisters[id], Clock: v.clk, ApplyCh: ch,
		Rand: rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*131)),
	})
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-ch: // drain committed entries; the viz only needs counts
			}
		}
	}()
	v.nodes[id] = rf
	v.stops[id] = stop
	v.up[id] = true
	v.connected[id] = true
	v.net.AddNode(id, rf) // (re)attach a connected endpoint for this node
}

func (v *viz) crash(id int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.up[id] {
		return
	}
	v.nodes[id].Kill()
	close(v.stops[id])
	v.net.Crash(id)
	delete(v.nodes, id)
	v.up[id] = false
}

func (v *viz) restart(id int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.up[id] {
		return
	}
	v.start(id)
}

func (v *viz) setConnected(id int, c bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if c {
		v.net.Connect(id)
	} else {
		v.net.Disconnect(id)
	}
	v.connected[id] = c
}

func (v *viz) heal() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.net.Heal()
	for id := 0; id < numNodes; id++ {
		v.net.Connect(id)
		v.connected[id] = true
	}
}

// propose appends one entry to the current leader's log; returns false if there is
// no leader right now (e.g. mid-election).
func (v *viz) propose() bool {
	v.mu.Lock()
	v.proposed++
	cmd := []byte(fmt.Sprintf("v%d", v.proposed))
	nodes := make([]*raft.Raft, 0, len(v.nodes))
	for _, rf := range v.nodes {
		nodes = append(nodes, rf)
	}
	v.mu.Unlock()
	for _, rf := range nodes {
		if _, _, ok := rf.Propose(cmd); ok {
			return true
		}
	}
	return false
}

type nodeState struct {
	ID        int    `json:"id"`
	Up        bool   `json:"up"`
	Connected bool   `json:"connected"`
	Role      string `json:"role"`
	Term      int    `json:"term"`
	Commit    int    `json:"commit"`
	Log       int    `json:"log"`
	Leader    bool   `json:"leader"`
}

func (v *viz) state() []nodeState {
	v.mu.Lock()
	type ref struct {
		rf            *raft.Raft
		up, connected bool
	}
	refs := map[int]ref{}
	for id := 0; id < numNodes; id++ {
		refs[id] = ref{rf: v.nodes[id], up: v.up[id], connected: v.connected[id]}
	}
	v.mu.Unlock()

	out := make([]nodeState, 0, numNodes)
	for id := 0; id < numNodes; id++ {
		r := refs[id]
		ns := nodeState{ID: id, Up: r.up, Connected: r.connected, Role: "Down"}
		if r.up && r.rf != nil {
			m := r.rf.Metrics()
			ns.Role = m.Role
			ns.Term = m.Term
			ns.Commit = m.CommitIndex
			ns.Log = m.LogEntries
			ns.Leader = m.Role == "Leader"
			if !r.connected {
				ns.Role = "Isolated:" + m.Role
			}
		}
		out = append(out, ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func main() {
	v := newViz()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"nodes": v.state()})
	})
	mux.HandleFunc("/api/propose", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"ok": v.propose()})
	})
	mux.HandleFunc("/api/heal", func(w http.ResponseWriter, r *http.Request) {
		v.heal()
		writeJSON(w, map[string]any{"ok": true})
	})
	mux.HandleFunc("/api/node", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.URL.Query().Get("id"))
		if err != nil || id < 0 || id >= numNodes {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		switch r.URL.Query().Get("action") {
		case "crash":
			v.crash(id)
		case "restart":
			v.restart(id)
		case "isolate":
			v.setConnected(id, false)
		case "rejoin":
			v.setConnected(id, true)
		default:
			http.Error(w, "bad action", http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	})
	mux.Handle("/", http.FileServer(http.FS(webFS)))
	// Serve the embedded index.html at the root path.
	mux.HandleFunc("/index.html", func(w http.ResponseWriter, r *http.Request) {
		b, _ := webFS.ReadFile("web/index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(b)
	})
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
		b, _ := webFS.ReadFile("web/index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(b)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "7860"
	}
	addr := "0.0.0.0:" + port
	log.Printf("raftviz: serving a %d-node cluster on http://%s", numNodes, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
