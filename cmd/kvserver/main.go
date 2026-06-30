// Command kvserver runs a single KV+Raft node: it joins a cluster over net/rpc,
// persists its state to disk, and serves client put/get/del/cas requests.
//
// Example: a 3-node cluster on one machine (three terminals), each with its own
// data directory:
//
//	kvserver -id 0 -peers "0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002" -data ./data/0
//	kvserver -id 1 -peers "0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002" -data ./data/1
//	kvserver -id 2 -peers "0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002" -data ./data/2
//
// Drive it with kvctl pointed at the same addresses.
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/janak/raftkv/internal/clock"
	"github.com/janak/raftkv/internal/kv"
	"github.com/janak/raftkv/internal/netrpc"
	"github.com/janak/raftkv/internal/raft"
	"github.com/janak/raftkv/internal/storage"
)

func main() {
	id := flag.Int("id", -1, "this node's id (must appear in -peers)")
	peersStr := flag.String("peers", "", `cluster as "id=host:port,id=host:port,..." including this node`)
	dataDir := flag.String("data", "", "directory for durable Raft state (required)")
	kvTimeout := flag.Duration("kv-timeout", 2*time.Second, "server-side deadline for a client request to commit")
	rpcTimeout := flag.Duration("rpc-timeout", 200*time.Millisecond, "per-call deadline for outbound Raft RPCs")
	maxRaftState := flag.Int("snapshot-threshold", 64*1024, "snapshot (compact the log) once persisted Raft state exceeds this many bytes; 0 disables")
	flag.Parse()

	if *id < 0 || *peersStr == "" || *dataDir == "" {
		flag.Usage()
		os.Exit(2)
	}

	addrs, ids, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("kvserver: -peers: %v", err)
	}
	listenAddr, ok := addrs[*id]
	if !ok {
		log.Fatalf("kvserver: -id %d not found in -peers", *id)
	}

	persister, err := storage.NewFileStorage(*dataDir)
	if err != nil {
		log.Fatalf("kvserver: storage: %v", err)
	}

	tr := netrpc.NewRaftTransport(*id, addrs, *rpcTimeout)
	applyCh := make(chan raft.ApplyMsg, 1024)
	rf := raft.Make(raft.Config{
		ID:        *id,
		Peers:     ids,
		Transport: tr,
		Persister: persister,
		Clock:     clock.NewRealClock(),
		ApplyCh:   applyCh,
		Rand:      rand.New(rand.NewSource(time.Now().UnixNano() + int64(*id))),
	})
	kvSrv := kv.NewServer(rf, applyCh, kv.NewMapStateMachine(), *maxRaftState)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("kvserver: listen on %s: %v", listenAddr, err)
	}
	srv, err := netrpc.Serve(ln, rf, kvSrv, *kvTimeout)
	if err != nil {
		log.Fatalf("kvserver: serve: %v", err)
	}

	log.Printf("kvserver: node %d listening on %s, peers=%v, data=%s", *id, listenAddr, ids, *dataDir)

	// Block until interrupted, then shut down cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("kvserver: node %d shutting down", *id)
	srv.Close()
	rf.Kill()
	kvSrv.Stop()
	tr.Close()
}

// parsePeers parses "id=host:port,id=host:port,..." into an address map and a
// sorted slice of ids.
func parsePeers(s string) (map[int]string, []int, error) {
	addrs := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			return nil, nil, fmt.Errorf("bad entry %q (want id=host:port)", part)
		}
		id, err := strconv.Atoi(strings.TrimSpace(part[:eq]))
		if err != nil {
			return nil, nil, fmt.Errorf("bad id in %q: %w", part, err)
		}
		addr := strings.TrimSpace(part[eq+1:])
		if addr == "" {
			return nil, nil, fmt.Errorf("empty address in %q", part)
		}
		if _, dup := addrs[id]; dup {
			return nil, nil, fmt.Errorf("duplicate id %d", id)
		}
		addrs[id] = addr
	}
	if len(addrs) == 0 {
		return nil, nil, fmt.Errorf("no peers given")
	}
	ids := make([]int, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return addrs, ids, nil
}
