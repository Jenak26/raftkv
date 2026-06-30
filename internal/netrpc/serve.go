package netrpc

import (
	"net"
	"net/rpc"
	"time"

	"github.com/janak/raftkv/internal/kv"
	"github.com/janak/raftkv/internal/transport"
)

// Server hosts one node's RPC endpoints (Raft, and optionally KV) on a listener.
// Each node uses its own rpc.Server rather than the package-global default so that
// many nodes can run in one process (the loopback test) without colliding.
type Server struct {
	ln     net.Listener
	rpcSrv *rpc.Server
}

// Serve registers the Raft handler (and the KV service, if kvSrv is non-nil) on a
// fresh rpc.Server and starts accepting connections on ln in the background.
// kvTimeout bounds each client Submit on the server side.
func Serve(ln net.Listener, raftHandler transport.Server, kvSrv *kv.Server, kvTimeout time.Duration) (*Server, error) {
	rpcSrv := rpc.NewServer()
	if err := rpcSrv.RegisterName("Raft", &raftService{h: raftHandler}); err != nil {
		return nil, err
	}
	if kvSrv != nil {
		if err := rpcSrv.RegisterName("KV", &kvService{srv: kvSrv, timeout: kvTimeout}); err != nil {
			return nil, err
		}
	}
	s := &Server{ln: ln, rpcSrv: rpcSrv}
	go s.acceptLoop()
	return s, nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go s.rpcSrv.ServeConn(conn)
	}
}

// Addr returns the address the server is listening on (useful when listening on
// :0 to obtain an OS-assigned port).
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Close stops accepting new connections. In-flight connections are not forcibly
// torn down; a process exit handles that.
func (s *Server) Close() error { return s.ln.Close() }
