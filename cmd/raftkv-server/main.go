// Command raftkv-server runs one node of a RaftKV cluster.
//
//	raftkv-server -id 0 -listen :7000 -data ./data/node0 \
//	    -peers host0:7000,host1:7000,host2:7000
//
// -peers lists every node's address in node-ID order, including this node's.
// The same flags, with a different -id, start each of the other nodes.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "github.com/ritvik-devireddy/raftkv/api/raftkvpb"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
	"github.com/ritvik-devireddy/raftkv/internal/rpc"
	"github.com/ritvik-devireddy/raftkv/internal/server"
	"github.com/ritvik-devireddy/raftkv/internal/storage"
)

func main() {
	id := flag.Int("id", 0, "this node's ID (its index in -peers)")
	listen := flag.String("listen", ":7000", "address to listen on")
	peers := flag.String("peers", "", "comma-separated addresses of all nodes, in node-ID order")
	clientAddrs := flag.String("client-addrs", "", "addresses clients use to reach each node, in node-ID order (default: same as -peers); used for leader redirects")
	dataDir := flag.String("data", "./data", "directory for the write-ahead log")
	tick := flag.Duration("tick", 100*time.Millisecond, "length of one Raft tick")
	electionMin := flag.Int("election-min", 10, "minimum election timeout, in ticks")
	electionMax := flag.Int("election-max", 20, "maximum election timeout, in ticks")
	heartbeat := flag.Int("heartbeat", 2, "heartbeat interval, in ticks")
	timeout := flag.Duration("request-timeout", 5*time.Second, "how long a client request may wait to commit")
	noSync := flag.Bool("no-sync", false, "skip fsync (UNSAFE: acknowledged writes can be lost on a crash; for benchmarks only)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("node", *id)

	addrs := splitList(*peers)
	if len(addrs) == 0 || *id < 0 || *id >= len(addrs) {
		log.Error("-peers must list every node, and -id must be an index into it", "id", *id, "peers", addrs)
		os.Exit(2)
	}
	clients := addrs
	if *clientAddrs != "" {
		if clients = splitList(*clientAddrs); len(clients) != len(addrs) {
			log.Error("-client-addrs must have one address per node")
			os.Exit(2)
		}
	}
	peerAddrs := make(map[raft.NodeID]string)
	clientMap := make(map[raft.NodeID]string)
	ids := make([]raft.NodeID, len(addrs))
	for i := range addrs {
		ids[i] = raft.NodeID(i)
		peerAddrs[raft.NodeID(i)] = addrs[i]
		clientMap[raft.NodeID(i)] = clients[i]
	}

	wal, err := storage.Open(*dataDir, storage.Options{NoSync: *noSync})
	if err != nil {
		log.Error("cannot open the write-ahead log", "dir", *dataDir, "err", err)
		os.Exit(1)
	}
	defer wal.Close()

	transport, err := rpc.NewTransport(raft.NodeID(*id), peerAddrs, log)
	if err != nil {
		log.Error("cannot set up peer connections", "err", err)
		os.Exit(1)
	}
	defer transport.Close()

	srv, err := server.New(raft.Config{
		ID:              raft.NodeID(*id),
		Peers:           ids,
		ElectionTickMin: *electionMin,
		ElectionTickMax: *electionMax,
		HeartbeatTick:   *heartbeat,
		Seed:            time.Now().UnixNano(),
	}, wal, transport)
	if err != nil {
		log.Error("cannot start the node", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("cannot listen", "addr", *listen, "err", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(rpc.MaxMessageSize), grpc.MaxSendMsgSize(rpc.MaxMessageSize))
	pb.RegisterKVServer(grpcServer, rpc.NewKVService(srv, clientMap, *timeout))
	pb.RegisterRaftServer(grpcServer, rpc.NewRaftService(srv))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go srv.Run(ctx, *tick)
	go logRoleChanges(ctx, srv, log)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("grpc server stopped", "err", err)
			stop()
		}
	}()

	st := srv.Status()
	log.Info("node started", "listen", *listen, "peers", addrs, "term", st.Term, "log_len", st.LastLogIndex)

	<-ctx.Done()
	log.Info("shutting down")
	stopped := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
	}
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// logRoleChanges prints a line whenever the node's role, term or leader changes.
func logRoleChanges(ctx context.Context, srv *server.Server, log *slog.Logger) {
	var last raft.Status
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st := srv.Status()
			if st.Role != last.Role || st.Term != last.Term || st.Leader != last.Leader {
				log.Info("raft state", "role", st.Role, "term", st.Term, "leader", st.Leader, "commit", st.CommitIndex)
				last = st
			}
		}
	}
}
