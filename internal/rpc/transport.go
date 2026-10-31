package rpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/ritvik-devireddy/raftkv/api/raftkvpb"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

const (
	peerQueueSize = 256
	sendTimeout   = 500 * time.Millisecond
)

// Transport implements raft.Transport over gRPC.
//
// Raft tolerates lost, delayed, duplicated and reordered messages, so Send is
// best effort and never blocks: each peer has a bounded queue drained by one
// goroutine, and a message that does not fit (the peer is down or slow) is
// dropped. The node holds its lock while sending, which is why Send must not
// wait on the network.
type Transport struct {
	log   *slog.Logger
	peers map[raft.NodeID]*peer

	closeOnce sync.Once
	done      chan struct{}
}

type peer struct {
	id     raft.NodeID
	addr   string
	conn   *grpc.ClientConn
	client pb.RaftClient
	queue  chan *pb.RaftMessage
}

// NewTransport prepares connections to every address except self's. gRPC
// connects lazily, so a peer that is not up yet is not an error.
func NewTransport(self raft.NodeID, addrs map[raft.NodeID]string, log *slog.Logger) (*Transport, error) {
	t := &Transport{log: log, peers: make(map[raft.NodeID]*peer), done: make(chan struct{})}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Close()
			return nil, err
		}
		p := &peer{id: id, addr: addr, conn: conn, client: pb.NewRaftClient(conn), queue: make(chan *pb.RaftMessage, peerQueueSize)}
		t.peers[id] = p
		go t.drain(p)
	}
	return t, nil
}

// Send queues a message for its destination. It never blocks.
func (t *Transport) Send(m raft.Message) {
	p := t.peers[m.To]
	if p == nil {
		return
	}
	wire, err := ToProto(m)
	if err != nil {
		t.log.Error("cannot encode raft message", "err", err)
		return
	}
	select {
	case p.queue <- wire:
	default: // peer too slow or down: drop, Raft retransmits
	}
}

func (t *Transport) drain(p *peer) {
	var lastWarn time.Time
	for {
		select {
		case <-t.done:
			return
		case m := <-p.queue:
			ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
			_, err := p.client.Step(ctx, m)
			cancel()
			if err != nil && time.Since(lastWarn) > 5*time.Second {
				lastWarn = time.Now()
				t.log.Warn("cannot reach peer", "peer", p.id, "addr", p.addr, "err", err)
			}
		}
	}
}

// Close stops sending and closes the connections.
func (t *Transport) Close() {
	t.closeOnce.Do(func() {
		close(t.done)
		for _, p := range t.peers {
			p.conn.Close()
		}
	})
}
