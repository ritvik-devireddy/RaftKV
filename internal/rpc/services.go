package rpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/ritvik-devireddy/raftkv/api/raftkvpb"
	"github.com/ritvik-devireddy/raftkv/internal/kv"
	"github.com/ritvik-devireddy/raftkv/internal/raft"
	"github.com/ritvik-devireddy/raftkv/internal/server"
)

// RaftService receives Raft messages from other nodes.
type RaftService struct {
	pb.UnimplementedRaftServer
	srv *server.Server
}

func NewRaftService(srv *server.Server) *RaftService { return &RaftService{srv: srv} }

// Step hands the message to the node and acknowledges receipt. Any reply the
// node produces travels back as a separate message.
func (s *RaftService) Step(_ context.Context, m *pb.RaftMessage) (*pb.StepAck, error) {
	msg, err := FromProto(m)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.srv.Step(msg)
	return &pb.StepAck{}, nil
}

// KVService is the client-facing API.
type KVService struct {
	pb.UnimplementedKVServer
	srv     *server.Server
	clients map[raft.NodeID]string // client-facing address of each node, for redirects
	timeout time.Duration
}

// NewKVService wraps srv. clientAddrs maps node IDs to the addresses clients
// should use to reach them; timeout bounds how long a request waits to commit.
func NewKVService(srv *server.Server, clientAddrs map[raft.NodeID]string, timeout time.Duration) *KVService {
	return &KVService{srv: srv, clients: clientAddrs, timeout: timeout}
}

// run executes a command. Exactly one of the three results is meaningful: a
// result, a redirect (this node is not the leader), or an error.
func (k *KVService) run(ctx context.Context, cmd kv.Command) (kv.Result, *pb.RedirectInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()

	res, err := k.srv.Execute(ctx, cmd)
	var notLeader *server.NotLeaderError
	switch {
	case err == nil:
		return res, nil, nil
	case errors.As(err, &notLeader):
		return kv.Result{}, &pb.RedirectInfo{NotLeader: true, LeaderHint: k.clients[notLeader.Leader]}, nil
	case errors.Is(err, server.ErrLostLeadership):
		return kv.Result{}, nil, status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return kv.Result{}, nil, status.Error(codes.DeadlineExceeded, "timed out waiting for the command to commit")
	case errors.Is(err, context.Canceled):
		return kv.Result{}, nil, status.Error(codes.Canceled, err.Error())
	default:
		return kv.Result{}, nil, status.Error(codes.Internal, err.Error())
	}
}

func (k *KVService) Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	res, redirect, err := k.run(ctx, kv.Command{Op: kv.OpGet, Key: r.GetKey(), ClientID: r.GetClientId(), Seq: r.GetSeq()})
	if err != nil || redirect != nil {
		return &pb.GetResponse{Redirect: redirect}, err
	}
	return &pb.GetResponse{Found: res.OK, Value: res.Value}, nil
}

func (k *KVService) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	_, redirect, err := k.run(ctx, kv.Command{Op: kv.OpPut, Key: r.GetKey(), Value: r.GetValue(), ClientID: r.GetClientId(), Seq: r.GetSeq()})
	if err != nil || redirect != nil {
		return &pb.PutResponse{Redirect: redirect}, err
	}
	return &pb.PutResponse{}, nil
}

func (k *KVService) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	res, redirect, err := k.run(ctx, kv.Command{Op: kv.OpDelete, Key: r.GetKey(), ClientID: r.GetClientId(), Seq: r.GetSeq()})
	if err != nil || redirect != nil {
		return &pb.DeleteResponse{Redirect: redirect}, err
	}
	return &pb.DeleteResponse{Existed: res.OK}, nil
}

func (k *KVService) CompareAndSwap(ctx context.Context, r *pb.CASRequest) (*pb.CASResponse, error) {
	res, redirect, err := k.run(ctx, kv.Command{Op: kv.OpCAS, Key: r.GetKey(), Expected: r.GetExpected(), Value: r.GetValue(), ClientID: r.GetClientId(), Seq: r.GetSeq()})
	if err != nil || redirect != nil {
		return &pb.CASResponse{Redirect: redirect}, err
	}
	return &pb.CASResponse{Swapped: res.OK, Current: res.Value}, nil
}
