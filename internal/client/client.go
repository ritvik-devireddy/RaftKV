// Package client is a RaftKV client that finds the leader, follows redirects,
// and retries safely.
//
// Each logical operation is given one (ClientID, Seq) pair up front and every
// retry of it reuses that pair. If an attempt times out after the command
// actually committed, the retry returns the original result instead of running
// the command a second time.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/ritvik-devireddy/raftkv/api/raftkvpb"
)

// Client talks to a cluster. Operations on one Client run one at a time, which
// keeps its sequence numbers in order; use several Clients for concurrency.
type Client struct {
	id string

	mu      sync.Mutex // serializes operations and guards the fields below
	seq     uint64
	addrs   []string
	next    int // index of the address to try next
	conns   map[string]*grpc.ClientConn
	attempt time.Duration
}

// New creates a client for the cluster reachable at addrs (any subset of the
// nodes will do; redirects fill in the rest).
func New(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client: no addresses")
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	return &Client{
		id:      hex.EncodeToString(b[:]),
		addrs:   append([]string(nil), addrs...),
		conns:   make(map[string]*grpc.ClientConn),
		attempt: 3 * time.Second,
	}, nil
}

// Close releases all connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *Client) kvClient(addr string) (pb.KVClient, error) {
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns[addr] = conn
	}
	return pb.NewKVClient(conn), nil
}

func (c *Client) known(addr string) bool {
	for _, a := range c.addrs {
		if a == addr {
			return true
		}
	}
	return false
}

// do runs one logical operation to completion or until ctx ends. call performs
// a single attempt against one node and returns its redirect, if any.
func (c *Client) do(ctx context.Context, call func(ctx context.Context, kv pb.KVClient, clientID string, seq uint64) (*pb.RedirectInfo, error)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	seq := c.seq

	var lastErr error
	for try := 0; ; try++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("%w (last error: %v)", err, lastErr)
			}
			return err
		}
		addr := c.addrs[c.next%len(c.addrs)]
		kvc, err := c.kvClient(addr)
		if err != nil {
			return err
		}

		actx, cancel := context.WithTimeout(ctx, c.attempt)
		redirect, err := call(actx, kvc, c.id, seq)
		cancel()

		switch {
		case err == nil && redirect == nil:
			return nil
		case err == nil && redirect.GetNotLeader():
			lastErr = fmt.Errorf("%s is not the leader", addr)
			if hint := redirect.GetLeaderHint(); hint != "" && c.known(hint) {
				for i, a := range c.addrs {
					if a == hint {
						c.next = i
					}
				}
				continue // go straight to the leader
			}
			c.next++ // no hint: try the next node, after a pause for an election
		default:
			if code := status.Code(err); code == codes.InvalidArgument || code == codes.Internal {
				return err // retrying will not help
			}
			lastErr = err
			c.next++
		}

		// Back off a little so a cluster mid-election is not hammered.
		select {
		case <-ctx.Done():
		case <-time.After(backoff(try)):
		}
	}
}

func backoff(try int) time.Duration {
	d := 50 * time.Millisecond << min(try, 4)
	return min(d, 800*time.Millisecond)
}

// Get returns the value for key and whether it exists. The read goes through
// the Raft log, so it is linearizable.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	err = c.do(ctx, func(ctx context.Context, kv pb.KVClient, id string, seq uint64) (*pb.RedirectInfo, error) {
		r, err := kv.Get(ctx, &pb.GetRequest{Key: key, ClientId: id, Seq: seq})
		if err != nil {
			return nil, err
		}
		value, found = r.GetValue(), r.GetFound()
		return r.GetRedirect(), nil
	})
	return
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	return c.do(ctx, func(ctx context.Context, kv pb.KVClient, id string, seq uint64) (*pb.RedirectInfo, error) {
		r, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: value, ClientId: id, Seq: seq})
		if err != nil {
			return nil, err
		}
		return r.GetRedirect(), nil
	})
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (existed bool, err error) {
	err = c.do(ctx, func(ctx context.Context, kv pb.KVClient, id string, seq uint64) (*pb.RedirectInfo, error) {
		r, err := kv.Delete(ctx, &pb.DeleteRequest{Key: key, ClientId: id, Seq: seq})
		if err != nil {
			return nil, err
		}
		existed = r.GetExisted()
		return r.GetRedirect(), nil
	})
	return
}

// CompareAndSwap sets key to value if its current value is expected (a missing
// key counts as ""). It returns whether it swapped and the value afterwards.
func (c *Client) CompareAndSwap(ctx context.Context, key, expected, value string) (swapped bool, current string, err error) {
	err = c.do(ctx, func(ctx context.Context, kv pb.KVClient, id string, seq uint64) (*pb.RedirectInfo, error) {
		r, err := kv.CompareAndSwap(ctx, &pb.CASRequest{Key: key, Expected: expected, Value: value, ClientId: id, Seq: seq})
		if err != nil {
			return nil, err
		}
		swapped, current = r.GetSwapped(), r.GetCurrent()
		return r.GetRedirect(), nil
	})
	return
}
