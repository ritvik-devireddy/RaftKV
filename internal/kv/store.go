// Package kv is the replicated state machine: a key-value map that applies
// committed Raft log entries in order.
//
// Every command carries a (ClientID, Seq) pair. A client that times out and
// retries sends the same pair again, and the store returns the cached result
// instead of applying the command twice. This is the same idempotency idea as
// a payment system's idempotency keys, applied inside the state machine.
package kv

// Op is the kind of operation a Command performs.
type Op uint8

const (
	OpGet Op = iota
	OpPut
	OpDelete
	OpCAS
)

// Command is one client operation, as stored in the Raft log.
type Command struct {
	Op       Op
	Key      string
	Value    string // Put: new value. CAS: new value.
	Expected string // CAS only. A missing key compares equal to "".
	ClientID string // empty means "do not deduplicate"
	Seq      uint64 // per-client, increasing
}

// Result is what a client gets back.
//
//	Get:    Value = current value, OK = key exists
//	Put:    OK = true
//	Delete: OK = key existed
//	CAS:    OK = swapped, Value = value after the operation
type Result struct {
	Value string
	OK    bool
}

type session struct {
	lastSeq    uint64
	lastResult Result
}

// Store is the state machine. It is not safe for concurrent use; the Raft
// apply loop is the only writer.
type Store struct {
	data     map[string]string
	sessions map[string]session
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		data:     make(map[string]string),
		sessions: make(map[string]session),
	}
}

// Apply executes a committed command and returns its result. A command whose
// Seq is not newer than the client's last applied Seq is a retry: it is not
// executed again and the cached result is returned.
//
// TODO(snapshots): sessions grow without bound; include them in snapshots and
// expire idle clients.
func (s *Store) Apply(cmd Command) Result {
	if cmd.ClientID != "" {
		if sess, ok := s.sessions[cmd.ClientID]; ok && cmd.Seq <= sess.lastSeq {
			return sess.lastResult
		}
	}

	res := s.execute(cmd)

	if cmd.ClientID != "" {
		s.sessions[cmd.ClientID] = session{lastSeq: cmd.Seq, lastResult: res}
	}
	return res
}

func (s *Store) execute(cmd Command) Result {
	switch cmd.Op {
	case OpGet:
		v, ok := s.data[cmd.Key]
		return Result{Value: v, OK: ok}

	case OpPut:
		s.data[cmd.Key] = cmd.Value
		return Result{Value: cmd.Value, OK: true}

	case OpDelete:
		_, ok := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		return Result{OK: ok}

	case OpCAS:
		cur := s.data[cmd.Key] // missing key reads as ""
		if cur != cmd.Expected {
			return Result{Value: cur, OK: false}
		}
		s.data[cmd.Key] = cmd.Value
		return Result{Value: cmd.Value, OK: true}

	default:
		return Result{}
	}
}

// Snapshot returns a copy of the current data. It is for tests and, later,
// snapshotting; like every other Store method it must not race with Apply.
func (s *Store) Snapshot() map[string]string {
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
