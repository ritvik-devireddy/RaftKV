package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// The state machine's snapshot is its whole state: the key-value data AND the
// dedup sessions. Leaving the sessions out would let a replica restored from a
// snapshot apply a retried command a second time.
//
// The encoding is deterministic (keys are sorted), so every replica that has
// applied the same entries produces identical bytes:
//
//	version (1) | nKeys | { key | value }... | nSessions | { clientID | lastSeq (8, LE) | value | ok (1) }...
//
// where nKeys and nSessions are uvarints and each string is a uvarint length
// followed by that many bytes.

const stateVersion = 1

var errBadState = errors.New("kv: malformed snapshot")

// MarshalState returns the store's state in the deterministic snapshot form.
func (s *Store) MarshalState() []byte {
	buf := []byte{stateVersion}

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendString(buf, k)
		buf = appendString(buf, s.data[k])
	}

	clients := make([]string, 0, len(s.sessions))
	for c := range s.sessions {
		clients = append(clients, c)
	}
	sort.Strings(clients)
	buf = binary.AppendUvarint(buf, uint64(len(clients)))
	for _, c := range clients {
		sess := s.sessions[c]
		buf = appendString(buf, c)
		buf = binary.LittleEndian.AppendUint64(buf, sess.lastSeq)
		buf = appendString(buf, sess.lastResult.Value)
		ok := byte(0)
		if sess.lastResult.OK {
			ok = 1
		}
		buf = append(buf, ok)
	}
	return buf
}

// UnmarshalState replaces the store's state with a snapshot produced by
// MarshalState. If the snapshot is malformed it returns an error and leaves the
// store as it was.
func (s *Store) UnmarshalState(b []byte) error {
	r := reader{b: b}
	if v, ok := r.byte(); !ok || v != stateVersion {
		return fmt.Errorf("%w: version %d", errBadState, v)
	}

	nKeys, ok := r.uvarint()
	if !ok || nKeys > uint64(len(r.b)) { // each pair takes at least two bytes
		return fmt.Errorf("%w: key count", errBadState)
	}
	data := make(map[string]string, nKeys)
	for i := uint64(0); i < nKeys; i++ {
		k, ok1 := r.str()
		v, ok2 := r.str()
		if !ok1 || !ok2 {
			return fmt.Errorf("%w: truncated data", errBadState)
		}
		if _, dup := data[k]; dup {
			return fmt.Errorf("%w: duplicate key", errBadState)
		}
		data[k] = v
	}

	nSessions, ok := r.uvarint()
	if !ok || nSessions > uint64(len(r.b)) {
		return fmt.Errorf("%w: session count", errBadState)
	}
	sessions := make(map[string]session, nSessions)
	for i := uint64(0); i < nSessions; i++ {
		c, ok1 := r.str()
		seq, ok2 := r.u64()
		v, ok3 := r.str()
		flag, ok4 := r.byte()
		if !ok1 || !ok2 || !ok3 || !ok4 || flag > 1 {
			return fmt.Errorf("%w: truncated or invalid session", errBadState)
		}
		if _, dup := sessions[c]; dup {
			return fmt.Errorf("%w: duplicate client", errBadState)
		}
		sessions[c] = session{lastSeq: seq, lastResult: Result{Value: v, OK: flag == 1}}
	}
	if len(r.b) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", errBadState, len(r.b))
	}

	s.data, s.sessions = data, sessions
	return nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// reader consumes a byte slice; every method reports false on truncation.
type reader struct{ b []byte }

func (r *reader) byte() (byte, bool) {
	if len(r.b) < 1 {
		return 0, false
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v, true
}

func (r *reader) u64() (uint64, bool) {
	if len(r.b) < 8 {
		return 0, false
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v, true
}

func (r *reader) uvarint() (uint64, bool) {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		return 0, false
	}
	r.b = r.b[n:]
	return v, true
}

func (r *reader) str() (string, bool) {
	n, ok := r.uvarint()
	if !ok || n > uint64(len(r.b)) {
		return "", false
	}
	v := string(r.b[:n])
	r.b = r.b[n:]
	return v, true
}
