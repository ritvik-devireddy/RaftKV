package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/ritvik-devireddy/raftkv/internal/raft"
)

// FileName is the name of the log file inside the WAL directory.
const FileName = "raft.wal"

const (
	headerSize = 8        // crc32c (4) + body length (4)
	maxBody    = 64 << 20 // a longer length field can only be corruption

	recHardState = 1 // term u64, votedFor i64
	recEntry     = 2 // term u64, index u64, command bytes
	recTruncate  = 3 // index u64: delete this entry and everything after it
)

var (
	// ErrCorrupt means the log is damaged somewhere other than a torn final
	// record. Recovery refuses to guess: silently dropping committed history
	// could break Raft's safety guarantees, so a human has to look.
	ErrCorrupt = errors.New("wal: corrupt")
	// ErrPoisoned means an earlier write or sync failed. After a failed fsync
	// the file's contents are unknowable, so nothing more may be written.
	ErrPoisoned = errors.New("wal: unusable after an earlier I/O failure")
	// ErrTooLarge means a single log entry exceeds what recovery can read back.
	ErrTooLarge = errors.New("wal: entry too large")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Options tune a WAL.
type Options struct {
	// NoSync skips fsync. A crash can then lose acknowledged writes, which
	// breaks Raft's guarantees, so this exists only for benchmarks.
	NoSync bool
}

// WAL is a write-ahead log implementing raft.Storage.
//
// The file is a sequence of records:
//
//	crc32c (4, LE) | bodyLen (4, LE) | body (type byte + payload)
//
// The checksum covers the length field and the body. Every method fsyncs
// before returning, so once it returns nil the change survives a crash.
//
// Recovery (Open) replays the file. A torn final record, the result of a crash
// in the middle of a write, is discarded and the file is cut back to the last
// whole record. Damage anywhere else is reported as ErrCorrupt.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
	opts Options

	size     int64 // bytes of valid records; the next write goes here
	term     raft.Term
	votedFor raft.NodeID
	logLen   raft.Index
	poisoned bool
}

var _ raft.Storage = (*WAL)(nil)

// Open opens (creating if needed) the WAL in dir and recovers its contents.
func Open(dir string, opts Options) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create directory: %w", err)
	}
	path := filepath.Join(dir, FileName)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	fail := func(err error) (*WAL, error) {
		f.Close()
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fail(fmt.Errorf("wal: read: %w", err))
	}
	st, valid, err := replay(data)
	if err != nil {
		return fail(err)
	}
	if valid < len(data) {
		// A torn tail: cut it off so new records start on a clean boundary.
		if err := f.Truncate(int64(valid)); err != nil {
			return fail(fmt.Errorf("wal: truncate torn tail: %w", err))
		}
		if !opts.NoSync {
			if err := f.Sync(); err != nil {
				return fail(fmt.Errorf("wal: sync after truncate: %w", err))
			}
		}
	}
	if created && !opts.NoSync {
		if err := syncDir(dir); err != nil {
			return fail(err)
		}
	}

	return &WAL{
		f:        f,
		path:     path,
		opts:     opts,
		size:     int64(valid),
		term:     st.CurrentTerm,
		votedFor: st.VotedFor,
		logLen:   raft.Index(len(st.Log)),
	}, nil
}

// Close releases the file. It does not sync: every write already did.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// Path returns the log file's path.
func (w *WAL) Path() string { return w.path }

func (w *WAL) SaveHardState(term raft.Term, votedFor raft.NodeID) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poisoned {
		return ErrPoisoned
	}
	if term < w.term {
		return fmt.Errorf("wal: term would go backwards: %d -> %d", w.term, term)
	}
	if term == w.term && votedFor == w.votedFor {
		return nil // nothing changed, so skip the fsync
	}
	body := make([]byte, 17)
	body[0] = recHardState
	binary.LittleEndian.PutUint64(body[1:], uint64(term))
	binary.LittleEndian.PutUint64(body[9:], uint64(votedFor))
	if err := w.write(frame(body)); err != nil {
		return err
	}
	w.term, w.votedFor = term, votedFor
	return nil
}

func (w *WAL) Append(entries []raft.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poisoned {
		return ErrPoisoned
	}
	if len(entries) == 0 {
		return nil
	}
	var buf []byte
	for i, e := range entries {
		if want := w.logLen + raft.Index(i) + 1; e.Index != want {
			return fmt.Errorf("wal: append index %d, want %d", e.Index, want)
		}
		if 17+len(e.Command) > maxBody {
			return ErrTooLarge
		}
		body := make([]byte, 17+len(e.Command))
		body[0] = recEntry
		binary.LittleEndian.PutUint64(body[1:], uint64(e.Term))
		binary.LittleEndian.PutUint64(body[9:], uint64(e.Index))
		copy(body[17:], e.Command)
		buf = append(buf, frame(body)...)
	}
	if err := w.write(buf); err != nil {
		return err
	}
	w.logLen += raft.Index(len(entries))
	return nil
}

func (w *WAL) TruncateFrom(index raft.Index) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poisoned {
		return ErrPoisoned
	}
	if index < 1 || index > w.logLen {
		return fmt.Errorf("wal: truncate index %d outside log of length %d", index, w.logLen)
	}
	body := make([]byte, 9)
	body[0] = recTruncate
	binary.LittleEndian.PutUint64(body[1:], uint64(index))
	if err := w.write(frame(body)); err != nil {
		return err
	}
	w.logLen = index - 1
	return nil
}

// Load replays the file, so it always reflects exactly what is on disk.
func (w *WAL) Load() (raft.PersistentState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := os.ReadFile(w.path)
	if err != nil {
		return raft.PersistentState{}, fmt.Errorf("wal: read: %w", err)
	}
	st, _, err := replay(data)
	return st, err
}

// write appends buf at the end of the valid data and makes it durable. Any
// failure poisons the WAL: after a failed write or fsync the file may hold
// part of the record, and the kernel may have dropped dirty pages, so the only
// safe move is to stop and recover from disk with a fresh Open.
func (w *WAL) write(buf []byte) error {
	if _, err := w.f.WriteAt(buf, w.size); err != nil {
		w.poisoned = true
		return fmt.Errorf("wal: write: %w", err)
	}
	if !w.opts.NoSync {
		if err := w.f.Sync(); err != nil {
			w.poisoned = true
			return fmt.Errorf("wal: sync: %w", err)
		}
	}
	w.size += int64(len(buf))
	return nil
}

// frame wraps a body in the record header.
func frame(body []byte) []byte {
	buf := make([]byte, headerSize+len(body))
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(body)))
	copy(buf[headerSize:], body)
	binary.LittleEndian.PutUint32(buf[0:], crc32.Checksum(buf[4:], castagnoli))
	return buf
}

// replay rebuilds the persistent state from the file contents. It returns how
// many leading bytes are whole, valid records; anything after that is a torn
// tail for the caller to cut off.
//
// What counts as a torn tail (a crash mid-write) and what counts as corruption:
//
//   - Fewer than a header's worth of bytes, or a record that runs past the end
//     of the file: torn. The write never finished.
//   - A checksum mismatch: torn only if nothing but zeros follows the record
//     (a crash can leave the file extended with zeros, or the last record
//     half-written). If any non-zero byte follows, a later write completed, so
//     this record was damaged after being written: corrupt.
//   - A zero length: torn only if the rest of the file is zeros, else corrupt.
//   - A length beyond maxBody, or a valid record that makes no sense (a gap in
//     the log indexes, a term going backwards): corrupt.
//
// Known limitation: damage to a middle record's length field that makes it
// look like it runs past the end of the file is indistinguishable from a torn
// tail, so it is treated as one.
func replay(data []byte) (raft.PersistentState, int, error) {
	st := raft.PersistentState{VotedFor: raft.NoVote}
	off := 0
	for off < len(data) {
		rest := data[off:]
		if len(rest) < headerSize {
			break
		}
		crc := binary.LittleEndian.Uint32(rest[0:])
		bodyLen := binary.LittleEndian.Uint32(rest[4:])

		if bodyLen == 0 {
			if allZero(rest[headerSize:]) {
				break
			}
			return st, off, fmt.Errorf("%w: empty record at offset %d followed by data", ErrCorrupt, off)
		}
		if bodyLen > maxBody {
			return st, off, fmt.Errorf("%w: record at offset %d claims length %d", ErrCorrupt, off, bodyLen)
		}
		end := headerSize + int(bodyLen)
		if end > len(rest) {
			break
		}
		if crc32.Checksum(rest[4:end], castagnoli) != crc {
			if allZero(rest[end:]) {
				break
			}
			return st, off, fmt.Errorf("%w: checksum mismatch at offset %d", ErrCorrupt, off)
		}
		if err := apply(&st, rest[headerSize:end]); err != nil {
			return st, off, fmt.Errorf("%w: record at offset %d: %v", ErrCorrupt, off, err)
		}
		off += end
	}
	return st, off, nil
}

// apply replays one valid record's body onto st.
func apply(st *raft.PersistentState, body []byte) error {
	switch body[0] {
	case recHardState:
		if len(body) != 17 {
			return fmt.Errorf("hard state record has length %d", len(body))
		}
		term := raft.Term(binary.LittleEndian.Uint64(body[1:]))
		if term < st.CurrentTerm {
			return fmt.Errorf("term goes backwards: %d -> %d", st.CurrentTerm, term)
		}
		st.CurrentTerm = term
		st.VotedFor = raft.NodeID(int64(binary.LittleEndian.Uint64(body[9:])))
	case recEntry:
		if len(body) < 17 {
			return fmt.Errorf("entry record has length %d", len(body))
		}
		e := raft.LogEntry{
			Term:  raft.Term(binary.LittleEndian.Uint64(body[1:])),
			Index: raft.Index(binary.LittleEndian.Uint64(body[9:])),
		}
		if want := raft.Index(len(st.Log)) + 1; e.Index != want {
			return fmt.Errorf("entry index %d, want %d", e.Index, want)
		}
		if len(body) > 17 {
			e.Command = append([]byte(nil), body[17:]...)
		}
		st.Log = append(st.Log, e)
	case recTruncate:
		if len(body) != 9 {
			return fmt.Errorf("truncate record has length %d", len(body))
		}
		idx := raft.Index(binary.LittleEndian.Uint64(body[1:]))
		if idx < 1 || idx > raft.Index(len(st.Log)) {
			return fmt.Errorf("truncate at %d outside log of length %d", idx, len(st.Log))
		}
		st.Log = st.Log[:idx-1]
	default:
		return fmt.Errorf("unknown record type %d", body[0])
	}
	return nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// syncDir fsyncs a directory so a newly created file's directory entry is
// durable. Windows cannot open a directory for syncing; NTFS makes the entry
// durable with the file's own flush.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: sync directory: %w", err)
	}
	return nil
}
