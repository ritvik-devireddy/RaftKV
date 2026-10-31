package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Commands travel through the Raft log as opaque bytes. The encoding is
//
//	op (1) | seq (8, little endian) | key | value | expected | clientID
//
// where each string is a uvarint length followed by that many bytes. An
// encoded command is never empty, which matters because the Raft layer uses an
// empty command to mean "no-op".

var errShort = errors.New("kv: truncated command")

// Encode returns the command's wire form.
func (c Command) Encode() []byte {
	buf := make([]byte, 0, 9+4*binary.MaxVarintLen32+len(c.Key)+len(c.Value)+len(c.Expected)+len(c.ClientID))
	buf = append(buf, byte(c.Op))
	buf = binary.LittleEndian.AppendUint64(buf, c.Seq)
	for _, s := range [...]string{c.Key, c.Value, c.Expected, c.ClientID} {
		buf = binary.AppendUvarint(buf, uint64(len(s)))
		buf = append(buf, s...)
	}
	return buf
}

// DecodeCommand parses the output of Encode. It rejects anything malformed,
// including trailing bytes and an unknown operation.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < 9 {
		return Command{}, errShort
	}
	c := Command{Op: Op(b[0]), Seq: binary.LittleEndian.Uint64(b[1:9])}
	if c.Op > OpCAS {
		return Command{}, fmt.Errorf("kv: unknown operation %d", c.Op)
	}
	rest := b[9:]
	fields := [...]*string{&c.Key, &c.Value, &c.Expected, &c.ClientID}
	for _, f := range fields {
		n, w := binary.Uvarint(rest)
		if w <= 0 {
			return Command{}, errShort
		}
		rest = rest[w:]
		if n > uint64(len(rest)) {
			return Command{}, errShort
		}
		*f = string(rest[:n])
		rest = rest[n:]
	}
	if len(rest) != 0 {
		return Command{}, fmt.Errorf("kv: %d trailing bytes after command", len(rest))
	}
	return c, nil
}
