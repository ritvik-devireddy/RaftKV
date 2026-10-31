package kv

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	cmds := []Command{
		{},
		{Op: OpPut, Key: "k", Value: "v", ClientID: "c", Seq: 7},
		{Op: OpCAS, Key: "k", Expected: "", Value: "new", ClientID: "client-1", Seq: 1 << 60},
		{Op: OpGet, Key: strings.Repeat("x", 300)},
		{Op: OpDelete, Key: "ключ", Value: "\x00\xff"},
	}
	for _, c := range cmds {
		b := c.Encode()
		if len(b) == 0 {
			t.Fatalf("%+v encoded to nothing; empty means no-op to raft", c)
		}
		got, err := DecodeCommand(b)
		if err != nil || !reflect.DeepEqual(got, c) {
			t.Fatalf("round trip of %+v gave %+v, %v", c, got, err)
		}
	}
}

func TestDecodeRejectsMalformedInput(t *testing.T) {
	good := Command{Op: OpPut, Key: "key", Value: "value", ClientID: "c", Seq: 3}.Encode()
	for cut := 0; cut < len(good); cut++ {
		if _, err := DecodeCommand(good[:cut]); err == nil {
			t.Fatalf("a command cut to %d of %d bytes decoded", cut, len(good))
		}
	}
	if _, err := DecodeCommand(append(append([]byte(nil), good...), 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
	bad := append([]byte(nil), good...)
	bad[0] = 99
	if _, err := DecodeCommand(bad); err == nil {
		t.Fatal("unknown op accepted")
	}
	// Random garbage must never panic.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		b := make([]byte, rng.Intn(40))
		rng.Read(b)
		DecodeCommand(b)
	}
}
