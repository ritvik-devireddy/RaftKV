package linearizability

import (
	"fmt"
	"math"
	"sort"

	"github.com/anishathalye/porcupine"

	"github.com/ritvik-devireddy/raftkv/internal/chaos"
	"github.com/ritvik-devireddy/raftkv/internal/kv"
)

// input and output are what Porcupine sees of each operation.
type input struct {
	Op       kv.Op
	Key      string
	Value    string // Put/CAS: the new value
	Expected string // CAS only
}

type output struct {
	// Unknown means the client never learned the outcome. The operation may
	// have taken effect at any time after it was called, or not at all.
	Unknown bool
	Value   string
	OK      bool
}

// keyState is the state of one key: whether it exists and what it holds.
type keyState struct {
	exists bool
	value  string
}

// Model is the sequential specification of the store, one key at a time. It
// mirrors kv.Store: the semantics of Result.OK for each operation are
// documented there.
//
// Keys are independent, so histories are partitioned by key; that keeps the
// search small and is what makes checking thousands of runs feasible.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(input).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},

	Init: func() interface{} { return keyState{} },

	Step: func(state, in, out interface{}) (bool, interface{}) {
		s, i, o := state.(keyState), in.(input), out.(output)
		switch i.Op {
		case kv.OpGet:
			if !o.Unknown && (o.OK != s.exists || o.Value != s.value) {
				return false, s
			}
			return true, s

		case kv.OpPut:
			if !o.Unknown && !o.OK {
				return false, s
			}
			return true, keyState{exists: true, value: i.Value}

		case kv.OpDelete:
			if !o.Unknown && o.OK != s.exists {
				return false, s
			}
			return true, keyState{}

		case kv.OpCAS:
			// A missing key compares equal to "".
			if s.value == i.Expected {
				if !o.Unknown && (!o.OK || o.Value != i.Value) {
					return false, s
				}
				return true, keyState{exists: true, value: i.Value}
			}
			if !o.Unknown && (o.OK || o.Value != s.value) {
				return false, s
			}
			return true, s
		}
		return false, s
	},

	Equal: func(a, b interface{}) bool { return a.(keyState) == b.(keyState) },

	DescribeOperation: func(in, out interface{}) string {
		i, o := in.(input), out.(output)
		var call string
		switch i.Op {
		case kv.OpGet:
			call = fmt.Sprintf("get(%s)", i.Key)
		case kv.OpPut:
			call = fmt.Sprintf("put(%s, %s)", i.Key, i.Value)
		case kv.OpDelete:
			call = fmt.Sprintf("delete(%s)", i.Key)
		case kv.OpCAS:
			call = fmt.Sprintf("cas(%s, %q -> %s)", i.Key, i.Expected, i.Value)
		}
		if o.Unknown {
			return call + " -> ?"
		}
		switch i.Op {
		case kv.OpGet:
			if !o.OK {
				return call + " -> not found"
			}
			return fmt.Sprintf("%s -> %s", call, o.Value)
		case kv.OpPut:
			return call + " -> ok"
		case kv.OpDelete:
			return fmt.Sprintf("%s -> existed=%v", call, o.OK)
		default:
			return fmt.Sprintf("%s -> swapped=%v (now %s)", call, o.OK, o.Value)
		}
	},

	DescribeState: func(state interface{}) string {
		s := state.(keyState)
		if !s.exists {
			return "(absent)"
		}
		return fmt.Sprintf("%q", s.value)
	},
}

// Operations converts a chaos history into Porcupine's form. An operation whose
// outcome the client never learned gets a return time later than everything
// else: it may have taken effect at any point after it was called.
func Operations(history []chaos.Op) []porcupine.Operation {
	out := make([]porcupine.Operation, len(history))
	for i, op := range history {
		in := input{Op: op.Cmd.Op, Key: op.Cmd.Key, Value: op.Cmd.Value, Expected: op.Cmd.Expected}
		o := output{Unknown: true}
		ret := int64(math.MaxInt64)
		if op.Done {
			o = output{Value: op.Result.Value, OK: op.Result.OK}
			ret = op.Return
		}
		out[i] = porcupine.Operation{ClientId: op.Client, Input: in, Call: op.Call, Output: o, Return: ret}
	}
	return out
}
