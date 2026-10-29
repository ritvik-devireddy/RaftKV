// Package linearizability will hold the chaos tests that prove the cluster
// behaves like a single correct key-value store.
//
// Planned for phase 5:
//   - run clients against a simnet cluster while injecting partitions, crashes,
//     restarts, drops and reordering, all driven by a seed;
//   - record every operation (invoke time, return time, input, output);
//   - check the history with Porcupine (github.com/anishathalye/porcupine);
//   - turn every failing seed into a named regression test.
//
// Add the Porcupine dependency when you reach that phase:
//
//	go get github.com/anishathalye/porcupine
package linearizability
