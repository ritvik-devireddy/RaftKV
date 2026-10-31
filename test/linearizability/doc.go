// Package linearizability checks recorded client histories against a model of
// the key-value store using Porcupine, and runs thousands of seeded chaos
// simulations (partitions, crashes, message loss) through that check.
//
// A failing seed is a reproducible bug report; see chaos_test.go for the
// replay command and for the tests that break the system on purpose to prove
// the checker can fail.
package linearizability
