// Command raftkv-server runs one node of a RaftKV cluster.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	id := flag.Int("id", 0, "this node's ID (index into -peers)")
	addr := flag.String("addr", "127.0.0.1:7000", "address to listen on")
	peers := flag.String("peers", "", "comma-separated addresses of all nodes, in node-ID order")
	dataDir := flag.String("data", "./data", "directory for the write-ahead log and snapshots")
	flag.Parse()

	// TODO(phase 4): start the Raft node, the WAL, and the gRPC servers.
	fmt.Fprintf(os.Stderr,
		"raftkv-server: not implemented yet (id=%d addr=%s peers=%q data=%s)\n",
		*id, *addr, *peers, *dataDir)
	os.Exit(1)
}
