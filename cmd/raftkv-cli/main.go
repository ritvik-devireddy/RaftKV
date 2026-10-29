// Command raftkv-cli is a command-line client for a RaftKV cluster.
package main

import (
	"flag"
	"fmt"
	"os"
)

const usage = `usage: raftkv-cli [-servers host:port,host:port,...] <command> [args]

commands:
  get <key>
  put <key> <value>
  delete <key>
  cas <key> <expected> <value>
`

func main() {
	servers := flag.String("servers", "127.0.0.1:7000", "comma-separated server addresses")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	// TODO(phase 4): connect over gRPC, follow leader redirects, retry with the
	// same (client_id, seq) so a retried request is applied at most once.
	fmt.Fprintf(os.Stderr, "raftkv-cli: not implemented yet (servers=%s cmd=%v)\n",
		*servers, flag.Args())
	os.Exit(1)
}
