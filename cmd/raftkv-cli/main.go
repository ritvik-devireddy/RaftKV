// Command raftkv-cli is a command-line client for a RaftKV cluster.
//
//	raftkv-cli -addrs localhost:7001,localhost:7002,localhost:7003 put name ritvik
//	raftkv-cli -addrs ... get name
//	raftkv-cli -addrs ... delete name
//	raftkv-cli -addrs ... cas name <expected> <new>
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ritvik-devireddy/raftkv/internal/client"
)

const usage = `usage: raftkv-cli [-addrs host:port,...] [-timeout 10s] <command>

commands:
  get <key>
  put <key> <value>
  delete <key>
  cas <key> <expected> <value>   (a missing key counts as "")
`

func main() {
	addrs := flag.String("addrs", "localhost:7001,localhost:7002,localhost:7003", "comma-separated node addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "give up after this long")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	want := map[string]int{"get": 2, "put": 3, "delete": 2, "cas": 4}[args[0]]
	if want == 0 || len(args) != want {
		flag.Usage()
		os.Exit(2)
	}

	c, err := client.New(strings.Split(*addrs, ","))
	if err != nil {
		fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "get":
		v, found, err := c.Get(ctx, args[1])
		if err != nil {
			fatal(err)
		}
		if !found {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fmt.Println(v)
	case "put":
		if err := c.Put(ctx, args[1], args[2]); err != nil {
			fatal(err)
		}
		fmt.Println("OK")
	case "delete":
		existed, err := c.Delete(ctx, args[1])
		if err != nil {
			fatal(err)
		}
		if existed {
			fmt.Println("deleted")
		} else {
			fmt.Println("(not found)")
		}
	case "cas":
		swapped, cur, err := c.CompareAndSwap(ctx, args[1], args[2], args[3])
		if err != nil {
			fatal(err)
		}
		if swapped {
			fmt.Println("swapped")
		} else {
			fmt.Printf("not swapped (current value: %q)\n", cur)
			os.Exit(1)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
