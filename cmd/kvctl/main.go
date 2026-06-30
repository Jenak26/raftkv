// Command kvctl is the CLI client for a kvserver cluster. It builds a Clerk over
// the given server addresses, which transparently finds the leader and retries
// across leadership changes, so the caller just issues an operation.
//
// Usage:
//
//	kvctl -servers "127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002" put <key> <value>
//	kvctl -servers "..." get <key>
//	kvctl -servers "..." del <key>
//	kvctl -servers "..." cas <key> <expected> <new>
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/janak/raftkv/internal/kv"
	"github.com/janak/raftkv/internal/netrpc"
)

func main() {
	servers := flag.String("servers", "", "comma-separated server addresses (host:port,...)")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline for the operation")
	flag.Usage = usage
	flag.Parse()

	if *servers == "" || flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}

	var endpoints []kv.KV
	for _, addr := range strings.Split(*servers, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		kc := netrpc.DialKV(addr, 2*time.Second)
		defer kc.Close()
		endpoints = append(endpoints, kc)
	}
	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "kvctl: no server addresses")
		os.Exit(2)
	}

	ck := kv.NewClerk(endpoints)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := run(ctx, ck, flag.Args()); err != nil {
		fmt.Fprintf(os.Stderr, "kvctl: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, ck *kv.Clerk, args []string) error {
	cmd := args[0]
	rest := args[1:]
	switch cmd {
	case "put":
		if len(rest) != 2 {
			return fmt.Errorf("put needs <key> <value>")
		}
		if err := ck.Put(ctx, rest[0], rest[1]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "get":
		if len(rest) != 1 {
			return fmt.Errorf("get needs <key>")
		}
		v, ok, err := ck.Get(ctx, rest[0])
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("(key not found)")
			return nil
		}
		fmt.Println(v)
	case "del":
		if len(rest) != 1 {
			return fmt.Errorf("del needs <key>")
		}
		existed, err := ck.Delete(ctx, rest[0])
		if err != nil {
			return err
		}
		if existed {
			fmt.Println("OK (deleted)")
		} else {
			fmt.Println("OK (key not found)")
		}
	case "cas":
		if len(rest) != 3 {
			return fmt.Errorf("cas needs <key> <expected> <new>")
		}
		swapped, err := ck.CAS(ctx, rest[0], rest[1], rest[2])
		if err != nil {
			return err
		}
		if swapped {
			fmt.Println("OK (swapped)")
		} else {
			fmt.Println("not swapped (current value did not match expected)")
		}
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `kvctl -servers "host:port,..." <command> [args]

Commands:
  put <key> <value>          set key to value
  get <key>                  print the value (or "(key not found)")
  del <key>                  delete key
  cas <key> <expected> <new> set key to new only if it currently equals expected

Flags:`)
	flag.PrintDefaults()
}
