// utxo_split.go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"
)

func main() {
	var (
		socketPath     string
		testnetMagic   int
		skeyFile       string
		inputAddrFile  string
		outputAddrFile string
		splits         int
		capPerTx       int
		threads        int
	)
	flag.StringVar(&socketPath, "socket-path", "", "Path to node.socket")
	flag.IntVar(&testnetMagic, "testnet-magic", 42, "Testnet magic")
	flag.StringVar(&skeyFile, "skey-file", "", "Signing key for input address")
	flag.StringVar(&inputAddrFile, "input-addr-file", "", "Input address file (bech32 or JSON with {\"address\":...})")
	flag.StringVar(&outputAddrFile, "output-addr-file", "", "Final output address file")
	flag.IntVar(&splits, "splits", 2, "Total number of equal-ish outputs to produce")
	flag.IntVar(&capPerTx, "cap", 200, "Max outputs per transaction")
	flag.IntVar(&threads, "threads", runtime.NumCPU(), "Parallel workers per level")
	flag.Parse()

	if socketPath == "" || skeyFile == "" || inputAddrFile == "" || outputAddrFile == "" || splits <= 0 || capPerTx <= 0 {
		fmt.Fprintln(os.Stderr, "missing required flags. usage:")
		flag.PrintDefaults()
		os.Exit(2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = time.Now()

	if err := SplitEqualPlanned(ctx, socketPath, testnetMagic, skeyFile, inputAddrFile, outputAddrFile, splits, capPerTx, threads); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("done")
}
