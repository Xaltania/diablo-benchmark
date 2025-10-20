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
		finalSkeyFile  string
		skipPrepare    bool
	)

	flag.StringVar(&socketPath, "socket-path", "", "Path to node.socket")
	flag.IntVar(&testnetMagic, "testnet-magic", 42, "Testnet magic")
	flag.StringVar(&skeyFile, "skey-file", "", "Signing key for input address (used for split)")
	flag.StringVar(&inputAddrFile, "input-addr-file", "", "Input address file")
	flag.StringVar(&outputAddrFile, "output-addr-file", "", "Final output address file")
	flag.IntVar(&splits, "splits", 2, "Total outputs to produce")
	flag.IntVar(&capPerTx, "cap", 200, "Max outputs per transaction")
	flag.IntVar(&threads, "threads", runtime.NumCPU(), "Parallel workers per level")
	flag.StringVar(&finalSkeyFile, "final-skey-file", "", "Signing key for final outputs (defaults to --skey-file)")
	flag.BoolVar(&skipPrepare, "skip-prepare", false, "Skip building the final self-spend transactions")
	flag.Parse()

	if socketPath == "" || skeyFile == "" || inputAddrFile == "" || outputAddrFile == "" || splits <= 0 || capPerTx <= 0 {
		fmt.Fprintln(os.Stderr, "missing required flags. usage:")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if finalSkeyFile == "" {
		finalSkeyFile = skeyFile
	}

	ctx := context.Background()

	finalUTxOs, splitDur, err := SplitEqualPlanned(ctx, socketPath, testnetMagic, skeyFile, inputAddrFile, outputAddrFile, splits, capPerTx, threads)
	if err != nil {
		fmt.Fprintf(os.Stderr, "split error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("split complete: %d UTxOs; split_elapsed=%s\n", len(finalUTxOs), splitDur.Truncate(time.Millisecond))

	if skipPrepare {
		fmt.Println("skipping final build+sign phase")
		return
	}

	outAddr, err := readAddressFile(outputAddrFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "output address error: %v\n", err)
		os.Exit(1)
	}
	dir, prepDur, err := PrepareSelfSpends(ctx, socketPath, testnetMagic, finalSkeyFile, outAddr, finalUTxOs, threads)
	if err != nil {
		fmt.Fprintf(os.Stderr, "prepare error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("final txs built and signed (not submitted). dir=%s; prepare_elapsed=%s\n", dir, prepDur.Truncate(time.Millisecond))
}
