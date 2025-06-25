package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

func main() {
	inputFile := flag.String("input", "", "Path to the transaction file to process")
	outputFile := flag.String("output", "", "Path to save re-serialised transaction")
	flag.Parse()

	if *inputFile == "" {
		fmt.Println("Error: Input file must be specified with -input flag")
		flag.Usage()
		os.Exit(1)
	}

	txBytes, err := os.ReadFile(*inputFile)
	if err != nil {
		fmt.Printf("Error reading transaction file: %v\n", err)
		os.Exit(1)
	}

	// Deserialise
	fmt.Println("Deserializing transaction...")
	tx, err := conway.NewConwayTransactionFromCbor(txBytes)
	if err != nil {
		fmt.Printf("Error deserializing transaction: %v\n", err)
		os.Exit(1)
	}

	// Test ledger Determine Transaction Type
	txType, err := ledger.DetermineTransactionType(txBytes)
	if err != nil {
		fmt.Printf("Something's wrong")
	}
	if txType == conway.TxTypeConway {
		fmt.Printf("Conway")
	}

	// Print transaction information
	fmt.Println("\nTRANSACTION DETAILS!!!")
	fmt.Printf("Hash: %s\n", tx.Hash().String())
	fmt.Printf("Is Valid: %t\n", tx.IsValid())
	fmt.Printf("Fee: %d lovelace\n", tx.Fee())

	serializedTx := tx.Cbor()
	fmt.Printf("\nRe-serialized transaction size: %d bytes\n", len(serializedTx))

	if len(serializedTx) > 0 {
		displayLen := len(serializedTx)
		fmt.Printf("Transaction hash %d bytes (hex): %s\n",
			displayLen, hex.EncodeToString(serializedTx[:displayLen]))
	}

	if *outputFile != "" {
		err = os.WriteFile(*outputFile, serializedTx, 0644)
		if err != nil {
			fmt.Printf("Error writing output file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Re-serialized transaction saved to %s\n", *outputFile)
	}
}
