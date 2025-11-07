package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

var conn *ouroboros.Connection

func getImmutableTip() (*common.Point, int64, error) {
	if conn == nil {
		return nil, 0, fmt.Errorf("connection not set")
	}

	lsq := conn.LocalStateQuery().Client

	// Acquire the immutable tip snapshot
	if err := lsq.AcquireImmutableTip(); err != nil {
		return nil, 0, err
	}
	defer lsq.Release()

	// Point = {Slot, Hash}
	pt, err := lsq.GetChainPoint()
	if err != nil {
		return nil, 0, err
	}
	// Optional but handy for logging
	bn, err := lsq.GetChainBlockNo()
	if err != nil {
		return nil, 0, err
	}

	return pt, bn, nil
}

func handleNewBlock(
	ctx chainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip chainsync.Tip,
) error {
	// Get immutable tip
	immutablePt, immutableBlockNo, err := getImmutableTip()
	if err != nil {
		fmt.Printf("Error getting immutable tip: %v\n", err)
	} else {
		fmt.Printf("Immutable tip: slot=%d blockNo=%d hash=%s\n",
			immutablePt.Slot, immutableBlockNo, hex.EncodeToString(immutablePt.Hash))
	}

	// Current tip is provided in the callback
	fmt.Printf("Current tip: slot=%d hash=%s\n",
		tip.Point.Slot, hex.EncodeToString(tip.Point.Hash))
	fmt.Printf("----------------------------------------\n\n")

	return nil
}

func handleRollback(
	ctx chainsync.CallbackContext,
	point common.Point,
	tip chainsync.Tip,
) error {
	fmt.Printf("CHAIN REORGANIZATION - Rollback occurring!\n")
	fmt.Printf("  Rolling back to: slot %d, hash %x\n", point.Slot, point.Hash)
	fmt.Printf("  New tip: slot %d, hash %x\n", tip.Point.Slot, tip.Point.Hash)
	fmt.Printf("----------------------------------------\n\n")
	return nil
}

func main() {
	// Parse command line arguments
	var networkMagic = flag.Int("network-magic", 764824073, "Network magic number (2 for preview, 1 for preprod, 764824073 for mainnet)")
	var socketPath = flag.String("socket", "", "Path to the Cardano node socket")
	flag.Parse()

	// Validate that socket path is provided
	if *socketPath == "" {
		fmt.Println("Error: You must specify -socket")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("Starting Cardano Tip Monitor...\n")
	fmt.Printf("Network Magic: %d\n", *networkMagic)
	fmt.Printf("Connecting to node via Unix socket: %s\n", *socketPath)

	// Create the network connection
	netConn, err := net.Dial("unix", *socketPath)
	if err != nil {
		log.Fatalf("Failed to connect to Cardano node: %v", err)
	}

	// Create error channel to handle async errors
	errorChan := make(chan error, 10)

	// Start goroutine to handle connection errors
	go func() {
		for err := range errorChan {
			fmt.Printf("Connection error: %v\n", err)
			os.Exit(1)
		}
	}()

	// Create the Ouroboros connection with ChainSync and LocalStateQuery
	conn, err = ouroboros.New(
		ouroboros.WithConnection(netConn),
		ouroboros.WithNetworkMagic(uint32(*networkMagic)),
		ouroboros.WithNodeToNode(false), // Node-to-Client over the local socket
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithChainSyncConfig(
			chainsync.NewConfig(
				chainsync.WithRollForwardFunc(handleNewBlock),
				chainsync.WithRollBackwardFunc(handleRollback),
			),
		),
		ouroboros.WithLocalStateQueryConfig(
			localstatequery.NewConfig(
				localstatequery.WithAcquireTimeout(5*time.Second),
				localstatequery.WithQueryTimeout(5*time.Second),
			),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create Ouroboros connection: %v", err)
	}
	defer conn.Close()


	// Get current tip to start monitoring from
	tip, err := conn.ChainSync().Client.GetCurrentTip()
	if err != nil {
		log.Fatalf("Failed to get current tip: %v", err)
	}

	fmt.Printf("Current chain tip: slot %d, hash %x\n", tip.Point.Slot, tip.Point.Hash)
	fmt.Printf("Starting to monitor for new blocks...\n\n")

	// Print initial tips
	immutablePt, immutableBlockNo, err := getImmutableTip()
	if err != nil {
		fmt.Printf("Error getting initial immutable tip: %v\n", err)
	} else {
		fmt.Printf("Initial immutable tip: slot=%d blockNo=%d hash=%s\n",
			immutablePt.Slot, immutableBlockNo, hex.EncodeToString(immutablePt.Hash))
	}
	fmt.Printf("Initial current tip: slot=%d hash=%s\n",
		tip.Point.Slot, hex.EncodeToString(tip.Point.Hash))
	fmt.Printf("----------------------------------------\n\n")

	// Start syncing from the current tip - this means we'll only see NEW blocks
	err = conn.ChainSync().Client.Sync([]common.Point{tip.Point})
	if err != nil {
		log.Fatalf("Failed to start chain sync: %v", err)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Press Ctrl+C to stop monitoring...\n")

	// Wait for shutdown signal
	<-sigChan
	fmt.Printf("\nShutting down...\n")

	fmt.Printf("Block monitor stopped.\n")
}
