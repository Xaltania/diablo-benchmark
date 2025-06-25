package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

func main() {
	// Parse command line arguments
	var networkMagic = flag.Int("network-magic", 2, "Network magic number (2 for preview, 1 for preprod, 764824073 for mainnet)")
	var socketPath = flag.String("socket", "", "Path to the Cardano node socket")
	var address = flag.String("address", "", "TCP address:port to connect to (alternative to socket)")
	flag.Parse()

	// Validate that we have either socket or address
	if *socketPath == "" && *address == "" {
		fmt.Println("Error: You must specify either -socket or -address")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("Starting Cardano Block Monitor...\n")
	fmt.Printf("Network Magic: %d\n", *networkMagic)

	// Create the network connection
	var conn net.Conn
	var err error

	if *socketPath != "" {
		fmt.Printf("Connecting to node via Unix socket: %s\n", *socketPath)
		conn, err = net.Dial("unix", *socketPath)
	} else {
		fmt.Printf("Connecting to node via TCP: %s\n", *address)
		conn, err = net.Dial("tcp", *address)
	}

	if err != nil {
		fmt.Printf("Failed to connect to Cardano node: %v\n", err)
		os.Exit(1)
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

	// Create the Ouroboros connection with chain sync configuration
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(uint32(*networkMagic)),
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithNodeToNode(false), // Use Node-to-Client protocol
		ouroboros.WithKeepAlive(true),   // Keep the connection alive
		ouroboros.WithChainSyncConfig(
			chainsync.NewConfig(
				// Configure the roll forward handler - this gets called for each new block
				chainsync.WithRollForwardFunc(handleNewBlock),
				// Configure the roll backward handler - this gets called during chain reorganizations
				chainsync.WithRollBackwardFunc(handleRollback),
			),
		),
	)

	if err != nil {
		fmt.Printf("Failed to create Ouroboros connection: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully connected to Cardano node!\n")

	// Get the current tip to understand where we are in the chain
	tip, err := oConn.ChainSync().Client.GetCurrentTip()
	if err != nil {
		fmt.Printf("Failed to get current tip: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Current chain tip: slot %d, hash %x\n", tip.Point.Slot, tip.Point.Hash)
	fmt.Printf("Starting to monitor for new blocks...\n\n")

	// Start syncing from the current tip - this means we'll only see NEW blocks
	// If you wanted to sync from genesis, you'd use: common.NewPointOrigin()
	err = oConn.ChainSync().Client.Sync([]common.Point{tip.Point})
	if err != nil {
		fmt.Printf("Failed to start chain sync: %v\n", err)
		os.Exit(1)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Press Ctrl+C to stop monitoring...\n")

	// Wait for shutdown signal
	<-sigChan
	fmt.Printf("\nShutting down...\n")

	// Close the connection
	if err := oConn.Close(); err != nil {
		fmt.Printf("Error closing connection: %v\n", err)
	}

	fmt.Printf("Block monitor stopped.\n")
}

// handleNewBlock is called whenever a new block is received from the chain
// This is where the magic happens - real-time block notifications!
func handleNewBlock(
	ctx chainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip chainsync.Tip,
) error {
	// Record when we received this block
	receivedAt := time.Now()

	// The blockData can be either a full Block or just a BlockHeader
	// depending on the sync mode and protocol version
	var block ledger.Block
	var blockInfo string

	switch v := blockData.(type) {
	case ledger.Block:
		// We received a full block with transactions
		block = v
		txCount := len(block.Transactions())
		blockInfo = fmt.Sprintf("Full block with %d transactions", txCount)

	case ledger.BlockHeader:
		// We received just the block header
		blockInfo = fmt.Sprintf("Block header (slot %d)", v.SlotNumber())

		// You could fetch the full block here if needed:
		// block, err := oConn.BlockFetch().Client.GetBlock(common.NewPoint(v.SlotNumber(), v.Hash().Bytes()))

	default:
		blockInfo = "Unknown block data type"
	}

	// Print detailed information about the new block
	fmt.Printf("NEW BLOCK RECEIVED!\n")
	fmt.Printf("  Received at: %s\n", receivedAt.Format("15:04:05.000"))
	fmt.Printf("  Block type: %s\n", blockInfo)

	if block != nil {
		fmt.Printf("  Era: %s\n", block.Era().Name)
		fmt.Printf("  Slot: %d\n", block.SlotNumber())
		fmt.Printf("  Block number: %d\n", block.BlockNumber())
		fmt.Printf("  Block hash: %x\n", block.Hash().Bytes())
		fmt.Printf("  Previous hash: %x\n", block.PrevHash().Bytes())
		fmt.Printf("  Block size: %d bytes\n", block.BlockBodySize())

		// Show issuer information (which pool minted this block)
		issuer := block.IssuerVkey()
		fmt.Printf("  Minted by pool: %s\n", issuer.PoolId())
	}

	// Show chain tip information
	fmt.Printf("  Chain tip: slot %d, hash %x\n", tip.Point.Slot, tip.Point.Hash)
	fmt.Printf("  Blocks behind tip: %d\n", tip.Point.Slot-block.SlotNumber())
	fmt.Printf("----------------------------------------\n\n")

	return nil
}

// handleRollback is called when the chain reorganizes and we need to "roll back"
// This happens when the network discovers a better chain fork
func handleRollback(
	ctx chainsync.CallbackContext,
	point common.Point,
	tip chainsync.Tip,
) error {
	fmt.Printf("CHAIN REORGANIZATION!\n")
	fmt.Printf("  Rolling back to: slot %d, hash %x\n", point.Slot, point.Hash)
	fmt.Printf("  New tip: slot %d, hash %x\n", tip.Point.Slot, tip.Point.Hash)
	fmt.Printf("----------------------------------------\n\n")

	return nil
}
