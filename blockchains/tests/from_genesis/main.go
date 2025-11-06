package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

func main() {
	socket := flag.String("socket", "", "path to cardano-node Node-to-Client socket (or host:port if using TCP via socat)")
	magic := flag.Uint("magic", 1, "network magic (uint)")
	flag.Parse()

	if *socket == "" {
		log.Fatal("missing -socket")
	}

	// Create the network connection
	var conn net.Conn
	var err error
	if *socket != "" && net.ParseIP(*socket) == nil {
		// Unix socket
		conn, err = net.Dial("unix", *socket)
	} else {
		// TCP connection
		conn, err = net.Dial("tcp", *socket)
	}
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Create error channel
	errorChan := make(chan error, 10)
	go func() {
		for err := range errorChan {
			log.Printf("connection error: %v", err)
		}
	}()

	// Channel to signal when we've reached the tip
	reachedTipChan := make(chan struct{})

	// Build the Ouroboros connection with ChainSync
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(uint32(*magic)),
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithNodeToNode(false), // NtC
		ouroboros.WithKeepAlive(true),
		ouroboros.WithChainSyncConfig(
			chainsync.NewConfig(
				chainsync.WithRollForwardFunc(func(ctx chainsync.CallbackContext, blockType uint, blockData any, tip chainsync.Tip) error {
					return handleNewBlock(ctx, blockType, blockData, tip, reachedTipChan)
				}),
				chainsync.WithRollBackwardFunc(handleRollback),
			),
		),
	)
	if err != nil {
		log.Fatalf("new connection: %v", err)
	}

	// Start sync from genesis
	startPoint := common.NewPointOrigin()
	if err := oConn.ChainSync().Client.Sync([]common.Point{startPoint}); err != nil {
		log.Fatalf("chainsync sync: %v", err)
	}

	// Wait until we reach the tip or are interrupted
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case <-reachedTipChan:
		log.Printf("Reached chain tip, exiting...")
	case <-sig:
		log.Printf("Interrupted, exiting...")
	}

	// Close the connection gracefully
	if err := oConn.Close(); err != nil {
		log.Printf("Error closing connection: %v", err)
	}
}

// handleNewBlock processes blocks from the chain sync
func handleNewBlock(ctx chainsync.CallbackContext, blockType uint, blockData any, tip chainsync.Tip, reachedTipChan chan<- struct{}) error {
	var block ledger.Block

	switch v := blockData.(type) {
	case ledger.Block:
		block = v
	case ledger.BlockHeader:
		// For full transaction info, we'd need to fetch the full block
		// But for now, we'll skip headers since we need transactions
		return nil
	default:
		return nil
	}

	if block == nil {
		return nil
	}

	// Extract block information
	era := block.Era().Name
	slot := block.SlotNumber()
	blockHash := block.Hash().Bytes()
	transactions := block.Transactions()

	dump(era, slot, blockHash, transactions)

	// Check if we've reached the tip (slot matches and hash matches)
	if slot == tip.Point.Slot && len(tip.Point.Hash) > 0 && bytes.Equal(blockHash, tip.Point.Hash) {
		// Signal that we've reached the tip (non-blocking)
		select {
		case reachedTipChan <- struct{}{}:
		default:
		}
	}

	return nil
}

func dump(era string, slot uint64, blockHash []byte, txs []ledger.Transaction) {
	if len(txs) == 0 {
		// no transactions in this block
		return
	}
	fmt.Printf("slot=%d era=%s block=%s txs=%d\n", slot, era, hex.EncodeToString(blockHash), len(txs))
	for _, tx := range txs {
		fmt.Printf("  tx=%s\n", hex.EncodeToString(tx.Hash().Bytes()))
	}
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
