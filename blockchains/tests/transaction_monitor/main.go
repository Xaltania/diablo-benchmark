package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

// TxInfo represents information about a confirmed transaction
type TxInfo struct {
	Hash        string
	ConfirmedAt time.Time
	BlockSlot   uint64
	BlockHash   string
	BlockNumber uint64
	Fee         uint64
	InputCount  int
	OutputCount int
	TotalOutput uint64
	TxType      string
	Era         string
}

// TxStats keeps track of transaction statistics
type TxStats struct {
	mu                 sync.RWMutex
	totalTransactions  int
	transactionsByEra  map[string]int
	transactionsByType map[string]int
	totalFees          uint64
	totalValue         uint64
	startTime          time.Time
}

func NewTxStats() *TxStats {
	return &TxStats{
		transactionsByEra:  make(map[string]int),
		transactionsByType: make(map[string]int),
		startTime:          time.Now(),
	}
}

func (s *TxStats) AddTransaction(tx *TxInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalTransactions++
	s.transactionsByEra[tx.Era]++
	s.transactionsByType[tx.TxType]++
	s.totalFees += tx.Fee
	s.totalValue += tx.TotalOutput
}

func (s *TxStats) GetStats() (int, map[string]int, map[string]int, uint64, uint64, time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Copy maps to avoid race conditions
	eraStats := make(map[string]int)
	typeStats := make(map[string]int)

	for k, v := range s.transactionsByEra {
		eraStats[k] = v
	}
	for k, v := range s.transactionsByType {
		typeStats[k] = v
	}

	return s.totalTransactions, eraStats, typeStats, s.totalFees, s.totalValue, time.Since(s.startTime)
}

var (
	txStats *TxStats
	oConn   *ouroboros.Connection
)

func main() {
	// Parse command line arguments
	var networkMagic = flag.Int("network-magic", 2, "Network magic number (2 for preview, 1 for preprod, 764824073 for mainnet)")
	var socketPath = flag.String("socket", "", "Path to the Cardano node socket")
	var address = flag.String("address", "", "TCP address:port to connect to (alternative to socket)")
	var showStats = flag.Bool("stats", false, "Show periodic statistics every 30 seconds")
	var verbose = flag.Bool("verbose", false, "Show detailed transaction information")
	// var fromTip = flag.Bool("tip", true, "Start monitoring from current tip (only new transactions)")
	var fromGenesis = flag.Bool("genesis", false, "Start monitoring from genesis (all historical transactions)")
	flag.Parse()

	// Validate that we have either socket or address
	if *socketPath == "" && *address == "" {
		fmt.Println("Error: You must specify either -socket or -address")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize transaction stats
	txStats = NewTxStats()

	fmt.Printf("Starting Cardano Transaction Monitor...\n")
	fmt.Printf("Network Magic: %d\n", *networkMagic)

	if *fromGenesis {
		fmt.Printf("Mode: Historical sync from genesis (WARNING: This will take a very long time!)\n")
	} else {
		fmt.Printf("Mode: Real-time monitoring from current tip\n")
	}

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

	// Create the Ouroboros connection with ChainSync
	oConn, err = ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(uint32(*networkMagic)),
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithNodeToNode(false), // Use Node-to-Client protocol
		ouroboros.WithKeepAlive(true),   // Keep the connection alive
		ouroboros.WithChainSyncConfig(
			chainsync.NewConfig(
				chainsync.WithRollForwardFunc(func(ctx chainsync.CallbackContext, blockType uint, blockData any, tip chainsync.Tip) error {
					return handleNewBlock(ctx, blockType, blockData, tip, *verbose)
				}),
				chainsync.WithRollBackwardFunc(handleRollback),
			),
		),
	)

	if err != nil {
		fmt.Printf("Failed to create Ouroboros connection: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully connected to Cardano node!\n")

	// Determine starting point
	var startPoint common.Point
	if *fromGenesis {
		startPoint = common.NewPointOrigin()
		fmt.Printf("Starting sync from genesis...\n")
	} else {
		tip, err := oConn.ChainSync().Client.GetCurrentTip()
		if err != nil {
			fmt.Printf("Failed to get current tip: %v\n", err)
			os.Exit(1)
		}
		startPoint = tip.Point
		fmt.Printf("Current chain tip: slot %d, hash %x\n", tip.Point.Slot, tip.Point.Hash)
		fmt.Printf("Starting sync from current tip for real-time monitoring...\n")
	}

	// Start chain sync
	err = oConn.ChainSync().Client.Sync([]common.Point{startPoint})
	if err != nil {
		fmt.Printf("Failed to start chain sync: %v\n", err)
		os.Exit(1)
	}

	// Start stats reporting if requested
	if *showStats {
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				printStats()
			}
		}()
	}

	fmt.Printf("\n=== TRANSACTION MONITOR ACTIVE ===\n")
	fmt.Printf("Monitoring: Transactions confirmed in blocks\n")
	fmt.Printf("Verbose mode: %t\n", *verbose)
	fmt.Printf("Stats reporting: %t\n", *showStats)
	fmt.Printf("=====================================\n\n")

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

	// Print final stats
	printFinalStats()
	fmt.Printf("Transaction monitor stopped.\n")
}

func handleNewBlock(
	ctx chainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip chainsync.Tip,
	verbose bool,
) error {
	var block ledger.Block

	switch v := blockData.(type) {
	case ledger.Block:
		block = v
	case ledger.BlockHeader:
		// For transaction monitoring, we need the full block
		// Fetch the full block if we only got the header
		var err error
		block, err = oConn.BlockFetch().Client.GetBlock(common.NewPoint(v.SlotNumber(), v.Hash().Bytes()))
		if err != nil {
			// Just log the warning and continue - some older blocks might not be available
			if verbose {
				fmt.Printf("Warning: Could not fetch full block for slot %d: %v\n", v.SlotNumber(), err)
			}
			return nil
		}
	default:
		return nil
	}

	if block == nil {
		return nil
	}

	transactions := block.Transactions()

	// Only process blocks with transactions
	if len(transactions) == 0 {
		return nil
	}

	blockInfo := &struct {
		Era         string
		Slot        uint64
		BlockNumber uint64
		BlockHash   string
		TxCount     int
		Timestamp   time.Time
	}{
		Era:         block.Era().Name,
		Slot:        block.SlotNumber(),
		BlockNumber: block.BlockNumber(),
		BlockHash:   hex.EncodeToString(block.Hash().Bytes()),
		TxCount:     len(transactions),
		Timestamp:   time.Now(),
	}

	fmt.Printf("BLOCK CONFIRMED: %s Era, Slot %d, Block %d, %d transactions\n",
		blockInfo.Era, blockInfo.Slot, blockInfo.BlockNumber, blockInfo.TxCount)

	// Process each transaction in the block
	for i, tx := range transactions {
		txInfo := extractTransactionInfo(tx, blockInfo)
		txStats.AddTransaction(txInfo)

		if verbose {
			printDetailedTransactionInfo(i, txInfo)
		} else {
			fmt.Printf("TX[%d]: %s (Fee: %d, I/O: %d/%d)\n",
				i, txInfo.Hash[:16]+"...", txInfo.Fee, txInfo.InputCount, txInfo.OutputCount)
		}
	}

	fmt.Printf("----------------------------------------\n")
	return nil
}

func extractTransactionInfo(tx ledger.Transaction, blockInfo interface{}) *TxInfo {
	block := blockInfo.(*struct {
		Era         string
		Slot        uint64
		BlockNumber uint64
		BlockHash   string
		TxCount     int
		Timestamp   time.Time
	})

	// Calculate total output value
	var totalOutput uint64
	for _, output := range tx.Outputs() {
		totalOutput += output.Amount()
	}

	// Determine transaction type
	txType := "Unknown"
	switch tx.Type() {
	case 0:
		txType = "Byron"
	case 1:
		txType = "Shelley"
	case 2:
		txType = "Allegra"
	case 3:
		txType = "Mary"
	case 4:
		txType = "Alonzo"
	case 5:
		txType = "Babbage"
	case 6:
		txType = "Conway"
	}

	return &TxInfo{
		Hash:        hex.EncodeToString(tx.Hash().Bytes()),
		ConfirmedAt: block.Timestamp,
		BlockSlot:   block.Slot,
		BlockHash:   block.BlockHash,
		BlockNumber: block.BlockNumber,
		Fee:         tx.Fee(),
		InputCount:  len(tx.Inputs()),
		OutputCount: len(tx.Outputs()),
		TotalOutput: totalOutput,
		TxType:      txType,
		Era:         block.Era,
	}
}

func printDetailedTransactionInfo(index int, tx *TxInfo) {
	fmt.Printf("    TX[%d] CONFIRMED\n", index)
	fmt.Printf("    Hash: %s\n", tx.Hash)
	fmt.Printf("    Type: %s (%s Era)\n", tx.TxType, tx.Era)
	fmt.Printf("    Fee: %d lovelace (%.6f ADA)\n", tx.Fee, float64(tx.Fee)/1000000)
	fmt.Printf("    Inputs: %d, Outputs: %d\n", tx.InputCount, tx.OutputCount)
	fmt.Printf("    Total Output: %d lovelace (%.6f ADA)\n", tx.TotalOutput, float64(tx.TotalOutput)/1000000)
	fmt.Printf("    Block: %d (Slot %d)\n", tx.BlockNumber, tx.BlockSlot)
	fmt.Printf("    Confirmed: %s\n", tx.ConfirmedAt.Format("15:04:05.000"))
}

func handleRollback(
	ctx chainsync.CallbackContext,
	point common.Point,
	tip chainsync.Tip,
) error {
	fmt.Printf("  CHAIN REORGANIZATION\n")
	fmt.Printf("  Rolling back to: slot %d, hash %x\n", point.Slot, point.Hash)
	fmt.Printf("  New tip: slot %d, hash %x\n", tip.Point.Slot, tip.Point.Hash)
	fmt.Printf("  Note: Some confirmed transactions may become unconfirmed\n")
	fmt.Printf("========================================\n\n")
	return nil
}

func printStats() {
	total, eraStats, typeStats, totalFees, totalValue, duration := txStats.GetStats()

	fmt.Printf("\n === TRANSACTION STATISTICS ===\n")
	fmt.Printf("Runtime: %v\n", duration.Truncate(time.Second))
	fmt.Printf("Total Transactions: %d\n", total)

	if total > 0 {
		fmt.Printf("Total Fees: %d lovelace (%.6f ADA)\n", totalFees, float64(totalFees)/1000000)
		fmt.Printf("Total Value: %d lovelace (%.6f ADA)\n", totalValue, float64(totalValue)/1000000)
		fmt.Printf("Average Fee: %.2f lovelace\n", float64(totalFees)/float64(total))
		fmt.Printf("Rate: %.2f tx/min\n", float64(total)/duration.Minutes())

		fmt.Printf("\nBy Era:\n")
		for era, count := range eraStats {
			percentage := float64(count) / float64(total) * 100
			fmt.Printf("  %s: %d (%.1f%%)\n", era, count, percentage)
		}

		fmt.Printf("\nBy Type:\n")
		for txType, count := range typeStats {
			percentage := float64(count) / float64(total) * 100
			fmt.Printf("  %s: %d (%.1f%%)\n", txType, count, percentage)
		}
	}
	fmt.Printf("================================\n\n")
}

func printFinalStats() {
	fmt.Printf("\n === FINAL STATISTICS ===\n")
	printStats()
}
