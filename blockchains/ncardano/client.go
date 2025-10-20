package ncardano

import (
	"context"
	"diablo-benchmark/core"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
)

type BlockchainClient struct {
	logger core.Logger
	conn   *ouroboros.Connection
	ctx    context.Context
	// commitment rpc.CommitmentType
	// provider   parameterProvider
	confirmer  transactionConfirmer
	subscriber *blockSubscriber
}

type transactionConfirmer interface {
	prepare(core.Interaction, string) (transactionConfirmerHandle, error)
	remove(string)
	reportTransaction(string, uint64, string)
}

type confirmResult struct {
	resend bool
	err    error
}

type transactionConfirmerHandle interface {
	confirm() confirmResult
}

type cardanoTransactionConfirmer struct {
	logger    core.Logger
	pendings  map[string]*cardanoTransactionConfirmerPending
	committed map[string]*committedTransaction
	invalidated map[string]bool // Track transactions invalidated by rollbacks
	lock      sync.Mutex
}

type committedTransaction struct {
	interaction core.Interaction
	blockSlot   uint64
	blockHash   string
}

type cardanoTransactionConfirmerPending struct {
	channel chan confirmResult
	iact    core.Interaction
	txHash  string
}

type blockSubscriber struct {
	logger    core.Logger
	conn      *ouroboros.Connection
	ctx       context.Context
	confirmer transactionConfirmer
}

func newCardanoTransactionConfirmer(logger core.Logger) *cardanoTransactionConfirmer {
	return &cardanoTransactionConfirmer{
		logger:    logger,
		pendings:  make(map[string]*cardanoTransactionConfirmerPending),
		committed: make(map[string]*committedTransaction),
		invalidated: make(map[string]bool),
	}
}

func (c *cardanoTransactionConfirmer) prepare(
	iact core.Interaction, txHash string) (transactionConfirmerHandle, error) {
	channel := make(chan confirmResult, 1)

	pending := &cardanoTransactionConfirmerPending{
		channel: channel,
		iact:    iact,
		txHash:  txHash,
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if transaction is already committed
	if _, ok := c.committed[txHash]; ok {
		delete(c.committed, txHash)
		iact.ReportCommit()
		channel <- confirmResult{false, nil}
		close(channel)
		return pending, nil
	}

	// Add to pending transactions
	c.pendings[txHash] = pending
	return pending, nil
}

func (c *cardanoTransactionConfirmer) remove(txHash string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if pending, exists := c.pendings[txHash]; exists {
		delete(c.pendings, txHash)
		close(pending.channel)
	}
}

func (p *cardanoTransactionConfirmerPending) confirm() confirmResult {
	result, ok := <-p.channel
	if !ok {
		return confirmResult{true, nil}
	}
	return result
}

func (c *cardanoTransactionConfirmer) reportTransaction(txHash string, blockSlot uint64, blockHash string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if transaction is in pending transactions
	if pending, ok := c.pendings[txHash]; ok {
		delete(c.pendings, txHash)
		pending.iact.ReportCommit()
		pending.channel <- confirmResult{false, nil}
		close(pending.channel)
		
		// Store committed transaction with block info
		c.committed[txHash] = &committedTransaction{
			interaction: pending.iact,
			blockSlot:   blockSlot,
			blockHash:   blockHash,
		}
		return
	}

	// If not in pending, add to committed for future reference
	c.committed[txHash] = &committedTransaction{
		interaction: nil, // We don't have the interaction reference
		blockSlot:   blockSlot,
		blockHash:   blockHash,
	}
	c.logger.Tracef("Transaction %s already committed in block %d", txHash, blockSlot)
}

func (c *cardanoTransactionConfirmer) handleRollback(rollbackSlot uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	
	invalidatedCount := 0
	var invalidatedTxs []string
	
	// Check all committed transactions to see if they were in rolled-back blocks
	for txHash, committedTx := range c.committed {
		if committedTx.blockSlot >= rollbackSlot {
			// This transaction was in a block that was rolled back
			invalidatedCount++
			invalidatedTxs = append(invalidatedTxs, txHash)
			
			// If we have the interaction reference, we can report an error
			if committedTx.interaction != nil {
				// Note: We can't directly set HasError here since we don't have access to the runtime
				// But we can log a warning and the transaction will be marked as failed
				c.logger.Warnf("Transaction %s was in rolled-back block %d (rollback to slot %d)", 
					txHash, committedTx.blockSlot, rollbackSlot)
			}
			
			// Mark as invalidated and remove from committed
			c.invalidated[txHash] = true
			delete(c.committed, txHash)
		}
	}
	
	if invalidatedCount > 0 {
		c.logger.Warnf("Chain rollback invalidated %d transactions: %v", invalidatedCount, invalidatedTxs)
	}
}

func newBlockSubscriber(logger core.Logger, conn *ouroboros.Connection, confirmer transactionConfirmer) *blockSubscriber {
	return &blockSubscriber{
		logger:    logger,
		conn:      conn, // This might be nil initially
		ctx:       context.Background(),
		confirmer: confirmer,
	}
}

func (bs *blockSubscriber) start() error {
	// Create error channel for async errors
	errorChan := make(chan error, 10)

	// Start goroutine to handle connection errors
	go func() {
		for err := range errorChan {
			bs.logger.Errorf("Connection error: %v", err)
			os.Exit(1)
		}
	}()

	// Get current tip
	tip, err := bs.conn.ChainSync().Client.GetCurrentTip()
	if err != nil {
		return fmt.Errorf("failed to get current tip: %w", err)
	}

	// Start chain sync from current tip
	err = bs.conn.ChainSync().Client.Sync([]common.Point{tip.Point})
	if err != nil {
		return fmt.Errorf("failed to start chain sync: %w", err)
	}

	return nil
}

func (bs *blockSubscriber) handleNewBlock(
	ctx chainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip chainsync.Tip,
) error {
	var block ledger.Block

	switch v := blockData.(type) {
	case ledger.Block:
		block = v
	case ledger.BlockHeader:
		// For transaction monitoring, we need the full block
		var err error
		block, err = bs.conn.BlockFetch().Client.GetBlock(common.NewPoint(v.SlotNumber(), v.Hash().Bytes()))
		if err != nil {
			bs.logger.Warnf("Could not fetch full block for slot %d: %v", v.SlotNumber(), err)
			return nil
		}
	default:
		return nil
	}

	if block == nil {
		return nil
	}

	transactions := block.Transactions()
	if len(transactions) == 0 {
		return nil
	}

	// Process each transaction in the block
	blockSlot := block.SlotNumber()
	blockHash := hex.EncodeToString(block.Hash().Bytes())
	
	for _, tx := range transactions {
		txHash := tx.Hash().String()
		bs.logger.Tracef("Found transaction %s in block %d (%s)", txHash, blockSlot, blockHash)
		bs.confirmer.reportTransaction(txHash, blockSlot, blockHash)
	}

	return nil
}

func (bs *blockSubscriber) handleRollback(
	ctx chainsync.CallbackContext,
	point common.Point,
	tip chainsync.Tip,
) error {
	bs.logger.Warnf("Chain reorganisation detected - rolling back to slot %d", point.Slot)
	
	// Get the confirmer to check for invalidated transactions
	confirmer := bs.confirmer.(*cardanoTransactionConfirmer)
	confirmer.handleRollback(point.Slot)
	
	return nil
}

func NewBlockchainClient(logger core.Logger, socketPath string) (*BlockchainClient, error) {
	conn, err := net.Dial("tcp", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Cardano node: %w", err)
	}

	errorChan := make(chan error, 10)
	// Create confirmer and subscriber first (confirm again with Andrei)
	confirmer := newCardanoTransactionConfirmer(logger)
	subscriber := newBlockSubscriber(logger, nil, confirmer)

	// Create the Ouroboros connection
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(42), // Mainnet magic number, take in parameter soon
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithNodeToNode(false), // Use Node-to-Client protocol
		ouroboros.WithKeepAlive(true),   // Keep the connection alive
		ouroboros.WithChainSyncConfig(
			chainsync.NewConfig(
				chainsync.WithRollForwardFunc(subscriber.handleNewBlock),
				chainsync.WithRollBackwardFunc(subscriber.handleRollback),
			),
		),
		ouroboros.WithLocalTxSubmissionConfig(
			localtxsubmission.NewConfig(),
		),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create Ouroboros connection: %w", err)
	}

	subscriber.conn = oConn

	// Start the block subscriber
	if err := subscriber.start(); err != nil {
		return nil, fmt.Errorf("failed to start block subscriber: %w", err)
	}

	// Start goroutine to handle connection errors
	go func() {
		for err := range errorChan {
			logger.Errorf("Connection error: %v", err)
			os.Exit(1)
		}
	}()

	return &BlockchainClient{
		logger:     logger,
		conn:       oConn,
		ctx:        context.Background(),
		confirmer:  confirmer,
		subscriber: subscriber,
	}, nil
}

func (c *BlockchainClient) DecodePayload(cbor_bytes []byte) (interface{}, error) {
	return cbor_bytes, nil // Trying without re-serialising
	// tx, err := decodeTransaction(cbor_bytes) // Returns ConwayTransaction
	// // conway.NewConwayTransactionFromCbor(encoded) // Note the buffer wrapper in the original
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to decode CBOR payload: %w", err)
	// }
	// c.logger.Tracef("decode transaction: %s", tx.Hash().String()) // Blake2b256 hash
	// return tx, nil
}

func (c *BlockchainClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *BlockchainClient) TriggerInteraction(iact core.Interaction) error {
	// Get transaction from payload
	payload := iact.Payload()
	if payload == nil {
		return fmt.Errorf("empty payload received")
	}

	// Handle different payload types
	var tx *conway.ConwayTransaction
	var txBytes []byte

	switch p := payload.(type) {
	case []byte:
		txBytes = p
		var err error
		tx, err = decodeTransaction(p)
		if err != nil {
			return fmt.Errorf("failed to decode transaction bytes: %w", err)
		}
	case string:
		b, err := hex.DecodeString(p)
		if err != nil {
			return fmt.Errorf("failed to decode hex payload: %w", err)
		}
		txBytes = b
		tx, err = decodeTransaction(b)
		if err != nil {
			return fmt.Errorf("failed to decode transaction bytes: %w", err)
		}
	case *conway.ConwayTransaction:
		tx = p
		txBytes = p.Cbor() // last resort; primary should send []byte
	default:
		return fmt.Errorf("invalid payload type %T, expected []byte, hex string, or *conway.ConwayTransaction", payload)
	}

	if tx == nil {
		return fmt.Errorf("empty transaction")
	}

	txHash := tx.Hash().String()
	c.logger.Tracef("Processing transaction with hash: %s", txHash)

	// Check if transaction is already committed or was invalidated
	c.confirmer.(*cardanoTransactionConfirmer).lock.Lock()
	if _, ok := c.confirmer.(*cardanoTransactionConfirmer).committed[txHash]; ok {
		delete(c.confirmer.(*cardanoTransactionConfirmer).committed, txHash)
		c.confirmer.(*cardanoTransactionConfirmer).lock.Unlock()
		iact.ReportCommit()
		return nil
	}
	
	// Check if transaction was invalidated by a rollback
	if invalidated, ok := c.confirmer.(*cardanoTransactionConfirmer).invalidated[txHash]; ok && invalidated {
		c.confirmer.(*cardanoTransactionConfirmer).lock.Unlock()
		c.logger.Warnf("Transaction %s was invalidated by chain rollback", txHash)
		return fmt.Errorf("transaction %s was invalidated by chain rollback", txHash)
	}
	c.confirmer.(*cardanoTransactionConfirmer).lock.Unlock()

	// Prepare confirmation
	handle, err := c.confirmer.prepare(iact, txHash)
	if err != nil {
		return err
	}

	// Report submission before sending
	iact.ReportSubmit()

	// Submit transaction
	c.logger.Debugf("Submitting transaction %s", txHash)
	if err := c.conn.LocalTxSubmission().Client.SubmitTx(ledger.TxTypeConway, txBytes); err != nil {
		// If transaction is already submitted, we can ignore the error
		if !strings.Contains(err.Error(), "already submitted") {
			return fmt.Errorf("failed to submit transaction: %w", err)
		}
	}

	// Wait for confirmation
	result := handle.confirm()
	if result.err != nil {
		return fmt.Errorf("transaction %s failed: %w", txHash, result.err)
	}
	if result.resend {
		return fmt.Errorf("transaction %s needs to be resent", txHash)
	}

	return nil
}
