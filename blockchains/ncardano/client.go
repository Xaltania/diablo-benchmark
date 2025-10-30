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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

type BlockchainClient struct {
	logger core.Logger
	conn   *ouroboros.Connection
	ctx    context.Context
	// commitment rpc.CommitmentType
	// provider   parameterProvider
	confirmer  transactionConfirmer
	subscriber *blockSubscriber
	
	// Finality tracking
	finalityTime    time.Duration
	lastConfirmedTx time.Time
	allTxsSubmitted bool
	finalityMutex   sync.Mutex
}

type transactionConfirmer interface {
	prepare(core.Interaction, string) (transactionConfirmerHandle, error)
	remove(string)
	reportTransaction(string, uint64, string)
	storeTxCBOR(string, []byte)
	resubmitInvalidatedTx(string, []byte) error
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
	txCBOR    map[string][]byte // Store original CBOR for resubmission
	lock      sync.Mutex
}

type committedTransaction struct {
	interaction core.Interaction
	blockSlot   uint64
	blockHash   string
	txCBOR      []byte // Store CBOR for potential resubmission
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
		txCBOR:    make(map[string][]byte),
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

	// Get stored CBOR for this transaction
	txCBOR := c.txCBOR[txHash]

	// Check if transaction is in pending transactions
	if pending, ok := c.pendings[txHash]; ok {
		delete(c.pendings, txHash)
		pending.iact.ReportCommit()
		pending.channel <- confirmResult{false, nil}
		close(pending.channel)
		
		// Store committed transaction with block info and CBOR
		c.committed[txHash] = &committedTransaction{
			interaction: pending.iact,
			blockSlot:   blockSlot,
			blockHash:   blockHash,
			txCBOR:      txCBOR,
		}
		
		// Update last confirmed transaction time (this will be called from the block subscriber)
		// Note: We need access to the client to update this, but we don't have it here
		// The client will handle this in TriggerInteraction
		return
	}

	// If not in pending, add to committed for future reference
	c.committed[txHash] = &committedTransaction{
		interaction: nil, // We don't have the interaction reference
		blockSlot:   blockSlot,
		blockHash:   blockHash,
		txCBOR:      txCBOR,
	}
	c.logger.Tracef("Transaction %s already committed in block %d", txHash, blockSlot)
}

func (c *cardanoTransactionConfirmer) storeTxCBOR(txHash string, txCBOR []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.txCBOR[txHash] = txCBOR
}

func (c *cardanoTransactionConfirmer) resubmitInvalidatedTx(txHash string, txCBOR []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	
	// Store the CBOR for potential future resubmission
	c.txCBOR[txHash] = txCBOR
	
	// Mark as invalidated
	c.invalidated[txHash] = true
	
	c.logger.Infof("Transaction %s marked for resubmission due to rollback", txHash)
	return nil
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
			
			// Store CBOR for resubmission if available
			if len(committedTx.txCBOR) > 0 {
				c.txCBOR[txHash] = committedTx.txCBOR
			}
			
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

// calculateFinalityTime calculates the transaction finality time based on protocol parameters
func calculateFinalityTime(protocolParams interface{}) time.Duration {
	// Default values (mainnet-like)
	activeSlotsCoeff := 0.05
	slotLength := 20 * time.Second
	securityParam := uint64(2160)
	
	// Try to extract actual values from protocol parameters
	if _, ok := protocolParams.(*conway.ConwayProtocolParameters); ok {
		// Note: These fields might not be directly available in the protocol parameters
		// We'll use defaults for now - in a real implementation, we would extract
		// activeSlotsCoeff, slotLength, and securityParam from the protocol parameters
		// For now, we use the default values which are reasonable for mainnet
	}
	
	// Calculate finality time: ((1 / activeSlotsCoeff) * slotLength) * securityParam
	finalityTime := time.Duration(float64(1.0/activeSlotsCoeff) * float64(slotLength) * float64(securityParam))
	
	return finalityTime
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
		ouroboros.WithLocalStateQueryConfig(
			localstatequery.NewConfig(),
		),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create Ouroboros connection: %w", err)
	}

	subscriber.conn = oConn

	// Query protocol parameters to calculate finality time
	logger.Debugf("Querying protocol parameters for finality calculation...")
	protocolParams, err := oConn.LocalStateQuery().Client.GetCurrentProtocolParams()
	var finalityTime time.Duration
	if err != nil {
		logger.Warnf("Failed to query protocol parameters, using default finality time: %v", err)
		finalityTime = calculateFinalityTime(nil)
	} else {
		finalityTime = calculateFinalityTime(protocolParams)
		logger.Infof("Calculated finality time: %v", finalityTime)
	}

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

	client := &BlockchainClient{
		logger:     logger,
		conn:       oConn,
		ctx:        context.Background(),
		confirmer:  confirmer,
		subscriber: subscriber,
		finalityTime: finalityTime,
		lastConfirmedTx: time.Time{},
		allTxsSubmitted: false,
	}
	
	// Start the invalidated transaction monitor
	client.StartInvalidatedTransactionMonitor()
	
	return client, nil
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

	// Store transaction CBOR for potential resubmission
	c.confirmer.storeTxCBOR(txHash, txBytes)

	// Check if transaction is already committed or was invalidated
	c.confirmer.(*cardanoTransactionConfirmer).lock.Lock()
	if _, ok := c.confirmer.(*cardanoTransactionConfirmer).committed[txHash]; ok {
		delete(c.confirmer.(*cardanoTransactionConfirmer).committed, txHash)
		c.confirmer.(*cardanoTransactionConfirmer).lock.Unlock()
		iact.ReportCommit()
		
		// Update last confirmed transaction time
		c.finalityMutex.Lock()
		c.lastConfirmedTx = time.Now()
		c.finalityMutex.Unlock()
		
		return nil
	}
	
	// Check if transaction was invalidated by a rollback
	if invalidated, ok := c.confirmer.(*cardanoTransactionConfirmer).invalidated[txHash]; ok && invalidated {
		c.confirmer.(*cardanoTransactionConfirmer).lock.Unlock()
		c.logger.Warnf("Transaction %s was invalidated by chain rollback - marking as failed (resubmission disabled)", txHash)
		
		// TEMPORARILY DISABLED: Resubmit the transaction immediately
		// if err := c.resubmitTransaction(txHash, txBytes); err != nil {
		// 	return fmt.Errorf("failed to resubmit invalidated transaction %s: %w", txHash, err)
		// }
		
		// TEMPORARILY DISABLED: Prepare confirmation for the resubmitted transaction
		// handle, err := c.confirmer.prepare(iact, txHash)
		// if err != nil {
		// 	return err
		// }
		
		// TEMPORARILY DISABLED: Wait for confirmation
		// result := handle.confirm()
		// if result.err != nil {
		// 	return fmt.Errorf("resubmitted transaction %s failed: %w", txHash, result.err)
		// }
		// if result.resend {
		// 	return fmt.Errorf("resubmitted transaction %s needs to be resent", txHash)
		// }
		
		// TEMPORARILY DISABLED: Update last confirmed transaction time
		// c.finalityMutex.Lock()
		// c.lastConfirmedTx = time.Now()
		// c.finalityMutex.Unlock()
		
		// Mark as failed instead of resubmitting
		iact.ReportAbort()
		return fmt.Errorf("transaction %s was invalidated by chain rollback (resubmission disabled)", txHash)
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

	// Update last confirmed transaction time
	c.finalityMutex.Lock()
	c.lastConfirmedTx = time.Now()
	c.finalityMutex.Unlock()

	return nil
}

// resubmitTransaction resubmits a transaction that was invalidated by a rollback
func (c *BlockchainClient) resubmitTransaction(txHash string, txBytes []byte) error {
	c.logger.Infof("Resubmitting transaction %s (CBOR length: %d bytes)", txHash, len(txBytes))
	
	// Submit the transaction again
	if err := c.conn.LocalTxSubmission().Client.SubmitTx(ledger.TxTypeConway, txBytes); err != nil {
		// If transaction is already submitted, we can ignore the error
		if !strings.Contains(err.Error(), "already submitted") {
			c.logger.Errorf("Failed to resubmit transaction %s: %v", txHash, err)
			return fmt.Errorf("failed to resubmit transaction: %w", err)
		}
		c.logger.Debugf("Transaction %s was already submitted (ignoring error)", txHash)
	} else {
		c.logger.Debugf("Successfully resubmitted transaction %s", txHash)
	}
	
	// Clean up transaction state since we're resubmitting
	confirmer := c.confirmer.(*cardanoTransactionConfirmer)
	confirmer.lock.Lock()
	defer confirmer.lock.Unlock()
	
	// Remove from invalidated map
	delete(confirmer.invalidated, txHash)
	
	// Remove from committed map if it exists (it shouldn't, but just in case)
	if _, wasCommitted := confirmer.committed[txHash]; wasCommitted {
		c.logger.Debugf("Removing transaction %s from committed map during resubmission", txHash)
		delete(confirmer.committed, txHash)
	}
	
	// Remove from pendings if it exists (it shouldn't, but just in case)
	if pending, exists := confirmer.pendings[txHash]; exists {
		c.logger.Debugf("Removing transaction %s from pendings map during resubmission", txHash)
		delete(confirmer.pendings, txHash)
		close(pending.channel)
	}
	
	return nil
}

// WaitForFinality waits for the finality period after the last transaction was confirmed
func (c *BlockchainClient) WaitForFinality() error {
	c.finalityMutex.Lock()
	lastConfirmed := c.lastConfirmedTx
	allSubmitted := c.allTxsSubmitted
	c.finalityMutex.Unlock()
	
	if lastConfirmed.IsZero() {
		c.logger.Warnf("No transactions have been confirmed yet")
		return nil
	}
	
	if !allSubmitted {
		c.logger.Warnf("Not all transactions have been submitted yet")
		return nil
	}
	
	// Calculate how long to wait
	timeSinceLastTx := time.Since(lastConfirmed)
	remainingTime := c.finalityTime - timeSinceLastTx
	
	if remainingTime <= 0 {
		c.logger.Infof("Finality period has already elapsed since last transaction")
		return nil
	}
	
	c.logger.Infof("Waiting %v for transaction finality (last tx confirmed %v ago)", remainingTime, timeSinceLastTx)
	time.Sleep(remainingTime)
	
	c.logger.Infof("Finality period completed - all transactions should now be permanently on chain")
	return nil
}

// MarkAllTransactionsSubmitted marks that all transactions have been submitted
func (c *BlockchainClient) MarkAllTransactionsSubmitted() {
	c.finalityMutex.Lock()
	defer c.finalityMutex.Unlock()
	c.allTxsSubmitted = true
	c.logger.Infof("All transactions have been submitted - finality tracking enabled")
}

// ResubmitAllInvalidatedTransactions resubmits all transactions that were invalidated by rollbacks
func (c *BlockchainClient) ResubmitAllInvalidatedTransactions() error {
	confirmer := c.confirmer.(*cardanoTransactionConfirmer)
	
	// Get list of invalidated transactions first
	confirmer.lock.Lock()
	invalidatedTxs := make([]string, 0)
	for txHash, invalidated := range confirmer.invalidated {
		if invalidated {
			invalidatedTxs = append(invalidatedTxs, txHash)
		}
	}
	confirmer.lock.Unlock()
	
	resubmittedCount := 0
	for _, txHash := range invalidatedTxs {
		confirmer.lock.Lock()
		txCBOR, exists := confirmer.txCBOR[txHash]
		confirmer.lock.Unlock()
		
		if exists {
			c.logger.Infof("Resubmitting invalidated transaction %s", txHash)
			if err := c.resubmitTransaction(txHash, txCBOR); err != nil {
				c.logger.Errorf("Failed to resubmit transaction %s: %v", txHash, err)
				continue
			}
			resubmittedCount++
		} else {
			c.logger.Warnf("No CBOR found for invalidated transaction %s", txHash)
		}
	}
	
	if resubmittedCount > 0 {
		c.logger.Infof("Resubmitted %d invalidated transactions", resubmittedCount)
	}
	
	return nil
}

// GetInvalidatedTransactionCount returns the number of transactions that were invalidated by rollbacks
func (c *BlockchainClient) GetInvalidatedTransactionCount() int {
	confirmer := c.confirmer.(*cardanoTransactionConfirmer)
	confirmer.lock.Lock()
	defer confirmer.lock.Unlock()
	
	count := 0
	for _, invalidated := range confirmer.invalidated {
		if invalidated {
			count++
		}
	}
	return count
}

// StartInvalidatedTransactionMonitor starts a background goroutine that monitors for invalidated transactions
// and resubmits them automatically
func (c *BlockchainClient) StartInvalidatedTransactionMonitor() {
	go func() {
		ticker := time.NewTicker(5 * time.Second) // Check every 5 seconds
		defer ticker.Stop()
		
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Debugf("Stopping invalidated transaction monitor")
				return
			case <-ticker.C:
				// Check for invalidated transactions and resubmit them
				if c.GetInvalidatedTransactionCount() > 0 {
					c.logger.Debugf("Found invalidated transactions (resubmission disabled)")
					// TEMPORARILY DISABLED: Resubmit invalidated transactions
					// if err := c.ResubmitAllInvalidatedTransactions(); err != nil {
					// 	c.logger.Errorf("Error resubmitting invalidated transactions: %v", err)
					// }
				}
			}
		}
	}()
	
	c.logger.Infof("Started invalidated transaction monitor")
}

// GetFinalityTime returns the calculated finality time for this client
func (c *BlockchainClient) GetFinalityTime() time.Duration {
	return c.finalityTime
}

// GetLastConfirmedTransactionTime returns the time when the last transaction was confirmed
func (c *BlockchainClient) GetLastConfirmedTransactionTime() time.Time {
	c.finalityMutex.Lock()
	defer c.finalityMutex.Unlock()
	return c.lastConfirmedTx
}
