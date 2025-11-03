package ncardano

import (
	"context"
	"diablo-benchmark/core"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"reflect"
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
	setResubmitFunc(func(string, []byte) error) // Set callback to resubmit transactions
}

type confirmResult struct {
	resend bool
	err    error
}

type transactionConfirmerHandle interface {
	confirm() confirmResult
}

type cardanoTransactionConfirmer struct {
	logger          core.Logger
	pendings        map[string]*cardanoTransactionConfirmerPending
	committed       map[string]*committedTransaction
	invalidated     map[string]bool // Track transactions invalidated by rollbacks
	txCBOR          map[string][]byte // Store original CBOR for resubmission
	blockDepth      map[string]*pendingTransactionBlock // Track transactions waiting for finality (txHash -> block info)
	securityParam   uint64                              // Number of blocks required for finality
	activeSlotsCoeff float64                            // Active slots coefficient for finality depth calculation
	currentTip      uint64                               // Current chain tip slot
	resubmitFunc    func(string, []byte) error           // Callback to resubmit transactions
	lock            sync.Mutex
}

type pendingTransactionBlock struct {
	txHash       string
	blockSlot    uint64
	blockHash    string
	iact         core.Interaction
	channel      chan confirmResult // Channel to notify when committed
	isRecovery   bool               // True if this is tracking a transaction after rollback
	rollbackSlot uint64             // Slot where rollback occurred (for recovery transactions)
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

func newCardanoTransactionConfirmer(logger core.Logger, securityParam uint64, activeSlotsCoeff float64) *cardanoTransactionConfirmer {
	return &cardanoTransactionConfirmer{
		logger:          logger,
		pendings:        make(map[string]*cardanoTransactionConfirmerPending),
		committed:       make(map[string]*committedTransaction),
		invalidated:     make(map[string]bool),
		txCBOR:          make(map[string][]byte),
		blockDepth:      make(map[string]*pendingTransactionBlock),
		securityParam:   securityParam,
		activeSlotsCoeff: activeSlotsCoeff,
		currentTip:      0,
	}
}

// getFinalityDepthThreshold returns the number of blocks required for finality: (3 * securityParam / activeSlotsCoeff)
func (c *cardanoTransactionConfirmer) getFinalityDepthThreshold() uint64 {
	return uint64(3.0 * float64(c.securityParam) / c.activeSlotsCoeff)
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

	// Check if transaction is already committed (deep enough)
	if _, ok := c.committed[txHash]; ok {
		delete(c.committed, txHash)
		iact.ReportCommit()
		channel <- confirmResult{false, nil}
		close(channel)
		return pending, nil
	}

	// Check if transaction is already in blockDepth (found in a block but not deep enough yet)
	if blockPending, ok := c.blockDepth[txHash]; ok {
		// Associate the interaction and channel with this pending transaction
		blockPending.iact = iact
		blockPending.channel = channel
		
		// Check if it's already deep enough
		depth := c.currentTip - blockPending.blockSlot
		finalityThreshold := c.getFinalityDepthThreshold()
		if depth >= finalityThreshold {
			// Already deep enough, commit immediately
			txCBOR := c.txCBOR[txHash]
			c.commitTransactionFromPending(blockPending, txCBOR)
			return pending, nil
		}
		
		// Not deep enough yet, will be committed when depth is reached
		// Channel is already stored in blockPending
		c.logger.Debugf("Transaction %s associated with interaction, waiting for finality (depth: %d, required: %d)", 
			txHash, depth, finalityThreshold)
		return pending, nil
	}

	// Transaction not found yet, add to pending transactions (will be moved to blockDepth when found)
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

	// Check if this is a recovery transaction that was found again
	if pending, ok := c.blockDepth[txHash]; ok && pending.isRecovery {
		// Transaction was in recovery tracking and was found again
		c.logger.Infof("Recovery transaction %s found again in block %d after rollback", txHash, blockSlot)
		
		// Update with the new block information
		pending.blockSlot = blockSlot
		pending.blockHash = blockHash
		
		// Clear invalidated flag since we found it again
		delete(c.invalidated, txHash)
		
		// Check if it's already deep enough
		depth := c.currentTip - blockSlot
		finalityThreshold := c.getFinalityDepthThreshold()
		if depth >= finalityThreshold {
			// Already deep enough, commit immediately
			c.commitTransaction(txHash, blockSlot, blockHash, pending.iact, txCBOR)
			delete(c.blockDepth, txHash)
			return
		}
		
		// Remove recovery flag and track normally for finality
		pending.isRecovery = false
		pending.rollbackSlot = 0
		
		c.logger.Debugf("Transaction %s found again in block %d, waiting for finality (current depth: %d, required: %d)", 
			txHash, blockSlot, depth, finalityThreshold)
		return
	}

	// Check if transaction is in pending transactions (waiting for submission)
	if pending, ok := c.pendings[txHash]; ok {
		delete(c.pendings, txHash)
		
		// Store transaction in blockDepth map to wait for finality
		c.blockDepth[txHash] = &pendingTransactionBlock{
			txHash:     txHash,
			blockSlot:  blockSlot,
			blockHash:  blockHash,
			iact:       pending.iact,
			channel:    pending.channel, // Store channel for notification
			isRecovery: false,
		}
		
		// Check if this block is already deep enough (shouldn't happen normally, but possible)
		depth := c.currentTip - blockSlot
		finalityThreshold := c.getFinalityDepthThreshold()
		if depth >= finalityThreshold {
			// Already deep enough, commit immediately
			c.commitTransaction(txHash, blockSlot, blockHash, pending.iact, txCBOR)
			pending.channel <- confirmResult{false, nil}
			close(pending.channel)
		} else {
			// Not deep enough yet, will be committed when depth is reached
			// Channel is stored in blockDepth for later notification
			c.logger.Debugf("Transaction %s found in block %d, waiting for finality (current depth: %d, required: %d)", 
				txHash, blockSlot, depth, finalityThreshold)
		}
		return
	}

	// If not in pending, check if we already have it in blockDepth (was found before prepare was called)
	if _, exists := c.blockDepth[txHash]; !exists {
		// Add to blockDepth for future finality check (we'll have interaction reference later)
		c.blockDepth[txHash] = &pendingTransactionBlock{
			txHash:     txHash,
			blockSlot:  blockSlot,
			blockHash:  blockHash,
			iact:       nil,    // No interaction reference yet
			channel:    nil,    // No channel yet
			isRecovery: false,
		}
		c.logger.Tracef("Transaction %s found in block %d, waiting for interaction reference", txHash, blockSlot)
	}
	
	// Check depth immediately in case it's already deep enough
	depth := c.currentTip - blockSlot
	finalityThreshold := c.getFinalityDepthThreshold()
	if depth >= finalityThreshold {
		c.checkAndCommitDeepTransactions()
	}
}

// commitTransaction commits a transaction that has reached finality depth
func (c *cardanoTransactionConfirmer) commitTransaction(txHash string, blockSlot uint64, blockHash string, iact core.Interaction, txCBOR []byte) {
	if iact != nil {
		iact.ReportCommit()
	}
	
	// Store committed transaction with block info and CBOR
	c.committed[txHash] = &committedTransaction{
		interaction: iact,
		blockSlot:   blockSlot,
		blockHash:   blockHash,
		txCBOR:      txCBOR,
	}
	
	// Remove from blockDepth
	delete(c.blockDepth, txHash)
	
	c.logger.Debugf("Transaction %s committed (block %d is %d blocks deep)", txHash, blockSlot, c.currentTip-blockSlot)
}

// commitTransactionFromPending commits a transaction from pendingTransactionBlock and notifies channel
func (c *cardanoTransactionConfirmer) commitTransactionFromPending(pending *pendingTransactionBlock, txCBOR []byte) {
	// Commit the transaction
	c.commitTransaction(pending.txHash, pending.blockSlot, pending.blockHash, pending.iact, txCBOR)
	
	// Notify channel if it exists
	if pending.channel != nil {
		select {
		case pending.channel <- confirmResult{false, nil}:
			// Successfully sent
		default:
			// Channel buffer full or closed, ignore
		}
		close(pending.channel)
	}
}

// checkAndCommitDeepTransactions checks all pending transactions and commits those that are deep enough
func (c *cardanoTransactionConfirmer) checkAndCommitDeepTransactions() {
	var toCommit []*pendingTransactionBlock
	
	// First pass: identify transactions to commit (skip recovery transactions)
	finalityThreshold := c.getFinalityDepthThreshold()
	for txHash, pending := range c.blockDepth {
		if pending.isRecovery {
			// Skip recovery transactions - handled by checkRecoveryTransactions
			continue
		}
		
		depth := c.currentTip - pending.blockSlot
		if depth >= finalityThreshold {
			// This transaction's block is deep enough, prepare to commit it
			pendingCopy := *pending
			pendingCopy.txHash = txHash // Ensure txHash is set
			toCommit = append(toCommit, &pendingCopy)
			
			// Remove from blockDepth immediately to avoid double-processing
			delete(c.blockDepth, txHash)
		}
	}
	
	// Second pass: commit transactions (outside of lock iteration)
	for _, pending := range toCommit {
		txCBOR := c.txCBOR[pending.txHash]
		c.commitTransactionFromPending(pending, txCBOR)
	}
	
	// Check recovery transactions after checking normal finality
	c.checkRecoveryTransactions()
}

// checkRecoveryTransactions checks recovery transactions and resubmits if not found after securityParam blocks
func (c *cardanoTransactionConfirmer) checkRecoveryTransactions() {
	if c.resubmitFunc == nil {
		// Can't resubmit without callback
		return
	}
	
	type resubmitInfo struct {
		txHash string
		txCBOR []byte
	}
	var toResubmit []resubmitInfo
	
	// Check all recovery transactions
	for txHash, pending := range c.blockDepth {
		if !pending.isRecovery {
			continue
		}
		
		// If transaction was found again (blockSlot > 0), handle it normally
		if pending.blockSlot > 0 {
			// Transaction was found again after rollback
			// Remove recovery flag and track normally for finality
			pending.isRecovery = false
			pending.rollbackSlot = 0
			
			// Clear invalidated flag since we found it again
			delete(c.invalidated, txHash)
			
			c.logger.Infof("Transaction %s found again after rollback in block %d, tracking for finality", txHash, pending.blockSlot)
			
			// Check if it's already deep enough
			depth := c.currentTip - pending.blockSlot
			finalityThreshold := c.getFinalityDepthThreshold()
			if depth >= finalityThreshold {
				// Already deep enough, commit immediately
				txCBOR := c.txCBOR[txHash]
				c.commitTransactionFromPending(pending, txCBOR)
				delete(c.blockDepth, txHash)
			}
			continue
		}
		
		// Transaction hasn't been found yet - check if enough blocks have passed since rollback
		blocksSinceRollback := c.currentTip - pending.rollbackSlot
		if blocksSinceRollback >= c.securityParam {
			// Transaction not found after securityParam blocks - resubmit it
			txCBOR := c.txCBOR[txHash]
			if len(txCBOR) > 0 {
				toResubmit = append(toResubmit, resubmitInfo{txHash: txHash, txCBOR: txCBOR})
				c.logger.Warnf("Transaction %s not found after %d blocks since rollback - will resubmit", 
					txHash, blocksSinceRollback)
			} else {
				c.logger.Warnf("Transaction %s not found after %d blocks since rollback but no CBOR available - cannot resubmit", 
					txHash, blocksSinceRollback)
				// Remove from recovery tracking
				delete(c.blockDepth, txHash)
			}
		}
	}
	
	// If no transactions to resubmit, return early (lock already held)
	if len(toResubmit) == 0 {
		return
	}
	
	// Remove from tracking before unlocking (to avoid double-processing)
	for _, info := range toResubmit {
		delete(c.blockDepth, info.txHash)
		delete(c.invalidated, info.txHash)
	}
	
	// Unlock before calling resubmit callbacks (which may take time)
	c.lock.Unlock()
	
	// Resubmit transactions (outside of lock)
	for _, info := range toResubmit {
		err := c.resubmitFunc(info.txHash, info.txCBOR)
		
		if err != nil {
			c.logger.Errorf("Failed to resubmit transaction %s: %v", info.txHash, err)
			// Re-add to recovery tracking for another attempt later
			c.lock.Lock()
			c.blockDepth[info.txHash] = &pendingTransactionBlock{
				txHash:       info.txHash,
				blockSlot:    0,
				blockHash:    "",
				iact:         nil,
				channel:      nil,
				isRecovery:   true,
				rollbackSlot: c.currentTip, // Use current tip as new rollback point
			}
			c.invalidated[info.txHash] = true
			c.lock.Unlock()
		} else {
			c.logger.Infof("Successfully resubmitted transaction %s after not finding it for %d blocks", info.txHash, c.securityParam)
		}
	}
	
	// Re-lock before returning (since caller expects lock to be held)
	c.lock.Lock()
}

// updateTip updates the current chain tip and checks for finality
func (c *cardanoTransactionConfirmer) updateTip(newTip uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	
	if newTip > c.currentTip {
		c.currentTip = newTip
		c.checkAndCommitDeepTransactions()
	}
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

func (c *cardanoTransactionConfirmer) setResubmitFunc(fn func(string, []byte) error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.resubmitFunc = fn
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
				c.logger.Warnf("Transaction %s was in rolled-back block %d (rollback to slot %d) - tracking for recovery", 
					txHash, committedTx.blockSlot, rollbackSlot)
			}
			
			// Add to recovery tracking instead of just marking as invalidated
			// This will check if the transaction appears again in subsequent blocks
			c.blockDepth[txHash] = &pendingTransactionBlock{
				txHash:       txHash,
				blockSlot:    0, // Will be set when found in a block
				blockHash:    "",
				iact:         committedTx.interaction,
				channel:      nil, // No channel for recovery tracking
				isRecovery:   true,
				rollbackSlot: rollbackSlot,
			}
			
			// Mark as invalidated for now (will be cleared if found again)
			c.invalidated[txHash] = true
			delete(c.committed, txHash)
			
			c.logger.Infof("Transaction %s added to recovery tracking after rollback", txHash)
		}
	}
	
	// Check all pending transactions waiting for finality (in blockDepth)
	for txHash, pending := range c.blockDepth {
		if pending.blockSlot >= rollbackSlot {
			// This transaction was in a block that was rolled back
			invalidatedCount++
			invalidatedTxs = append(invalidatedTxs, txHash)
			
			// Store CBOR for resubmission if available
			txCBOR := c.txCBOR[txHash]
			if len(txCBOR) > 0 {
				c.txCBOR[txHash] = txCBOR
			}
			
			// If we have the interaction reference, log a warning
			if pending.iact != nil {
				c.logger.Warnf("Transaction %s waiting for finality was in rolled-back block %d (rollback to slot %d) - tracking for recovery", 
					txHash, pending.blockSlot, rollbackSlot)
			}
			
			// Convert to recovery tracking
			pending.blockSlot = 0     // Reset - will be set when found again
			pending.blockHash = ""     // Reset
			pending.isRecovery = true
			pending.rollbackSlot = rollbackSlot
			
			// Mark as invalidated for now (will be cleared if found again)
			c.invalidated[txHash] = true
			
			c.logger.Infof("Transaction %s added to recovery tracking after rollback", txHash)
		}
	}
	
	// Update current tip to the rollback point (blocks behind this were rolled back)
	if rollbackSlot < c.currentTip {
		c.currentTip = rollbackSlot
	}
	
	if invalidatedCount > 0 {
		c.logger.Warnf("Chain rollback invalidated %d transactions (tracking for recovery): %v", invalidatedCount, invalidatedTxs)
		// Check recovery transactions after updating tip
		c.checkRecoveryTransactions()
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

	// Update the current tip (from the tip parameter)
	confirmer := bs.confirmer.(*cardanoTransactionConfirmer)
	confirmer.updateTip(tip.Point.Slot)

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

// extractSecurityParam extracts SecurityParam from genesis config using reflection (like main.go)
func extractSecurityParam(genesisConfig interface{}) (uint64, error) {
	// Default value (mainnet-like)
	defaultSecurityParam := uint64(2160)
	
	if genesisConfig == nil {
		return defaultSecurityParam, fmt.Errorf("genesis config is nil")
	}
	
	// Use reflection to access struct fields (like main.go does)
	v := reflect.ValueOf(genesisConfig)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	
	// Try to get SecurityParam field
	field := v.FieldByName("SecurityParam")
	if !field.IsValid() {
		// Field not found - log available fields for debugging
		var fieldNames []string
		for i := 0; i < v.NumField(); i++ {
			fieldNames = append(fieldNames, v.Type().Field(i).Name)
		}
		return defaultSecurityParam, fmt.Errorf("SecurityParam field not found in genesis config. Available fields: %v", fieldNames)
	}
	
	// Convert to uint64
	var securityParam uint64
	switch field.Kind() {
	case reflect.Uint64:
		securityParam = field.Uint()
	case reflect.Uint32:
		securityParam = uint64(field.Uint())
	case reflect.Uint:
		securityParam = uint64(field.Uint())
	case reflect.Int64:
		securityParam = uint64(field.Int())
	case reflect.Int32:
		securityParam = uint64(field.Int())
	case reflect.Int:
		securityParam = uint64(field.Int())
	default:
		// Can't convert, use default
		return defaultSecurityParam, nil
	}
	
	if securityParam == 0 {
		// Invalid value, use default
		return defaultSecurityParam, nil
	}
	
	return securityParam, nil
}

// convertToFloat64 is a helper function to convert various numeric types to float64 (like main.go)
func convertToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}

// extractActiveSlotsCoeff extracts ActiveSlotsCoeff from genesis config using reflection (like main.go)
func extractActiveSlotsCoeff(genesisConfig interface{}) (float64, error) {
	// Default value (mainnet-like)
	defaultActiveSlotsCoeff := 0.2 // Set this because parsing is being a pain.
	
	if genesisConfig == nil {
		return defaultActiveSlotsCoeff, fmt.Errorf("genesis config is nil")
	}
	
	// Use reflection to access struct fields (like main.go does)
	v := reflect.ValueOf(genesisConfig)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	
	// Try to get ActiveSlotsCoeff field
	field := v.FieldByName("ActiveSlotsCoeff")
	if !field.IsValid() {
		return defaultActiveSlotsCoeff, fmt.Errorf("ActiveSlotsCoeff field not found in genesis config")
	}
	
	// Handle ActiveSlotsCoeff which appears to be a slice representing a rational number [numerator, denominator]
	if field.Kind() == reflect.Slice && field.Len() == 2 {
		num := field.Index(0).Interface()
		den := field.Index(1).Interface()
		
		// Try to convert to float for calculation
		numFloat, ok := convertToFloat64(num)
		if !ok {
			return defaultActiveSlotsCoeff, fmt.Errorf("failed to convert numerator to float64")
		}
		
		denFloat, ok := convertToFloat64(den)
		if !ok {
			return defaultActiveSlotsCoeff, fmt.Errorf("failed to convert denominator to float64")
		}
		
		if denFloat == 0 {
			return defaultActiveSlotsCoeff, fmt.Errorf("denominator is zero")
		}
		
		return numFloat / denFloat, nil
	}
	
	// Try direct float conversion if not a slice
	switch field.Kind() {
	case reflect.Float64:
		return field.Float(), nil
	case reflect.Float32:
		return float64(field.Float()), nil
	default:
		return defaultActiveSlotsCoeff, fmt.Errorf("ActiveSlotsCoeff field is not a slice or float")
	}
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
	
	// Calculate finality time: (3 * securityParam / activeSlotsCoeff) * slotLength
	finalityTime := time.Duration(float64(3.0 * float64(securityParam) / activeSlotsCoeff) * float64(slotLength))
	
	return finalityTime
}

func NewBlockchainClient(logger core.Logger, socketPath string) (*BlockchainClient, error) {
	conn, err := net.Dial("tcp", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Cardano node: %w", err)
	}

	errorChan := make(chan error, 10)
	
	// Create confirmer with default securityParam and activeSlotsCoeff (will be updated after querying)
	defaultSecurityParam := uint64(2160)
	defaultActiveSlotsCoeff := 0.05
	confirmer := newCardanoTransactionConfirmer(logger, defaultSecurityParam, defaultActiveSlotsCoeff)
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

	// Query genesis config to get SecurityParam and ActiveSlotsCoeff (like main.go does)
	logger.Debugf("Querying genesis config for SecurityParam and ActiveSlotsCoeff...")
	genesisConfig, err := oConn.LocalStateQuery().Client.GetGenesisConfig()
	var securityParam uint64 = defaultSecurityParam
	var activeSlotsCoeff float64 = defaultActiveSlotsCoeff
	if err != nil {
		logger.Warnf("Failed to query genesis config, using default SecurityParam and ActiveSlotsCoeff: %v", err)
	} else {
		// Extract SecurityParam from genesis config
		securityParam, err = extractSecurityParam(genesisConfig)
		if err != nil {
			logger.Warnf("Failed to extract SecurityParam, using default: %v", err)
			securityParam = defaultSecurityParam
		} else {
			logger.Infof("Extracted SecurityParam: %d blocks", securityParam)
		}
		
		// Extract ActiveSlotsCoeff from genesis config
		activeSlotsCoeff, err = extractActiveSlotsCoeff(genesisConfig)
		if err != nil {
			logger.Warnf("Failed to extract ActiveSlotsCoeff, using default: %v", err)
			activeSlotsCoeff = defaultActiveSlotsCoeff
		} else {
			logger.Infof("Extracted ActiveSlotsCoeff: %.6f", activeSlotsCoeff)
		}
		
		// Update confirmer with actual values
		confirmer.lock.Lock()
		confirmer.securityParam = securityParam
		confirmer.activeSlotsCoeff = activeSlotsCoeff
		confirmer.lock.Unlock()
	}

	// Query protocol parameters for finality time calculation
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
	
	// Set resubmit callback for the confirmer
	confirmer.setResubmitFunc(client.resubmitTransaction)
	
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
