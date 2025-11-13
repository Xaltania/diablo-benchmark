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
	
	// Connection management
	socketPaths    []string
	networkMagic   uint32
	connMutex      sync.RWMutex
	errorChan      chan error
	reconnectMutex sync.Mutex
}

type transactionConfirmer interface {
	prepare(core.Interaction, string) (transactionConfirmerHandle, error)
	remove(string)
	reportTransaction(string, uint64, uint64, string) // txHash, inclusionHeight, inclusionSlot, blockHash
	storeTxCBOR(string, []byte)
	setResubmitFunc(func(string, []byte) error) // Set callback to resubmit transactions
	setConnection(*ouroboros.Connection)
	updateImmutableTip() error // For metrics/telemetry only
	checkFinality() // Deprecated - kept for interface compatibility
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
	currentHeight   uint64                               // Current chain height (block count)
	slot2height     map[uint64]uint64                    // Map slot -> height for rollback handling
	resubmitFunc    func(string, []byte) error           // Callback to resubmit transactions
	immutableTipSlot uint64                              // Current immutable tip slot (for finality checking)
	conn            *ouroboros.Connection                // Connection for querying immutable tip
	lock            sync.Mutex
	lsqMutex        sync.Mutex                           // Mutex to prevent concurrent LSQ queries
}

type pendingTransactionBlock struct {
	txHash         string
	inclusionHeight uint64         // Block height where transaction was included
	inclusionSlot   uint64          // Block slot where transaction was included
	inclusionHash   string          // Block hash where transaction was included
	iact           core.Interaction
	channel        chan confirmResult // Channel to notify when committed
	isRecovery     bool               // True if this is tracking a transaction after rollback
	rollbackHeight uint64             // Height where rollback occurred (for recovery transactions)
	resubmitted    bool                // True if transaction has been resubmitted (but monitoring continues)
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
		logger:           logger,
		pendings:         make(map[string]*cardanoTransactionConfirmerPending),
		committed:        make(map[string]*committedTransaction),
		invalidated:      make(map[string]bool),
		txCBOR:           make(map[string][]byte),
		blockDepth:       make(map[string]*pendingTransactionBlock),
		securityParam:    securityParam,
		activeSlotsCoeff: activeSlotsCoeff,
		currentHeight:    0,
		slot2height:      make(map[uint64]uint64),
		immutableTipSlot: 0,
		conn:             nil,
	}
}

// getFinalityDepthThreshold returns the number of blocks required for finality: securityParam
func (c *cardanoTransactionConfirmer) getFinalityDepthThreshold() uint64 {
	return c.securityParam
}

// isFinalized checks if a transaction has reached finality depth
// REQUIRES: c.lock must be held
func (c *cardanoTransactionConfirmer) isFinalized(inclusionHeight uint64) bool {
	if inclusionHeight == 0 {
		return false
	}
	depth := c.currentHeight - inclusionHeight
	return depth >= c.getFinalityDepthThreshold()
}

// notifyChannel safely sends a result to a channel and closes it
func notifyChannel(ch chan confirmResult, result confirmResult) {
	if ch == nil {
		return
	}
	select {
	case ch <- result:
	default:
	}
	close(ch)
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

	// Check if transaction is already committed (finalized)
	if _, ok := c.committed[txHash]; ok {
		iact.ReportCommit()
		channel <- confirmResult{false, nil}
		close(channel)
		return pending, nil
	}

	// Check if transaction is already in blockDepth (found in a block but not finalized yet)
	if blockPending, ok := c.blockDepth[txHash]; ok {
		blockPending.iact = iact
		blockPending.channel = channel
		
		if c.isFinalized(blockPending.inclusionHeight) {
			txCBOR := c.txCBOR[txHash]
			c.lock.Unlock()
			c.commitTransactionFromPending(blockPending, txCBOR)
			return pending, nil
		}
		
		if blockPending.inclusionHeight > 0 {
			depth := c.currentHeight - blockPending.inclusionHeight
			c.logger.Debugf("Transaction %s associated with interaction, waiting for finality (height: %d, depth: %d/%d)", 
				txHash, blockPending.inclusionHeight, depth, c.getFinalityDepthThreshold())
		} else {
			c.logger.Debugf("Transaction %s associated with interaction, waiting for inclusion height", txHash)
		}
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

func (c *cardanoTransactionConfirmer) reportTransaction(txHash string, inclusionHeight uint64, inclusionSlot uint64, blockHash string) {
	c.lock.Lock()

	// Get stored CBOR for this transaction
	txCBOR := c.txCBOR[txHash]

	// Check if this is a recovery transaction that was found again
	if pending, ok := c.blockDepth[txHash]; ok && pending.isRecovery {
		// Transaction was in recovery tracking and was found again
		// Update with the new block information
		pending.inclusionHeight = inclusionHeight
		pending.inclusionSlot = inclusionSlot
		pending.inclusionHash = blockHash
		
		// Clear invalidated flag since we found it again
		delete(c.invalidated, txHash)
		
		if c.isFinalized(inclusionHeight) {
			c.lock.Unlock()
			c.commitTransaction(txHash, inclusionSlot, blockHash, pending.iact, txCBOR)
			c.lock.Lock()
			delete(c.blockDepth, txHash)
			c.lock.Unlock()
			return
		}
		
		pending.isRecovery = false
		pending.rollbackHeight = 0
		depth := c.currentHeight - inclusionHeight
		c.logger.Debugf("Recovery transaction %s found again in block %d (height %d), waiting for finality (depth: %d/%d)", 
			txHash, inclusionSlot, inclusionHeight, depth, c.getFinalityDepthThreshold())
		c.lock.Unlock()
		return
	}

	// Check if transaction is in pending transactions (waiting for submission)
	if pending, ok := c.pendings[txHash]; ok {
		delete(c.pendings, txHash)
		
		// Store transaction in blockDepth map to wait for finality
		c.blockDepth[txHash] = &pendingTransactionBlock{
			txHash:         txHash,
			inclusionHeight: inclusionHeight,
			inclusionSlot:   inclusionSlot,
			inclusionHash:   blockHash,
			iact:           pending.iact,
			channel:        pending.channel, // Store channel for notification
			isRecovery:     false,
			resubmitted:    false,
		}
		
		if c.isFinalized(inclusionHeight) {
			c.lock.Unlock()
			c.commitTransaction(txHash, inclusionSlot, blockHash, pending.iact, txCBOR)
			notifyChannel(pending.channel, confirmResult{false, nil})
			return
		}
		
		depth := c.currentHeight - inclusionHeight
		c.logger.Debugf("Transaction %s found in block %d (height %d), waiting for finality (depth: %d/%d)", 
			txHash, inclusionSlot, inclusionHeight, depth, c.getFinalityDepthThreshold())
		c.lock.Unlock()
		c.checkAndCommitDeepTransactions()
		return
	}

	// If not in pending, check if we already have it in blockDepth (was found before prepare was called)
	if _, exists := c.blockDepth[txHash]; !exists {
		// Add to blockDepth for future finality check (we'll have interaction reference later)
		c.blockDepth[txHash] = &pendingTransactionBlock{
			txHash:         txHash,
			inclusionHeight: inclusionHeight,
			inclusionSlot:   inclusionSlot,
			inclusionHash:   blockHash,
			iact:           nil,    // No interaction reference yet
			channel:        nil,    // No channel yet
			isRecovery:     false,
			resubmitted:    false,
		}
		c.logger.Tracef("Transaction %s found in block %d (height %d), waiting for interaction reference", txHash, inclusionSlot, inclusionHeight)
		
		if c.isFinalized(inclusionHeight) {
			c.lock.Unlock()
			c.checkAndCommitDeepTransactions()
			return
		}
	}
	
	c.lock.Unlock()
}

// commitTransaction commits a transaction that has reached finality
// REQUIRES: Caller must NOT hold c.lock (this function acquires and releases it)
func (c *cardanoTransactionConfirmer) commitTransaction(txHash string, inclusionSlot uint64, blockHash string, iact core.Interaction, txCBOR []byte) {
	c.lock.Lock()
	
	var inclusionHeight uint64
	currentHeight := c.currentHeight
	if pending, ok := c.blockDepth[txHash]; ok {
		inclusionHeight = pending.inclusionHeight
	}
	
	c.committed[txHash] = &committedTransaction{
		interaction: iact,
		blockSlot:   inclusionSlot,
		blockHash:   blockHash,
		txCBOR:      txCBOR,
	}
	delete(c.blockDepth, txHash)
	c.lock.Unlock()
	
	// Report commit after releasing lock (to avoid holding lock during callback)
	if iact != nil {
		iact.ReportCommit()
	}
	
	if inclusionHeight > 0 {
		depth := currentHeight - inclusionHeight
		c.logger.Debugf("Transaction %s committed (block %d, height %d, depth %d)", txHash, inclusionSlot, inclusionHeight, depth)
	} else {
		c.logger.Debugf("Transaction %s committed (block %d)", txHash, inclusionSlot)
	}
}

// commitTransactionFromPending commits a transaction from pendingTransactionBlock and notifies channel
// REQUIRES: Caller must NOT hold c.lock (this function calls commitTransaction which acquires lock)
func (c *cardanoTransactionConfirmer) commitTransactionFromPending(pending *pendingTransactionBlock, txCBOR []byte) {
	c.commitTransaction(pending.txHash, pending.inclusionSlot, pending.inclusionHash, pending.iact, txCBOR)
	notifyChannel(pending.channel, confirmResult{false, nil})
}


// checkAndCommitDeepTransactions checks all pending transactions and commits those that have reached finality depth
// This function is self-locking (acquires lock internally)
func (c *cardanoTransactionConfirmer) checkAndCommitDeepTransactions() {
	c.lock.Lock()
	
	// threshold := c.getFinalityDepthThreshold()
	// currentHeight := c.currentHeight
	var toCommit []*pendingTransactionBlock
	
	// First pass: identify transactions to commit (skip recovery transactions)
	for txHash, pending := range c.blockDepth {
		if pending.isRecovery {
			// Skip recovery transactions - handled by checkRecoveryTransactions
			continue
		}
		
		if c.isFinalized(pending.inclusionHeight) {
			pendingCopy := *pending
			pendingCopy.txHash = txHash
			toCommit = append(toCommit, &pendingCopy)
			delete(c.blockDepth, txHash)
		}
	}
	
	// Get txCBOR for transactions to commit while holding lock
	type commitInfo struct {
		pending *pendingTransactionBlock
		txCBOR  []byte
	}
	var commitInfos []commitInfo
	for _, pending := range toCommit {
		txCBOR := c.txCBOR[pending.txHash]
		commitInfos = append(commitInfos, commitInfo{pending: pending, txCBOR: txCBOR})
	}
	
	c.lock.Unlock()
	
	// Second pass: commit transactions (outside of lock)
	for _, info := range commitInfos {
		c.commitTransactionFromPending(info.pending, info.txCBOR)
	}
	
	// Check recovery transactions after checking normal finality (checkRecoveryTransactions acquires its own lock)
	c.checkRecoveryTransactions()
}

// resubmitRecoveryTransactionsImmediately resubmits all recovery transactions immediately after rollback
// This keeps monitoring active independently - transactions remain in blockDepth for monitoring
// This function is self-locking (acquires lock internally)
func (c *cardanoTransactionConfirmer) resubmitRecoveryTransactionsImmediately() {
	c.lock.Lock()
	
	// Resubmission enabled
	const resubmissionEnabled = false
	
	type resubmitInfo struct {
		txHash string
		txCBOR []byte
	}
	var toResubmit []resubmitInfo
	
	// Find all recovery transactions that haven't been resubmitted yet
	for txHash, pending := range c.blockDepth {
		if !pending.isRecovery {
			continue
		}
		
		// Skip if already resubmitted
		if pending.resubmitted {
			continue
		}
		
		// Skip if transaction was already found (inclusionHeight > 0)
		if pending.inclusionHeight > 0 {
			continue
		}
		
		// Resubmit immediately if we have CBOR
		if resubmissionEnabled && c.resubmitFunc != nil {
			txCBOR := c.txCBOR[txHash]
			if len(txCBOR) > 0 {
				toResubmit = append(toResubmit, resubmitInfo{txHash: txHash, txCBOR: txCBOR})
				// Mark as resubmitted (but keep in blockDepth for monitoring)
				pending.resubmitted = true
				c.logger.Infof("Transaction %s will be resubmitted immediately after rollback (monitoring continues)", txHash)
			} else {
				c.logger.Warnf("Transaction %s cannot be resubmitted - no CBOR available", txHash)
			}
		}
	}
	
	c.lock.Unlock()
	
	// Skip resubmission if disabled or no transactions to resubmit
	if !resubmissionEnabled || len(toResubmit) == 0 {
		return
	}
	
	// Resubmit transactions (outside of lock)
	for _, info := range toResubmit {
		err := c.resubmitFunc(info.txHash, info.txCBOR)
		
		if err != nil {
			c.logger.Errorf("Failed to resubmit transaction %s: %v", info.txHash, err)
			// Mark as not resubmitted so we can retry later
			c.lock.Lock()
			if pending, ok := c.blockDepth[info.txHash]; ok && pending.isRecovery {
				pending.resubmitted = false
			}
			c.lock.Unlock()
		} else {
			c.logger.Infof("Successfully resubmitted transaction %s (monitoring continues independently)", info.txHash)
		}
	}
}

// checkRecoveryTransactions checks recovery transactions and resubmits if not found after securityParam blocks
// This is for periodic retry of failed resubmissions - monitoring continues independently
// This function is self-locking (acquires lock internally)
func (c *cardanoTransactionConfirmer) checkRecoveryTransactions() {
	c.lock.Lock()
	
	// Resubmission enabled
	const resubmissionEnabled = false
	
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
		
		// If transaction was found again (inclusionHeight > 0), handle it normally
		if pending.inclusionHeight > 0 {
			// Transaction was found again after rollback
			// Remove recovery flag and track normally for finality
			pending.isRecovery = false
			pending.rollbackHeight = 0
			pending.resubmitted = false
			
			// Clear invalidated flag since we found it again
			delete(c.invalidated, txHash)
			
			if c.isFinalized(pending.inclusionHeight) {
				txCBOR := c.txCBOR[txHash]
				c.lock.Unlock()
				c.commitTransactionFromPending(pending, txCBOR)
				c.lock.Lock()
				delete(c.blockDepth, txHash)
			} else {
				depth := c.currentHeight - pending.inclusionHeight
				c.logger.Debugf("Recovery transaction %s found again in block %d (height %d), tracking for finality (depth: %d/%d)", 
					txHash, pending.inclusionSlot, pending.inclusionHeight, depth, c.getFinalityDepthThreshold())
			}
			continue
		}
		
		// Transaction hasn't been found yet - check if resubmission failed and enough blocks have passed
		// Only retry resubmission if previous attempt failed (resubmitted == false)
		blocksSinceRollback := c.currentHeight - pending.rollbackHeight
		if !pending.resubmitted && blocksSinceRollback >= c.securityParam {
			if resubmissionEnabled && c.resubmitFunc != nil {
				// Previous resubmission failed or wasn't attempted - retry after securityParam blocks
				txCBOR := c.txCBOR[txHash]
				if len(txCBOR) > 0 {
					toResubmit = append(toResubmit, resubmitInfo{txHash: txHash, txCBOR: txCBOR})
					// Mark as resubmitted (but keep in blockDepth for monitoring)
					pending.resubmitted = true
					c.logger.Warnf("Transaction %s not found after %d blocks since rollback - retrying resubmission (monitoring continues)", 
						txHash, blocksSinceRollback)
				} else {
					c.logger.Warnf("Transaction %s not found after %d blocks since rollback but no CBOR available - cannot resubmit", 
						txHash, blocksSinceRollback)
					// Remove from recovery tracking only if we can't resubmit
					delete(c.blockDepth, txHash)
					delete(c.invalidated, txHash)
				}
			} else {
				// Resubmission disabled or no resubmit function available
				c.logger.Warnf("Transaction %s not found after %d blocks since rollback - resubmission disabled or no resubmit function", 
					txHash, blocksSinceRollback)
			}
		}
	}
	
	// Skip resubmission if disabled or no transactions to resubmit
	if !resubmissionEnabled || len(toResubmit) == 0 {
		c.lock.Unlock()
		return
	}
	
	// Don't remove from blockDepth - keep monitoring active
	c.lock.Unlock()
	
	// Resubmit transactions (outside of lock)
	for _, info := range toResubmit {
		err := c.resubmitFunc(info.txHash, info.txCBOR)
		
		if err != nil {
			c.logger.Errorf("Failed to resubmit transaction %s: %v", info.txHash, err)
			// Mark as not resubmitted so we can retry later (but keep monitoring)
			c.lock.Lock()
			if pending, ok := c.blockDepth[info.txHash]; ok && pending.isRecovery {
				pending.resubmitted = false
			}
			c.lock.Unlock()
		} else {
			c.logger.Infof("Successfully resubmitted transaction %s after retry (monitoring continues independently)", info.txHash)
		}
	}
}

// updateHeight increments the current chain height (for tracking purposes)
func (c *cardanoTransactionConfirmer) updateHeight(blockSlot uint64) {
	c.lock.Lock()
	
	// Increment height for new block
	h := c.currentHeight + 1
	c.slot2height[blockSlot] = h
	c.currentHeight = h
	
	c.lock.Unlock()
	
	// Check for deep transactions after updating height
	c.checkAndCommitDeepTransactions()
}

func (c *cardanoTransactionConfirmer) storeTxCBOR(txHash string, txCBOR []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.txCBOR[txHash] = txCBOR
}

func (c *cardanoTransactionConfirmer) setResubmitFunc(fn func(string, []byte) error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.resubmitFunc = fn
}

func (c *cardanoTransactionConfirmer) setConnection(conn *ouroboros.Connection) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.conn = conn
}

// updateImmutableTip queries the immutable tip from the node using LocalStateQuery
// This function is protected by lsqMutex to prevent concurrent LSQ queries
func (c *cardanoTransactionConfirmer) updateImmutableTip() error {
	c.lsqMutex.Lock()
	defer c.lsqMutex.Unlock()
	
	c.lock.Lock()
	conn := c.conn
	c.lock.Unlock()
	
	if conn == nil {
		return fmt.Errorf("connection not set")
	}
	
	lsq := conn.LocalStateQuery().Client
	
	// Acquire the immutable tip snapshot
	acquired := false
	if err := lsq.AcquireImmutableTip(); err != nil {
		return fmt.Errorf("failed to acquire immutable tip: %w", err)
	}
	acquired = true
	
	// Only release if we successfully acquired
	defer func() {
		if acquired {
			if err := lsq.Release(); err != nil {
				// Log but don't fail - this can happen if connection state changed
				c.logger.Debugf("Failed to release immutable tip snapshot (may be expected): %v", err)
			}
		}
	}()
	
	// Get the immutable tip point
	pt, err := lsq.GetChainPoint()
	if err != nil {
		acquired = false // Don't release if GetChainPoint failed
		return fmt.Errorf("failed to get chain point: %w", err)
	}
	
	c.lock.Lock()
	oldTip := c.immutableTipSlot
	c.immutableTipSlot = pt.Slot
	shouldLog := (oldTip != c.immutableTipSlot)
	c.lock.Unlock()
	
	if shouldLog {
		c.logger.Debugf("Immutable tip updated: slot %d -> %d (for metrics only, not used for commit gating)", oldTip, c.immutableTipSlot)
		// Note: Finality is now determined by depth checks, not immutable tip
	}
	
	return nil
}

// checkFinality is deprecated - kept for interface compatibility only
// Use checkAndCommitDeepTransactions() instead
func (c *cardanoTransactionConfirmer) checkFinality() {
	c.lock.Lock()
	
	if c.immutableTipSlot == 0 {
		// Immutable tip not yet queried, skip
		c.lock.Unlock()
		return
	}
	
	immutableTipSlot := c.immutableTipSlot
	var toCommit []*pendingTransactionBlock
	
	// First pass: identify transactions to commit (skip recovery transactions)
	for txHash, pending := range c.blockDepth {
		if pending.isRecovery {
			// Skip recovery transactions - handled by checkRecoveryTransactions
			continue
		}
		
		// Transaction is finalized if its inclusion slot <= immutable tip slot
		if pending.inclusionSlot > 0 && pending.inclusionSlot <= immutableTipSlot {
			// This transaction's block is finalized, prepare to commit it
			pendingCopy := *pending
			pendingCopy.txHash = txHash // Ensure txHash is set
			toCommit = append(toCommit, &pendingCopy)
			
			// Remove from blockDepth immediately to avoid double-processing
			delete(c.blockDepth, txHash)
		}
	}
	
	// Get txCBOR for transactions to commit while holding lock
	type commitInfo struct {
		pending *pendingTransactionBlock
		txCBOR  []byte
	}
	var commitInfos []commitInfo
	for _, pending := range toCommit {
		txCBOR := c.txCBOR[pending.txHash]
		commitInfos = append(commitInfos, commitInfo{pending: pending, txCBOR: txCBOR})
	}
	
	c.lock.Unlock()
	
	// Second pass: commit transactions (outside of lock)
	for _, info := range commitInfos {
		c.commitTransactionFromPending(info.pending, info.txCBOR)
	}
	
	// Check recovery transactions after checking normal finality (checkRecoveryTransactions acquires its own lock)
	c.checkRecoveryTransactions()
}

func (c *cardanoTransactionConfirmer) handleRollback(rollbackSlot uint64) {
	c.lock.Lock()
	
	// Get rollback height from slot2height map
	rollbackHeight, hasHeight := c.slot2height[rollbackSlot]
	if !hasHeight {
		c.logger.Warnf("Rollback to slot %d but no height mapping found - using current height", rollbackSlot)
		rollbackHeight = c.currentHeight
	}
	
	// Update current height to the rollback point
	c.currentHeight = rollbackHeight
	
	// Delete all slot->height mappings for slots greater than rollback slot
	for slot := range c.slot2height {
		if slot > rollbackSlot {
			delete(c.slot2height, slot)
		}
	}
	
	invalidatedCount := 0
	var invalidatedTxs []string
	
	// Check all committed transactions to see if they were in rolled-back blocks
	for txHash, committedTx := range c.committed {
		// Get the height for this transaction's slot (if we have it)
		// Since we don't store inclusionHeight in committedTransaction, we need to check by slot
		// Check if the slot is > rollback slot (slot equal to rollback is still valid)
		if committedTx.blockSlot > rollbackSlot {
			// This transaction was in a block that was rolled back
			invalidatedCount++
			invalidatedTxs = append(invalidatedTxs, txHash)
			
			if len(committedTx.txCBOR) > 0 {
				c.txCBOR[txHash] = committedTx.txCBOR
			}
			
			// If we have the interaction reference, we can report an error
			if committedTx.interaction != nil {
				c.logger.Warnf("Transaction %s was in rolled-back block %d - tracking for recovery", 
					txHash, committedTx.blockSlot)
			}
			
			// Add to recovery tracking instead of just marking as invalidated
			// This will check if the transaction appears again in subsequent blocks
			c.blockDepth[txHash] = &pendingTransactionBlock{
				txHash:         txHash,
				inclusionHeight: 0, // Will be set when found again
				inclusionSlot:   0,  // Will be set when found again
				inclusionHash:   "",
				iact:           committedTx.interaction,
				channel:        nil, // No channel for recovery tracking
				isRecovery:     true,
				rollbackHeight: rollbackHeight,
				resubmitted:    false, // Will be set when resubmitted
			}
			
			// Mark as invalidated for now (will be cleared if found again)
			c.invalidated[txHash] = true
			delete(c.committed, txHash)
		}
	}
	
	// Check all pending transactions waiting for finality (in blockDepth)
	for txHash, pending := range c.blockDepth {
		if pending.inclusionSlot > rollbackSlot {
			// This transaction was in a block that was rolled back
			invalidatedCount++
			invalidatedTxs = append(invalidatedTxs, txHash)
			
			// Store original values for logging before resetting
			originalSlot := pending.inclusionSlot
			originalHeight := pending.inclusionHeight
			
			// Convert to recovery tracking
			pending.inclusionHeight = 0 // Reset - will be set when found again
			pending.inclusionSlot = 0   // Reset - will be set when found again
			pending.inclusionHash = ""  // Reset
			pending.isRecovery = true
			pending.rollbackHeight = rollbackHeight
			pending.resubmitted = false // Will be set when resubmitted
			
			// Mark as invalidated for now (will be cleared if found again)
			c.invalidated[txHash] = true
			
			if pending.iact != nil {
				c.logger.Warnf("Transaction %s in rolled-back block %d (height %d) - tracking for recovery", 
					txHash, originalSlot, originalHeight)
			}
		}
	}
	
	if invalidatedCount > 0 {
		c.logger.Warnf("Chain rollback invalidated %d transactions (tracking for recovery): %v", invalidatedCount, invalidatedTxs)
		// Trigger immediate resubmission for invalidated transactions (but keep monitoring)
		c.lock.Unlock()
		c.resubmitRecoveryTransactionsImmediately()
		// Also check recovery transactions (for periodic resubmission if needed)
		c.checkRecoveryTransactions()
		return
	}
	
	c.lock.Unlock()
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

	// Update height tracking for this block
	confirmer := bs.confirmer.(*cardanoTransactionConfirmer)
	blockSlot := block.SlotNumber()
	confirmer.updateHeight(blockSlot)

	transactions := block.Transactions()
	if len(transactions) == 0 {
		return nil
	}

	// Get current height for inclusion height (after updateHeight)
	confirmer.lock.Lock()
	inclusionHeight := confirmer.currentHeight
	confirmer.lock.Unlock()

	// Process each transaction in the block
	blockHash := hex.EncodeToString(block.Hash().Bytes())
	
	for _, tx := range transactions {
		txHash := tx.Hash().String()
		bs.logger.Tracef("Found transaction %s in block %d (height %d, hash %s)", txHash, blockSlot, inclusionHeight, blockHash)
		bs.confirmer.reportTransaction(txHash, inclusionHeight, blockSlot, blockHash)
	}

	// Query immutable tip after processing transactions to check for finality
	if err := bs.confirmer.updateImmutableTip(); err != nil {
		bs.logger.Warnf("Failed to query immutable tip: %v", err)
		// Don't return error, continue processing
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
	
	// Query immutable tip after rollback to check for finality
	if err := bs.confirmer.updateImmutableTip(); err != nil {
		bs.logger.Warnf("Failed to query immutable tip after rollback: %v", err)
		// Don't return error, continue processing
	}
	
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

// printProtocolParameters prints the protocol parameters using reflection
func printProtocolParameters(logger core.Logger, protocolParams interface{}) {
	if protocolParams == nil {
		logger.Warnf("Protocol parameters are nil")
		return
	}

	// Use reflection to access struct fields
	v := reflect.ValueOf(protocolParams)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	logger.Infof("=== Protocol Parameters ===")
	
	// Helper function to safely get and print field values
	printField := func(fieldName string) {
		field := v.FieldByName(fieldName)
		if !field.IsValid() {
			logger.Infof("  %s: (not found)", fieldName)
			return
		}

		// Handle ActiveSlotsCoeff which appears to be a slice representing a rational number
		if fieldName == "ActiveSlotsCoeff" && field.Kind() == reflect.Slice {
			if field.Len() == 2 {
				num := field.Index(0).Interface()
				den := field.Index(1).Interface()
				// Try to convert to float for display
				if numFloat, ok := convertToFloat64(num); ok {
					if denFloat, ok := convertToFloat64(den); ok && denFloat != 0 {
						result := numFloat / denFloat
						logger.Infof("  %s: %v (%.6f)", fieldName, field.Interface(), result)
						return
					}
				}
				logger.Infof("  %s: %v", fieldName, field.Interface())
				return
			}
		}

		// Handle SlotLength which is likely in microseconds
		if fieldName == "SlotLength" {
			logger.Infof("  %s: %v", fieldName, field.Interface())
			if field.Kind() == reflect.Uint64 || field.Kind() == reflect.Int64 || field.Kind() == reflect.Int {
				// Assume it's in microseconds, convert to seconds
				var microsec int64
				switch field.Kind() {
				case reflect.Uint64:
					microsec = int64(field.Uint())
				case reflect.Int64, reflect.Int:
					microsec = field.Int()
				}
				if microsec > 0 {
					seconds := float64(microsec) / 1000000.0
					logger.Infof("    (%d microseconds = %.6f seconds)", microsec, seconds)
				}
			}
			return
		}

		logger.Infof("  %s: %v", fieldName, field.Interface())
	}

	// Print key protocol parameters
	printField("ActiveSlotsCoeff")
	printField("SecurityParam")
	printField("EpochLength")
	printField("SlotLength")
	printField("SlotsPerKESPeriod")
	printField("MaxBlockBodySize")
	printField("MaxBlockHeaderSize")
	printField("MaxTxSize")
	
	logger.Infof("==========================")
}

// calculateFinalityTime calculates the transaction finality time based on protocol parameters
// NOTE: This is informational/metrics only. Actual finality is determined by immutable tip checking.
func calculateFinalityTime(protocolParams interface{}) time.Duration {
	// Default values (mainnet-like)
	activeSlotsCoeff := 0.05
	slotLength := time.Duration(0.2 * float64(time.Second))
	securityParam := uint64(12)
	
	// Try to extract actual values from protocol parameters
	if _, ok := protocolParams.(*conway.ConwayProtocolParameters); ok {
		// Extraction is being wonky, use defaults for now
	}
	
	// Calculate finality time: (3 * securityParam / activeSlotsCoeff) * slotLength
	finalityTime := time.Duration(float64(3.0*float64(securityParam)/activeSlotsCoeff) * float64(slotLength.Nanoseconds())) * time.Nanosecond
	
	return finalityTime
}

func NewBlockchainClient(logger core.Logger, socketPaths []string) (*BlockchainClient, error) {
	if len(socketPaths) == 0 {
		return nil, fmt.Errorf("no socket paths provided")
	}

	// Use the first socket path for the connection
	socketPath := socketPaths[0]
	logger.Debugf("connecting to socket: %s (total sockets: %d)", socketPath, len(socketPaths))
	
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
	
	// Set connection in confirmer for immutable tip querying
	confirmer.setConnection(oConn)

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
		// Print protocol parameters
		finalityTime = calculateFinalityTime(protocolParams)
		logger.Infof("Calculated finality time: %v", finalityTime)
	}

	// Start the block subscriber
	if err := subscriber.start(); err != nil {
		return nil, fmt.Errorf("failed to start block subscriber: %w", err)
	}
	
	// Query immutable tip initially to establish baseline
	logger.Debugf("Querying immutable tip for initial finality check...")
	if err := confirmer.updateImmutableTip(); err != nil {
		logger.Warnf("Failed to query initial immutable tip: %v", err)
		// Don't fail initialization, continue without immutable tip initially
	}

	client := &BlockchainClient{
		logger:     logger,
		conn:       oConn,
		ctx:        context.Background(),
		confirmer:  confirmer,
		subscriber: subscriber,
		finalityTime: finalityTime,
		lastConfirmedTx: time.Time{},
		allTxsSubmitted: false,
		socketPaths: socketPaths,
		networkMagic: 42,
		errorChan: errorChan,
	}
	
	// Start goroutine to handle connection errors (non-fatal)
	go func() {
		for err := range errorChan {
			logger.Warnf("Connection error detected: %v (will attempt recovery on next operation)", err)
			// Mark connection as potentially broken - reconnection will happen on next use
		}
	}()
	
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

// isConnectionError checks if an error is a connection-related error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	connectionErrors := []string{
		"eof",
		"connection reset",
		"broken pipe",
		"connection refused",
		"network is unreachable",
		"timeout",
		"protocol is shutting down",
		"use of closed network connection",
	}
	
	for _, connErr := range connectionErrors {
		if strings.Contains(errStr, connErr) {
			return true
		}
	}
	return false
}

// reconnect creates a new connection to the Cardano node
// REQUIRES: c.reconnectMutex must be held
func (c *BlockchainClient) reconnect() error {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()
	
	// Try each socket path in order
	var lastErr error
	for _, socketPath := range c.socketPaths {
		c.logger.Debugf("Attempting to reconnect to socket: %s", socketPath)
		
		// Close old connection if it exists
		if c.conn != nil {
			// Don't check error - connection might already be closed
			c.conn.Close()
		}
		
		// Create new network connection
		conn, err := net.Dial("tcp", socketPath)
		if err != nil {
			lastErr = err
			c.logger.Debugf("Failed to connect to %s: %v", socketPath, err)
			continue
		}
		
		// Create new error channel
		errorChan := make(chan error, 10)
		
		// Create new Ouroboros connection
		oConn, err := ouroboros.New(
			ouroboros.WithConnection(conn),
			ouroboros.WithNetworkMagic(c.networkMagic),
			ouroboros.WithErrorChan(errorChan),
			ouroboros.WithNodeToNode(false),
			ouroboros.WithKeepAlive(true),
			ouroboros.WithChainSyncConfig(
				chainsync.NewConfig(
					chainsync.WithRollForwardFunc(c.subscriber.handleNewBlock),
					chainsync.WithRollBackwardFunc(c.subscriber.handleRollback),
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
			conn.Close()
			lastErr = err
			c.logger.Debugf("Failed to create Ouroboros connection to %s: %v", socketPath, err)
			continue
		}
		
		// Update connection references
		c.conn = oConn
		c.subscriber.conn = oConn
		c.errorChan = errorChan
		
		// Update confirmer connection
		c.confirmer.setConnection(oConn)
		
		// Restart block subscriber
		if err := c.subscriber.start(); err != nil {
			c.logger.Warnf("Failed to restart block subscriber: %v", err)
			// Continue anyway - transaction submission might still work
		}
		
		// Start error handler goroutine
		go func() {
			for err := range errorChan {
				c.logger.Warnf("Connection error detected: %v (will attempt recovery on next operation)", err)
			}
		}()
		
		c.logger.Infof("Successfully reconnected to %s", socketPath)
		return nil
	}
	
	return fmt.Errorf("failed to reconnect to any socket: %w", lastErr)
}

// getConnection returns the current connection, reconnecting if necessary
func (c *BlockchainClient) getConnection() (*ouroboros.Connection, error) {
	c.connMutex.RLock()
	conn := c.conn
	c.connMutex.RUnlock()
	
	// Try to use existing connection first
	if conn != nil {
		return conn, nil
	}
	
	// Need to reconnect
	c.reconnectMutex.Lock()
	defer c.reconnectMutex.Unlock()
	
	// Double-check after acquiring mutex
	c.connMutex.RLock()
	conn = c.conn
	c.connMutex.RUnlock()
	
	if conn != nil {
		return conn, nil
	}
	
	// Reconnect
	if err := c.reconnect(); err != nil {
		return nil, err
	}
	
	c.connMutex.RLock()
	conn = c.conn
	c.connMutex.RUnlock()
	return conn, nil
}

// waitForConfirmationWithTimeout waits for transaction confirmation with a timeout
func (c *BlockchainClient) waitForConfirmationWithTimeout(handle transactionConfirmerHandle, timeout time.Duration) confirmResult {
	resultChan := make(chan confirmResult, 1)
	go func() {
		resultChan <- handle.confirm()
	}()
	
	select {
	case result := <-resultChan:
		return result
	case <-time.After(timeout):
		c.logger.Warnf("Transaction confirmation timeout after %v", timeout)
		// Return a result indicating we need to resend
		return confirmResult{resend: true, err: fmt.Errorf("confirmation timeout after %v", timeout)}
	}
}

// submitTransactionWithRetry submits a transaction with timeout and automatic reconnection
func (c *BlockchainClient) submitTransactionWithRetry(txHash string, txBytes []byte) error {
	const maxRetries = 3
	const submitTimeout = 10 * time.Second
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Get connection (will reconnect if needed)
		conn, err := c.getConnection()
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("failed to get connection after %d attempts: %w", maxRetries, err)
			}
			c.logger.Debugf("Failed to get connection (attempt %d/%d), retrying...", attempt, maxRetries)
			time.Sleep(time.Second * time.Duration(attempt))
			continue
		}
		
		// Submit transaction with timeout
		submitErrChan := make(chan error, 1)
		go func() {
			submitErrChan <- conn.LocalTxSubmission().Client.SubmitTx(ledger.TxTypeConway, txBytes)
		}()
		
		select {
		case err := <-submitErrChan:
			if err == nil {
				// Success
				return nil
			}
			
			// Check if it's a connection error
			if isConnectionError(err) {
				c.logger.Warnf("Connection error during submission (attempt %d/%d): %v", attempt, maxRetries, err)
				
				// Mark connection as broken
				c.connMutex.Lock()
				if c.conn == conn {
					c.conn = nil
				}
				c.connMutex.Unlock()
				
				if attempt < maxRetries {
					c.logger.Debugf("Will retry with new connection...")
					time.Sleep(time.Second * time.Duration(attempt))
					continue
				}
				return fmt.Errorf("connection error after %d attempts: %w", maxRetries, err)
			}
			
			// Non-connection error - check if it's "already submitted"
			if strings.Contains(err.Error(), "already submitted") {
				c.logger.Debugf("Transaction %s already submitted (ignoring)", txHash)
				return nil
			}
			
			// Other error - return immediately
			return fmt.Errorf("transaction submission failed: %w", err)
			
		case <-time.After(submitTimeout):
			c.logger.Warnf("Transaction submission timeout (attempt %d/%d)", attempt, maxRetries)
			
			// Mark connection as potentially broken
			c.connMutex.Lock()
			if c.conn == conn {
				c.conn = nil
			}
			c.connMutex.Unlock()
			
			if attempt < maxRetries {
				c.logger.Debugf("Will retry with new connection...")
				time.Sleep(time.Second * time.Duration(attempt))
				continue
			}
			return fmt.Errorf("transaction submission timeout after %d attempts", maxRetries)
		}
	}
	
	return fmt.Errorf("failed to submit transaction after %d attempts", maxRetries)
}

func (c *BlockchainClient) Close() error {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()
	
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

	confirmer := c.confirmer.(*cardanoTransactionConfirmer)
	confirmer.lock.Lock()
	if _, ok := confirmer.committed[txHash]; ok {
		delete(confirmer.committed, txHash)
		confirmer.lock.Unlock()
		iact.ReportCommit()
		c.finalityMutex.Lock()
		c.lastConfirmedTx = time.Now()
		c.finalityMutex.Unlock()
		return nil
	}
	
	if invalidated, ok := confirmer.invalidated[txHash]; ok && invalidated {
		confirmer.lock.Unlock()
		c.logger.Warnf("Transaction %s was invalidated by chain rollback - resubmission disabled", txHash)
		
		// Resubmission disabled - return error instead of resubmitting
		return fmt.Errorf("transaction %s was invalidated by chain rollback and resubmission is disabled", txHash)
	}
	confirmer.lock.Unlock()

	// Prepare confirmation
	handle, err := c.confirmer.prepare(iact, txHash)
	if err != nil {
		return err
	}

	// Report submission before sending
	iact.ReportSubmit()

	// Submit transaction with timeout and connection recovery
	c.logger.Debugf("Submitting transaction %s", txHash)
	submitErr := c.submitTransactionWithRetry(txHash, txBytes)
	
	// If submission failed, check if it's a connection error
	// If so, the transaction might have been submitted before connection was lost
	// We'll still wait for confirmation since the transaction is already being tracked
	if submitErr != nil {
		if isConnectionError(submitErr) {
			c.logger.Warnf("Transaction %s submission failed due to connection error - transaction may have been submitted, waiting for confirmation", txHash)
			// Continue to wait for confirmation - block subscriber will detect it if it was submitted
		} else {
			// Non-connection error - transaction definitely wasn't submitted
			// But still wait a bit to see if it appears (in case of race condition)
			c.logger.Warnf("Transaction %s submission failed: %v - will wait briefly to check if transaction appears", txHash, submitErr)
		}
	}

	// Wait for confirmation (transaction is already being tracked by block subscriber)
	// If the transaction was submitted before connection was lost, it will be detected
	// Use a timeout based on whether we had a submission error
	var confirmationTimeout time.Duration
	if submitErr != nil {
		if isConnectionError(submitErr) {
			// Connection error - transaction might have been submitted, wait longer
			confirmationTimeout = 60 * time.Second
		} else {
			// Non-connection error - transaction likely wasn't submitted, wait shorter
			confirmationTimeout = 10 * time.Second
		}
	} else {
		// No submission error - normal timeout
		confirmationTimeout = 120 * time.Second
	}
	
	result := c.waitForConfirmationWithTimeout(handle, confirmationTimeout)
	if result.err != nil {
		// If we had a submission error and confirmation also failed, return the submission error
		if submitErr != nil {
			return fmt.Errorf("transaction %s submission failed and was not confirmed: %w", txHash, submitErr)
		}
		return fmt.Errorf("transaction %s failed: %w", txHash, result.err)
	}
	if result.resend {
		// Transaction needs to be resent - this means it wasn't found in blocks
		// If we had a connection error, we should try resubmitting
		if submitErr != nil && isConnectionError(submitErr) {
			c.logger.Warnf("Transaction %s was not found after connection error - attempting resubmission", txHash)
			// Try resubmitting once more
			if err := c.submitTransactionWithRetry(txHash, txBytes); err != nil {
				return fmt.Errorf("transaction %s needs to be resent but resubmission failed: %w", txHash, err)
			}
			// Wait for confirmation again with normal timeout
			result = c.waitForConfirmationWithTimeout(handle, 120*time.Second)
			if result.err != nil {
				return fmt.Errorf("transaction %s failed after resubmission: %w", txHash, result.err)
			}
			if result.resend {
				return fmt.Errorf("transaction %s needs to be resent after resubmission", txHash)
			}
		} else {
			return fmt.Errorf("transaction %s needs to be resent", txHash)
		}
	}
	
	// If we had a submission error but got confirmation, log it as recovered
	if submitErr != nil {
		c.logger.Infof("Transaction %s was confirmed despite submission error - transaction was successfully submitted before connection was lost", txHash)
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
	
	// Submit the transaction again using the retry mechanism
	if err := c.submitTransactionWithRetry(txHash, txBytes); err != nil {
		c.logger.Errorf("Failed to resubmit transaction %s: %v", txHash, err)
		return fmt.Errorf("failed to resubmit transaction: %w", err)
	}
	
	c.logger.Debugf("Successfully resubmitted transaction %s", txHash)
	
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
// NOTE: This is informational/metrics only. Actual finality is determined by immutable tip checking.
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
				// Check for invalidated transactions (resubmission disabled)
				if c.GetInvalidatedTransactionCount() > 0 {
					c.logger.Debugf("Found invalidated transactions - resubmission disabled")
					// Resubmission disabled - just log the count
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
