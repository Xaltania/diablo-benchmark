package ncardano

import (
	"bytes"
	"context"
	"diablo-benchmark/core"
	"diablo-benchmark/util"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"gopkg.in/yaml.v3"
)

func ctxErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type BlockchainBuilder struct {
	logger       core.Logger
	nextAccount  int
	nextContract int
	txPrepareConfig *TxPrepareConfig
}

func newBuilder(logger core.Logger) *BlockchainBuilder {
	return &BlockchainBuilder{
		logger:       logger,
		nextAccount:  0,
		nextContract: 0,
		txPrepareConfig: nil,
	}
}

func (b *BlockchainBuilder) CreateAccount(stake int) (interface{}, error) {
	var account int = b.nextAccount

	b.logger.Tracef("mint new account %d with stake %d", account, stake)
	b.nextAccount += 1

	return account, nil
}

func (b *BlockchainBuilder) CreateContract(name string) (interface{}, error) {
	var contract int = b.nextContract

	b.logger.Tracef("upload new contract '%s' with id %d", name,
		contract)
	b.nextContract += 1

	return contract, nil
}

func (b *BlockchainBuilder) CreateResource(domain string) (core.SampleFactory, bool) {
	return nil, false
}

func (b *BlockchainBuilder) EncodeTransfer(stake int, from, to interface{}, info core.InteractionInfo) ([]byte, error) {
	params := TransferParams{
		Amount: uint64(stake),
		// Other fields will be ignored since we're using pre-prepared transactions for now
	}

	tx, err := NewTransferTransaction(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create transfer transaction: %w", err)
	}

	// Get the actual ConwayTransaction
	conwayTx, err := tx.getTx(common.Blake2b256{})
	if err != nil {
		return nil, fmt.Errorf("failed to get Conway transaction: %w", err)
	}

	// Log the transaction creation in the primary
	b.logger.Tracef("Created transaction with hash: %s", conwayTx.Hash().String())

	/// Using original bytes now
	txBytes, err := tx.GetTxBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction bytes: %w", err)
	}
	if len(txBytes) == 0 {
		return nil, fmt.Errorf("failed to get transaction bytes: empty result")
	}
	return txBytes, nil
}

func (b *BlockchainBuilder) EncodeInvoke(from, contract interface{}, function string, info core.InteractionInfo) ([]byte, error) {
	var buf bytes.Buffer

	util.NewMonadOutputWriter(&buf).
		SetOrder(binary.LittleEndian).
		Write(uint64(from.(int))).
		Write(uint64(contract.(int))).
		Trust()

	return buf.Bytes(), nil
}

func (b *BlockchainBuilder) EncodeInteraction(itype string, expr core.BenchmarkExpression, info core.InteractionInfo) ([]byte, error) {
	return nil, fmt.Errorf("unknown interaction type '%s'", itype)
}

// loadTxPrepareConfig loads transaction preparation configuration from a YAML file
func (b *BlockchainBuilder) loadTxPrepareConfig(configPath string) error {
	// Read the YAML file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	// Parse the YAML
	var config TxPrepareConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return fmt.Errorf("failed to parse YAML config file '%s': %w", configPath, err)
	}

	// Validate the configuration
	if config.Splits <= 0 {
		return fmt.Errorf("splits must be greater than 0, got %d", config.Splits)
	}
	if config.CapPerTx <= 0 {
		return fmt.Errorf("cap_per_tx must be greater than 0, got %d", config.CapPerTx)
	}
	if config.Threads <= 0 {
		return fmt.Errorf("threads must be greater than 0, got %d", config.Threads)
	}
	if config.InputAddrFile == "" {
		return fmt.Errorf("input_addr_file is required")
	}
	if config.OutputAddrFile == "" {
		return fmt.Errorf("output_addr_file is required")
	}
	if config.SkeyFile == "" {
		return fmt.Errorf("skey_file is required")
	}

	// Set default final skey file if not provided
	if config.FinalSkeyFile == "" {
		config.FinalSkeyFile = config.SkeyFile
	}

	b.txPrepareConfig = &config
	b.logger.Debugf("loaded transaction preparation config: splits=%d, cap_per_tx=%d, threads=%d", 
		config.Splits, config.CapPerTx, config.Threads)
	
	return nil
}

// GetTxPrepareConfig returns the loaded transaction preparation configuration
func (b *BlockchainBuilder) GetTxPrepareConfig() *TxPrepareConfig {
	return b.txPrepareConfig
}
