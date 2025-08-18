package ncardano

import (
	"bytes"
	"diablo-benchmark/core"
	"diablo-benchmark/util"
	"encoding/binary"
	"fmt"
	"context"

	"github.com/blinklabs-io/gouroboros/ledger/common"
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
}

func newBuilder(logger core.Logger) *BlockchainBuilder {
	return &BlockchainBuilder{
		logger:       logger,
		nextAccount:  0,
		nextContract: 0,
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

	// Get the CBOR bytes for the transaction
	txBytes := conwayTx.Cbor()
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
