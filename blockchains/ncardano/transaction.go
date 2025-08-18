package ncardano

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// Common operations for both transfer and invoke transactions
type transaction interface {
	GetTxBytes() ([]byte, error)
	GetBodyBytes() ([]byte, error)
	GetHash() string
	GetDetails() string
	getTx(blockhash common.Blake2b256) (*conway.ConwayTransaction, error)
}

// Parameters for creating a transfer transaction
type TransferParams struct {
	InputTxHash   string
	InputIndex    uint32
	ToAddress     string
	Amount        uint64
	Fee           uint64
	ChangeAddress string
	TTL           uint64
}

var preparedTxCounter = 0

// Use path for now, update to accept from YAML, ideally create transactions in go
//
//	to reduce overhead.
var preparedTxDir = "blockchains/ncardano/prepare_transactions/prepared_transactions"

// TxJson represents the JSON format of the prepared transaction files
type TxJson struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	CborHex     string `json:"cborHex"`
}

// NewTransferTransaction creates a new transaction that transfers ADA to an address
func NewTransferTransaction(params TransferParams) (transaction, error) {
	fmt.Printf("[TRACE] Creating new transfer transaction - To: %s, Amount: %d\n", params.ToAddress, params.Amount)

	// Load pre-prepared transaction
	preparedTxCounter = (preparedTxCounter % 1000000) + 1
	txFileName := fmt.Sprintf("%06d.tx", preparedTxCounter)
	txPath := filepath.Join(preparedTxDir, txFileName)

	// Read the transaction file
	txBytes, err := os.ReadFile(txPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read prepared transaction file: %w", err)
	}

	// Parse the JSON format
	var txJson TxJson
	if err := json.Unmarshal(txBytes, &txJson); err != nil {
		return nil, fmt.Errorf("failed to parse transaction JSON: %w", err)
	}

	// Decode the CBOR hex
	rawTxBytes, err := hex.DecodeString(txJson.CborHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode CBOR hex: %w", err)
	}

	// Create a new base transaction that just returns the raw bytes
	return newBaseTransaction(func(blockhash common.Blake2b256) (*conway.ConwayTransaction, error) {
		// In the primary node, we don't need to decode the transaction
		// We just need to return a dummy transaction that will be used to get the hash
		tx, err := conway.NewConwayTransactionFromCbor(rawTxBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to create transaction from CBOR: %w", err)
		}
		return tx, nil
	}, nil), nil
}

// decodeTransaction is only used by the secondary node to decode transactions
// WARNING decodeTransaction is called to handle strings in client.go, but it doesn't here.
// UPDATE we handled it in client.go, check if it's a good solution
func decodeTransaction(txBytes []byte) (*conway.ConwayTransaction, error) {
	fmt.Printf("[TRACE] Decoding transaction bytes of length: %d\n", len(txBytes))

	txType, err := ledger.DetermineTransactionType(txBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to determine transaction type: %w", err)
	}

	fmt.Printf("[TRACE] Transaction type: %d\n", txType)

	if txType != ledger.TxTypeConway {
		return nil, fmt.Errorf("expected Conway transaction type, got: %d", txType)
	}

	tx, err := conway.NewConwayTransactionFromCbor(txBytes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing transaction: %w", err)
	}

	fmt.Printf("[TRACE] Successfully decoded transaction with hash: %s\n", tx.Hash().String())
	return tx, nil
}

func encodeTransaction(tx *conway.ConwayTransaction) ([]byte, error) {
	if tx == nil {
		return nil, fmt.Errorf("cannot encode nil transaction")
	}

	fmt.Printf("[TRACE] Encoding transaction with hash: %s\n", tx.Hash().String())

	// Use the Cbor() method to serialize the transaction
	serializedTx := tx.Cbor()

	// Verify we got valid CBOR bytes
	if len(serializedTx) == 0 {
		return nil, fmt.Errorf("serialization produced empty result")
	}

	fmt.Printf("[TRACE] Successfully encoded transaction to %d bytes\n", len(serializedTx))
	return serializedTx, nil
}

func FromHex(hexStr string) (*conway.ConwayTransaction, error) {
	bytes, err := hexToCbor(hexStr)
	if err != nil {
		return nil, err
	}
	return decodeTransaction(bytes)
}

// Use in encode?
func GetTxBytes(tx *conway.ConwayTransaction) ([]byte, error) {
	return tx.Cbor(), nil
}

// GetBodyBytes returns the serialised transaction body bytes
func GetBodyBytes(tx *conway.ConwayTransaction) ([]byte, error) {
	return tx.Body.Cbor(), nil
}

func GetHashStr(tx *conway.ConwayTransaction) string {
	return tx.Hash().String()
}

func GetHash(tx *conway.ConwayTransaction) common.Blake2b256 {
	return tx.Hash()
}

func AddWitness(tx *conway.ConwayTransaction, vkey, signature []byte) {
	// witness := common.VkeyWitness{
	// 	Vkey:      vkey,
	// 	Signature: signature,
	// }
}

func Sign(tx *conway.ConwayTransaction, privateKey []byte) error {
	// TODO
	return fmt.Errorf("No signing")
}

// UNCHECKED GPT----------------------------------------
// We don't even use this, maybe we don't need it. Keep it for transaction creation perhaps.
func ToJson(tx *conway.ConwayTransaction) (string, error) {
	type txJson struct {
		Type        string `json:"type"`
		Description string `json:"description"`
		CborHex     string `json:"cborHex"`
	}

	txBytes, err := GetTxBytes(tx)
	if err != nil {
		return "", err
	}

	result := txJson{
		Type:        "Tx ConwayEra",
		Description: "Ledger Cddl Format",
		CborHex:     hex.EncodeToString(txBytes),
	}

	// In a real implementation, you would use json.Marshal to convert to JSON
	// Simplified example:
	jsonString := fmt.Sprintf(`{
    "type": "%s",
    "description": "%s",
    "cborHex": "%s"
}`, result.Type, result.Description, result.CborHex)

	return jsonString, nil
}

// Helper function to convert CBOR hex string to binary
func hexToCbor(hexStr string) ([]byte, error) {
	// Strip the "0x" prefix if present
	if len(hexStr) > 2 && hexStr[:2] == "0x" {
		hexStr = hexStr[2:]
	}
	return hex.DecodeString(hexStr)
}

// GetDetails returns a formatted string with transaction details
// func (ct *conway.ConwayTransaction) GetDetails() string {
// 	details := fmt.Sprintf("Hash: %s\n", ct.Hash().String())
// 	details += fmt.Sprintf("Is Valid: %t\n", ct.TxIsValid)
// 	details += fmt.Sprintf("Fee: %d lovelace\n", ct.Body.TxFee)
// 	details += fmt.Sprintf("TTL: %d\n", ct.Body.Ttl)

// 	// Add input details
// 	details += "Inputs:\n"
// 	for _, input := range ct.Body.TxInputs.Items() {
// 		details += fmt.Sprintf("  - TxHash: %s, Index: %d\n", input.Id(), input.Index())
// 	}

// 	// Add output details
// 	details += "Outputs:\n"
// 	for _, output := range ct.Body.TxOutputs {
// 		details += fmt.Sprintf("  - Address: %s, Amount: %d lovelace\n", output.Address.String(), output.Amount)
// 	}

// 	return details
// }

type baseTransaction struct {
	buildTx func(blockhash common.Blake2b256) (*conway.ConwayTransaction, error)
	signers [][]byte
}

func newBaseTransaction(
	buildTx func(blockhash common.Blake2b256) (*conway.ConwayTransaction, error),
	signers [][]byte) transaction {
	return &baseTransaction{buildTx, signers}
}

func (bt *baseTransaction) getTx(blockhash common.Blake2b256) (*conway.ConwayTransaction, error) {
	return bt.buildTx(blockhash)
}

func (bt *baseTransaction) GetTxBytes() ([]byte, error) {
	tx, err := bt.buildTx(common.Blake2b256{})
	if err != nil {
		return nil, err
	}
	return tx.Cbor(), nil
}

func (bt *baseTransaction) GetBodyBytes() ([]byte, error) {
	tx, err := bt.buildTx(common.Blake2b256{})
	if err != nil {
		return nil, err
	}
	return tx.Body.Cbor(), nil
}

func (bt *baseTransaction) GetHash() string {
	tx, err := bt.buildTx(common.Blake2b256{})
	if err != nil {
		return ""
	}
	return tx.Hash().String()
}

func (bt *baseTransaction) GetDetails() string {
	tx, err := bt.buildTx(common.Blake2b256{})
	if err != nil {
		return fmt.Sprintf("Error building transaction: %v", err)
	}
	return fmt.Sprintf("Transaction Hash: %s", tx.Hash().String())
}

func newTransferTransaction(params TransferParams, signers [][]byte) transaction { // Helper
	buildTx := func(blockhash common.Blake2b256) (*conway.ConwayTransaction, error) {
		tx, err := NewTransferTransaction(params)
		if err != nil {
			return nil, err
		}
		return tx.getTx(blockhash)
	}
	return newBaseTransaction(buildTx, signers)
}
