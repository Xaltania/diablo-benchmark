package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
)

type Config struct {
	SocketPath      string
	TransactionFile string
	NetworkMagic    uint32
	Verbose         bool
}

// parseFlags parses command line arguments
func parseFlags() *Config {
	config := &Config{}

	flag.StringVar(&config.SocketPath, "socket", "", "Path to Cardano node socket (required)")
	flag.StringVar(&config.TransactionFile, "tx-file", "", "Path to transaction file (required)")
	flag.Func("network-magic", "Network magic (1 for preprod, 2 for preview, 764824073 for mainnet)", func(s string) error {
		var val uint64
		if _, err := fmt.Sscanf(s, "%d", &val); err != nil {
			return err
		}
		config.NetworkMagic = uint32(val)
		return nil
	})
	flag.BoolVar(&config.Verbose, "verbose", false, "Enable verbose logging")

	flag.Parse()

	if config.SocketPath == "" {
		fmt.Fprintf(os.Stderr, "Error: -socket flag is required\n")
		flag.Usage()
		os.Exit(1)
	}

	if config.TransactionFile == "" {
		fmt.Fprintf(os.Stderr, "Error: -tx-file flag is required\n")
		flag.Usage()
		os.Exit(1)
	}

	return config
}

// loadTransactionFromFile loads a transaction from various file formats
func loadTransactionFromFile(filename string) ([]byte, error) {
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("transaction file does not exist: %s", filename)
	}

	// Read file content
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read transaction file: %w", err)
	}

	// Determine file format based on extension and content
	ext := strings.ToLower(filepath.Ext(filename))

	switch ext {
	case ".json":
		return loadFromJSON(content)
	case ".cbor", ".tx":
		return loadFromRawCBOR(content)
	default:
		// Try to detect format by content
		if isJSON(content) {
			return loadFromJSON(content)
		}
		return loadFromRawCBOR(content)
	}
}

// isJSON checks if content appears to be JSON
func isJSON(content []byte) bool {
	trimmed := strings.TrimSpace(string(content))
	return strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")
}

// loadFromJSON loads transaction from JSON format (like Cardano CLI output)
func loadFromJSON(content []byte) ([]byte, error) {
	var jsonData map[string]interface{}
	if err := json.Unmarshal(content, &jsonData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Look for common JSON transaction formats
	if cborHex, ok := jsonData["cborHex"].(string); ok {
		// Cardano CLI format
		return hex.DecodeString(cborHex)
	}

	if txBytes, ok := jsonData["txBody"].(string); ok {
		// Alternative format
		return hex.DecodeString(txBytes)
	}

	if envelope, ok := jsonData["envelope"].(map[string]interface{}); ok {
		if cborHex, ok := envelope["cborHex"].(string); ok {
			return hex.DecodeString(cborHex)
		}
	}

	return nil, fmt.Errorf("unsupported JSON transaction format - expected 'cborHex' field")
}

// loadFromRawCBOR loads transaction from raw CBOR bytes
func loadFromRawCBOR(content []byte) ([]byte, error) {
	// If content looks like hex string, decode it
	contentStr := strings.TrimSpace(string(content))
	if isHexString(contentStr) {
		return hex.DecodeString(contentStr)
	}

	// Otherwise, assume it's raw CBOR bytes
	return content, nil
}

// isHexString checks if string appears to be hexadecimal
func isHexString(s string) bool {
	if len(s)%2 != 0 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// createConwayTransaction creates a Conway transaction from CBOR bytes
func createConwayTransaction(txBytes []byte) (*conway.ConwayTransaction, error) {
	// First, determine if this is actually a Conway transaction
	txType, err := ledger.DetermineTransactionType(txBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to determine transaction type: %w", err)
	}

	// For Conway era, we expect type 6
	if txType != ledger.TxTypeConway {
		return nil, fmt.Errorf("expected Conway transaction (type %d), got type %d", ledger.TxTypeConway, txType)
	}

	// Parse as Conway transaction
	conwayTx, err := conway.NewConwayTransactionFromCbor(txBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Conway transaction: %w", err)
	}

	return conwayTx, nil
}

// submitTransaction submits the transaction to the blockchain
func submitTransaction(config *Config, tx *conway.ConwayTransaction) error {
	if config.Verbose {
		fmt.Printf("Connecting to node at socket: %s\n", config.SocketPath)
		fmt.Printf("Network magic: %d\n", config.NetworkMagic)
		fmt.Printf("Transaction hash: %s\n", tx.Hash().String())
	}

	// Create error channel for async errors
	errorChan := make(chan error, 10)
	go func() {
		for err := range errorChan {
			fmt.Printf("ERROR (async): %s\n", err)
			os.Exit(1)
		}
	}()

	// Create Ouroboros connection
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(config.NetworkMagic),
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithLocalTxSubmissionConfig(
			localtxsubmission.NewConfig(),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create Ouroboros connection: %w", err)
	}
	defer conn.Close()

	// Connect to the socket
	if err := conn.Dial("unix", config.SocketPath); err != nil {
		return fmt.Errorf("failed to connect to node socket: %w", err)
	}

	if config.Verbose {
		fmt.Println("Connected to Cardano node successfully")
		fmt.Println("Submitting transaction...")
	}

	// Submit the transaction
	txBytes := tx.Cbor()
	if err := conn.LocalTxSubmission().Client.SubmitTx(ledger.TxTypeConway, txBytes); err != nil {
		return fmt.Errorf("failed to submit transaction: %w", err)
	}

	return nil
}

func main() {
	// Parse command line flags
	config := parseFlags()

	if config.Verbose {
		fmt.Println("Conway Transaction Submitter")
		fmt.Println("===========================")
	}

	// Load transaction from file
	if config.Verbose {
		fmt.Printf("Loading transaction from: %s\n", config.TransactionFile)
	}

	txBytes, err := loadTransactionFromFile(config.TransactionFile)
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		os.Exit(1)
	}

	if config.Verbose {
		fmt.Printf("Loaded %d bytes of transaction data\n", len(txBytes))
	}

	// Create Conway transaction
	conwayTx, err := createConwayTransaction(txBytes)
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		os.Exit(1)
	}

	if config.Verbose {
		fmt.Printf("Successfully parsed Conway transaction\n")
		fmt.Printf("Transaction hash: %s\n", conwayTx.Hash().String())
		fmt.Printf("Fee: %d lovelace\n", conwayTx.Fee())
		fmt.Printf("Inputs: %d\n", len(conwayTx.Inputs()))
		fmt.Printf("Outputs: %d\n", len(conwayTx.Outputs()))
	}

	// Submit transaction
	if err := submitTransaction(config, conwayTx); err != nil {
		fmt.Printf("ERROR: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Transaction submitted successfully!\n")
	fmt.Printf("Transaction hash: %s\n", conwayTx.Hash().String())

	if config.Verbose {
		fmt.Println("Note: Check the blockchain explorer to confirm transaction inclusion")
	}
}
