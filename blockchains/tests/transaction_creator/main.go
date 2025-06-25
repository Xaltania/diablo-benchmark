package main

import (
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
)

type txCreatorFlags struct {
	address      string
	socket       string
	network      string
	networkMagic int
	inAddress    string
	outAddress   string
	amount       uint64
	useTls       bool
	saveToFile   string
	dryRun       bool
	privateKey   string
	verbose      bool
}

func main() {
	flags := &txCreatorFlags{}
	flag.StringVar(&flags.address, "address", "", "TCP address to connect to in address:port format")
	flag.StringVar(&flags.socket, "socket", "", "UNIX socket path to connect to")
	flag.StringVar(&flags.network, "network", "preview", "network name")
	flag.IntVar(&flags.networkMagic, "network-magic", 0, "network magic value (overrides -network)")
	flag.StringVar(&flags.inAddress, "in-address", "", "input address to spend from")
	flag.StringVar(&flags.outAddress, "out-address", "", "output address to send to")
	flag.Uint64Var(&flags.amount, "amount", 1000000, "amount to send in lovelace")
	flag.BoolVar(&flags.useTls, "tls", false, "enable TLS")
	flag.StringVar(&flags.saveToFile, "save-to-file", "", "save transaction to file instead of submitting")
	flag.BoolVar(&flags.dryRun, "dry-run", false, "create transaction but don't submit")
	flag.StringVar(&flags.privateKey, "private-key", "", "private key as CBOR hex for signing (optional)")
	flag.BoolVar(&flags.verbose, "verbose", false, "show detailed debug information")
	flag.Parse()

	if flags.inAddress == "" || flags.outAddress == "" {
		fmt.Println("ERROR: both -in-address and -out-address are required")
		flag.Usage()
		os.Exit(1)
	}

	if flags.address == "" && flags.socket == "" {
		fmt.Println("ERROR: either -address or -socket must be specified")
		flag.Usage()
		os.Exit(1)
	}

	// Set up network magic
	if flags.networkMagic == 0 {
		network, ok := ouroboros.NetworkByName(flags.network)
		if !ok {
			fmt.Printf("ERROR: unknown network: %s\n", flags.network)
			os.Exit(1)
		}
		flags.networkMagic = int(network.NetworkMagic)
	}

	if err := createAndSubmitTransaction(flags); err != nil {
		fmt.Printf("ERROR: %s\n", err)
		os.Exit(1)
	}
}

func createAndSubmitTransaction(flags *txCreatorFlags) error {
	fmt.Println("🚀 Starting transaction creation and submission process...")
	if flags.verbose {
		fmt.Printf("📋 Transaction Parameters:\n")
		fmt.Printf("  - Input Address: %s\n", flags.inAddress)
		fmt.Printf("  - Output Address: %s\n", flags.outAddress)
		fmt.Printf("  - Amount: %d lovelace\n", flags.amount)
		fmt.Printf("  - Network Magic: %d\n", flags.networkMagic)
		fmt.Printf("  - Using TLS: %v\n", flags.useTls)
	}

	// Create connection with timeout
	fmt.Println("📡 Connecting to Cardano node...")
	conn, err := createConnection(flags)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	errorChan := make(chan error, 10)
	go func() {
		for err := range errorChan {
			fmt.Printf("WARNING (async): %s\n", err)
		}
	}()

	// Create Ouroboros connection with keep-alive and optimized settings
	o, err := ouroboros.New(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(uint32(flags.networkMagic)),
		ouroboros.WithErrorChan(errorChan),
		ouroboros.WithKeepAlive(true), // Keep connection alive
		ouroboros.WithLocalStateQueryConfig(localstatequery.NewConfig()),
		ouroboros.WithLocalTxSubmissionConfig(localtxsubmission.NewConfig()),
	)
	if err != nil {
		return fmt.Errorf("failed to create Ouroboros connection: %w", err)
	}
	defer o.Close()

	fmt.Println("✅ Connected to Cardano node successfully!")

	// Parse addresses
	inAddr, err := ledger.NewAddress(flags.inAddress)
	if err != nil {
		return fmt.Errorf("invalid input address: %w", err)
	}
	if flags.verbose {
		fmt.Printf("🔑 Parsed Input Address: %+v\n", inAddr)
	}

	outAddr, err := ledger.NewAddress(flags.outAddress)
	if err != nil {
		return fmt.Errorf("invalid output address: %w", err)
	}
	if flags.verbose {
		fmt.Printf("🔑 Parsed Output Address: %+v\n", outAddr)
	}

	// STEP 1: Query UTxOs quickly
	fmt.Printf("🔍 Querying UTxOs for address: %s\n", flags.inAddress)
	utxos, err := o.LocalStateQuery().Client.GetUTxOByAddress([]ledger.Address{inAddr})
	if err != nil {
		return fmt.Errorf("failed to query UTxOs: %w", err)
	}

	if flags.verbose {
		fmt.Printf("📊 Found %d UTxOs\n", len(utxos.Results))
		for utxoId, utxo := range utxos.Results {
			fmt.Printf("  UTxO %s#%d: %d lovelace\n",
				getUtxoIdHash(utxoId),
				getUtxoIdIdx(utxoId),
				getUtxoAmount(utxo))
		}
	}

	if len(utxos.Results) == 0 {
		return fmt.Errorf("no UTxOs found at input address")
	}

	// STEP 2: Get protocol parameters quickly
	fmt.Println("📋 Getting protocol parameters...")
	baseFee := uint64(200000) // Default fee
	protocolParams, err := o.LocalStateQuery().Client.GetCurrentProtocolParams()
	if err != nil {
		fmt.Printf("⚠️  Could not get protocol parameters, using default fee: %v\n", err)
	} else {
		if flags.verbose {
			fmt.Printf("✅ Got protocol parameters: %+v\n", protocolParams)
		}
		// Use protocol parameters to calculate more accurate fee if available
		if pp, ok := protocolParams.(*conway.ConwayProtocolParameters); ok {
			if flags.verbose {
				fmt.Printf("📊 Protocol Parameters Details:\n")
				fmt.Printf("  - MinFeeA: %d\n", pp.MinFeeA)
				fmt.Printf("  - MinFeeB: %d\n", pp.MinFeeB)
			}
			// Calculate fee based on protocol parameters: minFeeA * txSize + minFeeB
			// Estimate transaction size (will be refined later)
			estimatedTxSize := uint64(300) // Rough estimate for simple transaction
			calculatedFee := uint64(pp.MinFeeA)*estimatedTxSize + uint64(pp.MinFeeB)
			if calculatedFee > baseFee {
				baseFee = calculatedFee
				fmt.Printf("📊 Using calculated fee: %d lovelace\n", baseFee)
			}
		}
	}

	// STEP 3: Select UTxO and build transaction quickly
	fmt.Println("🔧 Building transaction...")

	var selectedUtxoId interface{}
	var selectedUtxo interface{}
	var foundSuitableUtxo bool
	var totalAvailable uint64

	for utxoId, utxo := range utxos.Results {
		amount := getUtxoAmount(utxo)
		totalAvailable += amount
		if flags.verbose {
			fmt.Printf("  Checking UTxO %s#%d: %d lovelace\n",
				getUtxoIdHash(utxoId),
				getUtxoIdIdx(utxoId),
				amount)
		}
		if !foundSuitableUtxo && amount >= flags.amount+baseFee {
			selectedUtxoId = utxoId
			selectedUtxo = utxo
			foundSuitableUtxo = true
			if flags.verbose {
				fmt.Printf("  ✅ Selected this UTxO for transaction\n")
			}
		}
	}

	if !foundSuitableUtxo {
		return fmt.Errorf("no suitable UTxO found (total: %d, needed: %d)", totalAvailable, flags.amount+baseFee)
	}

	fmt.Printf("✅ Selected UTxO: %s#%d (%d lovelace)\n",
		getUtxoIdHash(selectedUtxoId),
		getUtxoIdIdx(selectedUtxoId),
		getUtxoAmount(selectedUtxo))

	// Build transaction components
	txInput := shelley.NewShelleyTransactionInput(
		getUtxoIdHash(selectedUtxoId),
		getUtxoIdIdx(selectedUtxoId),
	)
	if flags.verbose {
		fmt.Printf("📝 Created transaction input: %+v\n", txInput)
	}

	inputItems := []shelley.ShelleyTransactionInput{txInput}
	inputSet := conway.NewConwayTransactionInputSet(inputItems)
	if flags.verbose {
		fmt.Printf("📝 Created input set: %+v\n", inputSet)
	}

	// Calculate outputs
	fee := baseFee
	utxoAmount := getUtxoAmount(selectedUtxo)
	change := utxoAmount - flags.amount - fee

	if flags.verbose {
		fmt.Printf("💰 Transaction amounts:\n")
		fmt.Printf("  - UTxO amount: %d lovelace\n", utxoAmount)
		fmt.Printf("  - Transfer amount: %d lovelace\n", flags.amount)
		fmt.Printf("  - Fee: %d lovelace\n", fee)
		fmt.Printf("  - Change: %d lovelace\n", change)
	}

	var outputs []babbage.BabbageTransactionOutput

	// Destination output
	destOutput := babbage.BabbageTransactionOutput{
		OutputAddress: outAddr,
		OutputAmount: mary.MaryTransactionOutputValue{
			Amount: flags.amount,
			Assets: nil,
		},
		DatumOption: nil,
		ScriptRef:   nil,
	}
	if flags.verbose {
		fmt.Printf("📝 Created destination output: %+v\n", destOutput)
	}
	outputs = append(outputs, destOutput)

	// Change output (if any)
	if change > 0 {
		changeOutput := babbage.BabbageTransactionOutput{
			OutputAddress: inAddr,
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: change,
				Assets: nil,
			},
			DatumOption: nil,
			ScriptRef:   nil,
		}
		if flags.verbose {
			fmt.Printf("📝 Created change output: %+v\n", changeOutput)
		}
		outputs = append(outputs, changeOutput)
	}

	// Create transaction body
	txBody := conway.ConwayTransactionBody{
		TxInputs:  inputSet,
		TxOutputs: outputs,
		TxFee:     fee,
		Ttl:       uint64(time.Now().Add(24 * time.Hour).Unix()), // Set TTL to 24 hours from now
	}
	if flags.verbose {
		fmt.Printf("📝 Created transaction body: %+v\n", txBody)
	}

	// Create witness set
	var witnessSet conway.ConwayTransactionWitnessSet
	if flags.privateKey != "" {
		fmt.Println("🔐 Creating signed witness set...")
		witnessSet, err = createSignedWitnessSet(txBody, flags.privateKey)
		if err != nil {
			return fmt.Errorf("failed to create signed witness set: %w", err)
		}
		if flags.verbose {
			fmt.Printf("✅ Created signed witness set: %+v\n", witnessSet)
		}
	} else {
		fmt.Println("⚠️  Creating unsigned witness set...")
		witnessSet = createMinimalWitnessSet()
		if flags.verbose {
			fmt.Printf("✅ Created minimal witness set: %+v\n", witnessSet)
		}
	}

	// Create complete transaction
	tx := conway.ConwayTransaction{
		Body:       txBody,
		WitnessSet: witnessSet,
		TxIsValid:  true,
		TxMetadata: nil,
	}
	if flags.verbose {
		fmt.Printf("📝 Created complete transaction: %+v\n", tx)
	}

	// Serialize to CBOR using the proper Cbor() method
	txCbor, err := cbor.Encode(&tx)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction to CBOR: %w", err)
	}

	fmt.Printf("📦 Transaction created successfully!\n")
	fmt.Printf("   - Transaction ID: %s\n", tx.Hash().String())
	fmt.Printf("   - CBOR size: %d bytes\n", len(txCbor))
	fmt.Printf("   - Inputs: %d\n", len(tx.Inputs()))
	fmt.Printf("   - Outputs: %d\n", len(tx.Outputs()))
	fmt.Printf("   - Fee: %d lovelace\n", tx.Fee())
	if flags.verbose {
		fmt.Printf("   - CBOR hex: %s\n", hex.EncodeToString(txCbor))
	}

	// STEP 4: Handle submission or saving
	if flags.saveToFile != "" {
		err := os.WriteFile(flags.saveToFile, txCbor, 0644)
		if err != nil {
			return fmt.Errorf("failed to save transaction to file: %w", err)
		}
		fmt.Printf("💾 Transaction saved to file: %s\n", flags.saveToFile)
		return nil
	}

	if flags.dryRun {
		fmt.Println("🧪 Dry run mode - transaction not submitted")
		fmt.Printf("📄 Transaction CBOR: %s\n", hex.EncodeToString(txCbor))
		return nil
	}

	// STEP 5: Submit transaction immediately (minimize time gap)
	if flags.privateKey == "" {
		fmt.Println("❌ No private key provided - transaction would be rejected")
		fmt.Println("💡 Add -private-key flag to submit a signed transaction")
		return nil
	}

	fmt.Println("🚀 Submitting transaction to blockchain...")

	// Submit with retry logic and fresh connection if needed
	return submitTransactionWithRetry(flags, txCbor, tx.Hash().String())
}

func submitTransactionWithRetry(flags *txCreatorFlags, txCbor []byte, txHash string) error {
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("📤 Submission attempt %d/%d...\n", attempt, maxRetries)

		// Create fresh connection for submission
		conn, err := createConnection(flags)
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("failed to create connection on final attempt: %w", err)
			}
			fmt.Printf("⚠️  Connection failed, retrying... (%v)\n", err)
			time.Sleep(time.Second * 2)
			continue
		}

		errorChan := make(chan error, 10)
		// Create a channel to signal submission completion
		doneChan := make(chan bool)

		// Actually handle errors (this was commented out in your code!)
		go func() {
			for err := range errorChan {
				fmt.Printf("⚠️  Async error during submission: %v\n", err)
			}
		}()

		// Create fresh Ouroboros connection for submission
		o, err := ouroboros.New(
			ouroboros.WithConnection(conn),
			ouroboros.WithNetworkMagic(uint32(flags.networkMagic)),
			ouroboros.WithErrorChan(errorChan),
			ouroboros.WithKeepAlive(true),
			ouroboros.WithLocalTxSubmissionConfig(localtxsubmission.NewConfig()),
		)
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("failed to create Ouroboros connection on final attempt: %w", err)
			}
			fmt.Printf("⚠️  Ouroboros connection failed, retrying... (%v)\n", err)
			conn.Close()
			time.Sleep(time.Second * 2)
			continue
		}

		// Give the connection time to establish properly
		time.Sleep(500 * time.Millisecond)

		// Submit transaction in a goroutine
		var submitErr error
		go func() {
			submitErr = o.LocalTxSubmission().Client.SubmitTx(ledger.TxTypeConway, txCbor)
			doneChan <- true
		}()

		// Wait for submission to complete or timeout
		select {
		case <-doneChan:
			// Submission completed
			if submitErr == nil {
				fmt.Println("🎉 Transaction submitted successfully!")
				fmt.Printf("🔗 Transaction ID: %s\n", txHash)
				fmt.Println("✅ Your ADA transfer is now on the blockchain!")
				o.Close()
				return nil
			}
			fmt.Printf("❌ Attempt %d failed: %v\n", attempt, submitErr)
		case <-time.After(10 * time.Second):
			// Timeout
			fmt.Printf("❌ Attempt %d timed out\n", attempt)
			submitErr = fmt.Errorf("submission timeout")
		}

		// Clean up
		o.Close()

		if attempt < maxRetries {
			if isConnectionError(submitErr) || submitErr.Error() == "submission timeout" {
				fmt.Println("🔄 Will retry with fresh connection...")
			} else {
				// If it's not a connection error, the transaction might be invalid
				return fmt.Errorf("transaction submission failed: %w", submitErr)
			}
			time.Sleep(time.Second * 3)
		}
	}

	return fmt.Errorf("failed to submit transaction after %d attempts", maxRetries)
}

// Helper function to detect connection-related errors
func isConnectionError(err error) bool {
	errStr := strings.ToLower(err.Error())
	connectionErrors := []string{
		"protocol is shutting down",
		"eof",
		"connection reset",
		"broken pipe",
		"connection refused",
		"network is unreachable",
		"timeout",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(errStr, connErr) {
			return true
		}
	}
	return false
}

func createConnection(flags *txCreatorFlags) (net.Conn, error) {
	if flags.socket != "" {
		if flags.useTls {
			return nil, fmt.Errorf("TLS not supported with Unix socket")
		}
		return net.Dial("unix", flags.socket)
	} else {
		if flags.useTls {
			return tls.Dial("tcp", flags.address, nil)
		} else {
			return net.Dial("tcp", flags.address)
		}
	}
}

func parsePrivateKeyFromCborHex(cborHex string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	cborData, err := hex.DecodeString(cborHex)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode hex: %w", err)
	}

	var privateKeyBytes []byte
	if _, err := cbor.Decode(cborData, &privateKeyBytes); err != nil {
		if len(cborData) == 32 || len(cborData) == 64 {
			privateKeyBytes = cborData
		} else {
			return nil, nil, fmt.Errorf("failed to decode private key CBOR: %w", err)
		}
	}

	var privKey ed25519.PrivateKey
	var pubKey ed25519.PublicKey

	if len(privateKeyBytes) == 32 {
		privKey = ed25519.NewKeyFromSeed(privateKeyBytes)
		pubKey = privKey.Public().(ed25519.PublicKey)
	} else if len(privateKeyBytes) == 64 {
		privKey = ed25519.PrivateKey(privateKeyBytes)
		pubKey = privKey.Public().(ed25519.PublicKey)
	} else {
		return nil, nil, fmt.Errorf("invalid private key length: %d (expected 32 or 64 bytes)", len(privateKeyBytes))
	}

	return privKey, pubKey, nil
}

func createSignedWitnessSet(txBody conway.ConwayTransactionBody, privateKeyCborHex string) (conway.ConwayTransactionWitnessSet, error) {
	privKey, pubKey, err := parsePrivateKeyFromCborHex(privateKeyCborHex)
	if err != nil {
		return conway.ConwayTransactionWitnessSet{}, fmt.Errorf("failed to parse private key: %w", err)
	}

	txBodyHash := txBody.Hash()
	signature := ed25519.Sign(privKey, txBodyHash[:])

	vkeyWitness := common.VkeyWitness{
		Vkey:      pubKey,
		Signature: signature,
	}

	return conway.ConwayTransactionWitnessSet{
		VkeyWitnesses:      []common.VkeyWitness{vkeyWitness},
		WsNativeScripts:    []common.NativeScript{},
		BootstrapWitnesses: []common.BootstrapWitness{},
		WsPlutusV1Scripts:  [][]byte{},
		WsPlutusData:       []cbor.Value{},
		WsRedeemers:        conway.ConwayRedeemers{Redeemers: make(map[conway.ConwayRedeemerKey]conway.ConwayRedeemerValue)},
		WsPlutusV2Scripts:  [][]byte{},
		WsPlutusV3Scripts:  [][]byte{},
	}, nil
}

func createMinimalWitnessSet() conway.ConwayTransactionWitnessSet {
	return conway.ConwayTransactionWitnessSet{
		VkeyWitnesses:      []common.VkeyWitness{},
		WsNativeScripts:    []common.NativeScript{},
		BootstrapWitnesses: []common.BootstrapWitness{},
		WsPlutusV1Scripts:  [][]byte{},
		WsPlutusData:       []cbor.Value{},
		WsRedeemers:        conway.ConwayRedeemers{Redeemers: make(map[conway.ConwayRedeemerKey]conway.ConwayRedeemerValue)},
		WsPlutusV2Scripts:  [][]byte{},
		WsPlutusV3Scripts:  [][]byte{},
	}
}

func getUtxoIdHash(utxoId interface{}) string {
	v := reflect.ValueOf(utxoId)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	hashField := v.FieldByName("Hash")
	if !hashField.IsValid() {
		panic("Could not find Hash field in UTxO ID")
	}

	stringMethod := hashField.MethodByName("String")
	if !stringMethod.IsValid() {
		panic("Could not find String() method on Hash field")
	}

	result := stringMethod.Call([]reflect.Value{})
	return result[0].String()
}

func getUtxoIdIdx(utxoId interface{}) int {
	v := reflect.ValueOf(utxoId)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	idxField := v.FieldByName("Idx")
	if !idxField.IsValid() {
		panic("Could not find Idx field in UTxO ID")
	}

	switch idxField.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(idxField.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(idxField.Uint())
	default:
		panic("Idx field is not a numeric type")
	}
}

func getUtxoAmount(utxo interface{}) uint64 {
	v := reflect.ValueOf(utxo)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	outputAmountField := v.FieldByName("OutputAmount")
	if !outputAmountField.IsValid() {
		panic("Could not find OutputAmount field")
	}

	if outputAmountField.Kind() == reflect.Ptr {
		outputAmountField = outputAmountField.Elem()
	}

	amountField := outputAmountField.FieldByName("Amount")
	if !amountField.IsValid() || amountField.Kind() != reflect.Uint64 {
		panic("Could not find Amount field in OutputAmount")
	}

	return amountField.Uint()
}
