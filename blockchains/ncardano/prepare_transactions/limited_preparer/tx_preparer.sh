#!/bin/bash

# Cardano Transaction Preparer Script
# This script prepares (builds and signs) transactions to pool accounts but does not submit them

set -e

# Configuration
NUM_TRANSACTIONS=${1:-3}  # Default to 3 transactions if not provided
AMOUNT_PER_TX=${2:-1000000}  # Default 1 ADA (1000000 lovelace) per transaction
TESTNET_MAGIC=42
SOCKET_PATH="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/bft1.socket"
GENESIS_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/shelley/genesis-utxo.addr"
GENESIS_SKEY_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/shelley/genesis-utxo.skey"
PPARAMS_FILE="pparams.json"

# Pool addresses
POOL1_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool1/owner.addr"
POOL2_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool2/owner.addr"
POOL3_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool3/owner.addr"

# Output directory for prepared transactions
OUTPUT_DIR="prepared_transactions"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Cardano Transaction Preparer ===${NC}"
echo "Preparing $NUM_TRANSACTIONS transactions"
echo "Amount per transaction: $AMOUNT_PER_TX lovelace"

# Ensure socket path is set
export CARDANO_NODE_SOCKET_PATH="$SOCKET_PATH"
echo -e "${YELLOW}Socket path set to: $CARDANO_NODE_SOCKET_PATH${NC}"

# Function to check if file exists
check_file() {
    if [[ ! -f "$1" ]]; then
        echo -e "${RED}Error: File $1 not found${NC}"
        exit 1
    fi
}

# Function to check if cardano-cli is available
check_cardano_cli() {
    if ! command -v cardano-cli &> /dev/null; then
        echo -e "${RED}Error: cardano-cli not found in PATH${NC}"
        exit 1
    fi
}

# Verify prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"
check_cardano_cli
check_file "$GENESIS_ADDR_FILE"
check_file "$GENESIS_SKEY_FILE"
check_file "$POOL1_ADDR_FILE"
check_file "$POOL2_ADDR_FILE"
check_file "$POOL3_ADDR_FILE"

# Create output directory
mkdir -p "$OUTPUT_DIR"
echo -e "${GREEN}Created output directory: $OUTPUT_DIR${NC}"

# Query protocol parameters if not exists
if [[ ! -f "$PPARAMS_FILE" ]]; then
    echo -e "${YELLOW}Querying protocol parameters...${NC}"
    cardano-cli conway query protocol-parameters \
        --out-file "$PPARAMS_FILE" \
        --testnet-magic $TESTNET_MAGIC
fi

check_file "$PPARAMS_FILE"
echo -e "${GREEN}Using protocol parameters from $PPARAMS_FILE${NC}"

# Get addresses
GENESIS_ADDR=$(cat "$GENESIS_ADDR_FILE")
POOL1_ADDR=$(cat "$POOL1_ADDR_FILE")
POOL2_ADDR=$(cat "$POOL2_ADDR_FILE")
POOL3_ADDR=$(cat "$POOL3_ADDR_FILE")

echo -e "${BLUE}Pool addresses:${NC}"
echo "  Pool1: $POOL1_ADDR"
echo "  Pool2: $POOL2_ADDR"
echo "  Pool3: $POOL3_ADDR"

# Query available UTxOs
echo -e "${YELLOW}Querying available UTxOs...${NC}"
UTXO_OUTPUT=$(cardano-cli conway query utxo \
    --address "$GENESIS_ADDR" \
    --testnet-magic $TESTNET_MAGIC)

echo "Available UTxOs:"
echo "$UTXO_OUTPUT"

# Parse UTxOs into arrays
declare -a UTXO_HASHES
declare -a UTXO_INDICES
declare -a UTXO_AMOUNTS

# Skip header and parse UTxO lines - using a more robust approach
echo -e "${YELLOW}Parsing UTxO data...${NC}"
while IFS= read -r line; do
    # Skip empty lines, headers, and separator lines
    if [[ -z "$line" || "$line" =~ ^[[:space:]]*$ || "$line" =~ ^[[:space:]]*TxHash || "$line" =~ ^[-]+$ ]]; then
        continue
    fi
    
    # Extract hash, index, and amount using awk
    HASH=$(echo "$line" | awk '{print $1}')
    INDEX=$(echo "$line" | awk '{print $2}')
    AMOUNT=$(echo "$line" | awk '{print $3}')
    
    # Validate that we have all three components
    if [[ -n "$HASH" && -n "$INDEX" && -n "$AMOUNT" && "$HASH" =~ ^[0-9a-f]{64}$ ]]; then
        UTXO_HASHES+=("$HASH")
        UTXO_INDICES+=("$INDEX")
        UTXO_AMOUNTS+=("$AMOUNT")
        echo "  Parsed UTxO: ${HASH}#${INDEX} = ${AMOUNT} lovelace"
    fi
done <<< "$UTXO_OUTPUT"

AVAILABLE_UTXOS=${#UTXO_HASHES[@]}
echo -e "${GREEN}Found $AVAILABLE_UTXOS available UTxOs${NC}"

if [[ $AVAILABLE_UTXOS -lt $NUM_TRANSACTIONS ]]; then
    echo -e "${RED}Error: Not enough UTxOs ($AVAILABLE_UTXOS) for $NUM_TRANSACTIONS transactions${NC}"
    exit 1
fi

# Array of pool addresses for round-robin distribution
POOL_ADDRS=("$POOL1_ADDR" "$POOL2_ADDR" "$POOL3_ADDR")

# Function to prepare a single transaction
prepare_transaction() {
    local tx_num=$1
    local utxo_index=$2
    local pool_addr_index=$((tx_num % 3))  # Round-robin pool selection
    
    # Validate utxo_index
    if [[ $utxo_index -ge ${#UTXO_HASHES[@]} ]]; then
        echo -e "${RED}Error: UTxO index $utxo_index out of range${NC}"
        return 1
    fi
    
    local hash=${UTXO_HASHES[$utxo_index]}
    local index=${UTXO_INDICES[$utxo_index]}
    local amount=${UTXO_AMOUNTS[$utxo_index]}
    local pool_addr=${POOL_ADDRS[$pool_addr_index]}
    
    local tx_prefix="tx_$(printf "%03d" $tx_num)"
    local draft_file="$OUTPUT_DIR/${tx_prefix}.draft"
    local raw_file="$OUTPUT_DIR/${tx_prefix}.raw"
    local signed_file="$OUTPUT_DIR/${tx_prefix}.signed"
    
    echo -e "${BLUE}Preparing transaction $tx_num:${NC}"
    echo "  Input: ${hash}#${index} ($amount lovelace)"
    echo "  Output: $pool_addr ($AMOUNT_PER_TX lovelace)"
    
    # Validate inputs
    if [[ -z "$hash" || -z "$index" || -z "$amount" || -z "$pool_addr" ]]; then
        echo -e "${RED}Error: Missing required transaction data${NC}"
        return 1
    fi
    
    # Calculate change amount (we'll adjust after fee calculation)
    local change_amount=$((amount - AMOUNT_PER_TX))
    
    # Build draft transaction
    echo "  Building draft transaction..."
    if ! cardano-cli conway transaction build-raw \
        --tx-in "${hash}#${index}" \
        --tx-out "${pool_addr}+${AMOUNT_PER_TX}" \
        --tx-out "${GENESIS_ADDR}+${change_amount}" \
        --fee 0 \
        --protocol-params-file "$PPARAMS_FILE" \
        --out-file "$draft_file" 2>/dev/null; then
        echo -e "${RED}Error: Failed to build draft transaction${NC}"
        return 1
    fi
    
    # Calculate fee
    echo "  Calculating fee..."
    local fee_output
    if ! fee_output=$(cardano-cli conway transaction calculate-min-fee \
        --tx-body-file "$draft_file" \
        --protocol-params-file "$PPARAMS_FILE" \
        --witness-count 1 2>/dev/null); then
        echo -e "${RED}Error: Failed to calculate fee${NC}"
        rm -f "$draft_file"
        return 1
    fi
    
    local fee=$(echo "$fee_output" | awk '{print $1}')
    
    # Recalculate change amount with fee
    change_amount=$((amount - AMOUNT_PER_TX - fee))
    
    if [[ $change_amount -lt 0 ]]; then
        echo -e "${RED}Error: Insufficient funds in UTxO for transaction $tx_num${NC}"
        echo "  Required: $((AMOUNT_PER_TX + fee)), Available: $amount"
        rm -f "$draft_file"
        return 1
    fi
    
    echo "  Fee: $fee lovelace"
    echo "  Change: $change_amount lovelace"
    
    # Build final transaction
    echo "  Building final transaction..."
    if ! cardano-cli conway transaction build-raw \
        --tx-in "${hash}#${index}" \
        --tx-out "${pool_addr}+${AMOUNT_PER_TX}" \
        --tx-out "${GENESIS_ADDR}+${change_amount}" \
        --fee "$fee" \
        --protocol-params-file "$PPARAMS_FILE" \
        --out-file "$raw_file" 2>/dev/null; then
        echo -e "${RED}Error: Failed to build final transaction${NC}"
        rm -f "$draft_file"
        return 1
    fi
    
    # Sign transaction
    echo "  Signing transaction..."
    if ! cardano-cli conway transaction sign \
        --tx-body-file "$raw_file" \
        --signing-key-file "$GENESIS_SKEY_FILE" \
        --testnet-magic $TESTNET_MAGIC \
        --out-file "$signed_file" 2>/dev/null; then
        echo -e "${RED}Error: Failed to sign transaction${NC}"
        rm -f "$draft_file" "$raw_file"
        return 1
    fi
    
    echo -e "${GREEN}  Transaction $tx_num prepared and signed: $signed_file${NC}"
    
    # Clean up intermediate files
    rm -f "$draft_file" "$raw_file"
    
    return 0
}

# Prepare transactions
echo -e "${YELLOW}Preparing $NUM_TRANSACTIONS transactions...${NC}"
echo -e "${BLUE}Available UTxOs for use:${NC}"
for ((i=0; i<NUM_TRANSACTIONS && i<AVAILABLE_UTXOS; i++)); do
    echo "  UTxO $((i+1)): ${UTXO_HASHES[$i]}#${UTXO_INDICES[$i]} = ${UTXO_AMOUNTS[$i]} lovelace"
done
echo ""

SUCCESSFUL_TXS=0
for ((i=1; i<=NUM_TRANSACTIONS; i++)); do
    utxo_idx=$((i-1))
    
    echo -e "${YELLOW}Starting preparation of transaction $i...${NC}"
    
    if prepare_transaction $i $utxo_idx; then
        SUCCESSFUL_TXS=$((SUCCESSFUL_TXS + 1))
        echo -e "${GREEN}Transaction $i completed successfully${NC}"
    else
        echo -e "${RED}Failed to prepare transaction $i${NC}"
        # Continue with next transaction instead of exiting
    fi
    echo ""
done

echo -e "${GREEN}=== Transaction Preparation Complete ===${NC}"
echo -e "${GREEN}Successfully prepared: $SUCCESSFUL_TXS/$NUM_TRANSACTIONS transactions${NC}"
echo -e "${YELLOW}Signed transactions saved in: $OUTPUT_DIR/${NC}"

# List prepared transactions
echo -e "${BLUE}Prepared transaction files:${NC}"
ls -la "$OUTPUT_DIR"/*.signed 2>/dev/null || echo "No signed transactions found"

echo ""
echo -e "${YELLOW}To submit all transactions, run:${NC}"
echo "for tx in $OUTPUT_DIR/*.signed; do"
echo "    echo \"Submitting \$tx...\""
echo "    cardano-cli conway transaction submit --tx-file \"\$tx\" --testnet-magic $TESTNET_MAGIC"
echo "    sleep 1  # Optional: wait between submissions"
echo "done"

echo ""
echo -e "${YELLOW}Or use the companion submission script (to be created)${NC}"
