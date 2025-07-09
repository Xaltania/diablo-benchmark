#!/bin/bash

# Cardano Multi-Stage UTxO Splitter Script
# This script splits a genesis UTxO in two stages:
# Stage 1: Split genesis UTxO into N transactions
# Stage 2: Split each resulting UTxO into M outputs each

set -e

# Configuration
STAGE1_SPLITS=${1:-3}  # Default to 3 initial splits
STAGE2_SPLITS=${2:-5}  # Default to 5 splits per UTxO in stage 2
TESTNET_MAGIC=42
SOCKET_PATH="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/bft1.socket"
GENESIS_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/shelley/genesis-utxo.addr"
GENESIS_SKEY_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/shelley/genesis-utxo.skey"
PPARAMS_FILE="pparams.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Cardano Multi-Stage UTxO Splitter ===${NC}"
echo -e "${BLUE}Stage 1: Splitting genesis UTxO into $STAGE1_SPLITS parts${NC}"
echo -e "${BLUE}Stage 2: Splitting each UTxO into $STAGE2_SPLITS parts${NC}"
echo -e "${BLUE}Total final UTxOs: $((STAGE1_SPLITS * STAGE2_SPLITS))${NC}"

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

# Function to wait for transaction confirmation
wait_for_confirmation() {
    echo -e "${YELLOW}Waiting for transaction confirmation...${NC}"
    sleep 5  # Wait 5 seconds for the transaction to be included in a block
}

# Function to split a specific UTxO
split_utxo() {
    local utxo_hash=$1
    local utxo_index=$2
    local utxo_amount=$3
    local num_splits=$4
    local tx_prefix=$5
    
    echo -e "${YELLOW}Splitting UTxO ${utxo_hash}#${utxo_index} into $num_splits parts...${NC}"
    
    # Calculate amount per split
    local amount_per_split=$((utxo_amount / num_splits))
    
    # Build transaction outputs
    local tx_outs=""
    for ((i=1; i<=num_splits; i++)); do
        tx_outs="$tx_outs --tx-out $GENESIS_ADDR+$amount_per_split"
    done
    
    # Build draft transaction
    cardano-cli conway transaction build-raw \
        --tx-in "${utxo_hash}#${utxo_index}" \
        $tx_outs \
        --fee 0 \
        --protocol-params-file "$PPARAMS_FILE" \
        --out-file "${tx_prefix}.draft"
    
    # Calculate minimum fee
    local fee_output=$(cardano-cli conway transaction calculate-min-fee \
        --tx-body-file "${tx_prefix}.draft" \
        --protocol-params-file "$PPARAMS_FILE" \
        --witness-count 1)
    
    local transaction_fee=$(echo "$fee_output" | awk '{print $1}')
    echo -e "${GREEN}Calculated fee: $transaction_fee lovelace${NC}"
    
    # Recalculate amounts accounting for fees
    local available_for_outputs=$((utxo_amount - transaction_fee))
    local base_amount_per_split=$((available_for_outputs / num_splits))
    local remainder=$((available_for_outputs % num_splits))
    
    # Build TX_OUTS with exact amounts
    tx_outs=""
    for ((i=1; i<=num_splits; i++)); do
        if [[ $i -le $remainder ]]; then
            local split_amount=$((base_amount_per_split + 1))
        else
            local split_amount=$base_amount_per_split
        fi
        tx_outs="$tx_outs --tx-out $GENESIS_ADDR+$split_amount"
    done
    
    # Build final transaction
    cardano-cli conway transaction build-raw \
        --tx-in "${utxo_hash}#${utxo_index}" \
        $tx_outs \
        --fee $transaction_fee \
        --protocol-params-file "$PPARAMS_FILE" \
        --out-file "${tx_prefix}.raw"
    
    # Sign the transaction
    cardano-cli conway transaction sign \
        --tx-body-file "${tx_prefix}.raw" \
        --signing-key-file "$GENESIS_SKEY_FILE" \
        --testnet-magic $TESTNET_MAGIC \
        --out-file "${tx_prefix}.signed"
    
    # Submit the transaction
    cardano-cli conway transaction submit \
        --tx-file "${tx_prefix}.signed" \
        --testnet-magic $TESTNET_MAGIC
    
    echo -e "${GREEN}Transaction submitted successfully!${NC}"
    
    # Clean up transaction files
    rm -f "${tx_prefix}.draft" "${tx_prefix}.raw" "${tx_prefix}.signed"
}

# Function to get all UTxOs at the genesis address
get_utxos() {
    local utxo_output=$(cardano-cli conway query utxo \
        --address "$GENESIS_ADDR" \
        --testnet-magic $TESTNET_MAGIC)
    
    echo "$utxo_output"
}

# Function to parse UTxO line and return hash, index, amount
parse_utxo_line() {
    local utxo_line="$1"
    local utxo_hash=$(echo "$utxo_line" | awk '{print $1}')
    local utxo_index=$(echo "$utxo_line" | awk '{print $2}')
    local utxo_amount=$(echo "$utxo_line" | awk '{print $3}')
    
    echo "$utxo_hash $utxo_index $utxo_amount"
}

# Verify prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"
check_cardano_cli
check_file "$GENESIS_ADDR_FILE"
check_file "$GENESIS_SKEY_FILE"

# Query protocol parameters
echo -e "${YELLOW}Querying protocol parameters...${NC}"
cardano-cli conway query protocol-parameters \
    --out-file "$PPARAMS_FILE" \
    --testnet-magic $TESTNET_MAGIC

check_file "$PPARAMS_FILE"
echo -e "${GREEN}Protocol parameters saved to $PPARAMS_FILE${NC}"

# Get genesis address
GENESIS_ADDR=$(cat "$GENESIS_ADDR_FILE")
echo -e "${YELLOW}Genesis address: $GENESIS_ADDR${NC}"

echo -e "\n${BLUE}=== STAGE 1: Initial Split ===${NC}"

# Query initial UTxO
echo -e "${YELLOW}Querying genesis UTxO...${NC}"
INITIAL_UTXO_OUTPUT=$(get_utxos)
echo "Initial UTxO Query Result:"
echo "$INITIAL_UTXO_OUTPUT"

# Parse initial UTxO
INITIAL_UTXO_LINE=$(echo "$INITIAL_UTXO_OUTPUT" | grep -v "TxHash\|^-" | head -n 1 | xargs)

if [[ -z "$INITIAL_UTXO_LINE" ]]; then
    echo -e "${RED}Error: No UTxO found at genesis address${NC}"
    exit 1
fi

# Extract initial UTxO details
read INITIAL_HASH INITIAL_INDEX INITIAL_AMOUNT <<< $(parse_utxo_line "$INITIAL_UTXO_LINE")

echo -e "${GREEN}Found initial UTxO:${NC}"
echo "  Hash: $INITIAL_HASH"
echo "  Index: $INITIAL_INDEX"
echo "  Amount: $INITIAL_AMOUNT lovelace"

# Perform Stage 1 split
split_utxo "$INITIAL_HASH" "$INITIAL_INDEX" "$INITIAL_AMOUNT" "$STAGE1_SPLITS" "stage1_tx"
wait_for_confirmation

echo -e "\n${BLUE}=== STAGE 2: Secondary Splits ===${NC}"

# Query UTxOs after stage 1
echo -e "${YELLOW}Querying UTxOs after stage 1...${NC}"
STAGE2_UTXO_OUTPUT=$(get_utxos)
echo "Stage 2 UTxO Query Result:"
echo "$STAGE2_UTXO_OUTPUT"

# Parse all UTxOs and split each one
counter=1

# Filter out header lines and get actual UTxO data
FILTERED_UTXOS=$(echo "$STAGE2_UTXO_OUTPUT" | grep -v "TxHash\|^-" | grep -v "^$")

echo -e "${YELLOW}Filtered UTxOs to process:${NC}"
echo "$FILTERED_UTXOS"

while IFS= read -r utxo_line; do
    # Skip empty lines
    if [[ -z "$utxo_line" ]]; then
        continue
    fi
    
    # Clean up the line (remove extra whitespace)
    utxo_line=$(echo "$utxo_line" | xargs)
    
    # Parse UTxO details
    read utxo_hash utxo_index utxo_amount <<< $(parse_utxo_line "$utxo_line")
    
    # Validate that we have proper hex hash (64 characters)
    if [[ -n "$utxo_hash" && -n "$utxo_index" && -n "$utxo_amount" && ${#utxo_hash} -eq 64 ]]; then
        echo -e "\n${YELLOW}Processing UTxO $counter: ${utxo_hash}#${utxo_index} (${utxo_amount} lovelace)${NC}"
        split_utxo "$utxo_hash" "$utxo_index" "$utxo_amount" "$STAGE2_SPLITS" "stage2_tx_$counter"
        wait_for_confirmation
        ((counter++))
    else
        echo -e "${RED}Skipping invalid UTxO line: $utxo_line${NC}"
        echo -e "${RED}Parsed values: hash='$utxo_hash' index='$utxo_index' amount='$utxo_amount'${NC}"
    fi
done <<< "$FILTERED_UTXOS"

echo -e "\n${GREEN}=== All splits completed successfully! ===${NC}"
echo -e "${GREEN}Genesis UTxO has been split into $((STAGE1_SPLITS * STAGE2_SPLITS)) UTxOs${NC}"

# Clean up protocol parameters file
rm -f "$PPARAMS_FILE"

echo -e "\n${YELLOW}Final UTxO state:${NC}"
get_utxos

echo -e "\n${GREEN}You can query the final UTxOs with:${NC}"
echo "cardano-cli conway query utxo --address $GENESIS_ADDR --testnet-magic $TESTNET_MAGIC"