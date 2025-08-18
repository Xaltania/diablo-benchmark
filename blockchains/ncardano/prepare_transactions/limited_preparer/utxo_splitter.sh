#!/bin/bash

# Cardano UTxO Splitter Script
# This script splits a genesis UTxO into multiple UTxOs

set -e

# Configuration
NUM_SPLITS=${1:-5}  # Default to 5 splits if not provided
TESTNET_MAGIC=42
SOCKET_PATH="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/bft1.socket"
GENESIS_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/shelley/genesis-utxo.addr"
GENESIS_SKEY_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/shelley/genesis-utxo.skey"
PPARAMS_FILE="pparams.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Cardano UTxO Splitter ===${NC}"
echo "Splitting genesis UTxO into $NUM_SPLITS parts"

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

# Query UTxO
echo -e "${YELLOW}Querying genesis UTxO...${NC}"
UTXO_OUTPUT=$(cardano-cli conway query utxo \
    --address "$GENESIS_ADDR" \
    --testnet-magic $TESTNET_MAGIC)

echo "UTxO Query Result:"
echo "$UTXO_OUTPUT"

# Parse UTxO output to get hash, index, and amount
# Skip header lines and get the first UTxO
UTXO_LINE=$(echo "$UTXO_OUTPUT" | grep -v "TxHash\|^-" | head -n 1 | xargs)

if [[ -z "$UTXO_LINE" ]]; then
    echo -e "${RED}Error: No UTxO found at genesis address${NC}"
    exit 1
fi

# Extract UTxO hash, index, and amount
UTXO_HASH=$(echo "$UTXO_LINE" | awk '{print $1}')
UTXO_INDEX=$(echo "$UTXO_LINE" | awk '{print $2}')
UTXO_AMOUNT=$(echo "$UTXO_LINE" | awk '{print $3}')

echo -e "${GREEN}Found UTxO:${NC}"
echo "  Hash: $UTXO_HASH"
echo "  Index: $UTXO_INDEX" 
echo "  Amount: $UTXO_AMOUNT lovelace"

# Calculate amount per split (we'll subtract fees later)
AMOUNT_PER_SPLIT=$((UTXO_AMOUNT / NUM_SPLITS))
echo -e "${YELLOW}Amount per split (before fees): $AMOUNT_PER_SPLIT lovelace${NC}"

# Build transaction outputs for splitting
TX_OUTS=""
for ((i=1; i<=NUM_SPLITS; i++)); do
    TX_OUTS="$TX_OUTS --tx-out $GENESIS_ADDR+$AMOUNT_PER_SPLIT"
done

echo -e "${YELLOW}Building draft transaction...${NC}"

# Build draft transaction
cardano-cli conway transaction build-raw \
    --tx-in "${UTXO_HASH}#${UTXO_INDEX}" \
    $TX_OUTS \
    --fee 0 \
    --protocol-params-file "$PPARAMS_FILE" \
    --out-file tx.draft

echo -e "${GREEN}Draft transaction created${NC}"

# Calculate minimum fee
echo -e "${YELLOW}Calculating transaction fee...${NC}"
FEE_OUTPUT=$(cardano-cli conway transaction calculate-min-fee \
    --tx-body-file tx.draft \
    --protocol-params-file "$PPARAMS_FILE" \
    --witness-count 1)

TRANSACTION_FEE=$(echo "$FEE_OUTPUT" | awk '{print $1}')
echo -e "${GREEN}Calculated fee: $TRANSACTION_FEE lovelace${NC}"

# Recalculate amounts accounting for fees
echo -e "${YELLOW}Adjusting split amounts to account for fees...${NC}"

# Calculate the total amount available for outputs (input minus fee)
AVAILABLE_FOR_OUTPUTS=$((UTXO_AMOUNT - TRANSACTION_FEE))

# Calculate base amount per split
BASE_AMOUNT_PER_SPLIT=$((AVAILABLE_FOR_OUTPUTS / NUM_SPLITS))

# Calculate remainder to distribute
REMAINDER=$((AVAILABLE_FOR_OUTPUTS % NUM_SPLITS))

echo -e "${GREEN}Base amount per split: $BASE_AMOUNT_PER_SPLIT lovelace${NC}"
echo -e "${GREEN}Remainder to distribute: $REMAINDER lovelace${NC}"

# Build TX_OUTS with exact amounts
TX_OUTS=""
for ((i=1; i<=NUM_SPLITS; i++)); do
    if [[ $i -le $REMAINDER ]]; then
        # First few outputs get one extra lovelace to distribute remainder
        SPLIT_AMOUNT=$((BASE_AMOUNT_PER_SPLIT + 1))
    else
        SPLIT_AMOUNT=$BASE_AMOUNT_PER_SPLIT
    fi
    TX_OUTS="$TX_OUTS --tx-out $GENESIS_ADDR+$SPLIT_AMOUNT"
done

# Verify the math
TOTAL_OUTPUTS=$((NUM_SPLITS * BASE_AMOUNT_PER_SPLIT + REMAINDER))
echo -e "${GREEN}Total outputs: $TOTAL_OUTPUTS lovelace${NC}"
echo -e "${GREEN}Input amount: $UTXO_AMOUNT lovelace${NC}"
echo -e "${GREEN}Fee: $TRANSACTION_FEE lovelace${NC}"
echo -e "${GREEN}Balance check: $((UTXO_AMOUNT - TRANSACTION_FEE - TOTAL_OUTPUTS)) (should be 0)${NC}"

# Build final transaction with proper fees
echo -e "${YELLOW}Building final transaction with fees...${NC}"
cardano-cli conway transaction build-raw \
    --tx-in "${UTXO_HASH}#${UTXO_INDEX}" \
    $TX_OUTS \
    --fee $TRANSACTION_FEE \
    --protocol-params-file "$PPARAMS_FILE" \
    --out-file tx.raw

echo -e "${GREEN}Final transaction built${NC}"

# Display transaction for verification
echo -e "${YELLOW}Transaction details:${NC}"
cardano-cli debug transaction view --tx-body-file tx.raw

# Sign the transaction
echo -e "${YELLOW}Signing transaction...${NC}"
cardano-cli conway transaction sign \
    --tx-body-file tx.raw \
    --signing-key-file "$GENESIS_SKEY_FILE" \
    --testnet-magic $TESTNET_MAGIC \
    --out-file tx.signed

echo -e "${GREEN}Transaction signed successfully${NC}"

# Submit the transaction
echo -e "${YELLOW}Submitting transaction...${NC}"
cardano-cli conway transaction submit \
    --tx-file tx.signed \
    --testnet-magic $TESTNET_MAGIC

echo -e "${GREEN}=== Transaction submitted successfully! ===${NC}"
echo -e "${GREEN}Genesis UTxO has been split into $NUM_SPLITS UTxOs${NC}"

# Clean up temporary files (optional)
read -p "Clean up temporary files? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -f tx.draft tx.raw tx.signed
    echo -e "${GREEN}Temporary files cleaned up${NC}"
fi

echo -e "${YELLOW}You can now query the UTxOs again to see the splits:${NC}"
echo "cardano-cli conway query utxo --address $GENESIS_ADDR --testnet-magic $TESTNET_MAGIC"
