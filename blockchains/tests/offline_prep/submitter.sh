#!/bin/bash

# Cardano Transaction Submitter Script
# This script submits prepared transactions (either sequentially or concurrently)

set -e

# Configuration
SUBMISSION_MODE=${1:-sequential}  # sequential or concurrent
DELAY_BETWEEN_TXS=${2:-1}        # Delay in seconds between sequential submissions
TESTNET_MAGIC=42
SOCKET_PATH="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/bft1.socket"
TRANSACTIONS_DIR="prepared_transactions"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Cardano Transaction Submitter ===${NC}"
echo "Submission mode: $SUBMISSION_MODE"

# Ensure socket path is set
export CARDANO_NODE_SOCKET_PATH="$SOCKET_PATH"
echo -e "${YELLOW}Socket path set to: $CARDANO_NODE_SOCKET_PATH${NC}"

# Check if transactions directory exists
if [[ ! -d "$TRANSACTIONS_DIR" ]]; then
    echo -e "${RED}Error: Transactions directory $TRANSACTIONS_DIR not found${NC}"
    echo "Please run the transaction preparer script first"
    exit 1
fi

# Find all signed transaction files
SIGNED_TXS=($(find "$TRANSACTIONS_DIR" -name "*.signed" | sort))
NUM_TXS=${#SIGNED_TXS[@]}

if [[ $NUM_TXS -eq 0 ]]; then
    echo -e "${RED}Error: No signed transactions found in $TRANSACTIONS_DIR${NC}"
    exit 1
fi

echo -e "${GREEN}Found $NUM_TXS prepared transactions${NC}"

# Function to submit a single transaction
submit_transaction() {
    local tx_file=$1
    local tx_name=$(basename "$tx_file" .signed)
    
    echo -e "${BLUE}Submitting $tx_name...${NC}"
    
    if cardano-cli conway transaction submit \
        --tx-file "$tx_file" \
        --testnet-magic $TESTNET_MAGIC 2>/dev/null; then
        echo -e "${GREEN}✓ $tx_name submitted successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ $tx_name submission failed${NC}"
        return 1
    fi
}

# Function for sequential submission
submit_sequential() {
    echo -e "${YELLOW}Submitting transactions sequentially...${NC}"
    
    local successful=0
    local failed=0
    
    for tx_file in "${SIGNED_TXS[@]}"; do
        if submit_transaction "$tx_file"; then
            ((successful++))
        else
            ((failed++))
        fi
        
        # Add delay between submissions (except for the last one)
        if [[ $DELAY_BETWEEN_TXS -gt 0 && $tx_file != "${SIGNED_TXS[-1]}" ]]; then
            echo -e "${YELLOW}Waiting $DELAY_BETWEEN_TXS seconds...${NC}"
            sleep $DELAY_BETWEEN_TXS
        fi
    done
    
    echo -e "${GREEN}Sequential submission complete:${NC}"
    echo -e "${GREEN}  Successful: $successful${NC}"
    echo -e "${RED}  Failed: $failed${NC}"
}

# Function for concurrent submission
submit_concurrent() {
    echo -e "${YELLOW}Submitting transactions concurrently...${NC}"
    
    local pids=()
    local results_file="/tmp/tx_submission_results_$$"
    
    # Submit all transactions in parallel
    for i in "${!SIGNED_TXS[@]}"; do
        {
            local tx_file="${SIGNED_TXS[$i]}"
            local tx_name=$(basename "$tx_file" .signed)
            
            if submit_transaction "$tx_file"; then
                echo "SUCCESS:$tx_name" >> "$results_file"
            else
                echo "FAILED:$tx_name" >> "$results_file"
            fi
        } &
        
        pids+=($!)
    done
    
    # Wait for all submissions to complete
    echo -e "${YELLOW}Waiting for all submissions to complete...${NC}"
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    # Count results
    local successful=0
    local failed=0
    
    if [[ -f "$results_file" ]]; then
        successful=$(grep -c "SUCCESS:" "$results_file" 2>/dev/null || echo 0)
        failed=$(grep -c "FAILED:" "$results_file" 2>/dev/null || echo 0)
        rm -f "$results_file"
    fi
    
    echo -e "${GREEN}Concurrent submission complete:${NC}"
    echo -e "${GREEN}  Successful: $successful${NC}"
    echo -e "${RED}  Failed: $failed${NC}"
}

# Function to show transaction status
show_status() {
    echo -e "${BLUE}Transaction files to submit:${NC}"
    for tx_file in "${SIGNED_TXS[@]}"; do
        local tx_name=$(basename "$tx_file" .signed)
        echo "  $tx_name"
    done
    echo ""
}

# Main execution
show_status

case "$SUBMISSION_MODE" in
    "sequential")
        submit_sequential
        ;;
    "concurrent")
        submit_concurrent
        ;;
    *)
        echo -e "${RED}Error: Invalid submission mode '$SUBMISSION_MODE'${NC}"
        echo "Valid modes: sequential, concurrent"
        exit 1
        ;;
esac

echo -e "${YELLOW}Checking final UTxO state...${NC}"
GENESIS_ADDR=$(cat "/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/shelley/genesis-utxo.addr")
cardano-cli conway query utxo \
    --address "$GENESIS_ADDR" \
    --testnet-magic $TESTNET_MAGIC

echo -e "${GREEN}=== Submission Complete ===${NC}"
