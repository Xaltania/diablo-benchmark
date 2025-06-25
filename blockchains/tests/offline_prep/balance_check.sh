#!/bin/bash

# Pool UTxO Checker Script
# Checks UTxOs at all three pool owner addresses

set -e

# Configuration
TESTNET_MAGIC=42
SOCKET_PATH="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/bft1.socket"

# Pool addresses
POOL1_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool1/owner.addr"
POOL2_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool2/owner.addr"
POOL3_ADDR_FILE="/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool3/owner.addr"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}=== Pool UTxO Status ===${NC}"

# Ensure socket path is set
export CARDANO_NODE_SOCKET_PATH="$SOCKET_PATH"

# Function to query and display UTxOs for a pool
check_pool_utxos() {
    local pool_name=$1
    local addr_file=$2
    
    if [[ ! -f "$addr_file" ]]; then
        echo -e "${YELLOW}$pool_name: Address file not found${NC}"
        return
    fi
    
    local addr=$(cat "$addr_file")
    echo -e "${BLUE}$pool_name ($addr):${NC}"
    
    cardano-cli conway query utxo \
        --address "$addr" \
        --testnet-magic $TESTNET_MAGIC || echo "  No UTxOs found"
    
    echo ""
}

# Check each pool
check_pool_utxos "Pool 1" "$POOL1_ADDR_FILE"
check_pool_utxos "Pool 2" "$POOL2_ADDR_FILE" 
check_pool_utxos "Pool 3" "$POOL3_ADDR_FILE"

echo -e "${GREEN}=== Status Check Complete ===${NC}"
