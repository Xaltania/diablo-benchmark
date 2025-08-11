#!/usr/bin/env bash
set -euo pipefail

# tx_fanout.sh - minimal UTxO fan-out builder using cardano-cli (Conway)
# Requires: cardano-cli in PATH, CARDANO_NODE_SOCKET_PATH set, and tx_split_plan.sh in the same dir (or set SPLIT_PLAN_LIB).
#
# Usage:
#   ./tx_fanout.sh \
#     --n 250000 \
#     --cap 300 \
#     --sender-addr state-cluster0/shelley/genesis-utxo.addr \
#     --sender-skey state-cluster0/shelley/genesis-utxo.skey \
#     --out-addr  state-cluster0/nodes/node-pool1/owner.addr \
#     --magic 42 \
#     --work ./fanout_work \
#     --fee-budget 300000 \
#     [--tx-in TXHASH#TXIX] \
#     [--wait-blocks 1]
#
# Notes (simple, robust defaults):
# - We compute a balanced plan under CAP, then compute the *value per child output* per level so each next level can split.
# - We default leaf outputs to LEAF_ADA (set via --leaf-ada; default 1500000). Tune as needed for your network params.
# - We submit all txs in a level, then wait a few blocks before the next level (so children are spendable).

# ---------- defaults ----------
N=0
CAP=300
SENDER_ADDR=""
SENDER_SKEY=""
OUT_ADDR=""
MAGIC=""
MAINNET=0
WORK="./fanout_work"
FEE_BUDGET=300000           # Lovelace budget per split tx for fees (rough)
LEAF_ADA=1500000            # Lovelace per final leaf output (override with --leaf-ada)
TXIN_OVERRIDE=""
WAIT_BLOCKS=1
SPLIT_PLAN_LIB="${SPLIT_PLAN_LIB:-./tx_split_plan.sh}"
PROGRESS_EVERY=20

# ---------- args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --n) N="$2"; shift 2;;
    --cap) CAP="$2"; shift 2;;
    --sender-addr) SENDER_ADDR="$2"; shift 2;;
    --sender-skey) SENDER_SKEY="$2"; shift 2;;
    --out-addr) OUT_ADDR="$2"; shift 2;;
    --magic) MAGIC="$2"; MAINNET=0; shift 2;;
    --mainnet) MAINNET=1; shift;;
    --work) WORK="$2"; shift 2;;
    --fee-budget) FEE_BUDGET="$2"; shift 2;;
    --leaf-ada) LEAF_ADA="$2"; shift 2;;
    --tx-in) TXIN_OVERRIDE="$2"; shift 2;;
    --wait-blocks) WAIT_BLOCKS="$2"; shift 2;;
    --plan-lib) SPLIT_PLAN_LIB="$2"; shift 2;;
    -h|--help)
      echo "Usage: $0 --n N --sender-addr ADDR --sender-skey SKEY --out-addr ADDR (--magic M|--mainnet) [opts]"
      exit 0;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ -z "${CARDANO_NODE_SOCKET_PATH:-}" ]]; then
  echo "CARDANO_NODE_SOCKET_PATH not set" >&2
  exit 2
fi

if [[ "$N" -le 0 ]]; then
  echo "--n must be > 0" >&2
  exit 2
fi

if [[ -z "$SENDER_ADDR" || -z "$SENDER_SKEY" || -z "$OUT_ADDR" ]]; then
  echo "--sender-addr, --sender-skey, and --out-addr are required" >&2
  exit 2
fi

if [[ ! -f "$SPLIT_PLAN_LIB" ]]; then
  echo "Missing planner library: $SPLIT_PLAN_LIB (set with --plan-lib or SPLIT_PLAN_LIB)" >&2
  exit 2
fi

NET_ARGS=()
if (( MAINNET )); then
  NET_ARGS=(--mainnet)
else
  NET_ARGS=(--testnet-magic "$MAGIC")
fi

mkdir -p "$WORK"

# ---------- source planner and compute plan ----------
# shellcheck source=/dev/null
source "$SPLIT_PLAN_LIB"
split_plan "$N" "$CAP"

echo "Plan: levels=$LEVELS, bfs=(${BFS[*]}), txs=(${TXS_PER_LEVEL[*]}), total=$((${TOTAL_SPLIT_TXS}))"
echo "Leaf per-output value: $LEAF_ADA lovelace ; fee budget per tx: $FEE_BUDGET lovelace"

# ---------- compute child output value per level (top-down) ----------
# OUT_AMTS[i] = value per output created at level i
# Base: OUT_AMTS[L-1] = LEAF_ADA
# Recurrence (from bottom to top):
#   OUT_AMTS[i-1] = BFS[i] * OUT_AMTS[i] + FEE_BUDGET
declare -a OUT_AMTS
if (( LEVELS == 1 )); then
  OUT_AMTS=( "$LEAF_ADA" )
else
  # Build from bottom
  OUT_AMTS=()
  for (( i=0; i<LEVELS; i++ )); do OUT_AMTS+=(0); done
  OUT_AMTS[$((LEVELS-1))]="$LEAF_ADA"
  for (( i=LEVELS-1; i>=1; i-- )); do
    # OUT_AMTS[i-1] = BFS[i] * OUT_AMTS[i] + FEE_BUDGET
    bi="${BFS[$i]}"
    OUT_AMTS[$((i-1))]=$(( bi * OUT_AMTS[$i] + FEE_BUDGET ))
  done
fi
echo -n "Per-level output values: "
for (( i=0; i<LEVELS; i++ )); do
  printf "%s" "${OUT_AMTS[$i]}"
  if (( i < LEVELS-1 )); then printf " "; fi
done
echo ""

# ---------- helper: pick largest UTxO at address ----------
pick_largest_utxo() {
  local addr="$1"
  # outputs lines "TxHash TxIx Amount" -> pick largest lovelace
  cardano-cli conway query utxo --address "$addr" "${NET_ARGS[@]}" \
  | awk 'NR>2 {print $1 "#" $2, $3}' \
  | sort -k2,2nr \
  | awk 'NR==1{print $1}'
}

# ---------- helper: current block number ----------
current_block() {
  cardano-cli conway query tip "${NET_ARGS[@]}" | sed -n 's/.*"block": \([0-9]\+\).*/\1/p'
}

# ---------- begin levels ----------
start_ts=$(date +%s)

# Level 0 input
if [[ -n "$TXIN_OVERRIDE" ]]; then
  L0_INPUT="$TXIN_OVERRIDE"
else
  L0_INPUT=$(pick_largest_utxo "$SENDER_ADDR")
fi
if [[ -z "$L0_INPUT" ]]; then
  echo "Could not find a UTxO to spend at $SENDER_ADDR" >&2
  exit 2
fi

echo "Level 0 input: $L0_INPUT"

# Array of inputs for current level
declare -a CUR_INPUTS NEXT_INPUTS
CUR_INPUTS=( "$L0_INPUT" )

for (( level=0; level<LEVELS; level++ )); do
  bi="${BFS[$level]}"
  outv="${OUT_AMTS[$level]}"
  num_txs="${TXS_PER_LEVEL[$level]}"
  echo "Level $level: $num_txs txs, each creates $bi outputs of $outv lovelace"

  NEXT_INPUTS=()
  lvl_start=$(date +%s)
  for (( t=0; t<num_txs; t++ )); do
    in_ref="${CUR_INPUTS[$t]}"
    if [[ -z "$in_ref" ]]; then
      echo "Internal error: missing input for level $level tx $t" >&2
      exit 2
    fi

    txbase="$WORK/L${level}_T${t}"
    body="$txbase.body"
    signed="$txbase.signed"
    txid_file="$txbase.txid"

    # Build outputs list
    # shellcheck disable=SC2206
    outs=()
    for (( k=0; k<bi; k++ )); do
      outs+=( --tx-out "${OUT_ADDR}+${outv}" )
    done

    # Build, sign, submit
    cardano-cli conway transaction build \
      --tx-in "$in_ref" \
      "${outs[@]}" \
      --change-address "$SENDER_ADDR" \
      "${NET_ARGS[@]}" \
      --out-file "$body" >/dev/null

    cardano-cli conway transaction sign \
      --tx-body-file "$body" \
      --signing-key-file "$SENDER_SKEY" \
      "${NET_ARGS[@]}" \
      --out-file "$signed" >/dev/null

    cardano-cli conway transaction submit \
      --tx-file "$signed" \
      "${NET_ARGS[@]}" >/dev/null

    txid=$(cardano-cli conway transaction txid --tx-file "$signed")
    echo "$txid" > "$txid_file"

    # Record outputs for next level as txid#0..#(bi-1)
    for (( k=0; k<bi; k++ )); do
      NEXT_INPUTS+=( "${txid}#${k}" )
    done

    # minimal progress
    if (( (t+1) % PROGRESS_EVERY == 0 )); then
      now=$(date +%s)
      elapsed=$(( now - lvl_start ))
      rate=$(awk -v n=$((t+1)) -v e=$elapsed 'BEGIN{ if(e<=0){print 0}else{printf "%.2f", n/e} }')
      remaining=$(( num_txs - (t+1) ))
      eta=$(awk -v r="$rate" -v rem="$remaining" 'BEGIN{ if(r<=0){print "?"} else {printf "%.0f", rem/r} }')
      echo "  progress: $((t+1))/$num_txs txs (elapsed ${elapsed}s, ~${rate} tx/s, ETA ${eta}s)"
    fi
  done

  # After finishing this level, wait for a few blocks so outputs become spendable
  if (( level < LEVELS-1 )) && (( WAIT_BLOCKS > 0 )); then
    start_block=$(current_block || echo 0)
    target_block=$(( start_block + WAIT_BLOCKS ))
    echo "Waiting for $WAIT_BLOCKS block(s) (from $start_block to >= $target_block)..."
    while true; do
      blk=$(current_block || echo 0)
      if (( blk >= target_block )); then break; fi
      sleep 2
    done
  fi

  CUR_INPUTS=( "${NEXT_INPUTS[@]}" )
done

end_ts=$(date +%s)
echo "Done. Total elapsed: $(( end_ts - start_ts ))s"
