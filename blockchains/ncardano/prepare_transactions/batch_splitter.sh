#!/usr/bin/env bash
# batch_splitter.sh
# Split a genesis UTxO across levels. Pays back to genesis at all levels except the last,
# which pays to pool1 owner. Signs with genesis throughout. Chunks outputs to avoid oversize txs.

set -euo pipefail

# ---------- config ----------
: "${MAGIC:=42}"
: "${CAP:=300}"                 # planning cap (max outputs/tx)
: "${LEAF:=2000000}"            # lovelace per final leaf UTxO (≥ minUTxO)
: "${FEE_MARGIN:=2000000}"      # fee buffer per parent tx
: "${CHANGE_MIN:=1200000}"      # min change to avoid dust
: "${THREADS:=$(nproc)}"        # parallelism per level
: "${STATE_DIR:=$HOME/cardano/cardano-node-tests/dev_workdir/state-cluster0}"
: "${MAX_OUTS_PER_TX:=$CAP}"    # per-tx outputs (keep ≤ CAP; reduce if size errors)

# Addresses and keys
GENESIS_ADDR_FILE="$STATE_DIR/shelley/genesis-utxo.addr"
GENESIS_SKEY="$STATE_DIR/shelley/genesis-utxo.skey"
OWNER_ADDR_FILE="$STATE_DIR/nodes/node-pool1/owner.addr"
OWNER_SKEY="$STATE_DIR/nodes/node-pool1/owner-utxo.skey"   # not used for signing here

# Socket (set if not already exported)
: "${CARDANO_NODE_SOCKET_PATH:=$STATE_DIR/bft1.socket}"

# ---------- args ----------
usage(){ echo "usage: $0 <desired_leaves> [cap=${CAP}]"; }
[[ $# -ge 1 ]] || { usage >&2; exit 2; }
TARGET="$1"
CAP="${2:-$CAP}"

# ---------- prerequisites ----------
for f in "$GENESIS_ADDR_FILE" "$GENESIS_SKEY" "$OWNER_ADDR_FILE"; do
  [[ -f "$f" ]] || { echo "missing: $f" >&2; exit 1; }
done
[[ -S "$CARDANO_NODE_SOCKET_PATH" ]] || { echo "missing socket: $CARDANO_NODE_SOCKET_PATH" >&2; exit 1; }

GENESIS_ADDR="$(<"$GENESIS_ADDR_FILE")"
OWNER_ADDR="$(<"$OWNER_ADDR_FILE")"

command -v cardano-cli >/dev/null || { echo "cardano-cli not on PATH" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq not on PATH" >&2; exit 1; }

# ---------- plan library ----------
[[ -f ./tx_split_plan.sh ]] || { echo "missing ./tx_split_plan.sh" >&2; exit 1; }
# shellcheck source=/dev/null
source ./tx_split_plan.sh

# Plan for desired leaves
split_plan "$TARGET" "$CAP"
: "${LEVELS:?}" "${TOTAL_SPLIT_TXS:?}"

# ---------- per-level amounts (AMTS[]) ----------
declare -ag AMTS=()
if (( LEVELS > 0 )); then
  AMTS[$((LEVELS-1))]="$LEAF"
  for ((i=LEVELS-2; i>=0; i--)); do
    AMTS[$i]=$(( BFS[i+1] * AMTS[i+1] + FEE_MARGIN + CHANGE_MIN ))
  done
fi
for ((i=0;i<LEVELS;i++)); do
  [[ -v "BFS[$i]" && -v "AMTS[$i]" ]] || { echo "planner/amounts not set" >&2; exit 1; }
done

# ---------- helpers ----------
wait_blocks(){ # n_blocks
  local n="${1:-2}" s c
  s=$(cardano-cli conway query tip --testnet-magic "$MAGIC" | jq -r '.slot // .slotNo // 0')
  while true; do
    sleep 1
    c=$(cardano-cli conway query tip --testnet-magic "$MAGIC" | jq -r '.slot // .slotNo // 0')
    (( c - s >= n )) && break
  done
}

wait_for_inputs(){ # addr file_with_txins
  local addr="$1" file="$2" need have
  need=$(wc -l <"$file")
  [[ "$need" -eq 0 ]] && return 0
  while true; do
    have=$(
      cardano-cli conway query utxo --address "$addr" --testnet-magic "$MAGIC" \
      | awk 'NR>2{print $1"#"$2}' | grep -F -f "$file" | wc -l || true
    )
    (( have >= need )) && break
    sleep 1
  done
}

wait_for_txin(){ # addr txid#ix
  local addr="$1" txin="$2"
  while true; do
    cardano-cli conway query utxo --address "$addr" --testnet-magic "$MAGIC" \
      | awk 'NR>2{print $1"#"$2}' | grep -qx "$txin" && return 0
    sleep 0.5
  done
}

first_genesis_txin(){
  cardano-cli conway query utxo --address "$GENESIS_ADDR" --testnet-magic "$MAGIC" \
  | awk 'NR==3{print $1"#"$2}'
}

# Build + sign + submit a split from one tx-in, possibly in multiple chunks.
# Records child outputs as "txid#index" lines in a .next file for the next level. Thanks ChatGPT
do_split_one(){ # lvl txin b amt pay_to change_addr sign_key
  local lvl="$1" txin="$2" b="$3" amt="$4" pay="$5" change="$6" skey="$7"
  local work="fanout_work/l$lvl"; mkdir -p "$work/parts"
  local part="$work/parts/$(tr -dc a-f0-9 </dev/urandom | head -c 8).next"; : > "$part"

  local remain="$b"
  while (( remain > 0 )); do
    local k=$(( remain > MAX_OUTS_PER_TX ? MAX_OUTS_PER_TX : remain ))
    local base="$work/$(tr -dc a-f0-9 </dev/urandom | head -c 8)"
    local -a OUTS=()
    for ((j=0;j<k;j++)); do OUTS+=( --tx-out "$pay+$amt" ); done

    cardano-cli conway transaction build \
      --tx-in "$txin" "${OUTS[@]}" --change-address "$change" \
      --testnet-magic "$MAGIC" --out-file "$base.txbody" >/dev/null

    local txid; txid=$(cardano-cli conway transaction txid --tx-body-file "$base.txbody")

    cardano-cli conway transaction sign \
      --signing-key-file "$skey" --testnet-magic "$MAGIC" \
      --tx-body-file "$base.txbody" --out-file "$base.signed" >/dev/null

    local ok=0
    for _ in {1..10}; do
      if cardano-cli conway transaction submit --tx-file "$base.signed" --testnet-magic "$MAGIC" >/dev/null 2>"$base.err"; then
        ok=1; break
      fi
      sleep 1
    done
    (( ok == 1 )) || { echo "submit failed (lvl=$lvl, txin=$txin)"; sed 's/^/  /' "$base.err" >&2 || true; exit 1; }

    # Children from this chunk are indices [0..k-1]
    for ((j=0;j<k;j++)); do echo "$txid#$j" >> "$part"; done

    # Next chunk spends the change output at index k -> wait until it exists
    local next_in="$txid#$k"
    wait_for_txin "$change" "$next_in"
    txin="$next_in"
    remain=$(( remain - k ))
  done
}
export MAGIC CAP LEAF FEE_MARGIN CHANGE_MIN THREADS STATE_DIR
export MAX_OUTS_PER_TX
export GENESIS_ADDR GENESIS_SKEY OWNER_ADDR OWNER_SKEY
export -f do_split_one wait_for_txin

# ---------- Tele ----------
echo "Branching factors: ${BFS[*]}"
echo "Txs per level: ${TXS_PER_LEVEL[*]}"
echo "Total split txs: $TOTAL_SPLIT_TXS"

# ---------- bootstrap ----------
mkdir -p fanout_work
g0="$(first_genesis_txin)"
[[ -n "$g0" ]] || { echo "no genesis UTxO found at $GENESIS_ADDR" >&2; exit 1; }
printf '%s\n' "$g0" > fanout_work/inputs_l0.txt

overall_start=$(date +%s)

# ---------- main levels ----------
for ((lvl=0; lvl<LEVELS; lvl++)); do
  b="${BFS[lvl]}"
  amt="${AMTS[lvl]}"
  in_file="fanout_work/inputs_l${lvl}.txt"
  next_file="fanout_work/inputs_l$((lvl+1)).txt"
  rm -f "$next_file"
  rm -rf "fanout_work/l$lvl"
  mkdir -p "fanout_work/l$lvl/parts"

  # Routing: keep funds at genesis until final level; final pays to owner.
  if (( lvl < LEVELS - 1 )); then
    pay_to="$GENESIS_ADDR"; change_to="$GENESIS_ADDR"; sign_with="$GENESIS_SKEY"; next_addr="$GENESIS_ADDR"
  else
    pay_to="$OWNER_ADDR";  change_to="$GENESIS_ADDR";  sign_with="$GENESIS_SKEY"; next_addr="$OWNER_ADDR"
  fi

  # Ensure inputs exist at the address that holds them
  if (( lvl > 0 )); then
    wait_for_inputs "$GENESIS_ADDR" "$in_file"
  fi

  mapfile -t CUR_INPUTS < "$in_file"
  n=${#CUR_INPUTS[@]}
  echo "Level $lvl: b=$b, amt=$amt, txs=$n, threads=$THREADS"
  lvl_start=$(date +%s)

  if (( n > 0 )); then
    printf '%s\n' "${CUR_INPUTS[@]}" \
    | xargs -I{} -P "$THREADS" bash -c \
      'set -euo pipefail; do_split_one "$1" "$2" "$3" "$4" "$5" "$6" "$7"' _ \
      "$lvl" {} "$b" "$amt" "$pay_to" "$change_to" "$sign_with"
  fi

  if compgen -G "fanout_work/l$lvl/parts/"'*.next' > /dev/null; then
    cat fanout_work/l"$lvl"/parts/*.next > "$next_file"
    wait_for_inputs "$next_addr" "$next_file"
  else
    : > "$next_file"
  fi

  echo "Level $lvl done in $(( $(date +%s) - lvl_start ))s"
  wait_blocks 2
done

echo "All done in $(( $(date +%s) - overall_start ))s"
