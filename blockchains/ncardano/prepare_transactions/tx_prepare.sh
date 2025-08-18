#!/usr/bin/env bash
# prepare_owner_txs.sh
# Build & sign (NO submit) one Conway tx per UTxO at owner address,
# paying back to the same address. Outputs ONLY signed txs in prepared_transactions/.

set -euo pipefail

# --- config ---
: "${MAGIC:=42}"
: "${STATE_DIR:=$HOME/cardano/cardano-node-tests/dev_workdir/state-cluster0}"
: "${THREADS:=8}"        # safe default; raise if stable
: "${RETRIES:=10}"       # backoff attempts for "resource exhausted"
: "${OUTDIR:=prepared_transactions}"

OWNER_ADDR_FILE="$STATE_DIR/nodes/node-pool1/owner.addr"
OWNER_SKEY="$STATE_DIR/nodes/node-pool1/owner-utxo.skey"
: "${CARDANO_NODE_SOCKET_PATH:=$STATE_DIR/bft1.socket}"

command -v cardano-cli >/dev/null || { echo "cardano-cli not on PATH" >&2; exit 1; }
[[ -f "$OWNER_ADDR_FILE" ]] || { echo "missing: $OWNER_ADDR_FILE" >&2; exit 1; }
[[ -f "$OWNER_SKEY"      ]] || { echo "missing: $OWNER_SKEY" >&2; exit 1; }
[[ -S "$CARDANO_NODE_SOCKET_PATH" ]] || { echo "missing socket: $CARDANO_NODE_SOCKET_PATH" >&2; exit 1; }

OWNER_ADDR="$(<"$OWNER_ADDR_FILE")"
mkdir -p "$OUTDIR"
TMPDIR="$(mktemp -d)"; trap 'rm -rf "$TMPDIR"' EXIT
: > "$TMPDIR/done.count"
: > "$TMPDIR/failed.txins"

# list owner UTxOs (txhash#txix)
cardano-cli conway query utxo --address "$OWNER_ADDR" --testnet-magic "$MAGIC" \
| awk 'NR>2{print $1"#"$2}' > "$TMPDIR/utxos.txt"

TOTAL=$(wc -l < "$TMPDIR/utxos.txt")
[[ "$TOTAL" -gt 0 ]] || { echo "No UTxOs at owner address."; exit 0; }
echo "UTxOs: $TOTAL | threads: $THREADS | out: $OUTDIR"

build_one() { # idx txin
  set -euo pipefail
  idx="$1"; txin="$2"
  body="$(mktemp "$TMPDIR/body.XXXXXX")"
  signed="$OUTDIR/$(printf '%06d' "$idx").tx"
  errf="$TMPDIR/err.$idx"

  success=0
  for attempt in $(seq 1 "$RETRIES"); do
    if cardano-cli conway transaction build \
         --tx-in "$txin" \
         --change-address "$OWNER_ADDR" \
         --testnet-magic "$MAGIC" \
         --out-file "$body" >/dev/null 2>"$errf"; then
      success=1
      break
    fi
    if grep -qi "resource exhausted" "$errf"; then
      sleep "$attempt" # linear backoff: 1,2,3,...
      continue
    else
      break
    fi
  done

  if (( success == 0 )); then
    echo "$txin" >> "$TMPDIR/failed.txins"
    rm -f "$body"
    echo 1 >> "$TMPDIR/done.count"
    return 0
  fi

  cardano-cli conway transaction sign \
    --signing-key-file "$OWNER_SKEY" \
    --testnet-magic "$MAGIC" \
    --tx-body-file "$body" \
    --out-file "$signed" >/dev/null

  rm -f "$body" "$errf"
  echo 1 >> "$TMPDIR/done.count"
}
export OWNER_ADDR OWNER_SKEY MAGIC OUTDIR TMPDIR RETRIES
export -f build_one

progress() {
  local start now done elapsed rate left eta
  start=$(date +%s)
  while :; do
    sleep 2
    now=$(date +%s)
    done=$(wc -l < "$TMPDIR/done.count" 2>/dev/null || echo 0)
    (( done > TOTAL )) && done="$TOTAL"
    elapsed=$((now-start))
    if (( done > 0 && elapsed > 0 )); then
      rate=$(( done / (elapsed>0?elapsed:1) ))
      left=$(( TOTAL - done ))
      eta=$(( rate>0 ? left / rate : 0 ))
      echo "progress: $done/$TOTAL | elapsed: ${elapsed}s | eta: ~${eta}s"
    else
      echo "progress: 0/$TOTAL | elapsed: ${elapsed}s"
    fi
    (( done >= TOTAL )) && break
  done
}
progress & PROG_PID=$!

# index lines, run in parallel
nl -w1 -s' ' "$TMPDIR/utxos.txt" \
| xargs -P "$THREADS" -n 2 bash -c 'build_one "$0" "$1"'

wait "$PROG_PID" || true

if [[ -s "$TMPDIR/failed.txins" ]]; then
  echo "Done with some failures. See: $TMPDIR/failed.txins"
else
  echo "Done. Signed txs in $OUTDIR/"
fi
