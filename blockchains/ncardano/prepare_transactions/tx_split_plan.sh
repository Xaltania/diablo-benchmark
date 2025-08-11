#!/usr/bin/env bash
# tx_split_plan.sh - Bash library to plan balanced UTxO split levels
# Usage:
#   source tx_split_plan.sh
#   split_plan <TARGET_UTXOS> [CAP]
# After calling split_plan, the following are populated:
#   LEVELS                # minimal number of levels
#   BFS                   # array of branching factors per level
#   TXS_PER_LEVEL         # array: number of split txs to build at each level
#   TOTAL_SPLIT_TXS       # sum(TXS_PER_LEVEL)
#   FINAL_OUTPUTS         # product(BFS)
# Helper:
#   split_plan_print_json # pretty-prints the plan as JSON to stdout

# ---
# Taken from https://stackoverflow.com/questions/2394988/get-ceiling-integer-from-number-in-linux-bash
ceil_div() {
  local a="$1" b="$2"
  echo $(( (a + b - 1) / b ))
}
# ---

ipow() {
  local base="$1" exp="$2" result=1
  while (( exp > 0 )); do
    if (( exp & 1 )); then
      result=$(( result * base ))
    fi
    base=$(( base * base ))
    exp=$(( exp >> 1 ))
  done
  echo "$result"
}

iprod() {
  local p=1 x
  for x in "$@"; do
    p=$(( p * x ))
  done
  echo "$p"
}

isum() {
  local s=0 x
  for x in "$@"; do
    s=$(( s + x ))
  done
  echo "$s"
}

split_plan() {
  local N="$1"
  local CAP="${2:-300}"
  if [[ -z "$N" ]]; then
    echo "split_plan: missing N" >&2
    return 2
  fi
  if (( N <= 0 )); then
    LEVELS=0
    BFS=()
    TXS_PER_LEVEL=()
    TOTAL_SPLIT_TXS=0
    FINAL_OUTPUTS=0
    return 0
  fi

  if (( N <= CAP )); then
    LEVELS=1
    BFS=("$N")
    TXS_PER_LEVEL=(1)
    TOTAL_SPLIT_TXS=1
    FINAL_OUTPUTS="$N"
    return 0
  fi

  local L=1
  while (( $(ipow "$CAP" "$L") < N )); do
    L=$(( L + 1 ))
  done
  LEVELS="$L"

  BFS=()
  local Nrem="$N"
  local i remaining denom bi
  for (( i=1; i<=L; i++ )); do
    remaining=$(( L - i ))
    denom=$(ipow "$CAP" "$remaining")
    bi=$(ceil_div "$Nrem" "$denom")
    if (( bi < 1 )); then bi=1; fi
    if (( bi > CAP )); then bi="$CAP"; fi
    BFS+=("$bi")
    Nrem=$(ceil_div "$Nrem" "$bi")
  done

  TXS_PER_LEVEL=()
  local running=1
  for bi in "${BFS[@]}"; do
    TXS_PER_LEVEL+=("$running")
    running=$(( running * bi ))
  done

  TOTAL_SPLIT_TXS=$(isum "${TXS_PER_LEVEL[@]}")
  FINAL_OUTPUTS=$(iprod "${BFS[@]}")
}

split_plan_print_json() {
  local i
  printf '{\n'
  printf '  "levels": %d,\n' "${LEVELS:-0}"
  printf '  "branching_factors": ['
  for (( i=0; i<${#BFS[@]}; i++ )); do
    printf '%s%d' "$([[ $i -gt 0 ]] && echo ', ')" "${BFS[i]}"
  done
  printf '],\n'
  printf '  "txs_per_level": ['
  for (( i=0; i<${#TXS_PER_LEVEL[@]}; i++ )); do
    printf '%s%d' "$([[ $i -gt 0 ]] && echo ', ')" "${TXS_PER_LEVEL[i]}"
  done
  printf '],\n'
  printf '  "total_split_txs": %d,\n' "${TOTAL_SPLIT_TXS:-0}"
  printf '  "final_outputs_produced": %d\n' "${FINAL_OUTPUTS:-0}"
  printf '}\n'
}
