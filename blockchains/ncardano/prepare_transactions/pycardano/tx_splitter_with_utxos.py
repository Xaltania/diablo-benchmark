#!/usr/bin/env python3
# tx_splitter.py
# Plan branching then split a funding UTxO across levels to produce many leaf UTxOs.
# Now requires explicit flags for paths and network.
# Also outputs UTxOs to JSON for easier access.

import argparse
import json
import math
import os
import random
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Semaphore
from typing import List, Tuple, Dict

# ---------- tunables (env overrides allowed) ----------
DEF_THREADS = int(os.getenv("THREADS", str(os.cpu_count() or 4)))
DEF_CAP = int(os.getenv("CAP", "200"))
DEF_LEAF = int(os.getenv("LEAF", "2000000"))
DEF_FEE_MARGIN = int(os.getenv("FEE_MARGIN", "2000000"))
DEF_CHANGE_MIN = int(os.getenv("CHANGE_MIN", "1200000"))
DEF_MIN_OUTPUT = int(os.getenv("MIN_OUTPUT", "1000000"))  # floor per produced output
DEF_MAX_OUTS = int(os.getenv("MAX_OUTS_PER_TX", str(DEF_CAP)))

# ---------- globals set from CLI ----------
TESTNET_MAGIC: int | None = None
SOCKET_PATH: str | None = None

# throttles (set in main)
BUILD_SEM: Semaphore | None = None
SUBMIT_SEM: Semaphore | None = None

# ---------- utilities ----------
def read_address_from_file(path_like: Path | str) -> str:
    """Read an address file containing either a bech32 string or JSON with 'address'."""
    text = Path(path_like).read_text().strip()
    if not text:
        return ""
    if text.startswith("{"):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                addr = data.get("address")
                if isinstance(addr, str) and addr:
                    return addr.strip()
        except Exception:
            pass
    return text

def sh(args, *, capture=True) -> str:
    env = os.environ.copy()
    if not SOCKET_PATH:
        raise RuntimeError("Socket path not set")
    env["CARDANO_NODE_SOCKET_PATH"] = SOCKET_PATH

    for i in range(6):
        try:
            p = subprocess.run(
                args,
                check=True,
                env=env,
                stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            return (p.stdout or "").strip()
        except subprocess.CalledProcessError as e:
            msg = (e.stderr or "").strip()
            low = msg.lower()
            transient = ("resource exhausted" in low) or ("temporarily unavailable" in low)
            if transient and i < 5:
                time.sleep(0.5 * (2 ** i))
                continue
            raise RuntimeError(f"cmd failed: {' '.join(args)}\n{msg}") from e

def cli(args, capture=True) -> str:
    if TESTNET_MAGIC is None:
        raise RuntimeError("Testnet magic not set")
    cmd = ["cardano-cli", "conway", *args]

    needs_magic = False
    if args:
        if args[0] == "query":
            needs_magic = True
        elif args[0] == "transaction" and len(args) > 1:
            if args[1] in ("build", "submit"):
                needs_magic = True
    if needs_magic:
        cmd += ["--testnet-magic", str(TESTNET_MAGIC)]
    return sh(cmd, capture=capture)

def rand_hex(n=8) -> str:
    return "".join(random.choices("0123456789abcdef", k=n))

def ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b

def ipow(base: int, exp: int) -> int:
    out = 1
    while exp > 0:
        if exp & 1:
            out *= base
        base *= base
        exp >>= 1
    return out

# ---------- planner ----------
def split_plan(N: int, CAP: int) -> Dict:
    if N <= 0:
        return dict(levels=0, branching_factors=[], txs_per_level=[], total_split_txs=0, final_outputs_produced=0)

    if N <= CAP:
        BFS = [N]
        TXS = [1]
        return dict(levels=1, branching_factors=BFS, txs_per_level=TXS, total_split_txs=sum(TXS), final_outputs_produced=math.prod(BFS))

    L = 1
    while ipow(CAP, L) < N:
        L += 1

    BFS = []
    Nrem = N
    for i in range(1, L + 1):
        remaining = L - i
        denom = ipow(CAP, remaining)
        bi = ceil_div(Nrem, denom)
        bi = max(1, min(bi, CAP))
        BFS.append(bi)
        Nrem = ceil_div(Nrem, bi)

    TXS = []
    running = 1
    for bi in BFS:
        TXS.append(running)
        running *= bi

    return dict(
        levels=L,
        branching_factors=BFS,
        txs_per_level=TXS,
        total_split_txs=sum(TXS),
        final_outputs_produced=math.prod(BFS),
    )

# ---------- chain helpers ----------
def query_utxos(addr: str) -> Dict[str, dict]:
    raw = cli(["query", "utxo", "--address", addr, "--output-json"])
    return json.loads(raw or "{}")

def utxo_keys(addr: str) -> List[str]:
    u = query_utxos(addr)
    return list(u.keys())

def utxo_lovelace(addr: str, txin: str) -> int:
    u = query_utxos(addr)
    if txin not in u:
        return 0
    val = u[txin].get("value", {})
    if isinstance(val, dict):
        return int(val.get("lovelace", 0))
    try:
        return int(u[txin].get("value", 0))
    except Exception:
        return 0

def wait_blocks(n: int = 2):
    def slot() -> int:
        tip = json.loads(cli(["query", "tip"]))
        return int(tip.get("slot", tip.get("slotNo", 0)))
    s0 = slot()
    while True:
        time.sleep(1)
        if slot() - s0 >= n:
            return

def wait_for_txin(addr: str, txin: str):
    while True:
        if txin in utxo_keys(addr):
            return
        time.sleep(0.5)

def wait_for_inputs(addr: str, needed_txins: List[str]):
    need = len(needed_txins)
    if need == 0:
        return
    needed = set(needed_txins)
    while True:
        have = set(utxo_keys(addr))
        if len(needed & have) >= need:
            return
        time.sleep(1)

def first_txin_at(addr: str) -> str:
    keys = utxo_keys(addr)
    return keys[0] if keys else ""

# ---------- splitting ----------
def build_sign_submit(txin: str, outs: List[Tuple[str, int]], change_addr: str, sign_key: str, base: Path):
    # build
    out_args = []
    for (pay_to, amt) in outs:
        out_args += ["--tx-out", f"{pay_to}+{amt}"]
    body = str(base.with_suffix(".txbody"))
    signed = str(base.with_suffix(".signed"))

    if BUILD_SEM is not None:
        with BUILD_SEM:
            cli(["transaction", "build",
                 "--tx-in", txin, *out_args,
                 "--change-address", change_addr,
                 "--out-file", body], capture=False)
    else:
        cli(["transaction", "build",
             "--tx-in", txin, *out_args,
             "--change-address", change_addr,
             "--out-file", body], capture=False)

    if not os.path.exists(body):
        raise RuntimeError(f"build produced no file: {body}")

    # sign (offline)
    cli(["transaction", "sign",
         "--signing-key-file", sign_key,
         "--tx-body-file", body,
         "--out-file", signed], capture=False)

    if not os.path.exists(signed):
        raise RuntimeError(f"sign produced no file: {signed}")

    # txid: prefer signed, fallback to body (both offline)
    try:
        txid = cli(["transaction", "txid", "--tx-file", signed])
    except RuntimeError:
        txid = cli(["transaction", "txid", "--tx-body-file", body])

    # submit with retry and throttle
    last_err = None
    for _ in range(10):
        try:
            if SUBMIT_SEM is not None:
                with SUBMIT_SEM:
                    cli(["transaction", "submit", "--tx-file", signed], capture=False)
            else:
                cli(["transaction", "submit", "--tx-file", signed], capture=False)
            break
        except Exception as e:
            last_err = e
            time.sleep(1)
    else:
        raise RuntimeError(f"submit failed for {txin}: {last_err}")

    # children are 0..k-1, change at k
    k = len(outs)
    return txid, k

def do_split_one(
    lvl:int,
    txin:str,
    b:int,
    pay_to:str,
    change_addr:str,
    sign_key:str,
    workdir:Path,
    current_addr:str,
) -> Path:
    """Equal-split only."""
    workdir.mkdir(parents=True, exist_ok=True)
    parts = workdir / "parts"
    parts.mkdir(exist_ok=True)
    part_file = parts / f"{rand_hex(8)}.next"
    part_file.write_text("")

    remain = b
    current_in = txin
    while remain > 0:
        k = min(remain, DEF_MAX_OUTS)

        # compute per-output amount from current input
        in_val = utxo_lovelace(current_addr, current_in)
        if in_val <= 0:
            raise RuntimeError(f"unable to read lovelace for {current_in} at {current_addr}")
        usable = in_val - DEF_FEE_MARGIN - DEF_CHANGE_MIN
        share = usable // k
        if share < DEF_MIN_OUTPUT:
            raise RuntimeError(
                f"input {current_in} value={in_val} too small for k={k} with "
                f"fee_margin={DEF_FEE_MARGIN}, change_min={DEF_CHANGE_MIN}, min_output={DEF_MIN_OUTPUT}"
            )

        base = workdir / rand_hex(8)
        outs = [(pay_to, share)] * k

        txid, change_index = build_sign_submit(current_in, outs, change_addr, sign_key, base)

        # record child outputs 0..k-1
        with part_file.open("a") as f:
            for j in range(k):
                f.write(f"{txid}#{j}\n")

        # next chunk spends change at index k
        next_in = f"{txid}#{change_index}"
        wait_for_txin(change_addr, next_in)
        current_in = next_in
        remain -= k

    return part_file

# ---------- main ----------
def main():
    global BUILD_SEM, SUBMIT_SEM
    global TESTNET_MAGIC, SOCKET_PATH

    ap = argparse.ArgumentParser(
        description="Plan branching and split a funding UTxO into many leaves. Equal-split only."
    )
    ap.add_argument("target", type=int, help="desired leaf UTxOs")
    ap.add_argument("-s", "--socket-path", type=str, required=True, help="path to CARDANO_NODE_SOCKET_PATH")
    ap.add_argument("-m", "--testnet-magic", type=int, required=True, help="testnet magic number")
    ap.add_argument("-i", "--input-address", type=Path, required=True, help="file containing funding address (.addr or JSON with 'address')")
    ap.add_argument("-o", "--output-address", type=Path, required=True, help="file containing final leaves address")
    ap.add_argument("-k", "--skey", type=Path, required=True, help="signing key file used to spend funding and change")
    ap.add_argument("--threads", type=int, default=DEF_THREADS, help="parallel splits per level")
    ap.add_argument("--utxo-out-file", type=Path, help="Optional: File to save the final list of created UTxOs")
    args = ap.parse_args()

    TESTNET_MAGIC = int(args.testnet_magic)
    SOCKET_PATH = args.socket_path

    # fixed throttles
    BUILD_SEM = Semaphore(6)
    SUBMIT_SEM = Semaphore(12)

    # prerequisites
    for f in [args.input_address, args.output_address, args.skey]:
        if not Path(f).exists():
            print(f"missing: {f}", file=sys.stderr)
            sys.exit(1)
    # Only check socket path if it's a local file (not a remote path)
    if "/" not in SOCKET_PATH or SOCKET_PATH.startswith("/"):
        if not Path(SOCKET_PATH).exists():
            print(f"missing socket: {SOCKET_PATH}", file=sys.stderr)
            sys.exit(1)

    input_addr = read_address_from_file(args.input_address)
    output_addr = read_address_from_file(args.output_address)
    sign_key = str(args.skey)

    plan = split_plan(args.target, DEF_CAP)
    BFS = plan["branching_factors"]
    TXS = plan["txs_per_level"]
    L = plan["levels"]
    if L == 0:
        print("Nothing to do")
        return

    print("Branching factors:", " ".join(map(str, BFS)))
    print("Txs per level:", " ".join(map(str, TXS)))
    print("Total split txs:", plan["total_split_txs"])

    workroot = Path("fanout_work")
    workroot.mkdir(exist_ok=True)

    g0 = first_txin_at(input_addr)
    if not g0:
        print(f"no UTxO found at {input_addr}", file=sys.stderr)
        sys.exit(1)

    (workroot / "inputs_l0.txt").write_text(g0 + "\n")
    overall_start = time.time()

    for lvl in range(L):
        b = BFS[lvl]
        in_file = workroot / f"inputs_l{lvl}.txt"
        next_file = workroot / f"inputs_l{lvl+1}.txt"
        lvl_dir = workroot / f"l{lvl}"
        parts_dir = lvl_dir / "parts"
        if next_file.exists():
            next_file.unlink()
        if lvl_dir.exists():
            for p in parts_dir.glob("*.next"):
                p.unlink()
        lvl_dir.mkdir(parents=True, exist_ok=True)
        parts_dir.mkdir(parents=True, exist_ok=True)

        if lvl < L - 1:
            pay_to, change_to, next_addr, current_addr = input_addr, input_addr, input_addr, input_addr
        else:
            pay_to, change_to, next_addr, current_addr = output_addr, input_addr, output_addr, input_addr

        if lvl > 0:
            need = in_file.read_text().splitlines()
            wait_for_inputs(input_addr, need)

        cur_inputs = [x for x in in_file.read_text().splitlines() if x.strip()]
        n = len(cur_inputs)
        print(f"Level {lvl}: b={b}, amt=equal, txs={n}, threads={args.threads}")
        t0 = time.time()

        part_paths: List[Path] = []
        if n > 0:
            with ThreadPoolExecutor(max_workers=args.threads) as ex:
                futs = [
                    ex.submit(
                        do_split_one,
                        lvl, txin, b, pay_to, change_to, sign_key, lvl_dir, current_addr
                    )
                    for txin in cur_inputs
                ]
                for fu in as_completed(futs):
                    part_paths.append(fu.result())

        with next_file.open("w") as out:
            for p in parts_dir.glob("*.next"):
                out.write(p.read_text())

        next_list = next_file.read_text().splitlines()
        wait_for_inputs(next_addr, next_list)

        print(f"Level {lvl} done in {int(time.time()-t0)}s")
        wait_blocks(2)

    print(f"All done in {int(time.time()-overall_start)}s")

    # Write final UTxOs to JSON file if requested
    if args.utxo_out_file:
        # Get the final UTxOs from the last level
        final_inputs_file = workroot / f"inputs_l{L}.txt"
        if final_inputs_file.exists():
            final_txins = [line.strip() for line in final_inputs_file.read_text().splitlines() if line.strip()]
            final_utxos = []
            
            for txin in final_txins:
                # Parse txin format: "txid#index"
                if "#" in txin:
                    tx_id, index = txin.split("#", 1)
                    amount = utxo_lovelace(output_addr, txin)
                    if amount > 0:
                        final_utxos.append({
                            "tx_id": tx_id,
                            "index": int(index),
                            "amount": amount,
                        })
            
            with open(args.utxo_out_file, "w") as f:
                json.dump(final_utxos, f, indent=2)
            print(f"Wrote final UTxOs to {args.utxo_out_file}")

if __name__ == "__main__":
    main()
