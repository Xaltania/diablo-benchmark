#!/usr/bin/env python3
# tx_splitter.py
# Plan branching then split a funding UTxO across levels to produce many leaf UTxOs.

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

# ---------- defaults ----------
DEF_MAGIC = int(os.getenv("MAGIC", "42"))
DEF_STATE = Path(os.getenv("STATE_DIR", str(Path.home() / "cardano/cardano-node-tests/dev_workdir/state-cluster0")))
DEF_THREADS = int(os.getenv("THREADS", str(os.cpu_count() or 4)))
DEF_CAP = int(os.getenv("CAP", "200"))
DEF_LEAF = int(os.getenv("LEAF", "2000000"))
DEF_FEE_MARGIN = int(os.getenv("FEE_MARGIN", "2000000"))
DEF_CHANGE_MIN = int(os.getenv("CHANGE_MIN", "1200000"))
DEF_MAX_OUTS = int(os.getenv("MAX_OUTS_PER_TX", str(DEF_CAP)))
DEF_SOCKET = os.getenv("CARDANO_NODE_SOCKET_PATH", str(DEF_STATE / "bft1.socket"))

GENESIS_ADDR_FILE = DEF_STATE / "shelley/genesis-utxo.addr"
GENESIS_SKEY_FILE = DEF_STATE / "shelley/genesis-utxo.skey"
OWNER_ADDR_FILE   = DEF_STATE / "nodes/node-pool1/owner.addr"
OWNER_SKEY_FILE   = DEF_STATE / "nodes/node-pool1/owner-utxo.skey"  # not used to sign here

# throttles (set in main)
BUILD_SEM: Semaphore | None = None
SUBMIT_SEM: Semaphore | None = None

# ---------- utilities ----------
def sh(args, *, capture=True) -> str:
    env = os.environ.copy()
    env["CARDANO_NODE_SOCKET_PATH"] = DEF_SOCKET
    # retry on transient socket pressure
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
    cmd = ["cardano-cli", "conway", *args]
    # Network flag only where accepted
    needs_magic = False
    if args:
        if args[0] == "query":
            needs_magic = True
        elif args[0] == "transaction" and len(args) > 1:
            if args[1] in ("build", "submit"):  # txid/sign do not need magic
                needs_magic = True
    if needs_magic:
        cmd += ["--testnet-magic", str(DEF_MAGIC)]
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

# ---------- planner (Python port of tx_split_plan.sh) ----------
def split_plan(N: int, CAP: int) -> Dict:
    if N <= 0:
        return dict(levels=0, branching_factors=[], txs_per_level=[], total_split_txs=0, final_outputs_produced=0)

    if N <= CAP:
        BFS = [N]
        TXS = [1]
        return dict(levels=1, branching_factors=BFS, txs_per_level=TXS, total_split_txs=sum(TXS), final_outputs_produced=math.prod(BFS))

    # minimal levels
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

def per_level_amounts(BFS: List[int], leaf:int, fee_margin:int, change_min:int) -> List[int]:
    if not BFS:
        return []
    L = len(BFS)
    AMTS = [0] * L
    AMTS[L-1] = leaf
    for i in range(L-2, -1, -1):
        AMTS[i] = BFS[i+1] * AMTS[i+1] + fee_margin + change_min
    return AMTS

# ---------- chain helpers ----------
def query_utxos(addr: str) -> Dict[str, dict]:
    raw = cli(["query", "utxo", "--address", addr, "--output-json"])
    return json.loads(raw or "{}")

def utxo_keys(addr: str) -> List[str]:
    u = query_utxos(addr)
    return list(u.keys())

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

def first_genesis_txin(gen_addr: str) -> str:
    keys = utxo_keys(gen_addr)
    return keys[0] if keys else ""

# ---------- splitting ----------
def build_sign_submit(txin: str, outs: List[Tuple[str, int]], change_addr: str, sign_key: str, base: Path):
    # build
    out_args = []
    for (pay_to, amt) in outs:
        out_args += ["--tx-out", f"{pay_to}+{amt}"]
    body = str(base.with_suffix(".txbody"))
    signed = str(base.with_suffix(".signed"))

    # throttle BUILD calls (socket use)
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

    # submit with retry and throttle (socket use)
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

def do_split_one(lvl:int, txin:str, b:int, amt:int, pay_to:str, change_addr:str, sign_key:str, max_outs:int, workdir:Path) -> Path:
    workdir.mkdir(parents=True, exist_ok=True)
    parts = workdir / "parts"
    parts.mkdir(exist_ok=True)
    part_file = parts / f"{rand_hex(8)}.next"
    part_file.write_text("")  # truncate

    remain = b
    current_in = txin
    while remain > 0:
        k = min(remain, max_outs)
        base = workdir / rand_hex(8)
        outs = [(pay_to, amt)] * k

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

def main():
    global DEF_MAGIC, DEF_SOCKET, BUILD_SEM, SUBMIT_SEM  # set from args

    ap = argparse.ArgumentParser(description="Plan branching and split a funding UTxO into many leaves.")
    ap.add_argument("target", type=int, help="desired leaf UTxOs")
    ap.add_argument("--cap", type=int, default=DEF_CAP, help="max per-tx outputs used in planning")
    ap.add_argument("--leaf", type=int, default=DEF_LEAF, help="lovelace per final leaf UTxO")
    ap.add_argument("--fee-margin", type=int, default=DEF_FEE_MARGIN, help="fee buffer per parent tx")
    ap.add_argument("--change-min", type=int, default=DEF_CHANGE_MIN, help="minimum change to avoid dust")
    ap.add_argument("--threads", type=int, default=DEF_THREADS, help="parallel splits per level")
    ap.add_argument("--max-outs-per-tx", type=int, default=DEF_MAX_OUTS, help="outputs per tx during fan-out")
    ap.add_argument("--build-threads", type=int, default=6, help="concurrent transaction builds (socket users)")
    ap.add_argument("--submit-threads", type=int, default=12, help="concurrent transaction submits (socket users)")
    ap.add_argument("--state-dir", type=Path, default=DEF_STATE, help="cluster state dir")
    ap.add_argument("--socket", type=Path, default=Path(DEF_SOCKET), help="node socket path")
    ap.add_argument("--magic", type=int, default=DEF_MAGIC, help="testnet magic")
    args = ap.parse_args()

    # bind globals from args
    DEF_MAGIC = args.magic
    DEF_SOCKET = str(args.socket)
    BUILD_SEM = Semaphore(max(1, args.build_threads))
    SUBMIT_SEM = Semaphore(max(1, args.submit_threads))

    # prerequisites
    for f in [GENESIS_ADDR_FILE, GENESIS_SKEY_FILE, OWNER_ADDR_FILE]:
        if not f.exists():
            print(f"missing: {f}", file=sys.stderr)
            sys.exit(1)
    if not Path(DEF_SOCKET).exists():
        print(f"missing socket: {DEF_SOCKET}", file=sys.stderr)
        sys.exit(1)
    genesis_addr = GENESIS_ADDR_FILE.read_text().strip()
    owner_addr = OWNER_ADDR_FILE.read_text().strip()

    plan = split_plan(args.target, args.cap)
    BFS = plan["branching_factors"]
    TXS = plan["txs_per_level"]
    L = plan["levels"]
    if L == 0:
        print("Nothing to do")
        return

    AMTS = per_level_amounts(BFS, args.leaf, args.fee_margin, args.change_min)

    print("Branching factors:", " ".join(map(str, BFS)))
    print("Txs per level:", " ".join(map(str, TXS)))
    print("Total split txs:", plan["total_split_txs"])

    workroot = Path("fanout_work")
    workroot.mkdir(exist_ok=True)

    g0 = first_genesis_txin(genesis_addr)
    if not g0:
        print(f"no genesis UTxO found at {genesis_addr}", file=sys.stderr)
        sys.exit(1)

    (workroot / "inputs_l0.txt").write_text(g0 + "\n")
    overall_start = time.time()

    for lvl in range(L):
        b = BFS[lvl]
        amt = AMTS[lvl]
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
            pay_to, change_to, sign_with, next_addr = genesis_addr, genesis_addr, str(GENESIS_SKEY_FILE), genesis_addr
        else:
            pay_to, change_to, sign_with, next_addr = owner_addr, genesis_addr, str(GENESIS_SKEY_FILE), owner_addr

        if lvl > 0:
            need = in_file.read_text().splitlines()
            wait_for_inputs(genesis_addr, need)

        cur_inputs = [x for x in in_file.read_text().splitlines() if x.strip()]
        n = len(cur_inputs)
        print(f"Level {lvl}: b={b}, amt={amt}, txs={n}, threads={args.threads}")
        t0 = time.time()

        part_paths: List[Path] = []
        if n > 0:
            with ThreadPoolExecutor(max_workers=args.threads) as ex:
                futs = [
                    ex.submit(
                        do_split_one,
                        lvl, txin, b, amt, pay_to, change_to, sign_with, args.max_outs_per_tx, lvl_dir
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

if __name__ == "__main__":
    main()
