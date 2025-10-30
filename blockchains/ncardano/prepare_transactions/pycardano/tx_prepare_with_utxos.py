#!/usr/bin/env python3
# build_owner_txs.py
# Build and sign one transaction per UTxO at the input address.
# Inputs are sent to the output address (can be the same as input).
# Produces JSON files that `cardano-cli conway transaction submit` accepts.
# Also outputs UTxOs to JSON for easier access.

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from pycardano import (
    Transaction,
    TransactionBody,
    TransactionInput,
    TransactionOutput,
    TransactionWitnessSet,
    VerificationKeyWitness,
    PaymentSigningKey,
    PaymentVerificationKey,
)

# ---------- globals from CLI ----------
TESTNET_MAGIC: int | None = None
SOCKET_PATH: str | None = None
FEE: int = 200_000
TTL_DELTA: int = 0
OUTDIR: Path = Path("./prepared_transactions")


# ---------- helpers ----------
def read_address_from_file(path_like: Path | str) -> str:
    """Read an address file containing either a bech32 string or JSON with 'address'."""
    text = Path(path_like).read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"empty address file: {path_like}")
    if text.startswith("{"):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                for key in ("address", "addr", "bech32", "bech32Address"):
                    val = data.get(key)
                    if isinstance(val, str) and val.strip():
                        return val.strip()
        except json.JSONDecodeError:
            pass
        raise ValueError(f"address JSON missing a usable field: {path_like}")
    return text


def cli(args: list[str], *, capture: bool = True) -> str:
    """Run cardano-cli conway ... with testnet magic and socket env."""
    if TESTNET_MAGIC is None or not SOCKET_PATH:
        raise RuntimeError("network flags not initialised")
    env = os.environ.copy()
    env["CARDANO_NODE_SOCKET_PATH"] = SOCKET_PATH
    cmd = ["cardano-cli", "conway", *args, "--testnet-magic", str(TESTNET_MAGIC)]
    p = subprocess.run(
        cmd,
        check=False,
        env=env,
        stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        err = (p.stderr or "").strip()
        raise RuntimeError(f"cardano-cli failed: {' '.join(cmd)}\n{err}")
    return (p.stdout or "").strip()


def get_tip_slot() -> int:
    tip = json.loads(cli(["query", "tip"]))
    return int(tip.get("slot", tip.get("slotNo", 0)))


def list_utxos(addr: str) -> list[tuple[str, int, int]]:
    """Return list of (txhash, index, lovelace)."""
    j = json.loads(cli(["query", "utxo", "--address", addr, "--output-json"]))
    out = []
    for k, v in j.items():
        if "#" not in k:
            continue
        txh, ix = k.split("#", 1)
        amt = int(v.get("value", {}).get("lovelace", 0))
        out.append((txh, int(ix), amt))
    out.sort(key=lambda t: (t[0], t[1]))
    return out


def ensure_outdir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


# ---------- main ----------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build and sign one transaction per UTxO at the input address."
    )
    ap.add_argument("-s", "--socket-path", required=True, help="Path to node socket")
    ap.add_argument("-m", "--testnet-magic", required=True, type=int, help="Testnet magic number")
    ap.add_argument("-i", "--input-address", required=True, type=Path, help="File containing input address")
    ap.add_argument("-o", "--output-address", required=True, type=Path, help="File containing output address")
    ap.add_argument("-k", "--skey", required=True, type=Path, help="Payment signing key file")
    ap.add_argument("--fee", type=int, default=200_000, help="Fixed fee per tx in lovelace")
    ap.add_argument("--ttl-delta", type=int, default=0, help="Slots to add to current tip for ttl. 0 = omit ttl")
    ap.add_argument("--outdir", type=Path, default=Path("./prepared_transactions"), help="Directory to write tx JSONs")
    ap.add_argument("--utxo-out-file", type=Path, help="Optional: File to save the final list of created UTxOs")
    return ap.parse_args()


def main() -> int:
    global TESTNET_MAGIC, SOCKET_PATH, FEE, TTL_DELTA, OUTDIR

    args = parse_args()
    TESTNET_MAGIC = int(args.testnet_magic)
    SOCKET_PATH = str(args.socket_path)
    FEE = int(args.fee)
    TTL_DELTA = int(args.ttl_delta)
    OUTDIR = args.outdir

    # validate files
    for p in (args.input_address, args.output_address, args.skey):
        if not Path(p).exists():
            print(f"missing file: {p}", file=sys.stderr)
            return 1
    # Only check socket path if it's a local file (not a remote path)
    if "/" not in SOCKET_PATH or SOCKET_PATH.startswith("/"):
        if not Path(SOCKET_PATH).exists():
            print(f"missing socket: {SOCKET_PATH}", file=sys.stderr)
            return 1

    in_addr = read_address_from_file(args.input_address)
    out_addr = read_address_from_file(args.output_address)

    # keys
    sk = PaymentSigningKey.load(str(args.skey))
    vk = PaymentVerificationKey.from_signing_key(sk)

    ensure_outdir(OUTDIR)

    utxos = list_utxos(in_addr)
    if not utxos:
        print("No UTxOs at input address.")
        return 0

    print(f"UTxOs: {len(utxos)} | fee: {FEE} | out: {OUTDIR}")
    start = time.time()
    written = 0
    skipped = 0
    final_utxos = []  # Store final UTxOs for JSON output

    for idx, (txh, ix, amt) in enumerate(utxos, 1):
        if amt <= FEE:
            skipped += 1
            continue

        tx_in = TransactionInput.from_primitive([txh, ix])
        sendable = amt - FEE
        tx_out = TransactionOutput.from_primitive([out_addr, sendable])

        inputs = {tx_in}
        outputs = [tx_out]

        if TTL_DELTA > 0:
            ttl = get_tip_slot() + TTL_DELTA
            body = TransactionBody(inputs=inputs, outputs=outputs, fee=FEE, ttl=ttl)
        else:
            body = TransactionBody(inputs=inputs, outputs=outputs, fee=FEE)

        sig = sk.sign(body.hash())
        wset = TransactionWitnessSet(vkey_witnesses=[VerificationKeyWitness(vk, sig)])
        signed_tx = Transaction(body, wset)

        data = {
            "type": "Witnessed Tx ConwayEra",
            "description": "Ledger Cddl Format",
            "cborHex": signed_tx.to_cbor().hex(),
        }
        written += 1
        out_path = OUTDIR / f"{written:06d}.tx"
        with out_path.open("w", encoding="utf-8", newline="\n") as f:
            json.dump(data, f, indent=2)

        # Store the final UTxO that will be created by this transaction
        # The output will be at the output address with the sendable amount
        final_utxos.append({
            "tx_id": signed_tx.id.payload.hex(),
            "index": 0,  # This will be the first (and only) output
            "amount": sendable,
        })

        if idx % 200 == 0 or idx == len(utxos):
            elapsed = int(time.time() - start)
            print(f"progress: {idx}/{len(utxos)} | written: {written} | skipped: {skipped} | elapsed: {elapsed}s")

    print(f"Done. Written: {written} | Skipped: {skipped}. Files in {OUTDIR}/")

    # Write final UTxOs to JSON file if requested
    if args.utxo_out_file:
        with open(args.utxo_out_file, "w") as f:
            json.dump(final_utxos, f, indent=2)
        print(f"Wrote final UTxOs to {args.utxo_out_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
