#!/usr/bin/env python3
# build_owner_txs.py
# Build and sign one transaction per UTxO at the input address.
# Inputs are sent to the output address (can be the same as input).
# Produces JSON files that `cardano-cli conway transaction submit` accepts.

import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from pycardano import (
    Transaction,
    TransactionBody,
    TransactionInput,
    TransactionOutput,
    TransactionId,
    TransactionWitnessSet,
    VerificationKeyWitness,
    PaymentSigningKey,
    PaymentVerificationKey,
    Address,
)
from tqdm import tqdm

DEF_THREADS = int(os.getenv("THREADS", str(os.cpu_count() or 4)))

# ---------- globals from CLI ----------
FEE: int = 200_000
OUTDIR: Path = Path("./prepared_transactions")


# ---------- helpers ----------
def read_address_from_file(path_like: Path | str) -> Address:
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
                        return Address.from_primitive(val.strip())
        except json.JSONDecodeError:
            pass
        raise ValueError(f"address JSON missing a usable field: {path_like}")
    return Address.from_primitive(text)

def ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b

def ensure_outdir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

@dataclass
class BuildUtxo:
    input_tx_id: bytes
    input_index: int
    output_amount: int

def build_and_sign_tx(
    utxo: BuildUtxo,
    out_addr: Address,
    sk: PaymentSigningKey,
    vk: PaymentVerificationKey,
) -> bytes:
    tx_in = TransactionInput(TransactionId(utxo.input_tx_id), utxo.input_index)
    sendable = utxo.output_amount - FEE
    tx_out = TransactionOutput(out_addr, sendable)

    inputs = {tx_in}
    outputs = [tx_out]

    body = TransactionBody(inputs=inputs, outputs=outputs, fee=FEE)

    sig = sk.sign(body.hash())
    wset = TransactionWitnessSet(vkey_witnesses=[VerificationKeyWitness(vk, sig)])
    signed_tx = Transaction(body, wset)

    return signed_tx.to_cbor()

WORKER_PAY_TO: Address = None
WORKER_SIGN_KEY: PaymentSigningKey = None
WORKER_VERIFY_KEY: PaymentVerificationKey = None

def init_worker(pay_to: bytes, sign_key: bytes):
    global WORKER_PAY_TO, WORKER_SIGN_KEY, WORKER_VERIFY_KEY
    WORKER_PAY_TO = Address.from_primitive(pay_to)
    WORKER_SIGN_KEY = PaymentSigningKey.from_primitive(sign_key)
    WORKER_VERIFY_KEY = WORKER_SIGN_KEY.to_verification_key()

def process_chunk_worker(
        utxo_chunk: list[BuildUtxo],
) -> list[bytes]:
    """
    Processes a whole chunk of UTxOs, reducing IPC overhead.
    """
    all_signed_txs = []

    for utxo in utxo_chunk:
        signed_tx = build_and_sign_tx(
            utxo, WORKER_PAY_TO, WORKER_SIGN_KEY, WORKER_VERIFY_KEY,
        )

        all_signed_txs.append(signed_tx)

    return all_signed_txs

# ---------- main ----------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build and sign one transaction per UTxO at the input address."
    )
    ap.add_argument("--utxos-file", type=Path, required=True, help="JSON file containing UTxOs to process")
    ap.add_argument("-o", "--output-address", required=True, type=Path, help="File containing output address")
    ap.add_argument("-k", "--skey", required=True, type=Path, help="Payment signing key file")
    ap.add_argument("--threads", type=int, default=DEF_THREADS, help="parallelism threads")
    ap.add_argument("--fee", type=int, default=200_000, help="Fixed fee per tx in lovelace")
    ap.add_argument("--outdir", type=Path, default=Path("./prepared_transactions"), help="Directory to write tx JSONs")
    ap.add_argument("--first-tx-index", type=int, default=1, help="Starting index for naming output files")
    return ap.parse_args()


def main() -> int:
    global FEE, OUTDIR

    args = parse_args()
    FEE = int(args.fee)
    OUTDIR = args.outdir

    # validate files
    for p in (args.utxos_file, args.output_address, args.skey):
        if not Path(p).exists():
            print(f"missing file: {p}", file=sys.stderr)
            return 1

    out_addr = read_address_from_file(args.output_address)

    # keys
    sk: PaymentSigningKey = PaymentSigningKey.load(str(args.skey))

    ensure_outdir(OUTDIR)

    print(f"Loading UTxOs from {args.utxos_file}...")
    with args.utxos_file.open("r", encoding="utf-8") as f:
        utxo_data = json.load(f)
    utxos = [BuildUtxo(
        input_tx_id=bytes.fromhex(u["tx_id"]),
        input_index=u["index"],
        output_amount=u["amount"])
        for u in utxo_data
        if u["amount"] > FEE
        ]
    if not utxos:
        print("No writeable UTxOs at input address.")
        return 0

    print(f"UTxOs: {len(utxos)} | fee: {FEE} | out: {OUTDIR}")
    written = args.first_tx_index
    skipped = len(utxo_data) - len(utxos)

    num_tasks = len(utxos)
    # Ensure at least one chunk even if tasks < threads
    num_chunks = min(num_tasks, args.threads * 4) # Heuristic: give each thread a few chunks to work on
    if num_chunks == 0:
        chunk_size = 0
    else:
        chunk_size = ceil_div(num_tasks, num_chunks)

    utxo_chunks = [
        utxos[i : i + chunk_size]
        for i in range(0, num_tasks, chunk_size)
    ]
    print(f"Distributing {num_tasks} UTxOs into {len(utxo_chunks)} chunks of size ~{chunk_size}...")

    with ProcessPoolExecutor(max_workers=args.threads, initializer=init_worker, initargs=(out_addr.to_primitive(), sk.to_primitive())) as ex:
        futs = [
            ex.submit(
                process_chunk_worker,
                chunk,
            )
            for chunk in utxo_chunks
        ]
        pbar = tqdm(total=num_tasks, desc="Building Txs", unit="utxo")
        for fut in as_completed(futs):
            txs = fut.result()

            for tx in txs:
                data = {
                    "type": "Witnessed Tx ConwayEra",
                    "description": "Ledger Cddl Format",
                    "cborHex": tx.hex(),
                }
                out_path = OUTDIR / f"{written:06d}.tx"
                with out_path.open("w", encoding="utf-8", newline="\n") as f:
                    json.dump(data, f, indent=2)
                written += 1

            pbar.update(len(txs))
        pbar.close()

    print(f"Done. Written: {written} | Skipped: {skipped}. Files in {OUTDIR}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
