#!/usr/bin/env python3
# Build & sign one owner->owner transaction PER UTxO at the address.
# Queries UTxOs via cardano-cli. Constructs and signs with pycardano.
# Outputs JSON files accepted by `cardano-cli conway transaction submit`.

import os, json, subprocess, sys, time
from pathlib import Path
from pycardano import (
    Transaction, TransactionBody, TransactionInput, TransactionOutput,
    TransactionWitnessSet, VerificationKeyWitness,
    PaymentSigningKey, PaymentVerificationKey
)

# ---------- config ----------
ADDR_FILE = os.getenv("OWNER_ADDR_FILE", "/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool1/owner.addr")
SKEY_FILE = os.getenv("OWNER_SKEY_FILE", "/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool1/owner-utxo.skey")
SOCKET    = os.getenv("CARDANO_NODE_SOCKET_PATH", "/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/bft1.socket")
MAGIC     = os.getenv("MAGIC", "42")
FEE       = int(os.getenv("FEE", "200000"))
TTL_DELTA = int(os.getenv("TTL_DELTA", "0"))    # 0 = omit ttl to avoid expiry
OUTDIR    = Path(os.getenv("OUTDIR", "../prepared_transactions"))

# ---------- helpers ----------
def run_cli(args, capture=True):
    cmd = ["cardano-cli", "conway", *args, "--testnet-magic", str(MAGIC), "--socket-path", str(SOCKET)]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(proc.returncode)
    return proc.stdout if capture else ""

def get_tip_slot():
    tip = json.loads(run_cli(["query", "tip"]))
    return int(tip.get("slot", tip.get("slotNo", 0)))

def list_owner_utxos(addr: str):
    j = json.loads(run_cli(["query", "utxo", "--address", addr, "--output-json"]))
    utxos = []
    for k, v in j.items():
        if "#" not in k:
            continue
        txh, ix = k.split("#", 1)
        amt = int(v.get("value", {}).get("lovelace", 0))
        utxos.append((txh, int(ix), amt))
    utxos.sort(key=lambda t: (t[0], t[1]))
    return utxos

def ensure_outdir():
    OUTDIR.mkdir(parents=True, exist_ok=True)

# ---------- main ----------
def main():
    addr = Path(ADDR_FILE).read_text().strip()
    sk = PaymentSigningKey.load(SKEY_FILE)
    vk = PaymentVerificationKey.from_signing_key(sk) # Shorten by just loading vkey?

    ensure_outdir()

    utxos = list_owner_utxos(addr)
    if not utxos:
        print("No UTxOs at owner address.")
        return 0

    print(f"UTxOs: {len(utxos)} | fee: {FEE} | out: {OUTDIR}")
    start = time.time()
    written = 0
    skipped = 0

    for idx, (txh, ix, amt) in enumerate(utxos, 1):
        if amt <= FEE:
            skipped += 1
            continue

        tx_in = TransactionInput.from_primitive([txh, ix])
        sendable = amt - FEE
        tx_out = TransactionOutput.from_primitive([addr, sendable])

        # Inputs must be a set to encode as CBOR tag 258 apparently
        inputs = {tx_in}
        outputs = [tx_out]

        # Build body. Omit ttl by default to avoid OutsideValidityIntervalUTxO.
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
            f.write("\n")

        if idx % 200 == 0 or idx == len(utxos):
            elapsed = int(time.time() - start)
            print(f"progress: {idx}/{len(utxos)} | written: {written} | skipped: {skipped} | elapsed: {elapsed}s")

    print(f"Done. Written: {written} | Skipped: {skipped}. Files in {OUTDIR}/")
    return 0

if __name__ == "__main__":
    sys.exit(main())
