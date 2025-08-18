# python3
import os, json, subprocess
from pycardano import (
    Transaction, TransactionBody, TransactionInput, TransactionOutput,
    TransactionWitnessSet, VerificationKeyWitness,
    PaymentSigningKey, PaymentVerificationKey
)

ADDR_FILE = "/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool1/owner.addr"
SOCKET    = "/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/bft1.socket"
MAGIC     = "42"                          # local cluster magic
SPLITS    = 10                            # how many outputs to create
FEE       = 200_000                       # replace with min-fee you compute via CLI

# Get owner address
addr = open(ADDR_FILE).read().strip()

def run_cli(args, text=True):
    cmd = ["cardano-cli", "conway"] + args + ["--testnet-magic", MAGIC, "--socket-path", SOCKET]
    return subprocess.check_output(cmd, text=text)

# Query all UTxOs at addr as JSON
raw = run_cli(["query", "utxo", "--address", addr, "--output-json"])
utxos = json.loads(raw)
print("UTxOs:")
print(utxos)

# Build inputs from UTxOs
inputs = []
total_lovelace = 0
for k, v in utxos.items():
    txid, ix = k.split("#")
    inputs.append(TransactionInput.from_primitive([txid, int(ix)]))
    total_lovelace += v["value"]["lovelace"]

if not inputs:
    raise SystemExit("No UTxOs at address")

# 4) Optional TTL (invalid-hereafter): current slot + buffer
tip = json.loads(run_cli(["query", "tip"]))
ttl = int(tip["slot"]) + 2000

# 5) Create split outputs back to same address
sendable = total_lovelace - FEE
per_out = sendable // SPLITS
outputs = [TransactionOutput.from_primitive([addr, per_out]) for _ in range(SPLITS)]

# 6) Construct, sign, and export
body = TransactionBody(inputs=inputs, outputs=outputs, fee=FEE, ttl=ttl)

sk = PaymentSigningKey.load("/home/ubuntu/cardano/cardano-node-tests/dev_workdir/state-cluster0/nodes/node-pool1/owner-utxo.skey")
vk = PaymentVerificationKey.from_signing_key(sk)
sig = sk.sign(body.hash())
wset = TransactionWitnessSet(vkey_witnesses=[VerificationKeyWitness(vk, sig)])
signed_tx = Transaction(body, wset)

# JSON file with CBOR hex (based off one of the cli outputs)
data = {
    "type": "Witnessed Tx ConwayEra",
    "description": "Ledger Cddl Format",
    "cborHex": signed_tx.to_cbor().hex(),
}
with open("transaction.tx", "w") as f:
    json.dump(data, f, indent=4)
print("Wrote transaction.tx")
