#!/usr/bin/env python3
# tx_splitter.py
# Plan branching then split a funding UTxO across levels to produce many leaf UTxOs.

import argparse
import json
import math
import os
import sys
import time
import threading
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm
from ogmios.client import Client as OgmiosClient
from ogmios.datatypes import Address as OgmiosAddress
from ogmios.datatypes import TxOutputReference as OgmiosTxOutputReference
from ogmios.datatypes import ProtocolParameters as OgmiosProtocolParameters
from ogmios.datatypes import Direction as OgmiosDirection
from ogmios.datatypes import Block as OgmiosBlock
from ogmios.errors import ResponseError as OgmiosResponseError
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
from pycardano import (
    Network, PaymentSigningKey, PaymentVerificationKey, Address,
    Transaction, TransactionId, TransactionInput, TransactionOutput, TransactionBody, Value,
    OrderedSet, TransactionWitnessSet, VerificationKey, VerificationKeyWitness,
)

thread_local_data = threading.local()

# ---------- tunables (env overrides allowed) ----------
DEF_THREADS = int(os.getenv("THREADS", str(os.cpu_count() or 4)))
DEF_CAP = int(os.getenv("CAP", "200"))
DEF_LEAF = int(os.getenv("LEAF", "2000000"))
DEF_FEE_MARGIN = int(os.getenv("FEE_MARGIN", "2000000"))
DEF_CHANGE_MIN = int(os.getenv("CHANGE_MIN", "1200000"))
DEF_MIN_OUTPUT = int(os.getenv("MIN_OUTPUT", "1000000"))  # floor per produced output
DEF_MAX_OUTS = int(os.getenv("MAX_OUTS_PER_TX", str(DEF_CAP)))

# ---------- utilities ----------
def read_address_from_file(path_like: Path | str) -> Address:
    """Read an address file containing either a bech32 string or JSON with 'address'."""
    text = Path(path_like).read_text().strip()
    if not text:
        raise ValueError("Address file is empty")
    if text.startswith("{"):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                addr_str = data.get("address")
                if isinstance(addr_str, str) and addr_str:
                    return Address.from_primitive(addr_str.strip())
        except Exception:
            pass
    return Address.from_primitive(text)

def ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b

# ---------- planner ----------
def split_plan(N: int, CAP: int) -> dict:
    if N <= 0:
        return dict(levels=0, branching_factors=[], txs_per_level=[], total_split_txs=0, final_outputs_produced=0)

    if N <= CAP:
        BFS = [N]
        TXS = [1]
        return dict(levels=1, branching_factors=BFS, txs_per_level=TXS, total_split_txs=sum(TXS), final_outputs_produced=math.prod(BFS))

    L = 1
    while CAP ** L < N:
        L += 1

    BFS = []
    Nrem = N
    for i in range(1, L + 1):
        remaining = L - i
        denom = CAP ** remaining
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

# ---------- helpers ----------
@dataclass
class BuildUtxo:
    input_tx_id: bytes
    input_index: int
    output_amount: int

@dataclass
class BuildProtocolParameters:
    min_fee_coefficient: int
    min_fee_constant: int
    coins_per_utxo_byte: int

def min_lovelace_post_alonzo(output: TransactionOutput, params: BuildProtocolParameters) -> int:
    """Calculate minimum lovelace a transaction output needs to hold post alonzo."""
    constant_overhead = 160

    amt = output.amount

    # If the amount of ADA is 0, a default value of 1 ADA will be used
    if amt.coin == 0:
        amt.coin = 1000000

    # Make sure we are using post-alonzo output
    tmp_out = TransactionOutput(
        output.address,
        output.amount,
        output.datum_hash,
        output.datum,
        output.script,
        True,
    )

    return (
        constant_overhead + len(tmp_out.to_cbor())
    ) * params.coins_per_utxo_byte

def fee(
    params: BuildProtocolParameters,
    length: int,
) -> int:
    """Calculate fee based on the length of a transaction's CBOR bytes."""
    return int(
        math.ceil(length * params.min_fee_coefficient)
        + math.ceil(params.min_fee_constant)
    )

# ---------- splitting ----------
FAKE_VKEY = VerificationKey.from_primitive(
    bytes.fromhex("5797dc2cc919dfec0bb849551ebdf30d96e5cbe0f33f734a87fe826db30f7ef9")
)
FAKE_TX_SIGNATURE = bytes.fromhex(
    "577ccb5b487b64e396b0976c6f71558e52e44ad254db7d06dfb79843e5441a5d763dd42a"
    "dcf5e8805d70373722ebbce62a58e3f30dd4560b9a898b8ceeab6a03"
)

_MAX_UINT64 = (1 << 64) - 1

def calculate_max_fee(params: BuildProtocolParameters, max_outputs: int) -> int:
    """
    Calculates a single, fixed fee sufficient for any transaction this script builds.
    This is done by constructing a worst-case scenario transaction and calculating its fee.
    """
    # Use a real, long testnet address to represent the worst-case address size.
    # This is a Base address (payment + staking parts), which is a common and long format.
    dummy_addr = Address.from_primitive(
        "addr_test1qqf0v4wdfstjerfwn4kv3z0hq46qes8rmv74etat5nnnsmtad526kufvtyzvk3mqpy2pyselufrp45uqhrst2qcmquyse9zsdq"
    )

    # 1. Worst-case structure
    # One input UTXO
    dummy_input = TransactionInput(TransactionId(b'\0' * 32), 0)

    # Max payment outputs + 1 change output
    # Use a max-byte-encoded integer for the amount to ensure worst-case CBOR size.
    dummy_outputs = [
        TransactionOutput(dummy_addr, _MAX_UINT64) for _ in range(max_outputs + 1)
    ]

    # 2. Build the fake transaction body
    # Use a max-byte-encoded integer for the fee as well.
    tx_body_for_size = TransactionBody(
        inputs=OrderedSet([dummy_input]),
        outputs=dummy_outputs,
        fee=_MAX_UINT64,
    )

    # 3. Add a fake witness set (its size is constant)
    fake_witness = TransactionWitnessSet(
        vkey_witnesses=[VerificationKeyWitness(FAKE_VKEY, FAKE_TX_SIGNATURE)]
    )
    tx_for_size = Transaction(tx_body_for_size, fake_witness)

    # 4. Calculate the fee based on the size of this maximal transaction.
    # By using max-sized components (address length, integer encoding), this
    # calculation is a deterministic upper bound. No additional safety margin is needed.
    max_tx_size = len(tx_for_size.to_cbor())
    return fee(params, max_tx_size)

def build_and_sign_split_tx(
    utxo_to_spend: BuildUtxo,
    b: int,  # This 'b' is now the number of outputs for THIS transaction
    pay_to: Address,
    change_addr: Address,
    sign_key: PaymentSigningKey,
    params: BuildProtocolParameters,
    fixed_fee: int,
) -> tuple[bytes, TransactionId, list[BuildUtxo]]:
    """
    Builds and signs a single transaction to split one UTxO into 'b' outputs.
    This function is purely offline/CPU-bound and does not interact with the network.
    It returns a single signed transaction.
    """
    if not (1 <= b <= DEF_MAX_OUTS):
        raise ValueError(f"Branching factor b={b} must be between 1 and {DEF_MAX_OUTS}")

    verification_key = sign_key.to_verification_key()

    current_utxo_input = TransactionInput(TransactionId(utxo_to_spend.input_tx_id), utxo_to_spend.input_index)
    in_val = utxo_to_spend.output_amount
    if in_val <= 0:
        raise RuntimeError(f"Input {current_utxo_input} has no lovelace.")

    # Usable amount must account for fee and potential change
    usable = in_val - fixed_fee - DEF_CHANGE_MIN
    share = usable // b
    if share < DEF_MIN_OUTPUT:
        raise RuntimeError(
            f"Input {current_utxo_input} value={in_val} too small for b={b} outputs. Share per output would be {share}."
        )

    payment_outputs = [TransactionOutput(pay_to, share) for _ in range(b)]
    total_payment_lovelace = share * b

    change_val = in_val - total_payment_lovelace - fixed_fee

    if change_val < 0:
        raise RuntimeError(f"Not enough funds for fee. Deficit: {-change_val}")

    change_output = TransactionOutput(address=change_addr, amount=Value(coin=change_val))
    all_outputs = payment_outputs + [change_output]

    # Check if change is sufficient
    min_lovelace = min_lovelace_post_alonzo(change_output, params)
    if change_val < min_lovelace:
        # If change is too small, we can't create a change output.
        # For simplicity, we fail. A more complex script might try to absorb the dust into the fee.
        raise RuntimeError(
            f"Change amount {change_val} is less than min required {min_lovelace}. Cannot proceed."
        )

    final_tx_body = TransactionBody(
        inputs=OrderedSet([current_utxo_input]),
        outputs=all_outputs,
        fee=fixed_fee,
    )

    signature = sign_key.sign(final_tx_body.hash())
    witness_set = TransactionWitnessSet(vkey_witnesses=[VerificationKeyWitness(verification_key, signature)])

    signed_tx = Transaction(final_tx_body, witness_set)
    txid = signed_tx.id

    newly_created_utxos = [
        BuildUtxo(
            input_tx_id=txid.payload,
            input_index=i,
            output_amount=signed_tx.transaction_body.outputs[i].amount.coin,
        )
        for i in range(b)
    ]

    return signed_tx.to_cbor(), txid, newly_created_utxos

WORKER_PAY_TO: Address = None
WORKER_CHANGE_ADDR: Address = None
WORKER_SIGN_KEY: PaymentSigningKey = None

def init_worker(pay_to: bytes, change_addr: bytes, sign_key_bytes: bytes):
    global WORKER_PAY_TO, WORKER_CHANGE_ADDR, WORKER_SIGN_KEY
    WORKER_PAY_TO = Address.from_primitive(pay_to)
    WORKER_CHANGE_ADDR = Address.from_primitive(change_addr)
    WORKER_SIGN_KEY = PaymentSigningKey.from_primitive(sign_key_bytes)

def process_chunk_worker(
        utxo_chunk: list[BuildUtxo],
        b: int,
        params: BuildProtocolParameters,
        fixed_fee: int,
) -> tuple[list[tuple[bytes, bytes]], list[BuildUtxo]]:
    """
    Processes a whole chunk of UTxOs, reducing IPC overhead.
    """
    all_signed_txs = []
    all_new_utxos = []

    for utxo in utxo_chunk:
        signed_tx, txid, newly_created_utxos = build_and_sign_split_tx(
            utxo, b, WORKER_PAY_TO, WORKER_CHANGE_ADDR, WORKER_SIGN_KEY, params, fixed_fee
        )

        all_signed_txs.append((signed_tx, txid.payload))
        all_new_utxos.extend(newly_created_utxos)

    return all_signed_txs, all_new_utxos

# ---------- main ----------
class BlockListener(threading.Thread):
    """
    A dedicated thread to listen for new blocks. It internally manages a thread-safe
    map of transaction IDs to threading.Event objects, allowing other threads
    to wait for transaction confirmations.
    """
    def __init__(self, host: str, port: int):
        super().__init__(daemon=True)
        self.ogmios_host = host
        self.ogmios_port = port
        self.client = None
        self.stop_event = threading.Event()

        self._lock = threading.Lock()
        # Map of tx_id -> (event, required_depth)
        self._events_map: dict[TransactionId, tuple[threading.Event, int]] = {}
        # Map of tx_id -> block number it was confirmed in
        self._confirmed_txs: dict[TransactionId, int] = {}
        self._current_block_no = 0

    def run(self):
        print("Block listener thread started.")
        # This connection logic is simplified for brevity. A production system
        # would have more robust reconnection handling.
        while not self.stop_event.is_set():
            try:
                self.client = OgmiosClient(self.ogmios_host, self.ogmios_port)
                tip, _ = self.client.query_network_tip.execute()
                block_no, _ = self.client.query_block_height.execute()
                if isinstance(block_no, int):
                    with self._lock:
                        self._current_block_no = block_no
                self.client.find_intersection.execute([tip])
                for _ in range(100):
                    self.client.next_block.send()

                while not self.stop_event.is_set():
                    direction, _, block, _ = self.client.next_block.receive()
                    if self.stop_event.is_set(): break
                    self.client.next_block.send()

                    if direction == OgmiosDirection.forward and isinstance(block, OgmiosBlock) and hasattr(block, "transactions"):
                        block_no = block.height
                        with self._lock:
                            self._current_block_no = block_no
                            for tx in block.transactions:
                                tx_id = TransactionId(bytes.fromhex(tx.get("id")))
                                if tx_id not in self._confirmed_txs:
                                    self._confirmed_txs[tx_id] = block_no

                            for tx_id, (event, req_depth) in list(self._events_map.items()):
                                confirmed_block = self._confirmed_txs.get(tx_id)
                                if confirmed_block is not None and (block_no - confirmed_block + 1) >= req_depth:
                                    event.set()
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"Block listener error: {e}. Reconnecting in 5s...")
                    time.sleep(5)
            finally:
                if self.client:
                    self.client.connection.close()

        print("Block listener thread stopped.")

    # --- Public API for other threads ---
    def wait_for_confirmation(self, tx_id: TransactionId, timeout: float, depth: int) -> bool:
        """
        Registers a tx_id for confirmation and waits for a specified timeout.
        Returns True if confirmed, False if timed out. This is a complete,
        atomic operation from the user's perspective.
        """
        confirmation_event = threading.Event()
        with self._lock:
            confirmed_at = self._confirmed_txs.get(tx_id)
            if confirmed_at is not None and (self._current_block_no - confirmed_at + 1) >= 1:
                return True
            self._events_map[tx_id] = (confirmation_event, depth)

        try:
            confirmed = confirmation_event.wait(timeout=timeout)
        finally:
            # Always clean up the map to prevent memory leaks
            with self._lock:
                self._events_map.pop(tx_id, None)

        return confirmed

    def shutdown(self):
        self.stop_event.set()
        if self.client:
            try:
                self.client.connection.close()
            except:
                pass

def submit_and_wait(
    tx: tuple[bytes, bytes],
    block_listener: BlockListener,
    ogmios_host: str,
    ogmios_port: int,
) -> TransactionId:
    """Submits a transaction and uses the BlockListener to wait for confirmation."""
    max_retries = 15
    wait_timeout = 40.0  # seconds
    confirmation_depth = 2
    ogmios_client: OgmiosClient = thread_local_data.ogmios_client
    tx_cbor, txid = tx
    tx_id = TransactionId(txid)

    for attempt in range(max_retries):
        try:
            ogmios_client.submit_transaction.execute(tx_cbor.hex())
        except (ConnectionClosedOK, ConnectionClosedError):
            print(f"Ogmios connection closed. Reconnecting...")
            ogmios_client = OgmiosClient(ogmios_host, ogmios_port)
            thread_local_data.ogmios_client = ogmios_client
            continue
        except Exception as e:
            if isinstance(e, OgmiosResponseError):
                response = json.loads(str(e).removeprefix("Ogmios responded with error: "))
                if response.get("error", {}).get("code") == 3117:
                    pass
            print(f"ERROR: Submission for attempt {attempt + 1} failed: {e}. Retrying after 2s...")
            time.sleep(2)
            continue

        confirmed = block_listener.wait_for_confirmation(tx_id, timeout=wait_timeout, depth=confirmation_depth)
        if confirmed:
            return tx_id

    raise RuntimeError(f"Failed to confirm transaction after {max_retries} attempts.")

def init_worker_client(ogmios_host: str, ogmios_port: int):
    """
    This function is run once per thread in the ThreadPoolExecutor.
    It creates a dedicated OgmiosClient for the current thread.
    """
    thread_local_data.ogmios_client = OgmiosClient(ogmios_host, ogmios_port)

def cleanup_worker_client():
    """
    Cleans up the client connection for the current thread.
    (Note: ThreadPoolExecutor does not have a built-in finalizer,
    so this would need a custom pool wrapper for perfect cleanup.
    For this script, OS will handle it on exit.)
    """
    if hasattr(thread_local_data, 'ogmios_client'):
        thread_local_data.ogmios_client.connection.close()

def main():
    ap = argparse.ArgumentParser(
        description="Plan branching and split a funding UTxO into many leaves. Equal-split only."
    )
    ap.add_argument("target", type=int, help="desired leaf UTxOs")
    ap.add_argument("--ogmios-host", type=str, default="127.0.0.1", help="Ogmios host address")
    ap.add_argument("--ogmios-port", type=int, default=1337, help="Ogmios port")

    ap.add_argument("-o", "--output-address", type=Path, required=True, help="file containing final leaves address")
    ap.add_argument("-k", "--skey", type=Path, required=True, help="signing key file used to spend funding and change")
    ap.add_argument("--threads", type=int, default=DEF_THREADS, help="parallel splits per level")
    ap.add_argument("--utxo-out-file", type=Path, help="Optional: File to save the final list of created UTxOs")
    args = ap.parse_args()

    # prerequisites
    for f in [args.output_address, args.skey]:
        if not Path(f).exists():
            print(f"missing: {f}", file=sys.stderr)
            sys.exit(1)

    try:
        sign_key: PaymentSigningKey = PaymentSigningKey.load(str(args.skey))
        input_addr = Address(PaymentVerificationKey.from_signing_key(sign_key).hash(), network=Network.TESTNET)
        output_addr = read_address_from_file(args.output_address)
    except Exception as e:
        print(f"Error loading keys or addresses: {e}", file=sys.stderr)
        sys.exit(1)
    sign_key_bytes = sign_key.to_primitive()
    input_addr_bytes = input_addr.to_primitive()
    output_addr_bytes = output_addr.to_primitive()

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

    print(f"Querying initial UTxO at {input_addr}...")
    oprotocol_param: OgmiosProtocolParameters
    with OgmiosClient(args.ogmios_host, args.ogmios_port) as client:
        outxos, _ = client.query_utxo.execute([OgmiosAddress(str(input_addr))])
        oprotocol_param, _ = client.query_protocol_parameters.execute()
    funding_utxos = [BuildUtxo(
        input_tx_id=bytes.fromhex(outxo.tx_id),
        input_index=outxo.index,
        output_amount=outxo.value.get("ada", {}).get("lovelace", 0),
    ) for outxo in outxos]
    params = BuildProtocolParameters(
        min_fee_coefficient=oprotocol_param.min_fee_coefficient,
        min_fee_constant=oprotocol_param.min_fee_constant.lovelace,
        coins_per_utxo_byte=oprotocol_param.min_utxo_deposit_coefficient,
    )
    fixed_fee = calculate_max_fee(params, DEF_MAX_OUTS)
    if not funding_utxos:
        print(f"No UTxOs found at the funding address: {input_addr}", file=sys.stderr)
        sys.exit(1)

    g0_utxo = max(funding_utxos, key=lambda u: u.output_amount)
    print(f"Found funding UTxO: {TransactionInput(TransactionId(g0_utxo.input_tx_id), g0_utxo.input_index)} with {g0_utxo.output_amount / 1_000_000} ADA")

    current_level_utxos = [g0_utxo]
    overall_start = time.time()

    block_listener = BlockListener(args.ogmios_host, args.ogmios_port)
    block_listener.start()
    time.sleep(2)  # Give the listener a moment to start

    for lvl in range(L):
        b = BFS[lvl]
        pay_to_bytes = output_addr_bytes if lvl == L - 1 else input_addr_bytes

        num_inputs = len(current_level_utxos)
        print(f"Level {lvl}: b={b}, amt=equal, txs={num_inputs}, threads={args.threads}")
        lvl_start_time = time.time()

        all_signed_txs: list[tuple[bytes, bytes]] = []
        next_level_utxos: list[BuildUtxo] = []

        num_tasks = len(current_level_utxos)
        # Ensure at least one chunk even if tasks < threads
        num_chunks = min(num_tasks, args.threads * 4) # Heuristic: give each thread a few chunks to work on
        if num_chunks == 0:
            chunk_size = 0
        else:
            chunk_size = ceil_div(num_tasks, num_chunks)

        utxo_chunks = [
            current_level_utxos[i : i + chunk_size]
            for i in range(0, num_tasks, chunk_size)
        ]
        print(f"Distributing {num_tasks} UTxOs into {len(utxo_chunks)} chunks of size ~{chunk_size}...")

        with ProcessPoolExecutor(max_workers=args.threads, initializer=init_worker, initargs=(pay_to_bytes, input_addr_bytes, sign_key_bytes)) as ex:
            futs = {
                ex.submit(process_chunk_worker, chunk, b, params, fixed_fee): chunk
                for chunk in utxo_chunks
            }
            pbar = tqdm(total=num_tasks, desc=f"Building L{lvl} Txs", unit="utxo")
            for fut in as_completed(futs):
                chunk = futs[fut]
                txs, utxos = fut.result()
                all_signed_txs.extend(txs)
                next_level_utxos.extend(utxos)
                pbar.update(len(chunk))
            pbar.close()

        print(f"Built {len(all_signed_txs)} transactions in {time.time() - lvl_start_time:.2f}s")
        build_time = time.time()

        with ThreadPoolExecutor(max_workers=10,
            initializer=init_worker_client,
            initargs=(args.ogmios_host, args.ogmios_port)
        ) as ex:
            futs = {
                ex.submit(submit_and_wait, tx, block_listener, args.ogmios_host, args.ogmios_port): tx
                for tx in all_signed_txs
            }
            for _ in tqdm(as_completed(futs), total=len(futs), desc=f"Submitting L{lvl} Txs", unit="tx"):
                pass
            for _ in range(10):
                ex.submit(cleanup_worker_client)

        with OgmiosClient(args.ogmios_host, args.ogmios_port) as client:
            for utxo in next_level_utxos:
                outxos, _ = client.query_utxo.execute([OgmiosTxOutputReference(utxo.input_tx_id.hex(), utxo.input_index)])
                if not outxos:
                    print(f"WARNING: UTxO {TransactionInput(TransactionId(utxo.input_tx_id), utxo.input_index)} not found on-chain after submission.", file=sys.stderr)
                    sys.exit(1)

        print(f"Submitted and confirmed {len(all_signed_txs)} transactions in {time.time() - build_time:.2f}s")

        current_level_utxos = next_level_utxos
        print(f"Level {lvl} total time: {time.time() - lvl_start_time:.2f}s")

    block_listener.shutdown()
    block_listener.join()

    print(f"\nAll levels complete. Produced {len(current_level_utxos)} final UTxOs.")
    print(f"Total script time: {time.time() - overall_start:.2f}s")

    if args.utxo_out_file:
        with open(args.utxo_out_file, "w") as f:
            json.dump([
                {
                    "tx_id": u.input_tx_id.hex(),
                    "index": u.input_index,
                    "amount": u.output_amount,
                }
                for u in current_level_utxos
            ], f)
        print(f"Wrote final UTxOs to {args.utxo_out_file}")

if __name__ == "__main__":
    main()
