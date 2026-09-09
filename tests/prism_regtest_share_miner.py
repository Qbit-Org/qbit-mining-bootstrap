#!/usr/bin/env python3
"""Seed a real regtest payout window with one acknowledged non-block share.

The live harness subsequently runs the ordinary block-solving miner. Keeping
this fixture separate leaves the deployed miner simulator's behavior intact.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import time


def load_miner():
    path = Path(__file__).resolve().parents[1] / "lab/miner-sim/miner_sim.py"
    spec = importlib.util.spec_from_file_location("prism_fixture_miner", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def solve_share(miner, job, extranonce1, extranonce2_size, share_target, deadline):
    block_target = miner.target_from_compact(job.nbits)
    if share_target <= block_target:
        raise RuntimeError("primed regtest requires a share target easier than the block target")
    for extranonce2 in range(1 << (8 * extranonce2_size)):
        extranonce2_hex = f"{extranonce2:0{extranonce2_size * 2}x}"
        _, raw = miner.assemble_header(job, extranonce1, extranonce2_hex, "00000000")
        header = bytearray(raw)
        for nonce in range(0x100000000):
            if nonce % 4096 == 0 and time.time() >= deadline:
                raise miner.RecoverableError("ordinary-share fixture timed out")
            header[76:80] = nonce.to_bytes(4, "little")
            value = int.from_bytes(miner.double_sha256(header), "little")
            if block_target < value <= share_target:
                return extranonce2_hex, f"{nonce:08x}"
    raise RuntimeError("ordinary-share fixture exhausted the nonce space")


def main() -> int:
    miner = load_miner()
    username = miner.resolve_miner_username()
    deadline = time.time() + miner.MINER_TIMEOUT_SECONDS
    while time.time() < deadline:
        client = miner.StratumClient(miner.STRATUM_HOST, miner.STRATUM_PORT, deadline)
        try:
            extranonce1, size, job, target, _ = miner.receive_handshake_and_job(client, username)
            extranonce2, nonce = solve_share(miner, job, extranonce1, size, target, deadline)
            request_id = client.send("mining.submit", [
                username, job.job_id, extranonce2, job.ntime, nonce,
            ])
            while time.time() < deadline:
                message = client.recv()
                if message.get("id") != request_id:
                    continue
                if message.get("result") is True:
                    print("ordinary share durably accepted", flush=True)
                    return 0
                raise miner.RecoverableError(str(message.get("error") or "share rejected"))
        except miner.RecoverableError as exc:
            print(f"ordinary-share fixture retry: {exc}", flush=True)
        finally:
            client.close()
    raise SystemExit("ordinary-share fixture timed out without a durable acknowledgment")


if __name__ == "__main__":
    raise SystemExit(main())
