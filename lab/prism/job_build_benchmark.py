#!/usr/bin/env python3
"""PRISM coordinator job-build latency benchmark.

Runs an in-process coordinator against a synthetic ledger/RPC whose latencies
are injected to match measurements from a live host, subscribes N clients,
and replays tip changes the way the blockpoll does. It reports the
distribution of the same per-client elapsed the coordinator logs as
"sent job ... elapsed=".

Two modes on the same code:
  --mode cached    per-template job cache enabled (the fix)
  --mode uncached  all cache TTLs forced to 0, reproducing the legacy
                   build-everything-per-client-per-rebuild behavior

The audit bundle builder can be the real Rust subprocess (--real-builder,
honoring PRISM_TOOL_BIN_DIR / PRISM_CARGO and using test signing seeds) fed a
share window of --shares synthetic shares, or a stand-in that sleeps for the
measured builder latency (--fake-builder-delay).

The benchmark never touches a real ledger database and never mutates chain
state: the only external process it may run is the (pure computation) bundle
builder.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.prism_coordinator import (
    ClientState,
    PRISM_REJECTION_REASON_IDS,
    PrismCoordinator,
    WorkerIdentity,
    default_prism_coinbase_tag_hex,
)

EXTRANONCE2_SIZE = 8
MINER_PROGRAMS = ("aa" * 32, "bb" * 32)


def sanitize_environment() -> None:
    """Make runs reproducible regardless of ambient container env: drop all
    PRISM_*/QBIT_* configuration except the tool binary resolution knobs."""
    keep = {"PRISM_TOOL_BIN_DIR", "PRISM_CARGO", "CARGO"}
    for name in list(os.environ):
        if (name.startswith("PRISM_") or name.startswith("QBIT_")) and name not in keep:
            del os.environ[name]


@dataclass
class InjectedLatencies:
    gbt_seconds: float
    snapshot_seconds: float
    stats_seconds: float
    balances_seconds: float
    reorg_watch_seconds: float
    mark_mature_seconds: float
    rpc_small_seconds: float


class BenchLedger:
    backend_name = "bench"

    def __init__(self, share_count: int, latencies: InjectedLatencies) -> None:
        self.latencies = latencies
        now = int(time.time() * 1000)
        self.shares_json = [
            {
                "share_seq": index + 1,
                "share_id": f"bench-share-{index + 1}",
                "miner_id": f"bench-miner-{index % len(MINER_PROGRAMS)}",
                "order_key": f"bench-miner-{index % len(MINER_PROGRAMS)}",
                "p2mr_program_hex": MINER_PROGRAMS[index % len(MINER_PROGRAMS)],
                "share_difficulty": 16384,
                "network_difficulty": 226646186,
                "template_height": 9,
                "job_id": f"bench-job-{index + 1}",
                "job_issued_at_ms": now - (share_count - index) * 1_000,
                "accepted_at_ms": now - (share_count - index) * 1_000,
                "ntime": int(time.time()) - (share_count - index),
            }
            for index in range(share_count)
        ]

    class _Record:
        __slots__ = ("payload",)

        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        def to_prism_json(self) -> dict[str, object]:
            return self.payload

    def accepted_share_stats(self) -> dict[str, int]:
        time.sleep(self.latencies.stats_seconds)
        return {
            "accepted_share_count": len(self.shares_json),
            "distinct_miner_count": len(MINER_PROGRAMS),
        }

    def all_shares(self) -> list[object]:
        raise AssertionError("benchmark expects accepted_share_stats to be used")

    def snapshot_at_job_issue(self, anchor_job_issued_at_ms: int, *, window_weight: int | None = None) -> list[object]:
        time.sleep(self.latencies.snapshot_seconds)
        return [self._Record(payload) for payload in self.shares_json]

    def current_prior_balances(self) -> list[dict[str, object]]:
        time.sleep(self.latencies.balances_seconds)
        return []

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        time.sleep(self.latencies.reorg_watch_seconds)
        return []

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, int]:
        time.sleep(self.latencies.mark_mature_seconds)
        return {"matured_count": 0}

    def metrics(self) -> dict[str, int]:
        return {"blocks": 0, "owed_accounts": 0}


class BenchRpc:
    def __init__(self, template: dict[str, object], latencies: InjectedLatencies) -> None:
        self.template = template
        self.latencies = latencies
        self.calls: dict[str, int] = {}
        self.lock = threading.Lock()

    def call(self, method: str, params: list[object] | None = None) -> object:
        with self.lock:
            self.calls[method] = self.calls.get(method, 0) + 1
        if method == "getblocktemplate":
            time.sleep(self.latencies.gbt_seconds)
            with self.lock:
                return dict(self.template)
        time.sleep(self.latencies.rpc_small_seconds)
        if method == "getbestblockhash":
            with self.lock:
                return str(self.template["previousblockhash"])
        if method == "getblockchaininfo":
            with self.lock:
                height = int(self.template["height"]) - 1
            return {"initialblockdownload": False, "blocks": height, "headers": height}
        if method == "getblockcount":
            with self.lock:
                return int(self.template["height"]) - 1
        if method == "getblockhash":
            return "00" * 32
        raise AssertionError(f"unexpected RPC method {method}")

    def advance_tip(self, new_tip_hex: str) -> None:
        with self.lock:
            template = dict(self.template)
            template["previousblockhash"] = new_tip_hex
            template["height"] = int(template["height"]) + 1
            template["curtime"] = int(time.time())
            self.template = template


def synthetic_manifest_coinbase_hex(suffix_hex: str) -> str:
    height_push = "03aabbcc"
    script_sig = height_push + suffix_hex
    output = (50_00000000).to_bytes(8, "little").hex() + "0151"
    return (
        "01000000"
        + "01"
        + "00" * 32
        + "ffffffff"
        + direct_stratum.compact_size(len(bytes.fromhex(script_sig))).hex()
        + script_sig
        + "ffffffff"
        + "01"
        + output
        + "00000000"
    )


def base_template(height: int) -> dict[str, object]:
    return {
        "height": height,
        "previousblockhash": "11" * 32,
        "bits": "1b00ffff",
        "version": 0x20000000,
        "curtime": int(time.time()),
        "coinbasevalue": 50_00000000,
        "transactions": [],
    }


def build_coordinator(args: argparse.Namespace, latencies: InjectedLatencies) -> tuple[PrismCoordinator, BenchRpc, BenchLedger]:
    server = PrismCoordinator.__new__(PrismCoordinator)
    template = base_template(height=1000)
    rpc = BenchRpc(template, latencies)
    ledger = BenchLedger(args.shares, latencies)
    server.rpc = rpc
    server.ledger = ledger
    server.qbit_chain = "testnet4"
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.clients = set()
    server.jobs = {}
    server.job_counter = 0
    server.connection_counter = 0
    server.accepted_block_count = 0
    server.max_blocks = 1_000_000_000
    server.started_monotonic = time.monotonic()
    server.submitted_share_count = 0
    server.stale_share_count = 0
    server.duplicate_share_count = 0
    server.low_difficulty_share_count = 0
    server.rejection_counts_by_reason = {reason: 0 for reason in PRISM_REJECTION_REASON_IDS}
    server.job_build_failure_count = 0
    server.tip_refresh_job_count = 0
    server.post_accept_refresh_failure_count = 0
    server.reorg_reconciler_enabled = True
    server.reorg_inactive_block_count = 0
    server.reorg_reactivated_block_count = 0
    server.reorg_reconcile_skip_count = 0
    server.reorg_reconcile_error_count = 0
    server.matured_payout_count = 0
    server.last_reorg_reconciled_tip_hash = None
    server.last_reorg_reconciled_trusted = False
    server.last_reorg_reconciled_monotonic = None
    server.latest_evidence = None
    server.latest_bundle = None
    server.tip_template_snapshot = None
    server.extranonce2_size = EXTRANONCE2_SIZE
    server.coinbase_tag_hex = default_prism_coinbase_tag_hex()
    server.share_difficulty = Decimal("1")
    server.vardiff_config = vardiff.VardiffConfig(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("0.000000001"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal("90"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("1"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )
    server.default_share_weight = 1
    server.share_weights_by_username = {}
    server.min_ready_miners = 1
    server.blockpoll_seconds = 2.0
    server.signing_seed_hex = "42" * 32
    server.ledger_attestation_signing_seed_hex = "43" * 32
    server._prism_payout_policy_cache = None
    server._ctv_fanout_market_fee_rate_cache = {}
    if args.mode == "cached":
        server.job_bundle_cache_seconds = args.bundle_ttl
        server.template_cache_seconds = args.template_ttl
        server.reorg_reconcile_cache_seconds = args.reorg_ttl
    else:
        server.job_bundle_cache_seconds = 0.0
        server.template_cache_seconds = 0.0
        server.reorg_reconcile_cache_seconds = 0.0
    server.health_refresh_seconds = 5.0
    server.stratum_send_timeout_seconds = 20.0
    server._ensure_job_cache_state()
    server._ensure_watchdog_state()
    server.watchdog_timeout_seconds = 3600.0

    if not args.real_builder:
        delay = args.fake_builder_delay

        def fake_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            time.sleep(delay)
            suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
            return {
                "found_block": dict(kwargs["found_block"]),
                "signed_coinbase_manifest": {
                    "manifest": {"coinbase_tx_hex": synthetic_manifest_coinbase_hex(suffix_hex)}
                },
            }

        server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
    return server, rpc, ledger


class BenchClientPool:
    def __init__(self, server: PrismCoordinator, count: int) -> None:
        self.server = server
        self.clients: list[ClientState] = []
        self.drains: list[threading.Thread] = []
        self.peer_sockets: list[socket.socket] = []
        for index in range(count):
            left, right = socket.socketpair()
            drain = threading.Thread(target=self._drain, args=(right,), daemon=True)
            drain.start()
            program = MINER_PROGRAMS[index % len(MINER_PROGRAMS)]
            worker = WorkerIdentity(
                username=f"bench-miner-{index % len(MINER_PROGRAMS)}.rig{index}",
                payout_address=f"bench-miner-{index % len(MINER_PROGRAMS)}",
                worker_name=f"rig{index}",
                script_pubkey_hex="5220" + program,
                p2mr_program_hex=program,
            )
            state = ClientState(
                sock=left,
                address=("127.0.0.1", 50_000 + index),
                connection_id=index + 1,
                extranonce1_hex=f"{index + 1:08x}",
            )
            state.subscribed = True
            state.authorized = True
            state.username = worker.username
            state.worker = worker
            # Spread desired difficulties so stamping is exercised per client.
            state.share_difficulty = Decimal(2 ** (index % 6))
            self.clients.append(state)
            self.peer_sockets.append(right)
            with server.lock:
                server.clients.add(state)

    @staticmethod
    def _drain(sock: socket.socket) -> None:
        try:
            while sock.recv(1 << 16):
                pass
        except OSError:
            pass

    def close(self) -> None:
        for state in self.clients:
            try:
                state.close()
            except OSError:
                pass
        for sock in self.peer_sockets:
            try:
                sock.close()
            except OSError:
                pass


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return float("nan")
    index = min(len(sorted_values) - 1, max(0, int(round(fraction * (len(sorted_values) - 1)))))
    return sorted_values[index]


def summarize(samples: list[float]) -> dict[str, object]:
    ordered = sorted(samples)
    return {
        "n": len(ordered),
        "p50": round(percentile(ordered, 0.50), 4),
        "p90": round(percentile(ordered, 0.90), 4),
        "p99": round(percentile(ordered, 0.99), 4),
        "max": round(ordered[-1], 4) if ordered else float("nan"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("cached", "uncached"), default="cached")
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--tip-flips", type=int, default=10)
    parser.add_argument("--shares", type=int, default=21_868, help="synthetic share window size")
    parser.add_argument("--real-builder", action="store_true", help="run the real qbit-prism bundle builder subprocess")
    parser.add_argument("--fake-builder-delay", type=float, default=1.0, help="simulated bundle builder seconds when not using --real-builder")
    parser.add_argument("--gbt-latency", type=float, default=0.017)
    parser.add_argument("--snapshot-latency", type=float, default=0.44)
    parser.add_argument("--stats-latency", type=float, default=0.02)
    parser.add_argument("--balances-latency", type=float, default=0.065)
    parser.add_argument("--reorg-watch-latency", type=float, default=0.057)
    parser.add_argument("--mark-mature-latency", type=float, default=0.07)
    parser.add_argument("--rpc-small-latency", type=float, default=0.002)
    parser.add_argument("--bundle-ttl", type=float, default=10.0)
    parser.add_argument("--template-ttl", type=float, default=2.0)
    parser.add_argument("--reorg-ttl", type=float, default=5.0)
    parser.add_argument("--output-json", type=str, default="")
    args = parser.parse_args()

    sanitize_environment()
    latencies = InjectedLatencies(
        gbt_seconds=args.gbt_latency,
        snapshot_seconds=args.snapshot_latency,
        stats_seconds=args.stats_latency,
        balances_seconds=args.balances_latency,
        reorg_watch_seconds=args.reorg_watch_latency,
        mark_mature_seconds=args.mark_mature_latency,
        rpc_small_seconds=args.rpc_small_latency,
    )
    server, rpc, _ledger = build_coordinator(args, latencies)

    samples: list[float] = []
    phase_totals: dict[str, float] = {}
    original_observe = server.observe_job_build_elapsed

    def observing(elapsed_seconds: float, phases: dict[str, float]) -> None:
        samples.append(elapsed_seconds)
        for phase, duration in phases.items():
            phase_totals[phase] = phase_totals.get(phase, 0.0) + duration
        original_observe(elapsed_seconds, phases)

    server.observe_job_build_elapsed = observing  # type: ignore[method-assign]

    pool = BenchClientPool(server, args.clients)
    flip_wall_times: list[float] = []
    try:
        # Coordinator job logs go to stderr so stdout stays a clean JSON report.
        with contextlib.redirect_stdout(sys.stderr):
            # Initial poll acts as the join storm: every client needs a first job.
            for flip in range(args.tip_flips + 1):
                if flip > 0:
                    rpc.advance_tip(f"{flip:064x}")
                started = time.monotonic()
                refreshed = server.poll_qbit_tip_template_once()
                flip_wall_times.append(time.monotonic() - started)
                if refreshed != args.clients:
                    print(
                        f"benchmark: warning flip={flip} refreshed {refreshed} of {args.clients} clients",
                        file=sys.stderr,
                    )
    finally:
        pool.close()

    report = {
        "schema": "qbit.prism.job-build-benchmark.v1",
        "mode": args.mode,
        "clients": args.clients,
        "tip_flips": args.tip_flips,
        "share_window": args.shares,
        "real_builder": bool(args.real_builder),
        "injected_latencies": latencies.__dict__,
        "sent_job_elapsed_seconds": summarize(samples),
        "full_refresh_wall_seconds": summarize(flip_wall_times),
        "phase_totals_seconds": {k: round(v, 3) for k, v in sorted(phase_totals.items())},
        "cache_hits": dict(server.job_cache_hit_counts),
        "cache_misses": dict(server.job_cache_miss_counts),
        "getblocktemplate_calls": rpc.calls.get("getblocktemplate", 0),
    }
    text = json.dumps(report, indent=2)
    print(text)
    if args.output_json:
        Path(args.output_json).write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
