"""Export the 2.x.x bootstrap and below-target credit rule decisions.

Run from the root of a 2.x.x source tree (see README.md in this directory).
Each scenario is fed through the real 2.x.x Python decision code with
in-memory stand-ins for the ledger and bundle builder, so no database or
psycopg import is needed. The JSON printed on stdout is consumed by
export_money_path_vectors.rs, which computes the payout consequence of every
decision with the qbit-prism engine.

Facts that the 2.x.x code only establishes against a live node or database
(credit timing, what happens after a reorg) are recorded as cited rule-table
entries rather than executed.
"""

from __future__ import annotations

import json
import sys
import threading
import types
from typing import Any

from lab.prism.coordinator_config import env_int
from lab.prism.job_bundle import JobBundleService
from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism.share_submission import SubmitRejected, classify_submit_work
from lab.prism.template_artifacts import scaled_target_difficulty, target_from_compact

POW_LIMIT_TARGET = target_from_compact("207fffff")


def program(byte: int) -> str:
    return f"{byte:02x}" * 32


def ledger_share(
    share_seq: int, miner: str, byte: int, difficulty: int, at_ms: int, network: int
) -> dict[str, Any]:
    return {
        "share_seq": share_seq,
        "share_id": f"share-{share_seq}",
        "miner_id": miner,
        "order_key": miner,
        "p2mr_program_hex": program(byte),
        "share_difficulty": difficulty,
        "network_difficulty": network,
        "template_height": 100,
        "job_id": f"job-{at_ms}",
        "job_issued_at_ms": at_ms,
        "accepted_at_ms": at_ms,
        "ntime": 1_800_000_000,
    }


# ---------------------------------------------------------------------------
# Topic 8: bootstrap transition


class _Ledger:
    """Share ledger without an aggregate query: accepted_share_stats falls back
    to distinct miner_id over all_shares, as the in-memory 2.x.x ledger does."""

    def __init__(self, shares: list[dict[str, Any]]) -> None:
        self._shares = [types.SimpleNamespace(**share) for share in shares]

    def all_shares(self) -> list[Any]:
        return list(self._shares)


class _CaptureBundleRuntime:
    def build_audit_bundle(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs


def bootstrap_decision(scenario: dict[str, Any]) -> dict[str, Any]:
    coordinator = types.SimpleNamespace(
        ledger=_Ledger(scenario["ledger_shares"]),
        min_ready_miners=scenario["min_ready_miners"],
        lock=threading.Lock(),
        _pool_ready_latched=False,
        _retained_collection_refresh=None,
        _progress_note_refresh_pending=lambda: None,
    )
    coordinator.accepted_share_stats = types.MethodType(
        PrismCoordinator.accepted_share_stats, coordinator
    )
    coordinator.pool_readiness_latched = types.MethodType(
        PrismCoordinator.pool_readiness_latched, coordinator
    )
    stats = coordinator.accepted_share_stats()
    service = object.__new__(JobBundleService)
    service._runtime = coordinator
    mode = JobBundleService._job_bundle_mode(service, None)

    template = scenario["template"]
    solver = scenario["solver"]
    if mode == "collection":
        capture = object.__new__(JobBundleService)
        capture._runtime = _CaptureBundleRuntime()
        captured = JobBundleService.build_collection_bundle(
            capture,
            template=template,
            transaction_hexes=(),
            worker=types.SimpleNamespace(
                payout_address=solver["payout_address"],
                p2mr_program_hex=solver["p2mr_program_hex"],
            ),
            network_difficulty=scenario["network_difficulty"],
            issued_at_ms=scenario["anchor_ms"],
            suffix_hex="",
        )
        bundle = {
            "shares": captured["shares"],
            "found_block": captured["found_block"],
            "prior_balances": captured["prior_balances"],
        }
        built_by = "lab/prism/job_bundle.py:3550 build_collection_bundle (executed)"
    else:
        # The ready path builds from the ledger window and the published
        # prior balances (lab/prism/job_bundle.py:3405-3408); it needs the
        # ledger service, so its economic inputs are restated here.
        bundle = {
            "shares": scenario["ledger_shares"],
            "found_block": {
                "block_height": int(template["height"]),
                "coinbase_value_sats": int(template["coinbasevalue"]),
                "network_difficulty": scenario["network_difficulty"],
                "anchor_job_issued_at_ms": scenario["anchor_ms"],
            },
            "prior_balances": scenario["prior_balances"],
        }
        built_by = "lab/prism/job_bundle.py:3405 ready build (ledger window and prior balances)"
    return {
        "accepted_share_count": stats[0],
        "distinct_miner_count": stats[1],
        "pool_readiness_latched": coordinator._pool_ready_latched,
        "job_bundle_mode": mode,
        "bundle_built_by": built_by,
        "implemented_at": [
            "lab/prism/prism_coordinator.py:7778 pool_readiness_latched",
            "lab/prism/job_bundle.py:2132 _job_bundle_mode",
            "lab/prism/job_bundle.py:3589 prior_balances=[] in build_collection_bundle",
        ],
        "bundle": bundle,
    }


def bootstrap_cases() -> list[dict[str, Any]]:
    network = 100
    anchor = 5_000
    min_ready = env_int("PRISM_MIN_READY_MINERS", 3, environ={})
    template = {
        "height": 101,
        "coinbasevalue": 500_000_000,
        "curtime": 1_800_000_000,
        "previousblockhash": "00" * 32,
    }
    solver_b = {"payout_address": "miner-b", "p2mr_program_hex": program(2)}
    two_miners = [
        ledger_share(1, "miner-a", 1, 30, 4_000, network),
        ledger_share(2, "miner-b", 2, 20, 4_001, network),
    ]
    three_miners = two_miners + [ledger_share(3, "miner-c", 3, 50, 4_002, network)]
    carry_only = [
        {
            "recipient_id": "miner-old",
            "order_key": "miner-old",
            "p2mr_program_hex": program(9),
            "balance_sats": 20_000,
        }
    ]
    scenarios = [
        {
            "name": "below-gate-with-other-miners-shares",
            "why": (
                "Two distinct miners have accepted shares, below the default "
                "readiness gate of three. 2.x.x issues a collection job that "
                "pays the solver the whole coinbase; 3.x.x uses the normal "
                "window as soon as any share exists."
            ),
            "ledger_shares": two_miners,
            "prior_balances": [],
        },
        {
            "name": "at-readiness-gate",
            "why": (
                "Three distinct miners reach the gate, so 2.x.x latches ready "
                "and both versions build the normal window with prior balances."
            ),
            "ledger_shares": three_miners,
            "prior_balances": carry_only,
        },
        {
            "name": "empty-ledger",
            "why": (
                "No shares and no balances: both versions pay the solver the "
                "whole coinbase through the synthetic bootstrap share."
            ),
            "ledger_shares": [],
            "prior_balances": [],
        },
        {
            "name": "bootstrap-carry-only-account-at-or-above-floor",
            "why": (
                "An empty share window with a carry-only account whose carry "
                "(20000 sats) is above the 14720-sat floor. 2.x.x drops prior "
                "balances from the collection bundle, so the account is not paid "
                "in this block; 3.x.x keeps the snapshot's prior balances in "
                "bootstrap and pays it."
            ),
            "ledger_shares": [],
            "prior_balances": carry_only,
        },
    ]
    cases = []
    for scenario in scenarios:
        full = {
            "min_ready_miners": min_ready,
            "network_difficulty": network,
            "anchor_ms": anchor,
            "template": template,
            "solver": solver_b,
            "ledger_shares": scenario["ledger_shares"],
            "prior_balances": scenario["prior_balances"],
        }
        cases.append(
            {
                "name": scenario["name"],
                "why": scenario["why"],
                "scenario": full,
                "decision_2xx": bootstrap_decision(full),
            }
        )
    return cases


# ---------------------------------------------------------------------------
# Topic 9: below-target block credit


def accepted_share_difficulty(share_target: int) -> int:
    coordinator = types.SimpleNamespace(share_weights_by_username={})
    context = types.SimpleNamespace(
        worker=types.SimpleNamespace(username="miner-a.rig", payout_address="miner-a"),
        job=types.SimpleNamespace(share_target=share_target),
    )
    return PrismCoordinator.accepted_share_difficulty(coordinator, context)


# Node and chain outcomes, with what 2.x.x does after the submit route. These
# paths run against the node and the database, so they are cited, not run.
OUTCOMES_2XX = {
    "accepted": {
        "credited": True,
        "credited_at": "node acceptance of the block (synchronous submit)",
        "implemented_at": [
            "lab/prism/prism_coordinator.py:8155 _submit_synchronous_credit_candidate",
            "lab/prism/block_finalization.py:2027 append_accepted_share when credit_share_on_accept",
        ],
    },
    "confirmed-after-reconciliation": {
        "credited": True,
        "credited_at": "node acceptance of the block; later confirmation adds nothing",
        "implemented_at": [
            "lab/prism/block_finalization.py:2027 append_accepted_share when credit_share_on_accept",
        ],
    },
    "reorged-after-acceptance": {
        "credited": True,
        "credited_at": "node acceptance; the share row survives the disconnect",
        "implemented_at": [
            "lab/prism/block_finalization.py:2027 append_accepted_share when credit_share_on_accept",
            "lab/prism/accepted_preview_telemetry.py:148 no reorg path updates or deletes qbit_share_ledger",
        ],
    },
    "rejected": {
        "credited": False,
        "credited_at": None,
        "implemented_at": [
            "lab/prism/prism_coordinator.py:8221 block_landed false rejects low difficulty share",
        ],
    },
}


def credit_decision(scenario: dict[str, Any]) -> dict[str, Any]:
    submission = types.SimpleNamespace(
        share_pass=scenario["share_pass"],
        block_pass=scenario["block_pass"],
        header_hex="00" * 80,
    )
    context = types.SimpleNamespace(worker=types.SimpleNamespace(username="miner-a.rig"))
    assigned = accepted_share_difficulty(scenario["share_target"])
    network = scaled_target_difficulty(scenario["network_target"])
    try:
        decision = classify_submit_work(context, submission, credit_policy=None)
    except SubmitRejected as rejected:
        return {
            "route": None,
            "rejection": {"code": rejected.code, "reason": rejected.reason},
            "assigned_share_difficulty": assigned,
            "network_difficulty": network,
            "credited": False,
            "credited_difficulty": None,
            "credited_at": None,
            "implemented_at": ["lab/prism/share_submission.py:360 classify_submit_work"],
        }
    outcome = OUTCOMES_2XX[scenario["node_outcome"]]
    credited = decision.route != "synchronous_block" or outcome["credited"]
    return {
        "route": decision.route,
        "block_worthy": decision.block_worthy,
        "credit_share_on_accept": decision.credit_share_on_accept,
        "rejection": None,
        "assigned_share_difficulty": assigned,
        "network_difficulty": network,
        "credited": credited,
        # 2.x.x stamps every accepted share, synchronous or not, with the
        # assigned share difficulty (lab/prism/share_writer.py:281).
        "credited_difficulty": assigned if credited else None,
        "credited_at": (
            outcome["credited_at"]
            if decision.route == "synchronous_block"
            else "share acceptance (share passed its target)"
        ),
        "implemented_at": [
            "lab/prism/share_submission.py:360 classify_submit_work",
            "lab/prism/share_writer.py:281 runtime.accepted_share_difficulty(context)",
            "lab/prism/prism_coordinator.py:8410 accepted_share_difficulty",
        ]
        + (outcome["implemented_at"] if decision.route == "synchronous_block" else []),
    }


def credit_cases() -> list[dict[str, Any]]:
    # Network target at the pow limit (difficulty 1_000_000); the listener
    # floor holds the share target four times harder (difficulty 4_000_000).
    network_target = POW_LIMIT_TARGET
    share_target = POW_LIMIT_TARGET * 1_000_000 // 4_000_000
    next_network = scaled_target_difficulty(network_target)
    ledger = [
        ledger_share(1, "miner-b", 2, 2_000_000, 9_000, next_network),
        ledger_share(2, "miner-b", 2, 2_000_000, 9_001, next_network),
        ledger_share(3, "miner-c", 3, 1_000_000, 9_002, next_network),
    ]
    common = {
        "share_target": share_target,
        "network_target": network_target,
        "solver": {"payout_address": "miner-a", "p2mr_program_hex": program(1)},
        "ledger_shares": ledger,
        "credited_share": {
            "share_seq": 4,
            "share_id": "miner-a.rig:" + "ab" * 32,
            "job_issued_at_ms": 9_003,
            "accepted_at_ms": 9_003,
        },
        "next_block": {
            "block_height": 202,
            "coinbase_value_sats": 500_000_000,
            "network_difficulty": next_network,
            "anchor_job_issued_at_ms": 9_010,
        },
    }
    scenarios = [
        (
            "block-only-proof-accepted",
            "The hash meets the network target but not the floor-raised share "
            "target, and the node accepts the block.",
            False,
            True,
            "accepted",
        ),
        (
            "block-only-proof-confirmed-after-reconciliation",
            "The node accepted the block but the durable acknowledgement was "
            "lost; reconciliation later finds it on the active chain.",
            False,
            True,
            "confirmed-after-reconciliation",
        ),
        (
            "block-only-proof-reorged-after-acceptance",
            "The block was accepted and credited, then disconnected by a reorg; "
            "the next block's window is built afterwards.",
            False,
            True,
            "reorged-after-acceptance",
        ),
        (
            "block-only-proof-rejected",
            "The node rejects the block, or it is never on the active chain: "
            "no credit on either version.",
            False,
            True,
            "rejected",
        ),
        (
            "share-and-block-proof-control",
            "Control: the hash also meets the share target, so both versions "
            "credit the assigned share difficulty.",
            True,
            True,
            "accepted",
        ),
    ]
    cases = []
    for name, why, share_pass, block_pass, node_outcome in scenarios:
        scenario = dict(common)
        scenario.update(
            {"share_pass": share_pass, "block_pass": block_pass, "node_outcome": node_outcome}
        )
        cases.append(
            {
                "name": name,
                "why": why,
                "scenario": scenario,
                "decision_2xx": credit_decision(scenario),
            }
        )
    return cases


def main() -> int:
    json.dump(
        {"bootstrap": bootstrap_cases(), "below_target_credit": credit_cases()},
        sys.stdout,
        sort_keys=True,
        separators=(",", ":"),
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
