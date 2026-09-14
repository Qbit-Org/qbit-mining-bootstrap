#!/usr/bin/env python3
"""Summarize prism-recovery-evidence.sql JSONL without loading share history.

Usage: python3 scripts/prism-recovery-evidence.py source.rows.jsonl
No database access or mutation. The carry head is byte-compatible with 2.x's
_carry_forward_audit_head_locked (v2.0.2, 504846cc), including ASCII escaping.
"""

import argparse
import hashlib
import json
from pathlib import Path


def summarize(lines):
    kinds = (
        "shares", "share_sequence", "share_hashes", "blocks", "audits", "audit_bodies", "audit_snapshots",
        "carry", "payouts", "candidates", "candidate_balances",
        "ctv_sets", "ctv_artifacts", "ctv_checkpoints", "ctv_broadcast_attempts",
        "cpfp_packages", "cpfp_retired_funding", "deferred_shares",
        "fatal_state", "fatal_state_events", "active_carry",
    )
    hashes = {kind: hashlib.sha256() for kind in kinds}
    counts = dict.fromkeys(kinds, 0)
    head = bytes(32)
    last_share_seq = 0
    accepted = 0
    pending = 0
    integrity = None
    complete = False
    for line in lines:
        if complete:
            raise ValueError("records follow completion marker")
        record = json.loads(line)
        kind, row = record["kind"], record["row"]
        if kind == "complete":
            if row is not True or integrity is None:
                raise ValueError("invalid completion marker or missing integrity report")
            complete = True
        elif kind == "integrity":
            if integrity is not None:
                raise ValueError("duplicate integrity report")
            integrity = row
        elif kind in hashes:
            if integrity is not None:
                raise ValueError("accounting records follow integrity report")
            canonical = json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
            hashes[kind].update(canonical + b"\n")
            counts[kind] += 1
            if kind == "active_carry":
                head = hashlib.sha256(head + canonical).digest()
            elif kind == "shares":
                if row["share_seq"] <= last_share_seq:
                    raise ValueError("share sequence is not strictly increasing")
                last_share_seq = row["share_seq"]
                accepted += int(row["accepted"])
            elif kind == "candidates":
                pending += int(row["state"] == "pending")
        else:
            raise ValueError(f"unknown evidence kind: {kind}")
    if not complete:
        raise ValueError("incomplete export; psql must finish successfully")
    for field in ("mismatch_count", "current_drift_count"):
        if integrity.get(field) != 0:
            raise ValueError(f"carry-forward integrity failure: {field}")
    if integrity.get("checked_active_rows") != counts["active_carry"]:
        raise ValueError("active carry count differs from integrity report")
    return {
        "schema": "qbit.prism.recovery-evidence.v1",
        "records": {kind: {"count": counts[kind], "sha256": hashes[kind].hexdigest()}
                    for kind in kinds},
        "accepted_shares": accepted,
        "last_share_seq": last_share_seq,
        "pending_candidates": pending,
        "audit_chain_version": "qbit.prism.carry-forward-active-delta-chain.v1",
        "audit_head_sha256": head.hex(),
        "carry_forward_integrity": integrity,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    try:
        with args.evidence.open(encoding="utf-8") as source:
            report = summarize(source)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"recovery evidence failed: {error}\n")
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
