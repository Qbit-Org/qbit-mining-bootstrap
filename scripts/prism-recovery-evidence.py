#!/usr/bin/env python3
"""Summarize prism-recovery-evidence.sql JSONL without loading share history.

Usage: python3 scripts/prism-recovery-evidence.py source.rows.jsonl
No database access or mutation. The carry head is byte-compatible with 2.x's
_carry_forward_audit_head_locked (v2.0.2, 504846cc), including ASCII escaping.

Carry integrity doesn't depend on which validator the database ran (#708):
- Rows landed before migration 011, on unmarked blocks, are re-checked here as
  one running chain per payout program, as 2.x keeps their balances.
- A finding the database reports on such a row must be one its per-label rule
  (2.x's, and 011's) also finds there; the per-program chain clears it.
- Findings on marked (as-issued) blocks come from the database's manifest
  rule, which this export can't repeat, and fail as before.
- The summary keeps the database's report, less the findings cleared, so a 2.x
  source and its migrated copy summarize alike.
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

INTEGER = re.compile(r"-?[0-9]+")
BALANCE_FIELDS = ("prior_balance", "candidate_balance", "carry_forward_balance")


def _amount(row, field):
    value = row[field]
    if isinstance(value, bool) or not isinstance(value, (int, str)) or (
            isinstance(value, str) and not INTEGER.fullmatch(value)):
        raise ValueError(f"active carry {field} is not an integer amount")
    return int(value)


def _off_chain(row, prior, gross, onchain):
    """Whether a row's stored balances differ from a chain at `prior`."""
    expected = (prior, prior + gross, prior + gross - onchain)
    return any(_amount(row, f"{field}_sats") != value
               for field, value in zip(BALANCE_FIELDS, expected))


def summarize(lines, notes=None):
    kinds = (
        "shares", "share_sequence", "sequences", "share_hashes", "blocks", "audits", "audit_bodies", "audit_snapshots",
        "carry", "payouts", "candidates", "candidate_balances",
        "ctv_sets", "ctv_artifacts", "ctv_checkpoints", "ctv_retry_progress",
        "ctv_broadcast_attempts",
        "cpfp_packages", "cpfp_retired_funding", "deferred_shares",
        "fatal_state", "fatal_state_events", "policy_transitions", "chain_checkpoint", "cluster_config",
        "payout_revision", "ledger_clock", "active_carry",
    )
    hashes = {kind: hashlib.sha256() for kind in kinds}
    counts = dict.fromkeys(kinds, 0)
    head = bytes(32)
    last_share_seq = 0
    accepted = 0
    pending = 0
    unfinished = 0
    integrity = None
    complete = False
    marked_blocks = set()
    carry_started = False
    carry_order = None
    # Running (gross - onchain) over earlier active rows, marked ones
    # included, as the SQL rules sum it: per program (025) and per label.
    by_program = {}
    by_label = {}
    legacy_breaks = 0
    label_breaks = set()
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
            if kind == "blocks":
                if carry_started:
                    raise ValueError("block records follow active carry records")
                if row.get("as_issued_audit_sha256") is not None:
                    marked_blocks.add(row["block_hash"])
            elif kind == "active_carry":
                carry_started = True
                head = hashlib.sha256(head + canonical).digest()
                order = (row["block_height"], row["carry_forward_seq"])
                if carry_order is not None and order <= carry_order:
                    raise ValueError("active carry records are not in chain order")
                carry_order = order
                program = row["p2mr_program_hex"]
                label = (row["recipient_id"], row["order_key"], program)
                gross = _amount(row, "gross_amount_sats")
                onchain = _amount(row, "onchain_amount_sats")
                program_prior = by_program.get(program, 0)
                label_prior = by_label.get(label, 0)
                if row["block_hash"] not in marked_blocks:
                    if _off_chain(row, program_prior, gross, onchain):
                        legacy_breaks += 1
                    if _off_chain(row, label_prior, gross, onchain):
                        label_breaks.add(row["carry_forward_seq"])
                by_program[program] = program_prior + gross - onchain
                by_label[label] = label_prior + gross - onchain
            elif kind == "shares":
                if row["share_seq"] <= last_share_seq:
                    raise ValueError("share sequence is not strictly increasing")
                last_share_seq = row["share_seq"]
                accepted += int(row["accepted"])
            elif kind == "candidates":
                pending += int(row["state"] == "pending")
                # Unknown states must not make a drained-work check pass.
                # `orphaned` (migration 015) is terminal and keeps its evidence.
                unfinished += int(row["state"] not in ("submitted", "abandoned", "orphaned"))
        else:
            raise ValueError(f"unknown evidence kind: {kind}")
    if not complete:
        raise ValueError("incomplete export; psql must finish successfully")
    if not isinstance(integrity, dict):
        raise ValueError("carry-forward integrity report is not an object")
    for field in ("mismatch_count", "current_drift_count", "checked_active_rows"):
        value = integrity.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"carry-forward integrity report lacks {field}")
    if integrity["current_drift_count"] != 0:
        raise ValueError("carry-forward integrity failure: current_drift_count")
    if integrity["checked_active_rows"] != counts["active_carry"]:
        raise ValueError("active carry count differs from integrity report")
    mismatches = integrity.get("mismatches", [])
    if not isinstance(mismatches, list) or len(mismatches) != integrity["mismatch_count"]:
        raise ValueError("carry-forward integrity report does not list every mismatch")
    if not all(isinstance(finding, dict) for finding in mismatches):
        raise ValueError("carry-forward integrity report lists a mismatch that is not an object")
    # Block, manifest and payout findings (no carry row) are as-issued only.
    as_issued = [finding for finding in mismatches
                 if finding.get("carry_forward_seq") is None
                 or finding.get("block_hash") in marked_blocks]
    if as_issued:
        raise ValueError(f"carry-forward integrity failure: {len(as_issued)} as-issued finding(s)")
    if legacy_breaks:
        raise ValueError(
            f"carry-forward integrity failure: {legacy_breaks} legacy row(s) break their payout program's chain")
    # What is left are legacy findings. Each must be the per-label rule's
    # (2.x's, and 011's), on a row whose program chain holds; any other is
    # a rule this summary does not know, and fails.
    unexplained = [finding for finding in mismatches
                   if finding["carry_forward_seq"] not in label_breaks]
    if unexplained:
        raise ValueError(
            f"carry-forward integrity failure: {len(unexplained)} legacy finding(s) no chain rule explains")
    if notes is not None and mismatches:
        notes.append(
            f"the database's per-label legacy carry rule reported {len(mismatches)} finding(s) that each "
            "payout program's chain clears: a program paid under more than one label (#708)")
    return {
        "schema": "qbit.prism.recovery-evidence.v1",
        "records": {kind: {"count": counts[kind], "sha256": hashes[kind].hexdigest()}
                    for kind in kinds},
        "accepted_shares": accepted,
        "last_share_seq": last_share_seq,
        "pending_candidates": pending,
        "unfinished_candidates": unfinished,
        "audit_chain_version": "qbit.prism.carry-forward-active-delta-chain.v1",
        "audit_head_sha256": head.hex(),
        # The database's report, less the findings cleared above: a clean
        # report is unchanged.
        "carry_forward_integrity": dict(integrity, mismatch_count=0, mismatches=[]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    notes = []
    try:
        with args.evidence.open(encoding="utf-8") as source:
            report = summarize(source, notes)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        parser.exit(1, f"recovery evidence failed: {error}\n")
    for note in notes:
        print(f"note: {note}", file=sys.stderr)
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
