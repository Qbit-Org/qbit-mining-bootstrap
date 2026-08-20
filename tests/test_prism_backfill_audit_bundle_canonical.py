#!/usr/bin/env python3

import copy
import hashlib
import io
import json
import re
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest import mock

from lab.prism.audit_artifacts import AuditArtifactConfig, AuditArtifactStore
from lab.prism.backfill_audit_bundle_canonical import (
    LEDGER_PREFLIGHT_CANDIDATES,
    AuditBundleLedgerAdapter,
    BackfillWriterLeaseUnavailable,
    CanonicalArtifactAdapter,
    CanonicalBundleBackfill,
    DEFAULT_BATCH_SIZE,
    MissingCanonicalCapability,
    audit_bundle_page_sql,
    build_audit_store_from_env,
    build_parser,
    ledger_from_args,
    main,
)
from lab.prism.share_ledger import PsqlShareLedger


AUDIT_SHARE_SEGMENT_SCHEMA = "qbit.prism.audit-share-segment.v1"


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_bytes(bundle: dict[str, Any]) -> bytes:
    """Stand-in for J1's canonicalizer: deterministic and order-independent."""

    return json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest_of(bundle: dict[str, Any]) -> str:
    return sha256_hex(canonical_bytes(bundle))


def block_hash(index: int) -> str:
    return f"{index:064x}"


def share(seq: int) -> dict[str, Any]:
    return {
        "share_seq": seq,
        "share_id": f"share-{seq}",
        "miner_id": f"miner-{seq % 3}",
        "share_difficulty": "1024",
    }


def bundle_for(hash_hex: str, shares: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema": "qbit.prism.audit-bundle",
        "block_hash": hash_hex,
        "found_block": {"block_hash": hash_hex, "block_height": 4242},
        "shares": shares,
    }


@dataclass
class ExternalBody:
    """A compact/v2 external body as the artifact store would resolve it."""

    bundle_without_shares: dict[str, Any]
    shares_key: str
    parts: list[dict[str, Any]] = field(default_factory=list)


class FakeArtifactStore:
    """The assumed phase-2 canonical artifact surface, in memory."""

    def __init__(self, journal: list[tuple[Any, ...]]) -> None:
        self.journal = journal
        self.canonical: dict[tuple[str, str], bytes] = {}
        self.literal_bodies: dict[str, bytes] = {}
        self.external_bodies: dict[str, ExternalBody] = {}
        self.canonicalized: list[dict[str, Any]] = []
        self.write_failures: set[tuple[str, str]] = set()

    # -- phase-1 surface, unchanged ------------------------------------

    def canonical_audit_bundle_bytes(self, bundle: dict[str, Any]) -> bytes:
        self.canonicalized.append(copy.deepcopy(bundle))
        return canonical_bytes(bundle)

    def storage_json_bytes(self, payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def audit_share_segment_payload(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> dict[str, Any]:
        return {
            "schema": AUDIT_SHARE_SEGMENT_SCHEMA,
            "first_share_seq": first_share_seq,
            "last_share_seq": last_share_seq,
            "share_count": len(shares),
            "shares": list(shares),
        }

    # -- assumed phase-2 surface ---------------------------------------

    def read_canonical_audit_bundle(self, block_hash_hex: str, digest: str) -> bytes | None:
        self.journal.append(("read_canonical", block_hash_hex, digest))
        return self.canonical.get((block_hash_hex, digest))

    def write_canonical_audit_bundle(
        self,
        block_hash_hex: str,
        digest: str,
        payload: bytes,
    ) -> None:
        self.journal.append(("write_canonical", block_hash_hex, digest))
        if (block_hash_hex, digest) in self.write_failures:
            raise RuntimeError("canonical artifact write refused")
        self.canonical[(block_hash_hex, digest)] = bytes(payload)

    def read_literal_audit_body(
        self,
        body_uri: object,
        *,
        expected_sha256: str,
    ) -> bytes | None:
        self.journal.append(("read_literal", str(body_uri)))
        payload = self.literal_bodies.get(str(body_uri))
        if payload is None:
            return None
        # A verified reader declines bytes that are not the advertised ones.
        return payload if sha256_hex(payload) == expected_sha256 else None

    def reconstruct_external_audit_body(
        self,
        body_uri: object,
        *,
        expected_sha256: str,
        load_missing_range: Any,
    ) -> dict[str, Any]:
        self.journal.append(("reconstruct", str(body_uri)))
        body = self.external_bodies.get(str(body_uri))
        if body is None:
            raise RuntimeError(f"no external audit body at {body_uri}")
        shares: list[Any] = []
        for part in body.parts:
            if part.get("on_disk", True):
                shares.extend(copy.deepcopy(part["shares"]))
                continue
            declared = {
                key: value
                for key, value in part.items()
                if key not in {"on_disk", "shares"}
            }
            shares.extend(
                load_missing_range(
                    first_share_seq=part["first_share_seq"],
                    last_share_seq=part["last_share_seq"],
                    part=declared,
                )
            )
        assembled = dict(body.bundle_without_shares)
        assembled[body.shares_key] = shares
        return assembled


class FakeLedger:
    """The PostgreSQL surface the backfill depends on, in memory."""

    def __init__(
        self,
        journal: list[tuple[Any, ...]],
        rows: list[dict[str, Any]],
        shares: dict[int, dict[str, Any]] | None = None,
    ) -> None:
        self.journal = journal
        self.rows = {str(row["block_hash"]): row for row in rows}
        self.shares = shares or {}
        self.page_requests: list[tuple[str | None, int]] = []
        self.preflight_failures: set[tuple[str, str]] = set()
        self.preflight_results: dict[tuple[str, str], Any] = {}

    @staticmethod
    def _text_literal(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _run_read_json(self, sql: str) -> list[dict[str, Any]]:
        if "FROM qbit_pool_audit_bundles" not in sql:
            raise AssertionError("page query does not read the audit bundle table")
        if "ORDER BY block_hash ASC" not in sql:
            raise AssertionError("page query is not ordered by the primary key")
        limit_match = re.search(r"LIMIT (\d+)", sql)
        if limit_match is None:
            raise AssertionError("page query is not bounded")
        limit = int(limit_match.group(1))
        cursor_match = re.search(r"WHERE block_hash > '([^']*)'", sql)
        start_after = cursor_match.group(1) if cursor_match else None
        self.page_requests.append((start_after, limit))
        keys = sorted(self.rows)
        if start_after is not None:
            keys = [key for key in keys if key > start_after]
        return [copy.deepcopy(self.rows[key]) for key in keys[:limit]]

    def load_audit_share_ledger_range(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
    ) -> list[dict[str, Any]]:
        self.journal.append(("load_share_range", first_share_seq, last_share_seq))
        return [
            copy.deepcopy(self.shares[seq])
            for seq in range(first_share_seq, last_share_seq + 1)
            if seq in self.shares
        ]

    def preflight_audit_bundle_publication(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
    ) -> Any:
        self.journal.append(("preflight", block_hash, audit_bundle_sha256))
        if (block_hash, audit_bundle_sha256) in self.preflight_failures:
            raise RuntimeError("writer lease is not active")
        return self.preflight_results.get(
            (block_hash, audit_bundle_sha256),
            {"confirmed": True},
        )


def inline_row(index: int, *, digest: str | None = None) -> dict[str, Any]:
    hash_hex = block_hash(index)
    bundle = bundle_for(hash_hex, [share(index)])
    return {
        "block_hash": hash_hex,
        "audit_bundle_sha256": digest or digest_of(bundle),
        "body_uri": None,
        "audit_bundle": bundle,
    }


class BackfillHarness:
    """Wire fakes to the backfill under test and expose what each run did."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        shares: dict[int, dict[str, Any]] | None = None,
    ) -> None:
        self.journal: list[tuple[Any, ...]] = []
        self.store = FakeArtifactStore(self.journal)
        self.ledger = FakeLedger(self.journal, rows, shares)
        self.stderr = io.StringIO()

    def run(self, **kwargs: Any) -> dict[str, Any]:
        backfill = CanonicalBundleBackfill(
            artifacts=CanonicalArtifactAdapter(self.store),
            ledger=AuditBundleLedgerAdapter(self.ledger),
            stderr=self.stderr,
            **kwargs,
        )
        return backfill.run()

    def publication_events(self) -> list[tuple[Any, ...]]:
        return [
            event
            for event in self.journal
            if event[0] in {"preflight", "write_canonical"}
        ]

    def errors(self) -> str:
        return self.stderr.getvalue()


class CanonicalBundleBackfillTest(unittest.TestCase):
    def test_inline_history_publishes_through_the_real_phase_2_store(self) -> None:
        row = inline_row(1)
        journal: list[tuple[Any, ...]] = []
        ledger = FakeLedger(journal, [row])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = AuditArtifactStore(
                AuditArtifactConfig(
                    root=root,
                    evidence_path=root / "evidence.json",
                ),
                canonicalizer=canonical_bytes,
            )
            try:
                result = CanonicalBundleBackfill(
                    artifacts=CanonicalArtifactAdapter(store),
                    ledger=AuditBundleLedgerAdapter(ledger),
                ).run()

                self.assertEqual(result["written"], 1)
                self.assertEqual(
                    store.read_canonical_audit_bundle(
                        str(row["block_hash"]),
                        str(row["audit_bundle_sha256"]),
                    ),
                    canonical_bytes(row["audit_bundle"]),
                )
                self.assertEqual(
                    journal[-1],
                    (
                        "preflight",
                        row["block_hash"],
                        row["audit_bundle_sha256"],
                    ),
                )
            finally:
                store.close()

    def test_first_run_publishes_every_shape_and_rerun_is_idempotent(self) -> None:
        inline = inline_row(1)

        literal_hash = block_hash(2)
        # Deliberately not what any canonicalizer here would emit: promotion
        # must copy these bytes, not re-derive them.
        literal_payload = b'{ "legacy": true, "block_hash": "' + literal_hash.encode() + b'" }\n'
        literal = {
            "block_hash": literal_hash,
            "audit_bundle_sha256": sha256_hex(literal_payload),
            "body_uri": f"prism-audit-bundle-body-{literal_hash}.json",
            "audit_bundle": None,
        }

        compact_hash = block_hash(3)
        compact_bundle = bundle_for(compact_hash, [share(7), share(8)])
        compact = {
            "block_hash": compact_hash,
            "audit_bundle_sha256": digest_of(compact_bundle),
            "body_uri": f"prism-audit-bundle-body-{compact_hash}.json",
            "audit_bundle": None,
        }

        harness = BackfillHarness([inline, literal, compact])
        harness.store.literal_bodies[str(literal["body_uri"])] = literal_payload
        harness.store.external_bodies[str(compact["body_uri"])] = ExternalBody(
            bundle_without_shares={
                key: value for key, value in compact_bundle.items() if key != "shares"
            },
            shares_key="shares",
            parts=[
                {
                    "kind": "segment",
                    "first_share_seq": 7,
                    "last_share_seq": 8,
                    "shares": [share(7), share(8)],
                    "on_disk": True,
                }
            ],
        )

        first = harness.run()
        self.assertEqual(first["visited"], 3)
        self.assertEqual(first["written"], 3)
        self.assertEqual(first["already_present"], 0)
        self.assertEqual(first["literal_freebie"], 1)
        self.assertEqual(first["reconstructed"], 1)
        self.assertEqual(first["inline_canonicalized"], 1)
        self.assertEqual(first["mismatch"], 0)
        self.assertEqual(first["errors"], 0)
        self.assertEqual(first["exit_code"], 0)
        self.assertEqual(first["last_checkpoint"], compact_hash)
        self.assertEqual(harness.errors(), "")

        published = dict(harness.store.canonical)
        self.assertEqual(len(published), 3)
        for (_hash, digest), payload in published.items():
            self.assertEqual(sha256_hex(payload), digest)

        # An identical rerun against the same store republishes nothing.
        rerun_journal_start = len(harness.journal)
        second = harness.run()
        self.assertEqual(second["visited"], 3)
        self.assertEqual(second["already_present"], 3)
        self.assertEqual(second["written"], 0)
        self.assertEqual(second["mismatch"], 0)
        self.assertEqual(second["errors"], 0)
        self.assertEqual(second["exit_code"], 0)
        self.assertEqual(dict(harness.store.canonical), published)
        rerun_events = [
            event
            for event in harness.journal[rerun_journal_start:]
            if event[0] in {"preflight", "write_canonical"}
        ]
        self.assertEqual(rerun_events, [])

    def test_pre_v2_literal_body_is_promoted_without_reserialization(self) -> None:
        hash_hex = block_hash(11)
        # Whitespace and key order a re-serialization would normalize away.
        literal_payload = b'{\n  "shares": [],\n  "block_hash": "' + hash_hex.encode() + b'"\n}\n'
        digest = sha256_hex(literal_payload)
        row = {
            "block_hash": hash_hex,
            "audit_bundle_sha256": digest,
            "body_uri": f"prism-audit-bundle-body-{hash_hex}.json",
            "audit_bundle": None,
        }
        harness = BackfillHarness([row])
        harness.store.literal_bodies[str(row["body_uri"])] = literal_payload

        summary = harness.run()

        self.assertEqual(summary["written"], 1)
        self.assertEqual(summary["literal_freebie"], 1)
        self.assertEqual(summary["reconstructed"], 0)
        self.assertEqual(summary["inline_canonicalized"], 0)
        self.assertEqual(harness.store.canonical[(hash_hex, digest)], literal_payload)
        # The canonicalizer is the proof: promoting must not parse or re-emit.
        self.assertEqual(harness.store.canonicalized, [])
        self.assertNotIn("reconstruct", [event[0] for event in harness.journal])

    def test_modern_reconstruction_reloads_absent_segment_from_database(self) -> None:
        hash_hex = block_hash(21)
        on_disk = [share(1), share(2)]
        from_db = [share(3), share(4)]
        full_bundle = bundle_for(hash_hex, on_disk + from_db)
        digest = digest_of(full_bundle)
        row = {
            "block_hash": hash_hex,
            "audit_bundle_sha256": digest,
            "body_uri": f"prism-audit-bundle-body-{hash_hex}.json",
            "audit_bundle": None,
        }
        harness = BackfillHarness(
            [row],
            shares={item["share_seq"]: item for item in from_db},
        )
        missing_part = {
            "kind": "segment_range",
            "first_share_seq": 3,
            "last_share_seq": 4,
            "range_sha256": sha256_hex(
                harness.store.storage_json_bytes(
                    harness.store.audit_share_segment_payload(
                        first_share_seq=3,
                        last_share_seq=4,
                        shares=from_db,
                    )
                )
            ),
            "on_disk": False,
        }
        harness.store.external_bodies[str(row["body_uri"])] = ExternalBody(
            bundle_without_shares={
                key: value for key, value in full_bundle.items() if key != "shares"
            },
            shares_key="shares",
            parts=[
                {
                    "kind": "segment",
                    "first_share_seq": 1,
                    "last_share_seq": 2,
                    "shares": on_disk,
                    "on_disk": True,
                },
                missing_part,
            ],
        )

        summary = harness.run()

        self.assertEqual(summary["written"], 1)
        self.assertEqual(summary["reconstructed"], 1)
        self.assertEqual(summary["db_segment_reloads"], 1)
        self.assertEqual(summary["literal_freebie"], 0)
        self.assertEqual(summary["mismatch"], 0)
        self.assertEqual(summary["errors"], 0)
        self.assertIn(("load_share_range", 3, 4), harness.journal)
        self.assertEqual(
            harness.store.canonical[(hash_hex, digest)],
            canonical_bytes(full_bundle),
        )

    def test_database_range_that_fails_its_segment_digest_publishes_nothing(self) -> None:
        hash_hex = block_hash(22)
        on_disk = [share(1)]
        expected_db = [share(3), share(4)]
        full_bundle = bundle_for(hash_hex, on_disk + expected_db)
        digest = digest_of(full_bundle)
        row = {
            "block_hash": hash_hex,
            "audit_bundle_sha256": digest,
            "body_uri": f"prism-audit-bundle-body-{hash_hex}.json",
            "audit_bundle": None,
        }
        tampered = [dict(expected_db[0], miner_id="someone-else"), expected_db[1]]
        harness = BackfillHarness(
            [row],
            shares={item["share_seq"]: item for item in tampered},
        )
        harness.store.external_bodies[str(row["body_uri"])] = ExternalBody(
            bundle_without_shares={
                key: value for key, value in full_bundle.items() if key != "shares"
            },
            shares_key="shares",
            parts=[
                {
                    "kind": "segment",
                    "first_share_seq": 1,
                    "last_share_seq": 1,
                    "shares": on_disk,
                    "on_disk": True,
                },
                {
                    "kind": "segment_range",
                    "first_share_seq": 3,
                    "last_share_seq": 4,
                    "range_sha256": sha256_hex(
                        harness.store.storage_json_bytes(
                            harness.store.audit_share_segment_payload(
                                first_share_seq=3,
                                last_share_seq=4,
                                shares=expected_db,
                            )
                        )
                    ),
                    "on_disk": False,
                },
            ],
        )

        summary = harness.run()

        self.assertEqual(summary["mismatch"], 1)
        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["db_segment_reloads"], 0)
        self.assertEqual(summary["exit_code"], 1)
        self.assertEqual(harness.store.canonical, {})
        self.assertIn("canonicalizes to", harness.errors())

    def test_incomplete_database_range_is_refused(self) -> None:
        hash_hex = block_hash(23)
        full_bundle = bundle_for(hash_hex, [share(3), share(4)])
        row = {
            "block_hash": hash_hex,
            "audit_bundle_sha256": digest_of(full_bundle),
            "body_uri": f"prism-audit-bundle-body-{hash_hex}.json",
            "audit_bundle": None,
        }
        # The ledger can only serve half of the absent range.
        harness = BackfillHarness([row], shares={3: share(3)})
        harness.store.external_bodies[str(row["body_uri"])] = ExternalBody(
            bundle_without_shares={
                key: value for key, value in full_bundle.items() if key != "shares"
            },
            shares_key="shares",
            parts=[
                {
                    "kind": "segment_range",
                    "first_share_seq": 3,
                    "last_share_seq": 4,
                    "on_disk": False,
                }
            ],
        )

        summary = harness.run()

        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["exit_code"], 1)
        self.assertEqual(harness.store.canonical, {})
        self.assertIn("incomplete or out of order", harness.errors())

    def test_digest_mismatch_is_loud_and_publishes_nothing(self) -> None:
        good = inline_row(31)
        bad = inline_row(32, digest="ab" * 32)
        harness = BackfillHarness([good, bad])

        summary = harness.run()

        self.assertEqual(summary["visited"], 2)
        self.assertEqual(summary["written"], 1)
        self.assertEqual(summary["mismatch"], 1)
        self.assertEqual(summary["exit_code"], 1)
        self.assertEqual(list(harness.store.canonical), [(good["block_hash"], good["audit_bundle_sha256"])])

        reported = harness.errors()
        self.assertIn(str(bad["block_hash"]), reported)
        self.assertIn("ab" * 32, reported)
        self.assertIn("inline bundle canonicalizes to", reported)
        # Never rewrite the advertised digest to whatever the bytes hash to.
        self.assertNotIn(("write_canonical", bad["block_hash"], digest_of(bad["audit_bundle"])), harness.journal)
        # The bad row still advances the checkpoint so a resume makes progress.
        self.assertEqual(summary["last_checkpoint"], bad["block_hash"])
        self.assertEqual(len(summary["failed_rows"]), 1)
        self.assertEqual(
            summary["failed_rows"][0]["block_hash"],
            bad["block_hash"],
        )
        self.assertEqual(
            summary["failed_rows"][0]["audit_bundle_sha256"],
            bad["audit_bundle_sha256"],
        )
        self.assertEqual(summary["failed_rows"][0]["kind"], "mismatch")

    def test_existing_canonical_artifact_with_wrong_bytes_is_reported(self) -> None:
        row = inline_row(41)
        harness = BackfillHarness([row])
        harness.store.canonical[
            (str(row["block_hash"]), str(row["audit_bundle_sha256"]))
        ] = b"not the canonical bytes"

        summary = harness.run()

        self.assertEqual(summary["mismatch"], 1)
        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["already_present"], 0)
        self.assertEqual(summary["exit_code"], 1)
        self.assertIn("existing canonical artifact hashes to", harness.errors())
        # The corrupt artifact is left exactly as found, never overwritten.
        self.assertEqual(
            harness.store.canonical[
                (str(row["block_hash"]), str(row["audit_bundle_sha256"]))
            ],
            b"not the canonical bytes",
        )

    def test_dry_run_verifies_without_publishing_or_taking_the_lease(self) -> None:
        rows = [inline_row(51), inline_row(52)]
        harness = BackfillHarness(rows)

        summary = harness.run(dry_run=True)

        self.assertEqual(summary["visited"], 2)
        self.assertEqual(summary["dry_run"], 2)
        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["mismatch"], 0)
        self.assertEqual(summary["errors"], 0)
        self.assertEqual(summary["exit_code"], 0)
        self.assertTrue(summary["dry_run_mode"])
        self.assertEqual(harness.store.canonical, {})
        # No filesystem publication means no lease refresh either.
        self.assertEqual(harness.publication_events(), [])

    def test_dry_run_still_reports_mismatches(self) -> None:
        harness = BackfillHarness([inline_row(53, digest="cd" * 32)])

        summary = harness.run(dry_run=True)

        self.assertEqual(summary["mismatch"], 1)
        self.assertEqual(summary["dry_run"], 0)
        self.assertEqual(summary["exit_code"], 1)
        self.assertEqual(harness.publication_events(), [])

    def test_batches_page_in_resumable_primary_key_order(self) -> None:
        indexes = [61, 62, 63, 64, 65]
        rows = [inline_row(index) for index in indexes]
        # Insertion order is shuffled: the ordering must come from the query.
        harness = BackfillHarness([rows[3], rows[0], rows[4], rows[2], rows[1]])

        summary = harness.run(batch_size=2)

        self.assertEqual(summary["visited"], 5)
        self.assertEqual(summary["written"], 5)
        self.assertEqual(summary["last_checkpoint"], block_hash(65))
        self.assertEqual(
            harness.ledger.page_requests,
            [(None, 2), (block_hash(62), 2), (block_hash(64), 2)],
        )
        written = [
            event[1] for event in harness.journal if event[0] == "write_canonical"
        ]
        self.assertEqual(written, [block_hash(index) for index in indexes])

    def test_limit_stops_early_and_start_after_resumes_from_the_checkpoint(self) -> None:
        indexes = [71, 72, 73, 74, 75]
        rows = [inline_row(index) for index in indexes]

        first = BackfillHarness(rows)
        first_summary = first.run(batch_size=2, limit=3)
        self.assertEqual(first_summary["visited"], 3)
        self.assertEqual(first_summary["written"], 3)
        self.assertEqual(first_summary["last_checkpoint"], block_hash(73))
        self.assertEqual(first_summary["limit"], 3)
        self.assertEqual(first.ledger.page_requests, [(None, 2), (block_hash(72), 1)])

        resumed = BackfillHarness(rows)
        resumed_summary = resumed.run(
            batch_size=2,
            start_after=first_summary["last_checkpoint"],
        )
        self.assertEqual(resumed_summary["visited"], 2)
        self.assertEqual(resumed_summary["written"], 2)
        self.assertEqual(resumed_summary["start_after"], block_hash(73))
        self.assertEqual(resumed_summary["last_checkpoint"], block_hash(75))
        self.assertEqual(
            [event[1] for event in resumed.journal if event[0] == "write_canonical"],
            [block_hash(74), block_hash(75)],
        )
        self.assertEqual(resumed.ledger.page_requests[0], (block_hash(73), 2))

    def test_resuming_past_the_last_row_is_a_clean_no_op(self) -> None:
        harness = BackfillHarness([inline_row(81)])

        summary = harness.run(start_after=block_hash(81))

        self.assertEqual(summary["visited"], 0)
        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["exit_code"], 0)
        self.assertEqual(summary["last_checkpoint"], block_hash(81))

    def test_lease_preflight_runs_immediately_before_every_publication(self) -> None:
        rows = [inline_row(91), inline_row(92)]
        harness = BackfillHarness(rows)

        summary = harness.run()

        self.assertEqual(summary["written"], 2)
        self.assertEqual(
            harness.publication_events(),
            [
                ("preflight", rows[0]["block_hash"], rows[0]["audit_bundle_sha256"]),
                ("write_canonical", rows[0]["block_hash"], rows[0]["audit_bundle_sha256"]),
                ("preflight", rows[1]["block_hash"], rows[1]["audit_bundle_sha256"]),
                ("write_canonical", rows[1]["block_hash"], rows[1]["audit_bundle_sha256"]),
            ],
        )

    def test_preflight_refusal_withholds_the_publication(self) -> None:
        rows = [inline_row(101), inline_row(102)]
        harness = BackfillHarness(rows)
        harness.ledger.preflight_failures.add(
            (str(rows[0]["block_hash"]), str(rows[0]["audit_bundle_sha256"]))
        )

        summary = harness.run()

        self.assertEqual(summary["written"], 1)
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["exit_code"], 1)
        self.assertNotIn(
            ("write_canonical", rows[0]["block_hash"], rows[0]["audit_bundle_sha256"]),
            harness.journal,
        )
        self.assertIn("writer lease is not active", harness.errors())
        # A refusal on one row does not stop the requested range.
        self.assertIn(
            ("write_canonical", rows[1]["block_hash"], rows[1]["audit_bundle_sha256"]),
            harness.journal,
        )

    def test_preflight_error_result_withholds_the_publication(self) -> None:
        row = inline_row(103)
        harness = BackfillHarness([row])
        harness.ledger.preflight_results[
            (str(row["block_hash"]), str(row["audit_bundle_sha256"]))
        ] = {"error": "existing audit bundle does not match payload"}

        summary = harness.run()

        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["exit_code"], 1)
        self.assertEqual(harness.store.canonical, {})
        self.assertIn("existing audit bundle does not match payload", harness.errors())

    def test_unconfirmed_preflight_withholds_the_publication(self) -> None:
        row = inline_row(104)
        harness = BackfillHarness([row])
        harness.ledger.preflight_results[
            (str(row["block_hash"]), str(row["audit_bundle_sha256"]))
        ] = {"confirmed": False}

        summary = harness.run()

        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(harness.store.canonical, {})

    def test_row_without_any_recoverable_body_is_reported(self) -> None:
        hash_hex = block_hash(111)
        row = {
            "block_hash": hash_hex,
            "audit_bundle_sha256": digest_of(bundle_for(hash_hex, [])),
            "body_uri": None,
            "audit_bundle": None,
        }
        harness = BackfillHarness([row])

        summary = harness.run()

        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["exit_code"], 1)
        self.assertIn("neither an inline bundle nor an external body", harness.errors())

    def test_non_canonical_row_identity_is_reported(self) -> None:
        row = inline_row(112)
        row["audit_bundle_sha256"] = "not-hex"
        harness = BackfillHarness([row])

        summary = harness.run()

        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["written"], 0)
        self.assertEqual(summary["exit_code"], 1)
        self.assertIn("row identity is not canonical", harness.errors())

    def test_unreadable_external_body_falls_back_to_the_inline_bundle(self) -> None:
        hash_hex = block_hash(121)
        bundle = bundle_for(hash_hex, [share(5)])
        row = {
            "block_hash": hash_hex,
            "audit_bundle_sha256": digest_of(bundle),
            # Registered nowhere: the literal reader declines and the
            # reconstruction helper raises.
            "body_uri": f"prism-audit-bundle-body-{hash_hex}.json",
            "audit_bundle": bundle,
        }
        harness = BackfillHarness([row])

        summary = harness.run()

        self.assertEqual(summary["written"], 1)
        self.assertEqual(summary["inline_canonicalized"], 1)
        self.assertEqual(summary["mismatch"], 0)
        self.assertEqual(summary["errors"], 0)
        self.assertEqual(
            harness.store.canonical[(hash_hex, str(row["audit_bundle_sha256"]))],
            canonical_bytes(bundle),
        )

    def test_store_write_failure_is_counted_and_scanning_continues(self) -> None:
        rows = [inline_row(131), inline_row(132)]
        harness = BackfillHarness(rows)
        harness.store.write_failures.add(
            (str(rows[0]["block_hash"]), str(rows[0]["audit_bundle_sha256"]))
        )

        summary = harness.run()

        self.assertEqual(summary["written"], 1)
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["exit_code"], 1)
        self.assertEqual(summary["last_checkpoint"], rows[1]["block_hash"])

    def test_summary_is_machine_readable_and_names_the_bound_interface(self) -> None:
        harness = BackfillHarness([inline_row(141)])

        summary = harness.run()

        for key in (
            "schema",
            "visited",
            "written",
            "already_present",
            "dry_run",
            "literal_freebie",
            "mismatch",
            "errors",
            "last_checkpoint",
            "exit_code",
            "failed_rows",
        ):
            self.assertIn(key, summary)
        self.assertEqual(
            summary["schema"],
            "qbit.prism.audit-bundle-canonical-backfill.v1",
        )
        # The summary must survive a round trip through JSON unchanged.
        self.assertEqual(json.loads(json.dumps(summary, sort_keys=True)), summary)
        self.assertEqual(
            summary["interface"]["artifacts"]["read_canonical"],
            "read_canonical_audit_bundle",
        )
        self.assertEqual(
            summary["interface"]["ledger"]["publication_preflight"],
            "preflight_audit_bundle_publication",
        )
        self.assertEqual(
            summary["interface"]["ledger"]["preflight_digest_keyword"],
            "audit_bundle_sha256",
        )


class WithoutAttributes:
    """A build that simply does not have some of the assumed methods."""

    def __init__(self, target: Any, *hidden: str) -> None:
        self.__dict__["_target"] = target
        self.__dict__["_hidden"] = frozenset(hidden)

    def __getattr__(self, name: str) -> Any:
        if name in self.__dict__["_hidden"]:
            raise AttributeError(name)
        return getattr(self.__dict__["_target"], name)


class AdapterBindingTest(unittest.TestCase):
    def test_phase_2_production_interfaces_bind_and_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = AuditArtifactStore(
                AuditArtifactConfig(
                    root=root,
                    evidence_path=root / "evidence.json",
                ),
                canonicalizer=canonical_bytes,
            )
            try:
                artifacts = CanonicalArtifactAdapter(store)
                block = "ab" * 32
                payload = b'{"schema":"qbit.prism.audit-bundle.v1"}'
                digest = sha256_hex(payload)

                artifacts.write_canonical(block, digest, payload)

                self.assertEqual(artifacts.read_canonical(block, digest), payload)
                self.assertEqual(
                    artifacts.resolved_interface()["literal_body_reader"],
                    "literal_canonical_bundle_bytes",
                )
                self.assertEqual(
                    artifacts.resolved_interface()["external_body_reconstruction"],
                    "canonical_bundle_bytes_from_external_body",
                )
            finally:
                store.close()

        ledger = AuditBundleLedgerAdapter(PsqlShareLedger.__new__(PsqlShareLedger))
        self.assertEqual(
            ledger.resolved_interface()["publication_preflight"],
            "preflight_canonical_bundle_publication",
        )

    def test_missing_canonical_capability_is_named(self) -> None:
        journal: list[tuple[Any, ...]] = []
        store = WithoutAttributes(
            FakeArtifactStore(journal),
            "read_canonical_audit_bundle",
        )
        with self.assertRaises(MissingCanonicalCapability) as caught:
            CanonicalArtifactAdapter(store)
        self.assertIn("read_canonical_audit_bundle", str(caught.exception))

    def test_missing_ledger_preflight_is_named(self) -> None:
        journal: list[tuple[Any, ...]] = []
        ledger = WithoutAttributes(
            FakeLedger(journal, []),
            *LEDGER_PREFLIGHT_CANDIDATES,
        )
        with self.assertRaises(MissingCanonicalCapability) as caught:
            AuditBundleLedgerAdapter(ledger)
        self.assertIn("preflight", str(caught.exception))

    def test_reconstruction_helper_without_callback_still_binds(self) -> None:
        """A build without the phase-2 helper falls back to the phase-1 reader."""

        journal: list[tuple[Any, ...]] = []
        target = FakeArtifactStore(journal)
        target.read_external_body = (  # type: ignore[attr-defined]
            lambda body_uri, *, expected_sha256: None
        )
        store = WithoutAttributes(target, "reconstruct_external_audit_body")
        adapter = CanonicalArtifactAdapter(store)
        self.assertEqual(
            adapter.resolved_interface()["external_body_reconstruction"],
            "read_external_body",
        )
        # The phase-1 reader takes no missing-range callback, so it must not be
        # handed one; binding still succeeds and the row simply cannot recover
        # an absent segment.
        self.assertIsNone(
            adapter.canonical_bytes_from_external_body(
                "bb" * 32,
                "body",
                "aa" * 32,
                lambda **_: [],
            )
        )


class PageQueryTest(unittest.TestCase):
    def test_first_page_has_no_cursor_predicate(self) -> None:
        sql = audit_bundle_page_sql(start_after=None, limit=25)
        self.assertNotIn("WHERE", sql)
        self.assertIn("ORDER BY block_hash ASC", sql)
        self.assertIn("LIMIT 25", sql)
        self.assertIn("FROM qbit_pool_audit_bundles", sql)

    def test_resumed_page_filters_and_escapes_the_cursor(self) -> None:
        sql = audit_bundle_page_sql(start_after="a b'c", limit=5)
        self.assertIn("WHERE block_hash > 'a b''c'", sql)
        self.assertIn("LIMIT 5", sql)

    def test_page_limit_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            audit_bundle_page_sql(start_after=None, limit=0)

    def test_page_query_selects_only_the_columns_the_backfill_reads(self) -> None:
        sql = audit_bundle_page_sql(start_after=None, limit=1)
        for column in ("block_hash", "audit_bundle_sha256", "body_uri", "audit_bundle"):
            self.assertIn(column, sql)
        # A backfill never writes the ledger.
        for statement in ("INSERT", "UPDATE", "DELETE"):
            self.assertNotIn(statement, sql.upper())


class ConstructionTest(unittest.TestCase):
    def _backfill(self, **kwargs: Any) -> CanonicalBundleBackfill:
        journal: list[tuple[Any, ...]] = []
        return CanonicalBundleBackfill(
            artifacts=CanonicalArtifactAdapter(FakeArtifactStore(journal)),
            ledger=AuditBundleLedgerAdapter(FakeLedger(journal, [])),
            **kwargs,
        )

    def test_batch_size_must_be_positive(self) -> None:
        with self.assertRaises(ValueError):
            self._backfill(batch_size=0)

    def test_limit_must_be_positive_when_given(self) -> None:
        with self.assertRaises(ValueError):
            self._backfill(limit=0)

    def test_dry_run_store_has_no_publication_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = build_audit_store_from_env(
                {
                    "PRISM_AUDIT_DIR": str(root),
                    "PRISM_EVIDENCE_PATH": str(root / "evidence.json"),
                },
                read_only=True,
            )
            try:
                self.assertFalse((root / ".prism-audit-publication.lock").exists())
                payload = b'{"schema":"qbit.prism.audit-bundle.v1"}'
                with self.assertRaisesRegex(RuntimeError, "read-only"):
                    store.write_canonical_audit_bundle(
                        "ab" * 32,
                        sha256_hex(payload),
                        payload,
                    )
            finally:
                store.close()

    def test_dry_run_ledger_uses_read_only_database_sessions(self) -> None:
        args = build_parser().parse_args(
            [
                "--dry-run",
                "--psql-command",
                "psql postgresql://example.invalid/qbit",
            ]
        )
        with mock.patch(
            "lab.prism.backfill_audit_bundle_canonical.PsqlShareLedger"
        ) as constructor:
            ledger_from_args(args, mock.sentinel.store)

        self.assertTrue(constructor.call_args.kwargs["read_only"])
        self.assertFalse(constructor.call_args.kwargs["initialize_schema"])

    def test_publish_ledger_refuses_same_identity_lease_waits(self) -> None:
        args = build_parser().parse_args(
            ["--psql-command", "psql postgresql://example.invalid/qbit"]
        )
        with mock.patch(
            "lab.prism.backfill_audit_bundle_canonical.PsqlShareLedger"
        ) as constructor:
            ledger_from_args(args, mock.sentinel.store)

        refuse_wait = constructor.call_args.kwargs["lease_retry_sleep"]
        with self.assertRaisesRegex(
            BackfillWriterLeaseUnavailable,
            "stop the coordinator",
        ):
            refuse_wait(60)


class CommandLifecycleTest(unittest.TestCase):
    @staticmethod
    def resources(journal: list[str]) -> tuple[mock.Mock, mock.Mock]:
        store = mock.Mock()
        ledger = mock.Mock()
        ledger.release_writer_lease.side_effect = lambda: (
            journal.append("release_writer_lease") or True
        )
        ledger.close.side_effect = lambda: journal.append("ledger_close")
        store.close.side_effect = lambda: journal.append("store_close")
        return store, ledger

    def run_main(
        self,
        *,
        store: mock.Mock,
        ledger: mock.Mock,
        backfill: mock.Mock,
    ) -> int:
        with (
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.build_audit_store_from_env",
                return_value=store,
            ),
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.CanonicalArtifactAdapter",
                return_value=mock.sentinel.artifacts,
            ),
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.ledger_from_args",
                return_value=ledger,
            ),
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.AuditBundleLedgerAdapter",
                return_value=mock.sentinel.ledger_adapter,
            ),
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.CanonicalBundleBackfill",
                return_value=backfill,
            ),
            mock.patch("sys.stdout", new=io.StringIO()),
        ):
            return main([])

    def test_success_releases_lease_then_closes_ledger_and_store(self) -> None:
        journal: list[str] = []
        store, ledger = self.resources(journal)
        backfill = mock.Mock()
        backfill.run.return_value = {"exit_code": 0}

        self.assertEqual(
            self.run_main(store=store, ledger=ledger, backfill=backfill),
            0,
        )
        self.assertEqual(
            journal,
            ["release_writer_lease", "ledger_close", "store_close"],
        )

    def test_failure_releases_lease_then_closes_ledger_and_store(self) -> None:
        journal: list[str] = []
        store, ledger = self.resources(journal)
        backfill = mock.Mock()
        backfill.run.side_effect = RuntimeError("backfill failed")

        with self.assertRaisesRegex(RuntimeError, "backfill failed"):
            self.run_main(store=store, ledger=ledger, backfill=backfill)
        self.assertEqual(
            journal,
            ["release_writer_lease", "ledger_close", "store_close"],
        )

    def test_conflicting_writer_lease_fails_clearly_and_closes_store(self) -> None:
        journal: list[str] = []
        store, _ledger = self.resources(journal)
        with (
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.build_audit_store_from_env",
                return_value=store,
            ),
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.CanonicalArtifactAdapter",
                return_value=mock.sentinel.artifacts,
            ),
            mock.patch(
                "lab.prism.backfill_audit_bundle_canonical.ledger_from_args",
                side_effect=RuntimeError("lease held by prism-coordinator"),
            ),
        ):
            with self.assertRaisesRegex(SystemExit, "stop the coordinator"):
                main([])

        self.assertEqual(journal, ["store_close"])


class CommandLineTest(unittest.TestCase):
    def test_checkpoint_batch_and_dry_run_options_exist(self) -> None:
        args = build_parser().parse_args(
            ["--start-after", block_hash(1), "--batch-size", "7", "--limit", "3", "--dry-run"]
        )
        self.assertEqual(args.start_after, block_hash(1))
        self.assertEqual(args.batch_size, 7)
        self.assertEqual(args.limit, 3)
        self.assertTrue(args.dry_run)

    def test_defaults_publish_the_whole_table(self) -> None:
        args = build_parser().parse_args([])
        self.assertIsNone(args.start_after)
        self.assertIsNone(args.limit)
        self.assertFalse(args.dry_run)
        self.assertEqual(args.batch_size, DEFAULT_BATCH_SIZE)

    def test_established_postgres_options_are_available(self) -> None:
        args = build_parser().parse_args([])
        for attribute in (
            "psql_command",
            "writer_id",
            "writer_epoch",
            "writer_session_token",
            "lease_ttl_seconds",
            "lease_acquire_attempts",
            "postgres_tcp_keepalives_count",
        ):
            self.assertTrue(hasattr(args, attribute), attribute)


if __name__ == "__main__":
    unittest.main()
