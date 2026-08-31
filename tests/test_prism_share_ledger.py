#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import gc
import gzip
import hashlib
import io
import json
import sys
import tempfile
import threading
import time
import tracemalloc
import types
import unittest
import unittest.mock

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from lab.prism.backfill_ctv_fanouts import backfill_input_from_path, backfill_input_from_payload, infer_block_hash_from_path
from lab.prism import public_api
from lab.prism import share_ledger as share_ledger_module
from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    CanonicalAuditBundleCorrupt,
)
from lab.prism.share_ledger import (
    database_url_from_psql_command,
    parse_single_json_value,
    AUDIT_BODY_REF_SCHEMA,
    AUDIT_BUNDLE_V2_SCHEMA,
    PendingShare,
    PsqlShareLedger,
    ShareReplayConflict,
    ShareReplayResult,
    LedgerOperationTimeout,
    AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA,
    DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
    SingleWriterShareLedger,
    WRITER_LEASE_ACQUIRE_RETRY_ATTEMPTS,
    WRITER_LEASE_ACQUIRE_RETRY_KEY,
    WRITER_LEASE_ACQUIRE_RETRY_SUBJECT,
    WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
    _NativePostgresClient,
    _NativePostgresLeaseGuard,
    _prism_window_shares,
    _writer_lease_advisory_lock_key,
    canonical_json_text,
    sha256_json_hex,
)


def pending_share(
    index: int,
    *,
    share_difficulty: int | None = None,
    job_issued_at_ms: int | None = None,
    accepted_at_ms: int | None = None,
    credit_policy: str | None = None,
) -> PendingShare:
    return PendingShare(
        share_id=f"share-{index}",
        miner_id=f"miner-{index % 3}",
        order_key=f"{index:04d}",
        p2mr_program_hex=f"{index % 256:02x}" * 32,
        share_difficulty=share_difficulty if share_difficulty is not None else 100 + index,
        network_difficulty=1_000,
        template_height=99,
        job_id=f"job-{index}",
        job_issued_at_ms=job_issued_at_ms if job_issued_at_ms is not None else 1_000 + index,
        accepted_at_ms=accepted_at_ms if accepted_at_ms is not None else 2_000 + index,
        ntime=1_700_000_000 + index,
        credit_policy=credit_policy,
    )


def sample_ctv_manifest_set() -> dict[str, object]:
    parent_coinbase_txid = "11" * 32
    fanout_txid = "22" * 32
    precommitment = {
        "chunk_index": 0,
        "chunk_count": 1,
        "block_height": 123450,
        "settlement_mode": "ctv_fanout",
        "fanout_tx_template_hex": "0300000001",
        "fanout_output_sum_sats": 25_000,
        "anchor_vout": 1,
        "ctv_hash_hex": "33" * 32,
    }
    manifest = {
        "schema": "qbit.prism.ctv-fanout-manifest.v1",
        "precommitment": precommitment,
        "precommitment_sha256_hex": "44" * 32,
        "commitment_witness_leaf_hex": "55" * 32,
        "parent_coinbase_txid": parent_coinbase_txid,
        "parent_coinbase_tx_hex": "0200000001",
        "parent_coinbase_vout": 2,
        "covenant_output_value_sats": 25_000,
        "fanout_tx_hex": "0300000002",
        "fanout_txid": fanout_txid,
    }
    return {
        "schema": "qbit.prism.ctv-fanout-manifest-set.v1",
        "block_height": 123450,
        "settlement_mode": "ctv_fanout",
        "parent_coinbase_txid": parent_coinbase_txid,
        "fanout_count": 1,
        "fanout_output_sum_sats": 25_000,
        "covenant_output_value_sats": 25_000,
        "manifests": [manifest],
    }


def sample_no_anchor_fee_ctv_manifest_set() -> dict[str, object]:
    manifest_set = sample_ctv_manifest_set()
    manifest = manifest_set["manifests"][0]  # type: ignore[index]
    precommitment = manifest["precommitment"]  # type: ignore[index]
    precommitment["fanout_fee_sats"] = 100  # type: ignore[index]
    precommitment["fanout_output_sum_sats"] = 24_900  # type: ignore[index]
    precommitment.pop("anchor_vout", None)  # type: ignore[union-attr]
    manifest_set["fanout_output_sum_sats"] = 24_900
    return manifest_set


def fake_audit_bundle_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


def fake_audit_bundle_sha256(payload: dict[str, Any]) -> str:
    return hashlib.sha256(fake_audit_bundle_bytes(payload)).hexdigest()


def acquired_lease(
    *,
    writer_id: str = "writer-a",
    writer_epoch: int = 1,
    session: str = "new-session",
) -> dict[str, object]:
    return {
        "acquired": True,
        "writer_id": writer_id,
        "writer_epoch": writer_epoch,
        "writer_session_token": session,
    }


def held_lease(
    *,
    writer_id: str = "writer-a",
    writer_epoch: int = 1,
    session: str = "old-session",
    wait_seconds: float = 5.0,
    updated_at: str | None = None,
    age_seconds: float | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "acquired": False,
        "writer_id": writer_id,
        "writer_epoch": writer_epoch,
        "writer_session_token": session,
        "lease_expires_at": "2026-06-26 19:50:22.233718+00",
        "lease_wait_seconds": wait_seconds,
    }
    if updated_at is not None:
        result["lease_updated_at"] = updated_at
    if age_seconds is not None:
        result["lease_age_seconds"] = age_seconds
    return result


def snapshot_retry_lease() -> dict[str, object]:
    """The acquisition statement's third arm, as PostgreSQL renders it.

    Built from the same constants the statement interpolates, so a change to
    either the key or the subject shows up as a failure in the retry tests
    rather than as a sentinel the ledger silently stops recognising.
    """
    return {
        "acquired": False,
        WRITER_LEASE_ACQUIRE_RETRY_KEY: True,
        "lease": WRITER_LEASE_ACQUIRE_RETRY_SUBJECT,
        "retry_reason": (
            f"the {WRITER_LEASE_ACQUIRE_RETRY_SUBJECT} row was committed by a "
            "concurrent first acquisition after this statement snapshot"
        ),
    }


class FakeMonotonicClock:
    """Deterministic stand-in for time.monotonic in lease-adoption tests.

    Guard-acquisition silence is measured against the monotonic clock, so
    these tests advance it exactly by each simulated retry sleep instead of
    depending on wall time.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


# Fixed hashes for the bulk terminal-update cases, named for the role each
# row plays in a page rather than for its bytes.
BULK_PENDING_A = "1a" * 32
BULK_PENDING_B = "1b" * 32
BULK_SUBMITTED = "1c" * 32
BULK_MISSING = "99" * 32


class CountingLock:
    """Context-manager lock proxy that counts how often it is entered."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.acquisitions = 0

    def __enter__(self) -> Any:
        self.acquisitions += 1
        return self._inner.__enter__()

    def __exit__(self, *exc_info: Any) -> Any:
        return self._inner.__exit__(*exc_info)


class FakeLeasePsqlShareLedger(PsqlShareLedger):
    def __init__(self, lease_results: list[dict[str, object]], **kwargs: Any):
        self.lease_results = list(lease_results)
        self.lease_queries: list[str] = []
        self.sleeps: list[float] = []
        kwargs.setdefault("native_client_mode", "0")
        retry_sleep = kwargs.pop("lease_retry_sleep", self.sleeps.append)
        super().__init__(
            psql_command="psql postgresql://example.invalid/qbit",
            lease_retry_sleep=retry_sleep,
            # One retry sleep must be able to cover a whole adoption silence,
            # or a scenario that waits out the silence is split into several
            # capped sleeps and consumes lease observations the fixture never
            # scripted. Derived from the policy so retuning the silence does
            # not silently change what these scenarios exercise.
            lease_retry_max_sleep_seconds=max(
                1.0,
                DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
            ),
            **kwargs,
        )

    def _make_writer_lease_guard(self, _database_url: str | None) -> Any:
        class FakeGuard:
            held = True

            def try_acquire(self) -> bool:
                return True

            def close(self) -> None:
                self.held = False

        guard = FakeGuard()
        self.fake_writer_lease_guard = guard
        return guard

    def _run_json(self, sql: str) -> Any:
        self.lease_queries.append(sql)
        if not self.lease_results:
            raise AssertionError("unexpected extra lease query")
        return self.lease_results.pop(0)


class QueryCapturePsqlShareLedger(PsqlShareLedger):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._read_semaphore = threading.BoundedSemaphore(1)
        self.queries: list[str] = []

    def _run_json(self, sql: str) -> Any:
        self.queries.append(sql)
        return []


class GateStatementRecordingPsqlShareLedger(PsqlShareLedger):
    """Ledger whose gate-held statements and progress stamps interleave.

    Built for the multi-statement gate bodies (confirm/reactivate/carry-forward
    integrity), where the ordering of the stamp relative to each statement is
    the property under test, not merely that a stamp happened.
    """

    def __init__(self, results: list[object]) -> None:
        self._lock = threading.Lock()
        self._read_semaphore = threading.BoundedSemaphore(1)
        self._operation_timeout_local = threading.local()
        self._statement_timeout_local = threading.local()
        self._operation_progress_local = threading.local()
        self._writer_id = "writer-a"
        self._writer_epoch = 1
        self._writer_session_token = "session-a"
        self._lease_interval_sql = "make_interval(secs => 30)"
        self._pending_results = list(results)
        self.events: list[str] = []
        self.statements = 0

    def note_progress(self) -> None:
        self.events.append("progress")

    def _run_json(self, sql: str) -> Any:
        self.statements += 1
        self.events.append(f"statement-{self.statements}")
        if not self._pending_results:
            raise AssertionError(f"unexpected extra statement: {sql[:80]}")
        return self._pending_results.pop(0)


class BlockingReadPsqlShareLedger(PsqlShareLedger):
    def __init__(self, *, read_concurrency: int) -> None:
        self._lock = threading.Lock()
        self._read_semaphore = threading.BoundedSemaphore(read_concurrency)
        self._condition = threading.Condition()
        self._release = threading.Event()
        self.active_reads = 0
        self.max_active_reads = 0
        self.started_reads = 0

    def _run_json(self, sql: str) -> Any:
        with self._condition:
            self.active_reads += 1
            self.started_reads += 1
            self.max_active_reads = max(self.max_active_reads, self.active_reads)
            self._condition.notify_all()
        self._release.wait(timeout=5)
        with self._condition:
            self.active_reads -= 1
            self._condition.notify_all()
        return None

    def wait_for_started_reads(self, count: int) -> None:
        deadline = time.monotonic() + 5
        with self._condition:
            while self.started_reads < count and time.monotonic() < deadline:
                self._condition.wait(timeout=0.05)
            if self.started_reads < count:
                raise AssertionError(f"only {self.started_reads} reads started")

    def release_reads(self) -> None:
        self._release.set()


class PrismShareLedgerTests(unittest.TestCase):
    def test_psql_private_audit_delegates_and_subclass_override_route_through_a1(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        store = unittest.mock.Mock()
        ledger._audit_artifact_store = store
        ledger._audit_bundle_canonicalizer = lambda _bundle: b"legacy"
        bundle = {"schema": "test"}
        body_path = Path("body.json")
        part = {"kind": "segment"}
        recovery = {"load_missing_range": ledger._load_audit_share_ledger_range}
        cases = (
            (
                "_externalize_audit_body",
                ("aa" * 32, "bb" * 32, bundle),
                {},
                "externalize_audit_body",
                {},
            ),
            (
                "_canonical_audit_body_bytes_for_sha",
                (bundle, "bb" * 32),
                {},
                "canonical_audit_body_bytes_for_sha",
                {},
            ),
            ("_audit_body_ref", (), {"payload": bundle}, "audit_body_ref", {}),
            ("_audit_bundle_v2", (), {"payload": bundle}, "audit_bundle_v2", recovery),
            ("_audit_share_parts", ([{"share_seq": 1}],), {}, "audit_share_parts", {}),
            (
                "_audit_share_range_parts",
                ([{"share_seq": 1}],),
                {},
                "audit_share_range_parts",
                recovery,
            ),
            (
                "_audit_share_segment_payload",
                (),
                {"shares": [{"share_seq": 1}]},
                "audit_share_segment_payload",
                {},
            ),
            (
                "_write_audit_share_segment",
                (),
                {"shares": [{"share_seq": 1}]},
                "write_audit_share_segment",
                {},
            ),
            (
                "_write_audit_share_segment_range",
                (),
                {"shares": [{"share_seq": 1}]},
                "write_audit_share_segment_range",
                recovery,
            ),
            (
                "_merge_audit_share_ranges",
                ([{"share_seq": 1}], [{"share_seq": 2}]),
                {"segment_path": body_path},
                "merge_audit_share_ranges",
                recovery,
            ),
            (
                "_audit_shares_by_seq",
                ([{"share_seq": 1}],),
                {"segment_path": body_path},
                "audit_shares_by_seq",
                {"require_contiguous": True},
            ),
            ("_storage_json_bytes", (bundle,), {}, "storage_json_bytes", {}),
            (
                "_canonical_audit_bundle_bytes",
                (bundle,),
                {},
                "canonical_audit_bundle_bytes",
                {},
            ),
            (
                "_audit_body_path",
                ("aa" * 32, "bb" * 32),
                {},
                "body_path",
                {},
            ),
            ("_resolve_audit_body_path", (body_path,), {}, "resolve_owned_path", {}),
            (
                "_audit_body_byte_len",
                (body_path, bundle, body_path),
                {},
                "audit_body_byte_len",
                {},
            ),
            (
                "_read_external_body",
                (body_path,),
                {"expected_sha256": "bb" * 32},
                "read_external_body",
                {},
            ),
            (
                "_external_body_matches_sha",
                (body_path, "bb" * 32),
                {},
                "external_body_matches_sha",
                {},
            ),
            (
                "_external_body_available_for_sha",
                (body_path, "bb" * 32),
                {},
                "external_body_available_for_sha",
                {},
            ),
            (
                "_resolve_audit_body_ref",
                (bundle,),
                {"expected_sha256": "bb" * 32, "body_uri": body_path},
                "resolve_audit_body_ref",
                {},
            ),
            (
                "_resolve_audit_bundle_v2",
                (bundle,),
                {"expected_sha256": "bb" * 32, "body_uri": body_path},
                "resolve_audit_bundle_v2",
                {},
            ),
            (
                "_read_audit_share_segment",
                (part,),
                {"parent_body_uri": body_path},
                "read_audit_share_segment",
                {},
            ),
            (
                "_select_audit_share_segment_range",
                ([{"share_seq": 1}],),
                {
                    "first_share_seq": 1,
                    "last_share_seq": 1,
                    "parent_body_uri": body_path,
                    "body_uri": body_path,
                },
                "select_audit_share_segment_range",
                {},
            ),
        )
        token = object()
        for wrapper_name, args, kwargs, target_name, extra_kwargs in cases:
            with self.subTest(wrapper=wrapper_name):
                target = getattr(store, target_name)
                target.reset_mock()
                target.return_value = token
                self.assertIs(getattr(ledger, wrapper_name)(*args, **kwargs), token)
                target.assert_called_once_with(*args, **{**kwargs, **extra_kwargs})
                self.assertIs(ledger._audit_store(), store)

        store.read_audit_share_segment.reset_mock()
        self.assertTrue(
            ledger._audit_share_segment_available(
                part,
                parent_body_uri=body_path,
            )
        )
        store.read_audit_share_segment.assert_called_once_with(
            part,
            parent_body_uri=body_path,
        )

        override_calls: list[tuple[object, object]] = []
        ledger._read_external_body = (  # type: ignore[method-assign]
            lambda body_uri, *, expected_sha256=None: (
                override_calls.append((body_uri, expected_sha256))
                or {"resolved": True}
            )
        )
        self.assertEqual(
            ledger._resolve_audit_bundle_row(
                {
                    "audit_bundle": None,
                    "body_uri": "owned-body",
                    "audit_bundle_sha256": "bb" * 32,
                }
            ),
            {
                "audit_bundle": {"resolved": True},
                "audit_bundle_sha256": "bb" * 32,
            },
        )
        self.assertEqual(override_calls, [("owned-body", "bb" * 32)])

    def test_memory_recovery_append_distinguishes_insert_exact_and_conflict(self) -> None:
        ledger = SingleWriterShareLedger()
        share = pending_share(1)

        inserted = ledger.append_recovered_share(share)
        exact = ledger.append_recovered_share(share)

        self.assertIsInstance(inserted, ShareReplayResult)
        self.assertEqual(inserted.disposition, "inserted")
        self.assertEqual(exact.disposition, "exact_existing")
        with self.assertRaises(ShareReplayConflict):
            ledger.append_recovered_share(
                PendingShare(**{**share.__dict__, "ntime": share.ntime + 1})
            )

    def test_writer_lease_advisory_guard_is_stable_and_session_scoped(self) -> None:
        statements: list[str] = []
        connect_kwargs: dict[str, object] = {}

        class FakeConnection:
            closed = False

            def execute(self, sql: str) -> FakeConnection:
                statements.append(sql)
                self.row = (
                    True
                    if sql.startswith("SELECT pg_try_advisory_lock")
                    else {"renewed_count": 1}
                )
                return self

            def fetchone(self) -> tuple[object]:
                return (self.row,)

            def close(self) -> None:
                self.closed = True

        connection = FakeConnection()

        class FakePsycopg:
            @staticmethod
            def connect(_conninfo: str, **kwargs: object) -> FakeConnection:
                connect_kwargs.update(kwargs)
                return connection

        key = _writer_lease_advisory_lock_key("writer-a", 7)
        self.assertEqual(key, _writer_lease_advisory_lock_key("writer-a", 7))
        self.assertNotEqual(key, _writer_lease_advisory_lock_key("writer-a", 8))

        with unittest.mock.patch.dict(sys.modules, {"psycopg": FakePsycopg}):
            guard = _NativePostgresLeaseGuard("postgresql://example/qbit", key)

        self.assertTrue(guard.try_acquire())
        self.assertTrue(guard.held)
        self.assertEqual(
            guard.run_json("SELECT json_build_object('renewed_count', 1)"),
            {"renewed_count": 1},
        )
        self.assertEqual(connect_kwargs["connect_timeout"], 2)
        self.assertIn("statement_timeout=500", str(connect_kwargs["options"]))
        self.assertIn(str(key), statements[0])

        guard.close()
        self.assertFalse(guard.held)
        with self.assertRaisesRegex(RuntimeError, "guard is not held"):
            guard.run_json("SELECT 1")

    def test_single_writer_assigns_contiguous_sequence_numbers(self) -> None:
        ledger = SingleWriterShareLedger()

        records = [ledger.append(pending_share(index)) for index in range(1, 6)]

        self.assertEqual([record.share_seq for record in records], [1, 2, 3, 4, 5])
        self.assertEqual(len(ledger), 5)
        self.assertEqual([record["share_seq"] for record in (item.to_prism_json() for item in records)], [1, 2, 3, 4, 5])

    def test_credit_policy_round_trips_only_when_present(self) -> None:
        ledger = SingleWriterShareLedger()

        normal = ledger.append(pending_share(1))
        grace = ledger.append(pending_share(2, credit_policy="stale-grace"))

        self.assertNotIn("credit_policy", normal.to_prism_json())
        self.assertEqual(grace.credit_policy, "stale-grace")
        self.assertEqual(grace.to_prism_json()["credit_policy"], "stale-grace")

    def test_invalid_credit_policy_is_rejected_before_storage(self) -> None:
        ledger = SingleWriterShareLedger()

        for policy in ("", "bogus"):
            with self.subTest(policy=policy), self.assertRaisesRegex(
                ValueError,
                "unsupported credit_policy",
            ):
                ledger.append(pending_share(1, credit_policy=policy))

    def test_job_issue_snapshot_excludes_later_job_shares(self) -> None:
        ledger = SingleWriterShareLedger()
        ledger.append(pending_share(1, job_issued_at_ms=1_000, accepted_at_ms=1_001))
        ledger.append(pending_share(2, job_issued_at_ms=1_005, accepted_at_ms=1_005))
        snapshot = ledger.snapshot_at_job_issue(1_005)
        ledger.append(pending_share(3, job_issued_at_ms=1_006, accepted_at_ms=1_006))

        self.assertEqual([share.share_id for share in snapshot], ["share-1", "share-2"])
        self.assertEqual(
            [share.share_id for share in ledger.snapshot_at_job_issue(1_005)],
            ["share-1", "share-2"],
        )

    def test_job_issue_snapshot_accepts_window_weight_hint(self) -> None:
        # The in-memory backend follows the same exact crossing-row bound as
        # Postgres so synchronous and incremental artifact hashes cannot
        # diverge in local/embedded deployments.
        ledger = SingleWriterShareLedger()
        for index in range(1, 5):
            ledger.append(
                pending_share(
                    index,
                    share_difficulty=3,
                    job_issued_at_ms=1_000,
                    accepted_at_ms=1_000 + index,
                )
            )

        unbounded = [s.share_id for s in ledger.snapshot_at_job_issue(1_005)]
        hinted = [
            s.share_id
            for s in ledger.snapshot_at_job_issue(1_005, window_weight=5)
        ]

        self.assertEqual(
            unbounded,
            ["share-1", "share-2", "share-3", "share-4"],
        )
        self.assertEqual(hinted, ["share-3", "share-4"])

    def test_job_issue_snapshot_excludes_old_job_shares_accepted_after_anchor(self) -> None:
        ledger = SingleWriterShareLedger()
        ledger.append(pending_share(1, job_issued_at_ms=1_000, accepted_at_ms=1_001))
        ledger.append(pending_share(2, job_issued_at_ms=1_000, accepted_at_ms=1_006))

        self.assertEqual(
            [share.share_id for share in ledger.snapshot_at_job_issue(1_005)],
            ["share-1"],
        )

    def test_job_issue_delta_returns_each_newly_eligible_share_once(self) -> None:
        ledger = SingleWriterShareLedger()
        ledger.append(
            pending_share(1, job_issued_at_ms=1_000, accepted_at_ms=1_001)
        )
        ledger.append(
            pending_share(2, job_issued_at_ms=1_000, accepted_at_ms=1_006)
        )
        ledger.append(
            pending_share(3, job_issued_at_ms=1_006, accepted_at_ms=1_004)
        )
        ledger.append(
            pending_share(4, job_issued_at_ms=1_007, accepted_at_ms=1_007)
        )

        delta = ledger.snapshot_between_job_issues(1_005, 1_006)

        self.assertEqual([share.share_id for share in delta], ["share-2", "share-3"])

    def test_rejects_zero_difficulty_share(self) -> None:
        ledger = SingleWriterShareLedger()
        share = pending_share(1)

        with self.assertRaisesRegex(ValueError, "share_difficulty"):
            ledger.append(share.__class__(**{**share.__dict__, "share_difficulty": 0}))
        with self.assertRaisesRegex(ValueError, "network_difficulty"):
            ledger.append(share.__class__(**{**share.__dict__, "network_difficulty": 0}))

    def test_exact_duplicate_share_is_idempotent_but_mutation_is_rejected(self) -> None:
        ledger = SingleWriterShareLedger()
        first = pending_share(1)
        duplicate = pending_share(2).__class__(**{**pending_share(2).__dict__, "share_id": first.share_id})
        later_stamp = first.__class__(
            **{**first.__dict__, "accepted_at_ms": first.accepted_at_ms + 42}
        )

        self.assertEqual(ledger.append(first).share_seq, 1)
        replay = ledger.append(later_stamp)
        self.assertEqual(replay.share_seq, 1)
        self.assertFalse(replay.newly_inserted)
        self.assertEqual(replay.accepted_at_ms, first.accepted_at_ms)
        with self.assertRaisesRegex(ValueError, "payload mismatch"):
            ledger.append(duplicate)

        self.assertEqual(ledger.append(pending_share(3)).share_seq, 2)

    def test_share_and_block_candidate_intent_commit_atomically(self) -> None:
        ledger = SingleWriterShareLedger()
        share = pending_share(1)
        intent = {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": "ab" * 32,
            "block_hex": "00",
        }

        records = ledger.append_batch([(share, intent)])

        self.assertEqual(records[0].share_seq, 1)
        self.assertTrue(records[0].newly_inserted)
        self.assertEqual(records[0].candidate_outbox_state, "pending")
        self.assertEqual(ledger.pending_block_candidates(), [intent])
        rows = ledger.pending_block_candidate_rows()
        self.assertEqual(
            [
                {key: value for key, value in row.items() if key != "cursor"}
                for row in rows
            ],
            [
                {
                    "block_hash": "ab" * 32,
                    "candidate": intent,
                    "pool_block_exists": False,
                }
            ],
        )
        # The pagination cursor is opaque to callers but always keyed on the
        # row it came from.
        self.assertEqual(rows[0]["cursor"][1], "ab" * 32)
        # Exact replay returns the original row and does not duplicate outbox
        # work. A changed intent with the same hash is rejected as corruption.
        replay = ledger.append_batch([(share, intent)])[0]
        self.assertEqual(replay.share_seq, 1)
        self.assertFalse(replay.newly_inserted)
        self.assertEqual(replay.candidate_outbox_state, "pending")
        with self.assertRaisesRegex(ValueError, "candidate payload mismatch"):
            ledger.append_batch([(share, {**intent, "block_hex": "01"})])
        self.assertEqual(len(ledger), 1)
        self.assertTrue(ledger.mark_block_candidate_submitted(block_hash="ab" * 32))
        self.assertEqual(ledger.pending_block_candidates(), [])
        later_share = share.__class__(
            **{**share.__dict__, "accepted_at_ms": share.accepted_at_ms + 42}
        )
        terminal_replay = ledger.append_batch([(later_share, intent)])[0]
        self.assertFalse(terminal_replay.newly_inserted)
        self.assertEqual(terminal_replay.candidate_outbox_state, "submitted")
        self.assertFalse(
            ledger.mark_block_candidate_abandoned(
                block_hash="ab" * 32,
                error="must not invert terminal state",
            )
        )

    def test_pending_candidate_age_distinguishes_first_attempt(self) -> None:
        ledger = SingleWriterShareLedger()
        block_hash = "ac" * 32
        intent = {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": block_hash,
            "block_hex": "00",
        }
        ledger.persist_block_candidate_intent(intent)

        pending = ledger.block_candidate_pending_metrics()
        self.assertEqual(pending["pending_count"], 1)
        self.assertGreaterEqual(pending["oldest_pending_age_seconds"], 0)
        self.assertGreaterEqual(pending["oldest_unattempted_age_seconds"], 0)

        self.assertTrue(ledger.mark_block_candidate_attempted(block_hash=block_hash))
        attempted = ledger.block_candidate_pending_metrics()
        self.assertEqual(attempted["oldest_unattempted_age_seconds"], 0)
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["attempt_count"],
            1,
        )

    def _seeded_bulk_abandon_ledger(self) -> SingleWriterShareLedger:
        """One pending pair, one already-terminal row, one never persisted."""
        ledger = SingleWriterShareLedger()
        for block_hash in (BULK_PENDING_A, BULK_PENDING_B, BULK_SUBMITTED):
            ledger.persist_block_candidate_intent(
                {
                    "schema": "qbit.prism.block-candidate-intent.v1",
                    "block_hash_hex": block_hash,
                    "block_hex": "00",
                }
            )
        self.assertTrue(
            ledger.mark_block_candidate_submitted(block_hash=BULK_SUBMITTED)
        )
        return ledger

    def test_bulk_candidate_abandon_reports_only_the_rows_it_transitioned(
        self,
    ) -> None:
        """The report is the caller's cleanup authority, not an echo.

        A page handed to the bulk update is a candidate set, not a decided
        set: rows can already be terminal, or absent entirely. Returning the
        rows this call actually moved -- rather than a count or the request
        -- is what lets the caller confine its follow-up cleanup to rows it
        won, so it is asserted against a page deliberately containing both
        kinds of non-transition.
        """
        ledger = self._seeded_bulk_abandon_ledger()

        abandoned = ledger.mark_block_candidates_abandoned(
            block_hashes=[
                BULK_PENDING_B,
                BULK_PENDING_A.upper(),
                BULK_PENDING_A,
                BULK_SUBMITTED,
                BULK_MISSING,
            ],
            error="superseded by decided height",
        )

        # Duplicates and case collapse; ordering is the normalized set's, not
        # the caller's, so one input set always reports one identical result.
        self.assertEqual(abandoned, (BULK_PENDING_A, BULK_PENDING_B))
        for block_hash in (BULK_PENDING_A, BULK_PENDING_B):
            row = ledger._block_candidate_outbox[block_hash]
            self.assertEqual(row["state"], "abandoned")
            self.assertEqual(row["last_error"], "superseded by decided height")
            self.assertIsNone(row["candidate"])
        # An already-terminal row is neither reported nor rewritten, so its
        # own terminal reason survives a page that happens to name it.
        submitted = ledger._block_candidate_outbox[BULK_SUBMITTED]
        self.assertEqual(submitted["state"], "submitted")
        self.assertIsNone(submitted["last_error"])
        self.assertNotIn(BULK_MISSING, ledger._block_candidate_outbox)
        self.assertEqual(ledger.pending_block_candidates(), [])

        # Re-running the same page wins nothing: the rows are terminal now.
        self.assertEqual(
            ledger.mark_block_candidates_abandoned(
                block_hashes=[BULK_PENDING_A, BULK_PENDING_B],
                error="second attempt",
            ),
            (),
        )
        self.assertEqual(
            ledger._block_candidate_outbox[BULK_PENDING_A]["last_error"],
            "superseded by decided height",
        )

    def test_bulk_candidate_abandon_on_an_empty_page_touches_nothing(self) -> None:
        ledger = self._seeded_bulk_abandon_ledger()

        self.assertEqual(
            ledger.mark_block_candidates_abandoned(block_hashes=[], error="unused"),
            (),
        )
        self.assertEqual(
            ledger._block_candidate_outbox[BULK_PENDING_A]["state"],
            "pending",
        )
        self.assertEqual(len(ledger.pending_block_candidates()), 2)

    def test_pending_candidate_page_reports_pool_block_existence(self) -> None:
        """The page read answers the landed-block question, not a per-row call.

        Both backends carry the fact on the row itself so a caller reading a
        page never has to ask once per hash. The memory mirror reads the same
        durable evidence the Postgres ``EXISTS`` does: a pool-block row exists
        only for a candidate that reached ``persist_accepted_block``.
        """
        ledger = self._seeded_bulk_abandon_ledger()
        ledger._memory_pool_blocks[BULK_PENDING_B] = (10, "prepared", "00" * 32)

        rows = ledger.pending_block_candidate_rows(limit=32)

        self.assertEqual(
            {row["block_hash"]: row["pool_block_exists"] for row in rows},
            {BULK_PENDING_A: False, BULK_PENDING_B: True},
        )

    def test_bulk_candidate_abandon_spares_a_landed_candidate(self) -> None:
        """A pool-block row vetoes the transition even inside the page.

        The caller's page could have been read before the candidate landed,
        so the veto has to live in the write itself. A row that acquired one
        is simply absent from the report, which is exactly how the caller
        learns not to clean it up.
        """
        ledger = self._seeded_bulk_abandon_ledger()
        ledger._memory_pool_blocks[BULK_PENDING_B] = (10, "prepared", "00" * 32)

        abandoned = ledger.mark_block_candidates_abandoned(
            block_hashes=[BULK_PENDING_A, BULK_PENDING_B],
            error="superseded by decided height",
        )

        self.assertEqual(abandoned, (BULK_PENDING_A,))
        landed = ledger._block_candidate_outbox[BULK_PENDING_B]
        self.assertEqual(landed["state"], "pending")
        self.assertIsNone(landed["last_error"])
        self.assertIsNotNone(landed["candidate"])

    def test_bulk_candidate_abandon_decides_a_page_under_one_lock_hold(self) -> None:
        """One page is one atomic decision, matching the Postgres statement.

        The Postgres backend resolves a whole page inside a single fenced
        statement, so a memory backend that released and re-took the lock per
        hash would let a concurrent writer split the reported set across two
        views of the outbox -- a parity gap the returned set would then hide.
        """
        ledger = self._seeded_bulk_abandon_ledger()
        counting = CountingLock(ledger._lock)
        ledger._lock = counting

        abandoned = ledger.mark_block_candidates_abandoned(
            block_hashes=[BULK_PENDING_A, BULK_PENDING_B, BULK_MISSING],
            error="superseded by decided height",
        )

        self.assertEqual(abandoned, (BULK_PENDING_A, BULK_PENDING_B))
        self.assertEqual(counting.acquisitions, 1)

    def test_pending_candidate_cursor_pages_walk_a_stable_total_order(self) -> None:
        """Startup enumeration must complete for a backlog of any size.

        The doubling window it replaces fails closed once one query would
        have to hold the whole backlog, which blocks every job build until
        the outbox drains on its own. Pagination needs three properties for
        that gate to be safe: one stable total order, a cursor that resumes
        strictly after its own row, and a short page that proves nothing
        followed it.
        """
        ledger = SingleWriterShareLedger()
        hashes = [f"{index:02x}" * 32 for index in (0xA1, 0xA2, 0xB1, 0xB2, 0xC1)]
        # Inserted out of order so the assertions below prove the ordering
        # comes from the row keys and not from the outbox's insertion order.
        for index in (2, 4, 0, 3, 1):
            ledger.persist_block_candidate_intent(
                {
                    "schema": "qbit.prism.block-candidate-intent.v1",
                    "block_hash_hex": hashes[index],
                    "block_hex": "00",
                }
            )
        # Colliding creation stamps are the interesting case: time.monotonic
        # (like one transaction's clock_timestamp) repeats, so ordering on
        # the stamp alone would let a cursor replay or skip a whole group.
        stamps = {
            hashes[0]: 100.0,
            hashes[1]: 100.0,
            hashes[2]: 100.0,
            hashes[3]: 200.0,
            hashes[4]: 300.0,
        }
        for block_hash, stamp in stamps.items():
            ledger._block_candidate_outbox[block_hash]["created_monotonic"] = stamp

        walked: list[str] = []
        pages: list[int] = []
        after_cursor: object | None = None
        while True:
            page = ledger.pending_block_candidate_rows(
                limit=2,
                after_cursor=after_cursor,
            )
            pages.append(len(page))
            walked.extend(str(row["block_hash"]) for row in page)
            if len(page) < 2:
                break
            after_cursor = page[-1]["cursor"]

        # Total order: creation stamp first, block hash as the tiebreak.
        self.assertEqual(walked, sorted(hashes, key=lambda h: (stamps[h], h)))
        # Full pages until one short page proves the walk reached the end.
        self.assertEqual(pages, [2, 2, 1])
        # No row was served twice, and the walk saw every pending row.
        self.assertEqual(len(set(walked)), len(hashes))

        # A cursor resumes strictly after its own row, including when its
        # stamp collides with the next row's.
        tied_cursor = [100.0, hashes[0]]
        self.assertEqual(
            [
                str(row["block_hash"])
                for row in ledger.pending_block_candidate_rows(
                    limit=32,
                    after_cursor=tied_cursor,
                )
            ],
            [hashes[1], hashes[2], hashes[3], hashes[4]],
        )
        # The cursor round-trips: handing back a returned cursor verbatim is
        # equivalent to reconstructing it from the row's own ordering parts.
        last_first_page = ledger.pending_block_candidate_rows(limit=1)[0]
        self.assertEqual(last_first_page["cursor"], [100.0, hashes[0]])
        # A cursor past every row proves completion with an empty page.
        self.assertEqual(
            ledger.pending_block_candidate_rows(
                limit=32,
                after_cursor=[300.0, hashes[4]],
            ),
            [],
        )
        # Terminal rows leave the pending order entirely.
        self.assertTrue(
            ledger.mark_block_candidate_submitted(block_hash=hashes[1])
        )
        self.assertNotIn(
            hashes[1],
            [
                str(row["block_hash"])
                for row in ledger.pending_block_candidate_rows(limit=32)
            ],
        )

    def test_pending_candidate_cursor_rejects_unusable_shapes(self) -> None:
        """A cursor crosses process and JSON boundaries, so it is validated."""
        ledger = SingleWriterShareLedger()
        for cursor in (
            [],
            [1.0],
            [1.0, "ab" * 32, "extra"],
            [1.0, ""],
            ["not-a-stamp", "ab" * 32],
            "1.0,ab",
        ):
            with self.assertRaises(ValueError):
                ledger.pending_block_candidate_rows(after_cursor=cursor)

    def test_stranded_prepared_blocks_filter_by_depth_and_state(self) -> None:
        """The sweep's read only surfaces rows an operator-free heal may touch.

        reorg_watch_blocks selects confirmed/inactive rows only, so a
        prepared row whose outbox entry is gone has nothing left to
        re-examine it. The depth floor is what separates that from a
        finalization still legitimately in flight.
        """
        ledger = SingleWriterShareLedger()
        heights = {
            "deep": ("a1" * 32, 100),
            "at_floor": ("a2" * 32, 900),
            "shallow": ("a3" * 32, 901),
            "confirmed": ("a4" * 32, 200),
        }
        for block_hash, block_height in heights.values():
            ledger.persist_accepted_block(
                block_hash=block_hash,
                block_height=block_height,
                parent_hash="bb" * 32,
                final_bundle={},
                audit_report={},
            )
        confirmed_hash, confirmed_height = heights["confirmed"]
        self.assertEqual(
            ledger.confirm_accepted_block(
                block_hash=confirmed_hash,
                active_tip_height=confirmed_height,
            )["confirmed_count"],
            1,
        )

        stranded = ledger.stranded_prepared_blocks(
            active_tip_height=1000,
            min_depth=100,
        )

        self.assertEqual(
            [(row["block_hash"], row["block_height"]) for row in stranded],
            [
                (heights["deep"][0], heights["deep"][1]),
                (heights["at_floor"][0], heights["at_floor"][1]),
            ],
        )
        self.assertEqual(stranded[0]["parent_hash"], "bb" * 32)
        # A confirmed row belongs to the watch loop, not this sweep, and a
        # row shallower than the floor may still be finalizing.
        returned = {str(row["block_hash"]) for row in stranded}
        self.assertNotIn(heights["confirmed"][0], returned)
        self.assertNotIn(heights["shallow"][0], returned)
        # The limit bounds one pass; the remainder waits for the next one.
        self.assertEqual(
            [
                row["block_hash"]
                for row in ledger.stranded_prepared_blocks(
                    active_tip_height=1000,
                    min_depth=100,
                    limit=1,
                )
            ],
            [heights["deep"][0]],
        )
        # Rejecting through the fenced method retires the row from the sweep.
        self.assertEqual(
            ledger.reject_prepared_block(
                block_hash=heights["deep"][0],
                active_tip_height=1000,
            )["rejected_count"],
            1,
        )
        self.assertEqual(
            [
                row["block_hash"]
                for row in ledger.stranded_prepared_blocks(
                    active_tip_height=1000,
                    min_depth=100,
                )
            ],
            [heights["at_floor"][0]],
        )

    def test_batch_validation_is_all_or_nothing(self) -> None:
        ledger = SingleWriterShareLedger()
        first = pending_share(1)
        bad_duplicate = pending_share(2).__class__(
            **{**pending_share(2).__dict__, "share_id": first.share_id}
        )
        ledger.append(first)

        with self.assertRaisesRegex(ValueError, "payload mismatch"):
            ledger.append_batch([(pending_share(3), None), (bad_duplicate, None)])

        self.assertEqual(len(ledger), 1)

    def test_candidate_only_intent_links_to_share_after_block_lands(self) -> None:
        ledger = SingleWriterShareLedger()
        share = pending_share(1)
        intent = {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": "cd" * 32,
            "block_hex": "00",
            "credit_share_on_accept": True,
        }

        self.assertTrue(ledger.persist_block_candidate_intent(intent))
        self.assertEqual(ledger.pending_block_candidates(), [intent])
        self.assertEqual(ledger.append_batch([(share, intent)])[0].share_seq, 1)
        self.assertTrue(ledger.mark_block_candidate_submitted(block_hash="cd" * 32))
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_candidate_retry_with_new_acknowledgment_stamp_is_idempotent(self) -> None:
        # A miner can resubmit the same solved block after a transient submit
        # outcome, and the rebuilt intent differs from the persisted one only
        # in pending_share.accepted_at_ms. The outbox must treat that as the
        # same work and keep the first payload authoritative, while any other
        # divergence stays a hard payload mismatch.
        ledger = SingleWriterShareLedger()
        share = pending_share(1, accepted_at_ms=2_000)
        intent = {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": "ef" * 32,
            "block_hex": "00",
            "pending_share": dict(share.__dict__),
            "credit_share_on_accept": True,
        }
        retry_share = pending_share(1, accepted_at_ms=2_042)
        retry_intent = {**intent, "pending_share": dict(retry_share.__dict__)}

        first_persist = ledger.persist_block_candidate_intent(intent)
        retry_persist = ledger.persist_block_candidate_intent(retry_intent)
        self.assertTrue(first_persist)
        self.assertEqual(first_persist.state, "pending")
        self.assertFalse(retry_persist)
        self.assertEqual(retry_persist.state, "pending")
        self.assertEqual(ledger.pending_block_candidates(), [intent])
        with self.assertRaisesRegex(ValueError, "candidate payload mismatch"):
            ledger.persist_block_candidate_intent({**retry_intent, "block_hex": "01"})

        # A retry that lands links its share to the first persisted payload.
        self.assertEqual(
            ledger.append_batch([(retry_share, retry_intent)])[0].share_seq, 1
        )
        self.assertEqual(
            [
                {key: value for key, value in row.items() if key != "cursor"}
                for row in ledger.pending_block_candidate_rows()
            ],
            [
                {
                    "block_hash": "ef" * 32,
                    "candidate": intent,
                    "pool_block_exists": False,
                }
            ],
        )
        self.assertTrue(ledger.mark_block_candidate_submitted(block_hash="ef" * 32))
        terminal_persist = ledger.persist_block_candidate_intent(retry_intent)
        self.assertFalse(terminal_persist)
        self.assertEqual(terminal_persist.state, "submitted")

    def test_concurrent_append_still_has_one_canonical_sequence(self) -> None:
        ledger = SingleWriterShareLedger()

        def append_range(start: int) -> None:
            for index in range(start, start + 10):
                ledger.append(pending_share(index))

        threads = [threading.Thread(target=append_range, args=(start,)) for start in (1, 100, 200)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        records = ledger.all_shares()
        self.assertEqual(len(records), 30)
        self.assertEqual(sorted(record.share_seq for record in records), list(range(1, 31)))

    def test_memory_ledger_persists_ctv_fanout_recovery_artifact(self) -> None:
        ledger = SingleWriterShareLedger()
        block_hash = "aa" * 32
        manifest_set = sample_ctv_manifest_set()
        manifest_set_sha256 = "66" * 32

        first = ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256=manifest_set_sha256,
        )
        second = ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256=manifest_set_sha256,
        )
        recovery = ledger.audit_ctv_fanout_manifest_set(block_hash=block_hash)
        rows = ledger.audit_ctv_fanouts(block_hash=block_hash)

        self.assertEqual(first["fanout_artifact_count"], 1)
        self.assertEqual(second["fanout_artifact_count"], 1)
        self.assertIsNotNone(recovery)
        self.assertEqual(recovery["manifest_set_sha256"], manifest_set_sha256)  # type: ignore[index]
        self.assertEqual(recovery["block_height"], 123450)  # type: ignore[index]
        self.assertIn("manifest_set_json", recovery)  # type: ignore[operator]
        self.assertEqual(rows[0]["fanout_txid"], "22" * 32)
        self.assertEqual(rows[0]["block_height"], 123450)
        self.assertEqual(rows[0]["settlement_status"], "awaiting_maturity")
        self.assertEqual(
            ledger.ctv_fanout_status(fanout_txid="22" * 32)["block_height"],  # type: ignore[index, union-attr]
            123450,
        )

    def test_memory_ledger_persists_no_anchor_fee_fanout_recovery_artifact(self) -> None:
        ledger = SingleWriterShareLedger()
        block_hash = "ab" * 32
        manifest_set = sample_no_anchor_fee_ctv_manifest_set()

        ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256="67" * 32,
        )
        rows = ledger.audit_ctv_fanouts(block_hash=block_hash)
        status = ledger.ctv_fanout_status(fanout_txid="22" * 32)

        self.assertIsNone(rows[0]["anchor_vout"])
        self.assertIsNone(status["anchor_vout"])  # type: ignore[index]
        self.assertEqual(rows[0]["covenant_output_value_sats"], 25_000)
        self.assertEqual(rows[0]["fanout_output_sum_sats"], 24_900)

    def test_memory_ledger_rejects_built_in_fee_fanout_with_anchor(self) -> None:
        ledger = SingleWriterShareLedger()
        manifest_set = sample_no_anchor_fee_ctv_manifest_set()
        precommitment = manifest_set["manifests"][0]["precommitment"]  # type: ignore[index]
        precommitment["anchor_vout"] = 1  # type: ignore[index]

        with self.assertRaisesRegex(ValueError, "must not include a CPFP anchor"):
            ledger.persist_ctv_fanout_manifest_set(
                block_hash="ac" * 32,
                manifest_set=manifest_set,
                manifest_set_sha256="68" * 32,
            )

    def test_ctv_backfill_input_extracts_manifest_set_from_audit_bundle(self) -> None:
        manifest_set = sample_no_anchor_fee_ctv_manifest_set()

        item = backfill_input_from_payload(
            {"ctv_fanout_manifest_set": manifest_set},
            source="audit.json",
            block_hash="aa" * 32,
        )

        self.assertEqual(item.source, "audit.json")
        self.assertEqual(item.block_hash, "aa" * 32)
        self.assertEqual(item.manifest_set, manifest_set)
        self.assertEqual(item.manifest_set_sha256, sha256_json_hex(manifest_set))

    def test_ctv_backfill_input_accepts_audit_api_wrapper(self) -> None:
        manifest_set = sample_no_anchor_fee_ctv_manifest_set()

        item = backfill_input_from_payload(
            {
                "block_hash": "bb" * 32,
                "manifest_set": manifest_set,
                "manifest_set_sha256": "cc" * 32,
            },
            source="api",
        )

        self.assertEqual(item.block_hash, "bb" * 32)
        self.assertEqual(item.manifest_set_sha256, "cc" * 32)

    def test_ctv_backfill_infers_block_hash_from_live_bundle_filename(self) -> None:
        self.assertEqual(
            infer_block_hash_from_path(Path(f"prism-live-audit-bundle-21886-{'dd' * 32}.json")),
            "dd" * 32,
        )

    def test_ctv_backfill_path_follows_live_envelope_to_compact_body_ref(self) -> None:
        manifest_set = sample_no_anchor_fee_ctv_manifest_set()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body_ref = {
                "schema": AUDIT_BODY_REF_SCHEMA,
                "bundle_without_shares": {
                    "schema": "qbit.prism.audit-bundle.v1",
                    "found_block": {"block_hash": "aa" * 32},
                    "ctv_fanout_manifest_set": manifest_set,
                },
                "share_parts": [],
                "share_count": 0,
            }
            body_path = root / f"prism-audit-bundle-body-{'aa' * 32}-{'bb' * 32}.json"
            body_path.write_text(json.dumps(body_ref), encoding="utf-8")
            envelope_path = root / f"prism-live-audit-bundle-10-{'aa' * 32}.json"
            envelope_path.write_text(
                json.dumps(
                    {
                        "schema": "qbit.prism.live-audit-bundle-envelope.v1",
                        "block_hash": "aa" * 32,
                        "body_uri": str(body_path),
                    }
                ),
                encoding="utf-8",
            )

            item = backfill_input_from_path(envelope_path)

        self.assertEqual(item.block_hash, "aa" * 32)
        self.assertEqual(item.manifest_set, manifest_set)

    def test_ctv_backfill_requires_block_hash(self) -> None:
        with self.assertRaisesRegex(ValueError, "block hash"):
            backfill_input_from_payload(
                {"ctv_fanout_manifest_set": sample_no_anchor_fee_ctv_manifest_set()},
                source="audit.json",
            )

    def test_memory_ledger_public_artifact_returns_ctv_audit_bundle(self) -> None:
        ledger = SingleWriterShareLedger()
        block_hash = "aa" * 32
        audit_bundle_sha256 = "77" * 32
        manifest_set = {
            **sample_ctv_manifest_set(),
            "audit_bundle_sha256": audit_bundle_sha256,
            "audit_bundle": {"schema": "qbit.prism.audit-bundle.v1"},
        }

        ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256="66" * 32,
        )

        self.assertEqual(
            ledger.dashboard_public_artifact(sha256=audit_bundle_sha256),
            {"schema": "qbit.prism.audit-bundle.v1"},
        )
        status = ledger.ctv_fanout_status(fanout_txid="22" * 32)
        pending = ledger.pending_ctv_fanout_statuses()

        self.assertEqual(status["audit_bundle_sha256"], audit_bundle_sha256)  # type: ignore[index]
        self.assertEqual(pending[0]["audit_bundle_sha256"], audit_bundle_sha256)

    def test_memory_ledger_public_artifact_omits_missing_ctv_audit_bundle_body(self) -> None:
        ledger = SingleWriterShareLedger()
        block_hash = "aa" * 32
        audit_bundle_sha256 = "77" * 32
        manifest_set = {
            **sample_ctv_manifest_set(),
            "audit_bundle_sha256": audit_bundle_sha256,
        }

        ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256="66" * 32,
        )

        self.assertIsNone(ledger.dashboard_public_artifact(sha256=audit_bundle_sha256))

    def test_memory_ledger_public_artifact_document_returns_hashed_text(self) -> None:
        # The canonical text is the exact byte sequence the advertised sha256
        # was computed over; serving anything else breaks external
        # verification of content-addressed artifacts.
        ledger = SingleWriterShareLedger()
        block_hash = "aa" * 32
        audit_bundle_sha256 = "77" * 32
        manifest_set = {
            **sample_ctv_manifest_set(),
            "audit_bundle_sha256": audit_bundle_sha256,
            "audit_bundle": {"schema": "qbit.prism.audit-bundle.v1"},
        }
        manifest = manifest_set["manifests"][0]  # type: ignore[index]
        manifest_set_sha256 = sha256_json_hex(manifest_set)

        ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256=manifest_set_sha256,
        )

        set_document = ledger.dashboard_public_artifact_document(sha256=manifest_set_sha256)
        manifest_document = ledger.dashboard_public_artifact_document(sha256=sha256_json_hex(manifest))
        bundle_document = ledger.dashboard_public_artifact_document(sha256=audit_bundle_sha256)

        self.assertEqual(set_document["canonical_json"], canonical_json_text(manifest_set))  # type: ignore[index]
        self.assertEqual(set_document["payload"], manifest_set)  # type: ignore[index]
        self.assertEqual(manifest_document["canonical_json"], canonical_json_text(manifest))  # type: ignore[index]
        self.assertEqual(
            hashlib.sha256(str(set_document["canonical_json"]).encode()).hexdigest(),  # type: ignore[index]
            manifest_set_sha256,
        )
        self.assertEqual(bundle_document["payload"], {"schema": "qbit.prism.audit-bundle.v1"})  # type: ignore[index]
        self.assertIsNone(bundle_document["canonical_json"])  # type: ignore[index]
        self.assertIsNone(ledger.dashboard_public_artifact_document(sha256="99" * 32))

    def test_reorg_watch_blocks_keeps_height_mature_immature_rows(self) -> None:
        ledger = QueryCapturePsqlShareLedger()

        self.assertEqual(ledger.reorg_watch_blocks(active_tip_height=10_000), [])

        self.assertIn("chain_state IN ('confirmed', 'inactive')", ledger.queries[0])
        self.assertIn("maturity_state = 'immature'", ledger.queries[0])
        self.assertNotIn("block_height + 1000", ledger.queries[0])

    def test_pool_block_state_wraps_nullable_row_in_json(self) -> None:
        ledger = CannedQueryPsqlShareLedger(
            [
                acquired_lease_result(),
                {"state": None},
                {
                    "state": {
                        "block_hash": "aa" * 32,
                        "block_height": 10,
                        "parent_hash": "bb" * 32,
                        "chain_state": "confirmed",
                        "maturity_state": "immature",
                        "audit_publication_sequence": None,
                    }
                },
            ]
        )

        self.assertIsNone(ledger.pool_block_state(block_hash="aa" * 32))
        self.assertEqual(
            ledger.pool_block_state(block_hash="aa" * 32),
            {
                "block_hash": "aa" * 32,
                "block_height": 10,
                "parent_hash": "bb" * 32,
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": None,
            },
        )
        self.assertIn("SELECT json_build_object(\n    'state'", ledger.queries[1])

    def test_postgres_publication_floor_reads_max_durable_row_ordinal(self) -> None:
        ledger = CannedQueryPsqlShareLedger(
            [
                acquired_lease_result(),
                {"audit_publication_sequence_floor": 7},
            ]
        )

        self.assertEqual(ledger.audit_publication_sequence_floor(), 7)
        query = ledger.queries[1]
        self.assertIn("MAX(audit_publication_sequence)", query)
        self.assertIn("COALESCE", query)
        self.assertIn("FROM qbit_pool_blocks", query)
        self.assertNotIn("last_value", query)

        for invalid in (None, True, "7", -1):
            with self.subTest(invalid=invalid):
                invalid_ledger = CannedQueryPsqlShareLedger(
                    [
                        acquired_lease_result(),
                        {"audit_publication_sequence_floor": invalid},
                    ]
                )
                with self.assertRaisesRegex(RuntimeError, "floor is invalid"):
                    invalid_ledger.audit_publication_sequence_floor()

    def test_prior_balances_after_pool_block_is_height_bounded(self) -> None:
        balance = {
            "recipient_id": "miner-a",
            "order_key": "miner-a",
            "p2mr_program_hex": "11" * 32,
            "balance_sats": "25",
        }
        ledger = CannedQueryPsqlShareLedger(
            [acquired_lease_result(), [balance], []]
        )

        self.assertEqual(
            ledger.prior_balances_after_pool_block(block_hash="aa" * 32),
            [{**balance, "balance_sats": 25}],
        )
        query = ledger.queries[-1]
        self.assertIn("block.block_height <= target.block_height", query)
        self.assertIn("block.chain_state = 'confirmed'", query)
        self.assertIn("carry.maturity_state <> 'reversed'", query)
        self.assertIn("ORDER BY payout_order_key, miner_id", query)
        self.assertEqual(
            ledger.prior_balances_after_pool_block(block_hash="bb" * 32),
            [],
        )

    def test_memory_ledger_rejects_mutated_ctv_fanout_artifact(self) -> None:
        ledger = SingleWriterShareLedger()
        block_hash = "aa" * 32
        manifest_set = sample_ctv_manifest_set()
        ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256="66" * 32,
        )

        with self.assertRaisesRegex(RuntimeError, "existing CTV fanout manifest set does not match payload"):
            ledger.persist_ctv_fanout_manifest_set(
                block_hash=block_hash,
                manifest_set=manifest_set,
                manifest_set_sha256="67" * 32,
            )

    def test_memory_ledger_preserves_ctv_fanout_broadcast_attempts(self) -> None:
        ledger = SingleWriterShareLedger()
        block_hash = "aa" * 32
        fanout_txid = "22" * 32
        ledger.persist_ctv_fanout_manifest_set(
            block_hash=block_hash,
            manifest_set=sample_ctv_manifest_set(),
            manifest_set_sha256="66" * 32,
        )

        ledger.record_ctv_fanout_broadcast_attempt(
            fanout_txid=fanout_txid,
            attempt_status="rejected",
            package_tx_hexes=["parent", "child"],
            package_txids=[fanout_txid, "77" * 32],
            submit_result={"tx-results": {"child": {"error": "insufficient fee"}}},
            error="insufficient fee",
        )
        status = ledger.ctv_fanout_status(fanout_txid=fanout_txid)
        pending = ledger.pending_ctv_fanout_statuses()

        self.assertIsNotNone(status)
        self.assertEqual(status["settlement_status"], "failed")  # type: ignore[index]
        self.assertEqual(status["broadcast_attempt_count"], 1)  # type: ignore[index]
        self.assertEqual(status["broadcast_attempt_detail_count"], 1)  # type: ignore[index]
        self.assertEqual(status["last_broadcast_attempt_status"], "rejected")  # type: ignore[index]
        self.assertEqual(status["last_broadcast_error"], "insufficient fee")  # type: ignore[index]
        self.assertEqual(status["broadcast_attempt_summary"]["attempt_count"], 1)  # type: ignore[index]
        self.assertEqual(status["broadcast_attempts"][0]["attempt_status"], "rejected")  # type: ignore[index]
        self.assertEqual(status["broadcast_attempts"][0]["package_tx_hexes"], ["parent", "child"])  # type: ignore[index]
        self.assertEqual(pending, [])

        ledger.update_ctv_fanout_status(fanout_txid=fanout_txid, settlement_status="confirmed")
        self.assertEqual(ledger.pending_ctv_fanout_statuses(), [])

    def test_memory_ledger_caps_ctv_fanout_broadcast_attempt_details(self) -> None:
        ledger = SingleWriterShareLedger(ctv_broadcast_attempt_detail_limit=2)
        fanout_txid = "22" * 32
        ledger.persist_ctv_fanout_manifest_set(
            block_hash="aa" * 32,
            manifest_set=sample_ctv_manifest_set(),
            manifest_set_sha256="66" * 32,
        )

        for index in range(4):
            ledger.record_ctv_fanout_broadcast_attempt(
                fanout_txid=fanout_txid,
                attempt_status="planned",
                package_txids=[fanout_txid],
                submit_result={"package_msg": "error", "submitted": False},
                error=f"transient-{index}",
            )

        status = ledger.ctv_fanout_status(fanout_txid=fanout_txid)
        assert status is not None
        self.assertEqual(status["broadcast_attempt_count"], 4)
        self.assertEqual(status["broadcast_attempt_detail_count"], 2)
        self.assertEqual(status["last_broadcast_error"], "transient-3")
        self.assertEqual([row["attempt_seq"] for row in status["broadcast_attempts"]], [3, 4])
        self.assertEqual(status["broadcast_attempt_summary"]["status_counts"]["planned"], 4)  # type: ignore[index]

    def test_memory_ledger_ctv_broadcast_pending_respects_retry_backoff(self) -> None:
        ledger = SingleWriterShareLedger(ctv_broadcast_retry_backoff_seconds=300)
        fanout_txid = "22" * 32
        ledger.persist_ctv_fanout_manifest_set(
            block_hash="aa" * 32,
            manifest_set=sample_ctv_manifest_set(),
            manifest_set_sha256="66" * 32,
        )

        ledger.record_ctv_fanout_broadcast_attempt(
            fanout_txid=fanout_txid,
            attempt_status="planned",
            package_txids=[fanout_txid],
            submit_result={"package_msg": "error", "submitted": False},
            error="transient",
        )
        self.assertEqual(ledger.pending_ctv_fanout_statuses(), [])
        self.assertEqual(ledger.dashboard_pending_fanout_rows(page=1, limit=10)["rows"], [])

        ledger._ctv_fanout_statuses[fanout_txid]["next_broadcast_attempt_at"] = (  # type: ignore[attr-defined]
            datetime.now(timezone.utc) - timedelta(seconds=1)
        )
        self.assertEqual(ledger.pending_ctv_fanout_statuses()[0]["fanout_txid"], fanout_txid)
        self.assertEqual(
            ledger.dashboard_pending_fanout_rows(page=1, limit=10)["rows"][0]["fanout_txid"],
            fanout_txid,
        )

    def test_postgres_miner_worker_query_treats_percent_and_underscore_literally(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"total_count": 0, "active_count": 0, "rows": []},
            ]
        )

        payload = ledger.dashboard_miner_worker_rows(
            recipient_id="miner_%",
            page=1,
            limit=15,
            search="rig_%",
            hide_inactive=False,
        )
        query = ledger.lease_queries[-1]

        self.assertEqual(payload["pagination"], {"page": 1, "limit": 15, "total_count": 0, "total_pages": 0})
        self.assertIn("strpos(lower(worker_name), 'rig_%') > 0", query)
        self.assertIn("left(username, 8) = 'miner_%.'", query)
        self.assertNotIn("lower(worker_name) LIKE", query)
        self.assertNotIn("username LIKE", query)

    def test_postgres_miner_worker_query_bounds_scan_to_largest_reported_window(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"total_count": 0, "active_count": 0, "rows": []},
            ]
        )

        ledger.dashboard_miner_worker_rows(
            recipient_id="miner-a",
            page=1,
            limit=15,
            search=None,
            hide_inactive=False,
        )
        query = ledger.lease_queries[-1]

        # The share scan must carry the 3-hour lower bound (the largest rollup
        # window the endpoint reports) inside the ledger subquery itself, not
        # only in the per-window FILTER clauses that follow it.
        bound = "accepted_at >= (SELECT now_at FROM bounds) - interval '3 hours'"
        self.assertIn(bound, query)
        self.assertLess(query.index("FROM qbit_share_ledger"), query.index(bound))
        self.assertLess(query.index(bound), query.index(") shares"))

    def test_writer_lease_ttl_is_configurable_in_acquire_sql(self) -> None:
        ledger = FakeLeasePsqlShareLedger([acquired_lease()], lease_ttl_seconds=42)

        self.assertEqual(ledger._lease_interval_sql, "make_interval(secs => 42.0)")
        self.assertIn("make_interval(secs => 42.0)", ledger.lease_queries[0])
        self.assertNotIn("interval '5 minutes'", ledger.lease_queries[0])

    def test_postgres_batch_sql_fences_share_and_candidate_in_one_statement(self) -> None:
        share = pending_share(1)
        record = {
            "share_seq": 7,
            "share_id": share.share_id,
            "miner_id": share.miner_id,
            "order_key": share.order_key,
            "p2mr_program_hex": share.p2mr_program_hex,
            "share_difficulty": str(share.share_difficulty),
            "network_difficulty": str(share.network_difficulty),
            "template_height": share.template_height,
            "job_id": share.job_id,
            "job_issued_at_ms": share.job_issued_at_ms,
            "accepted_at_ms": share.accepted_at_ms,
            "ntime": share.ntime,
            "credit_policy": share.credit_policy,
        }
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"records": [record]}]
        )
        intent = {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": "ab" * 32,
            "block_hex": "00",
        }

        self.assertEqual(ledger.append_batch([(share, intent)])[0].share_seq, 7)
        query = ledger.lease_queries[-1]
        self.assertIn("inserted_shares AS", query)
        self.assertIn("inserted_candidates AS", query)
        self.assertIn("qbit_block_candidate_outbox", query)
        self.assertIn("duplicate share_id payload mismatch", query)
        self.assertIn("'candidate_outbox_state'", query)
        self.assertNotIn("ledger.accepted_at IS DISTINCT", query)
        self.assertEqual(query.count("SELECT CASE"), 1)

    def test_postgres_recovery_append_returns_exact_existing_from_batch_comparator(self) -> None:
        share = pending_share(2)
        record = {
            "share_seq": 8,
            "share_id": share.share_id,
            "miner_id": share.miner_id,
            "order_key": share.order_key,
            "p2mr_program_hex": share.p2mr_program_hex,
            "share_difficulty": str(share.share_difficulty),
            "network_difficulty": str(share.network_difficulty),
            "template_height": share.template_height,
            "job_id": share.job_id,
            "job_issued_at_ms": share.job_issued_at_ms,
            "accepted_at_ms": share.accepted_at_ms,
            "ntime": share.ntime,
            "credit_policy": share.credit_policy,
            "newly_inserted": False,
        }
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"records": [record]}]
        )

        outcome = ledger.append_recovered_share(share)

        self.assertEqual(outcome.disposition, "exact_existing")
        self.assertEqual(outcome.record.share_seq, 8)
        self.assertIn("share_mismatch AS", ledger.lease_queries[-1])

    def test_postgres_recovery_append_raises_typed_payload_conflict(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "error": "duplicate share_id payload mismatch",
                    "error_kind": "share_replay_conflict",
                },
            ]
        )

        with self.assertRaises(ShareReplayConflict):
            ledger.append_recovered_share(pending_share(3))

    def test_postgres_candidate_only_intent_forces_durable_fenced_commit(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"inserted": 1}]
        )
        intent = {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": "cd" * 32,
            "block_hex": "00",
            "credit_share_on_accept": True,
        }

        self.assertTrue(ledger.persist_block_candidate_intent(intent))
        query = ledger.lease_queries[-1]
        self.assertIn("set_config('synchronous_commit', 'on', true)", query)
        self.assertIn("qbit_ledger_writer_lease", query)
        self.assertIn("qbit_block_candidate_outbox", query)
        self.assertIn("SELECT candidate_sha256, state", query)

    def test_postgres_pending_candidate_rows_keep_authoritative_outbox_key(self) -> None:
        intent = {
            "schema": "unsupported",
            "block_hex": "00",
        }
        cursor = ["2026-07-08T21:02:03.123456Z", "ef" * 32]
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                [
                    {
                        "block_hash": "ef" * 32,
                        "candidate": intent,
                        "pool_block_exists": False,
                        "cursor": cursor,
                    }
                ],
            ]
        )

        self.assertEqual(
            ledger.pending_block_candidate_rows(),
            [
                {
                    "block_hash": "ef" * 32,
                    "candidate": intent,
                    "pool_block_exists": False,
                    "cursor": cursor,
                }
            ],
        )
        query = ledger.lease_queries[-1]
        self.assertIn("'block_hash', pending.block_hash,", query)
        self.assertIn("'candidate', pending.candidate,", query)
        # The cursor is the two ordering columns, and its stamp keeps the
        # microsecond precision the column stores.
        self.assertIn(
            "'cursor', json_build_array(\n                pending.cursor_created_at,\n                pending.block_hash\n            )",
            query,
        )
        self.assertIn(
            "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'",
            query,
        )
        self.assertIn("ORDER BY created_at, block_hash", query)
        # A first page carries no keyset predicate at all.
        self.assertNotIn("(created_at, block_hash) >", query)

    def test_postgres_pending_candidate_page_uses_an_indexable_keyset(self) -> None:
        """Each page must stay one bounded range scan of the partial index.

        qbit_block_candidate_outbox_pending_idx is (created_at, block_hash)
        WHERE state = 'pending'. A row-comparison predicate on exactly those
        columns is what keeps page N of a 5,000-row backlog as cheap as page
        one; an OFFSET walk, or a predicate on either column alone, would
        not be.
        """
        ledger = FakeLeasePsqlShareLedger([acquired_lease(), []])

        self.assertEqual(
            ledger.pending_block_candidate_rows(
                limit=1024,
                after_cursor=["2026-07-08T21:02:03.123456Z", "ef" * 32],
            ),
            [],
        )

        query = ledger.lease_queries[-1]
        self.assertIn(
            "AND (created_at, block_hash) > "
            "('2026-07-08T21:02:03.123456Z'::timestamptz, "
            f"'{'ef' * 32}')",
            query,
        )
        self.assertIn("WHERE state = 'pending'", query)
        self.assertIn("ORDER BY created_at, block_hash", query)
        self.assertIn("LIMIT 1024", query)
        # A hostile cursor is a literal like any other and is escaped, not
        # interpolated raw.
        ledger.lease_results.append([])
        ledger.pending_block_candidate_rows(
            after_cursor=["2026-07-08T21:02:03.123456Z", "ef' OR true --"],
        )
        self.assertIn("'ef'' OR true --'", ledger.lease_queries[-1])

    def test_postgres_stranded_prepared_blocks_bound_depth_and_page(self) -> None:
        """The sweep read is depth-floored, state-filtered, and bounded."""
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                [
                    {
                        "block_hash": "19" * 32,
                        "block_height": "30822",
                        "parent_hash": "20" * 32,
                    }
                ],
            ]
        )

        self.assertEqual(
            ledger.stranded_prepared_blocks(
                active_tip_height=31000,
                min_depth=100,
                limit=64,
            ),
            [
                {
                    "block_hash": "19" * 32,
                    "block_height": 30822,
                    "parent_hash": "20" * 32,
                }
            ],
        )

        query = ledger.lease_queries[-1]
        self.assertIn("FROM qbit_pool_blocks", query)
        # Leads with the qbit_pool_blocks_maturity_idx columns.
        self.assertIn("WHERE maturity_state = 'immature'", query)
        self.assertIn("AND block_height <= 30900", query)
        self.assertIn("AND chain_state = 'prepared'", query)
        self.assertIn("ORDER BY block_height ASC, block_hash ASC", query)
        self.assertIn("LIMIT 64", query)
        # Nothing is fetched at all when the caller allows no rows.
        self.assertEqual(
            ledger.stranded_prepared_blocks(
                active_tip_height=31000,
                min_depth=100,
                limit=0,
            ),
            [],
        )
        self.assertIs(ledger.lease_queries[-1], query)

    def test_postgres_pending_candidate_metrics_are_aggregate_and_indexable(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "pending_count": 2,
                    "oldest_pending_age_seconds": "7.5",
                    "oldest_unattempted_age_seconds": "3.25",
                },
            ]
        )

        self.assertEqual(
            ledger.block_candidate_pending_metrics(),
            {
                "pending_count": 2,
                "oldest_pending_age_seconds": 7.5,
                "oldest_unattempted_age_seconds": 3.25,
            },
        )
        query = ledger.lease_queries[-1]
        self.assertIn("min(created_at)", query)
        self.assertIn("FILTER (WHERE attempt_count = 0)", query)
        self.assertIn("WHERE state = 'pending'", query)

    def test_postgres_candidate_attempt_is_writer_fenced(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"updated": 1}],
            writer_id="writer-a",
            writer_epoch=7,
        )

        self.assertTrue(
            ledger.mark_block_candidate_attempted(block_hash="ad" * 32)
        )
        query = ledger.lease_queries[-1]
        self.assertIn("qbit_ledger_writer_lease", query)
        self.assertIn("writer_id = 'writer-a'", query)
        self.assertIn("writer_epoch = 7", query)
        self.assertIn("attempt_count = attempt_count + 1", query)

    def _bulk_abandon_query(
        self,
        *,
        block_hashes: list[str],
        error: str = "superseded by decided height",
        abandoned: list[str] | None = None,
    ) -> tuple[FakeLeasePsqlShareLedger, tuple[str, ...], str]:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"abandoned": abandoned or []}],
            writer_id="writer-a",
            writer_epoch=7,
        )
        result = ledger.mark_block_candidates_abandoned(
            block_hashes=block_hashes,
            error=error,
        )
        return ledger, result, ledger.lease_queries[-1]

    def test_postgres_bulk_candidate_abandon_is_one_fenced_set_statement(
        self,
    ) -> None:
        """One statement per page, carrying the same fence as the per-row path.

        The point of the batch form is that a storm-sized page costs one
        fenced write rather than one per hash, so the shape assertions are
        the behaviour: exactly one outbox ``UPDATE``, and a set predicate
        rather than a disjunction of equalities.
        """
        _, _, query = self._bulk_abandon_query(
            block_hashes=[BULK_PENDING_A, BULK_PENDING_B],
        )

        self.assertEqual(query.count("UPDATE qbit_block_candidate_outbox"), 1)
        self.assertIn("qbit_ledger_writer_lease", query)
        self.assertIn("writer_id = 'writer-a'", query)
        self.assertIn("writer_epoch = 7", query)
        self.assertIn("writer_session_token = ", query)
        self.assertIn(
            f"block_hash = ANY(ARRAY['{BULK_PENDING_A}', '{BULK_PENDING_B}']::text[])",
            query,
        )
        self.assertIn("AND state = 'pending'", query)
        self.assertIn("SET state = 'abandoned'", query)
        self.assertIn("last_error = 'superseded by decided height'", query)
        self.assertIn("updated_at = clock_timestamp()", query)
        self.assertIn("completed_at = clock_timestamp()", query)
        self.assertIn("candidate = NULL", query)
        self.assertIn("RETURNING block_hash", query)
        self.assertIn("'writer lease is not active'", query)

    def test_postgres_bulk_candidate_abandon_re_checks_pool_blocks(self) -> None:
        """The landed-block fact is re-asked inside the write, not trusted.

        ``pool_block_exists`` from a page read is a fact about the moment of
        that read. A candidate can land between the read and the write, and
        the pool-block row is what records that it did, so the terminal
        statement carries its own ``NOT EXISTS`` arm under the writer fence.
        """
        _, _, query = self._bulk_abandon_query(block_hashes=[BULK_PENDING_A])

        self.assertIn("AND NOT EXISTS (", query)
        self.assertIn("FROM qbit_pool_blocks pool", query)
        self.assertIn(
            "WHERE pool.block_hash = qbit_block_candidate_outbox.block_hash",
            query,
        )

    def test_postgres_bulk_candidate_abandon_returns_the_rows_it_won(self) -> None:
        """``RETURNING`` decides the report, so the request cannot inflate it."""
        _, abandoned, _ = self._bulk_abandon_query(
            block_hashes=[BULK_PENDING_A, BULK_PENDING_B, BULK_SUBMITTED],
            abandoned=[BULK_PENDING_A],
        )

        self.assertEqual(abandoned, (BULK_PENDING_A,))

    def test_postgres_bulk_candidate_abandon_reports_an_empty_win(self) -> None:
        _, abandoned, query = self._bulk_abandon_query(
            block_hashes=[BULK_PENDING_A],
            abandoned=[],
        )

        self.assertEqual(abandoned, ())
        # The empty case is the aggregate's, not a missing key: an outer
        # json_agg over zero rows is NULL and would decode as None.
        self.assertIn("'[]'::json", query)

    def test_postgres_bulk_candidate_abandon_normalizes_the_target_set(
        self,
    ) -> None:
        """Mixed case and duplicates resolve to one deterministic target set.

        Rows are keyed on the lowercase hash, so a page naming the same row
        twice in two cases must ask for it once. Sorting is what makes the
        generated statement a function of the set rather than of the caller's
        iteration order.
        """
        _, _, query = self._bulk_abandon_query(
            block_hashes=[
                BULK_PENDING_B,
                BULK_PENDING_A.upper(),
                BULK_PENDING_A,
                BULK_PENDING_B.upper(),
            ],
        )

        self.assertIn(
            f"ANY(ARRAY['{BULK_PENDING_A}', '{BULK_PENDING_B}']::text[])",
            query,
        )
        self.assertEqual(query.count(BULK_PENDING_A), 1)
        self.assertNotIn(BULK_PENDING_A.upper(), query)

    def test_postgres_bulk_candidate_abandon_quotes_the_reason(self) -> None:
        _, _, query = self._bulk_abandon_query(
            block_hashes=[BULK_PENDING_A],
            error="height 'H' decided by another block",
        )

        self.assertIn(
            "last_error = 'height ''H'' decided by another block'",
            query,
        )

    def test_postgres_bulk_candidate_abandon_on_an_empty_page_issues_no_query(
        self,
    ) -> None:
        """An empty page must not reach the server at all.

        ``ANY(ARRAY[])`` has no inferable element type and would fail to
        parse, so the guard is not an optimization: the alternative is a
        malformed statement.
        """
        ledger = FakeLeasePsqlShareLedger([acquired_lease()])
        queries_before = len(ledger.lease_queries)

        self.assertEqual(
            ledger.mark_block_candidates_abandoned(block_hashes=[], error="unused"),
            (),
        )

        self.assertEqual(len(ledger.lease_queries), queries_before)

    def test_postgres_bulk_candidate_abandon_fails_on_inactive_writer_lease(
        self,
    ) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"error": "writer lease is not active"}]
        )

        with self.assertRaisesRegex(RuntimeError, "writer lease is not active"):
            ledger.mark_block_candidates_abandoned(
                block_hashes=[BULK_PENDING_A, BULK_PENDING_B],
                error="superseded by decided height",
            )

    def test_postgres_pending_candidate_page_carries_pool_block_existence(
        self,
    ) -> None:
        """One page read answers the landed-block question for every row.

        Asking per row is one round trip per row, and the page read exists
        precisely for backlogs where that is the whole cost. The probe sits
        outside the limited subquery so it runs at most ``limit`` times
        rather than once per pending row in the backlog.
        """
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                [
                    {
                        "block_hash": BULK_PENDING_A,
                        "candidate": {"block_hash_hex": BULK_PENDING_A},
                        "pool_block_exists": False,
                        "cursor": ["2026-08-20T00:00:00.000001Z", BULK_PENDING_A],
                    },
                    {
                        "block_hash": BULK_PENDING_B,
                        "candidate": {"block_hash_hex": BULK_PENDING_B},
                        "pool_block_exists": True,
                        "cursor": ["2026-08-20T00:00:00.000002Z", BULK_PENDING_B],
                    },
                ],
            ]
        )

        rows = ledger.pending_block_candidate_rows(limit=32)

        self.assertEqual(
            [row["pool_block_exists"] for row in rows],
            [False, True],
        )
        query = ledger.lease_queries[-1]
        self.assertIn("'pool_block_exists', EXISTS (", query)
        self.assertIn("FROM qbit_pool_blocks pool", query)
        self.assertIn("WHERE pool.block_hash = pending.block_hash", query)
        # Outside the LIMITed subquery: the probe is bounded by the page, not
        # by the backlog behind it.
        self.assertLess(
            query.index("'pool_block_exists', EXISTS ("),
            query.index("FROM (\n    SELECT"),
        )
        self.assertIn("LIMIT 32", query)

    def test_postgres_pending_candidate_page_without_existence_fails_closed(
        self,
    ) -> None:
        """A page that cannot report the fact raises instead of implying false."""
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                [{"block_hash": BULK_PENDING_A, "candidate": {}, "cursor": [1, "a"]}],
            ]
        )

        with self.assertRaisesRegex(RuntimeError, "pool block existence"):
            ledger.pending_block_candidate_rows(limit=32)

    def test_postgres_pending_candidate_page_propagates_read_failure(self) -> None:
        """A failed page read must not read as an empty backlog."""

        class FailingPageLedger(FakeLeasePsqlShareLedger):
            def _run_retry_safe_read_json(self, sql: str) -> Any:
                raise RuntimeError("connection reset by peer")

        ledger = FailingPageLedger([acquired_lease()])

        with self.assertRaisesRegex(RuntimeError, "connection reset by peer"):
            ledger.pending_block_candidate_rows(limit=32)

    def test_writer_lease_ttl_defaults_to_sixty_seconds(self) -> None:
        ledger = FakeLeasePsqlShareLedger([acquired_lease()])

        self.assertEqual(ledger._lease_interval_sql, "make_interval(secs => 60.0)")

    def test_writer_lease_ttl_must_be_finite_positive(self) -> None:
        for value in (0, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "lease_ttl_seconds"):
                PsqlShareLedger(psql_command="psql postgresql://example.invalid/qbit", lease_ttl_seconds=value)

    def test_writer_lease_adoption_silence_must_be_finite_positive(self) -> None:
        for value in (0, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError,
                "lease_adoption_silence_seconds",
            ):
                PsqlShareLedger(
                    psql_command="psql postgresql://example.invalid/qbit",
                    lease_adoption_silence_seconds=value,
                )

    def test_release_writer_lease_expires_only_held_identity(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"released": 1}],
            writer_id="writer-a",
            writer_epoch=7,
        )

        self.assertTrue(ledger.release_writer_lease())

        query = ledger.lease_queries[-1]
        self.assertIn("UPDATE qbit_ledger_writer_lease", query)
        self.assertIn("lease_expires_at = clock_timestamp() - interval '1 second'", query)
        self.assertIn("qbit_ledger_writer_lease.singleton", query)
        self.assertIn("writer_session_token = data->>'writer_session_token'", query)
        self.assertIn("writer-a", query)

    def test_release_writer_lease_returns_false_when_not_held(self) -> None:
        ledger = FakeLeasePsqlShareLedger([acquired_lease(), {"released": 0}])

        self.assertFalse(ledger.release_writer_lease())

    def test_watchdog_release_uses_fresh_psql_connection_path(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._writer_id = "writer-a"
        ledger._writer_epoch = 7
        ledger._writer_session_token = "session-a"

        with unittest.mock.patch.object(
            ledger,
            "_run_fenced_json",
            side_effect=AssertionError("shared fenced path must not run"),
        ) as fenced, unittest.mock.patch.object(
            ledger,
            "_run_sql",
            return_value='{"released": 1}\n',
        ) as fresh:
            self.assertTrue(ledger.release_writer_lease_fresh_connection())

        fenced.assert_not_called()
        fresh.assert_called_once()
        self.assertIn("UPDATE qbit_ledger_writer_lease", fresh.call_args.args[0])

    def test_renew_writer_lease_refreshes_only_held_identity(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"backend": "postgres-psql", "renewed_count": 1}],
            writer_id="writer-a",
            writer_epoch=7,
        )

        self.assertEqual(
            ledger.renew_writer_lease(),
            {"backend": "postgres-psql", "renewed_count": 1},
        )

        query = ledger.lease_queries[-1]
        self.assertIn("UPDATE qbit_ledger_writer_lease", query)
        self.assertIn(
            "lease_expires_at = clock_timestamp() + make_interval(secs => 60.0)",
            query,
        )
        self.assertIn("qbit_ledger_writer_lease.singleton", query)
        self.assertIn("writer_session_token = data->>'writer_session_token'", query)
        self.assertIn("writer-a", query)
        self.assertNotIn("qbit_ctv_fanout_artifacts", query)

    def test_renew_writer_lease_raises_when_fenced_out(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease(), {"error": "writer lease is not active"}]
        )

        with self.assertRaisesRegex(RuntimeError, "writer lease is not active"):
            ledger.renew_writer_lease()

    @staticmethod
    def guarded_verification_ledger(
        guard: Any,
        *,
        session: str = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}session-a",
    ) -> PsqlShareLedger:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._writer_id = "writer-a"
        ledger._writer_epoch = 7
        ledger._writer_session_token = session
        ledger._pool_application_name = "qbit-prism-writer-test-pool"
        ledger._lease_interval_sql = "make_interval(secs => 60.0)"
        ledger._lease_authority_margin_sql = "make_interval(secs => 30.0)"
        ledger._writer_lease_guard = guard
        return ledger

    class InSlotFakeGuard:
        """Mimic the native guard's run_json slot semantics for fakes.

        One serialized-slot acquisition per run_json call, with followup
        statements executed inside that same acquisition — the property
        the attribution recheck relies on so its extra statement can never
        queue behind other guard callers. Subclasses supply the per-
        statement result via result_for(statement_index).
        """

        held = True

        def __init__(self) -> None:
            self.slot_acquisitions = 0
            self.query_starts = 0
            self.statements: list[str] = []

        def result_for(self, index: int) -> dict[str, object]:
            raise NotImplementedError

        def run_json(
            self,
            sql: str,
            *,
            on_query_start: Any = None,
            followup: Any = None,
        ) -> dict[str, object]:
            self.slot_acquisitions += 1
            if on_query_start is not None:
                self.query_starts += 1
                on_query_start()
            self.statements.append(sql)
            result = self.result_for(len(self.statements) - 1)
            while followup is not None:
                next_sql = followup(result)
                if next_sql is None:
                    break
                self.statements.append(next_sql)
                result = self.result_for(len(self.statements) - 1)
            return result

    def test_guard_session_verification_is_non_blocking_and_exact_session(self) -> None:
        class FakeGuard:
            held = True

            def __init__(self) -> None:
                self.statements: list[str] = []

            def run_json(
                self,
                sql: str,
                *,
                on_query_start: Any = None,
                followup: Any = None,
            ) -> dict[str, object]:
                self.statements.append(sql)
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 1,
                    "lease_expired": False,
                }

        guard = FakeGuard()
        ledger = self.guarded_verification_ledger(guard)

        with unittest.mock.patch.object(
            ledger,
            "_run_fenced_json",
            side_effect=AssertionError("shared fenced path must not run"),
        ) as fenced:
            self.assertEqual(
                ledger.verify_writer_lease_guard_session(),
                {
                    "backend": "postgres-psql",
                    "verified_count": 1,
                    "renewed_count": 1,
                    "renewal_deferred_to_own_write": False,
                },
            )

        fenced.assert_not_called()
        self.assertEqual(len(guard.statements), 1)
        statement = guard.statements[0]
        # The whole point: liveness must never wait on the lease tuple's row
        # lock, which fenced writes hold for entire transactions. The only
        # tuple-lock acquisition is the opportunistic TTL renewal, and it must
        # skip an already-locked row instead of queueing behind it.
        self.assertIn("FOR NO KEY UPDATE SKIP LOCKED", statement)
        self.assertNotIn("FOR UPDATE", statement)
        self.assertIn("pg_locks", statement)
        self.assertIn("pg_backend_pid()", statement)
        self.assertIn("qbit_ledger_writer_lease", statement)
        self.assertIn(f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}session-a", statement)
        # The statement must also report committed-row expiry so a skipped
        # renewal over an expired lease can fail closed.
        self.assertIn("lease_expires_at <= clock_timestamp()", statement)
        # And the remaining-TTL authority margin, so an own-write skip over
        # a nearly-lapsed row defers external side effects before its
        # authority degenerates into a rollback-dependent argument.
        self.assertIn(
            "<= clock_timestamp() + make_interval(secs => 30.0)",
            statement,
        )
        # And it must attribute the lease tuple's locker, so this process's
        # own fenced write outlasting the TTL is not mistaken for a
        # competing expiry claim: the committed row's xmax is the locker's
        # transaction id, and a pg_stat_activity backend running it under
        # the pool's unique application_name is our own write.
        self.assertIn("pg_stat_activity", statement)
        self.assertIn("backend_xid = qbit_ledger_writer_lease.xmax", statement)
        self.assertIn("qbit-prism-writer-test-pool", statement)

    def test_guard_verification_renews_idle_lease_ttl_for_exact_identity(self) -> None:
        """An idle coordinator's heartbeat must keep lease_expires_at ahead.

        With the CTV broadcaster disabled and no fenced writes for a full
        lease TTL, this heartbeat is the only writer-side refresh left. If it
        only read, the singleton row would expire while the coordinator is
        alive and any different-identity claimant could seize it through the
        expiry CAS — that identity's advisory-lock key differs, so the live
        guard would not block it.
        """

        class FakeGuard:
            held = True

            def __init__(self) -> None:
                self.statements: list[str] = []

            def run_json(
                self,
                sql: str,
                *,
                on_query_start: Any = None,
                followup: Any = None,
            ) -> dict[str, object]:
                self.statements.append(sql)
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 1,
                }

        guard = FakeGuard()
        ledger = self.guarded_verification_ledger(guard)

        result = ledger.verify_writer_lease_guard_session()

        self.assertEqual(result["renewed_count"], 1)
        statement = guard.statements[0]
        self.assertIn(
            "lease_expires_at = clock_timestamp() + make_interval(secs => 60.0)",
            statement,
        )
        self.assertIn("updated_at = clock_timestamp()", statement)
        # Renewal is fenced to this exact identity; it can never extend a
        # lease row another writer already took over.
        self.assertIn("writer_id = data->>'writer_id'", statement)
        self.assertIn("writer_epoch = (data->>'writer_epoch')::bigint", statement)
        self.assertIn(
            "writer_session_token = data->>'writer_session_token'",
            statement,
        )

    def test_guard_session_verification_fails_closed_on_lost_lock_or_token(self) -> None:
        for missing_field, message in (
            ("guard_advisory_lock_held", "advisory lock is no longer held"),
            ("writer_session_token_current", "writer lease is not active"),
        ):
            with self.subTest(missing_field=missing_field):
                class FakeGuard:
                    held = True

                    def run_json(
                        self,
                        sql: str,
                        *,
                        on_query_start: Any = None,
                        followup: Any = None,
                    ) -> dict[str, object]:
                        return {
                            "backend": "postgres-psql",
                            "guard_advisory_lock_held": True,
                            "writer_session_token_current": True,
                            missing_field: False,
                        }

                ledger = self.guarded_verification_ledger(FakeGuard())
                with self.assertRaisesRegex(RuntimeError, message):
                    ledger.verify_writer_lease_guard_session()

    def test_guard_session_verification_requires_live_guard(self) -> None:
        class ClosedGuard:
            held = False

            def run_json(self, sql: str) -> dict[str, object]:
                raise AssertionError("closed guard must not be queried")

        ledger = self.guarded_verification_ledger(ClosedGuard())
        with self.assertRaisesRegex(RuntimeError, "not heartbeat-capable"):
            ledger.verify_writer_lease_guard_session()

    def test_guard_verification_survives_lease_row_lock_held_by_fenced_write(self) -> None:
        """A fenced write holds the lease tuple past the guard statement timeout.

        Simulates the production PostgreSQL behavior behind block 39416: any
        guarded statement that waits on the qbit_ledger_writer_lease tuple
        lock while persist_accepted_block's transaction holds it dies with
        SQLSTATE 57014. The old heartbeat renewal did exactly that; the
        SKIP LOCKED verification must keep succeeding for the whole
        transaction, skipping the TTL renewal instead of queueing for it.
        """
        accepted_block_row_lock = threading.Lock()

        class RowLockEnforcingGuard:
            held = True

            def __init__(self) -> None:
                self.statements: list[str] = []

            def run_json(
                self,
                sql: str,
                *,
                on_query_start: Any = None,
                followup: Any = None,
            ) -> dict[str, object]:
                if on_query_start is not None:
                    on_query_start()
                self.statements.append(sql)
                takes_lease_tuple_lock = "qbit_ledger_writer_lease" in sql and (
                    "UPDATE" in sql or "DELETE" in sql or "FOR UPDATE" in sql
                )
                lease_tuple_locked = accepted_block_row_lock.locked()
                if takes_lease_tuple_lock and lease_tuple_locked:
                    if "SKIP LOCKED" not in sql:
                        raise RuntimeError(
                            "canceling statement due to statement timeout\n"
                            'CONTEXT:  while updating tuple (0,1) in relation '
                            '"qbit_ledger_writer_lease"'
                        )
                    # SKIP LOCKED sees the held tuple lock and moves on
                    # without waiting: the renewal simply does not happen.
                    renewed = 0
                else:
                    renewed = 1 if takes_lease_tuple_lock else 0
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": renewed,
                    # A healthy heartbeat kept the TTL ~a full lease ahead
                    # before the fenced write took the tuple lock.
                    "lease_expired": False,
                    # pg_stat_activity attributes the held tuple lock to
                    # this process's own pooled backend.
                    "lease_locked_by_this_process": lease_tuple_locked,
                }

        guard = RowLockEnforcingGuard()
        ledger = self.guarded_verification_ledger(guard)

        with accepted_block_row_lock:
            # Control: the pre-fix lease-row renewal dies on the tuple lock.
            with self.assertRaisesRegex(RuntimeError, "statement timeout"):
                ledger._renew_writer_lease_with(guard.run_json)
            # The corrected heartbeat keeps proving liveness throughout,
            # skipping the TTL renewal while the fenced write holds the row.
            for _ in range(3):
                result = ledger.verify_writer_lease_guard_session()
                self.assertEqual(result["verified_count"], 1)
                self.assertEqual(result["renewed_count"], 0)

        # Once the fenced transaction commits (and refreshes the TTL itself),
        # the very next heartbeat resumes renewing on the guard session.
        result = ledger.verify_writer_lease_guard_session()
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["renewed_count"], 1)

    def test_guard_verification_fails_closed_on_expired_lease_with_blocked_renewal(
        self,
    ) -> None:
        """An expired row plus a lock-blocked renewal is not proof of liveness.

        The tuple lock that made SKIP LOCKED skip may belong to a
        different-identity _try_acquire_writer_lease taking the expired row
        through its expiry CAS; the committed snapshot still names this
        session until that claim commits (reachable after host suspend,
        where the monotonic freshness gate cannot be trusted either). The
        queueing renewal used to detect this by re-evaluating after the
        claimant committed; the non-blocking spelling must fail closed
        instead of trusting the stale token read. The fake omits
        lease_locked_by_this_process to prove an absent locker attribution
        also defaults closed.
        """

        class ExpiredContendedGuard(self.InSlotFakeGuard):
            def result_for(self, index: int) -> dict[str, object]:
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 0,
                    "lease_expired": True,
                }

        guard = ExpiredContendedGuard()
        ledger = self.guarded_verification_ledger(guard)
        with self.assertRaisesRegex(
            RuntimeError,
            "expired and its renewal was lock-blocked",
        ):
            ledger.verify_writer_lease_guard_session()
        # The ambiguous shape earns exactly one in-slot attribution recheck
        # before the fail-closed stands.
        self.assertEqual(len(guard.statements), 2)
        self.assertEqual(guard.slot_acquisitions, 1)

    def test_guard_verification_survives_own_fenced_write_outlasting_ttl(self) -> None:
        """A fenced write outlasting the TTL must not hard-exit the coordinator.

        persist_accepted_block holds the lease tuple lock for its whole
        autocommit statement, so the heartbeat cannot renew the TTL while it
        runs; a statement outlasting the remaining TTL leaves the committed
        row expired with the renewal lock-blocked. When pg_stat_activity
        attributes that tuple lock to one of this process's own pooled
        backends, failing closed would roll back the valid write and
        restart-loop on every similarly slow block — and it is unnecessary:
        the exclusive tuple lock means no expiry claim can be in flight, and
        the write's own commit refreshes lease_expires_at before any queued
        claimant re-evaluates its expiry CAS.

        Liveness is all the exemption grants. The survival argument assumes
        the write commits — a rollback hands the expired row to a queued
        claimant — so the result must flag the deferral for external
        side-effect fences to withhold guarded RPCs until a renewal lands.
        """

        class OwnLongFencedWriteGuard:
            held = True

            def run_json(
                self,
                sql: str,
                *,
                on_query_start: Any = None,
                followup: Any = None,
            ) -> dict[str, object]:
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 0,
                    "lease_expired": True,
                    "lease_locked_by_this_process": True,
                }

        ledger = self.guarded_verification_ledger(OwnLongFencedWriteGuard())
        result = ledger.verify_writer_lease_guard_session()
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["renewed_count"], 0)
        self.assertIs(result["renewal_deferred_to_own_write"], True)

    def test_guard_verification_defers_own_write_skip_inside_authority_margin(
        self,
    ) -> None:
        """An own-write skip with a thin remaining TTL defers before expiry.

        The TTL erodes exactly while a long own fenced write withholds
        renewals, so an external side effect authorized on a nearly-lapsed
        committed row can outlive it mid-RPC — from expiry onward its
        authority is the same rollback-dependent argument the expired-case
        deferral rejects. The margin probe engages the deferral once less
        than half the lease TTL remains, while the row is still unexpired
        and the heartbeat keeps treating the session as live.
        """

        class ErodedMarginGuard:
            held = True

            def run_json(
                self,
                sql: str,
                *,
                on_query_start: Any = None,
                followup: Any = None,
            ) -> dict[str, object]:
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 0,
                    "lease_expired": False,
                    "lease_expiring_within_authority_margin": True,
                    "lease_locked_by_this_process": True,
                }

        ledger = self.guarded_verification_ledger(ErodedMarginGuard())
        result = ledger.verify_writer_lease_guard_session()
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["renewed_count"], 0)
        self.assertIs(result["renewal_deferred_to_own_write"], True)

    def test_guard_verification_authorizes_own_write_skip_with_healthy_margin(
        self,
    ) -> None:
        """A fresh-TTL own-write skip keeps authorizing external effects.

        Every fenced commit refreshes the TTL, so under steady append
        traffic the lease tuple is frequently locked by this process while
        the committed row still has most of a lease ahead. Deferring those
        skips would withhold submitblock and broadcasts behind saturated
        append traffic; the committed row's own unexpired validity — with
        at least half the TTL of runway — is standalone authority that
        needs no assumption about the in-flight write's fate.
        """

        class FreshTtlOwnWriteGuard:
            held = True

            def run_json(
                self,
                sql: str,
                *,
                on_query_start: Any = None,
                followup: Any = None,
            ) -> dict[str, object]:
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 0,
                    "lease_expired": False,
                    "lease_expiring_within_authority_margin": False,
                    "lease_locked_by_this_process": True,
                }

        ledger = self.guarded_verification_ledger(FreshTtlOwnWriteGuard())
        progress_marks: list[int] = []
        result = ledger.verify_writer_lease_guard_session(
            on_statement_progress=lambda: progress_marks.append(1),
        )
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["renewed_count"], 0)
        self.assertIs(result["renewal_deferred_to_own_write"], False)
        # A single-statement verification never reports statement progress.
        self.assertEqual(progress_marks, [])

    def test_lease_authority_margin_covers_guarded_rpc_deadlines(self) -> None:
        """The deferral margin floors at TTL/2 and rises with RPC deadlines.

        The margin must cover the longest RPC the lease fence can
        authorize, or an effect authorized just above the margin outlives
        its runway and degenerates into rollback-dependent authority. The
        floor keeps the deferral engaged through the eroded tail of a long
        own write even when configured deadlines are short.
        """
        resolve = PsqlShareLedger._resolve_lease_authority_margin_seconds
        self.assertEqual(resolve(60.0, None), 30.0)
        self.assertEqual(resolve(60.0, 20.0), 30.0)
        self.assertEqual(resolve(60.0, 45.0), 45.0)
        with self.assertRaises(ValueError):
            resolve(60.0, float("nan"))
        with self.assertRaises(ValueError):
            resolve(60.0, -1.0)

    def test_lease_authority_margin_reaching_the_ttl_is_rejected(self) -> None:
        """A margin at or above the TTL is a startup error, not a policy.

        The deferral only gates renewal skips; a verification over an
        uncontended row renews and authorizes unconditionally, and a
        landed renewal's runway is exactly one TTL. When the guarded
        effect's deadline can reach the TTL, even a freshly renewed lease
        cannot outlast the effect, so defer-every-skip would narrow but
        never close the authorize-then-expire window. Construction must
        refuse the configuration instead of running with a silent
        split-brain hazard.
        """
        resolve = PsqlShareLedger._resolve_lease_authority_margin_seconds
        for margin in (60.0, 90.0):
            with self.subTest(margin=margin):
                with self.assertRaisesRegex(
                    ValueError,
                    "must stay below lease_ttl_seconds",
                ):
                    resolve(60.0, margin)

    def test_guard_verification_rechecks_when_own_commit_clears_locker_attribution(
        self,
    ) -> None:
        """An own-write commit racing the locker probe must not hard-exit.

        The verification statement's lease-row reads share one MVCC
        snapshot while pg_stat_activity reports live backend state. When
        the writer's own fenced write commits after the SKIP LOCKED renewal
        skipped its locked row but before the probe runs, backend_xid has
        already cleared and the snapshot still shows the old expired tuple:
        renewal skipped, row expired, no attributable locker — the exact
        shape of a competing claim. That commit refreshed the TTL, so a
        fresh statement's snapshot renews normally; hard-exiting here would
        restart-loop the coordinator on the very writes the own-lock
        exemption exists to survive.
        """

        class CommitRacedGuard(self.InSlotFakeGuard):
            def result_for(self, index: int) -> dict[str, object]:
                if index == 0:
                    # First statement: snapshot taken before the own write's
                    # commit, probe run after it — locker unattributable.
                    return {
                        "backend": "postgres-psql",
                        "guard_advisory_lock_held": True,
                        "writer_session_token_current": True,
                        "lease_renewed_count": 0,
                        "lease_expired": True,
                        "lease_locked_by_this_process": False,
                    }
                # Recheck: the fresh snapshot sees the committed refresh and
                # the uncontended row renews.
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 1,
                    "lease_expired": False,
                }

        guard = CommitRacedGuard()
        ledger = self.guarded_verification_ledger(guard)
        progress_marks: list[int] = []
        result = ledger.verify_writer_lease_guard_session(
            on_query_start=lambda: None,
            on_statement_progress=lambda: progress_marks.append(1),
        )
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["renewed_count"], 1)
        self.assertIs(result["renewal_deferred_to_own_write"], False)
        self.assertEqual(len(guard.statements), 2)
        # The recheck reported the completed first round trip, so liveness
        # monitors sized for one statement can count it as progress.
        self.assertEqual(progress_marks, [1])
        # The recheck runs inside the same serialized slot acquisition: it
        # can never queue behind other guard callers, so the only cost
        # charged to the caller's execution budget is one more statement,
        # and callers budgeting queue wait via on_query_start see it fire
        # exactly once.
        self.assertEqual(guard.slot_acquisitions, 1)
        self.assertEqual(guard.query_starts, 1)

    def test_guard_verification_recheck_still_fails_closed_on_real_contention(
        self,
    ) -> None:
        """The recheck is bounded and never converts contention to liveness.

        A different-identity expiry claim still in flight shows the same
        expired-locked-unattributable shape on every fresh snapshot; the
        second identical read must raise, not loop.
        """

        class ContendedGuard(self.InSlotFakeGuard):
            def result_for(self, index: int) -> dict[str, object]:
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 0,
                    "lease_expired": True,
                    "lease_locked_by_this_process": False,
                }

        guard = ContendedGuard()
        ledger = self.guarded_verification_ledger(guard)
        with self.assertRaisesRegex(
            RuntimeError,
            "expired and its renewal was lock-blocked",
        ):
            ledger.verify_writer_lease_guard_session()
        self.assertEqual(len(guard.statements), 2)
        self.assertEqual(guard.slot_acquisitions, 1)

    def test_guard_verification_recheck_detects_completed_takeover(self) -> None:
        """A takeover committing mid-verification fails closed on identity.

        When the ambiguous first read was a competing claim that then
        committed, the recheck's fresh snapshot no longer matches this
        exact session and must raise the fenced-out error rather than
        re-reporting the stale lock-blocked one.
        """

        class TakeoverGuard(self.InSlotFakeGuard):
            def result_for(self, index: int) -> dict[str, object]:
                if index == 0:
                    return {
                        "backend": "postgres-psql",
                        "guard_advisory_lock_held": True,
                        "writer_session_token_current": True,
                        "lease_renewed_count": 0,
                        "lease_expired": True,
                        "lease_locked_by_this_process": False,
                    }
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": False,
                }

        guard = TakeoverGuard()
        ledger = self.guarded_verification_ledger(guard)
        with self.assertRaisesRegex(RuntimeError, "writer lease is not active"):
            ledger.verify_writer_lease_guard_session()
        self.assertEqual(len(guard.statements), 2)
        self.assertEqual(guard.slot_acquisitions, 1)

    def test_guard_verification_reports_no_deferral_outside_own_lock_expiry(
        self,
    ) -> None:
        """The deferral flag is scoped to exactly the own-lock expired skip.

        A landed renewal (idle recovery included) restores full authority:
        the TTL is a lease ahead again, so external side effects need no
        deferral. A healthy skipped renewal over an unexpired row keeps the
        pre-existing contract as well.
        """
        for renewed, expired in ((1, True), (1, False), (0, False)):
            with self.subTest(renewed=renewed, expired=expired):

                class Guard:
                    held = True

                    def run_json(
                        self,
                        sql: str,
                        *,
                        on_query_start: Any = None,
                        followup: Any = None,
                    ) -> dict[str, object]:
                        return {
                            "backend": "postgres-psql",
                            "guard_advisory_lock_held": True,
                            "writer_session_token_current": True,
                            "lease_renewed_count": renewed,
                            "lease_expired": expired,
                            "lease_locked_by_this_process": True,
                        }

                ledger = self.guarded_verification_ledger(Guard())
                result = ledger.verify_writer_lease_guard_session()
                self.assertIs(result["renewal_deferred_to_own_write"], False)

    def test_guard_verification_recovers_expired_lease_when_renewal_lands(self) -> None:
        """Renewing an expired-but-uncontended row is the idle-recovery path.

        Taking the tuple lock for the renewal proves no expiry claim was in
        flight, and any claimant arriving afterwards re-evaluates against the
        refreshed row and loses its CAS. Only a renewal that could not land
        makes an expired row disqualifying.
        """

        class ExpiredUncontendedGuard:
            held = True

            def run_json(
                self,
                sql: str,
                *,
                on_query_start: Any = None,
                followup: Any = None,
            ) -> dict[str, object]:
                return {
                    "backend": "postgres-psql",
                    "guard_advisory_lock_held": True,
                    "writer_session_token_current": True,
                    "lease_renewed_count": 1,
                    "lease_expired": True,
                }

        ledger = self.guarded_verification_ledger(ExpiredUncontendedGuard())
        result = ledger.verify_writer_lease_guard_session()
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["renewed_count"], 1)

    def test_block_state_functions_refresh_configured_lease_after_sql_function(self) -> None:
        cases = (
            (
                "confirm_accepted_block",
                {"confirmed_count": 1},
                "qbit_confirm_pool_block",
                "confirmed_count",
            ),
            (
                "reject_prepared_block",
                {"rejected_count": 1},
                "qbit_reject_prepared_pool_block",
                "rejected_count",
            ),
            (
                "reverse_immature_block",
                {"reversed_count": 1},
                "qbit_reverse_immature_pool_block",
                "reversed_count",
            ),
            (
                "mark_pool_block_inactive",
                {"inactive_count": 1},
                "qbit_mark_pool_block_inactive",
                "inactive_count",
            ),
            (
                "reactivate_pool_block",
                {"reactivated_count": 1},
                "qbit_reactivate_pool_block",
                "reactivated_count",
            ),
        )
        for method_name, result, function_name, count_key in cases:
            with self.subTest(method_name=method_name):
                canned = [acquired_lease(), {"backend": "postgres-psql", **result}]
                if method_name in {
                    "confirm_accepted_block",
                    "reactivate_pool_block",
                }:
                    canned.append({"audit_publication_sequence": 7})
                ledger = FakeLeasePsqlShareLedger(
                    canned,
                    lease_ttl_seconds=42,
                )

                payload = getattr(ledger, method_name)(block_hash="aa" * 32, active_tip_height=10)
                query = next(
                    query
                    for query in ledger.lease_queries
                    if function_name in query
                )

                self.assertEqual(payload[count_key], 1)
                if method_name in {
                    "confirm_accepted_block",
                    "reactivate_pool_block",
                }:
                    self.assertEqual(payload["audit_publication_sequence"], 7)
                self.assertIn(function_name, query)
                self.assertNotIn("lease_refresh AS", query)
                self.assertIn("make_interval(secs => 42.0)", query)

    def test_postgres_read_concurrency_bounds_public_reads_without_writer_lock(self) -> None:
        ledger = BlockingReadPsqlShareLedger(read_concurrency=2)
        errors: list[BaseException] = []

        def read_artifact() -> None:
            try:
                ledger.dashboard_public_artifact(sha256="a" * 64)
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=read_artifact) for _ in range(4)]
        for thread in threads:
            thread.start()

        ledger.wait_for_started_reads(2)
        with ledger._condition:
            self.assertEqual(ledger.active_reads, 2)
            self.assertEqual(ledger.started_reads, 2)

        ledger.release_reads()
        for thread in threads:
            thread.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(ledger.started_reads, 4)
        self.assertLessEqual(ledger.max_active_reads, 2)

    def test_job_snapshot_does_not_wait_for_writer_connection_lock(self) -> None:
        ledger = QueryCapturePsqlShareLedger()
        snapshots: list[list[object]] = []
        errors: list[BaseException] = []

        def snapshot() -> None:
            try:
                snapshots.append(ledger.snapshot_at_job_issue(1_000, window_weight=8))
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        thread = threading.Thread(target=snapshot)
        with ledger._lock:
            thread.start()
            thread.join(timeout=1)
            completed_while_writer_locked = not thread.is_alive()
        thread.join(timeout=5)

        self.assertTrue(completed_while_writer_locked)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(snapshots, [[]])
        self.assertIn("WITH RECURSIVE pages", ledger.queries[0])
        self.assertIn(
            "share_seq >= (SELECT min_share_seq FROM page_cutoff)",
            ledger.queries[0],
        )
        self.assertNotIn("CROSS JOIN page_cutoff", ledger.queries[0])

    def test_postgres_job_snapshot_delta_uses_disjoint_timestamp_ranges(self) -> None:
        ledger = QueryCapturePsqlShareLedger()

        self.assertEqual(ledger.snapshot_between_job_issues(1_000, 2_000), [])

        sql = ledger.queries[0]
        self.assertIn("ledger.accepted_at >", sql)
        self.assertIn("ledger.job_issued_at >", sql)
        self.assertIn("UNION ALL", sql)
        self.assertIn("ORDER BY share_seq ASC", sql)

    def test_postgres_read_concurrency_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "read_concurrency"):
            PsqlShareLedger(psql_command="psql postgresql://example.invalid/qbit", read_concurrency=0)

    def test_schema_defines_public_dashboard_indexes_and_recursive_window(self) -> None:
        schema_path = Path(__file__).resolve().parents[1] / "crates/qbit-prism/sql/001_share_ledger.sql"
        schema = schema_path.read_text(encoding="utf-8")

        for name in (
            "qbit_pool_blocks_public_recent_idx",
            "qbit_pool_payout_entries_miner_public_history_idx",
            "qbit_share_ledger_accepted_recent_idx",
            "qbit_share_ledger_accepted_miner_recent_idx",
            "qbit_share_ledger_accepted_seq_window_idx",
            "qbit_share_ledger_accepted_block_suffix_idx",
            "qbit_payout_carry_forward_miner_public_history_idx",
            "qbit_payout_carry_forward_block_amount_idx",
        ):
            self.assertIn(name, schema)

        self.assertIn("WITH RECURSIVE pages AS", schema)
        self.assertIn("AND ledger.share_seq < pages.min_share_seq", schema)
        self.assertIn(
            "ledger.share_seq >= (SELECT min_share_seq FROM page_cutoff)",
            schema,
        )
        self.assertNotIn("CROSS JOIN page_cutoff", schema)
        # Exactly one windowed difficulty sum may exist in the schema: the
        # cutoff-bounded ranked pass of qbit_prism_window. Any other schema
        # function adopting a windowed sum must justify its scan bound here.
        self.assertEqual(schema.count("sum(ledger.share_difficulty) OVER"), 1)
        self.assertIn("ON qbit_share_ledger ((lower(right(share_id, 64))), accepted_at DESC, share_seq DESC)", schema)
        self.assertIn("ALTER COLUMN anchor_vout DROP NOT NULL", schema)
        self.assertIn("CHECK (credit_policy IS NULL OR credit_policy IN ('stale-grace'))", schema)
        self.assertIn("NOT VALID", schema)
        self.assertNotIn("DROP CONSTRAINT IF EXISTS qbit_share_ledger_credit_policy_check", schema)
        self.assertLess(schema.index("writer_epoch bigint"), schema.index("credit_policy text"))
        self.assertLess(schema.index("credit_policy text"), schema.index("CHECK (accepted OR reject_reason IS NOT NULL)"))

    def test_memory_pool_snapshot_reward_window_uses_anchor_eligible_shares(self) -> None:
        ledger = SingleWriterShareLedger()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        old_eligible = pending_share(1, share_difficulty=5, job_issued_at_ms=now_ms - 10_000, accepted_at_ms=now_ms - 9_000)
        new_eligible = pending_share(2, share_difficulty=5, job_issued_at_ms=now_ms - 8_000, accepted_at_ms=now_ms - 7_000)
        future_share = pending_share(3, share_difficulty=5, job_issued_at_ms=now_ms + 60_000, accepted_at_ms=now_ms + 60_000)
        ledger.append(old_eligible)
        ledger.append(new_eligible)
        ledger.append(future_share)

        snapshot = ledger.dashboard_pool_snapshot(current_network_difficulty="1.2", generated_at=public_api.utc_now_iso())

        self.assertEqual(snapshot["reward_window"]["requested_window_weight"], "9.6")
        self.assertEqual(snapshot["reward_window"]["included_share_count"], 2)
        self.assertEqual(
            snapshot["reward_window"]["oldest_share_accepted_at"],
            public_api.iso_datetime(datetime.fromtimestamp(old_eligible.accepted_at_ms / 1000, timezone.utc)),
        )
        self.assertEqual(
            snapshot["reward_window"]["newest_share_accepted_at"],
            public_api.iso_datetime(datetime.fromtimestamp(new_eligible.accepted_at_ms / 1000, timezone.utc)),
        )

    def test_memory_pool_snapshot_reward_window_allows_zero_difficulty(self) -> None:
        ledger = SingleWriterShareLedger()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ledger.append(pending_share(1, share_difficulty=5, job_issued_at_ms=now_ms - 10_000, accepted_at_ms=now_ms - 9_000))

        snapshot = ledger.dashboard_pool_snapshot(current_network_difficulty="0", generated_at=public_api.utc_now_iso())

        self.assertEqual(snapshot["reward_window"]["requested_window_weight"], "0")
        self.assertEqual(snapshot["reward_window"]["included_share_count"], 0)

    def test_memory_miner_reward_window_uses_prism_window_not_three_hour_rollup(self) -> None:
        ledger = SingleWriterShareLedger()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ledger.append(pending_share(1, share_difficulty=8, job_issued_at_ms=now_ms - 120_000, accepted_at_ms=now_ms - 119_000))
        ledger.append(pending_share(2, share_difficulty=2, job_issued_at_ms=now_ms - 2_000, accepted_at_ms=now_ms - 1_900))

        payload = ledger.dashboard_miner_reward_window(recipient_id="miner-2", current_network_difficulty="0.25")

        self.assertEqual(payload["accepted_difficulty"], "2")
        self.assertEqual(payload["pool_accepted_difficulty"], "2")
        self.assertEqual(payload["share_percent"], "100")

    def test_memory_leaderboard_hash_percent_uses_hashrate_share(self) -> None:
        ledger = SingleWriterShareLedger()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ledger.append(pending_share(1, share_difficulty=1, job_issued_at_ms=now_ms - 2_000, accepted_at_ms=now_ms - 1_900))
        ledger.append(pending_share(2, share_difficulty=2, job_issued_at_ms=now_ms - 1_000, accepted_at_ms=now_ms - 900))

        payload = ledger.dashboard_leaderboard(page=1, limit=15)
        pool_hashrate = Decimal(payload["totals"]["pool_hashrate_ths"])  # type: ignore[index]

        for row in payload["rows"]:  # type: ignore[index]
            expected = public_api.decimal_string(Decimal(row["hashrate_ths_3h"]) * Decimal(100) / pool_hashrate)
            self.assertEqual(row["hash_percent"], expected)

    def test_memory_dashboard_windows_exclude_future_accepted_shares(self) -> None:
        ledger = SingleWriterShareLedger()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ledger.append(pending_share(1, share_difficulty=1, job_issued_at_ms=now_ms - 2_000, accepted_at_ms=now_ms - 1_900))
        ledger.append(pending_share(2, share_difficulty=999, job_issued_at_ms=now_ms + 60_000, accepted_at_ms=now_ms + 60_000))

        snapshot = ledger.dashboard_pool_snapshot(current_network_difficulty="1", generated_at=public_api.utc_now_iso())
        leaderboard = ledger.dashboard_leaderboard(page=1, limit=15)

        self.assertEqual(snapshot["participants_3h"], 1)
        self.assertEqual(snapshot["hashrate_ths"]["h3"], public_api.hashrate_ths_from_difficulty(1, 3 * 60 * 60))
        self.assertEqual(leaderboard["totals"]["pool_accepted_share_difficulty"], "1")
        self.assertEqual([row["recipient_id"] for row in leaderboard["rows"]], ["miner-1"])

    def test_memory_prism_window_counts_partial_boundary_share(self) -> None:
        ledger = SingleWriterShareLedger()
        first = ledger.append(pending_share(1, share_difficulty=5, job_issued_at_ms=1_000, accepted_at_ms=1_000))
        second = ledger.append(pending_share(2, share_difficulty=7, job_issued_at_ms=2_000, accepted_at_ms=2_000))

        window_rows = _prism_window_shares(
            [first, second],
            anchor_job_issued_at_ms=2_000,
            requested_window_weight=Decimal("9.5"),
        )

        self.assertEqual([row.share.share_seq for row in window_rows], [2, 1])
        self.assertEqual([row.counted_difficulty for row in window_rows], [Decimal(7), Decimal("2.5")])

    def test_memory_reward_leaderboard_counts_partial_boundary_and_preserves_global_rank(self) -> None:
        ledger = SingleWriterShareLedger()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ledger.append(
            pending_share(
                1,
                share_difficulty=5,
                job_issued_at_ms=now_ms - 10_000,
                accepted_at_ms=now_ms - 9_000,
            )
        )
        ledger.append(
            pending_share(
                2,
                share_difficulty=6,
                job_issued_at_ms=now_ms - 5_000,
                accepted_at_ms=now_ms - 4_000,
            )
        )

        payload = ledger.dashboard_reward_leaderboard(
            page=1,
            limit=15,
            current_network_difficulty="1",
            recipient_id="miner-1",
        )

        self.assertEqual(payload["window"]["requested_window_weight"], "8")
        self.assertEqual(payload["window"]["counted_window_weight"], "8")
        self.assertEqual(payload["window"]["included_share_count"], 2)
        self.assertTrue(payload["window"]["is_complete"])
        self.assertEqual(payload["totals"]["pool_counted_share_difficulty"], "8")
        self.assertEqual(payload["totals"]["participant_count"], 2)
        self.assertEqual(payload["pagination"]["total_count"], 1)
        self.assertEqual(payload["rows"][0]["rank"], 2)
        self.assertEqual(payload["rows"][0]["recipient_id"], "miner-1")
        self.assertEqual(payload["rows"][0]["counted_share_difficulty"], "2")
        self.assertEqual(payload["rows"][0]["share_percent"], "25")

        searched = ledger.dashboard_reward_leaderboard(
            page=1,
            limit=15,
            current_network_difficulty="1",
            search="MINER-1",
        )
        self.assertEqual(searched["rows"][0]["rank"], 2)
        self.assertEqual(searched["totals"]["participant_count"], 2)
        self.assertEqual(searched["pagination"]["total_count"], 1)

    def test_memory_reward_leaderboard_zero_weight_is_incomplete_and_has_null_rates(self) -> None:
        ledger = SingleWriterShareLedger()

        payload = ledger.dashboard_reward_leaderboard(
            page=1,
            limit=15,
            current_network_difficulty="0",
        )

        self.assertEqual(payload["window"]["requested_window_weight"], "0")
        self.assertEqual(payload["window"]["counted_window_weight"], "0")
        self.assertFalse(payload["window"]["is_complete"])
        self.assertIsNone(payload["window"]["observed_span_seconds"])
        self.assertIsNone(payload["totals"]["pool_hashrate_ths"])
        self.assertIsNone(payload["totals"]["expected_time_to_block_seconds"])
        self.assertEqual(payload["rows"], [])

    def test_memory_reward_leaderboard_underfilled_zero_span_has_null_rates(self) -> None:
        ledger = SingleWriterShareLedger()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ledger.append(
            pending_share(
                1,
                share_difficulty=5,
                job_issued_at_ms=now_ms,
                accepted_at_ms=now_ms,
            )
        )

        payload = ledger.dashboard_reward_leaderboard(
            page=1,
            limit=15,
            current_network_difficulty="2",
        )

        self.assertEqual(payload["window"]["requested_window_weight"], "16")
        self.assertEqual(payload["window"]["counted_window_weight"], "5")
        self.assertFalse(payload["window"]["is_complete"])
        self.assertEqual(payload["window"]["observed_span_seconds"], 0)
        self.assertIsNone(payload["totals"]["pool_hashrate_ths"])
        self.assertIsNone(payload["totals"]["expected_time_to_block_seconds"])
        self.assertIsNone(payload["rows"][0]["hashrate_ths"])
        self.assertEqual(payload["rows"][0]["share_percent"], "100")

    def test_memory_reward_leaderboard_rejects_ambiguous_filters(self) -> None:
        ledger = SingleWriterShareLedger()

        with self.assertRaisesRegex(ValueError, "search and recipient_id are mutually exclusive"):
            ledger.dashboard_reward_leaderboard(
                page=1,
                limit=15,
                current_network_difficulty="1",
                search="miner",
                recipient_id="miner-1",
            )

    def test_postgres_pool_snapshot_reward_window_timestamps_come_from_window_rows(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "h1_difficulty": "0",
                    "h3_difficulty": "0",
                    "h24_difficulty": "0",
                    "participants_3h": 0,
                    "blocks_found_total": 0,
                    "prism_blocks_total": 0,
                    "total_mined_bits": 0,
                    "latest_block": None,
                    "oldest_share_accepted_at": None,
                    "newest_share_accepted_at": None,
                    "included_share_count": 0,
                },
            ]
        )

        ledger.dashboard_pool_snapshot(current_network_difficulty="1.2", generated_at=public_api.utc_now_iso())
        query = ledger.lease_queries[-1]

        self.assertIn("window_summary AS", query)
        self.assertIn("qbit_prism_window(bounds.ended_at, 9.6::numeric)", query)
        self.assertIn("FROM window_rows", query)
        self.assertIn("accepted_at >= bounds.ended_at - interval '24 hours'", query)
        self.assertIn("accepted_at <= bounds.ended_at", query)
        self.assertIn("'oldest_share_accepted_at', (SELECT oldest_share_accepted_at FROM window_summary)", query)
        self.assertIn("'included_share_count', (SELECT included_share_count FROM window_summary)", query)

    def test_postgres_reward_leaderboard_uses_one_window_and_filters_after_global_rank(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "ended_at": "2026-06-26T20:45:00Z",
                    "oldest_share_accepted_at": "2026-06-26T20:44:50Z",
                    "observed_span_seconds": 10,
                    "counted_window_weight": "8",
                    "included_share_count": 2,
                    "participant_count": 2,
                    "total_count": 1,
                    "rows": [
                        {
                            "rank": 2,
                            "recipient_id": "miner-a",
                            "display_name": None,
                            "included_share_count": 1,
                            "counted_share_difficulty": "2",
                            "share_percent": "25",
                            "blocks_found_total": 4,
                            "last_share_at": "2026-06-26T20:44:50Z",
                        }
                    ],
                },
            ]
        )

        payload = ledger.dashboard_reward_leaderboard(
            page=1,
            limit=15,
            current_network_difficulty="1",
            recipient_id="miner-a",
        )
        query = ledger.lease_queries[-1]

        self.assertEqual(query.count("qbit_prism_window("), 1)
        self.assertIn("window_rows AS MATERIALIZED", query)
        self.assertIn("ranked AS (", query)
        self.assertIn("WHERE ranked.miner_id = 'miner-a'", query)
        self.assertLess(query.index("ranked AS ("), query.index("WHERE ranked.miner_id = 'miner-a'"))
        self.assertEqual(payload["rows"][0]["rank"], 2)
        self.assertEqual(payload["rows"][0]["counted_share_difficulty"], "2")
        self.assertEqual(payload["totals"]["participant_count"], 2)
        self.assertEqual(payload["pagination"]["total_count"], 1)
        self.assertEqual(payload["window"]["counted_window_weight"], "8")
        self.assertTrue(payload["window"]["is_complete"])

    def test_postgres_dashboard_pending_fanout_rows_include_broadcast_attempts(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"total_count": 0, "rows": []},
            ]
        )

        payload = ledger.dashboard_pending_fanout_rows(page=1, limit=15)
        query = ledger.lease_queries[-1]

        self.assertEqual(payload["pagination"], {"page": 1, "limit": 15, "total_count": 0, "total_pages": 0})
        self.assertIn("'broadcast_attempts'", query)
        self.assertIn("'broadcast_attempt_summary'", query)
        self.assertIn("broadcast_attempt_count", query)
        self.assertIn("qbit_ctv_fanout_broadcast_attempts", query)
        self.assertIn("settlement_status NOT IN ('confirmed', 'reorged', 'failed')", query)
        self.assertIn("artifact.next_broadcast_attempt_at IS NULL", query)
        self.assertIn("artifact.next_broadcast_attempt_at <= clock_timestamp()", query)

    def test_postgres_ctv_broadcast_pending_respects_retry_backoff(self) -> None:
        ledger = FakeLeasePsqlShareLedger([acquired_lease(), []])

        self.assertEqual(ledger.pending_ctv_fanout_statuses(), [])
        query = ledger.lease_queries[-1]

        self.assertIn("artifact.next_broadcast_attempt_at IS NULL", query)
        self.assertIn("artifact.next_broadcast_attempt_at <= clock_timestamp()", query)
        self.assertIn("settlement_status NOT IN ('confirmed', 'reorged', 'failed')", query)

    def test_postgres_records_ctv_broadcast_summary_and_caps_detail_rows(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "backend": "postgres-psql",
                    "attempt_count": 1,
                    "updated_count": 1,
                    "broadcast_attempt_count": 5,
                    "broadcast_attempt_detail_count": 2,
                },
            ],
            ctv_broadcast_attempt_detail_limit=2,
            ctv_broadcast_retry_backoff_seconds=17,
        )

        payload = ledger.record_ctv_fanout_broadcast_attempt(
            fanout_txid="22" * 32,
            attempt_status="planned",
            package_txids=["22" * 32],
            submit_result={"package_msg": "error", "submitted": False},
            error="rpc unavailable",
        )
        query = ledger.lease_queries[-1]

        self.assertEqual(payload["attempt_count"], 1)
        self.assertIn('"attempt_detail_limit":2', query)
        self.assertIn('"retry_backoff_seconds":17', query)
        self.assertIn("last_broadcast_attempt_status = data->>'attempt_status'", query)
        self.assertIn("last_broadcast_error = data->>'error'", query)
        self.assertIn("broadcast_attempt_status_counts = jsonb_set", query)
        self.assertIn("OFFSET GREATEST((data->>'attempt_detail_limit')::integer - 1, 0)", query)
        self.assertIn("next_broadcast_attempt_at = CASE", query)
        self.assertIn("unknown CTV fanout txid", query)

    def test_postgres_ctv_broadcast_attempt_rejects_unknown_fanout(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"error": "unknown CTV fanout txid"},
            ]
        )

        with self.assertRaisesRegex(RuntimeError, "unknown CTV fanout txid"):
            ledger.record_ctv_fanout_broadcast_attempt(
                fanout_txid="22" * 32,
                attempt_status="planned",
            )

    def test_postgres_miner_earnings_block_gross_keeps_reversed_rows_in_denominator(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"total_count": 0, "rows": []},
            ]
        )

        payload = ledger.dashboard_miner_earning_rows(recipient_id="miner-a", page=1, limit=15)
        query = ledger.lease_queries[-1]

        self.assertEqual(payload["pagination"], {"page": 1, "limit": 15, "total_count": 0, "total_pages": 0})
        block_totals = query.split("),\npage_rows AS", 1)[0].split("block_totals AS (", 1)[1]
        self.assertIn("FROM qbit_payout_carry_forward", block_totals)
        self.assertIn("WHERE block_hash IN (SELECT block_hash FROM page_base)", block_totals)
        self.assertNotIn("maturity_state <> 'reversed'", block_totals)

    def test_postgres_miner_pending_maturity_sums_net_immature_onchain_outputs(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"pending_maturity_bits": 37},
            ]
        )

        self.assertEqual(ledger.dashboard_miner_pending_maturity_bits(recipient_id="miner-a"), 37)
        query = ledger.lease_queries[-1]
        self.assertIn("FROM qbit_payout_carry_forward carry", query)
        self.assertIn(
            "sum(GREATEST(carry.onchain_amount_sats - carry.settlement_fee_sats, 0))",
            query,
        )
        self.assertIn("carry.miner_id = 'miner-a'", query)
        self.assertIn("carry.action = 'onchain'", query)
        self.assertIn("carry.maturity_state = 'immature'", query)
        self.assertIn("block.chain_state = 'confirmed'", query)
        self.assertIn("block.maturity_state = 'immature'", query)

        with self.assertRaisesRegex(ValueError, "recipient_id is required"):
            ledger.dashboard_miner_pending_maturity_bits(recipient_id="")

    def test_postgres_miner_payout_rows_resolve_ctv_fanout_outputs(self) -> None:
        fanout_txid = "c5" * 32
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "total_count": 2,
                    "rows": [
                        {
                            "block_hash": "a1" * 32,
                            "block_height": 19,
                            "coinbase_txid": "b2" * 32,
                            "payout_manifest_sha256": "d4" * 32,
                            "recipient_id": "miner-a",
                            "order_key": "miner-a",
                            "p2mr_program_hex": "e5" * 32,
                            "onchain_amount_sats": 7_767_471,
                            "carry_forward_balance_sats": "0",
                            "action": "onchain",
                            "maturity_state": "immature",
                            "created_at": "2026-07-15 21:50:03+00",
                            "fanout_txid": fanout_txid,
                            "fanout_vout": 1,
                            "fanout_amount_sats": 7_767_319,
                            "fanout_fee_sats": 152,
                            "fanout_gross_amount_sats": 7_767_471,
                            "fanout_status": "awaiting_maturity",
                        },
                        {
                            "block_hash": "a2" * 32,
                            "block_height": 17,
                            "coinbase_txid": "b3" * 32,
                            "payout_manifest_sha256": "d5" * 32,
                            "recipient_id": "miner-a",
                            "order_key": "miner-a",
                            "p2mr_program_hex": "e5" * 32,
                            "onchain_amount_sats": 11_991_078,
                            "carry_forward_balance_sats": "0",
                            "action": "onchain",
                            "maturity_state": "immature",
                            "created_at": "2026-07-15 20:50:03+00",
                            "fanout_txid": None,
                            "fanout_vout": None,
                            "fanout_amount_sats": None,
                            "fanout_fee_sats": None,
                            "fanout_gross_amount_sats": None,
                            "fanout_status": None,
                        },
                    ],
                },
            ]
        )

        payload = ledger.dashboard_miner_payout_rows(recipient_id="miner-a", page=1, limit=15)
        query = ledger.lease_queries[-1]

        self.assertIn("LEFT JOIN LATERAL", query)
        self.assertIn("qbit_ctv_fanout_artifacts", query)
        self.assertIn("manifest->'precommitment'->'outputs'", query)
        self.assertIn("output.value->>'recipient_id' = page_rows.miner_id", query)
        self.assertIn("output.value->>'order_key' = page_rows.payout_order_key", query)
        self.assertIn("output.value->>'p2mr_program_hex' = encode(page_rows.p2mr_program, 'hex')", query)
        self.assertIn("page_rows.action = 'onchain'", query)

        ctv_row, direct_row = payload["rows"]
        self.assertEqual(ctv_row["transaction_kind"], "ctv_fanout")
        self.assertEqual(ctv_row["transaction_id"], fanout_txid)
        self.assertEqual(ctv_row["onchain_amount_bits"], 7_767_319)
        self.assertEqual(direct_row["transaction_kind"], "coinbase")
        self.assertEqual(direct_row["transaction_id"], "b3" * 32)
        self.assertEqual(direct_row["onchain_amount_bits"], 11_991_078)

    def test_postgres_pool_snapshot_reward_window_allows_zero_difficulty(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "h1_difficulty": "0",
                    "h3_difficulty": "0",
                    "h24_difficulty": "0",
                    "participants_3h": 0,
                    "blocks_found_total": 0,
                    "prism_blocks_total": 0,
                    "total_mined_bits": 0,
                    "latest_block": None,
                    "oldest_share_accepted_at": None,
                    "newest_share_accepted_at": None,
                    "included_share_count": 0,
                },
            ]
        )

        ledger.dashboard_pool_snapshot(current_network_difficulty="0", generated_at=public_api.utc_now_iso())
        query = ledger.lease_queries[-1]

        self.assertIn("qbit_prism_window(bounds.ended_at, 0::numeric)", query)

    def test_postgres_miner_reward_window_uses_prism_window(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"pool_counted_difficulty": "4", "miner_counted_difficulty": {"miner-a": "1", "miner-b": "3"}},
            ]
        )

        payload = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")
        query = ledger.lease_queries[-1]

        self.assertEqual(payload["accepted_difficulty"], "1")
        self.assertEqual(payload["pool_accepted_difficulty"], "4")
        self.assertEqual(payload["share_percent"], "25")
        self.assertIn("qbit_prism_window(bounds.ended_at, 9.6::numeric)", query)
        self.assertIn("json_object_agg(miner_id, counted_difficulty)", query)
        self.assertIn("GROUP BY miner_id", query)

    def test_postgres_miner_reward_window_shares_one_cached_pool_aggregate(self) -> None:
        # FakeLeasePsqlShareLedger raises on any query beyond the canned
        # results, so the second and third requests below pass only if they
        # are served from the shared cached aggregate.
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"pool_counted_difficulty": "4", "miner_counted_difficulty": {"miner-a": "1", "miner-b": "3"}},
            ]
        )

        first = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")
        queries_after_first = len(ledger.lease_queries)
        second = ledger.dashboard_miner_reward_window(recipient_id="miner-b", current_network_difficulty="1.2")
        absent = ledger.dashboard_miner_reward_window(recipient_id="miner-absent", current_network_difficulty="1.2")

        self.assertEqual(len(ledger.lease_queries), queries_after_first)
        self.assertEqual(first["share_percent"], "25")
        self.assertEqual(second["share_percent"], "75")
        self.assertEqual(absent["accepted_difficulty"], "0")
        self.assertEqual(absent["pool_accepted_difficulty"], "4")
        self.assertEqual(absent["share_percent"], "0")

    def test_postgres_miner_reward_window_recomputes_when_window_weight_changes(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"pool_counted_difficulty": "4", "miner_counted_difficulty": {"miner-a": "1"}},
                {"pool_counted_difficulty": "16", "miner_counted_difficulty": {"miner-a": "8"}},
            ]
        )

        first = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")
        second = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="2")

        self.assertEqual(first["share_percent"], "25")
        self.assertEqual(second["share_percent"], "50")
        self.assertIn("qbit_prism_window(bounds.ended_at, 16::numeric)", ledger.lease_queries[-1])

    def test_postgres_miner_reward_window_cache_expires_after_ttl(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"pool_counted_difficulty": "4", "miner_counted_difficulty": {"miner-a": "1"}},
                {"pool_counted_difficulty": "8", "miner_counted_difficulty": {"miner-a": "2"}},
            ],
            reward_window_cache_seconds=30.0,
        )
        clock = {"now": 100.0}
        with unittest.mock.patch(
            "lab.prism.share_ledger.time.monotonic", side_effect=lambda: clock["now"]
        ):
            first = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")
            clock["now"] = 129.0
            cached = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")
            clock["now"] = 131.0
            refreshed = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")

        self.assertEqual(first["pool_accepted_difficulty"], "4")
        self.assertEqual(cached["pool_accepted_difficulty"], "4")
        self.assertEqual(refreshed["pool_accepted_difficulty"], "8")

    def test_postgres_miner_reward_window_cache_can_be_disabled(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"pool_counted_difficulty": "4", "miner_counted_difficulty": {"miner-a": "1"}},
                {"pool_counted_difficulty": "8", "miner_counted_difficulty": {"miner-a": "2"}},
            ],
            reward_window_cache_seconds=0,
        )

        first = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")
        second = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")

        self.assertEqual(first["pool_accepted_difficulty"], "4")
        self.assertEqual(second["pool_accepted_difficulty"], "8")

    def test_postgres_miner_share_summary_zero_fills_empty_payload(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "accepted_3h": None,
                    "m1_difficulty": None,
                    "m5_difficulty": None,
                    "m10_difficulty": None,
                    "h3_difficulty": None,
                    "h24_difficulty": None,
                    "pool_h3_difficulty": None,
                    "last_share_at": None,
                },
            ]
        )

        payload = ledger.dashboard_miner_share_summary(recipient_id="missing-miner")

        self.assertEqual(payload["accepted_3h"], 0)
        self.assertEqual(payload["accepted_difficulty_3h"], "0")
        self.assertIsNone(payload["last_share_at"])
        self.assertIsNone(payload["share_percent"])
        self.assertEqual(payload["hashrate_ths"], {"m1": "0", "m5": "0", "m10": "0", "h3": "0", "h24": "0"})

    def test_postgres_miner_share_summary_accepts_decimal_numeric_text(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "accepted_3h": 7,
                    "m1_difficulty": "0E-9",
                    "m5_difficulty": "0.500000000",
                    "m10_difficulty": "1.000000000",
                    "h3_difficulty": "1.500000000",
                    "h24_difficulty": "2.000000000",
                    "pool_h3_difficulty": "3.000000000",
                    "last_share_at": "2026-06-26T20:44:53Z",
                },
            ]
        )

        payload = ledger.dashboard_miner_share_summary(recipient_id="miner-a")

        self.assertEqual(payload["accepted_3h"], 7)
        self.assertEqual(payload["accepted_difficulty_3h"], "1.500000000")
        self.assertEqual(payload["share_percent"], "50")
        self.assertEqual(payload["hashrate_ths"]["m1"], "0")

    def test_postgres_leaderboard_hash_percent_uses_hashrate_share(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "started_at": "2026-06-26T17:45:00Z",
                    "ended_at": "2026-06-26T20:45:00Z",
                    "total_difficulty": "3",
                    "participant_count": 2,
                    "rows": [
                        {
                            "rank": 1,
                            "recipient_id": "miner-b",
                            "display_name": None,
                            "accepted_share_difficulty": "2",
                            "share_percent": "66.66666666666666666666666666666666666667",
                            "blocks_found": 0,
                            "last_share_at": "2026-06-26T20:44:53Z",
                        },
                        {
                            "rank": 2,
                            "recipient_id": "miner-a",
                            "display_name": None,
                            "accepted_share_difficulty": "1",
                            "share_percent": "33.33333333333333333333333333333333333333",
                            "blocks_found": 0,
                            "last_share_at": "2026-06-26T20:44:52Z",
                        },
                    ],
                },
            ]
        )

        payload = ledger.dashboard_leaderboard(page=1, limit=15)
        query = ledger.lease_queries[-1]
        pool_hashrate = Decimal(payload["totals"]["pool_hashrate_ths"])  # type: ignore[index]

        self.assertIn("ledger.accepted_at <= bounds.ended_at", query)
        self.assertIn("ORDER BY accepted_share_difficulty DESC, filtered.miner_id ASC", query)
        for row in payload["rows"]:  # type: ignore[index]
            expected = public_api.decimal_string(Decimal(row["hashrate_ths_3h"]) * Decimal(100) / pool_hashrate)
            self.assertEqual(row["hash_percent"], expected)

    def test_postgres_public_block_solver_queries_use_suffix_index_expression(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "h1_difficulty": "0",
                    "h3_difficulty": "0",
                    "h24_difficulty": "0",
                    "participants_3h": 0,
                    "blocks_found_total": 0,
                    "prism_blocks_total": 0,
                    "total_mined_bits": 0,
                    "latest_block": None,
                    "oldest_share_accepted_at": None,
                    "newest_share_accepted_at": None,
                    "included_share_count": 0,
                },
                {"total_count": 0, "rows": []},
                {
                    "started_at": "2026-06-26T17:45:00Z",
                    "ended_at": "2026-06-26T20:45:00Z",
                    "total_difficulty": "0",
                    "participant_count": 0,
                    "rows": [],
                },
            ]
        )

        ledger.dashboard_pool_snapshot(current_network_difficulty="1", generated_at=public_api.utc_now_iso())
        ledger.dashboard_blocks(page=1, limit=15)
        ledger.dashboard_leaderboard(page=1, limit=15)
        queries = "\n".join(ledger.lease_queries[1:])

        self.assertNotIn("LIKE '%:' || block.block_hash", queries)
        self.assertEqual(queries.count("lower(right(share.share_id, 64)) = block.block_hash"), 3)
        self.assertEqual(queries.count("length(share.share_id) >= 65"), 3)
        # solver_worker_name is derived from the solving share's share_id in
        # the two block-facing queries (pool-snapshot latest_block and the blocks
        # table) and is no longer hardcoded null.
        self.assertNotIn("'solver_worker_name', null", queries)
        self.assertIn("regexp_replace(rows.solver_share_id, ':[^:]*$', '')", queries)
        self.assertIn("regexp_replace(latest_block.solver_share_id, ':[^:]*$', '')", queries)

    def test_postgres_dashboard_blocks_reads_bits_from_promoted_column_then_bundle(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {"total_count": 0, "rows": []},
            ]
        )

        ledger.dashboard_blocks(page=1, limit=15)
        query = ledger.lease_queries[-1]

        # Reads the promoted column first, falling back to the inline JSONB for
        # legacy (pre-externalization) rows.
        self.assertIn(
            "COALESCE(bundle.found_block_bits, bundle.audit_bundle#>>'{found_block,bits}') AS audit_bits",
            query,
        )
        self.assertIn("'bits', COALESCE(rows.audit_bits, '00000000')", query)
        self.assertNotIn("rows.audit_bundle#>>", query)

    def test_psql_externalizes_audit_body_and_resolves_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease(), None],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [{"share_seq": 1}, {"share_seq": 2}],
                "found_block": {"bits": "207fffff"},
            }
            body_sha = fake_audit_bundle_sha256(bundle)
            body_uri = ledger._externalize_audit_body("aa" * 32, body_sha, bundle)
            self.assertIsNotNone(body_uri)
            self.assertTrue(Path(str(body_uri)).is_file())
            self.assertIn(body_sha, Path(str(body_uri)).name)
            self.assertEqual(json.loads(Path(str(body_uri)).read_text(encoding="utf-8")), bundle)
            self.assertEqual(ledger._externalize_audit_body("aa" * 32, body_sha, bundle), body_uri)
            with self.assertRaisesRegex(RuntimeError, "sha256 mismatch"):
                ledger._externalize_audit_body("aa" * 32, body_sha, {**bundle, "shares": []})
            with self.assertRaisesRegex(RuntimeError, "sha256 mismatch"):
                ledger._externalize_audit_body("bb" * 32, "00" * 32, bundle)
            # A row with a NULL inline body resolves the body from the file and
            # presents the same shape as an inline row (no body_uri leaks out).
            resolved = ledger._resolve_audit_bundle_row(
                {
                    "block_hash": "aa" * 32,
                    "audit_bundle_sha256": body_sha,
                    "coinbase_tx_hex": "00",
                    "audit_bundle": None,
                    "body_uri": body_uri,
                }
            )
            assert resolved is not None
            self.assertEqual(resolved["audit_bundle"], bundle)
            self.assertNotIn("body_uri", resolved)

    def test_psql_reuses_complete_10k_share_slot_without_parse_merge_or_memory_amplification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_share_segment_size=10_000,
            )
            shares = [
                {
                    "share_seq": share_seq,
                    "share_id": f"worker-{share_seq % 128}:{share_seq:016x}",
                    "miner_id": f"miner-{share_seq % 128}",
                    "order_key": f"{share_seq:020d}",
                    "p2mr_program_hex": f"{share_seq % 256:02x}" * 32,
                    "share_difficulty": 100_000 + share_seq,
                    "network_difficulty": 1_000_000,
                    "template_height": 123_456,
                    "job_id": f"job-{share_seq // 64}",
                    "job_issued_at_ms": 1_700_000_000_000 + share_seq,
                    "accepted_at_ms": 1_700_000_001_000 + share_seq,
                    "ntime": 1_700_000_000 + share_seq,
                }
                for share_seq in range(1, 10_001)
            ]
            segment_uri, expected_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=10_000,
                first_share_seq=1,
                last_share_seq=10_000,
                shares=shares,
            )
            segment_path = Path(segment_uri)
            expected_bytes = segment_path.read_bytes()
            expected_file_sha256 = hashlib.sha256(expected_bytes).hexdigest()
            expected_mtime_ns = segment_path.stat().st_mtime_ns
            store = ledger._audit_artifact_store
            assert store is not None

            gc.collect()
            tracemalloc.start()
            try:
                with (
                    unittest.mock.patch(
                        "lab.prism.audit_artifacts.json.loads",
                        side_effect=AssertionError("complete share slot must not be parsed"),
                    ),
                    unittest.mock.patch.object(
                        store,
                        "merge_audit_share_ranges",
                        side_effect=AssertionError("complete share slot must not be merged"),
                    ),
                ):
                    reused_uri, reused_range_sha256 = ledger._write_audit_share_segment_range(
                        segment_first_share_seq=1,
                        segment_last_share_seq=10_000,
                        first_share_seq=1,
                        last_share_seq=10_000,
                        shares=shares,
                    )
                _current_bytes, peak_bytes = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()

            self.assertEqual(reused_uri, segment_uri)
            self.assertEqual(reused_range_sha256, expected_range_sha256)
            self.assertEqual(segment_path.read_bytes(), expected_bytes)
            self.assertEqual(hashlib.sha256(segment_path.read_bytes()).hexdigest(), expected_file_sha256)
            self.assertEqual(segment_path.stat().st_mtime_ns, expected_mtime_ns)
            self.assertLess(
                peak_bytes,
                4 * len(expected_bytes),
                f"complete-slot reuse peaked at {peak_bytes} bytes for a {len(expected_bytes)}-byte slot",
            )

    def test_psql_segment_slow_path_rewrites_semantically_equal_byte_difference(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_share_segment_size=2,
            )
            existing_shares = [
                {"share_seq": 1, "share_id": "s1"},
                {"share_seq": 2, "share_id": "s2"},
            ]
            segment_uri, old_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=2,
                first_share_seq=1,
                last_share_seq=2,
                shares=existing_shares,
            )
            segment_path = Path(segment_uri)
            old_bytes = segment_path.read_bytes()
            # Dict equality ignores insertion order, while the canonical range
            # hash binds exact JSON bytes. The slow path must rewrite these.
            incoming_shares = [
                {"share_id": "s1", "share_seq": 1},
                {"share_id": "s2", "share_seq": 2},
            ]
            expected_bytes = ledger._storage_json_bytes(
                ledger._audit_share_segment_payload(
                    first_share_seq=1,
                    last_share_seq=2,
                    shares=incoming_shares,
                )
            )

            reused_uri, new_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=2,
                first_share_seq=1,
                last_share_seq=2,
                shares=incoming_shares,
            )

            self.assertEqual(reused_uri, segment_uri)
            self.assertNotEqual(old_bytes, expected_bytes)
            self.assertNotEqual(old_range_sha256, new_range_sha256)
            self.assertEqual(segment_path.read_bytes(), expected_bytes)
            self.assertEqual(new_range_sha256, hashlib.sha256(expected_bytes).hexdigest())

    def test_psql_segment_gap_backfills_missing_shares_from_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = SingleWriterShareLedger()
            shares = [
                source.append(pending_share(index)).to_prism_json()
                for index in range(1, 5)
            ]
            backfill_rows = [
                {
                    **share,
                    "share_difficulty": str(share["share_difficulty"]),
                    "network_difficulty": str(share["network_difficulty"]),
                    "credit_policy": None,
                }
                for share in shares[1:3]
            ]
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease(), backfill_rows],
                audit_body_dir=tmp,
                audit_share_segment_size=10,
            )
            segment_uri, _old_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=10,
                first_share_seq=1,
                last_share_seq=1,
                shares=[shares[0]],
            )
            segment_path = Path(segment_uri)
            incoming_bytes = ledger._storage_json_bytes(
                ledger._audit_share_segment_payload(
                    first_share_seq=4,
                    last_share_seq=4,
                    shares=[shares[3]],
                )
            )

            reused_uri, incoming_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=10,
                first_share_seq=4,
                last_share_seq=4,
                shares=[shares[3]],
            )

            merged = json.loads(segment_path.read_text(encoding="utf-8"))
            self.assertEqual(reused_uri, segment_uri)
            self.assertEqual(merged["first_share_seq"], 1)
            self.assertEqual(merged["last_share_seq"], 4)
            self.assertEqual(merged["share_count"], 4)
            self.assertEqual(merged["shares"], shares)
            self.assertEqual(incoming_range_sha256, hashlib.sha256(incoming_bytes).hexdigest())
            self.assertEqual(len(ledger.lease_queries), 2)
            self.assertIn("FROM qbit_share_ledger", ledger.lease_queries[-1])
            self.assertIn("WHERE accepted", ledger.lease_queries[-1])
            self.assertIn("share_seq BETWEEN 2 AND 3", ledger.lease_queries[-1])
            self.assertEqual(
                list(Path(tmp).glob(f"{segment_path.name}.conflict-*")),
                [],
            )

    def test_psql_segment_gap_without_ledger_rows_quarantines_existing_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = SingleWriterShareLedger()
            shares = [
                source.append(pending_share(index)).to_prism_json()
                for index in range(1, 5)
            ]
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease(), [], []],
                audit_body_dir=tmp,
                audit_share_segment_size=10,
            )
            segment_uri, old_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=10,
                first_share_seq=1,
                last_share_seq=1,
                shares=[shares[0]],
            )
            segment_path = Path(segment_uri)
            old_bytes = segment_path.read_bytes()
            old_stat = segment_path.stat()
            incoming_bytes = ledger._storage_json_bytes(
                ledger._audit_share_segment_payload(
                    first_share_seq=4,
                    last_share_seq=4,
                    shares=[shares[3]],
                )
            )

            fresh_uri, incoming_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=10,
                first_share_seq=4,
                last_share_seq=4,
                shares=[shares[3]],
            )

            quarantined = list(Path(tmp).glob(f"{segment_path.name}.conflict-*"))
            fresh_path = Path(fresh_uri)
            self.assertNotEqual(fresh_uri, segment_uri)
            self.assertEqual(segment_path.read_bytes(), old_bytes)
            self.assertEqual(segment_path.stat().st_ino, old_stat.st_ino)
            self.assertEqual(segment_path.stat().st_mtime_ns, old_stat.st_mtime_ns)
            self.assertEqual(fresh_path.read_bytes(), incoming_bytes)
            self.assertEqual(incoming_range_sha256, hashlib.sha256(incoming_bytes).hexdigest())
            self.assertEqual(len(quarantined), 1)
            self.assertEqual(quarantined[0].read_bytes(), old_bytes)
            self.assertEqual(len(ledger.lease_queries), 2)
            self.assertIn("FROM qbit_share_ledger", ledger.lease_queries[-1])
            self.assertIn("share_seq BETWEEN 2 AND 3", ledger.lease_queries[-1])
            old_part = {
                "kind": "segment_range",
                "first_share_seq": 1,
                "last_share_seq": 1,
                "share_count": 1,
                "range_sha256": old_range_sha256,
                "body_uri": segment_uri,
            }
            incoming_part = {
                "kind": "segment_range",
                "first_share_seq": 4,
                "last_share_seq": 4,
                "share_count": 1,
                "range_sha256": incoming_range_sha256,
                "body_uri": fresh_uri,
            }
            self.assertEqual(
                ledger._read_audit_share_segment(old_part, parent_body_uri="old-body"),
                [shares[0]],
            )
            self.assertEqual(
                ledger._read_audit_share_segment(incoming_part, parent_body_uri="new-body"),
                [shares[3]],
            )

            retried_uri, retried_range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=10,
                first_share_seq=4,
                last_share_seq=4,
                shares=[shares[3]],
            )

            self.assertEqual(retried_uri, fresh_uri)
            self.assertEqual(retried_range_sha256, incoming_range_sha256)
            self.assertEqual(segment_path.read_bytes(), old_bytes)
            self.assertEqual(segment_path.stat().st_ino, old_stat.st_ino)
            self.assertEqual(segment_path.stat().st_mtime_ns, old_stat.st_mtime_ns)
            self.assertEqual(fresh_path.read_bytes(), incoming_bytes)
            self.assertEqual(
                len(list(Path(tmp).glob(f"{segment_path.name}.conflict-*"))),
                1,
            )
            self.assertEqual(len(ledger.lease_queries), 3)

    def test_psql_segment_conflicting_duplicate_still_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = SingleWriterShareLedger()
            share = source.append(pending_share(1)).to_prism_json()
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_share_segment_size=10,
            )
            segment_uri, _range_sha256 = ledger._write_audit_share_segment_range(
                segment_first_share_seq=1,
                segment_last_share_seq=10,
                first_share_seq=1,
                last_share_seq=1,
                shares=[share],
            )
            segment_path = Path(segment_uri)
            old_bytes = segment_path.read_bytes()

            with self.assertRaisesRegex(RuntimeError, "conflicts at share_seq 1"):
                ledger._write_audit_share_segment_range(
                    segment_first_share_seq=1,
                    segment_last_share_seq=10,
                    first_share_seq=1,
                    last_share_seq=1,
                    shares=[{**share, "share_id": "different"}],
                )

            self.assertEqual(segment_path.read_bytes(), old_bytes)
            self.assertEqual(len(ledger.lease_queries), 1)
            self.assertEqual(
                list(Path(tmp).glob(f"{segment_path.name}.conflict-*")),
                [],
            )

    def test_psql_canonical_bundle_path_skips_canonicalizer_and_is_retry_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body_dir = root / "body-store"
            body_dir.mkdir()
            candidate_path = body_dir / "canonical-candidate.json"
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [{"share_seq": 1, "share_id": "s1"}],
                "found_block": {"bits": "207fffff"},
            }
            canonical_bytes = fake_audit_bundle_bytes(bundle)
            candidate_path.write_bytes(canonical_bytes)
            body_sha = hashlib.sha256(canonical_bytes).hexdigest()
            canonicalizer = unittest.mock.Mock(
                side_effect=AssertionError("canonical candidate must not be canonicalized again")
            )
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                    {"existing_block": False, "existing_body_uri": None},
                ],
                audit_body_dir=body_dir,
                audit_bundle_canonicalizer=canonicalizer,
                audit_share_segment_size=0,
            )
            payload = {
                "block_hash": "aa" * 32,
                "audit_bundle_sha256": body_sha,
                "coinbase_tx_hex": "00",
                "coinbase_txid": "11" * 32,
                "payout_manifest_sha256": "22" * 32,
                "block_height": 10,
                "parent_hash": "bb" * 32,
                "writer_id": ledger._writer_id,
                "writer_epoch": ledger._writer_epoch,
                "writer_session_token": ledger._writer_session_token,
            }

            first_uri = ledger._prepare_external_audit_body(
                payload,
                bundle,
                canonical_bundle_path=candidate_path,
            )
            assert first_uri is not None
            body_path = Path(first_uri)
            first_stat = body_path.stat()
            self.assertEqual(body_path.read_bytes(), canonical_bytes)

            # This models a retry after the atomic body rename succeeded but
            # before the corresponding database row was committed.
            second_uri = ledger._prepare_external_audit_body(
                payload,
                bundle,
                canonical_bundle_path=candidate_path,
            )

            self.assertEqual(second_uri, first_uri)
            self.assertEqual(body_path.read_bytes(), canonical_bytes)
            self.assertEqual(hashlib.sha256(body_path.read_bytes()).hexdigest(), body_sha)
            self.assertEqual(body_path.stat().st_ino, first_stat.st_ino)
            self.assertEqual(body_path.stat().st_mtime_ns, first_stat.st_mtime_ns)
            canonicalizer.assert_not_called()
            self.assertEqual(list(body_dir.glob(".*.tmp")), [])

    def test_psql_compact_v2_retry_reconstructs_without_recanonicalizing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body_dir = root / "body-store"
            body_dir.mkdir()
            candidate_path = body_dir / "canonical-candidate.json"
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "s1"},
                    {"share_seq": 2, "share_id": "s2"},
                ],
                "found_block": {"bits": "207fffff"},
            }
            canonical_bytes = fake_audit_bundle_bytes(bundle)
            candidate_path.write_bytes(canonical_bytes)
            body_sha = hashlib.sha256(canonical_bytes).hexdigest()
            canonicalizer = unittest.mock.Mock(
                side_effect=AssertionError("compact retry must not canonicalize a full bundle")
            )
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                    {"existing_block": False, "existing_body_uri": None},
                ],
                audit_body_dir=body_dir,
                audit_bundle_canonicalizer=canonicalizer,
                audit_share_segment_size=1,
            )
            payload = {
                "block_hash": "aa" * 32,
                "audit_bundle_sha256": body_sha,
                "coinbase_tx_hex": "00",
                "coinbase_txid": "11" * 32,
                "payout_manifest_sha256": "22" * 32,
                "block_height": 10,
                "parent_hash": "bb" * 32,
                "writer_id": ledger._writer_id,
                "writer_epoch": ledger._writer_epoch,
                "writer_session_token": ledger._writer_session_token,
            }

            first_uri = ledger._prepare_external_audit_body(
                payload,
                bundle,
                canonical_bundle_path=candidate_path,
            )
            assert first_uri is not None
            body_path = Path(first_uri)
            body_stat = body_path.stat()
            segment_paths = sorted(body_dir.glob("prism-audit-share-segment-slot-*.json"))
            segment_stats = {path: path.stat() for path in segment_paths}
            self.assertEqual(
                json.loads(body_path.read_text(encoding="utf-8"))["schema"],
                "qbit.prism.audit-bundle.v2",
            )
            store = ledger._audit_artifact_store
            assert store is not None

            with (
                unittest.mock.patch.object(
                    store,
                    "external_body_matches_sha",
                    side_effect=AssertionError("same-version compact retry must compare bounded storage"),
                ),
                unittest.mock.patch.object(
                    store,
                    "resolve_audit_bundle_v2",
                    wraps=store.resolve_audit_bundle_v2,
                ) as reconstruct,
            ):
                second_uri = ledger._prepare_external_audit_body(
                    payload,
                    bundle,
                    canonical_bundle_path=candidate_path,
                )

            self.assertEqual(second_uri, first_uri)
            self.assertEqual(body_path.stat().st_ino, body_stat.st_ino)
            self.assertEqual(body_path.stat().st_mtime_ns, body_stat.st_mtime_ns)
            self.assertEqual(segment_paths, sorted(body_dir.glob("prism-audit-share-segment-slot-*.json")))
            for path, first_stat in segment_stats.items():
                self.assertEqual(path.stat().st_ino, first_stat.st_ino)
                self.assertEqual(path.stat().st_mtime_ns, first_stat.st_mtime_ns)
            canonicalizer.assert_not_called()
            reconstruct.assert_called_once()
            self.assertFalse(reconstruct.call_args.kwargs["verify_digest"])
            self.assertEqual(list(body_dir.glob(".*.tmp")), [])

    def test_psql_compact_body_rejects_canonical_and_logical_bundle_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body_dir = root / "body-store"
            body_dir.mkdir()
            candidate_path = body_dir / "canonical-candidate.json"
            canonical_bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "canonical-a"},
                    {"share_seq": 2, "share_id": "canonical-a-2"},
                ],
            }
            logical_bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "logical-b"},
                    {"share_seq": 2, "share_id": "logical-b-2"},
                ],
            }
            canonical_bytes = fake_audit_bundle_bytes(canonical_bundle)
            candidate_path.write_bytes(canonical_bytes)
            body_sha = hashlib.sha256(canonical_bytes).hexdigest()
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=body_dir,
                audit_bundle_canonicalizer=unittest.mock.Mock(
                    side_effect=AssertionError("literal candidate should be authoritative")
                ),
                audit_share_segment_size=1,
            )
            payload = {
                "block_hash": "aa" * 32,
                "audit_bundle_sha256": body_sha,
                "body_uri": str(
                    body_dir
                    / f"prism-audit-bundle-body-{'aa' * 32}-{body_sha}.json"
                ),
            }

            with self.assertRaisesRegex(RuntimeError, "does not match logical bundle"):
                ledger._prepare_external_audit_body(
                    payload,
                    logical_bundle,
                    canonical_bundle_path=candidate_path,
                )

            self.assertEqual(
                [
                    path
                    for path in body_dir.iterdir()
                    if path.name != ".prism-audit-publication.lock"
                ],
                [candidate_path],
            )

    def test_psql_canonical_bundle_path_hash_mismatch_publishes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body_dir = root / "body-store"
            body_dir.mkdir()
            candidate_path = body_dir / "canonical-candidate.json"
            bundle = {"schema": "qbit.prism.audit-bundle.v1", "shares": []}
            candidate_path.write_bytes(fake_audit_bundle_bytes(bundle))
            canonicalizer = unittest.mock.Mock(
                side_effect=AssertionError("mismatched candidate must not be canonicalized")
            )
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                ],
                audit_body_dir=body_dir,
                audit_bundle_canonicalizer=canonicalizer,
                audit_share_segment_size=0,
            )
            payload = {
                "block_hash": "aa" * 32,
                "audit_bundle_sha256": "00" * 32,
                "coinbase_tx_hex": "00",
                "coinbase_txid": "11" * 32,
                "payout_manifest_sha256": "22" * 32,
                "block_height": 10,
                "parent_hash": "bb" * 32,
                "writer_id": ledger._writer_id,
                "writer_epoch": ledger._writer_epoch,
                "writer_session_token": ledger._writer_session_token,
            }

            with self.assertRaisesRegex(RuntimeError, "audit bundle sha256 mismatch"):
                ledger._prepare_external_audit_body(
                    payload,
                    bundle,
                    canonical_bundle_path=candidate_path,
                )

            canonicalizer.assert_not_called()
            self.assertEqual(len(ledger.lease_results), 1)
            self.assertEqual(
                [
                    path
                    for path in body_dir.iterdir()
                    if path.name != ".prism-audit-publication.lock"
                ],
                [candidate_path],
            )

    def test_psql_compact_audit_body_writes_v2_range_proof_and_resolves_v1_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "s1"},
                    {"share_seq": 2, "share_id": "s2"},
                    {"share_seq": 3, "share_id": "s3"},
                    {"share_seq": 4, "share_id": "s4"},
                    {"share_seq": 5, "share_id": "s5"},
                ],
                "found_block": {"bits": "207fffff"},
                "settlement_mode_decision": {"mode": "direct_coinbase"},
            }
            body_sha = fake_audit_bundle_sha256(bundle)
            writer = FakeLeasePsqlShareLedger(
                [acquired_lease(), {"existing_block": False, "existing_body_uri": None}],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
                audit_share_segment_size=2,
            )
            body_uri = writer._prepare_external_audit_body(
                {
                    "block_hash": "aa" * 32,
                    "audit_bundle_sha256": body_sha,
                    "coinbase_tx_hex": "00",
                    "coinbase_txid": "11" * 32,
                    "payout_manifest_sha256": "22" * 32,
                    "block_height": 10,
                    "parent_hash": "bb" * 32,
                    "writer_id": writer._writer_id,
                    "writer_epoch": writer._writer_epoch,
                    "writer_session_token": writer._writer_session_token,
                },
                bundle,
            )
            assert body_uri is not None
            body_path = Path(body_uri)
            artifact = json.loads(body_path.read_text(encoding="utf-8"))
            self.assertEqual(artifact["schema"], AUDIT_BUNDLE_V2_SCHEMA)
            self.assertEqual(artifact["share_count"], 5)
            self.assertNotIn("shares", artifact["bundle_without_shares"])
            proof = artifact["share_window_proof"]
            self.assertEqual(proof["schema"], AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA)
            self.assertEqual([part["kind"] for part in proof["share_parts"]], ["segment_range", "segment_range", "segment_range"])
            segment_files = sorted(Path(tmp).glob("prism-audit-share-segment-slot-*.json"))
            self.assertEqual(len(segment_files), 3)
            self.assertNotIn('"kind":"inline"', body_path.read_text(encoding="utf-8"))

            reader = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            resolved = reader._read_external_body(body_uri, expected_sha256=body_sha)
            self.assertEqual(resolved, bundle)

    def test_psql_v1_and_v2_body_tamper_fail_read_and_availability(self) -> None:
        def assert_tamper_rejected(
            ledger: PsqlShareLedger,
            body_path: Path,
            digest: str,
            original: dict[str, Any],
            mutate: Any,
        ) -> None:
            tampered = json.loads(json.dumps(original))
            mutate(tampered)
            body_path.write_text(
                json.dumps(tampered, separators=(",", ":")),
                encoding="utf-8",
            )
            self.assertFalse(
                ledger._external_body_available_for_sha(str(body_path), digest)
            )
            with self.assertRaises(RuntimeError):
                ledger._read_external_body(
                    str(body_path),
                    expected_sha256=digest,
                )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=root,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            v1_bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "s1"},
                    {"share_seq": 2, "share_id": "s2"},
                ],
                "found_block": {"bits": "207fffff"},
            }
            v1_digest = fake_audit_bundle_sha256(v1_bundle)
            v1_body = {
                "schema": AUDIT_BODY_REF_SCHEMA,
                "block_hash": "aa" * 32,
                "audit_bundle_sha256": v1_digest,
                "bundle_without_shares": {
                    "schema": "qbit.prism.audit-bundle.v1",
                    "found_block": {"bits": "207fffff"},
                },
                "share_count": 2,
                "shares_key_index": 1,
                "share_parts": [
                    {
                        "kind": "inline",
                        "first_share_seq": 1,
                        "last_share_seq": 1,
                        "share_count": 1,
                        "shares": [{"share_seq": 1, "share_id": "s1"}],
                    },
                    {
                        "kind": "inline",
                        "first_share_seq": 2,
                        "last_share_seq": 2,
                        "share_count": 1,
                        "shares": [{"share_seq": 2, "share_id": "s2"}],
                    },
                ],
            }
            v1_path = root / (
                f"prism-audit-bundle-body-{'aa' * 32}-{v1_digest}.json"
            )
            v1_path.write_text(
                json.dumps(v1_body, separators=(",", ":")),
                encoding="utf-8",
            )
            self.assertEqual(
                ledger._read_external_body(
                    str(v1_path),
                    expected_sha256=v1_digest,
                ),
                v1_bundle,
            )
            v1_mutations = (
                lambda body: body.__setitem__("block_hash", "bb" * 32),
                lambda body: body["bundle_without_shares"]["found_block"].__setitem__(
                    "bits",
                    "1d00ffff",
                ),
                lambda body: body.__setitem__("shares_key_index", 0),
                lambda body: body.__setitem__(
                    "share_parts",
                    list(reversed(body["share_parts"])),
                ),
                lambda body: body["share_parts"][0].__setitem__(
                    "first_share_seq",
                    2,
                ),
            )
            for index, mutate in enumerate(v1_mutations):
                with self.subTest(schema="v1", mutation=index):
                    assert_tamper_rejected(
                        ledger,
                        v1_path,
                        v1_digest,
                        v1_body,
                        mutate,
                    )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            v2_bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "s1"},
                    {"share_seq": 2, "share_id": "s2"},
                ],
                "reward_manifest": {
                    "anchor_job_issued_at_ms": 100,
                    "anchor_share_seq": 1,
                    "newest_share_seq": 2,
                    "oldest_share_seq": 1,
                    "included_share_count": 2,
                    "requested_window_weight": 20,
                    "counted_window_weight": 20,
                    "share_slice_digest_hex": "44" * 32,
                },
            }
            v2_digest = fake_audit_bundle_sha256(v2_bundle)
            writer = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                ],
                audit_body_dir=root,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
                audit_share_segment_size=1,
            )
            body_uri = writer._prepare_external_audit_body(
                {
                    "block_hash": "cc" * 32,
                    "audit_bundle_sha256": v2_digest,
                    "coinbase_tx_hex": "00",
                    "coinbase_txid": "11" * 32,
                    "payout_manifest_sha256": "22" * 32,
                    "block_height": 10,
                    "parent_hash": "bb" * 32,
                    "writer_id": writer._writer_id,
                    "writer_epoch": writer._writer_epoch,
                    "writer_session_token": writer._writer_session_token,
                },
                v2_bundle,
            )
            assert body_uri is not None
            v2_path = Path(body_uri)
            v2_body = json.loads(v2_path.read_text(encoding="utf-8"))
            self.assertEqual(v2_body["schema"], AUDIT_BUNDLE_V2_SCHEMA)
            self.assertEqual(
                writer._read_external_body(
                    body_uri,
                    expected_sha256=v2_digest,
                ),
                v2_bundle,
            )
            v2_mutations = (
                lambda body: body.__setitem__("block_hash", "dd" * 32),
                lambda body: body["bundle_without_shares"]["reward_manifest"].__setitem__(
                    "included_share_count",
                    3,
                ),
                lambda body: body.__setitem__("shares_key_index", 0),
                lambda body: body["share_window_proof"].__setitem__(
                    "share_parts",
                    list(reversed(body["share_window_proof"]["share_parts"])),
                ),
                lambda body: body["share_window_proof"].__setitem__(
                    "included_share_count",
                    3,
                ),
            )
            for index, mutate in enumerate(v2_mutations):
                with self.subTest(schema="v2", mutation=index):
                    assert_tamper_rejected(
                        writer,
                        v2_path,
                        v2_digest,
                        v2_body,
                        mutate,
                    )

    def test_psql_v2_range_segments_grow_without_breaking_old_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first_bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "s1"},
                    {"share_seq": 2, "share_id": "s2"},
                    {"share_seq": 3, "share_id": "s3"},
                ],
            }
            second_bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 2, "share_id": "s2"},
                    {"share_seq": 3, "share_id": "s3"},
                    {"share_seq": 4, "share_id": "s4"},
                ],
            }
            first_sha = fake_audit_bundle_sha256(first_bundle)
            second_sha = fake_audit_bundle_sha256(second_bundle)
            writer = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                    {"existing_block": False, "existing_body_uri": None},
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
                audit_share_segment_size=2,
            )
            first_uri = writer._prepare_external_audit_body(
                {
                    "block_hash": "aa" * 32,
                    "audit_bundle_sha256": first_sha,
                    "coinbase_tx_hex": "00",
                    "coinbase_txid": "11" * 32,
                    "payout_manifest_sha256": "22" * 32,
                    "block_height": 10,
                    "parent_hash": "bb" * 32,
                    "writer_id": writer._writer_id,
                    "writer_epoch": writer._writer_epoch,
                    "writer_session_token": writer._writer_session_token,
                },
                first_bundle,
            )
            second_uri = writer._prepare_external_audit_body(
                {
                    "block_hash": "cc" * 32,
                    "audit_bundle_sha256": second_sha,
                    "coinbase_tx_hex": "00",
                    "coinbase_txid": "33" * 32,
                    "payout_manifest_sha256": "44" * 32,
                    "block_height": 11,
                    "parent_hash": "aa" * 32,
                    "writer_id": writer._writer_id,
                    "writer_epoch": writer._writer_epoch,
                    "writer_session_token": writer._writer_session_token,
                },
                second_bundle,
            )
            assert first_uri is not None
            assert second_uri is not None
            segment_files = sorted(Path(tmp).glob("prism-audit-share-segment-slot-*.json"))
            self.assertEqual([path.name for path in segment_files], [
                "prism-audit-share-segment-slot-1-2.json",
                "prism-audit-share-segment-slot-3-4.json",
            ])
            slot_3_4 = json.loads((Path(tmp) / "prism-audit-share-segment-slot-3-4.json").read_text(encoding="utf-8"))
            self.assertEqual([share["share_seq"] for share in slot_3_4["shares"]], [3, 4])

            reader = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            self.assertEqual(reader._read_external_body(first_uri, expected_sha256=first_sha), first_bundle)
            self.assertEqual(reader._read_external_body(second_uri, expected_sha256=second_sha), second_bundle)

    def test_psql_public_artifact_resolves_external_audit_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            writer = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [{"share_seq": 1}],
            }
            body_sha = fake_audit_bundle_sha256(bundle)
            body_uri = writer._externalize_audit_body("aa" * 32, body_sha, bundle)
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {
                        "audit_bundle": None,
                        "audit_bundle_sha256": body_sha,
                        "body_uri": body_uri,
                        "has_audit_row": True,
                        "fallback": None,
                    },
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )

            self.assertEqual(ledger.dashboard_public_artifact(sha256=body_sha), bundle)
            query = ledger.lease_queries[-1]
            self.assertIn("SELECT block_hash, audit_bundle, audit_bundle_sha256, body_uri", query)

    def test_psql_public_artifact_exists_uses_metadata_only(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "has_audit_row": True,
                    "audit_bundle_inline": True,
                    "body_uri": None,
                    "fallback_exists": False,
                },
            ]
        )

        self.assertTrue(ledger.dashboard_public_artifact_exists(sha256="aa" * 32))
        query = ledger.lease_queries[-1]
        self.assertIn("FROM qbit_pool_audit_bundles", query)
        self.assertIn("body_uri", query)

    def test_psql_public_artifact_document_reads_persisted_text_in_one_query(self) -> None:
        manifest_set = sample_ctv_manifest_set()
        manifest_set_json = canonical_json_text(manifest_set)
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "has_audit_row": False,
                    "audit_bundle": None,
                    "audit_bundle_sha256": None,
                    "body_uri": None,
                    "fallback": manifest_set,
                    "fallback_canonical": manifest_set_json,
                },
            ]
        )

        document = ledger.dashboard_public_artifact_document(sha256=sha256_json_hex(manifest_set))

        self.assertEqual(document, {"payload": manifest_set, "canonical_json": manifest_set_json})
        # The payload and its canonical text come from the same statement so a
        # request touches each artifact table at most once.
        query = ledger.lease_queries[-1]
        self.assertIn("SELECT block_hash, audit_bundle, audit_bundle_sha256, body_uri", query)
        self.assertIn("SELECT manifest_set, manifest_set_json", query)
        self.assertIn("FROM qbit_ctv_fanout_sets", query)
        self.assertIn("SELECT manifest, manifest_json", query)
        self.assertIn("FROM qbit_ctv_fanout_artifacts", query)
        self.assertEqual(query.count("FROM qbit_ctv_fanout_sets"), 1)
        self.assertEqual(query.count("FROM qbit_ctv_fanout_artifacts"), 1)

    def test_psql_public_artifact_document_prefers_stored_canonical_bundle(self) -> None:
        canonical_bytes = b'{"schema":"qbit.prism.audit-bundle.v1","accepted_shares":[]}'
        digest = hashlib.sha256(canonical_bytes).hexdigest()
        block_hash = "ab" * 32
        calls: list[tuple[str, str]] = []

        def read_canonical(requested_block: str, requested_digest: str) -> bytes:
            calls.append((requested_block, requested_digest))
            return canonical_bytes

        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "has_audit_row": True,
                    "block_hash": block_hash,
                    "audit_bundle": {"schema": "legacy-reconstructed"},
                    "audit_bundle_sha256": digest,
                    "body_uri": None,
                    "fallback": None,
                },
            ],
            audit_artifact_store=types.SimpleNamespace(
                read_canonical_audit_bundle=read_canonical,
            ),
        )

        document = ledger.dashboard_public_artifact_document(sha256=digest)

        self.assertEqual(
            document,
            {
                "payload": json.loads(canonical_bytes),
                "canonical_json": canonical_bytes.decode(),
            },
        )
        self.assertEqual(calls, [(block_hash, digest)])

    def test_psql_public_artifact_serves_real_stored_bytes_without_reserialization(self) -> None:
        canonical_bytes = b'{"schema":"qbit.prism.audit-bundle.v1","z":0,"a":1}'
        digest = hashlib.sha256(canonical_bytes).hexdigest()
        block_hash = "ab" * 32
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = AuditArtifactStore(
                AuditArtifactConfig(
                    root=root,
                    evidence_path=root / "evidence.json",
                )
            )
            try:
                store.write_canonical_audit_bundle(
                    block_hash,
                    digest,
                    canonical_bytes,
                )
                ledger = FakeLeasePsqlShareLedger(
                    [
                        acquired_lease(),
                        {
                            "has_audit_row": True,
                            "block_hash": block_hash,
                            "audit_bundle": {"schema": "legacy-reconstructed"},
                            "audit_bundle_sha256": digest,
                            "body_uri": None,
                            "fallback": None,
                        },
                    ],
                    audit_artifact_store=store,
                )

                response = public_api.artifact(
                    types.SimpleNamespace(ledger=ledger),
                    sha256=digest,
                )

                self.assertIsInstance(response, public_api.RawJsonBody)
                self.assertEqual(response.body, canonical_bytes)  # type: ignore[union-attr]
                self.assertEqual(hashlib.sha256(response.body).hexdigest(), digest)  # type: ignore[union-attr]
            finally:
                store.close()

    def test_psql_public_artifact_visible_row_missing_file_uses_legacy_body(self) -> None:
        legacy = {"schema": "qbit.prism.audit-bundle.v1", "legacy": True}
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "has_audit_row": True,
                    "block_hash": "ab" * 32,
                    "audit_bundle": legacy,
                    "audit_bundle_sha256": "cd" * 32,
                    "body_uri": None,
                    "fallback": None,
                },
            ],
            audit_artifact_store=types.SimpleNamespace(
                read_canonical_audit_bundle=lambda _block, _digest: None,
            ),
        )

        with self.assertLogs("lab.prism.share_ledger", level="WARNING") as logs:
            document = ledger.dashboard_public_artifact_document(sha256="cd" * 32)

        self.assertEqual(
            document,
            {
                "payload": legacy,
                "canonical_json": None,
                "canonical_fallback_reason": "missing",
            },
        )
        self.assertIn("reason=missing", "\n".join(logs.output))

    def test_psql_public_artifact_corrupt_file_fails_closed(self) -> None:
        canonical_bytes = b'{"schema":"qbit.prism.audit-bundle.v1"}'
        digest = hashlib.sha256(canonical_bytes).hexdigest()
        block_hash = "ab" * 32
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = AuditArtifactStore(
                AuditArtifactConfig(
                    root=root,
                    evidence_path=root / "evidence.json",
                )
            )
            try:
                path = store.write_canonical_audit_bundle(
                    block_hash,
                    digest,
                    canonical_bytes,
                )
                path.chmod(0o600)
                path.write_bytes(b"not a gzip stream")
                ledger = FakeLeasePsqlShareLedger(
                    [
                        acquired_lease(),
                        {
                            "has_audit_row": True,
                            "block_hash": block_hash,
                            "audit_bundle": {"schema": "legacy-reconstructed"},
                            "audit_bundle_sha256": digest,
                            "body_uri": None,
                            "fallback": None,
                        },
                    ],
                    audit_artifact_store=store,
                )

                with self.assertLogs(
                    "lab.prism.share_ledger",
                    level="ERROR",
                ) as logs:
                    with self.assertRaises(CanonicalAuditBundleCorrupt):
                        ledger.dashboard_public_artifact_document(sha256=digest)

                output = "\n".join(logs.output)
                self.assertIn("reason=corrupt", output)
                self.assertIn(block_hash, output)
                self.assertIn(digest, output)
            finally:
                store.close()

    def test_psql_public_artifact_file_before_replica_row_is_not_visible(self) -> None:
        def unexpected_read(_block: str, _digest: str) -> bytes:
            self.fail("canonical storage must not bypass replica row visibility")

        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "has_audit_row": False,
                    "block_hash": None,
                    "audit_bundle": None,
                    "audit_bundle_sha256": None,
                    "body_uri": None,
                    "fallback": None,
                    "fallback_canonical": None,
                },
            ],
            audit_artifact_store=types.SimpleNamespace(
                read_canonical_audit_bundle=unexpected_read,
            ),
        )

        self.assertIsNone(ledger.dashboard_public_artifact_document(sha256="ef" * 32))

    def test_psql_public_artifact_exists_accepts_verified_canonical_file(self) -> None:
        canonical_bytes = b'{"schema":"qbit.prism.audit-bundle.v1"}'
        digest = hashlib.sha256(canonical_bytes).hexdigest()
        block_hash = "ab" * 32
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "has_audit_row": True,
                    "block_hash": block_hash,
                    "audit_bundle_sha256": digest,
                    "audit_bundle_inline": False,
                    "body_uri": "/missing/legacy-body.json",
                    "fallback_exists": False,
                },
            ],
            audit_artifact_store=types.SimpleNamespace(
                read_canonical_audit_bundle=lambda _block, _digest: canonical_bytes,
            ),
        )

        self.assertTrue(ledger.dashboard_public_artifact_exists(sha256=digest))

    def test_psql_public_artifact_document_returns_none_when_unmatched(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "has_audit_row": False,
                    "audit_bundle": None,
                    "audit_bundle_sha256": None,
                    "body_uri": None,
                    "fallback": None,
                    "fallback_canonical": None,
                },
            ]
        )

        self.assertIsNone(ledger.dashboard_public_artifact_document(sha256="aa" * 32))

    def test_psql_public_artifact_exists_rejects_missing_external_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {
                        "has_audit_row": True,
                        "audit_bundle_inline": False,
                        "body_uri": str(Path(tmp) / "missing.json"),
                        "fallback_exists": False,
                    },
                ],
                audit_body_dir=tmp,
            )

            self.assertFalse(ledger.dashboard_public_artifact_exists(sha256="aa" * 32))

    def test_psql_public_artifact_exists_validates_compact_body_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical_bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [{"share_seq": 1, "share_id": "s1"}],
            }
            body_sha = fake_audit_bundle_sha256(logical_bundle)
            segment = {
                "schema": "qbit.prism.audit-share-segment.v1",
                "first_share_seq": 1,
                "last_share_seq": 1,
                "share_count": 1,
                "shares": [{"share_seq": 1, "share_id": "s1"}],
            }
            segment_bytes = json.dumps(segment, separators=(",", ":")).encode("utf-8")
            segment_sha256 = hashlib.sha256(segment_bytes).hexdigest()
            segment_path = root / f"prism-audit-share-segment-1-1-{segment_sha256}.json"
            segment_path.write_bytes(segment_bytes)
            body_ref = {
                "schema": AUDIT_BODY_REF_SCHEMA,
                "block_hash": "bb" * 32,
                "audit_bundle_sha256": body_sha,
                "bundle_without_shares": {"schema": "qbit.prism.audit-bundle.v1"},
                "share_count": 1,
                "shares_key_index": 1,
                "share_parts": [
                    {
                        "kind": "segment",
                        "first_share_seq": 1,
                        "last_share_seq": 1,
                        "share_count": 1,
                        "sha256": segment_sha256,
                        "body_uri": str(segment_path),
                    }
                ],
            }
            body_path = root / f"prism-audit-bundle-body-{'bb' * 32}-{body_sha}.json"
            body_path.write_text(json.dumps(body_ref, separators=(",", ":")), encoding="utf-8")
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {
                        "has_audit_row": True,
                        "audit_bundle_inline": False,
                        "body_uri": str(body_path),
                        "fallback_exists": False,
                    },
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )

            self.assertTrue(ledger.dashboard_public_artifact_exists(sha256=body_sha))
            segment_path.unlink()
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {
                        "has_audit_row": True,
                        "audit_bundle_inline": False,
                        "body_uri": str(body_path),
                        "fallback_exists": False,
                    },
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            self.assertFalse(ledger.dashboard_public_artifact_exists(sha256=body_sha))

    def test_psql_public_artifact_exists_rejects_overstated_inline_share_count(self) -> None:
        body_ref = {
            "schema": AUDIT_BODY_REF_SCHEMA,
            "audit_bundle_sha256": "aa" * 32,
            "bundle_without_shares": {"schema": "qbit.prism.audit-bundle.v1"},
            "share_count": 2,
            "share_parts": [
                {
                    "kind": "inline",
                    "first_share_seq": 1,
                    "last_share_seq": 2,
                    "share_count": 2,
                    "shares": [{"share_seq": 1, "share_id": "s1"}],
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            body_path = Path(tmp) / (
                f"prism-audit-bundle-body-{'11' * 32}-{'aa' * 32}.json"
            )
            body_path.write_text(json.dumps(body_ref, separators=(",", ":")), encoding="utf-8")
            ledger = FakeLeasePsqlShareLedger([acquired_lease()], audit_body_dir=tmp)

            self.assertFalse(ledger._external_body_available_for_sha(str(body_path), "aa" * 32))

    def test_psql_body_ref_respects_zero_shares_key_index(self) -> None:
        bundle = {
            "shares": [{"share_seq": 1, "share_id": "s1"}],
            "schema": "qbit.prism.audit-bundle.v1",
            "found_block": {"bits": "207fffff"},
        }
        body_sha = fake_audit_bundle_sha256(bundle)
        body_ref = {
            "schema": AUDIT_BODY_REF_SCHEMA,
            "block_hash": "11" * 32,
            "audit_bundle_sha256": body_sha,
            "share_count": 1,
            "shares_key_index": 0,
            "bundle_without_shares": {
                "schema": "qbit.prism.audit-bundle.v1",
                "found_block": {"bits": "207fffff"},
            },
            "share_parts": [
                {
                    "kind": "inline",
                    "first_share_seq": 1,
                    "last_share_seq": 1,
                    "share_count": 1,
                    "shares": [{"share_seq": 1, "share_id": "s1"}],
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            body_path = Path(tmp) / (
                f"prism-audit-bundle-body-{'11' * 32}-{body_sha}.json"
            )
            body_path.write_text(json.dumps(body_ref, separators=(",", ":")), encoding="utf-8")
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )

            self.assertEqual(ledger._read_external_body(str(body_path), expected_sha256=body_sha), bundle)

    def test_psql_external_body_hash_mismatch_fails_readers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            writer = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [{"share_seq": 1}],
            }
            body_sha = fake_audit_bundle_sha256(bundle)
            body_uri = writer._externalize_audit_body("aa" * 32, body_sha, bundle)
            Path(str(body_uri)).write_text(json.dumps({"schema": "corrupt"}), encoding="utf-8")
            audit_row = {
                "block_hash": "aa" * 32,
                "audit_bundle_sha256": body_sha,
                "coinbase_tx_hex": "00",
                "audit_bundle": None,
                "body_uri": body_uri,
            }

            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    audit_row,
                    {**audit_row, "audit_commitment_leaf_hex": "ab" * 32},
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
                ledger.audit_bundle(block_hash="aa" * 32)
            with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
                ledger.audit_bundle_by_commitment(commitment_leaf_hex="ab" * 32)

            public_ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {
                        "audit_bundle": None,
                        "audit_bundle_sha256": body_sha,
                        "body_uri": body_uri,
                        "has_audit_row": True,
                        "fallback": None,
                    },
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
                public_ledger.dashboard_public_artifact(sha256=body_sha)

    def test_psql_resolves_inline_body_and_flags_missing_external_body(self) -> None:
        ledger = FakeLeasePsqlShareLedger([acquired_lease()])  # no body store configured
        self.assertIsNone(ledger._externalize_audit_body("aa" * 32, "bb" * 32, {"x": 1}))
        inline = {"schema": "qbit.prism.audit-bundle.v1"}
        resolved = ledger._resolve_audit_bundle_row(
            {"block_hash": "aa" * 32, "audit_bundle": inline, "body_uri": None}
        )
        assert resolved is not None
        self.assertEqual(resolved["audit_bundle"], inline)
        self.assertNotIn("body_uri", resolved)
        self.assertIsNone(ledger._resolve_audit_bundle_row(None))
        with self.assertRaisesRegex(RuntimeError, "not retrievable"):
            ledger._resolve_audit_bundle_row(
                {
                    "audit_bundle_sha256": "bb" * 32,
                    "audit_bundle": None,
                    "body_uri": "/nonexistent/prism-audit-bundle-body-zz.json",
                }
            )

    def test_psql_persist_externalizes_bundle_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                    {
                        "backend": "postgres-psql",
                        "share_count": 0,
                        "block_count": 1,
                        "bundle_count": 1,
                        "payout_entry_count": 1,
                        "carry_forward_count": 1,
                        "onchain_output_count": 0,
                    },
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "signed_coinbase_manifest": {"manifest": {"payout_count": 1}},
                "found_block": {"network_difficulty": 1000, "bits": "207fffff", "coinbase_value_sats": 600},
                "audit_commitment_leaves_hex": ["ab" * 32],
                "witness_merkle_leaves_hex": ["cd" * 32],
                "payout_policy_manifest": {
                    "accounts": [
                        {
                            "recipient_id": "miner-a",
                            "order_key": "a",
                            "p2mr_program_hex": "aa" * 32,
                            "gross_amount_sats": 1000,
                            "prior_balance_sats": 0,
                            "candidate_balance_sats": 1000,
                            "onchain_amount_sats": 0,
                            "carry_forward_balance_sats": 1000,
                            "action": "accrued",
                        }
                    ]
                },
            }
            report = {
                "coinbase_txid": "ee" * 32,
                "coinbase_manifest_sha256_hex": "11" * 32,
                "audit_bundle_sha256_hex": fake_audit_bundle_sha256(bundle),
                "coinbase_tx_hex": "00",
            }
            ledger.persist_accepted_block(
                block_hash="aa" * 32,
                block_height=10,
                parent_hash="bb" * 32,
                final_bundle=bundle,
                audit_report=report,
            )
            query = ledger.lease_queries[-1]
            # New columns are written, and the inline JSONB body is NULL (externalized).
            self.assertIn("body_uri", query)
            self.assertIn("found_block_network_difficulty", query)
            self.assertIn("audit_commitment_leaves_hex", query)
            self.assertIn('"audit_bundle":null', query)
            # The body lives in exactly one external file that round-trips.
            body_files = sorted(Path(tmp).glob("prism-audit-bundle-body-*.json"))
            self.assertEqual(len(body_files), 1)
            self.assertIn(report["audit_bundle_sha256_hex"], body_files[0].name)
            self.assertEqual(json.loads(body_files[0].read_text(encoding="utf-8")), bundle)

    def test_psql_persist_rejects_report_digest_mismatch_before_body_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease(), {"existing_block": False, "existing_body_uri": None}],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
            )
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "signed_coinbase_manifest": {"manifest": {"payout_count": 0}},
                "payout_policy_manifest": {"accounts": []},
            }
            report = {
                "coinbase_txid": "ee" * 32,
                "coinbase_manifest_sha256_hex": "11" * 32,
                "audit_bundle_sha256_hex": "22" * 32,
                "coinbase_tx_hex": "00",
            }

            with self.assertRaisesRegex(RuntimeError, "sha256 mismatch"):
                ledger.persist_accepted_block(
                    block_hash="aa" * 32,
                    block_height=10,
                    parent_hash="bb" * 32,
                    final_bundle=bundle,
                    audit_report=report,
                )
            self.assertEqual(list(Path(tmp).glob("prism-audit-bundle-body-*.json")), [])
            self.assertEqual(
                list(Path(tmp).glob("prism-audit-bundle-canonical-*.json.gz")),
                [],
            )

    def test_psql_persist_requires_lease_preflight_before_external_body_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease(), {"error": "writer lease is not active"}],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
                audit_share_segment_size=1,
            )
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [{"share_seq": 1, "share_id": "s1"}],
                "signed_coinbase_manifest": {"manifest": {"payout_count": 0}},
                "payout_policy_manifest": {"accounts": []},
            }
            report = {
                "coinbase_txid": "ee" * 32,
                "coinbase_manifest_sha256_hex": "11" * 32,
                "audit_bundle_sha256_hex": fake_audit_bundle_sha256(bundle),
                "coinbase_tx_hex": "00",
            }

            with self.assertRaisesRegex(RuntimeError, "writer lease is not active"):
                ledger.persist_accepted_block(
                    block_hash="aa" * 32,
                    block_height=10,
                    parent_hash="bb" * 32,
                    final_bundle=bundle,
                    audit_report=report,
                )
            self.assertEqual(list(Path(tmp).glob("prism-audit-bundle-body-*.json")), [])
            self.assertEqual(list(Path(tmp).glob("prism-audit-share-segment-*.json")), [])
            # The canonical copy is a filesystem side effect like any other and
            # must stay behind the same lease fence.
            self.assertEqual(
                list(Path(tmp).glob("prism-audit-bundle-canonical-*.json.gz")),
                [],
            )

    def test_psql_persist_publishes_the_canonical_bundle_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "s1"},
                    {"share_seq": 2, "share_id": "s2"},
                ],
                "signed_coinbase_manifest": {"manifest": {"payout_count": 0}},
                "payout_policy_manifest": {"accounts": []},
            }
            digest = fake_audit_bundle_sha256(bundle)
            report = {
                "coinbase_txid": "ee" * 32,
                "coinbase_manifest_sha256_hex": "11" * 32,
                "audit_bundle_sha256_hex": digest,
                "coinbase_tx_hex": "00",
            }
            insert_result = {
                "backend": "postgres-psql",
                "share_count": 0,
                "block_count": 1,
                "bundle_count": 1,
                "payout_entry_count": 0,
                "carry_forward_count": 0,
                "onchain_output_count": 0,
            }
            ledger = FakeLeasePsqlShareLedger(
                [
                    acquired_lease(),
                    {"existing_block": False, "existing_body_uri": None},
                    insert_result,
                ],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
                audit_share_segment_size=2,
            )
            ledger.persist_accepted_block(
                block_hash="aa" * 32,
                block_height=10,
                parent_hash="bb" * 32,
                final_bundle=bundle,
                audit_report=report,
            )
            canonical_files = sorted(
                Path(tmp).glob("prism-audit-bundle-canonical-*.json.gz")
            )
            self.assertEqual(len(canonical_files), 1)
            self.assertEqual(
                canonical_files[0].name,
                f"prism-audit-bundle-canonical-{'aa' * 32}-{digest}.json.gz",
            )
            # The compact body stays the pointer the row references; the
            # canonical copy carries the exact bytes the digest is taken over.
            compressed = canonical_files[0].read_bytes()
            self.assertEqual(gzip.decompress(compressed), fake_audit_bundle_bytes(bundle))
            body_files = sorted(Path(tmp).glob("prism-audit-bundle-body-*.json"))
            self.assertEqual(len(body_files), 1)
            self.assertNotEqual(body_files[0].read_bytes(), fake_audit_bundle_bytes(bundle))
            self.assertEqual(
                ledger._audit_store().read_canonical_audit_bundle("aa" * 32, digest),
                fake_audit_bundle_bytes(bundle),
            )

            # Re-persisting the same block republishes byte-for-byte.
            ledger.lease_results.extend(
                [{"existing_block": True, "existing_body_uri": str(body_files[0])}, insert_result]
            )
            ledger.persist_accepted_block(
                block_hash="aa" * 32,
                block_height=10,
                parent_hash="bb" * 32,
                final_bundle=bundle,
                audit_report=report,
            )
            self.assertEqual(canonical_files[0].read_bytes(), compressed)
            self.assertEqual(
                sorted(Path(tmp).glob("prism-audit-bundle-canonical-*.json.gz")),
                canonical_files,
            )

    def test_backfill_seams_bind_the_ledger_loader_and_the_owned_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = {
                "schema": "qbit.prism.audit-bundle.v1",
                "shares": [
                    {"share_seq": 1, "share_id": "s1"},
                    {"share_seq": 2, "share_id": "s2"},
                ],
                "signed_coinbase_manifest": {"manifest": {"payout_count": 0}},
                "payout_policy_manifest": {"accounts": []},
            }
            digest = fake_audit_bundle_sha256(bundle)
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease(), {"existing_block": False, "existing_body_uri": None}],
                audit_body_dir=tmp,
                audit_bundle_canonicalizer=fake_audit_bundle_bytes,
                audit_share_segment_size=2,
            )
            body_uri = ledger._prepare_external_audit_body(
                {"block_hash": "aa" * 32, "audit_bundle_sha256": digest},
                bundle,
            )
            assert body_uri is not None
            canonical_path = Path(
                ledger.publish_canonical_bundle_bytes(
                    block_hash="aa" * 32,
                    audit_bundle_sha256=digest,
                    canonical_bytes=fake_audit_bundle_bytes(bundle),
                )
            )
            self.assertTrue(canonical_path.exists())

            # Drop the segments; the seam must repair them from the ledger.
            for segment in Path(tmp).glob("prism-audit-share-segment-*"):
                segment.unlink()
            loaded: list[tuple[int, int]] = []

            def load_range(*, first_share_seq: int, last_share_seq: int) -> list[Any]:
                loaded.append((first_share_seq, last_share_seq))
                return [
                    {"share_seq": share_seq, "share_id": f"s{share_seq}"}
                    for share_seq in range(first_share_seq, last_share_seq + 1)
                ]

            with unittest.mock.patch.object(
                ledger,
                "_load_audit_share_ledger_range",
                side_effect=load_range,
            ):
                recovered = ledger.canonical_bundle_bytes_for_backfill(
                    block_hash="aa" * 32,
                    audit_bundle_sha256=digest,
                    body_uri=body_uri,
                )
            self.assertEqual(recovered, fake_audit_bundle_bytes(bundle))
            self.assertEqual(loaded, [(1, 2)])

    def test_preflight_canonical_bundle_publication_fences_each_backfill_write(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "block_hash": "aa" * 32,
                    "audit_bundle_sha256": "22" * 32,
                    "body_uri": "prism-audit-bundle-body-x.json",
                    "has_inline_bundle": False,
                },
            ]
        )
        result = ledger.preflight_canonical_bundle_publication(
            block_hash="aa" * 32,
            audit_bundle_sha256="22" * 32,
        )
        self.assertEqual(
            result,
            {
                "block_hash": "aa" * 32,
                "audit_bundle_sha256": "22" * 32,
                "body_uri": "prism-audit-bundle-body-x.json",
                "has_inline_bundle": False,
            },
        )
        query = ledger.lease_queries[-1]
        # Refreshes the lease this writer already holds, and reads the audit
        # row it is about to answer for. No schema change, no row rewrite.
        self.assertIn("UPDATE qbit_ledger_writer_lease", query)
        self.assertIn("lease_expires_at = clock_timestamp() + make_interval", query)
        self.assertIn("qbit_ledger_writer_lease.writer_session_token = data->>", query)
        self.assertIn("FROM qbit_pool_audit_bundles", query)
        self.assertIn("'writer lease is not active'", query)
        self.assertIn("'audit bundle digest does not match the stored row'", query)
        for forbidden in (
            "ALTER TABLE",
            "CREATE ",
            "INSERT INTO",
            "DELETE FROM",
            "UPDATE qbit_pool_audit_bundles",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, query)

    def test_preflight_canonical_bundle_publication_fails_closed(self) -> None:
        for failure, expected in (
            ({"error": "writer lease is not active"}, "writer lease is not active"),
            ({"error": "audit bundle row does not exist"}, "row does not exist"),
            (
                {"error": "audit bundle digest does not match the stored row"},
                "digest does not match",
            ),
            (None, "returned no row"),
        ):
            with self.subTest(failure=failure):
                ledger = FakeLeasePsqlShareLedger([acquired_lease(), failure])
                with self.assertRaisesRegex(RuntimeError, expected):
                    ledger.preflight_canonical_bundle_publication(
                        block_hash="aa" * 32,
                        audit_bundle_sha256="22" * 32,
                    )
        ledger = FakeLeasePsqlShareLedger([acquired_lease()])
        for block_hash, digest in (("aa" * 31, "22" * 32), ("aa" * 32, "zz" * 32)):
            with self.subTest(block_hash=block_hash, digest=digest):
                with self.assertRaises(ValueError):
                    ledger.preflight_canonical_bundle_publication(
                        block_hash=block_hash,
                        audit_bundle_sha256=digest,
                    )

    def test_psql_external_body_path_must_stay_under_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger([acquired_lease()], audit_body_dir=tmp)
            with self.assertRaisesRegex(RuntimeError, "escapes audit body store"):
                ledger._read_external_body("/tmp/prism-audit-bundle-body-aa.json")

    def test_psql_audit_bundle_readers_select_external_body_pointer(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [
                acquired_lease(),
                {
                    "block_hash": "aa" * 32,
                    "audit_bundle_sha256": "22" * 32,
                    "coinbase_tx_hex": "00",
                    "audit_bundle": {"schema": "qbit.prism.audit-bundle.v1"},
                    "body_uri": None,
                },
                {
                    "block_hash": "aa" * 32,
                    "audit_commitment_leaf_hex": "ab" * 32,
                    "audit_bundle_sha256": "22" * 32,
                    "coinbase_tx_hex": "00",
                    "audit_bundle": {"schema": "qbit.prism.audit-bundle.v1"},
                    "body_uri": None,
                },
            ]
        )

        ledger.audit_bundle(block_hash="aa" * 32)
        by_hash_query = ledger.lease_queries[-1]
        self.assertIn("'body_uri', bundle.body_uri", by_hash_query)
        self.assertIn("'block_height', block.block_height", by_hash_query)
        self.assertIn("'payout_manifest_sha256', block.payout_manifest_sha256", by_hash_query)
        self.assertIn("JOIN qbit_pool_blocks block", by_hash_query)

        ledger.audit_bundle_by_commitment(commitment_leaf_hex="ab" * 32)
        by_commitment_query = ledger.lease_queries[-1]
        self.assertIn("'body_uri', bundle.body_uri", by_commitment_query)
        self.assertIn("'block_height', block.block_height", by_commitment_query)
        self.assertIn("'payout_manifest_sha256', block.payout_manifest_sha256", by_commitment_query)
        # Queries the promoted leaf columns (new rows) plus the inline JSONB
        # (legacy rows), and orders by chain height rather than row creation time.
        self.assertIn("bundle.audit_commitment_leaves_hex ?", by_commitment_query)
        self.assertIn("bundle.audit_bundle->'audit_commitment_leaves_hex' ?", by_commitment_query)
        self.assertIn("ORDER BY block.block_height DESC", by_commitment_query)

    def test_postgres_startup_waits_for_same_writer_predecessor_lease(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            ledger = FakeLeasePsqlShareLedger(
                [
                    held_lease(wait_seconds=2.5),
                    held_lease(wait_seconds=0.1),
                    acquired_lease(session="replacement-session"),
                ],
                writer_id="writer-a",
                writer_epoch=1,
            )

        # The first wait is clamped by the fixture's retry ceiling (the
        # adoption silence), the second by the retry floor.
        retry_ceiling = max(1.0, DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS)
        self.assertEqual(ledger.sleeps, [retry_ceiling, 0.25])
        self.assertEqual(len(ledger.lease_queries), 3)
        self.assertIn(
            "prism ledger writer lease held until 2026-06-26 19:50:22.233718+00; "
            f"waiting {retry_ceiling:.3g}s before retry",
            stdout.getvalue(),
        )
        self.assertIn("holder writer=writer-a epoch=1 session=old-session", stdout.getvalue())

    def test_heartbeat_session_acquires_advisory_guard_before_lease_query(self) -> None:
        attempts: list[object] = []

        class FakeGuard:
            def __init__(self, acquired: bool) -> None:
                self.acquired = acquired
                self.held = False
                self.closed = False

            def try_acquire(self) -> bool:
                attempts.append(self)
                self.held = self.acquired
                return self.acquired

            def close(self) -> None:
                self.closed = True
                self.held = False

        guards = [FakeGuard(False), FakeGuard(True)]

        class GuardedLedger(FakeLeasePsqlShareLedger):
            def _make_writer_lease_guard(self, _database_url: str | None) -> Any:
                return guards.pop(0)

        session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}guarded"
        ledger = GuardedLedger(
            [acquired_lease(session=session)],
            writer_session_token=session,
        )

        self.assertEqual(len(attempts), 2)
        self.assertTrue(attempts[0].closed)  # type: ignore[attr-defined]
        self.assertTrue(ledger.writer_lease_fast_adoption_capable)
        self.assertEqual(ledger.sleeps, [0.25])
        self.assertEqual(len(ledger.lease_queries), 1)

    def test_psql_only_session_downgrades_fast_adoption_capability(self) -> None:
        class NoGuardLedger(FakeLeasePsqlShareLedger):
            def _make_writer_lease_guard(self, _database_url: str | None) -> None:
                return None

        requested_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}requested"
        ledger = NoGuardLedger(
            [acquired_lease()],
            writer_session_token=requested_session,
        )

        self.assertFalse(ledger.writer_lease_fast_adoption_capable)
        self.assertFalse(
            ledger._writer_session_token.startswith(
                WRITER_LEASE_HEARTBEAT_SESSION_PREFIX
            )
        )

    def test_postgres_startup_adopts_after_one_guard_acquisition_silence(self) -> None:
        updated_at = "2026-06-26 19:49:22.233718+00"
        old_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}old-session"
        new_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
        stdout = io.StringIO()
        clock = FakeMonotonicClock()
        sleeps: list[float] = []

        def sleep_and_advance(seconds: float) -> None:
            sleeps.append(seconds)
            clock.sleep(seconds)

        with contextlib.redirect_stdout(stdout), unittest.mock.patch.object(
            share_ledger_module.time,
            "monotonic",
            clock.monotonic,
        ):
            ledger = FakeLeasePsqlShareLedger(
                [
                    held_lease(
                        session=old_session,
                        updated_at=updated_at,
                        age_seconds=DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
                    ),
                    held_lease(
                        session=old_session,
                        updated_at=updated_at,
                        age_seconds=(
                            DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS * 2
                        ),
                    ),
                    acquired_lease(session=new_session) | {"adopted": True},
                ],
                writer_id="writer-a",
                writer_epoch=1,
                writer_session_token=new_session,
                lease_retry_sleep=sleep_and_advance,
            )

        # The row was already silent for a full interval, but adoption still
        # waits out one interval measured from this process's own guard
        # acquisition before the CAS.
        self.assertEqual(sleeps, [DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS])
        self.assertEqual(len(ledger.lease_queries), 3)
        self.assertNotIn("observed_writer_session_token", ledger.lease_queries[0])
        self.assertNotIn("observed_writer_session_token", ledger.lease_queries[1])
        adoption_query = ledger.lease_queries[2]
        self.assertIn("observed_writer_session_token", adoption_query)
        self.assertIn("observed_lease_updated_at", adoption_query)
        self.assertIn("qbit_ledger_writer_lease.updated_at =", adoption_query)
        self.assertIn(old_session, adoption_query)
        self.assertIn(updated_at, adoption_query)
        self.assertIn("adopted from same-identity predecessor", stdout.getvalue())

    def test_successor_waits_full_guard_silence_despite_minutes_stale_lease_row(self) -> None:
        """Regression: block-39416 restart loop.

        A predecessor dying inside a long persist_accepted_block transaction
        leaves updated_at minutes old the moment its guard session drops.
        The replacement must still grant it a full adoption-silence interval
        measured from advisory-guard acquisition so it can self-fence, no
        matter how stale the lease row already looks.
        """
        updated_at = "2026-06-26 19:44:22.233718+00"
        old_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}old-session"
        new_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
        clock = FakeMonotonicClock()
        sleeps: list[float] = []

        def sleep_and_advance(seconds: float) -> None:
            sleeps.append(seconds)
            clock.sleep(seconds)

        with contextlib.redirect_stdout(io.StringIO()), unittest.mock.patch.object(
            share_ledger_module.time,
            "monotonic",
            clock.monotonic,
        ):
            ledger = FakeLeasePsqlShareLedger(
                [
                    held_lease(
                        session=old_session,
                        updated_at=updated_at,
                        age_seconds=300.0,
                    ),
                    held_lease(
                        session=old_session,
                        updated_at=updated_at,
                        age_seconds=(
                            300.0 + DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS
                        ),
                    ),
                    acquired_lease(session=new_session) | {"adopted": True},
                ],
                writer_id="writer-a",
                writer_epoch=1,
                writer_session_token=new_session,
                lease_retry_sleep=sleep_and_advance,
            )

        self.assertEqual(sleeps, [DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS])
        self.assertEqual(len(ledger.lease_queries), 3)
        # No CAS before the guard-acquisition silence elapsed.
        self.assertNotIn("observed_writer_session_token", ledger.lease_queries[0])
        self.assertNotIn("observed_writer_session_token", ledger.lease_queries[1])
        adoption_query = ledger.lease_queries[2]
        # The CAS still targets the exact predecessor session and row state.
        self.assertIn(old_session, adoption_query)
        self.assertIn(updated_at, adoption_query)

    def test_postgres_startup_keeps_ttl_fallback_for_legacy_session(self) -> None:
        class StopAfterFirstSleep(RuntimeError):
            pass

        def stop_after_first_sleep(_seconds: float) -> None:
            raise StopAfterFirstSleep

        ledger = FakeLeasePsqlShareLedger.__new__(FakeLeasePsqlShareLedger)
        with self.assertRaises(StopAfterFirstSleep):
            ledger.__init__(
                [
                    held_lease(
                        session="legacy-session",
                        updated_at="2026-06-26 19:49:22.233718+00",
                        age_seconds=5.0,
                    )
                ],
                writer_id="writer-a",
                writer_epoch=1,
                writer_session_token=(
                    f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
                ),
                lease_retry_sleep=stop_after_first_sleep,
            )

        self.assertEqual(len(ledger.lease_queries), 1)
        self.assertNotIn("observed_writer_session_token", ledger.lease_queries[0])

    def test_postgres_startup_refuses_concurrently_renewing_same_identity_session(self) -> None:
        first_updated_at = "2026-06-26 19:49:22.233718+00"
        renewed_updated_at = "2026-06-26 19:49:23.233718+00"
        old_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}old-session"
        new_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
        clock = FakeMonotonicClock()
        sleeps: list[float] = []

        class StopAfterRenewalObserved(RuntimeError):
            pass

        def stop_after_second_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            clock.sleep(seconds)
            if len(sleeps) == 2:
                raise StopAfterRenewalObserved

        ledger = FakeLeasePsqlShareLedger.__new__(FakeLeasePsqlShareLedger)
        with self.assertRaises(StopAfterRenewalObserved), unittest.mock.patch.object(
            share_ledger_module.time,
            "monotonic",
            clock.monotonic,
        ):
            ledger.__init__(
                [
                    held_lease(
                        session=old_session,
                        updated_at=first_updated_at,
                        age_seconds=0.1,
                    ),
                    held_lease(
                        session=old_session,
                        updated_at=renewed_updated_at,
                        age_seconds=0.1,
                    ),
                ],
                writer_id="writer-a",
                writer_epoch=1,
                writer_session_token=new_session,
                lease_retry_sleep=stop_after_second_sleep,
            )

        # First wait is floored by the guard-acquisition silence; once that
        # elapsed, the renewing twin's fresh updated_at keeps gating the CAS
        # through the row-silence edge (the silence less the row's 0.1s age).
        self.assertEqual(
            sleeps,
            [
                DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
                round(DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS - 0.1, 6),
            ],
        )
        self.assertEqual(len(ledger.lease_queries), 2)
        self.assertTrue(
            all("observed_writer_session_token" not in query for query in ledger.lease_queries)
        )

    def test_postgres_startup_reobserves_after_losing_adoption_cas(self) -> None:
        first_updated_at = "2026-06-26 19:49:22.233718+00"
        renewed_updated_at = "2026-06-26 19:49:23.233718+00"
        old_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}old-session"
        new_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
        clock = FakeMonotonicClock()
        sleeps: list[float] = []

        def sleep_and_advance(seconds: float) -> None:
            sleeps.append(seconds)
            clock.sleep(seconds)

        with contextlib.redirect_stdout(io.StringIO()), unittest.mock.patch.object(
            share_ledger_module.time,
            "monotonic",
            clock.monotonic,
        ):
            ledger = FakeLeasePsqlShareLedger(
                [
                    held_lease(
                        session=old_session,
                        updated_at=first_updated_at,
                        age_seconds=(
                            DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS + 0.1
                        ),
                    ),
                    held_lease(
                        session=old_session,
                        updated_at=first_updated_at,
                        age_seconds=(
                            2 * DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS
                            + 0.1
                        ),
                    ),
                    held_lease(
                        session=old_session,
                        updated_at=renewed_updated_at,
                        age_seconds=0.0,
                    ),
                    held_lease(
                        session=old_session,
                        updated_at=renewed_updated_at,
                        age_seconds=(
                            DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS + 0.1
                        ),
                    ),
                    acquired_lease(session=new_session) | {"adopted": True},
                ],
                writer_id="writer-a",
                writer_epoch=1,
                writer_session_token=new_session,
                lease_retry_sleep=sleep_and_advance,
            )

        # Guard-acquisition silence first, then a lost CAS re-observes the
        # renewed row and requires a fresh full row-silence interval before
        # the second CAS.
        self.assertEqual(
            sleeps,
            [DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS] * 2,
        )
        self.assertEqual(len(ledger.lease_queries), 5)
        self.assertIn("observed_writer_session_token", ledger.lease_queries[2])
        self.assertIn("observed_writer_session_token", ledger.lease_queries[4])
        self.assertNotIn("observed_writer_session_token", ledger.lease_queries[3])

    def test_postgres_startup_never_adopts_different_writer_or_epoch(self) -> None:
        old_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}old-session"
        new_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
        for holder in (
            held_lease(
                writer_id="writer-b",
                session=old_session,
                updated_at="2026-06-26 19:49:22+00",
                age_seconds=2.0,
            ),
            held_lease(
                writer_epoch=2,
                session=old_session,
                updated_at="2026-06-26 19:49:22+00",
                age_seconds=2.0,
            ),
        ):
            with self.subTest(holder=holder):
                ledger = FakeLeasePsqlShareLedger.__new__(FakeLeasePsqlShareLedger)
                with self.assertRaisesRegex(RuntimeError, "qbit ledger writer lease is held by"):
                    ledger.__init__(
                        [holder],
                        writer_id="writer-a",
                        writer_epoch=1,
                        writer_session_token=new_session,
                    )
                self.assertEqual(len(ledger.lease_queries), 1)
                self.assertNotIn("observed_lease_updated_at", ledger.lease_queries[0])
                self.assertFalse(ledger.fake_writer_lease_guard.held)

    def test_postgres_startup_refuses_another_active_writer_lease(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            with self.assertRaisesRegex(RuntimeError, "qbit ledger writer lease is held by writer-b epoch=1"):
                FakeLeasePsqlShareLedger(
                    [held_lease(writer_id="writer-b", wait_seconds=10.0)],
                    writer_id="writer-a",
                    writer_epoch=1,
                )

        self.assertNotIn("waiting", stdout.getvalue())

    def test_postgres_startup_acquires_expired_lease_immediately(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                writer_id="writer-a",
                writer_epoch=1,
            )

        self.assertEqual(ledger.sleeps, [])
        self.assertEqual(len(ledger.lease_queries), 1)
        self.assertEqual(stdout.getvalue(), "")


class FirstAcquireWriterLeaseRaceTests(unittest.TestCase):
    """The first-ever concurrent acquisition, from the loser's side.

    Under READ COMMITTED the loser's ``ON CONFLICT DO UPDATE`` re-reads only
    the conflicting tuple after its lock wait, so the upsert affects zero rows
    while the holder ``SELECT`` still reads the pre-wait snapshot and finds
    nothing. Both arms empty used to make the whole statement SQL NULL and kill
    startup with a driver-level ``postgres query returned no JSON``. These
    tests drive that interleaving through canned statement results, so they
    pin the loser's experience without a live server.
    """

    def test_statement_carries_a_named_retry_arm_for_the_empty_snapshot(self) -> None:
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease()],
            writer_id="writer-a",
            writer_epoch=1,
        )

        acquire_sql = ledger.lease_queries[0]
        # A third COALESCE arm makes the statement total: it is a constant, so
        # the expression cannot evaluate to SQL NULL however the first two arms
        # resolve.
        self.assertIn(f"'{WRITER_LEASE_ACQUIRE_RETRY_KEY}', true", acquire_sql)
        self.assertIn(f"'lease', '{WRITER_LEASE_ACQUIRE_RETRY_SUBJECT}'", acquire_sql)
        self.assertIn("'acquired', false", acquire_sql)

    def test_first_acquire_loser_retries_into_the_named_lease_refusal(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            with self.assertRaises(RuntimeError) as caught:
                FakeLeasePsqlShareLedger(
                    [
                        snapshot_retry_lease(),
                        held_lease(writer_id="writer-b", wait_seconds=10.0),
                    ],
                    writer_id="writer-a",
                    writer_epoch=1,
                )

        message = str(caught.exception)
        # The operator-facing difference this change exists for: the loser now
        # names the lease and its holder instead of a driver-level parse error.
        self.assertIn(
            "qbit ledger writer lease is held by writer-b epoch=1",
            message,
        )
        self.assertNotIn("postgres query returned no JSON", message)

    def test_first_acquire_loser_can_retry_into_an_ordinary_acquisition(self) -> None:
        # The winner's row is visible on the fresh snapshot and has already
        # expired, so the second statement takes the lease through the ordinary
        # expiry CAS. Startup completes without operator action.
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            ledger = FakeLeasePsqlShareLedger(
                [snapshot_retry_lease(), acquired_lease()],
                writer_id="writer-a",
                writer_epoch=1,
            )

        self.assertEqual(len(ledger.lease_queries), 2)
        self.assertEqual(ledger.sleeps, [])
        self.assertIn("fresh statement snapshot", stdout.getvalue())

    def test_retry_result_never_reaches_the_wait_or_adopt_decisions(self) -> None:
        # The sentinel carries no holder identity, so letting it out of the
        # acquisition path would be read as a holder of None. It is consumed
        # where the remedy is -- a fresh statement snapshot -- and the caller
        # only ever sees an ordinary arm.
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._writer_id = "writer-a"
        ledger._writer_epoch = 1
        ledger._writer_session_token = (
            f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}session-a"
        )
        retry = snapshot_retry_lease()

        self.assertFalse(PsqlShareLedger._can_wait_for_writer_lease(ledger, retry))
        self.assertIsNone(
            PsqlShareLedger._writer_lease_adoption_wait_seconds(ledger, retry)
        )
        self.assertFalse(retry["acquired"])

    def test_unconverging_snapshot_retry_fails_naming_the_lease(self) -> None:
        # A caller whose transaction pinned one snapshot for its whole life
        # cannot advance it by re-running. The bound turns that into a visible
        # failure that still names the lease, never a raw parser error and
        # never an unbounded loop.
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            with self.assertRaises(RuntimeError) as caught:
                FakeLeasePsqlShareLedger(
                    [snapshot_retry_lease()] * WRITER_LEASE_ACQUIRE_RETRY_ATTEMPTS,
                    writer_id="writer-a",
                    writer_epoch=1,
                )

        message = str(caught.exception)
        self.assertIn(WRITER_LEASE_ACQUIRE_RETRY_SUBJECT, message)
        self.assertNotIn("postgres query returned no JSON", message)

    def test_unrelated_empty_json_statement_still_raises_the_parser_error(self) -> None:
        # The lease statement is what became total; the parser is unchanged, so
        # any other statement returning SQL NULL still fails loudly rather than
        # being reinterpreted as a retry.
        with self.assertRaisesRegex(
            RuntimeError,
            "postgres query returned no JSON",
        ):
            parse_single_json_value(None)

        class FakeCursor:
            @staticmethod
            def fetchone() -> tuple[object]:
                return (None,)

        class FakeConnection:
            closed = False

            @staticmethod
            def execute(sql: str) -> FakeCursor:
                return FakeCursor()

            def close(self) -> None:
                pass

        class FakeOperationalError(Exception):
            pass

        fake_psycopg = types.ModuleType("psycopg")
        fake_psycopg.connect = lambda conninfo, **kwargs: FakeConnection()  # type: ignore[attr-defined]
        fake_psycopg.conninfo = types.SimpleNamespace(  # type: ignore[attr-defined]
            conninfo_to_dict=lambda conninfo: {"dbname": "qbit"}
        )
        fake_psycopg.OperationalError = FakeOperationalError  # type: ignore[attr-defined]

        with unittest.mock.patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            client = _NativePostgresClient(
                "postgresql://example.invalid/qbit",
                pool_size=1,
            )
            with self.assertRaisesRegex(
                RuntimeError,
                "postgres query returned no JSON",
            ):
                client.run_json("SELECT to_json(NULL);")


class CannedQueryPsqlShareLedger(PsqlShareLedger):
    """Subprocess-mode ledger whose _run_json pops canned results.

    Mirrors FakeLeasePsqlShareLedger but is used for the hot-path stats and
    native-backend tests, which also need the recorded SQL for assertions.
    """

    def __init__(self, results: list[object], **kwargs: Any):
        self.canned_results = list(results)
        self.queries: list[str] = []
        kwargs.setdefault("native_client_mode", "0")
        super().__init__(
            psql_command="psql postgresql://example.invalid/qbit",
            lease_retry_sleep=lambda _seconds: None,
            **kwargs,
        )

    def _run_json(self, sql: str) -> Any:
        self.queries.append(sql)
        if not self.canned_results:
            raise AssertionError(f"unexpected extra query: {sql[:120]}")
        result = self.canned_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def acquired_lease_result() -> dict[str, object]:
    return acquired_lease(writer_id="prism-coordinator", writer_epoch=1)


def record_payload(
    index: int,
    share_seq: int,
    *,
    new_miner: bool = False,
) -> dict[str, object]:
    pending = pending_share(index)
    return {
        "share_seq": share_seq,
        "share_id": pending.share_id,
        "miner_id": pending.miner_id,
        "order_key": pending.order_key,
        "p2mr_program_hex": pending.p2mr_program_hex,
        "share_difficulty": str(pending.share_difficulty),
        "network_difficulty": str(pending.network_difficulty),
        "template_height": pending.template_height,
        "job_id": pending.job_id,
        "job_issued_at_ms": pending.job_issued_at_ms,
        "accepted_at_ms": pending.accepted_at_ms,
        "ntime": pending.ntime,
        "credit_policy": None,
        "new_miner": new_miner,
    }


def stats_payload(
    count: int,
    distinct_miner_count: int,
    max_share_seq: int,
) -> dict[str, object]:
    return {
        "accepted_share_count": count,
        "distinct_miner_count": distinct_miner_count,
        "max_share_seq": max_share_seq,
    }


class NativeClientSelectionTests(unittest.TestCase):
    def test_operation_timeout_bounds_local_writer_lock_admission(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        gate = threading.Lock()
        gate.acquire()
        started = time.monotonic()
        try:
            with ledger.operation_timeout(0.02):
                with self.assertRaisesRegex(
                    LedgerOperationTimeout,
                    "writer lock",
                ):
                    with ledger._operation_gate(gate, "writer lock"):
                        self.fail("contended writer lock unexpectedly acquired")
        finally:
            gate.release()
        self.assertLess(time.monotonic() - started, 0.5)

    def test_operation_gate_stamps_progress_while_admission_is_blocked(self) -> None:
        """A blocked admission wait must report liveness to its caller.

        Landing-class callers run this wait on a watchdog-monitored thread.
        No statement has been sent yet, so nothing on the server can bound or
        report the wait; without the progress hook the whole admission budget
        is silence and the watchdog kills a coordinator that is merely queued
        behind another writer. Slicing must not extend the wait either: the
        caller's deadline still ends it with the same error.
        """
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        gate = threading.Lock()
        gate.acquire()
        stamps: list[float] = []
        started = time.monotonic()
        try:
            with ledger.statement_timeout(0.4):
                with ledger.operation_progress(
                    lambda: stamps.append(time.monotonic()),
                    slice_seconds=0.05,
                ):
                    with self.assertRaisesRegex(
                        LedgerOperationTimeout,
                        "writer lock",
                    ):
                        with ledger._operation_gate(gate, "writer lock"):
                            self.fail("contended writer lock unexpectedly acquired")
        finally:
            gate.release()
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(len(stamps), 2)
        # No per-gap upper bound: a loaded CI host can stall a 0.05s slice
        # wakeup well past any tight cutoff, and cadence is already proven
        # deterministically under a virtual clock elsewhere in this module.
        self.assertGreaterEqual(elapsed, 0.4)
        self.assertLess(elapsed, 3.0)

    def test_operation_gate_slicing_times_out_on_the_virtual_clock(self) -> None:
        """The sliced admission wait is driven by the injected clock.

        With a virtual clock installed and a gate that consumes each slice
        without yielding, the deadline must expire exactly when the virtual
        clock crosses the admission budget — never on wall time — so the
        slicing loop can be exercised deterministically.
        """
        clock = FakeMonotonicClock()
        started = clock.now

        class SliceConsumingGate:
            def __init__(self) -> None:
                self.timeouts: list[float] = []

            def acquire(self, timeout: float = -1) -> bool:
                self.timeouts.append(timeout)
                if len(self.timeouts) > 50:
                    raise AssertionError(
                        "admission wait ignored the virtual-clock deadline"
                    )
                clock.sleep(timeout)
                return False

            def release(self) -> None:
                raise AssertionError("gate released without being acquired")

        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        ledger._monotonic = clock.monotonic
        gate = SliceConsumingGate()
        stamps: list[float] = []
        with ledger.statement_timeout(1.0):
            with ledger.operation_progress(
                lambda: stamps.append(clock.now),
                slice_seconds=0.25,
            ):
                with self.assertRaisesRegex(LedgerOperationTimeout, "writer lock"):
                    ledger._acquire_operation_gate(gate, "writer lock")
        self.assertEqual(clock.now, started + 1.0)
        self.assertEqual(gate.timeouts, [0.25, 0.25, 0.25, 0.25])
        self.assertEqual(
            stamps,
            [started + 0.25, started + 0.5, started + 0.75],
        )

    def test_operation_gate_without_progress_hook_waits_once(self) -> None:
        """The unhooked path is still a single blocking acquire.

        Ordinary ledger callers have no liveness monitor to satisfy and must
        not start paying for wakeups they cannot use.
        """
        acquires: list[float] = []

        class RecordingGate:
            def acquire(self, timeout: float = -1) -> bool:
                acquires.append(timeout)
                return False

            def release(self) -> None:
                raise AssertionError("gate released without being acquired")

        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        with ledger.statement_timeout(0.02):
            with self.assertRaisesRegex(LedgerOperationTimeout, "writer lock"):
                with ledger._operation_gate(RecordingGate(), "writer lock"):
                    self.fail("contended writer lock unexpectedly acquired")
        self.assertEqual(acquires, [0.02])

    def test_operation_progress_validates_and_restores_its_slice(self) -> None:
        """The hook scope matches its sibling timeout scopes exactly."""
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        for slice_seconds in (0.0, -1.0, float("inf"), float("nan")):
            with self.subTest(slice_seconds=slice_seconds):
                with self.assertRaisesRegex(ValueError, "finite and positive"):
                    with ledger.operation_progress(
                        lambda: None,
                        slice_seconds=slice_seconds,
                    ):
                        self.fail("invalid progress slice unexpectedly accepted")
        self.assertIsNone(ledger._operation_progress_hook())
        def outer() -> None:
            return None

        def inner() -> None:
            return None

        with ledger.operation_progress(outer, slice_seconds=1.0):
            self.assertEqual(ledger._operation_progress_hook(), (outer, 1.0))
            with ledger.operation_progress(inner, slice_seconds=0.25):
                self.assertEqual(ledger._operation_progress_hook(), (inner, 0.25))
            self.assertEqual(ledger._operation_progress_hook(), (outer, 1.0))
        self.assertIsNone(ledger._operation_progress_hook())

    def test_operation_progress_local_is_created_before_any_scope(self) -> None:
        """The production path must never lazily create the thread-local.

        ``operation_progress``'s ``getattr``-then-assign is not atomic, so a
        ledger that reaches its first scope without the attribute already in
        place can lose one caller's hook to a concurrent installer.
        """
        with contextlib.redirect_stdout(io.StringIO()):
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                writer_id="writer-a",
                writer_epoch=1,
            )
        self.assertIn("_operation_progress_local", vars(ledger))
        self.assertIsInstance(
            vars(ledger)["_operation_progress_local"],
            threading.local,
        )

    def test_concurrent_first_scopes_share_one_progress_local(self) -> None:
        """Two first-ever scopes at once must both keep their own hook.

        The lazy check-then-set lets both callers build a ``local()`` and one
        assignment win; the loser's hook then lives on an object the ledger
        no longer references, so its admission wait silently reverts to the
        heartbeat-silent path -- the exact failure this hook exists to
        remove. The patched ``local`` below makes that interleaving
        deterministic rather than a sleep race: it parks every constructing
        thread on a two-party barrier, so neither can assign before the other
        has decided to construct. A ledger that built the thread-local in
        ``__init__`` never constructs one here at all, and the barrier is
        simply never reached.
        """
        with contextlib.redirect_stdout(io.StringIO()):
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease()],
                writer_id="writer-a",
                writer_epoch=1,
            )
        constructing = threading.Barrier(2, timeout=10)
        installed = threading.Barrier(2, timeout=10)

        def racing_local() -> threading.local:
            # Neither thread may leave the constructor until both have
            # decided to build one, so the check-then-set cannot serialize
            # itself by luck.
            constructing.wait()
            return threading.local()

        def first_stamp() -> None:
            return None

        def second_stamp() -> None:
            return None

        observed: dict[str, object] = {}
        errors: list[BaseException] = []

        def install(name: str, hook: Any) -> None:
            try:
                with ledger.operation_progress(hook, slice_seconds=0.5):
                    # Both hooks are installed before either is read, so a
                    # lost assignment cannot hide behind thread ordering.
                    installed.wait()
                    observed[name] = ledger._operation_progress_hook()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        with unittest.mock.patch.object(
            share_ledger_module,
            "local",
            racing_local,
        ):
            threads = [
                threading.Thread(target=install, args=("first", first_stamp)),
                threading.Thread(target=install, args=("second", second_stamp)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
        self.assertEqual(errors, [])
        self.assertEqual(observed.get("first"), (first_stamp, 0.5))
        self.assertEqual(observed.get("second"), (second_stamp, 0.5))

    def test_operation_progress_nesting_keeps_the_tighter_slice(self) -> None:
        """Nesting tightens the stamp cadence, exactly like its siblings.

        ``operation_timeout`` and ``statement_timeout`` merge by minimum, so
        a nested scope can only narrow what the enclosing caller allowed. A
        progress scope that replaced the slice outright would let an inner
        caller widen the heartbeat gaps an outer caller had already sized
        against its own monitor.
        """
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)

        def outer() -> None:
            return None

        def inner() -> None:
            return None

        with ledger.operation_progress(outer, slice_seconds=0.25):
            self.assertEqual(ledger._operation_progress_hook(), (outer, 0.25))
            # A larger inner slice does not widen the cadence; the callback
            # is still the inner caller's, because it is the one whose
            # liveness the wait now belongs to.
            with ledger.operation_progress(inner, slice_seconds=4.0):
                self.assertEqual(
                    ledger._operation_progress_hook(),
                    (inner, 0.25),
                )
            self.assertEqual(ledger._operation_progress_hook(), (outer, 0.25))
        self.assertIsNone(ledger._operation_progress_hook())

    def test_operation_gate_propagates_progress_hook_failures(self) -> None:
        """A liveness stamp that cannot be taken is a failure, not noise.

        Swallowing it would leave the caller blocked inside a lock wait it
        believes is being reported, which is the exact silence this hook
        exists to remove.
        """

        class Stamped(RuntimeError):
            pass

        def on_progress() -> None:
            raise Stamped("heartbeat unavailable")

        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        gate = threading.Lock()
        gate.acquire()
        try:
            with ledger.statement_timeout(5.0):
                with ledger.operation_progress(on_progress, slice_seconds=0.01):
                    with self.assertRaises(Stamped):
                        with ledger._operation_gate(gate, "writer lock"):
                            self.fail("contended writer lock unexpectedly acquired")
        finally:
            gate.release()

    def test_note_operation_progress_reports_only_when_a_hook_is_installed(self) -> None:
        """The between-statements reporter is opt-in, exactly like the wait.

        Ordinary ledger callers have no liveness monitor, so an operation
        that reports unconditionally would be paying for stamps nobody
        reads. The hook is installed only by callers whose thread is
        watchdog-monitored.
        """
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        # No hook installed: reporting must be a silent no-op, not an error.
        ledger._note_operation_progress()
        stamps: list[int] = []
        with ledger.operation_progress(
            lambda: stamps.append(len(stamps)),
            slice_seconds=0.05,
        ):
            ledger._note_operation_progress()
            ledger._note_operation_progress()
        self.assertEqual(stamps, [0, 1])
        # The scope restored the previous (absent) hook, so this is silent.
        ledger._note_operation_progress()
        self.assertEqual(stamps, [0, 1])

    def test_note_operation_progress_propagates_hook_failures(self) -> None:
        """A stamp that cannot be taken is a failure, as inside the gate wait."""

        class Stamped(RuntimeError):
            pass

        def on_progress() -> None:
            raise Stamped("heartbeat unavailable")

        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        with ledger.operation_progress(on_progress, slice_seconds=0.05):
            with self.assertRaises(Stamped):
                ledger._note_operation_progress()

    def test_confirm_accepted_block_reports_between_its_two_statements(self) -> None:
        """The publication-ordinal read must not extend one silent span.

        confirm_accepted_block holds the writer lock across a mutating
        statement and the follow-up read its command snapshot could not see.
        No admission slice runs between them, so without a report the caller's
        monitor is asked to sit through two statement budgets in a row -- at
        the 120s default that is the whole watchdog tolerance with no margin
        at all (issue #125).
        """
        ledger = GateStatementRecordingPsqlShareLedger(
            [
                {"backend": "postgres-psql", "confirmed_count": 1},
                {"audit_publication_sequence": 7},
            ]
        )
        with ledger.operation_progress(
            ledger.note_progress,
            slice_seconds=0.05,
        ):
            response = ledger.confirm_accepted_block(
                block_hash="ab" * 32,
                active_tip_height=10,
            )
        self.assertEqual(response["audit_publication_sequence"], 7)
        self.assertEqual(
            ledger.events,
            ["statement-1", "progress", "statement-2"],
        )

    def test_confirmed_count_zero_stays_a_single_silent_statement(self) -> None:
        """A one-statement gate body must not add stamps it does not owe."""
        ledger = GateStatementRecordingPsqlShareLedger(
            [{"backend": "postgres-psql", "confirmed_count": 0}]
        )
        with ledger.operation_progress(
            ledger.note_progress,
            slice_seconds=0.05,
        ):
            ledger.confirm_accepted_block(
                block_hash="ab" * 32,
                active_tip_height=10,
            )
        self.assertEqual(ledger.events, ["statement-1"])

    def test_reactivate_pool_block_reports_between_its_two_statements(self) -> None:
        """The reorg-walk twin of confirm_accepted_block's ordinal read."""
        ledger = GateStatementRecordingPsqlShareLedger(
            [
                {"backend": "postgres-psql", "reactivated_count": 1},
                {"audit_publication_sequence": 4},
            ]
        )
        with ledger.operation_progress(
            ledger.note_progress,
            slice_seconds=0.05,
        ):
            response = ledger.reactivate_pool_block(
                block_hash="cd" * 32,
                active_tip_height=10,
            )
        self.assertEqual(response["audit_publication_sequence"], 4)
        self.assertEqual(
            ledger.events,
            ["statement-1", "progress", "statement-2"],
        )

    def test_carry_forward_integrity_report_reports_between_its_reads(self) -> None:
        """Two reads that must see one another cannot release the lock.

        The report and its audit head are consistent only because they run
        under one held writer lock, so the report between them is the only
        liveness a caller's monitor can get.
        """
        ledger = GateStatementRecordingPsqlShareLedger(
            [
                {"checked_active_rows": 0, "mismatch_count": 0},
                [],
            ]
        )
        with ledger.operation_progress(
            ledger.note_progress,
            slice_seconds=0.05,
        ):
            report = ledger.carry_forward_integrity_report()
        self.assertEqual(report["audit_row_count"], 0)
        self.assertEqual(
            ledger.events,
            ["statement-1", "progress", "statement-2"],
        )

    def test_statement_timeout_refreshes_for_each_database_step(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        clock = {"now": 10.0}

        with unittest.mock.patch(
            "lab.prism.share_ledger.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            with ledger.statement_timeout(0.5):
                self.assertEqual(ledger._remaining_operation_timeout(), 0.5)
                clock["now"] += 60.0
                self.assertEqual(ledger._remaining_operation_timeout(), 0.5)

    def test_subprocess_operation_timeout_sets_client_and_server_deadlines(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._command = ["psql"]
        ledger._native = None
        ledger._operation_timeout_local = threading.local()
        completed = unittest.mock.Mock(returncode=0, stdout="{}\n", stderr="")

        with unittest.mock.patch(
            "lab.prism.share_ledger.subprocess.run",
            return_value=completed,
        ) as run:
            with ledger.operation_timeout(0.5):
                self.assertEqual(ledger._run_sql("SELECT '{}'::json;"), "{}\n")

        kwargs = run.call_args.kwargs
        self.assertGreater(float(kwargs["timeout"]), 0.0)
        self.assertLessEqual(float(kwargs["timeout"]), 0.5)
        self.assertEqual(kwargs["env"]["PGCONNECT_TIMEOUT"], "1")
        self.assertIn("statement_timeout=", kwargs["env"]["PGOPTIONS"])
        self.assertIn("lock_timeout=", kwargs["env"]["PGOPTIONS"])
        self.assertIn("VERBOSITY=verbose", run.call_args.args[0])

    def test_subprocess_server_deadlines_raise_operation_timeout(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._command = ["psql"]
        ledger._native = None
        ledger._operation_timeout_local = threading.local()
        deadline_errors = (
            "ERROR:  57014: canceling statement due to statement timeout",
            "FEHLER:  57014: Anweisung wegen Zeitüberschreitung abgebrochen",
            "ERROR:  55P03: canceling statement due to lock timeout",
            "FEHLER:  55P03: Anweisung wegen Zeitüberschreitung abgebrochen",
            "psql: error: connection to server failed: timeout expired",
        )

        for stderr in deadline_errors:
            with self.subTest(stderr=stderr), unittest.mock.patch(
                "lab.prism.share_ledger.subprocess.run",
                return_value=unittest.mock.Mock(
                    returncode=3,
                    stdout="",
                    stderr=stderr,
                ),
            ):
                with ledger.operation_timeout(0.5):
                    with self.assertRaises(LedgerOperationTimeout):
                        ledger._run_sql("SELECT '{}'::json;")

    def test_subprocess_hard_error_remains_runtime_error(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._command = ["psql"]
        ledger._native = None
        ledger._operation_timeout_local = threading.local()
        completed = unittest.mock.Mock(
            returncode=3,
            stdout="",
            stderr="ERROR: permission denied for table qbit_block_candidate_outbox",
        )

        with unittest.mock.patch(
            "lab.prism.share_ledger.subprocess.run",
            return_value=completed,
        ), ledger.operation_timeout(0.5):
            with self.assertRaisesRegex(RuntimeError, "permission denied"):
                ledger._run_sql("SELECT '{}'::json;")

    def test_native_operation_timeout_is_transaction_local(self) -> None:
        class OperationalError(Exception):
            pass

        class FakePsycopg:
            pass

        FakePsycopg.OperationalError = OperationalError  # type: ignore[attr-defined]
        executions: list[str] = []
        borrowed_with: list[float | None] = []

        class FakeConnection:
            @contextlib.contextmanager
            def transaction(self) -> Any:
                yield

            def execute(self, sql: str) -> FakeConnection:
                executions.append(sql)
                return self

            def fetchone(self) -> tuple[object]:
                return ({"ok": True},)

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg

        @contextlib.contextmanager
        def connection(*, timeout_seconds: float | None = None) -> Any:
            borrowed_with.append(timeout_seconds)
            yield FakeConnection()

        client.connection = connection  # type: ignore[method-assign]

        self.assertEqual(
            client.run_json("SELECT json_build_object('ok', true)", timeout_seconds=0.5),
            {"ok": True},
        )
        self.assertEqual(len(borrowed_with), 1)
        self.assertIsNotNone(borrowed_with[0])
        self.assertGreater(float(borrowed_with[0]), 0.0)
        self.assertLessEqual(float(borrowed_with[0]), 0.5)
        self.assertRegex(executions[0], r"^SET LOCAL statement_timeout = '\d+ms'$")
        self.assertRegex(executions[1], r"^SET LOCAL lock_timeout = '\d+ms'$")
        self.assertEqual(executions[2], "SELECT json_build_object('ok', true)")

    def test_native_server_deadline_raises_operation_timeout(self) -> None:
        class OperationalError(Exception):
            pass

        class FakePsycopg:
            pass

        FakePsycopg.OperationalError = OperationalError  # type: ignore[attr-defined]

        class FakeConnection:
            def __init__(self, error: OperationalError):
                self.error = error

            @contextlib.contextmanager
            def transaction(self) -> Any:
                yield

            def execute(self, sql: str) -> FakeConnection:
                if sql.startswith("SET LOCAL"):
                    return self
                raise self.error

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg

        deadline_errors = (
            ("57014", "canceling statement due to statement timeout"),
            ("55P03", "Anweisung wegen Zeitüberschreitung abgebrochen"),
        )
        for sqlstate, message in deadline_errors:
            with self.subTest(sqlstate=sqlstate):
                error = OperationalError(message)
                error.sqlstate = sqlstate  # type: ignore[attr-defined]

                @contextlib.contextmanager
                def connection(*, timeout_seconds: float | None = None) -> Any:
                    yield FakeConnection(error)

                client.connection = connection  # type: ignore[method-assign]
                with self.assertRaises(LedgerOperationTimeout):
                    client.run_json("SELECT '{}'::json", timeout_seconds=0.5)

    def test_database_url_extraction_variants(self) -> None:
        self.assertEqual(
            database_url_from_psql_command(["psql", "postgres://u:p@h:5432/db"]),
            "postgres://u:p@h:5432/db",
        )
        self.assertEqual(
            database_url_from_psql_command(["psql", "-d", "postgresql://h/db"]),
            "postgresql://h/db",
        )
        self.assertEqual(
            database_url_from_psql_command(["psql", "--dbname=postgresql://h/db"]),
            "postgresql://h/db",
        )
        self.assertIsNone(database_url_from_psql_command(["psql", "-h", "host", "-U", "user"]))
        self.assertIsNone(database_url_from_psql_command(["./fake-psql.sh"]))

    def test_mode_off_forces_subprocess_backend(self) -> None:
        ledger = CannedQueryPsqlShareLedger([acquired_lease_result()], native_client_mode="0")
        self.assertIsNone(ledger._native)
        self.assertEqual(ledger.execution_backend, "psql-subprocess")

    def test_auto_mode_without_dsn_stays_on_subprocess(self) -> None:
        class NoDsnLedger(CannedQueryPsqlShareLedger):
            def __init__(self) -> None:
                self.canned_results = [acquired_lease_result()]
                self.queries = []
                PsqlShareLedger.__init__(
                    self,
                    psql_command="./fake-psql.sh --flag",
                    native_client_mode="auto",
                    lease_retry_sleep=lambda _seconds: None,
                )

        ledger = NoDsnLedger()
        self.assertIsNone(ledger._native)

    def test_forced_native_without_dsn_raises(self) -> None:
        # The DSN-free command means _make_native_client must raise before
        # any lease query regardless of whether psycopg is installed.
        with self.assertRaises(ValueError):
            PsqlShareLedger(psql_command="./fake-psql.sh", native_client_mode="1")

    def test_forced_native_without_psycopg_raises(self) -> None:
        with unittest.mock.patch.dict(sys.modules, {"psycopg": None}):
            with self.assertRaises(ValueError):
                PsqlShareLedger(
                    psql_command="psql postgres://example.invalid/qbit",
                    native_client_mode="1",
                )

    def test_auto_native_psycopg_import_fallback_warns(self) -> None:
        stdout = io.StringIO()
        with unittest.mock.patch.dict(sys.modules, {"psycopg": None}):
            with contextlib.redirect_stdout(stdout):
                ledger = CannedQueryPsqlShareLedger(
                    [acquired_lease_result()],
                    native_client_mode="auto",
                )
        self.assertIsNone(ledger._native)
        self.assertEqual(ledger.execution_backend, "psql-subprocess")
        self.assertIn(
            "psycopg import failed; falling back to the psql subprocess backend",
            stdout.getvalue(),
        )

    def test_psql_run_sql_uses_single_transaction(self) -> None:
        recorded: dict[str, object] = {}

        def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
            recorded["cmd"] = list(cmd)
            return types.SimpleNamespace(returncode=0, stdout="{}\n", stderr="")

        ledger = CannedQueryPsqlShareLedger([acquired_lease_result()])
        with unittest.mock.patch.object(
            share_ledger_module.subprocess, "run", side_effect=fake_run
        ):
            output = ledger._run_sql("SELECT '{}'::json;")
        self.assertEqual(output, "{}\n")
        cmd = recorded["cmd"]
        self.assertIn("--single-transaction", cmd)
        self.assertIn("--no-psqlrc", cmd)
        self.assertIn("ON_ERROR_STOP=1", cmd)

    def test_parse_single_json_value_contract(self) -> None:
        self.assertEqual(parse_single_json_value('{"a": 1}'), {"a": 1})
        self.assertEqual(parse_single_json_value({"a": 1}), {"a": 1})
        with self.assertRaises(RuntimeError):
            parse_single_json_value(None)

    def test_run_json_routes_through_native_client(self) -> None:
        class FakeNative:
            def __init__(self) -> None:
                self.statements: list[str] = []

            def run_json(self, sql: str, *, retry_safe: bool = False) -> Any:
                self.statements.append(sql)
                self.retry_safe = retry_safe
                return stats_payload(5, 1, 8)

            def close(self) -> None:
                return None

        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._lock = threading.Lock()
        ledger._read_semaphore = threading.BoundedSemaphore(1)
        ledger._stats_lock = threading.Lock()
        ledger._stats_refresh_lock = threading.Lock()
        ledger._stats_counts = None
        ledger._stats_max_share_seq = 0
        ledger._stats_note_buffer = None
        ledger._stats_refreshed_monotonic = None
        ledger._accepted_stats_cache_seconds = 60.0
        native = FakeNative()
        ledger._native = native

        stats = PsqlShareLedger.accepted_share_stats(ledger)
        self.assertEqual(
            stats,
            {"accepted_share_count": 5, "distinct_miner_count": 1},
        )
        self.assertEqual(len(native.statements), 1)
        self.assertIn("qbit_share_ledger", native.statements[0])
        self.assertTrue(native.retry_safe)

    def test_native_mutation_operational_error_is_not_retried(self) -> None:
        class OperationalError(Exception):
            pass

        class FakePsycopg:
            pass

        FakePsycopg.OperationalError = OperationalError  # type: ignore[attr-defined]
        executions: list[str] = []
        outcomes: list[object] = [OperationalError("response lost"), {"ok": True}]

        class FakeConnection:
            def execute(self, sql: str) -> FakeConnection:
                executions.append(sql)
                outcome = outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                self.row = outcome
                return self

            def fetchone(self) -> tuple[object]:
                return (self.row,)

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg

        @contextlib.contextmanager
        def connection() -> Any:
            yield FakeConnection()

        client.connection = connection  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "postgres query failed"):
            client.run_json("UPDATE counter SET value = value + 1 RETURNING value")

        self.assertEqual(executions, ["UPDATE counter SET value = value + 1 RETURNING value"])
        self.assertEqual(len(outcomes), 1)

    def test_native_retry_safe_read_retries_once(self) -> None:
        class OperationalError(Exception):
            pass

        class FakePsycopg:
            pass

        FakePsycopg.OperationalError = OperationalError  # type: ignore[attr-defined]
        executions: list[str] = []
        outcomes: list[object] = [OperationalError("stale connection"), {"count": 11}]

        class FakeConnection:
            def execute(self, sql: str) -> FakeConnection:
                executions.append(sql)
                outcome = outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                self.row = outcome
                return self

            def fetchone(self) -> tuple[object]:
                return (self.row,)

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg

        @contextlib.contextmanager
        def connection() -> Any:
            yield FakeConnection()

        client.connection = connection  # type: ignore[method-assign]

        result = client.run_json("SELECT count(*)", retry_safe=True)

        self.assertEqual(result, {"count": 11})
        self.assertEqual(executions, ["SELECT count(*)", "SELECT count(*)"])

    def test_ctv_attempt_journal_uses_non_retrying_native_execution(self) -> None:
        class FakeNative:
            def __init__(self) -> None:
                self.calls: list[tuple[str, bool]] = []

            def run_json(self, sql: str, *, retry_safe: bool = False) -> Any:
                self.calls.append((sql, retry_safe))
                raise RuntimeError("ambiguous connection loss")

        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._lock = threading.Lock()
        ledger._ctv_broadcast_attempt_detail_limit = 20
        ledger._ctv_broadcast_retry_backoff_seconds = 300
        ledger._writer_id = "writer-a"
        ledger._writer_epoch = 1
        ledger._writer_session_token = "session-a"
        ledger._lease_interval_sql = "make_interval(secs => 30)"
        native = FakeNative()
        ledger._native = native

        with self.assertRaisesRegex(RuntimeError, "ambiguous connection loss"):
            ledger.record_ctv_fanout_broadcast_attempt(
                fanout_txid="22" * 32,
                attempt_status="submitted",
            )

        self.assertEqual(len(native.calls), 1)
        sql, retry_safe = native.calls[0]
        self.assertFalse(retry_safe)
        self.assertIn("INSERT INTO qbit_ctv_fanout_broadcast_attempts", sql)
        self.assertIn("broadcast_attempt_count = artifact.broadcast_attempt_count + 1", sql)


def make_session_guards(**overrides: Any) -> Any:
    """Build a PostgresSessionGuards, resolving the class at call time.

    Resolved through the module object so a tree without the session guards
    fails each guard test individually with a clear AttributeError instead
    of breaking the whole module import.
    """
    kwargs: dict[str, Any] = {
        "idle_in_transaction_timeout_seconds": 15.0,
        "tcp_keepalives_idle_seconds": 30,
        "tcp_keepalives_interval_seconds": 10,
        "tcp_keepalives_count": 3,
    }
    kwargs.update(overrides)
    return share_ledger_module.PostgresSessionGuards(**kwargs)


class PostgresSessionGuardTests(unittest.TestCase):
    def test_native_pool_connect_carries_session_guard_options(self) -> None:
        connect_kwargs: list[dict[str, Any]] = []

        class FakeConninfo:
            @staticmethod
            def conninfo_to_dict(conninfo: str) -> dict[str, Any]:
                return {"dbname": "qbit"}

        class FakePsycopg:
            conninfo = FakeConninfo

            @staticmethod
            def connect(conninfo: str, **kwargs: Any) -> object:
                connect_kwargs.append(kwargs)
                return object()

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg
        client._conninfo = "postgresql://example.invalid/qbit"
        client._application_name = "qbit-prism-writer-test"
        client._session_guards = make_session_guards(
            idle_in_transaction_timeout_seconds=2.5,
            tcp_keepalives_idle_seconds=31,
            tcp_keepalives_interval_seconds=7,
            tcp_keepalives_count=4,
        )

        client._connect()

        self.assertEqual(len(connect_kwargs), 1)
        options = connect_kwargs[0]["options"]
        self.assertIn("-c idle_in_transaction_session_timeout=2500ms", options)
        self.assertIn("-c tcp_keepalives_idle=31", options)
        self.assertIn("-c tcp_keepalives_interval=7", options)
        self.assertIn("-c tcp_keepalives_count=4", options)

    def test_native_pool_connect_preserves_operator_conninfo_options(self) -> None:
        connect_kwargs: list[dict[str, Any]] = []
        operator_options = "-c geqo=off -c idle_in_transaction_session_timeout=0"

        class FakeConninfo:
            @staticmethod
            def conninfo_to_dict(conninfo: str) -> dict[str, Any]:
                return {"dbname": "qbit", "options": operator_options}

        class FakePsycopg:
            conninfo = FakeConninfo

            @staticmethod
            def connect(conninfo: str, **kwargs: Any) -> object:
                connect_kwargs.append(kwargs)
                return object()

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg
        client._conninfo = (
            "postgresql://example.invalid/qbit?options=-c%20geqo%3Doff"
        )
        client._application_name = None
        guards = make_session_guards()
        client._session_guards = guards

        client._connect()

        options = connect_kwargs[0]["options"]
        # Both the operator's DSN-level options and the guards are present,
        # and the guard fragment comes last so its -c duplicates win.
        self.assertIn("-c geqo=off", options)
        self.assertTrue(options.endswith(guards.options_fragment()))
        self.assertLess(
            options.index("geqo"),
            options.index("tcp_keepalives_idle"),
        )

    def test_lease_guard_keeps_statement_timeout_and_gains_session_guards(self) -> None:
        connect_calls: list[dict[str, Any]] = []

        class FakeGuardConnection:
            closed = False

            def close(self) -> None:
                pass

        def fake_connect(conninfo: str, **kwargs: Any) -> FakeGuardConnection:
            connect_calls.append(kwargs)
            return FakeGuardConnection()

        fake_psycopg = types.ModuleType("psycopg")
        fake_psycopg.connect = fake_connect  # type: ignore[attr-defined]
        fake_psycopg.conninfo = types.SimpleNamespace(  # type: ignore[attr-defined]
            conninfo_to_dict=lambda conninfo: {"dbname": "qbit"}
        )

        with unittest.mock.patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            _NativePostgresLeaseGuard(
                "postgresql://example.invalid/qbit",
                1234,
                session_guards=make_session_guards(),
            )

        self.assertEqual(len(connect_calls), 1)
        options = connect_calls[0]["options"]
        self.assertIn("-c statement_timeout=500", options)
        self.assertIn("-c idle_in_transaction_session_timeout=15000ms", options)
        self.assertIn("-c tcp_keepalives_idle=30", options)
        self.assertIn("-c tcp_keepalives_interval=10", options)
        self.assertIn("-c tcp_keepalives_count=3", options)

    def test_subprocess_backend_sets_session_guards_without_a_deadline(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._command = ["psql"]
        ledger._native = None
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        ledger._session_guards = make_session_guards()
        completed = unittest.mock.Mock(returncode=0, stdout="{}\n", stderr="")

        with unittest.mock.patch.dict(
            "os.environ",
            {"PGOPTIONS": "-c geqo=off"},
        ), unittest.mock.patch(
            "lab.prism.share_ledger.subprocess.run",
            return_value=completed,
        ) as run:
            self.assertIsNone(ledger._remaining_operation_timeout())
            self.assertEqual(ledger._run_sql("SELECT '{}'::json;"), "{}\n")

        kwargs = run.call_args.kwargs
        pgoptions = kwargs["env"]["PGOPTIONS"]
        self.assertIn("-c idle_in_transaction_session_timeout=15000ms", pgoptions)
        self.assertIn("-c tcp_keepalives_idle=30", pgoptions)
        self.assertIn("-c tcp_keepalives_interval=10", pgoptions)
        self.assertIn("-c tcp_keepalives_count=3", pgoptions)
        # Operator-supplied PGOPTIONS survive, ahead of the guard fragment.
        self.assertTrue(pgoptions.startswith("-c geqo=off"))
        # No armed deadline: no per-statement bounds, no subprocess timeout.
        self.assertNotIn("statement_timeout", pgoptions)
        self.assertNotIn("lock_timeout", pgoptions)
        self.assertNotIn("timeout", kwargs)


class ReadOnlySessionEnforcementTests(unittest.TestCase):
    """``read_only=True`` must be a promise PostgreSQL keeps, not a gate.

    The refusing writer gate only covers paths that take the gate. Any
    statement issued outside it — the lease upsert, a one-shot psql release —
    reaches SQL exactly as it would on a read-write ledger, and until #157
    pointed the public tier at a hot standby, nothing but the deployment
    topology stopped it. These tests pin the connect-time GUC that makes the
    server refuse instead, on every connection the instance can create.
    """

    READ_ONLY_GUC = "-c default_transaction_read_only=on"

    def test_read_only_ledger_builds_read_only_session_guards(self) -> None:
        ledger = PsqlShareLedger(
            psql_command="psql postgresql://example.invalid/qbit",
            native_client_mode="0",
            read_only=True,
        )
        self.addCleanup(ledger.close)

        self.assertTrue(ledger._session_guards.read_only)
        fragment = ledger._session_guards.options_fragment()
        self.assertIn(self.READ_ONLY_GUC, fragment)
        # The orphan-reaping guards #137 established are preserved, not
        # displaced: one options carrier, both guarantees.
        self.assertIn("-c idle_in_transaction_session_timeout=15000ms", fragment)
        self.assertIn("-c tcp_keepalives_idle=30", fragment)

    def test_writable_ledger_keeps_a_writable_session(self) -> None:
        # The coordinator's own ledger must be untouched by this: it commits
        # every share, block and settlement the pool has.
        ledger = FakeLeasePsqlShareLedger(
            [acquired_lease()],
            writer_id="writer-a",
            writer_epoch=1,
        )
        self.addCleanup(ledger.close)

        self.assertFalse(ledger._session_guards.read_only)
        self.assertNotIn(
            "default_transaction_read_only",
            ledger._session_guards.options_fragment(),
        )

    def test_read_only_native_pool_connections_are_read_only(self) -> None:
        connect_kwargs: list[dict[str, Any]] = []

        class FakeConninfo:
            @staticmethod
            def conninfo_to_dict(conninfo: str) -> dict[str, Any]:
                return {"dbname": "qbit"}

        class FakePsycopg:
            conninfo = FakeConninfo

            @staticmethod
            def connect(conninfo: str, **kwargs: Any) -> object:
                connect_kwargs.append(kwargs)
                return object()

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg
        client._conninfo = "postgresql://example.invalid/qbit"
        client._application_name = "qbit-prism-public-read"
        client._session_guards = make_session_guards(read_only=True)

        client._connect()

        options = connect_kwargs[0]["options"]
        self.assertIn(self.READ_ONLY_GUC, options)
        self.assertIn("-c idle_in_transaction_session_timeout=15000ms", options)

    def test_read_only_native_options_preserve_operator_conninfo_options(self) -> None:
        connect_kwargs: list[dict[str, Any]] = []
        operator_options = "-c geqo=off -c default_transaction_read_only=off"

        class FakeConninfo:
            @staticmethod
            def conninfo_to_dict(conninfo: str) -> dict[str, Any]:
                return {"dbname": "qbit", "options": operator_options}

        class FakePsycopg:
            conninfo = FakeConninfo

            @staticmethod
            def connect(conninfo: str, **kwargs: Any) -> object:
                connect_kwargs.append(kwargs)
                return object()

        client = _NativePostgresClient.__new__(_NativePostgresClient)
        client._psycopg = FakePsycopg
        client._conninfo = "postgresql://example.invalid/qbit"
        client._application_name = None
        client._session_guards = make_session_guards(read_only=True)

        client._connect()

        options = connect_kwargs[0]["options"]
        # The operator's own options survive, but libpq applies -c settings
        # left to right with the last duplicate winning, so a DSN that tries
        # to turn the session writable again cannot.
        self.assertIn("-c geqo=off", options)
        self.assertLess(
            options.index("-c default_transaction_read_only=off"),
            options.index(self.READ_ONLY_GUC),
        )
        self.assertTrue(options.endswith(self.READ_ONLY_GUC))

    def test_read_only_lease_guard_session_is_read_only(self) -> None:
        connect_calls: list[dict[str, Any]] = []

        class FakeGuardConnection:
            closed = False

            def close(self) -> None:
                pass

        fake_psycopg = types.ModuleType("psycopg")
        fake_psycopg.connect = lambda conninfo, **kwargs: (  # type: ignore[attr-defined]
            connect_calls.append(kwargs) or FakeGuardConnection()
        )
        fake_psycopg.conninfo = types.SimpleNamespace(  # type: ignore[attr-defined]
            conninfo_to_dict=lambda conninfo: {"dbname": "qbit"}
        )

        with unittest.mock.patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            _NativePostgresLeaseGuard(
                "postgresql://example.invalid/qbit",
                1234,
                session_guards=make_session_guards(read_only=True),
            )

        options = connect_calls[0]["options"]
        # A read-only ledger never builds a lease guard, but the guarantee is
        # a property of the carrier rather than of which paths happen to use
        # it, so the dedicated session carries it too.
        self.assertIn(self.READ_ONLY_GUC, options)
        self.assertIn("-c statement_timeout=500", options)

    def test_read_only_subprocess_backend_is_read_only(self) -> None:
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._command = ["psql"]
        ledger._native = None
        ledger._operation_timeout_local = threading.local()
        ledger._statement_timeout_local = threading.local()
        ledger._session_guards = make_session_guards(read_only=True)
        completed = unittest.mock.Mock(returncode=0, stdout="{}\n", stderr="")

        with unittest.mock.patch.dict(
            "os.environ",
            {"PGOPTIONS": "-c geqo=off"},
        ), unittest.mock.patch(
            "lab.prism.share_ledger.subprocess.run",
            return_value=completed,
        ) as run:
            ledger._run_sql("SELECT '{}'::json;")

        pgoptions = run.call_args.kwargs["env"]["PGOPTIONS"]
        self.assertIn(self.READ_ONLY_GUC, pgoptions)
        self.assertTrue(pgoptions.startswith("-c geqo=off"))

    def test_read_only_covers_a_write_method_that_never_takes_the_gate(self) -> None:
        # release_writer_lease_fresh_connection issues an UPDATE through a
        # one-shot psql connection, deliberately touching neither the writer
        # gate nor the pool. It is the shape #163 is about: the gate cannot
        # refuse it, so the session it opens must be the thing that does.
        ledger = PsqlShareLedger(
            psql_command="psql postgresql://example.invalid/qbit",
            native_client_mode="0",
            read_only=True,
        )
        self.addCleanup(ledger.close)
        completed = unittest.mock.Mock(returncode=0, stdout='{"released": 0}\n', stderr="")

        with unittest.mock.patch(
            "lab.prism.share_ledger.subprocess.run",
            return_value=completed,
        ) as run:
            ledger.release_writer_lease_fresh_connection()

        self.assertIn("UPDATE qbit_ledger_writer_lease", run.call_args.kwargs["input"])
        self.assertIn(
            self.READ_ONLY_GUC,
            run.call_args.kwargs["env"]["PGOPTIONS"],
        )

    def test_read_only_ledger_still_refuses_the_writer_gate(self) -> None:
        # The server-side refusal is added to the in-process gate, not
        # substituted for it.
        ledger = PsqlShareLedger(
            psql_command="psql postgresql://example.invalid/qbit",
            native_client_mode="0",
            read_only=True,
        )
        self.addCleanup(ledger.close)

        with self.assertRaises(share_ledger_module.ReadOnlyLedgerError):
            ledger.append(pending_share(0))


class BoundedLeaseAcquisitionTests(unittest.TestCase):
    class DeadlineRecordingLeaseLedger(FakeLeasePsqlShareLedger):
        def __init__(self, lease_results: list[dict[str, object]], **kwargs: Any):
            self.recorded_deadlines: list[float | None] = []
            super().__init__(lease_results, **kwargs)

        def _run_json(self, sql: str) -> Any:
            self.recorded_deadlines.append(self._remaining_operation_timeout())
            return super()._run_json(sql)

    def test_startup_lease_upsert_runs_under_bounded_deadline(self) -> None:
        ledger = self.DeadlineRecordingLeaseLedger(
            [acquired_lease()],
            writer_id="writer-a",
            writer_epoch=1,
        )

        self.assertEqual(len(ledger.recorded_deadlines), 1)
        # None here is precisely the issue #123 defect: the startup upsert
        # queueing on the lease row lock with no deadline at all.
        self.assertIsNotNone(ledger.recorded_deadlines[0])
        # DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS bounds the statement.
        self.assertGreater(ledger.recorded_deadlines[0], 0.0)
        self.assertLessEqual(ledger.recorded_deadlines[0], 5.0)

        configured = self.DeadlineRecordingLeaseLedger(
            [acquired_lease()],
            writer_id="writer-a",
            writer_epoch=1,
            lease_acquire_lock_timeout_seconds=2.5,
        )
        self.assertLessEqual(configured.recorded_deadlines[0], 2.5)

    def test_lock_blocked_acquisition_fails_visibly_after_bounded_retries(self) -> None:
        attempts: list[str] = []
        sleeps: list[float] = []

        class LockedLeaseLedger(PsqlShareLedger):
            def _run_json(self, sql: str) -> Any:
                attempts.append(sql)
                raise LedgerOperationTimeout(
                    "canceling statement due to lock timeout"
                )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            with self.assertRaises(RuntimeError) as caught:
                LockedLeaseLedger(
                    psql_command="psql postgresql://example.invalid/qbit",
                    native_client_mode="0",
                    lease_retry_sleep=sleeps.append,
                )

        # Exactly DEFAULT_LEASE_ACQUIRE_ATTEMPTS bounded attempts, with a
        # retry sleep between consecutive attempts, then a visible failure —
        # never LedgerOperationTimeout leaking out, never an unbounded wait.
        self.assertEqual(len(attempts), 5)
        self.assertEqual(len(sleeps), 4)
        self.assertIn("attempt 5/5", stdout.getvalue())
        # The diagnostic names the lock conflict as the likely cause without
        # asserting it — an unreachable or overloaded server expires the same
        # deadline — and quotes the underlying error on every line.
        message = str(caught.exception)
        self.assertIn("did not complete within its 5s deadline", message)
        self.assertIn("commonly", message)
        self.assertIn("lock-blocked", message)
        self.assertIn("canceling statement due to lock timeout", message)
        self.assertIn("lock-blocked", stdout.getvalue())
        self.assertIn("canceling statement due to lock timeout", stdout.getvalue())
        # The originating timeout is chained, not discarded: it is the only
        # record of which of the several causes actually fired.
        self.assertIsInstance(caught.exception.__cause__, LedgerOperationTimeout)

    def test_acquisition_recovers_once_the_orphaned_lock_is_reaped(self) -> None:
        outcomes: list[Any] = [
            LedgerOperationTimeout("canceling statement due to lock timeout"),
            LedgerOperationTimeout("canceling statement due to lock timeout"),
            acquired_lease(),
        ]

        class EventuallyReapedLeaseLedger(PsqlShareLedger):
            def _run_json(self, sql: str) -> Any:
                outcome = outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            ledger = EventuallyReapedLeaseLedger(
                psql_command="psql postgresql://example.invalid/qbit",
                native_client_mode="0",
                lease_retry_sleep=lambda _seconds: None,
            )

        # The retry budget outlasting idle_in_transaction_session_timeout is
        # what makes the ordinary orphaned-lock case self-heal: the server
        # reaps the orphan mid-budget and a later attempt lands the lease.
        self.assertEqual(outcomes, [])
        self.assertIn("attempt 2/5", stdout.getvalue())
        ledger.close()

    @staticmethod
    def _adoptable_predecessor_lease_results(
        *,
        old_session: str,
        new_session: str,
        updated_at: str,
    ) -> list[dict[str, object]]:
        """Two observations of a silent predecessor, then a won CAS.

        Mirrors test_postgres_startup_adopts_after_one_guard_acquisition_silence:
        the row is already silent for a full interval, so the second
        observation lands once the guard-acquisition edge has elapsed too and
        _ensure_writer_lease proceeds to the adoption CAS.
        """
        return [
            held_lease(
                session=old_session,
                updated_at=updated_at,
                age_seconds=DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
            ),
            held_lease(
                session=old_session,
                updated_at=updated_at,
                age_seconds=DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS * 2,
            ),
            acquired_lease(session=new_session) | {"adopted": True},
        ]

    def test_adoption_cas_runs_under_bounded_deadline(self) -> None:
        """The adoption CAS row-locks the lease too, so it must be bounded.

        Reached through the real _ensure_writer_lease adoption path rather
        than by calling the CAS helper directly: an unbounded
        _try_adopt_writer_lease reintroduces the issue #123 startup hang on
        exactly this branch, where the predecessor left the row locked.
        """
        updated_at = "2026-06-26 19:49:22.233718+00"
        old_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}old-session"
        new_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
        clock = FakeMonotonicClock()

        with contextlib.redirect_stdout(io.StringIO()), unittest.mock.patch.object(
            share_ledger_module.time,
            "monotonic",
            clock.monotonic,
        ):
            ledger = self.DeadlineRecordingLeaseLedger(
                self._adoptable_predecessor_lease_results(
                    old_session=old_session,
                    new_session=new_session,
                    updated_at=updated_at,
                ),
                writer_id="writer-a",
                writer_epoch=1,
                writer_session_token=new_session,
                lease_acquire_lock_timeout_seconds=2.5,
                lease_retry_sleep=clock.sleep,
            )

        self.assertEqual(len(ledger.lease_queries), 3)
        # The third query is the CAS, not another observation.
        self.assertIn("observed_writer_session_token", ledger.lease_queries[2])
        adoption_deadline = ledger.recorded_deadlines[2]
        # None here is the issue #123 defect on the adoption branch: the CAS
        # queueing on the lease row lock with no deadline at all.
        self.assertIsNotNone(adoption_deadline)
        self.assertGreater(adoption_deadline, 0.0)
        self.assertLessEqual(adoption_deadline, 2.5)

    def test_lock_blocked_adoption_fails_visibly_after_bounded_retries(self) -> None:
        updated_at = "2026-06-26 19:49:22.233718+00"
        old_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}old-session"
        new_session = f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}new-session"
        clock = FakeMonotonicClock()
        adoption_deadlines: list[float | None] = []

        class BlockedAdoptionLeaseLedger(FakeLeasePsqlShareLedger):
            def _run_json(self, sql: str) -> Any:
                if "observed_writer_session_token" not in sql:
                    return super()._run_json(sql)
                adoption_deadlines.append(self._remaining_operation_timeout())
                raise LedgerOperationTimeout(
                    "canceling statement due to lock timeout"
                )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout), unittest.mock.patch.object(
            share_ledger_module.time,
            "monotonic",
            clock.monotonic,
        ):
            with self.assertRaises(RuntimeError) as caught:
                BlockedAdoptionLeaseLedger(
                    self._adoptable_predecessor_lease_results(
                        old_session=old_session,
                        new_session=new_session,
                        updated_at=updated_at,
                    ),
                    writer_id="writer-a",
                    writer_epoch=1,
                    writer_session_token=new_session,
                    lease_retry_sleep=clock.sleep,
                )

        # Every CAS attempt ran under the bound, and the budget stopped at
        # DEFAULT_LEASE_ACQUIRE_ATTEMPTS instead of retrying forever.
        self.assertEqual(len(adoption_deadlines), 5)
        for deadline in adoption_deadlines:
            self.assertIsNotNone(deadline)
            self.assertLessEqual(deadline, 5.0)
        message = str(caught.exception)
        self.assertIn("writer lease adoption", message)
        self.assertIn("did not complete within its 5s deadline", message)
        self.assertIsInstance(caught.exception.__cause__, LedgerOperationTimeout)
        self.assertIn("adoption: attempt 5/5", stdout.getvalue())

    def test_non_timeout_lease_failure_propagates_without_retry(self) -> None:
        """Only deadline expiries are retried; everything else propagates.

        Swallowing a broader exception class into the retry loop would spend
        the whole budget on an error no retry can fix and then relabel it as
        a possible lock conflict, hiding the real fault from the operator.
        """

        class FakeIdleInTransactionSessionTimeout(Exception):
            """Shaped like psycopg.errors.IdleInTransactionSessionTimeout.

            sqlstate 25P03 is class 25 (invalid transaction state), which
            psycopg raises as an InternalError, not an OperationalError, so
            _NativePostgresClient.run_json's handler never wraps it and it
            reaches the lease helper raw. The stand-in keeps this case
            meaningful on interpreters without psycopg installed.
            """

            sqlstate = "25P03"

        errors: list[Exception] = [
            # What run_json raises for a wrapped, non-deadline OperationalError.
            RuntimeError("postgres query failed: server closed the connection"),
            FakeIdleInTransactionSessionTimeout(
                "terminating connection due to idle-in-transaction timeout"
            ),
        ]
        try:
            import psycopg
        except ImportError:
            psycopg = None  # type: ignore[assignment]
        if psycopg is not None:
            real_idle_timeout = psycopg.errors.IdleInTransactionSessionTimeout
            self.assertFalse(issubclass(real_idle_timeout, psycopg.OperationalError))
            errors.append(
                real_idle_timeout(
                    "terminating connection due to idle-in-transaction timeout"
                )
            )

        for error in errors:
            with self.subTest(error=type(error).__name__):
                attempts: list[str] = []
                sleeps: list[float] = []

                class FailingLeaseLedger(PsqlShareLedger):
                    def _run_json(self, sql: str) -> Any:
                        attempts.append(sql)
                        raise error

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    with self.assertRaises(type(error)) as caught:
                        FailingLeaseLedger(
                            psql_command="psql postgresql://example.invalid/qbit",
                            native_client_mode="0",
                            lease_retry_sleep=sleeps.append,
                        )

                # Unchanged, on the first attempt, with no retry and none of
                # the deadline wording.
                self.assertIs(caught.exception, error)
                self.assertEqual(len(attempts), 1)
                self.assertEqual(sleeps, [])
                self.assertEqual(stdout.getvalue(), "")

    def test_init_rejects_disarmed_session_guard_and_lease_bounds(self) -> None:
        cases = (
            {"lease_acquire_lock_timeout_seconds": 0},
            {"lease_acquire_lock_timeout_seconds": -1},
            {"lease_acquire_attempts": 0},
            {"postgres_idle_in_transaction_timeout_seconds": 0},
            {"postgres_tcp_keepalives_idle_seconds": 0},
            {"postgres_tcp_keepalives_interval_seconds": -1},
            {"postgres_tcp_keepalives_count": 0},
        )
        for overrides in cases:
            with self.subTest(**overrides), self.assertRaises(ValueError):
                FakeLeasePsqlShareLedger([acquired_lease()], **overrides)


class AcceptedStatsCacheTests(unittest.TestCase):
    def test_stats_cached_within_ttl_and_incremented_by_appends(self) -> None:
        ledger = CannedQueryPsqlShareLedger(
            [
                acquired_lease_result(),
                stats_payload(10, 2, 10),
                record_payload(3, share_seq=11, new_miner=True),
                {"records": [record_payload(6, share_seq=12)]},
                {
                    "records": [
                        {**record_payload(6, share_seq=12), "newly_inserted": False}
                    ]
                },
            ],
            accepted_stats_cache_seconds=60.0,
        )
        first = ledger.accepted_share_stats()
        self.assertEqual(
            first,
            {"accepted_share_count": 10, "distinct_miner_count": 2},
        )
        queries_after_seed = len(ledger.queries)

        # Repeated reads inside the TTL never touch the database.
        for _ in range(5):
            self.assertEqual(ledger.accepted_share_stats(), first)
        self.assertEqual(len(ledger.queries), queries_after_seed)

        # A committed append advances the counters without a query;
        # pending_share(3) mines as miner-0, a brand new miner id.
        ledger.append(pending_share(3))
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 11, "distinct_miner_count": 3},
        )
        # The writer-thread batch path advances the counters the same way;
        # pending_share(6) is miner-0 again so only the share count moves.
        ledger.append_batch([(pending_share(6), None)])
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 12, "distinct_miner_count": 3},
        )
        # Replaying the same committed batch returns its existing record but
        # does not increment the cache again.
        ledger.append_batch([(pending_share(6), None)])
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 12, "distinct_miner_count": 3},
        )

    def test_refresh_does_not_hold_writer_lock_during_aggregate(self) -> None:
        aggregate_started = threading.Event()
        aggregate_can_return = threading.Event()

        class SlowAggregateLedger(CannedQueryPsqlShareLedger):
            def __init__(self) -> None:
                super().__init__(
                    [acquired_lease_result()],
                    accepted_stats_cache_seconds=60.0,
                )

            def _run_json(self, sql: str) -> Any:
                self.queries.append(sql)
                if "'max_share_seq'" in sql:
                    aggregate_started.set()
                    if not aggregate_can_return.wait(5):
                        raise AssertionError("aggregate query was not released")
                    return stats_payload(10, 2, 10)
                if "INSERT INTO qbit_share_ledger" in sql:
                    return record_payload(3, share_seq=11, new_miner=True)
                return super()._run_json(sql)

        ledger = SlowAggregateLedger()
        stats: list[dict[str, int]] = []
        errors: list[BaseException] = []

        def refresh_stats() -> None:
            try:
                stats.append(ledger.accepted_share_stats())
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        stats_thread = threading.Thread(target=refresh_stats, name="stats-refresh")
        stats_thread.start()
        try:
            self.assertTrue(aggregate_started.wait(5))
            # The full-history aggregate is still blocked, but the serialized
            # writer must be able to commit and publish its note immediately.
            appended = ledger.append(pending_share(3))
            self.assertEqual(appended.share_seq, 11)
        finally:
            aggregate_can_return.set()
            stats_thread.join(5)

        self.assertFalse(stats_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(
            stats,
            [{"accepted_share_count": 11, "distinct_miner_count": 3}],
        )

    def test_refresh_does_not_replay_note_already_in_aggregate_snapshot(self) -> None:
        append_committed = threading.Event()
        append_can_publish_note = threading.Event()
        aggregate_started = threading.Event()
        aggregate_can_return = threading.Event()

        class CommitBeforeNoteLedger(CannedQueryPsqlShareLedger):
            def __init__(self) -> None:
                super().__init__(
                    [acquired_lease_result()],
                    accepted_stats_cache_seconds=60.0,
                )

            def _run_json(self, sql: str) -> Any:
                self.queries.append(sql)
                if "'max_share_seq'" in sql:
                    aggregate_started.set()
                    if not aggregate_can_return.wait(5):
                        raise AssertionError("aggregate query was not released")
                    return stats_payload(11, 2, 11)
                if "INSERT INTO qbit_share_ledger" in sql:
                    return record_payload(1, share_seq=11, new_miner=True)
                return super()._run_json(sql)

            def _record_from_json(self, payload: dict[str, Any]) -> Any:
                record = PsqlShareLedger._record_from_json(payload)
                if record.share_id == "share-1":
                    append_committed.set()
                    if not append_can_publish_note.wait(5):
                        raise AssertionError("append note was not released")
                return record

        ledger = CommitBeforeNoteLedger()
        appended: list[object] = []
        stats: list[dict[str, int]] = []
        errors: list[BaseException] = []

        def append_share() -> None:
            try:
                appended.append(ledger.append(pending_share(1)))
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        def refresh_stats() -> None:
            try:
                stats.append(ledger.accepted_share_stats())
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        append_thread = threading.Thread(target=append_share, name="share-append")
        stats_thread = threading.Thread(target=refresh_stats, name="stats-refresh")
        append_thread.start()
        try:
            self.assertTrue(append_committed.wait(5))
            stats_thread.start()
            self.assertTrue(aggregate_started.wait(5))
            # Publish a snapshot that already contains the committed share,
            # then release its delayed cache note. The published watermark
            # must suppress that late note rather than count share 11 twice.
            aggregate_can_return.set()
            stats_thread.join(5)
            self.assertFalse(stats_thread.is_alive())
        finally:
            append_can_publish_note.set()
            aggregate_can_return.set()
            append_thread.join(5)
            if stats_thread.ident is not None:
                stats_thread.join(5)

        self.assertFalse(append_thread.is_alive())
        self.assertFalse(stats_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(appended), 1)
        self.assertEqual(
            stats,
            [{"accepted_share_count": 11, "distinct_miner_count": 2}],
        )
        self.assertEqual(ledger.accepted_share_stats(), stats[0])

    def test_stats_requery_after_ttl_reconciles(self) -> None:
        ledger = CannedQueryPsqlShareLedger(
            [
                acquired_lease_result(),
                stats_payload(10, 1, 10),
                stats_payload(4, 2, 12),
            ],
            accepted_stats_cache_seconds=0.05,
        )
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 10, "distinct_miner_count": 1},
        )
        time.sleep(0.06)
        # The expired read stays non-blocking: it serves the maintained
        # counters and arms the background reconcile that publishes the
        # out-of-band correction.
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 10, "distinct_miner_count": 1},
        )
        reconcile_thread = ledger._stats_background_refresh_thread
        self.assertIsNotNone(reconcile_thread)
        reconcile_thread.join(5)
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 4, "distinct_miner_count": 2},
        )

    def test_stats_query_disabled_cache_runs_every_time(self) -> None:
        ledger = CannedQueryPsqlShareLedger(
            [
                acquired_lease_result(),
                stats_payload(1, 1, 1),
                stats_payload(2, 1, 2),
            ],
            accepted_stats_cache_seconds=0.0,
        )
        self.assertEqual(ledger.accepted_share_stats()["accepted_share_count"], 1)
        self.assertEqual(ledger.accepted_share_stats()["accepted_share_count"], 2)

    def test_metrics_reports_shares_without_share_ledger_scan(self) -> None:
        metrics_payload = {
            "blocks": 2,
            "confirmed_blocks": 1,
            "inactive_blocks": 0,
            "rejected_blocks": 0,
            "reversed_blocks": 1,
            "payout_entries": 5,
            "owed_accounts": 3,
            "ctv_fanouts_failed": 0,
        }
        ledger = CannedQueryPsqlShareLedger(
            [
                acquired_lease_result(),
                stats_payload(7, 1, 7),
                metrics_payload,
            ],
            accepted_stats_cache_seconds=60.0,
        )
        metrics = ledger.metrics()
        self.assertEqual(metrics["shares"], 7)
        self.assertEqual(metrics["blocks"], 2)
        metrics_sql = ledger.queries[-1]
        self.assertNotIn("qbit_share_ledger", metrics_sql)
        stats_sql = ledger.queries[-2]
        self.assertIn("count(DISTINCT miner_id)", stats_sql)
        self.assertNotIn("json_agg(DISTINCT miner_id)", stats_sql)


class AcceptedStatsBackgroundReconcileTests(unittest.TestCase):
    def _expire_stats(self, ledger: CannedQueryPsqlShareLedger) -> None:
        with ledger._stats_lock:
            ledger._stats_refreshed_monotonic = (
                time.monotonic() - ledger._accepted_stats_cache_seconds - 1.0
            )

    def test_expired_read_serves_counters_and_reconciles_in_background(self) -> None:
        aggregate_started = threading.Event()
        aggregate_can_return = threading.Event()

        class SlowReconcileLedger(CannedQueryPsqlShareLedger):
            block_aggregate = False

            def _run_json(self, sql: str) -> Any:
                if self.block_aggregate and "'max_share_seq'" in sql:
                    self.queries.append(sql)
                    aggregate_started.set()
                    if not aggregate_can_return.wait(5):
                        raise AssertionError("reconcile aggregate was not released")
                    return stats_payload(40, 4, 12)
                return super()._run_json(sql)

        ledger = SlowReconcileLedger(
            [
                acquired_lease_result(),
                stats_payload(10, 2, 10),
                record_payload(3, share_seq=11, new_miner=True),
                record_payload(4, share_seq=12),
                record_payload(5, share_seq=13),
            ],
            accepted_stats_cache_seconds=60.0,
        )
        seeded = ledger.accepted_share_stats()
        self.assertEqual(
            seeded, {"accepted_share_count": 10, "distinct_miner_count": 2}
        )
        self.assertIsNone(ledger._stats_background_refresh_thread)

        self._expire_stats(ledger)
        ledger.block_aggregate = True

        # The expired read returns a private copy of the maintained
        # counters without waiting for the aggregate it armed.
        stale = ledger.accepted_share_stats()
        self.assertEqual(stale, seeded)
        stale["accepted_share_count"] = -1
        self.assertEqual(
            ledger.accepted_share_stats()["accepted_share_count"], 10
        )
        self.assertTrue(aggregate_started.wait(5))
        reconcile_thread = ledger._stats_background_refresh_thread
        self.assertIsNotNone(reconcile_thread)
        self.assertTrue(reconcile_thread.daemon)
        try:
            # While the aggregate is blocked, the serialized writer commits
            # and readers observe its notes immediately -- and repeated
            # expired reads reuse the single in-flight reconcile. The three
            # appends straddle the reconcile snapshot's watermark of 12:
            # sequences 11 and 12 are inside the snapshot, 13 is newer.
            ledger.append(pending_share(3))
            ledger.append(pending_share(4))
            ledger.append(pending_share(5))
            self.assertEqual(
                ledger.accepted_share_stats(),
                {"accepted_share_count": 13, "distinct_miner_count": 3},
            )
            self.assertIs(ledger._stats_background_refresh_thread, reconcile_thread)
        finally:
            aggregate_can_return.set()
            reconcile_thread.join(5)
        self.assertFalse(reconcile_thread.is_alive())

        # The reconcile publishes aggregate plus replayed notes: sequences
        # 11 and 12 sit at or below the snapshot watermark (12 exactly at
        # the boundary guards the <= comparison) and are suppressed, while
        # sequence 13 is replayed on top of the aggregate.
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 41, "distinct_miner_count": 4},
        )
        aggregate_queries = [
            sql for sql in ledger.queries if "'max_share_seq'" in sql
        ]
        self.assertEqual(len(aggregate_queries), 2)

    def test_failed_background_reconcile_keeps_serving_and_rearms(self) -> None:
        class FlakyReconcileLedger(CannedQueryPsqlShareLedger):
            fail_aggregate = False

            def _run_json(self, sql: str) -> Any:
                if self.fail_aggregate and "'max_share_seq'" in sql:
                    self.queries.append(sql)
                    raise RuntimeError("reconcile aggregate failed")
                return super()._run_json(sql)

        ledger = FlakyReconcileLedger(
            [
                acquired_lease_result(),
                stats_payload(10, 2, 10),
                stats_payload(20, 3, 15),
            ],
            accepted_stats_cache_seconds=60.0,
        )
        seeded = ledger.accepted_share_stats()
        self.assertEqual(
            seeded, {"accepted_share_count": 10, "distinct_miner_count": 2}
        )
        status = ledger.accepted_stats_reconcile_status()
        self.assertEqual(status["failures"], 0)
        self.assertIsNotNone(status["age_seconds"])
        self.assertLess(status["age_seconds"], 5.0)

        self._expire_stats(ledger)
        # Reconcile age keeps growing while no pass publishes, so alerting
        # can see a stale (or wedged) reconcile that callers no longer feel.
        self.assertGreater(
            ledger.accepted_stats_reconcile_status()["age_seconds"], 60.0
        )
        ledger.fail_aggregate = True
        with unittest.mock.patch(
            "lab.prism.share_ledger.traceback.print_exc"
        ) as print_exc:
            self.assertEqual(ledger.accepted_share_stats(), seeded)
            failed_thread = ledger._stats_background_refresh_thread
            self.assertIsNotNone(failed_thread)
            failed_thread.join(5)
        self.assertFalse(failed_thread.is_alive())
        print_exc.assert_called_once()
        self.assertEqual(
            ledger.accepted_stats_reconcile_status()["failures"], 1
        )

        # The failure left the counters stale, so the next read arms a new
        # reconcile; a successful pass then publishes the fresh aggregate.
        ledger.fail_aggregate = False
        self.assertEqual(ledger.accepted_share_stats(), seeded)
        retry_thread = ledger._stats_background_refresh_thread
        self.assertIsNot(retry_thread, failed_thread)
        retry_thread.join(5)
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 20, "distinct_miner_count": 3},
        )
        final_status = ledger.accepted_stats_reconcile_status()
        self.assertEqual(final_status["failures"], 1)
        self.assertLess(final_status["age_seconds"], 5.0)

    def test_cold_seed_still_runs_exact_aggregate_synchronously(self) -> None:
        ledger = CannedQueryPsqlShareLedger(
            [acquired_lease_result(), stats_payload(5, 1, 4)],
            accepted_stats_cache_seconds=60.0,
        )
        self.assertIsNone(
            ledger.accepted_stats_reconcile_status()["age_seconds"]
        )
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 5, "distinct_miner_count": 1},
        )
        self.assertIsNone(ledger._stats_background_refresh_thread)


if __name__ == "__main__":
    unittest.main()
