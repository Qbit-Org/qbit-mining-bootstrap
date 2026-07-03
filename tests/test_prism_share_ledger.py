#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import threading
import time
import unittest

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from lab.prism.backfill_ctv_fanouts import backfill_input_from_payload, infer_block_hash_from_path
from lab.prism import public_api
from lab.prism.share_ledger import (
    PendingShare,
    PsqlShareLedger,
    SingleWriterShareLedger,
    _prism_window_shares,
    sha256_json_hex,
)


def pending_share(
    index: int,
    *,
    share_difficulty: int | None = None,
    job_issued_at_ms: int | None = None,
    accepted_at_ms: int | None = None,
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
) -> dict[str, object]:
    return {
        "acquired": False,
        "writer_id": writer_id,
        "writer_epoch": writer_epoch,
        "writer_session_token": session,
        "lease_expires_at": "2026-06-26 19:50:22.233718+00",
        "lease_wait_seconds": wait_seconds,
    }


class FakeLeasePsqlShareLedger(PsqlShareLedger):
    def __init__(self, lease_results: list[dict[str, object]], **kwargs: Any):
        self.lease_results = list(lease_results)
        self.lease_queries: list[str] = []
        self.sleeps: list[float] = []
        super().__init__(
            psql_command="psql postgresql://example.invalid/qbit",
            lease_retry_sleep=self.sleeps.append,
            lease_retry_max_sleep_seconds=1.0,
            **kwargs,
        )

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
    def test_single_writer_assigns_contiguous_sequence_numbers(self) -> None:
        ledger = SingleWriterShareLedger()

        records = [ledger.append(pending_share(index)) for index in range(1, 6)]

        self.assertEqual([record.share_seq for record in records], [1, 2, 3, 4, 5])
        self.assertEqual(len(ledger), 5)
        self.assertEqual([record["share_seq"] for record in (item.to_prism_json() for item in records)], [1, 2, 3, 4, 5])

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

    def test_job_issue_snapshot_excludes_old_job_shares_accepted_after_anchor(self) -> None:
        ledger = SingleWriterShareLedger()
        ledger.append(pending_share(1, job_issued_at_ms=1_000, accepted_at_ms=1_001))
        ledger.append(pending_share(2, job_issued_at_ms=1_000, accepted_at_ms=1_006))

        self.assertEqual(
            [share.share_id for share in ledger.snapshot_at_job_issue(1_005)],
            ["share-1"],
        )

    def test_rejects_zero_difficulty_share(self) -> None:
        ledger = SingleWriterShareLedger()
        share = pending_share(1)

        with self.assertRaisesRegex(ValueError, "share_difficulty"):
            ledger.append(share.__class__(**{**share.__dict__, "share_difficulty": 0}))
        with self.assertRaisesRegex(ValueError, "network_difficulty"):
            ledger.append(share.__class__(**{**share.__dict__, "network_difficulty": 0}))

    def test_rejects_duplicate_share_id_without_consuming_sequence(self) -> None:
        ledger = SingleWriterShareLedger()
        first = pending_share(1)
        duplicate = pending_share(2).__class__(**{**pending_share(2).__dict__, "share_id": first.share_id})

        self.assertEqual(ledger.append(first).share_seq, 1)
        with self.assertRaisesRegex(ValueError, "duplicate share_id"):
            ledger.append(duplicate)

        self.assertEqual(ledger.append(pending_share(3)).share_seq, 2)

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

    def test_reorg_watch_blocks_keeps_height_mature_immature_rows(self) -> None:
        ledger = QueryCapturePsqlShareLedger()

        self.assertEqual(ledger.reorg_watch_blocks(active_tip_height=10_000), [])

        self.assertIn("chain_state IN ('confirmed', 'inactive')", ledger.queries[0])
        self.assertIn("maturity_state = 'immature'", ledger.queries[0])
        self.assertNotIn("block_height + 1000", ledger.queries[0])

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
        self.assertEqual(status["broadcast_attempts"][0]["attempt_status"], "rejected")  # type: ignore[index]
        self.assertEqual(status["broadcast_attempts"][0]["package_tx_hexes"], ["parent", "child"])  # type: ignore[index]
        self.assertEqual(pending[0]["fanout_txid"], fanout_txid)

        ledger.update_ctv_fanout_status(fanout_txid=fanout_txid, settlement_status="confirmed")
        self.assertEqual(ledger.pending_ctv_fanout_statuses(), [])

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

    def test_writer_lease_ttl_is_configurable_in_acquire_sql(self) -> None:
        ledger = FakeLeasePsqlShareLedger([acquired_lease()], lease_ttl_seconds=42)

        self.assertEqual(ledger._lease_interval_sql, "make_interval(secs => 42.0)")
        self.assertIn("make_interval(secs => 42.0)", ledger.lease_queries[0])
        self.assertNotIn("interval '5 minutes'", ledger.lease_queries[0])

    def test_writer_lease_ttl_defaults_to_sixty_seconds(self) -> None:
        ledger = FakeLeasePsqlShareLedger([acquired_lease()])

        self.assertEqual(ledger._lease_interval_sql, "make_interval(secs => 60.0)")

    def test_writer_lease_ttl_must_be_finite_positive(self) -> None:
        for value in (0, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "lease_ttl_seconds"):
                PsqlShareLedger(psql_command="psql postgresql://example.invalid/qbit", lease_ttl_seconds=value)

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
                ledger = FakeLeasePsqlShareLedger(
                    [acquired_lease(), {"backend": "postgres-psql", **result}],
                    lease_ttl_seconds=42,
                )

                payload = getattr(ledger, method_name)(block_hash="aa" * 32, active_tip_height=10)
                query = ledger.lease_queries[-1]

                self.assertEqual(payload[count_key], 1)
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

        self.assertIn("WITH RECURSIVE eligible AS", schema)
        self.assertIn("AND ledger.share_seq < eligible.share_seq", schema)
        self.assertIn("ON qbit_share_ledger ((lower(right(share_id, 64))), accepted_at DESC, share_seq DESC)", schema)
        self.assertIn("ALTER COLUMN anchor_vout DROP NOT NULL", schema)
        self.assertNotIn("sum(ledger.share_difficulty) OVER", schema)

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
        self.assertIn("qbit_ctv_fanout_broadcast_attempts", query)

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
                {"pool_counted_difficulty": "4", "miner_counted_difficulty": "1"},
            ]
        )

        payload = ledger.dashboard_miner_reward_window(recipient_id="miner-a", current_network_difficulty="1.2")
        query = ledger.lease_queries[-1]

        self.assertEqual(payload["accepted_difficulty"], "1")
        self.assertEqual(payload["pool_accepted_difficulty"], "4")
        self.assertEqual(payload["share_percent"], "25")
        self.assertIn("qbit_prism_window(bounds.ended_at, 9.6::numeric)", query)
        self.assertIn("FILTER (WHERE miner_id = 'miner-a')", query)

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
                ]
            )

            self.assertEqual(ledger.dashboard_public_artifact(sha256=body_sha), bundle)
            query = ledger.lease_queries[-1]
            self.assertIn("SELECT audit_bundle, audit_bundle_sha256, body_uri", query)

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
                ]
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
                ]
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

    def test_psql_persist_requires_lease_preflight_before_external_body_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = FakeLeasePsqlShareLedger(
                [acquired_lease(), {"error": "writer lease is not active"}],
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
        self.assertIn("'body_uri', body_uri", by_hash_query)

        ledger.audit_bundle_by_commitment(commitment_leaf_hex="ab" * 32)
        by_commitment_query = ledger.lease_queries[-1]
        self.assertIn("'body_uri', bundle.body_uri", by_commitment_query)
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

        self.assertEqual(ledger.sleeps, [1.0, 0.25])
        self.assertEqual(len(ledger.lease_queries), 3)
        self.assertIn(
            "prism ledger writer lease held until 2026-06-26 19:50:22.233718+00; waiting 1s before retry",
            stdout.getvalue(),
        )
        self.assertIn("holder writer=writer-a epoch=1 session=old-session", stdout.getvalue())

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


if __name__ == "__main__":
    unittest.main()
