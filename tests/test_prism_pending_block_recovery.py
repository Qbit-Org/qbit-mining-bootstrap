"""Focused recovery safety tests; native cases run in the PostgreSQL gate."""

import hashlib
import json
import os
import tempfile
import threading
import unittest
import uuid
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab.auxpow.stratum_codec import header_hash_hex
from lab.prism import direct_stratum
from lab.prism import recover_pending_blocks as recovery
from lab.prism.block_candidates import block_candidate_from_intent
from lab.prism.coordinator_config import load_coordinator_config
from lab.prism.recovery_json import cooperative_json
from lab.prism.share_ledger import PsqlShareLedger, block_candidate_identity_sha256
from tests.prism_coordinator_test_support import durable_candidate_row


def fixture(index=0, parent="11" * 32, height=10):
    row = durable_candidate_row(index)
    intent = row["candidate"]
    header = bytes([index + 1]) * 80
    block_hash = header_hash_hex(header)
    intent.update(
        block_hex=header.hex(),
        block_hash_hex=block_hash,
        parent_hash=parent,
        expected_height=height,
    )
    intent["template"].update(previousblockhash=parent, height=height)
    row.update(block_hash=block_hash, candidate_sha256=block_candidate_identity_sha256(intent))
    block = recovery.RecoveryBlock(block_hash, height, parent, "pending")
    return block, row


def metadata(block, state="pending"):
    return dict(
        block_hash=block.block_hash,
        state=state,
        block_height=block.height if state == "submitted" else None,
        parent_hash=block.parent_hash if state == "submitted" else None,
        chain_state="confirmed" if state == "submitted" else None,
        has_audit=state == "submitted",
    )


def fake_rpc(blocks):
    def call(method, params):
        if method == "getblockheader":
            block = next(block for block in blocks if block.block_hash == params[0])
            return dict(height=block.height, previousblockhash=block.parent_hash, confirmations=1)
        if method == "getblockhash":
            return next(block.block_hash for block in blocks if block.height == params[0])
        raise AssertionError(f"unexpected RPC {method}")

    return SimpleNamespace(call=Mock(side_effect=call))


def test_config(root, database_url="postgresql://qbit:qbit@localhost/qbit"):
    return load_coordinator_config(
        {
            "QBIT_RPC_HOST": "localhost",
            "QBIT_RPC_USER": "test",
            "QBIT_RPC_PASSWORD": "test",
            "PRISM_DATABASE_URL": database_url,
            "PRISM_ALLOW_TEST_SIGNING_SEEDS": "1",
            "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1",
            "PRISM_AUDIT_DIR": str(root),
            "PRISM_EVIDENCE_PATH": str(root / "evidence.json"),
        }
    )


class RecoveryTests(unittest.TestCase):
    def test_plan_orders_parent_before_child_without_loading_payloads(self):
        parent, _ = fixture()
        child, _ = fixture(1, parent.block_hash, 11)
        rows = {block.block_hash: metadata(block) for block in [parent, child]}
        reader = Mock(metadata=lambda value: rows.get(value))
        planned = recovery.plan_recovery(
            reader, fake_rpc([parent, child]), [child.block_hash, parent.block_hash]
        )
        self.assertEqual(planned, [parent, child])
        reader.candidate.assert_not_called()

    def test_missing_pending_parent_rejects_plan(self):
        parent, _ = fixture()
        child, _ = fixture(1, parent.block_hash, 11)
        rows = {block.block_hash: metadata(block) for block in [parent, child]}
        with self.assertRaisesRegex(recovery.RecoveryError, "pending parent"):
            recovery.plan_recovery(
                Mock(metadata=lambda value: rows.get(value)),
                fake_rpc([parent, child]),
                [child.block_hash],
            )

    def test_duplicate_missing_abandoned_and_inactive_fail_closed(self):
        block, _ = fixture()
        for hashes, row in [
            ([], None),
            ([block.block_hash] * 2, metadata(block)),
            ([block.block_hash], None),
            ([block.block_hash], metadata(block, "abandoned")),
        ]:
            with self.subTest(hashes=hashes, row=row), self.assertRaises(recovery.RecoveryError):
                recovery.plan_recovery(Mock(metadata=lambda _: row), fake_rpc([block]), hashes)
        rpc = fake_rpc([block])
        rpc.call = Mock(
            side_effect=[dict(height=10, previousblockhash=block.parent_hash), "22" * 32]
        )
        with self.assertRaisesRegex(recovery.RecoveryError, "active chain"):
            recovery.require_active_block(rpc, block.block_hash)

    def test_submitted_without_complete_accounting_is_not_skipped(self):
        block, _ = fixture()
        row = metadata(block, "submitted")
        row["has_audit"] = False
        with self.assertRaisesRegex(recovery.RecoveryError, "completion"):
            recovery.plan_recovery(
                Mock(metadata=lambda _: row), fake_rpc([block]), [block.block_hash]
            )

    def test_payload_identity_height_and_header_are_verified(self):
        block, row = fixture()
        coordinator = SimpleNamespace(block_candidate_from_intent=block_candidate_from_intent)
        candidate = recovery.decode_candidate(coordinator, block, row)
        self.assertTrue(candidate.durable_replay)
        for field, value in [
            ("shares_json", [{"changed": True}]),
            ("expected_height", 12),
            ("block_hex", "00" * 80),
        ]:
            modified = json.loads(json.dumps(row))
            modified["candidate"][field] = value
            if field != "shares_json":
                modified["candidate_sha256"] = block_candidate_identity_sha256(
                    modified["candidate"]
                )
            with self.subTest(field=field), self.assertRaises(recovery.RecoveryError):
                recovery.decode_candidate(coordinator, block, modified)

    def test_apply_finalizes_only_after_normal_accounting_and_checks_durable_result(self):
        block, payload = fixture()
        events = []
        row = metadata(block)
        reader = Mock(metadata=lambda _: row, candidate=lambda _: payload)

        def submit(_candidate, *, node_submission):
            self.assertFalse(node_submission.attempted)
            events.append("accounting")
            return True

        def finalize(*_args, **kwargs):
            self.assertTrue(kwargs["accepted"])
            events.append("outbox")
            row.update(metadata(block, "submitted"))

        coordinator = Mock(
            rpc=fake_rpc([block]),
            stop_event=threading.Event(),
            block_candidate_from_intent=block_candidate_from_intent,
            _writer_operation=lambda _: nullcontext(),
            _block_landing_ledger_statement_timeout_scope=lambda _: nullcontext(),
            submit_block_candidate=submit,
            _finalize_block_candidate=finalize,
        )
        recovery.recover_block(coordinator, reader, block)
        self.assertEqual(events, ["accounting", "outbox"])
        recovery.recover_block(coordinator, reader, block)
        self.assertEqual(events, ["accounting", "outbox"])
        coordinator.serve.assert_not_called()

    def test_accounting_failure_and_lost_lease_leave_outbox_pending(self):
        block, payload = fixture()
        for failure in ("accounting", "lease", "terminal_update"):
            reader = Mock(metadata=lambda _: metadata(block), candidate=lambda _: payload)
            coordinator = Mock(
                rpc=fake_rpc([block]),
                stop_event=threading.Event(),
                block_candidate_from_intent=block_candidate_from_intent,
                _writer_operation=lambda _: nullcontext(),
                _block_landing_ledger_statement_timeout_scope=lambda _: nullcontext(),
            )
            coordinator.submit_block_candidate.return_value = failure != "accounting"
            if failure == "lease":
                coordinator._require_fresh_ledger_lease_for_external_side_effect.side_effect = (
                    recovery.RecoveryError("lease lost")
                )
            with self.subTest(failure=failure), self.assertRaises(recovery.RecoveryError):
                recovery.recover_block(coordinator, reader, block)
            if failure != "terminal_update":
                coordinator._finalize_block_candidate.assert_not_called()

    def test_recovery_config_preserves_lease_policy_and_signing_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            before = test_config(Path(directory))
        after = recovery.recovery_config(before)
        self.assertEqual(after.lifecycle, before.lifecycle)
        self.assertEqual(after.ledger.writer_id, before.ledger.writer_id)
        self.assertEqual(after.ledger.writer_epoch, before.ledger.writer_epoch)
        self.assertEqual(after.ledger.signing_seed_hex, before.ledger.signing_seed_hex)
        self.assertFalse(after.ledger.initialize_schema)
        self.assertEqual(after.ledger.native_client_mode, "on")
        self.assertIsNone(after.ledger.writer_session_token)

    def test_runner_never_offers_new_blocks(self):
        with self.assertRaisesRegex(recovery.RecoveryError, "refuses to submit"):
            recovery.RecoveryCoordinator.__new__(
                recovery.RecoveryCoordinator
            )._submit_block_candidate_to_node(None)


class CooperativeJsonTests(unittest.TestCase):
    def test_canonical_bytes_identity_and_parser_behavior_match_stdlib(self):
        values = [None, True, -(2**90), 1.25, '\u03bb\U0001f680\n\\"', {"z": [1, None], "a": "x"}]
        expected = [json.dumps(value, sort_keys=True, separators=(",", ":")) for value in values]
        _, payload = fixture()
        digest = block_candidate_identity_sha256(payload["candidate"])
        original = json._default_decoder
        with cooperative_json():
            for value, text in zip(values, expected):
                self.assertEqual(json.dumps(value, sort_keys=True, separators=(",", ":")), text)
                self.assertEqual(json.loads(text), value)
                self.assertEqual(json.loads(text.encode()), value)
            self.assertEqual(block_candidate_identity_sha256(payload["candidate"]), digest)
            for malformed in ('{"x":', "[1,]", '"\\q"', "{} garbage"):
                with self.assertRaises(json.JSONDecodeError):
                    json.loads(malformed)
            self.assertEqual(json.loads('{"x":1,"x":2}'), {"x": 2})
        self.assertIs(json._default_decoder, original)

    def test_codec_restores_after_error(self):
        original = json.scanner.make_scanner
        with self.assertRaises(RuntimeError), cooperative_json():
            raise RuntimeError("test")
        self.assertIs(json.scanner.make_scanner, original)


@unittest.skipUnless(
    os.environ.get("PRISM_RECOVERY_TEST_DATABASE_URL"), "requires the native PostgreSQL test gate"
)
class NativeRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import make_conninfo

        cls.schema = "recovery_" + uuid.uuid4().hex
        cls.admin = psycopg.connect(os.environ["PRISM_RECOVERY_TEST_DATABASE_URL"], autocommit=True)
        cls.admin.execute(f'CREATE SCHEMA "{cls.schema}"')
        cls.url = make_conninfo(
            os.environ["PRISM_RECOVERY_TEST_DATABASE_URL"],
            options=f"-csearch_path={cls.schema},public",
        )
        with psycopg.connect(cls.url, autocommit=True) as connection:
            connection.execute(Path("crates/qbit-prism/sql/001_share_ledger.sql").read_text())

    @classmethod
    def tearDownClass(cls):
        cls.admin.execute(f'DROP SCHEMA "{cls.schema}" CASCADE')
        cls.admin.close()

    def ledger(self, **kwargs):
        ledger = PsqlShareLedger(
            psql_command="psql", database_url=self.url, native_client_mode="on", **kwargs
        )
        self.addCleanup(ledger.close)
        return ledger

    def test_exclusive_guard_refuses_live_writer_and_reacquires_after_release(self):
        owner = self.ledger(writer_session_token="heartbeat-v1:" + uuid.uuid4().hex)
        token = owner._writer_session_token
        with self.assertRaisesRegex(recovery.RecoveryError, "exclusive writer lease"):
            self.ledger(
                writer_session_token="heartbeat-v1:" + uuid.uuid4().hex,
                lease_retry_sleep=recovery.refuse_lease_wait,
            )
        self.assertEqual(owner._writer_session_token, token)
        self.assertTrue(owner.release_writer_lease())
        owner.close()
        successor = self.ledger(
            writer_session_token="heartbeat-v1:" + uuid.uuid4().hex,
            lease_retry_sleep=recovery.refuse_lease_wait,
        )
        self.assertTrue(successor.release_writer_lease())

    def test_reader_is_read_only_and_loads_only_selected_candidate_with_cooperative_codec(self):
        import psycopg
        from psycopg.types.json import Jsonb

        block, payload = fixture()
        other, other_payload = fixture(1)
        with psycopg.connect(self.url, autocommit=True) as connection:
            for target, row in [(block, payload), (other, other_payload)]:
                connection.execute(
                    "INSERT INTO qbit_block_candidate_outbox (block_hash, candidate, candidate_sha256) VALUES (%s,%s,%s)",
                    (target.block_hash, Jsonb(row["candidate"]), row["candidate_sha256"]),
                )
        with cooperative_json():
            reader = recovery.RecoveryReader(self.url)
            try:
                self.assertEqual(reader.metadata(block.block_hash)["state"], "pending")
                self.assertNotIn("candidate", reader.metadata(block.block_hash))
                self.assertEqual(
                    reader.candidate(block.block_hash)["candidate"], payload["candidate"]
                )
                with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
                    reader.connection.execute(
                        "UPDATE qbit_block_candidate_outbox SET attempt_count=1"
                    )
            finally:
                reader.close()

    def test_real_finalizer_persists_confirms_publishes_and_completes_outbox(self):
        # Real coordinator, PostgreSQL, lease heartbeat, finalizer and audit
        # publication. Only the node and cryptographic builder/verifier use
        # deterministic fixtures; their own suites cover consensus proofs.
        import psycopg
        from psycopg.types.json import Jsonb

        from tests.prism_vardiff_test_support import (
            verified_audit_report,
            verified_block_bundle,
        )

        parent, parent_payload = fixture(3, height=20)
        child, child_payload = fixture(4, parent.block_hash, 21)
        with psycopg.connect(self.url, autocommit=True) as connection:
            for block, payload in [(parent, parent_payload), (child, child_payload)]:
                payload["candidate"]["prospective_prior_balances"] = []
                payload["candidate_sha256"] = block_candidate_identity_sha256(payload["candidate"])
                connection.execute(
                    "INSERT INTO qbit_block_candidate_outbox (block_hash,candidate,candidate_sha256) VALUES (%s,%s,%s)",
                    (block.block_hash, Jsonb(payload["candidate"]), payload["candidate_sha256"]),
                )
        rpc = fake_rpc([parent, child])
        chain_call = rpc.call.side_effect

        def call(method, params=None, **_kwargs):
            if method == "getbestblockhash":
                return child.block_hash
            if method == "getblockcount":
                return child.height
            if method == "getblockchaininfo":
                return dict(
                    chain="regtest",
                    blocks=child.height,
                    headers=child.height,
                    initialblockdownload=False,
                )
            if method == "getblockhash" and params == [parent.height - 1]:
                return parent.parent_hash
            return chain_call(method, params)

        rpc.call.side_effect = call
        bundle = verified_block_bundle("00")

        def canonical(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

        audit_report = verified_audit_report("00")
        audit_report.update(audit_bundle_sha256_hex=hashlib.sha256(canonical(bundle)).hexdigest())

        def verify(*_args, **kwargs):
            return {**audit_report, "block_height": kwargs["expected_block_height"]}

        selection = direct_stratum.VersionRollingMaskSelection(
            direct_stratum.QBIT_VERSION_ROLLING_MASK, "fallback", "test"
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            cooperative_json(),
            patch("lab.prism.prism_coordinator.JsonRpc", return_value=rpc),
            patch.object(
                recovery.RecoveryCoordinator, "resolve_version_rolling_mask", return_value=selection
            ),
            patch.object(recovery.RecoveryCoordinator, "validate_live_chain_identity"),
            patch.object(recovery.RecoveryCoordinator, "validate_live_template_and_fee_policy"),
            patch.object(recovery.RecoveryCoordinator, "build_audit_bundle", return_value=bundle),
            patch("lab.prism.audit_artifacts.AuditArtifactStore.verify_bundle", side_effect=verify),
            patch("lab.prism.prism_coordinator.canonical_bundle_bytes", canonical),
            patch("lab.prism.share_ledger._default_bundle_canonicalizer", return_value=canonical),
        ):
            reader = recovery.RecoveryReader(self.url)
            self.addCleanup(reader.close)
            config = test_config(Path(directory), self.url)
            events = []
            blocks = recovery.plan_recovery(reader, rpc, [child.block_hash, parent.block_hash])
            with patch(
                "lab.prism.audit_artifacts.AuditArtifactStore.publish_success",
                side_effect=RuntimeError("injected publication failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "injected publication failure"):
                    recovery.apply_recovery(
                        config, reader, blocks, timeout_seconds=30, report=events.append
                    )
            self.assertEqual(reader.metadata(parent.block_hash)["chain_state"], "confirmed")
            self.assertEqual(reader.metadata(parent.block_hash)["state"], "pending")
            self.assertIsNone(reader.metadata(child.block_hash)["chain_state"])
            events.clear()
            recovery.apply_recovery(
                config, reader, blocks, timeout_seconds=30, report=events.append
            )
            for block in blocks:
                recovery.require_completed(reader.metadata(block.block_hash), block)
            self.assertEqual(
                [event["block_hash"] for event in events if event["event"] == "completed"],
                [parent.block_hash, child.block_hash],
            )
            # Re-entry is a durable no-op; no payload or accounting is rebuilt.
            with patch.object(
                reader, "candidate", side_effect=AssertionError("terminal payload read")
            ):
                recovery.apply_recovery(
                    config, reader, blocks, timeout_seconds=30, report=events.append
                )
            self.assertFalse(any(call.args[0] == "submitblock" for call in rpc.call.call_args_list))


if __name__ == "__main__":
    unittest.main()
