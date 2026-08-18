#!/usr/bin/env python3
"""Focused PRISM coordinator block candidates tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import dataclasses
import io
import unittest
from unittest import mock
from tests.prism_vardiff_test_support import *
from lab.prism.coordinator_config import LifecycleConfig
from lab.prism.audit_artifacts import (
    AuditArtifactStore,
    AuditPublicationIdentity,
    RetentionResult,
)
from lab.prism.block_candidates import (
    BlockCandidateAttemptResult,
    BlockCandidateRunResult,
    _BlockCandidateNodeSubmission,
    block_candidate_from_intent,
    block_candidate_intent,
)
from lab.prism.bundle_compiler import BundleCompiler
from lab.prism.prism_coordinator import PrismCoordinator


_compat_verified_audit_report = verified_audit_report


def configure_temporary_audit_root(
    test_case: unittest.TestCase,
    server: PrismCoordinator,
) -> None:
    temporary = tempfile.TemporaryDirectory()
    server.audit_dir = Path(temporary.name) / "audit"
    server.evidence_path = Path(temporary.name) / "state" / "evidence.json"

    def cleanup() -> None:
        store = server.__dict__.get("_audit_artifact_store")
        if isinstance(store, AuditArtifactStore):
            store.close()
        temporary.cleanup()

    test_case.addCleanup(cleanup)


def verified_audit_report(
    coinbase_tx_hex: str = "c0ffee",
    block_height: int = 10,
) -> dict[str, object]:
    report = _compat_verified_audit_report(coinbase_tx_hex)
    report["schema"] = "qbit.prism.audit-verification-report.v1"
    report["block_height"] = block_height
    report["coinbase_value_sats"] = 50_00000000
    return report


class PrismCoordinatorVardiffTests(unittest.TestCase):
    def test_audit_store_lazy_adopts_compatibility_fields_before_and_after_construction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            server = PrismCoordinator.__new__(PrismCoordinator)
            server.audit_dir = base / "audit-a"
            server.evidence_path = base / "state-a" / "evidence.json"
            server.audit_live_bundle_retention = 3
            server.audit_candidate_retention_seconds = 7
            server.audit_share_segment_size = 11
            first = server._ensure_audit_artifact_store()
            self.assertEqual(first.root, server.audit_dir.resolve())
            self.assertEqual(first.evidence_path, server.evidence_path.resolve())
            self.assertEqual(first.live_bundle_retention, 3)
            self.assertEqual(first.candidate_retention_seconds, 7)
            self.assertEqual(first.share_segment_size, 11)

            server.audit_dir = base / "audit-b"
            server.evidence_path = base / "state-b" / "evidence.json"
            server.audit_live_bundle_retention = 5
            server.audit_candidate_retention_seconds = 13
            server.audit_share_segment_size = 17
            second = server._ensure_audit_artifact_store()
            self.assertIs(second, first)
            self.assertEqual(second.root, server.audit_dir.resolve())
            self.assertEqual(second.evidence_path, server.evidence_path.resolve())
            self.assertEqual(second.live_bundle_retention, 5)
            self.assertEqual(second.candidate_retention_seconds, 13)
            self.assertEqual(second.share_segment_size, 17)

            candidate = second.issue_candidate(block_hash="aa" * 32)
            server.audit_dir = base / "audit-c"
            with self.assertRaisesRegex(RuntimeError, "candidates are active"):
                server._ensure_audit_artifact_store()
            self.assertIs(server.__dict__["_audit_artifact_store"], first)
            self.assertEqual(first.root, (base / "audit-b").resolve())
            first.discard_candidate(candidate)

    def test_coordinator_latest_evidence_seed_is_stable_and_defensive_before_and_after_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server = PrismCoordinator.__new__(PrismCoordinator)
            seed = {"nested": {"value": 1}}
            server.latest_evidence = seed
            seed["nested"]["value"] = 2
            before = server.latest_evidence
            self.assertEqual(before, {"nested": {"value": 1}})
            assert before is not None
            before["nested"]["value"] = 3
            self.assertEqual(server.latest_evidence, {"nested": {"value": 1}})

            root = Path(tmp)
            server.audit_dir = root / "audit"
            server.evidence_path = root / "state" / "evidence.json"
            store = server._ensure_audit_artifact_store()
            self.assertNotIn("_audit_latest_evidence_seed", server.__dict__)
            self.assertEqual(server.latest_evidence_payload(), {"nested": {"value": 1}})
            snapshot = server.latest_evidence_payload()
            assert snapshot is not None
            snapshot["nested"]["value"] = 4  # type: ignore[index]
            self.assertEqual(server.latest_evidence_payload(), {"nested": {"value": 1}})
            server.latest_evidence = None
            self.assertIsNone(server.latest_evidence_payload())
            server.latest_evidence = {"after": {"value": 5}}
            self.assertEqual(server.latest_evidence_payload(), {"after": {"value": 5}})
            self.assertIs(server._ensure_audit_artifact_store(), store)

    def test_coordinator_audit_static_and_instance_facades_preserve_contract(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        sentinel = mock.Mock()
        sentinel.metrics_snapshot.return_value = {"scan_error": 0}
        sentinel.prune_best_effort.return_value = RetentionResult(live_removed=1)
        sentinel.verify_bundle.return_value = {"verified": True}
        server._ensure_audit_artifact_store = lambda: sentinel  # type: ignore[method-assign]

        self.assertEqual(
            PrismCoordinator.audit_artifact_kind(
                f"prism-live-audit-bundle-1-{'aa' * 32}.json"
            ),
            "live_bundle",
        )
        self.assertEqual(server.audit_artifact_metrics(), {"scan_error": 0})
        keep = Path("keep.json")
        server.prune_audit_artifacts(keep_live_path=keep)
        sentinel.prune_best_effort.assert_called_once_with(keep_live_path=keep)
        result = PrismCoordinator.verify_bundle(
            server,
            Path("bundle.json"),
            "00",
            "11" * 32,
            expected_coinbase_value_sats=5,
            expected_block_height=6,
        )
        self.assertEqual(result, {"verified": True})
        sentinel.verify_bundle.assert_called_once_with(
            Path("bundle.json"),
            "00",
            "11" * 32,
            expected_coinbase_value_sats=5,
            expected_block_height=6,
        )
        server.ledger_writer_public_key_hex = "22" * 32
        self.assertEqual(
            server.trusted_ledger_writer_public_key_hex({}),
            "22" * 32,
        )

        with tempfile.TemporaryDirectory() as tmp:
            candidate = Path(tmp) / "candidate.json"
            candidate.write_bytes(b"canonical")
            digest = hashlib.sha256(b"canonical").hexdigest()
            self.assertEqual(
                PrismCoordinator.verified_canonical_bundle_path(
                    candidate,
                    {"audit_bundle_sha256_hex": digest.upper()},
                ),
                candidate,
            )

    def test_candidate_verifier_override_is_resolved_after_store_construction(self) -> None:
        server, state, ledger = submit_coordinator()
        server.max_blocks = 10
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tmp:
            server.audit_dir = Path(tmp)
            server.evidence_path = Path(tmp) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            store = server._ensure_audit_artifact_store()
            calls: list[str] = []

            def verifier_a(*_args: object, **_kwargs: object) -> dict[str, object]:
                calls.append("a")
                return verified_audit_report()

            def verifier_b(*_args: object, **_kwargs: object) -> dict[str, object]:
                calls.append("b")
                return verified_audit_report()

            server.verify_bundle = verifier_a  # type: ignore[method-assign]
            server.verify_bundle = verifier_b  # type: ignore[method-assign]
            block_hash = "91" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )
            self.assertIs(server._ensure_audit_artifact_store(), store)
            self.assertEqual(calls, ["b"])

            direct_server, direct_state, direct_ledger = submit_coordinator()
            direct_server.max_blocks = 10
            direct_server.stop_after_block = False
            direct_server.audit_dir = Path(tmp) / "direct-a1"
            direct_server.evidence_path = Path(tmp) / "direct-a1-evidence.json"
            direct_server.ledger_writer_public_key_hex = "aa" * 32
            direct_store = direct_server._ensure_audit_artifact_store()
            direct_calls: list[str] = []

            def direct_a1_verifier(
                *_args: object,
                **_kwargs: object,
            ) -> dict[str, object]:
                direct_calls.append("a1")
                return verified_audit_report()

            direct_store.verify_bundle = direct_a1_verifier  # type: ignore[method-assign]
            self.assertNotIn("verify_bundle", direct_server.__dict__)
            direct_hash = "93" * 32
            direct_server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=direct_hash,
                ledger=direct_ledger,
            )
            direct_server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            direct_submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=direct_hash,
                block_hex="00",
            )
            self.assertTrue(
                direct_server.submit_block_candidate(
                    block_candidate(
                        direct_server,
                        direct_state,
                        direct_submission,
                    )
                )
            )
            self.assertEqual(direct_calls, ["a1"])

    def test_audit_publication_occurs_after_durable_confirm_and_before_success_tail(self) -> None:
        server, state, ledger = submit_coordinator()
        events: list[str] = []
        with tempfile.TemporaryDirectory() as tmp:
            server.audit_dir = Path(tmp)
            server.evidence_path = Path(tmp) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            store = server._ensure_audit_artifact_store()
            payout_service = server._ensure_payout_state_service()
            real_persist = ledger.persist_accepted_block
            real_confirm = ledger.confirm_accepted_block
            real_floor = ledger.audit_publication_sequence_floor
            real_identity = server._audit_publication_identity
            real_publish = store.publish_success
            real_shutdown = server.request_shutdown

            def verify(*_args: object, **_kwargs: object) -> dict[str, object]:
                events.append("verify")
                return verified_audit_report()

            def persist(**kwargs: object) -> dict[str, object]:
                events.append("persist")
                return real_persist(**kwargs)

            def confirm(**kwargs: object) -> dict[str, object]:
                self.assertTrue(
                    payout_service._payout_balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
                )
                self.assertEqual(store._publication_guard_owner, threading.get_ident())
                events.append("confirm")
                return real_confirm(**kwargs)

            def publication_identity(**kwargs: object) -> AuditPublicationIdentity:
                self.assertTrue(
                    payout_service._payout_balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
                )
                self.assertEqual(store._publication_guard_owner, threading.get_ident())
                events.append("publication_identity")
                return real_identity(**kwargs)

            def publication_floor() -> int:
                self.assertTrue(
                    payout_service._payout_balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
                )
                self.assertEqual(store._publication_guard_owner, threading.get_ident())
                events.append("publication_floor")
                return real_floor()

            def publish(**kwargs: object) -> object:
                self.assertTrue(
                    payout_service._payout_balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
                )
                self.assertEqual(store._publication_guard_owner, threading.get_ident())
                events.append("publish_success")
                return real_publish(**kwargs)

            def terminal_success() -> None:
                events.append("terminal_success")
                real_shutdown()

            server.verify_bundle = verify  # type: ignore[method-assign]
            ledger.persist_accepted_block = persist  # type: ignore[method-assign]
            ledger.confirm_accepted_block = confirm  # type: ignore[method-assign]
            ledger.audit_publication_sequence_floor = publication_floor  # type: ignore[method-assign]
            server._audit_publication_identity = publication_identity  # type: ignore[method-assign]
            store.publish_success = publish  # type: ignore[method-assign]
            server.request_shutdown = terminal_success  # type: ignore[method-assign]
            block_hash = "92" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )
            self.assertEqual(
                events,
                [
                    "verify",
                    "persist",
                    "confirm",
                    "publication_identity",
                    "publication_floor",
                    "publish_success",
                    "terminal_success",
                ],
            )

    def test_audit_publication_failure_after_confirm_retries_same_ordinal_once(self) -> None:
        server, state, ledger = submit_coordinator()
        block_hash = "94" * 32
        with tempfile.TemporaryDirectory() as tmp:
            server.audit_dir = Path(tmp)
            server.evidence_path = Path(tmp) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            store = server._ensure_audit_artifact_store()
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            real_confirm = ledger.confirm_accepted_block
            real_publish = store.publish_success
            real_shutdown = server.request_shutdown
            confirmation_sequences: list[int] = []
            publication_sequences: list[int] = []
            terminal_successes: list[str] = []
            publication_attempts = 0

            def confirm(**kwargs: object) -> dict[str, object]:
                result = real_confirm(**kwargs)
                result["audit_publication_sequence"] = 7
                confirmation_sequences.append(7)
                return result

            def publish(**kwargs: object) -> object:
                nonlocal publication_attempts
                publication_attempts += 1
                identity = kwargs["identity"]
                assert isinstance(identity, AuditPublicationIdentity)
                publication_sequences.append(identity.sequence)
                if publication_attempts == 1:
                    raise RuntimeError("injected evidence publication failure")
                return real_publish(**kwargs)

            def terminal_success() -> None:
                terminal_successes.append("success")
                real_shutdown()

            ledger.confirm_accepted_block = confirm  # type: ignore[method-assign]
            ledger.audit_publication_sequence_floor = lambda: 7  # type: ignore[method-assign]
            store.publish_success = publish  # type: ignore[method-assign]
            server.request_shutdown = terminal_success  # type: ignore[method-assign]
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            candidate = block_candidate(server, state, submission)

            with self.assertRaisesRegex(
                RuntimeError,
                "injected evidence publication failure",
            ):
                server.submit_block_candidate(candidate)
            self.assertEqual(confirmation_sequences, [7])
            self.assertEqual(publication_sequences, [7])
            self.assertIsNone(store.latest_evidence())
            self.assertEqual(server.accepted_block_count, 0)
            self.assertEqual(terminal_successes, [])

            assert isinstance(server.rpc, SubmitRpc)
            server.rpc.tip = block_hash
            self.assertTrue(server.submit_block_candidate(candidate))
            self.assertEqual(confirmation_sequences, [7, 7])
            self.assertEqual(publication_sequences, [7, 7])
            self.assertEqual(server.accepted_block_count, 1)
            self.assertEqual(terminal_successes, ["success"])
            latest = store.latest_evidence()
            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(
                latest["audit_publication_identity"]["sequence"],  # type: ignore[index]
                7,
            )

    def test_publication_floor_failure_or_mismatch_never_publishes_evidence(self) -> None:
        cases = (
            ("query_failure", RuntimeError("floor query failed"), "floor query failed"),
            ("below_identity", 0, "exceeds"),
            ("above_identity", 2, "behind"),
            ("invalid_bool", True, "floor sequence"),
        )
        for name, floor_result, error_pattern in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                server, state, ledger = submit_coordinator()
                block_hash = "95" * 32
                server.audit_dir = Path(tmp)
                server.evidence_path = Path(tmp) / "evidence.json"
                server.ledger_writer_public_key_hex = "aa" * 32
                store = server._ensure_audit_artifact_store()
                server.rpc = SubmitRpc(
                    tip="00" * 32,
                    block_hash=block_hash,
                    ledger=ledger,
                )
                server.build_audit_bundle = (  # type: ignore[method-assign]
                    lambda **_kwargs: verified_block_bundle()
                )
                server.verify_bundle = (  # type: ignore[method-assign]
                    lambda *_args, **_kwargs: verified_audit_report()
                )
                publish_calls = 0
                real_publish = store.publish_success

                def publication_floor() -> int:
                    if isinstance(floor_result, BaseException):
                        raise floor_result
                    return floor_result  # type: ignore[return-value]

                def publish(**kwargs: object) -> object:
                    nonlocal publish_calls
                    publish_calls += 1
                    return real_publish(**kwargs)

                ledger.audit_publication_sequence_floor = publication_floor  # type: ignore[method-assign]
                store.publish_success = publish  # type: ignore[method-assign]
                submission = SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                )
                with self.assertRaisesRegex(
                    (RuntimeError, ValueError),
                    error_pattern,
                ):
                    server.submit_block_candidate(
                        block_candidate(server, state, submission)
                    )
                self.assertEqual(
                    publish_calls,
                    0 if name == "query_failure" else 1,
                )
                self.assertIsNone(store.latest_evidence())
                self.assertEqual(server.accepted_block_count, 0)

    def test_make_ledger_preserves_single_a1_store_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server = PrismCoordinator.__new__(PrismCoordinator)
            server.audit_dir = Path(tmp) / "audit-a"
            server.evidence_path = Path(tmp) / "state-a" / "evidence.json"
            store = server._ensure_audit_artifact_store()
            ledger = SimpleNamespace()
            with mock.patch.dict(
                os.environ,
                {"PRISM_POSTGRES_PSQL_COMMAND": "psql example"},
                clear=True,
            ), mock.patch(
                "lab.prism.prism_coordinator.PsqlShareLedger",
                return_value=ledger,
            ) as constructor:
                self.assertIs(server.make_ledger(), ledger)
            self.assertIs(
                constructor.call_args.kwargs["audit_artifact_store"],
                store,
            )
            server.audit_dir = Path(tmp) / "audit-b"
            server.evidence_path = Path(tmp) / "state-b" / "evidence.json"
            self.assertIs(server._ensure_audit_artifact_store(), store)
            self.assertEqual(store.root, server.audit_dir.resolve())

            memory_server = PrismCoordinator.__new__(PrismCoordinator)
            with mock.patch.dict(
                os.environ,
                {"PRISM_ALLOW_MEMORY_LEDGER": "1"},
                clear=True,
            ):
                self.assertIsInstance(
                    memory_server.make_ledger(),
                    SingleWriterShareLedger,
                )
            self.assertNotIn("_audit_artifact_store", memory_server.__dict__)

    def test_legacy_evidence_upgrade_requires_exact_durable_pool_block_proof(self) -> None:
        legacy_identity = AuditPublicationIdentity(0, 10, "aa" * 32)

        for maturity_state in ("immature", "mature"):
            with self.subTest(valid_maturity=maturity_state):
                server = coordinator()
                store = mock.MagicMock()
                store.legacy_evidence_identity.return_value = legacy_identity
                server._ensure_audit_artifact_store = lambda: store  # type: ignore[method-assign]
                server.ledger = SimpleNamespace(
                    audit_publication_sequence_floor=lambda: 7,
                    pool_block_state=lambda **_kwargs: {
                        "block_hash": legacy_identity.block_hash,
                        "block_height": legacy_identity.block_height,
                        "chain_state": "confirmed",
                        "maturity_state": maturity_state,
                        "audit_publication_sequence": 7,
                    }
                )
                server._upgrade_legacy_audit_evidence()
                store.adopt_legacy_publication_identity.assert_called_once_with(
                    AuditPublicationIdentity(7, 10, legacy_identity.block_hash),
                    publication_floor_sequence=7,
                )
                store.invalidate_unprovable_legacy_evidence.assert_not_called()

        invalid_states = (
            None,
            {
                "block_height": 10,
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": 7,
            },
            {
                "block_hash": legacy_identity.block_hash.upper(),
                "block_height": 10,
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": 7,
            },
            {
                "block_hash": legacy_identity.block_hash,
                "block_height": "10",
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": 7,
            },
            {
                "block_hash": legacy_identity.block_hash,
                "block_height": True,
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": 7,
            },
            {
                "block_height": 10,
                "chain_state": "inactive",
                "maturity_state": "immature",
                "audit_publication_sequence": 7,
            },
            {
                "block_height": 10,
                "chain_state": "confirmed",
                "maturity_state": "reversed",
                "audit_publication_sequence": 7,
            },
            {
                "block_height": 10,
                "chain_state": "confirmed",
                "maturity_state": "unknown",
                "audit_publication_sequence": 7,
            },
            {
                "block_height": 11,
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": 7,
            },
            {
                "block_height": 10,
                "chain_state": "confirmed",
                "maturity_state": "immature",
                "audit_publication_sequence": True,
            },
        )
        for state in invalid_states:
            with self.subTest(invalid_state=state):
                server = coordinator()
                store = mock.MagicMock()
                store.legacy_evidence_identity.return_value = legacy_identity
                server._ensure_audit_artifact_store = lambda: store  # type: ignore[method-assign]
                server.ledger = SimpleNamespace(
                    pool_block_state=lambda **_kwargs: state
                )
                server._upgrade_legacy_audit_evidence()
                store.invalidate_unprovable_legacy_evidence.assert_called_once_with()
                store.adopt_legacy_publication_identity.assert_not_called()

        server = coordinator()
        store = mock.MagicMock()
        store.legacy_evidence_identity.return_value = legacy_identity
        server._ensure_audit_artifact_store = lambda: store  # type: ignore[method-assign]
        server.ledger = SingleWriterShareLedger()
        server._upgrade_legacy_audit_evidence()
        store.invalidate_unprovable_legacy_evidence.assert_called_once_with()

    def test_build_audit_bundle_passes_pool_fee_policy_to_cli_payload(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="99")
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
            )

        self.assertEqual(bundle, {"ok": True})
        self.assertEqual(captured["payload"]["payout_policy"]["pool_fee_policy"]["fee_bps"], 125)
        self.assertEqual(
            captured["payload"]["payout_policy"]["pool_fee_policy"]["p2mr_program_hex"],
            "99" * 32,
        )

    def test_build_audit_bundle_passes_ctv_settlement_config_to_cli_payload(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        with patch.dict(
            os.environ,
            {
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS": "10485760",
                "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS": "16",
                "PRISM_MAX_DIRECT_COINBASE_OUTPUTS": "12",
                "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION": "1000",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "25",
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS": "12000",
            },
            clear=True,
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
            )

        self.assertEqual(bundle, {"ok": True})
        self.assertEqual(
            captured["payload"]["ctv_settlement"],
            {
                "direct_floor_sats": 10_485_760,
                "config": {
                    "max_coinbase_settlement_outputs": 16,
                    "max_direct_coinbase_outputs": 12,
                    "max_fanout_recipients_per_transaction": 1000,
                    "reserved_coinbase_outputs": 0,
                },
                "fanout_fee_rate_policy": {
                    "market_fee_rate_sats_per_1000_weight": 25,
                    "premium_bps": 12_000,
                },
            },
        )

    def test_build_audit_bundle_preserves_exact_canonical_output_file(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        canonical_bytes = b'{"ok":true,"nested":[1,2]}'
        captured: dict[str, object] = {}

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(
                captured,
                output_text=canonical_bytes.decode("utf-8"),
            ),
        ):
            output_path = Path(tmp) / "candidate.audit.json"
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                canonical_output_path=output_path,
            )

            self.assertEqual(bundle, {"ok": True, "nested": [1, 2]})
            self.assertEqual(output_path.read_bytes(), canonical_bytes)
            self.assertIn("--canonical-output", captured["cmd"])
            self.assertEqual(captured["payload"]["shares"], [])

    def test_build_audit_bundle_transfers_the_exact_open_output_inode(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}
        adopted: list[tuple[Path, int, int]] = []

        def adopt(path: Path, value: os.stat_result) -> None:
            current = path.stat()
            self.assertEqual((value.st_dev, value.st_ino), (current.st_dev, current.st_ino))
            adopted.append((path, value.st_dev, value.st_ino))

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
            output_path = Path(tmp) / "candidate.audit.json"
            server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                canonical_output_path=output_path,
                canonical_output_adopter=adopt,
            )

            self.assertEqual(len(adopted), 1)
            self.assertEqual(adopted[0][0], output_path)
            self.assertEqual(adopted[0][1:], (output_path.stat().st_dev, output_path.stat().st_ino))

    def test_build_audit_bundle_pinned_parent_never_recreates_stale_path(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
            base = Path(tmp)
            root = base / "audit"
            root.mkdir()
            pinned = base / "audit-pinned"
            output_path = root / "candidate.audit.json"
            parent_fd = os.open(
                root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            root.rename(pinned)

            def reject_stale_authority(
                path: Path,
                value: os.stat_result,
            ) -> None:
                self.assertEqual(path, output_path)
                self.assertFalse(root.exists())
                pinned_output = pinned / output_path.name
                self.assertTrue(pinned_output.exists())
                self.assertEqual(
                    (value.st_dev, value.st_ino),
                    (pinned_output.stat().st_dev, pinned_output.stat().st_ino),
                )
                raise RuntimeError("stale A1 authority")

            try:
                with self.assertRaisesRegex(RuntimeError, "stale A1 authority"):
                    server.build_audit_bundle(
                        shares=[],
                        found_block={
                            "block_height": 10,
                            "coinbase_value_sats": 50_00000000,
                        },
                        prior_balances=[],
                        coinbase_script_sig_suffix_hex="00",
                        canonical_output_path=output_path,
                        canonical_output_parent_fd=parent_fd,
                        canonical_output_adopter=reject_stale_authority,
                    )
            finally:
                os.close(parent_fd)

            self.assertFalse(root.exists())
            self.assertFalse((pinned / output_path.name).exists())

    def test_build_audit_bundle_adopter_failure_preserves_path_replacement(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        def replace_then_reject(path: Path, _value: os.stat_result) -> None:
            path.unlink()
            path.write_bytes(b"competitor")
            raise RuntimeError("transfer rejected")

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
            output_path = Path(tmp) / "candidate.audit.json"
            with self.assertRaisesRegex(RuntimeError, "transfer rejected"):
                server.build_audit_bundle(
                    shares=[],
                    found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                    canonical_output_path=output_path,
                    canonical_output_adopter=replace_then_reject,
                )

            self.assertEqual(output_path.read_bytes(), b"competitor")

    def test_builder_cleanup_never_moves_an_existing_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "candidate.audit.json"
            path.write_bytes(b"owned-a")
            identity = path.stat()
            path.unlink()
            path.write_bytes(b"competitor-b")
            with patch(
                "os.replace",
                side_effect=AssertionError("replacement must not move"),
            ):
                BundleCompiler._remove_created_output_if_same(path, identity)

            self.assertEqual(path.read_bytes(), b"competitor-b")
            quarantines = list(root.glob(".candidate.audit.json.*.cleanup"))
            self.assertEqual(quarantines, [])

    def test_builder_cleanup_never_relocates_nonregular_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "candidate.audit.json"
            path.write_bytes(b"owned")
            identity = path.stat()
            path.unlink()
            path.mkdir()

            BundleCompiler._remove_created_output_if_same(path, identity)

            self.assertTrue(path.is_dir())
            self.assertEqual(
                list(root.glob(".candidate.audit.json.*.cleanup")),
                [],
            )

            path.rmdir()
            target = root / "operator"
            target.write_bytes(b"operator")
            path.symlink_to(target)
            BundleCompiler._remove_created_output_if_same(path, identity)
            self.assertTrue(path.is_symlink())
            self.assertEqual(path.read_bytes(), b"operator")

    def test_build_audit_bundle_summary_only_requests_and_parses_job_summary(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        summary = {
            "found_block": {
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
            },
            "signed_coinbase_manifest": {
                "manifest": {"coinbase_tx_hex": "c0ffee"},
                "signature": {"signature_hex": "11" * 64},
            },
        }
        captured: dict[str, object] = {}

        with patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(
                captured,
                output_text=json.dumps(summary, separators=(",", ":")),
            ),
        ), patch.object(
            Path,
            "open",
            side_effect=AssertionError("summary-only build must not open an output path"),
        ):
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
            )

        self.assertEqual(bundle, summary)
        self.assertEqual(set(bundle), {"found_block", "signed_coinbase_manifest"})
        self.assertIn("--job-summary-output", captured["cmd"])
        self.assertNotIn("--canonical-output", captured["cmd"])

    def test_build_audit_bundle_removes_partial_output_after_builder_failure(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32

        captured: dict[str, object] = {}
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "candidate.audit.json"
            with patch(
                "lab.prism.prism_coordinator.subprocess.Popen",
                fake_audit_bundle_popen(
                    captured,
                    output_text='{"partial":',
                    returncode=9,
                    stderr_text="synthetic builder failure",
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "synthetic builder failure"):
                    server.build_audit_bundle(
                        shares=[],
                        found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                        prior_balances=[],
                        coinbase_script_sig_suffix_hex="00",
                        canonical_output_path=output_path,
                    )
            self.assertFalse(output_path.exists())
            self.assertEqual(server.job_build_worker_counts["crashes"], 1)

            with patch(
                "lab.prism.prism_coordinator.subprocess.Popen",
                fake_audit_bundle_popen(captured),
            ):
                recovered = server.build_audit_bundle(
                    shares=[],
                    found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                )

            self.assertEqual(recovered, {"ok": True})
            self.assertEqual(server.job_build_worker_counts["restarts"], 1)


    def test_build_audit_bundle_removes_owned_output_after_parse_failure(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            fake_audit_bundle_popen(captured, output_text='{"partial":'),
        ):
            output_path = Path(tmp) / "candidate.audit.json"
            with self.assertRaises(json.JSONDecodeError):
                server.build_audit_bundle(
                    shares=[],
                    found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                    canonical_output_path=output_path,
                )
            self.assertFalse(output_path.exists())

    def test_build_audit_bundle_recovers_after_cancelled_worker_timeout(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        cancellation = _JobBuildCancellation(timeout_seconds=60)
        process_calls = 0

        class FakeStdin:
            def write(self, value: str) -> int:
                return len(value)

            def close(self) -> None:
                return None

        class HungThenHealthyPopen:
            def __init__(self, _cmd: list[str], **kwargs: object) -> None:
                nonlocal process_calls
                process_calls += 1
                self.healthy = process_calls == 2
                self.stdin = FakeStdin()
                self.stdout = kwargs["stdout"]
                self.stderr = kwargs["stderr"]
                self.returncode: int | None = None
                self.output_written = False

            def poll(self) -> int | None:
                if self.returncode is not None:
                    return self.returncode
                if self.healthy:
                    if not self.output_written:
                        self.stdout.write('{"ok":true}')  # type: ignore[union-attr]
                        self.output_written = True
                    self.returncode = 0
                    return 0
                cancellation.cancel("timeout")
                return None

            def terminate(self) -> None:
                self.returncode = -15

            def kill(self) -> None:
                self.returncode = -9

            def wait(self, timeout: float | None = None) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

        build_kwargs = {
            "shares": [],
            "found_block": {
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
            },
            "prior_balances": [],
            "coinbase_script_sig_suffix_hex": "00",
        }
        with patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            HungThenHealthyPopen,
        ):
            with self.assertRaisesRegex(JobBuildCancelled, "timeout"):
                server.build_audit_bundle(
                    **build_kwargs,
                    cancellation=cancellation,
                )
            recovered = server.build_audit_bundle(
                **build_kwargs,
                cancellation=_JobBuildCancellation(timeout_seconds=60),
            )

        self.assertEqual(recovered, {"ok": True})
        self.assertEqual(process_calls, 2)
        self.assertEqual(server.job_build_worker_counts["terminations"], 1)
        self.assertEqual(server.job_build_worker_counts["restarts"], 1)

    def test_build_audit_bundle_does_not_unlink_preexisting_output_path(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.prism_coordinator.subprocess.Popen"
        ) as popen:
            output_path = Path(tmp) / "preexisting-candidate.audit.json"
            output_path.write_bytes(b"do-not-clobber")
            with self.assertRaises(FileExistsError):
                server.build_audit_bundle(
                    shares=[],
                    found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                    canonical_output_path=output_path,
                )

            self.assertEqual(output_path.read_bytes(), b"do-not-clobber")
            popen.assert_not_called()

    def test_block_submit_rejects_job_when_prior_balances_changed_before_persist(self) -> None:
        server, state, _recording = submit_coordinator()
        configure_temporary_audit_root(self, server)
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]
        server.ledger = ledger
        server.jobs["job-1"].prior_balances = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("audit bundle should not be rebuilt from stale prior balances")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ef" * 32,
            block_hex="00",
        )
        pending = self._pending_append("balance-stale").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        # The share was already accepted at submit time, so a lost block is a
        # block-abandonment, not a stale share rejection.
        self.assertEqual(server.stale_share_count, 0)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 0, "balance_stale": 1, "append_epoch_stale": 0},
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "abandoned")
        self.assertEqual(
            outbox_row["last_error"],
            "prior balances changed since the job was issued",
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="tip_moved"} 0',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="balance_stale"} 1',
            metrics,
        )

    def test_block_submit_defers_descendant_until_active_ancestor_is_durable(
        self,
    ) -> None:
        parent_hash = "ed" * 32
        ancestor_hash = "ee" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        server.max_blocks = 10
        server.stop_after_block = False
        ledger.durable_payout_state = True
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        context = server.jobs["job-1"]
        context.template["height"] = 12
        context.prior_balances = preview
        original_rpc_call = server.rpc.call
        submit_calls: list[str] = []

        def active_ancestor_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return ancestor_hash
            if method == "submitblock":
                submit_calls.append(method)
            return original_rpc_call(method, params)

        server.rpc.call = active_ancestor_call  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            ancestor_hash,
            block_height=10,
        )
        server._publish_accepted_block_payout_preview(ancestor_hash, preview)
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("descendant must wait for ancestor durability")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ef" * 32,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(
            block_candidate(server, state, submission)
        )

        self.assertFalse(accepted)
        # Node propagation is the fast lane: the durable descendant is offered
        # to qbitd before payout/accounting notices the ancestor transition.
        # Its accounting still defers until that ancestor is durable.
        self.assertEqual(submit_calls, ["submitblock"])
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertFalse(server.stop_event.is_set())

    def test_active_descendant_replay_stays_landed_until_ancestor_is_durable(
        self,
    ) -> None:
        parent_hash = "ea" * 32
        ancestor_hash = "eb" * 32
        descendant_hash = "ec" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        server.max_blocks = 10
        server.stop_after_block = False
        ledger.durable_payout_state = True
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        context = server.jobs["job-1"]
        context.template["height"] = 12
        context.prior_balances = preview
        original_rpc_call = server.rpc.call

        def active_descendant_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getbestblockhash":
                return descendant_hash
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return ancestor_hash
            if method == "submitblock":
                raise AssertionError("active descendant must not be resubmitted")
            return original_rpc_call(method, params)

        server.rpc.call = active_descendant_call  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            ancestor_hash,
            block_height=10,
        )
        server._publish_accepted_block_payout_preview(ancestor_hash, preview)
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("active descendant must wait for ancestor durability")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=descendant_hash,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(
            block_candidate(server, state, submission)
        )

        self.assertFalse(accepted)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        )
        transition = server._accepted_block_payout_previews[descendant_hash]
        self.assertTrue(transition.landed)
        self.assertIsNone(transition.preview)
        self.assertFalse(server.stop_event.is_set())

    def test_block_submit_reconciliation_error_is_structured_rejection(self) -> None:
        tip = "f0" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.reorg_reconciler_enabled = True
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=10),
            height=9,
            block_hashes={9: tip},
        )

        class FailingSubmitReorgLedger(RecordingLedger):
            def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
                raise RuntimeError("ledger unavailable")

        ledger = FailingSubmitReorgLedger()
        server.ledger = ledger
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("audit bundle should not be rebuilt after reconcile failure")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="f1" * 32,
            block_hex="00",
        )

        with patch("builtins.print") as printed:
            accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        self.assertEqual(
            server.block_candidate_abandoned_counts.get(PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE, 0),
            0,
        )
        messages = [str(call.args[0]) for call in printed.call_args_list if call.args]
        self.assertTrue(any("block candidate deferred" in message for message in messages))
        self.assertFalse(any("block candidate abandoned" in message for message in messages))
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE], 0)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(ledger.pending, [])

    def test_address_worker_suffixes_share_one_payout_account(self) -> None:
        server, state_a, ledger = submit_coordinator()
        usernames = [f"{PAYOUT_ADDRESS}.rig-a", f"{PAYOUT_ADDRESS}.rig-b"]
        submissions = [
            SimpleNamespace(
                header_hex="aa" * 80,
                block_hash_hex="bd" * 32,
                share_pass=True,
                block_pass=False,
            ),
            SimpleNamespace(
                header_hex="bb" * 80,
                block_hash_hex="be" * 32,
                share_pass=True,
                block_pass=False,
            ),
        ]
        states = [state_a, client()]
        states[1].active_job_ids = {"job-1"}
        for state, username in zip(states, usernames, strict=True):
            worker = WorkerIdentity(
                username=username,
                payout_address=PAYOUT_ADDRESS,
                worker_name=username.rsplit(".", 1)[1],
                script_pubkey_hex="5220" + "55" * 32,
                p2mr_program_hex="55" * 32,
            )
            state.username = username
            state.worker = worker
            state.subscribed = True
            state.authorized = True
            server.jobs["job-1"].worker = worker
            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submissions.pop(0),
            ):
                server.handle_submit(
                    state,
                    [username, "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual([pending.miner_id for pending in ledger.pending], [PAYOUT_ADDRESS, PAYOUT_ADDRESS])
        self.assertEqual([pending.order_key for pending in ledger.pending], [PAYOUT_ADDRESS, PAYOUT_ADDRESS])

    def _pending_append(self, tag: str, accepted_at_ms: int = 2) -> PendingShareAppend:
        from lab.prism.share_ledger import PendingShare

        return PendingShareAppend(
            pending_share=PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex=tag * 32,
            collection_only=False,
            credit_policy=None,
        )

    def test_orphaned_block_candidate_keeps_share_credit(self) -> None:
        # Option-A semantics: a share that met its target stays credited even
        # when its block candidate loses the tip race in the submitter.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        configure_temporary_audit_root(self, server)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            block_hex="00",
            share_pass=True,
            block_pass=True,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(len(ledger.all_shares()), 1)
        # The tip moves before the submitter drains the candidate.
        server.rpc = RejectingSubmitTipRpc(new_tip)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(server.accepted_block_count, 0)
        # Counted as a block abandonment, not a share rejection.
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 1, "balance_stale": 0, "append_epoch_stale": 0},
        )
        self.assertEqual(server.stale_share_count, 0)
        # The credited share survives the lost block race.
        self.assertEqual(len(ledger.all_shares()), 1)
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "abandoned")
        self.assertEqual(
            outbox_row["last_error"],
            f"tip moved before submit: {new_tip}",
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="tip_moved"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="balance_stale"} 0',
            metrics,
        )

    def test_block_candidate_queue_overflow_coalesces_wakeup_without_drop(self) -> None:
        server, state, _ledger = submit_coordinator()
        configure_temporary_audit_root(self, server)
        server.block_candidate_queue = queue.Queue(maxsize=2)

        def candidate(tag: str) -> PrismBlockCandidate:
            return block_candidate(
                server,
                state,
                SimpleNamespace(block_hash_hex=tag * 32, share_pass=True, block_pass=True),
            )

        server.enqueue_block_candidate(candidate("aa"))
        server.enqueue_block_candidate(candidate("bb"))
        server.enqueue_block_candidate(candidate("cc"))

        self.assertEqual(server.block_candidates_dropped, 0)
        self.assertEqual(server.block_candidate_queue.qsize(), 2)
        remaining = [
            server.block_candidate_queue.get_nowait().submission.block_hash_hex
            for _ in range(2)
        ]
        # Existing wakeups remain ordered; the third candidate remains durable
        # in the outbox and will be re-read after the queue drains.
        self.assertEqual(remaining, ["aa" * 32, "bb" * 32])
        self.assertIn(
            "qbit_prism_block_candidates_dropped_total 0", server.metrics_payload()
        )
        self.assertIn(
            "qbit_prism_block_candidate_wakeups_coalesced_total 1",
            server.metrics_payload(),
        )

    def test_b1_codec_owner_round_trips_without_coordinator_state(self) -> None:
        server, state, _ledger = submit_coordinator()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                block_hash_hex="ca" * 32,
                block_hex="00",
                coinbase_tx_hex="11",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=PendingShare(
                share_id="codec-share",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=2,
                ntime=3,
            ),
        )

        intent = block_candidate_intent(candidate)
        replayed = block_candidate_from_intent(intent)

        self.assertEqual(replayed.submission.block_hash_hex, "ca" * 32)
        self.assertEqual(replayed.context.template, candidate.context.template)
        self.assertEqual(replayed.pending_share, candidate.pending_share)
        # The module decode carries the restart sentinel: process-local
        # append epochs are meaningless after a restart, so the landing
        # fence must revalidate the recorded window instead.
        self.assertEqual(replayed.context.payout_append_invalidation_epoch, -1)

    def test_b1_service_owns_queue_and_structured_attempt_results(self) -> None:
        server, state, _ledger = submit_coordinator()
        service = server._ensure_block_candidate_service()
        self.assertIs(server.block_candidate_queue, service.candidate_queue)
        self.assertIsInstance(service.submit_next(), BlockCandidateRunResult)
        self.assertFalse(service.submit_next().ran)

        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                block_hash_hex="cb" * 32,
                share_pass=True,
                block_pass=True,
            ),
        )

        def reject(_candidate: PrismBlockCandidate) -> bool:
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                "direct owner result",
                block_hash="cb" * 32,
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = reject  # type: ignore[method-assign]
        with patch("builtins.print"):
            result = service.attempt(candidate)

        self.assertIsInstance(result, BlockCandidateAttemptResult)
        self.assertFalse(result.accepted)
        self.assertEqual(result.reason, PRISM_REJECTION_STALE_JOB)

    def test_b1_service_resolves_replaced_stop_event_at_use_time(self) -> None:
        server, _state, _ledger = submit_coordinator()
        service = server._ensure_block_candidate_service()
        replacement = threading.Event()
        server.stop_event = replacement
        replacement.set()

        service.run()

        self.assertIs(service.ports.stop_event(), replacement)

    def test_failed_replay_adoption_does_not_abort_remaining_rows(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger

        def durable_candidate(tag: str, stamp: int) -> PrismBlockCandidate:
            pending = PendingShare(
                share_id=f"miner-a:{tag * 32}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=9,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=stamp,
                ntime=1,
            )
            value = dataclass_replace(
                block_candidate(
                    server,
                    state,
                    SimpleNamespace(
                        coinbase_tx_hex="00",
                        block_hash_hex=tag * 32,
                        block_hex="00",
                        share_pass=False,
                        block_pass=True,
                    ),
                    pending_share=pending,
                ),
                credit_share_on_accept=True,
            )
            ledger.append_batch([(pending, server.block_candidate_intent(value))])
            return value

        first = durable_candidate("a5", 1)
        second = durable_candidate("b5", 2)
        service = server._ensure_block_candidate_service()
        original_adopt = service.adopt_replayed_candidate

        def fail_first(value: PrismBlockCandidate) -> None:
            if value.submission.block_hash_hex == first.submission.block_hash_hex:
                raise RuntimeError("share-writer adoption unavailable")
            original_adopt(value)

        def unexpected_finish(_pending: PendingShare) -> None:
            raise AssertionError("unadopted replay must not be finished")

        service.adopt_replayed_candidate = fail_first  # type: ignore[method-assign]
        server._finish_pending_share_candidate = (  # type: ignore[method-assign]
            unexpected_finish
        )

        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 1)
        replayed = server._block_replay_candidate_queue.get_nowait()
        self.assertEqual(
            replayed.submission.block_hash_hex,
            second.submission.block_hash_hex,
        )
        # The unadopted row quarantines off the node-offer lane. Draining it
        # terminalizes only that row, and its never-adopted floor holder is
        # never finished.
        with patch("builtins.print"):
            self.assertTrue(server._run_one_invalid_block_candidate_quarantine())
        self.assertEqual(
            [intent["block_hash_hex"] for intent in ledger.pending_block_candidates()],
            [second.submission.block_hash_hex],
        )

    def test_durable_block_candidates_replay_after_queue_drains(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.block_candidate_queue = queue.Queue(maxsize=2)

        for index, tag in enumerate(("aa", "bb", "cc"), start=1):
            pending = PendingShare(
                share_id=f"miner-a:{tag * 32}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=9,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=index,
                ntime=1,
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=tag * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            )
            candidate = block_candidate(
                server, state, submission, pending_share=pending
            )
            ledger.append_batch([(pending, server.block_candidate_intent(candidate))])

        self.assertEqual(server.replay_pending_block_candidates(), 3)
        self.assertTrue(server.block_candidate_queue.empty())
        first = server._block_replay_candidate_queue.get_nowait()
        second = server._block_replay_candidate_queue.get_nowait()
        third = server._block_replay_candidate_queue.get_nowait()
        self.assertEqual(
            [
                first.submission.block_hash_hex,
                second.submission.block_hash_hex,
                third.submission.block_hash_hex,
            ],
            ["aa" * 32, "bb" * 32, "cc" * 32],
        )
        ledger.mark_block_candidate_submitted(block_hash="aa" * 32)
        ledger.mark_block_candidate_abandoned(block_hash="bb" * 32, error="stale")
        self.assertEqual(third.pending_share.share_id, "miner-a:" + "cc" * 32)

    def test_candidate_intent_avoids_duplicate_template_transaction_bodies(self) -> None:
        server, state, _ledger = submit_coordinator()
        witness_tx = synthetic_witness_transaction("55")
        server.jobs["job-1"].template["transactions"] = [{"data": witness_tx}]
        server.jobs["job-1"].job.transaction_hexes = (witness_tx,)
        pending = self._pending_append("ca").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="ca" * 32,
                block_hex="00" + witness_tx,
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )

        intent = server.block_candidate_intent(candidate)

        self.assertEqual(
            set(intent["template"]),
            {"previousblockhash", "height", "coinbasevalue"},
        )
        self.assertNotIn("transaction_hexes", intent)
        self.assertEqual(
            intent["witness_merkle_leaves_hex"],
            direct_stratum.witness_merkle_leaves_hex((witness_tx,)),
        )

    def test_transient_candidate_failure_remains_pending_for_retry(self) -> None:
        server, state, _recording = submit_coordinator()
        configure_temporary_audit_root(self, server)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = PendingShare(
            share_id="miner-a:" + "aa" * 32,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=2,
            ntime=1,
        )
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="aa" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        intent = server.block_candidate_intent(candidate)
        ledger.append_batch([(pending, intent)])
        server.enqueue_block_candidate(candidate)
        server.submit_block_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(RuntimeError("rpc unavailable"))
        )

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(ledger.pending_block_candidates(), [intent])
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertIn(
            "qbit_prism_block_candidate_retries_total 1",
            server.metrics_payload(),
        )

    def test_retryable_parent_stays_ahead_of_queued_child(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.block_candidate_retry_initial_seconds = 0
        server.block_candidate_retry_max_seconds = 0
        parent = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a1" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )
        child = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="b1" * 32,
                block_hex="01",
                share_pass=True,
                block_pass=True,
            ),
        )
        attempts: list[str] = []

        def submit(candidate: PrismBlockCandidate) -> bool:
            block_hash = str(candidate.submission.block_hash_hex)
            attempts.append(block_hash)
            if block_hash == "a1" * 32 and attempts.count(block_hash) == 1:
                server._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "temporary parent finalization failure",
                    block_hash=block_hash,
                    worker="miner-a",
                )
                return False
            return True

        server.submit_block_candidate = submit  # type: ignore[method-assign]
        self.assertTrue(server.enqueue_block_candidate(parent))
        self.assertTrue(server.enqueue_block_candidate(child))

        self.assertTrue(server.submit_next_block_candidate())
        self.assertIs(server._retry_block_candidate, parent)
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        self.assertTrue(server.submit_next_block_candidate())
        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(attempts, ["a1" * 32, "a1" * 32, "b1" * 32])
        self.assertTrue(server.block_candidate_queue.empty())

    def test_candidate_retry_backoff_is_capped_and_cleared_on_success(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("retry-success").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a1" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        attempts = 0

        def retry_then_succeed(_candidate: PrismBlockCandidate) -> bool:
            nonlocal attempts
            attempts += 1
            if attempts <= 4:
                server._defer_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "temporary RPC outage",
                    worker="miner-a",
                )
                return False
            return True

        server.submit_block_candidate = retry_then_succeed  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            for _attempt in range(5):
                server.enqueue_block_candidate(candidate)
                self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(len(waits), 6)
        for observed, expected in zip(
            waits,
            [0.1, 0.2, 0.25, 0.15, 0.25, 0.15],
            strict=True,
        ):
            self.assertAlmostEqual(observed, expected)
        self.assertNotIn(candidate.submission.block_hash_hex, server.block_candidate_retry_delays)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_candidate_retry_state_is_cleared_on_terminal_abandonment(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("retry-terminal").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a2" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_delays = {candidate.submission.block_hash_hex: 2.0}

        def terminal(_candidate: PrismBlockCandidate) -> bool:
            block_hash = _candidate.submission.block_hash_hex
            block_height = int(_candidate.context.template["height"])
            server._begin_accepted_block_payout_preview(
                block_hash,
                block_height=block_height,
            )
            server._mark_accepted_block_payout_landed(
                block_hash,
                block_height=block_height,
            )
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                "tip moved",
                block_hash=block_hash,
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = terminal  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertNotIn(candidate.submission.block_hash_hex, server.block_candidate_retry_delays)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertNotIn(
            candidate.submission.block_hash_hex,
            server._accepted_block_payout_previews,
        )
        self.assertNotIn(
            candidate.submission.block_hash_hex,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_terminal_abandonment_keeps_tombstone_when_outbox_update_fails(
        self,
    ) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("terminal-outbox-failure").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a3" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        block_hash = candidate.submission.block_hash_hex
        block_height = int(candidate.context.template["height"])
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])

        def terminal(_candidate: PrismBlockCandidate) -> bool:
            server._begin_accepted_block_payout_preview(
                block_hash,
                block_height=block_height,
            )
            server._mark_accepted_block_payout_landed(
                block_hash,
                block_height=block_height,
            )
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                "tip moved",
                block_hash=block_hash,
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = terminal  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)
        with patch.object(
            ledger,
            "mark_block_candidate_abandoned",
            side_effect=RuntimeError("postgres unavailable"),
        ):
            self.assertTrue(server.submit_next_block_candidate())

        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertIn(block_hash, server._invalidated_accepted_block_payout_previews)
        self.assertEqual(
            [intent["block_hash_hex"] for intent in ledger.pending_block_candidates()],
            [block_hash],
        )

    def test_finalize_failure_replays_with_candidate_backoff(self) -> None:
        server, state, _recording = submit_coordinator()
        configure_temporary_audit_root(self, server)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("f1").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f1" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        candidate_intent = server.block_candidate_intent(candidate)
        ledger.append_batch([(pending, candidate_intent)])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        # The block itself lands once; only the terminal outbox update fails.
        # Replays must pace like any other candidate retry AND run finalize-
        # only, never re-entering submission.
        submit_calls = 0

        def accepting_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            return True

        server.submit_block_candidate = accepting_submit  # type: ignore[method-assign]
        original_finish = ledger.mark_block_candidate_submitted
        original_mark_attempted = ledger.mark_block_candidate_attempted
        finish_attempts = 0
        attempt_marks = 0

        def mark_attempted(*, block_hash: str) -> bool:
            nonlocal attempt_marks
            attempt_marks += 1
            return original_mark_attempted(block_hash=block_hash)

        def flaky_finish(*, block_hash: str) -> bool:
            nonlocal finish_attempts
            finish_attempts += 1
            if finish_attempts <= 4:
                raise RuntimeError("ledger unavailable")
            return original_finish(block_hash=block_hash)

        ledger.mark_block_candidate_attempted = mark_attempted  # type: ignore[method-assign]
        ledger.mark_block_candidate_submitted = flaky_finish  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            for _attempt in range(4):
                server.enqueue_block_candidate(candidate)
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(ledger.pending_block_candidates(), [candidate_intent])
            server.enqueue_block_candidate(candidate)
            self.assertTrue(server.submit_next_block_candidate())

        # The first accepted finalize failure returns unpaced so the caller
        # can refresh the fleet immediately; the ladder starts at the first
        # finalize-only replay.
        self.assertEqual(len(waits), 4)
        for observed, expected in zip(
            waits,
            [0.1, 0.2, 0.25, 0.15],
            strict=True,
        ):
            self.assertAlmostEqual(observed, expected)
        self.assertEqual(finish_attempts, 5)
        self.assertEqual(attempt_marks, 1)
        self.assertEqual(submit_calls, 1)
        self.assertNotIn(candidate.submission.block_hash_hex, server.block_candidate_retry_delays)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(server._block_candidate_finalize_retries, {})
        self.assertIn(
            "qbit_prism_block_candidate_retries_total 4",
            server.metrics_payload(),
        )

    def test_abandon_finalize_failure_counts_one_abandonment(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        moved_tip = "11" * 32
        expected_error = f"tip moved before submit: {moved_tip}"
        pending = self._pending_append("f2").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f2" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        submit_calls = 0

        def terminal_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                expected_error,
                block_hash=_candidate.submission.block_hash_hex,
                worker="miner-a",
                stale_job_class="tip_moved",
            )
            return False

        server.submit_block_candidate = terminal_submit  # type: ignore[method-assign]
        original_finish = ledger.mark_block_candidate_abandoned
        finish_attempts = 0

        def flaky_finish(*, block_hash: str, error: str) -> bool:
            nonlocal finish_attempts
            finish_attempts += 1
            if finish_attempts <= 2:
                raise RuntimeError("ledger unavailable")
            return original_finish(block_hash=block_hash, error=error)

        ledger.mark_block_candidate_abandoned = flaky_finish  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            for _attempt in range(3):
                server.enqueue_block_candidate(candidate)
                self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(waits, [0.1, 0.2])
        self.assertEqual(submit_calls, 1)
        self.assertEqual(finish_attempts, 3)
        # Finalize-only replays must not recount the terminal abandonment.
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_REJECTION_STALE_JOB: 1},
        )
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 1, "balance_stale": 0, "append_epoch_stale": 0},
        )
        self.assertEqual(ledger.pending_block_candidates(), [])
        outbox_row = ledger._block_candidate_outbox[
            candidate.submission.block_hash_hex
        ]
        self.assertEqual(outbox_row["state"], "abandoned")
        self.assertEqual(outbox_row["last_error"], expected_error)
        self.assertEqual(server._block_candidate_finalize_retries, {})

    def test_accepted_finalize_failure_still_triggers_post_accept_refresh(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("f3").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f3" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        server.submit_block_candidate = lambda _candidate: True  # type: ignore[method-assign]

        def failing_finish(*, block_hash: str) -> bool:
            raise RuntimeError("ledger unavailable")

        ledger.mark_block_candidate_submitted = failing_finish  # type: ignore[method-assign]
        refreshed_clients: list[object] = []
        server.refresh_jobs_after_pending_accepted_block = (  # type: ignore[method-assign]
            lambda client, **_kwargs: refreshed_clients.append(client) or 0
        )
        released_shares: list[object] = []
        original_release = server._finish_pending_share_commit

        def recording_release(pending_share: object) -> None:
            released_shares.append(pending_share)
            original_release(pending_share)

        server._finish_pending_share_commit = recording_release  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            server.enqueue_block_candidate(candidate)
            self.assertTrue(server.submit_next_block_candidate())
            # The block is active on-chain; the fleet refresh fires on the
            # first finalize failure without waiting out a backoff, and the
            # snapshot anchor floor is released despite the pending outbox
            # mark.
            self.assertEqual(refreshed_clients, [state])
            self.assertEqual(waits, [])
            self.assertIn(pending, released_shares)
            self.assertTrue(server.submit_next_block_candidate())
            self.assertEqual(refreshed_clients, [state])
            self.assertEqual(waits, [0.1])

    def test_invalid_durable_candidate_is_quarantined_by_outbox_row_key(self) -> None:
        for payload_hash in (None, "ff" * 32):
            with self.subTest(payload_hash=payload_hash):
                server, _state, _recording = submit_coordinator()
                configure_temporary_audit_root(self, server)
                ledger = SingleWriterShareLedger()
                server.ledger = ledger
                durable_hash = "de" * 32
                invalid = {
                    "schema": "unsupported",
                    "block_hash_hex": durable_hash,
                    "block_hex": "00",
                }
                ledger.persist_block_candidate_intent(invalid)
                stored = ledger._block_candidate_outbox[durable_hash]["candidate"]
                if payload_hash is None:
                    stored.pop("block_hash_hex")
                else:
                    stored["block_hash_hex"] = payload_hash
                server.block_candidate_retry_delays = {durable_hash: 1.0}

                self.assertEqual(server.replay_pending_block_candidates(), 0)
                # Malformed-row cleanup is lower-priority maintenance, so it
                # cannot form an N x database-timeout convoy ahead of valid
                # recovered blocks on the node-offer lane.
                self.assertTrue(
                    server._run_one_invalid_block_candidate_quarantine()
                )

                self.assertEqual(ledger.pending_block_candidates(), [])
                self.assertNotIn(durable_hash, server.block_candidate_retry_delays)
                self.assertIn(
                    "qbit_prism_block_candidate_poisoned_total 1",
                    server.metrics_payload(),
                )

    def test_block_submitter_drops_candidate_when_pool_closed(self) -> None:
        server, state, ledger = submit_coordinator()
        server.accepted_block_count = 1
        server.max_blocks = 1
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dd" * 32,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_POOL_CLOSED], 1)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_POOL_CLOSED], 0)
        self.assertEqual(ledger.persisted, [])

    def test_block_worthy_share_is_credited_and_enqueued_before_block_submission(self) -> None:
        # The share ack must never wait on the block path: a block-worthy
        # share that met its target is credited immediately and the candidate
        # is queued for the submitter thread. Nothing submits synchronously
        # (the fixture RPC would raise on an unexpected submitblock call).
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertIsNone(ledger.pending[0].credit_policy)
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        queued = server.block_candidate_queue.get_nowait()
        self.assertIs(queued.submission, submission)
        self.assertFalse(queued.credit_share_on_accept)

    def test_block_candidate_submits_before_full_audit_persistence(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._ensure_tip_refresh_state()
        stale_bundle_key = ("stale-payout-bundle",)
        server._job_bundle_cache[stale_bundle_key] = object()  # type: ignore[assignment]
        active_fanout = _FanoutCancellation()
        server._active_tip_refresh = (  # type: ignore[assignment]
            SimpleNamespace(payout_state_generation=0),
            active_fanout,
        )
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "cc" * 32
            rpc = SubmitRpc(tip="00" * 32, block_hash=block_hash, ledger=ledger)
            server.rpc = rpc
            witness_tx = synthetic_witness_transaction("44")
            server.jobs["job-1"].job.transaction_hexes = (witness_tx,)
            build_kwargs: list[dict[str, object]] = []
            # Deliberately use insertion order that differs from this fixture's
            # stand-in canonical serialization. The alternate builder creates
            # the requested output but writes pretty, noncanonical JSON, so
            # existence alone must not make the path eligible for persistence.
            alternate_bundle = {
                "signed_coinbase_manifest": {
                    "manifest": {
                        "coinbase_tx_hex": "c0ffee",
                        "payout_count": 1,
                    }
                },
                "payout_policy_manifest": {"accounts": []},
                "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
                "found_block": {"coinbase_value_sats": 50_00000000},
            }
            canonical_bytes = json.dumps(
                alternate_bundle,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            canonical_sha256 = hashlib.sha256(canonical_bytes).hexdigest()

            def fake_build_audit_bundle(**kwargs: object) -> dict[str, object]:
                build_kwargs.append(kwargs)
                output_path = kwargs["canonical_output_path"]
                assert isinstance(output_path, Path)
                adopter = kwargs["canonical_output_adopter"]
                assert callable(adopter)
                with output_path.open("x+", encoding="utf-8") as output:
                    output.write(json.dumps(alternate_bundle, indent=2))
                    output.flush()
                    os.fsync(output.fileno())
                    adopter(output_path, os.fstat(output.fileno()))
                return alternate_bundle

            def fake_verify_bundle(bundle_path: Path, *_args: object, **_kwargs: object) -> dict[str, object]:
                candidate_bytes = bundle_path.read_bytes()
                self.assertEqual(json.loads(candidate_bytes), alternate_bundle)
                self.assertNotEqual(candidate_bytes, canonical_bytes)
                self.assertNotEqual(
                    hashlib.sha256(candidate_bytes).hexdigest(),
                    canonical_sha256,
                )
                return {
                    **verified_audit_report(),
                    "audit_bundle_sha256_hex": canonical_sha256,
                }

            persist_accepted_block = ledger.persist_accepted_block

            def persist_with_canonicalization(**kwargs: object) -> dict[str, object]:
                self.assertIsNone(kwargs["canonical_bundle_path"])
                self.assertEqual(kwargs["final_bundle"], alternate_bundle)
                report = kwargs["audit_report"]
                assert isinstance(report, dict)
                self.assertEqual(report["audit_bundle_sha256_hex"], canonical_sha256)
                return persist_accepted_block(**kwargs)

            server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
            server.verify_bundle = fake_verify_bundle  # type: ignore[method-assign]
            ledger.persist_accepted_block = persist_with_canonicalization  # type: ignore[method-assign]
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            pending = SimpleNamespace(share_id="miner-a:" + block_hash)

            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission, pending_share=pending)
            )
            self.assertTrue(accepted)
            self.assertTrue(
                server._ensure_shutdown_controller().writer_admission_closed()
            )

            live_files = sorted(Path(tempdir).glob("prism-live-audit-bundle-[0-9]*.json"))
            self.assertEqual(len(live_files), 1)
            self.assertEqual(list(Path(tempdir).glob("prism-live-audit-bundle-candidate-*.json")), [])
            self.assertEqual(list(Path(tempdir).glob(".prism-live-audit-bundle-candidate-*.tmp")), [])
            envelope = json.loads(live_files[0].read_text(encoding="utf-8"))
            self.assertEqual(envelope["schema"], "qbit.prism.live-audit-bundle-envelope.v1")
            self.assertEqual(envelope["block_hash"], block_hash)
            self.assertEqual(envelope["block_height"], 10)
            self.assertEqual(envelope["audit_bundle_sha256"], canonical_sha256)
            self.assertNotIn("signed_coinbase_manifest", envelope)
            self.assertEqual(
                Path(server.latest_evidence["audit_bundle_path"]),
                live_files[0].resolve(),
            )

        self.assertTrue(rpc.submitted)
        self.assertEqual(
            build_kwargs[0]["witness_merkle_leaves_hex"],
            direct_stratum.witness_merkle_leaves_hex((witness_tx,)),
        )
        self.assertEqual(
            build_kwargs[0]["coinbase_script_sig_suffix_hex"],
            server.coinbase_tag_hex + state.extranonce1_hex + "00" * 8,
        )
        self.assertEqual(ledger.persisted[0]["block_hash"], block_hash)
        self.assertEqual(ledger.persisted[0]["block_height"], 10)
        self.assertTrue(ledger.persisted[0]["submit_seen_at_persist"])
        # This alternate test builder wrote noncanonical bytes to the requested
        # path. The verified content is valid, but the path is never claimed as
        # a byte-canonical artifact for ledger persistence.
        self.assertIsNone(ledger.persisted[0]["canonical_bundle_path"])
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._job_bundle_cache, {})
        self.assertTrue(active_fanout.is_set())
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_issued_preview_is_invalidated_when_final_coinbase_mismatches(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        block_hash = "cf" * 32
        server.rpc = SubmitRpc(tip="00" * 32, block_hash=block_hash, ledger=ledger)
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle("deadbeef")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )
        candidate = block_candidate(server, state, submission)
        candidate.context.prospective_prior_balances = ()

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            self.assertFalse(server.submit_block_candidate(candidate))

        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._accepted_block_payout_previews, {})
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
        )

    def test_accepted_block_persistence_allows_delivery_but_serializes_reconciliation(
        self,
    ) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        persist_started = threading.Event()
        persist_finished = threading.Event()
        release_persist = threading.Event()
        confirm_started = threading.Event()
        release_delivery = threading.Event()
        reconcile_lock_attempted = threading.Event()
        reconcile_mutated = threading.Event()
        event_lock = threading.Lock()
        events: list[str] = []
        original_persist = ledger.persist_accepted_block
        original_confirm = ledger.confirm_accepted_block

        def note(name: str) -> None:
            with event_lock:
                events.append(name)

        def blocking_persist(**kwargs: object) -> dict[str, object]:
            note("persist-start")
            persist_started.set()
            if not release_persist.wait(15):
                raise AssertionError("timed out waiting to release accepted-block persistence")
            result = original_persist(**kwargs)
            note("persist-end")
            persist_finished.set()
            return result

        def observed_confirm(**kwargs: object) -> dict[str, object]:
            note("confirm")
            confirm_started.set()
            return original_confirm(**kwargs)

        ledger.reorg_watch_blocks = lambda *, active_tip_height: [  # type: ignore[method-assign]
            {
                "block_height": active_tip_height - 2,
                "block_hash": "aa" * 32,
                "chain_state": "confirmed",
            }
        ]

        def mark_pool_block_inactive(**_kwargs: object) -> dict[str, object]:
            note("reconcile-mutation")
            reconcile_mutated.set()
            return {"backend": "fake", "inactive_count": 1}

        ledger.persist_accepted_block = blocking_persist  # type: ignore[method-assign]
        ledger.confirm_accepted_block = observed_confirm  # type: ignore[method-assign]
        ledger.mark_pool_block_inactive = mark_pool_block_inactive  # type: ignore[method-assign]
        accepted: list[bool] = []
        reconcile_results: list[dict[str, object]] = []
        errors: list[BaseException] = []
        delivery_admitted = threading.Event()
        delivery_thread: threading.Thread | None = None
        reconcile_thread: threading.Thread | None = None
        mutation_lock = server._payout_balance_mutation_lock

        class ObservedBalanceLock:
            def __enter__(self) -> ObservedBalanceLock:
                if threading.current_thread() is reconcile_thread:
                    reconcile_lock_attempted.set()
                mutation_lock.acquire()
                return self

            def __exit__(self, *_args: object) -> None:
                mutation_lock.release()

            # The landing's audit-build release window drives the serializer
            # through the plain lock interface as well.
            def acquire(self, *args: object, **kwargs: object) -> bool:
                return mutation_lock.acquire(*args, **kwargs)  # type: ignore[arg-type]

            def release(self) -> None:
                mutation_lock.release()

        server._payout_balance_mutation_lock = ObservedBalanceLock()  # type: ignore[assignment]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "cd" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            candidate = block_candidate(server, state, submission)

            def submit() -> None:
                try:
                    accepted.append(server.submit_block_candidate(candidate))
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    errors.append(exc)

            def deliver() -> None:
                try:
                    with server._payout_state_delivery_gate.delivery():
                        note("replacement-delivery")
                        delivery_admitted.set()
                        if not release_delivery.wait(15):
                            raise AssertionError(
                                "timed out waiting to release payout delivery"
                            )
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    errors.append(exc)

            def reconcile() -> None:
                try:
                    reconcile_results.append(
                        server.reconcile_prism_pool_blocks_once(tip_hash=block_hash)
                    )
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    errors.append(exc)

            submit_thread = threading.Thread(target=submit)
            submit_thread.start()
            confirmation_waited_for_delivery = False
            try:
                self.assertTrue(persist_started.wait(5))
                server.reorg_reconciler_enabled = True
                reconcile_thread = threading.Thread(target=reconcile)
                reconcile_thread.start()
                self.assertTrue(reconcile_lock_attempted.wait(5))
                self.assertFalse(reconcile_mutated.is_set())
                delivery_thread = threading.Thread(target=deliver)
                delivery_thread.start()
                admitted_while_persisting = delivery_admitted.wait(5)
                if admitted_while_persisting:
                    self.assertFalse(reconcile_mutated.is_set())
                    release_persist.set()
                    self.assertTrue(persist_finished.wait(5))
                    confirmation_waited_for_delivery = not confirm_started.wait(0.1)
            finally:
                release_persist.set()
                release_delivery.set()
                submit_thread.join(10)
                if delivery_thread is not None:
                    delivery_thread.join(10)
                if reconcile_thread is not None:
                    reconcile_thread.join(10)

            self.assertFalse(submit_thread.is_alive())
            self.assertIsNotNone(delivery_thread)
            self.assertFalse(delivery_thread.is_alive())
            self.assertIsNotNone(reconcile_thread)
            self.assertFalse(reconcile_thread.is_alive())
            if errors:
                raise errors[0]
            self.assertTrue(admitted_while_persisting)
            # Confirmation is durable catch-up to the already-published
            # prospective state and therefore does not wait on delivery.
            self.assertFalse(confirmation_waited_for_delivery)
            self.assertTrue(confirm_started.is_set())
            self.assertTrue(reconcile_mutated.is_set())
            self.assertEqual(accepted, [True])
            self.assertEqual(len(reconcile_results), 1)
            self.assertEqual(reconcile_results[0]["inactive_blocks"], 1)
            self.assertEqual(server._payout_state_generation, 2)
            self.assertLess(events.index("persist-start"), events.index("replacement-delivery"))
            self.assertLess(events.index("replacement-delivery"), events.index("persist-end"))
            self.assertLess(events.index("persist-end"), events.index("confirm"))
            self.assertLess(events.index("confirm"), events.index("reconcile-mutation"))

    def test_next_tip_preview_job_lands_after_parent_persistence(self) -> None:
        old_tip = "00" * 32
        parent_hash = "cd" * 32
        child_hash = "ce" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server._ensure_job_cache_state()
        ledger.durable_payout_state = True
        server.reorg_reconciler_enabled = False
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        server.refresh_jobs_after_accepted_block = (  # type: ignore[method-assign]
            lambda **_kwargs: None
        )
        build_started = threading.Event()
        release_build = threading.Event()
        persist_started = threading.Event()
        release_persist = threading.Event()
        original_persist = ledger.persist_accepted_block
        original_confirm = ledger.confirm_accepted_block
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        def payout_bundle_payload() -> dict[str, object]:
            return {
                "found_block": {"coinbase_value_sats": 50_00000000},
                "ledger_window_attestation": {
                    "signature": {"public_key_hex": "aa" * 32}
                },
                "payout_policy_manifest": {
                    "accounts": [
                        {
                            "recipient_id": "miner-a",
                            "order_key": "miner-a",
                            "p2mr_program_hex": "11" * 32,
                            "gross_amount_sats": 25,
                            "prior_balance_sats": 0,
                            "candidate_balance_sats": 25,
                            "onchain_amount_sats": 0,
                            "carry_forward_balance_sats": 25,
                            "action": "accrued",
                        }
                    ]
                },
                "signed_coinbase_manifest": {
                    "manifest": {"coinbase_tx_hex": "c0ffee", "payout_count": 1}
                },
            }

        def blocking_payout_bundle(**_kwargs: object) -> dict[str, object]:
            if not build_started.is_set():
                build_started.set()
                if not release_build.wait(15):
                    raise AssertionError("timed out waiting to release parent bundle build")
            return payout_bundle_payload()

        def blocking_persist(**kwargs: object) -> dict[str, object]:
            if str(kwargs["block_hash"]).lower() == parent_hash:
                persist_started.set()
                if not release_persist.wait(15):
                    raise AssertionError("timed out waiting to release parent persistence")
            return original_persist(**kwargs)

        def expose_confirmed_preview(**kwargs: object) -> dict[str, object]:
            persisted = next(
                row
                for row in reversed(ledger.persisted)
                if str(row["block_hash"]).lower()
                == str(kwargs["block_hash"]).lower()
            )
            ledger.prior_balances = server._accepted_block_payout_preview_from_bundle(
                persisted["final_bundle"]  # type: ignore[arg-type]
            )
            return original_confirm(**kwargs)

        ledger.persist_accepted_block = blocking_persist  # type: ignore[method-assign]
        ledger.confirm_accepted_block = expose_confirmed_preview  # type: ignore[method-assign]

        class TwoBlockRpc:
            def __init__(self) -> None:
                self.tip = old_tip
                self.height = 9
                self.hashes = {9: old_tip}
                self.submitted: list[str] = []

            def call(self, method: str, params: object = None) -> object:
                if method == "getbestblockhash":
                    return self.tip
                if method == "getblockcount":
                    return self.height
                if method == "submitblock":
                    block_hex = str((params or [""])[0])  # type: ignore[index]
                    block_hash = parent_hash if block_hex == "00" else child_hash
                    self.height += 1
                    self.tip = block_hash
                    self.hashes[self.height] = block_hash
                    self.submitted.append(block_hash)
                    ledger.submit_seen = True
                    return None
                if method == "getblockhash":
                    return self.hashes[int((params or [0])[0])]  # type: ignore[index]
                raise RuntimeError(method)

        rpc = TwoBlockRpc()
        server.rpc = rpc
        server.build_audit_bundle = blocking_payout_bundle  # type: ignore[method-assign]
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **kwargs: verified_audit_report(
                block_height=int(kwargs["expected_block_height"])
            )
        )
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def build_child_job(client: ClientState, *, clean_jobs: bool) -> object:
            context = prism_context(
                "child-job",
                parent_hash,
                worker=client.worker,
                clean_jobs=clean_jobs,
            )
            context.template["height"] = 11
            context.prior_balances = server._prior_balances_for_job_parent(parent_hash)
            context.payout_state_generation = server._payout_state_generation
            return context

        server.build_job_for_client = build_child_job  # type: ignore[method-assign]
        parent_submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=parent_hash,
            block_hex="00",
        )
        parent_candidate = block_candidate(server, state, parent_submission)
        parent_candidate.context.prospective_prior_balances = (
            server._serialize_prior_balance_preview(preview)
        )
        parent_results: list[bool] = []
        parent_errors: list[BaseException] = []

        def submit_parent() -> None:
            try:
                parent_results.append(server.submit_block_candidate(parent_candidate))
            except BaseException as exc:  # noqa: BLE001 - asserted below
                parent_errors.append(exc)

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            parent_thread = threading.Thread(target=submit_parent)
            parent_thread.start()
            try:
                self.assertTrue(build_started.wait(5))
                self.assertEqual(ledger.current_prior_balances(), [])
                self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
                child_context = state.active_job
                self.assertIsNotNone(child_context)
                assert child_context is not None
                self.assertTrue(parent_thread.is_alive())
                self.assertEqual(child_context.prior_balances, preview)
                self.assertEqual(
                    child_context.payout_state_generation,
                    server._payout_state_generation,
                )
                preview_generation = server._payout_state_generation
                self.assertEqual(sent[-1]["method"], "mining.notify")
                self.assertTrue(sent[-1]["params"][8])  # type: ignore[index]

                child_submission = SimpleNamespace(
                    header_hex="bb" * 80,
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=child_hash,
                    block_hex="01",
                    share_pass=True,
                    block_pass=True,
                )
                with patch(
                    "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                    return_value=child_submission,
                ):
                    self.assertFalse(
                        server.handle_submit(
                            state,
                            ["miner-a", "child-job", "00" * 8, "00000001", "00000002"],
                        )
                    )
                self.assertEqual(server.block_candidate_queue.qsize(), 1)
                release_build.set()
                self.assertTrue(persist_started.wait(5))
                self.assertTrue(parent_thread.is_alive())
            finally:
                release_build.set()
                release_persist.set()
                parent_thread.join(10)

            self.assertFalse(parent_thread.is_alive())
            if parent_errors:
                raise parent_errors[0]
            self.assertEqual(parent_results, [True])
            self.assertEqual(ledger.current_prior_balances(), preview)
            self.assertEqual(server._payout_state_generation, preview_generation)
            self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(rpc.submitted, [parent_hash, child_hash])
        self.assertEqual(server.accepted_block_count, 2)
        self.assertEqual(len(ledger.persisted), 2)
        self.assertEqual(len(ledger.confirmed), 2)
        self.assertEqual(
            server.block_candidate_abandoned_counts.get(PRISM_REJECTION_STALE_JOB, 0),
            0,
        )
        self.assertEqual(server._accepted_block_payout_previews, {})

    def test_direct_block_preparation_does_not_hold_delivery_gate(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        entered = threading.Event()
        release = threading.Event()
        accepted: list[bool] = []
        errors: list[BaseException] = []
        block_hash = "cf" * 32
        original_persist = ledger.persist_accepted_block

        def blocking_persist(**kwargs: object) -> dict[str, object]:
            self.assertIsNone(
                server._payout_state_delivery_gate._mutation_owner
            )
            entered.set()
            if not release.wait(5):
                raise AssertionError("test did not release direct-block preparation")
            return original_persist(**kwargs)

        ledger.persist_accepted_block = blocking_persist  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            def submit() -> None:
                try:
                    accepted.append(
                        server.submit_block_candidate(
                            block_candidate(server, state, submission)
                        )
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            thread = threading.Thread(target=submit)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                with server._payout_state_delivery_gate.delivery_cancelable(
                    lambda: False,
                    generation=server._payout_state_generation,
                    priority=True,
                ) as admission:
                    self.assertTrue(admission)
                    release.set()
                    thread.join(5)
                    self.assertFalse(thread.is_alive())
                    self.assertTrue(
                        server._payout_state_prepare_lock.acquire(timeout=1)
                    )
                    server._payout_state_prepare_lock.release()
                    admission.mark_delivered()
            finally:
                release.set()
                thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(accepted, [True])
        self.assertEqual(server._payout_state_generation, 1)

    def test_failed_direct_block_audit_does_not_reserve_payout_source(self) -> None:
        for failure_phase in ("build", "verify"):
            with self.subTest(failure_phase=failure_phase):
                server, state, ledger = submit_coordinator()
                server._ensure_job_cache_state()
                initial_source = server._payout_state_source
                initial_published = server._published_payout_state
                block_hash = "ce" * 32
                server.rpc = SubmitRpc(
                    tip="00" * 32,
                    block_hash=block_hash,
                    ledger=ledger,
                )
                submission = SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                )

                with tempfile.TemporaryDirectory() as tempdir:
                    server.audit_dir = Path(tempdir)
                    server.ledger_writer_public_key_hex = "aa" * 32
                    if failure_phase == "build":
                        server.build_audit_bundle = (  # type: ignore[method-assign]
                            lambda **_kwargs: (_ for _ in ()).throw(
                                RuntimeError("audit reconstruction failed")
                            )
                        )
                    else:
                        server.build_audit_bundle = (  # type: ignore[method-assign]
                            lambda **_kwargs: verified_block_bundle()
                        )
                        server.verify_bundle = (  # type: ignore[method-assign]
                            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                                RuntimeError("audit verification failed")
                            )
                        )

                    with self.assertRaisesRegex(
                        RuntimeError,
                        "audit (reconstruction|verification) failed",
                    ):
                        server.submit_block_candidate(
                            block_candidate(server, state, submission)
                        )

                self.assertEqual(server._payout_state_source, initial_source)
                self.assertEqual(server._published_payout_state, initial_published)
                self.assertEqual(server._payout_state_generation, 0)
                self.assertEqual(ledger.persisted, [])

    def test_uncertain_direct_block_ledger_commit_fences_delivery(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        block_hash = "cd" * 32
        server.rpc = SubmitRpc(
            tip="00" * 32,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        ledger.confirm_accepted_block = (  # type: ignore[method-assign]
            lambda **_kwargs: (_ for _ in ()).throw(
                RuntimeError("ledger confirmation unavailable")
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.ledger_writer_public_key_hex = "aa" * 32
            with self.assertRaisesRegex(
                RuntimeError,
                "ledger confirmation unavailable",
            ):
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )

        self.assertEqual(len(ledger.persisted), 1)
        # The prospective preview was source/generation 1; the uncertain
        # durable commit supersedes it with fenced source 2.
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._payout_state_source[0], 2)
        self.assertEqual(server._published_payout_state.source_generation, 1)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)

    def test_uncertain_commit_supersedes_concurrently_published_source(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        block_hash = "cb" * 32
        newer_tip = "dc" * 32
        server.rpc = SubmitRpc(
            tip="00" * 32,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )

        def publish_newer_source_then_fail(**_kwargs: object) -> dict[str, object]:
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=newer_tip,
            )
            self.assertEqual(
                server._publish_payout_state_candidate(
                    server._current_payout_state_candidate()
                ),
                2,
            )
            raise RuntimeError("ledger confirmation unavailable")

        ledger.confirm_accepted_block = publish_newer_source_then_fail  # type: ignore[method-assign]
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.ledger_writer_public_key_hex = "aa" * 32
            with self.assertRaisesRegex(
                RuntimeError,
                "ledger confirmation unavailable",
            ):
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )

        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._published_payout_state.source_generation, 2)
        self.assertEqual(server._payout_state_source[0], 3)
        self.assertEqual(server._payout_state_source[1], newer_tip)
        self.assertEqual(
            server._payout_state_source[2],
            "direct_block_uncertain",
        )
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)

    def test_post_confirm_publication_loss_completes_candidate_and_fences(self) -> None:
        # Once persist + confirm are durable, losing the forced payout
        # publication must not abort the candidate: the outbox row is marked
        # submitted and the success tail runs, while delivery stays fenced
        # until the scheduled refresh publishes the newest source.
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "e1" * 32
        server.rpc = SubmitRpc(
            tip="00" * 32,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        submitted: list[dict[str, object]] = []
        ledger.mark_block_candidate_submitted = (  # type: ignore[attr-defined]
            lambda **kwargs: submitted.append(kwargs) or True
        )
        real_confirm = ledger.confirm_accepted_block

        def confirm_then_reserve_newer_source(**kwargs: object) -> dict[str, object]:
            result = real_confirm(**kwargs)
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash="ee" * 32,
            )
            return result

        ledger.confirm_accepted_block = confirm_then_reserve_newer_source  # type: ignore[method-assign]
        server._publish_current_payout_state_with_retry_budget = (  # type: ignore[method-assign]
            lambda **_kwargs: None
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            self.assertTrue(
                server._submit_next_block_candidate_writer(
                    block_candidate(server, state, submission)
                )
            )

        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        self.assertEqual(len(ledger.confirmed), 1)
        self.assertEqual(len(submitted), 1)
        self.assertEqual(submitted[0]["block_hash"], block_hash)
        self.assertEqual(server.accepted_block_count, 1)
        self.assertIn(block_hash, server._accounted_accepted_block_hashes)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())

        # The scheduled refresh publishes the pending source and reopens
        # delivery without any candidate replay.
        del server._publish_current_payout_state_with_retry_budget
        self.assertIsNotNone(server._publish_current_payout_state_with_retry_budget())
        self.assertFalse(server._payout_state_publication_blocked)
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)

    def test_idempotent_direct_block_replay_skips_publication(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "d0" * 32
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.rpc = rpc
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )
            self.assertEqual(server._payout_state_generation, 1)
            self.assertEqual(
                server._published_payout_state.source_tip_hash,
                block_hash,
            )
            self.assertEqual(server._payout_state_source[0], 1)

            # Replay the durable candidate after its block landed, its
            # confirmation committed, and the network built on top of it.
            # Exact confirmed replay reports confirmed_count=1 at the block's
            # own expected height even though the active chain tip is now a
            # child. The published payout state already covers the
            # candidate, so the replay must not reserve a source, bump the
            # generation, wipe the job-bundle cache, or schedule refresh
            # churn.
            child_tip = "d7" * 32

            class AncestorReplayRpc:
                def call(self, method: str, params: object = None) -> object:
                    if method == "getbestblockhash":
                        return child_tip
                    if method == "getblockheader":
                        if params != [block_hash]:
                            raise AssertionError(params)
                        return {"height": 10, "confirmations": 2}
                    if method == "getblockhash":
                        if params != [10]:
                            raise AssertionError(params)
                        return block_hash
                    if method == "getblockcount":
                        return 11
                    if method == "submitblock":
                        raise AssertionError(
                            "active ancestor must not be resubmitted"
                        )
                    raise RuntimeError(method)

            server.rpc = AncestorReplayRpc()
            ledger.pool_block_state = (  # type: ignore[attr-defined]
                lambda **_kwargs: {
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            )
            def confirm_exact_ancestor(**kwargs: object) -> dict[str, object]:
                self.assertEqual(kwargs["block_hash"], block_hash)
                self.assertEqual(kwargs["active_tip_height"], 10)
                return {"backend": "fake", "confirmed_count": 1}

            ledger.confirm_accepted_block = confirm_exact_ancestor  # type: ignore[method-assign]
            retry_calls = 0

            def count_retry() -> None:
                nonlocal retry_calls
                retry_calls += 1

            server._schedule_tip_refresh_retry = count_retry  # type: ignore[method-assign]
            cache_key = ("sentinel",)
            server._job_bundle_cache[cache_key] = object()
            discarded_before = server.payout_state_candidates_discarded
            pending_marks_before = server._tip_refresh_pending_counter

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )

        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._published_payout_state.source_generation, 1)
        self.assertEqual(server._published_payout_state.source_tip_hash, block_hash)
        self.assertEqual(server._payout_state_source[0], 1)
        self.assertIn(cache_key, server._job_bundle_cache)
        self.assertEqual(retry_calls, 0)
        self.assertEqual(
            server._tip_refresh_pending_counter,
            pending_marks_before,
        )
        self.assertEqual(
            server.payout_state_candidates_discarded,
            discarded_before,
        )
        self.assertFalse(server._payout_state_publication_blocked)
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)

    def test_leaked_publication_fence_replay_republishes(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "d8" * 32
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )
            self.assertEqual(server._payout_state_generation, 1)

            # Simulate the exception tail a replay must heal: a prior attempt
            # force-blocked delivery while its source generation already
            # matched the published source, then failed before republishing.
            # The leaked fence blocks every job build until a publication
            # lands, so an exact confirmed replay must heal the leaked fence
            # before taking the covered-state skip.
            server._block_payout_state_publication(force=True)
            self.assertTrue(server._payout_state_publication_blocked)
            self.assertEqual(
                server._payout_state_source[0],
                server._published_payout_state.source_generation,
            )
            child_tip = "d9" * 32

            class AncestorReplayRpc:
                def call(self, method: str, params: object = None) -> object:
                    if method == "getbestblockhash":
                        return child_tip
                    if method == "getblockheader":
                        if params != [block_hash]:
                            raise AssertionError(params)
                        return {"height": 10, "confirmations": 2}
                    if method == "getblockhash":
                        if params != [10]:
                            raise AssertionError(params)
                        return block_hash
                    if method == "getblockcount":
                        return 11
                    if method == "submitblock":
                        raise AssertionError(
                            "active ancestor must not be resubmitted"
                        )
                    raise RuntimeError(method)

            server.rpc = AncestorReplayRpc()
            ledger.pool_block_state = (  # type: ignore[attr-defined]
                lambda **_kwargs: {
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            )
            def confirm_exact_ancestor(**kwargs: object) -> dict[str, object]:
                self.assertEqual(kwargs["block_hash"], block_hash)
                self.assertEqual(kwargs["active_tip_height"], 10)
                return {"backend": "fake", "confirmed_count": 1}

            ledger.confirm_accepted_block = confirm_exact_ancestor  # type: ignore[method-assign]

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )

        self.assertEqual(server._payout_state_generation, 2)
        # Fence healing republishes identical covered state. It advances the
        # delivery generation without inventing a logical invalidation source.
        self.assertEqual(server._published_payout_state.source_generation, 1)
        self.assertEqual(server._payout_state_source[0], 1)
        self.assertFalse(server._payout_state_publication_blocked)
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)

    def test_direct_block_disabled_reconciler_bounds_publish_supersession(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.payout_reconcile_supersession_retries = 2
        block_hash = "d3" * 32
        real_publish = server._publish_payout_state_candidate
        publish_attempts = 0

        def supersede_before_publish(candidate: object) -> int | None:
            nonlocal publish_attempts
            publish_attempts += 1
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=f"{publish_attempts + 20:064x}",
            )
            return real_publish(candidate)  # type: ignore[arg-type]

        server._publish_payout_state_candidate = supersede_before_publish  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertTrue(accepted)
        self.assertEqual(publish_attempts, 3)
        self.assertEqual(server._payout_state_generation, 0)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())

    def test_untrusted_direct_block_reconcile_publishes_newer_source_once(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.reorg_reconciler_enabled = True
        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            lambda _tip, *, _coalesce_same_tip: not _coalesce_same_tip
        )
        server.qbit_chain_view_untrusted = lambda: True  # type: ignore[method-assign]
        block_hash = "d1" * 32
        newer_tip = "d2" * 32
        real_confirm = ledger.confirm_accepted_block

        def superseding_noop_confirmation(**kwargs: object) -> dict[str, object]:
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=newer_tip,
            )
            return real_confirm(**kwargs)

        ledger.confirm_accepted_block = superseding_noop_confirmation  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertTrue(accepted)
        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._published_payout_state.source_tip_hash, newer_tip)
        self.assertEqual(server.payout_state_candidates_discarded, 0)
        self.assertEqual(server.reorg_reconcile_skip_count, 1)

    def _superseded_confirm_coordinator(
        self,
        confirmed_count: int,
    ) -> tuple[PrismCoordinator, object, object, str, list[object]]:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        block_hash = "d3" * 32
        server.rpc = SubmitRpc(
            tip="00" * 32,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        ledger.confirm_accepted_block = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "backend": "fake",
                "confirmed_count": confirmed_count,
            }
        )
        shutdown_requests: list[object] = []
        real_shutdown = server.request_shutdown

        def observed_shutdown(signum: int | None = None) -> None:
            shutdown_requests.append(signum)
            real_shutdown(signum)

        server.request_shutdown = observed_shutdown  # type: ignore[method-assign]
        return server, state, ledger, block_hash, shutdown_requests

    def test_superseded_ledger_confirmation_abandons_without_shutdown(self) -> None:
        # A confirm disposition of -1 means the row was terminally disposed
        # (reorg quarantine, rejection, or reversal) before the confirmation
        # landed: terminal for the candidate, benign for the pool. The
        # coordinator must abandon the candidate and keep serving instead of
        # converting the routine race into a full-pool outage.
        server, state, ledger, block_hash, shutdown_requests = (
            self._superseded_confirm_coordinator(-1)
        )
        # The definitive submitblock ack normally defers terminal
        # abandonment; age that evidence out immediately to model the
        # settled post-reorg view in which the chain no longer carries the
        # candidate hash.
        server.observed_tip_accept_window_seconds = 1e-9
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertFalse(accepted)
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED,
        )
        self.assertEqual(shutdown_requests, [])
        self.assertFalse(server.stop_event.is_set())
        self.assertEqual(server.accepted_block_count, 0)
        # The prospective preview was withdrawn with publication
        # invalidation, exactly as on the fail-closed path.
        transition = server._accepted_block_payout_previews.get(block_hash)
        self.assertTrue(transition is None or transition.preview is None)

    def test_superseded_confirmation_with_fresh_acceptance_evidence_defers(
        self,
    ) -> None:
        # While first-party acceptance evidence (the definitive submitblock
        # ack) is still fresh, even a superseded disposition must not commit
        # a terminal abandonment: the reorg reconciler may still reactivate
        # the row. The candidate defers -- and the pool keeps serving.
        server, state, _ledger, block_hash, shutdown_requests = (
            self._superseded_confirm_coordinator(-1)
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertFalse(accepted)
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
        )
        self.assertEqual(shutdown_requests, [])
        self.assertFalse(server.stop_event.is_set())

    def test_unexplained_zero_ledger_confirmation_still_requests_shutdown(
        self,
    ) -> None:
        # A 0 with no superseding row remains unexplained: the fail-closed
        # shutdown escalation is preserved exactly as before the disposition
        # split.
        server, state, _ledger, block_hash, shutdown_requests = (
            self._superseded_confirm_coordinator(0)
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertFalse(accepted)
        self.assertEqual(len(shutdown_requests), 1)
        self.assertTrue(server.stop_event.is_set())
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        )

    def test_verified_canonical_bundle_path_requires_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            candidate_path = Path(tempdir) / "candidate.json"
            candidate_bytes = b'{"canonical":true}'
            candidate_path.write_bytes(candidate_bytes)
            canonical_sha256 = hashlib.sha256(candidate_bytes).hexdigest()

            self.assertEqual(
                PrismCoordinator.verified_canonical_bundle_path(
                    candidate_path,
                    {"audit_bundle_sha256_hex": canonical_sha256.upper()},
                ),
                candidate_path,
            )
            self.assertIsNone(
                PrismCoordinator.verified_canonical_bundle_path(
                    candidate_path,
                    {"audit_bundle_sha256_hex": "00" * 32},
                )
            )

    def test_active_ancestor_candidate_resumes_full_finalization_without_resubmit(self) -> None:
        server, state, ledger = submit_coordinator()
        server.max_blocks = 10
        server.stop_after_block = False
        refreshes: list[str] = []
        server.refresh_jobs_after_accepted_block = (  # type: ignore[method-assign]
            lambda **kwargs: refreshes.append(str(kwargs["block_hash"]))
        )
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "ac" * 32

            class ActiveAncestorRpc:
                def __init__(self) -> None:
                    self.revalidated_heights: list[int] = []

                def call(self, method: str, params: object = None) -> object:
                    if method == "getbestblockhash":
                        return "ef" * 32
                    if method == "getblockheader":
                        self.assert_candidate(params)
                        return {"height": 10, "confirmations": 2}
                    if method == "getblockcount":
                        return 11
                    if method == "getblockhash":
                        if params != [10]:
                            raise AssertionError(params)
                        self.revalidated_heights.append(10)
                        return block_hash
                    if method == "submitblock":
                        raise AssertionError("active ancestor must not be resubmitted")
                    raise RuntimeError(method)

                @staticmethod
                def assert_candidate(params: object) -> None:
                    if params != [block_hash]:
                        raise AssertionError(params)

            active_rpc = ActiveAncestorRpc()
            server.rpc = active_rpc
            server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
                lambda _tip, *, _coalesce_same_tip: not _coalesce_same_tip
            )
            server.build_audit_bundle = lambda **_kwargs: {  # type: ignore[method-assign]
                "found_block": {"coinbase_value_sats": 50_00000000},
                "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
                "payout_policy_manifest": {"accounts": []},
                "signed_coinbase_manifest": {
                    "manifest": {"coinbase_tx_hex": "c0ffee", "payout_count": 1}
                },
            }
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            candidate = block_candidate(server, state, submission)

            self.assertTrue(server.submit_block_candidate(candidate))
            # Direct finalization records the refresh for its caller so slow
            # job fanout happens after writer admission has been released.
            self.assertEqual(
                state.post_accept_refresh_block,
                (10, block_hash),
            )
            server.refresh_jobs_after_pending_accepted_block(
                state,
                heartbeat_name="block_submitter",
            )
            # A failed outbox terminal update can replay A after later block B
            # changes global balances. Validate A against its height-bounded
            # active-chain view and suppress duplicate process-local accounting.
            ledger.durable_payout_state = True
            ledger.prior_balances = [
                {
                    "recipient_id": "later-miner",
                    "order_key": "later-miner",
                    "p2mr_program_hex": "22" * 32,
                    "balance_sats": 50,
                }
            ]
            ledger.pool_block_state = lambda **_kwargs: {  # type: ignore[attr-defined]
                "block_hash": block_hash,
                "block_height": 10,
                "parent_hash": "00" * 32,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
            ledger.prior_balances_after_pool_block = (  # type: ignore[attr-defined]
                lambda **_kwargs: []
            )
            self.assertTrue(server.submit_block_candidate(candidate))
            latest_evidence = server.latest_evidence

        self.assertEqual([row["block_hash"] for row in ledger.persisted], [block_hash] * 2)
        # This compatibility builder ignores canonical_output_path, so its
        # Python fallback remains verifier-only.
        self.assertIsNone(ledger.persisted[0]["canonical_bundle_path"])
        self.assertEqual(ledger.confirmed[0]["active_tip_height"], 10)
        self.assertEqual(ledger.confirmed[0]["block_hash"], block_hash)
        self.assertEqual(active_rpc.revalidated_heights, [10, 10])
        self.assertFalse(ledger.confirmed[0]["submit_seen_at_confirm"])
        # The share credit happens on the client thread at submit time now;
        # the block path itself appends nothing.
        self.assertEqual(len(ledger.pending), 0)
        self.assertFalse(server.stop_event.is_set())
        self.assertEqual(server.accepted_block_count, 1)
        self.assertEqual(refreshes, [block_hash])
        self.assertIsNotNone(latest_evidence)
        assert latest_evidence is not None
        self.assertEqual(latest_evidence["persistence"]["block_count"], 1)
        self.assertEqual(latest_evidence["confirmation"]["confirmed_count"], 1)
        # Evidence carries an aggregate miner count, not a materialized list of
        # every miner id (which scanned the whole ledger twice under the lock).
        self.assertEqual(latest_evidence["accepted_share_count"], 0)
        self.assertEqual(latest_evidence["distinct_miner_count"], 0)
        self.assertNotIn("distinct_miners", latest_evidence)

    def test_audit_retention_prunes_only_live_and_candidate_files(self) -> None:
        server = coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.audit_live_bundle_retention = 2
            server.audit_candidate_retention_seconds = 0
            for index in range(4):
                path = Path(tempdir) / f"prism-live-audit-bundle-{index + 1}-{'aa' * 32}.json"
                path.write_text("{}", encoding="utf-8")
                os.utime(path, (100 + index, 100 + index))
            candidate = Path(tempdir) / f"prism-live-audit-bundle-candidate-{'bb' * 32}.json"
            candidate.write_text("{}", encoding="utf-8")
            temp_candidate = Path(tempdir) / f".prism-live-audit-bundle-candidate-{'bb' * 32}.json.tmp"
            temp_candidate.write_text("{}", encoding="utf-8")
            body = Path(tempdir) / f"prism-audit-bundle-body-{'cc' * 32}-{'dd' * 32}.json"
            body.write_text("{}", encoding="utf-8")
            segment = Path(tempdir) / f"prism-audit-share-segment-1-2-{'ee' * 32}.json"
            segment.write_text("{}", encoding="utf-8")

            # Live deletion is fail-closed unless durable evidence has supplied
            # publication authority. This facade test supplies that authority
            # explicitly while keeping its hand-written retention fixtures.
            store = server._ensure_audit_artifact_store()
            store._compatibility_evidence_override = True
            store._evidence_state = "valid"

            server.prune_audit_artifacts()

            live_names = sorted(path.name for path in Path(tempdir).glob("prism-live-audit-bundle-[0-9]*.json"))
            self.assertEqual(
                live_names,
                [
                    f"prism-live-audit-bundle-3-{'aa' * 32}.json",
                    f"prism-live-audit-bundle-4-{'aa' * 32}.json",
                ],
            )
            self.assertFalse(candidate.exists())
            self.assertFalse(temp_candidate.exists())
            self.assertTrue(body.exists())
            self.assertTrue(segment.exists())

    def test_audit_retention_zero_preserves_current_live_envelope(self) -> None:
        server = coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.audit_live_bundle_retention = 0
            old = Path(tempdir) / f"prism-live-audit-bundle-1-{'aa' * 32}.json"
            old.write_text("{}", encoding="utf-8")
            current = Path(tempdir) / f"prism-live-audit-bundle-2-{'bb' * 32}.json"
            current.write_text("{}", encoding="utf-8")

            store = server._ensure_audit_artifact_store()
            store._compatibility_evidence_override = True
            store._evidence_state = "valid"
            store._current_envelope = current.resolve()

            server.prune_audit_artifacts(keep_live_path=current)

            self.assertFalse(old.exists())
            self.assertTrue(current.exists())

    def test_accepted_direct_block_refreshes_clean_job_after_submit_response(self) -> None:
        old_tip = "00" * 32
        block_hash = "ab" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        old_context = server.jobs["job-1"]
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIs(client, state)
            self.assertNotIn(
                "accepted_block_handling",
                server._ensure_shutdown_controller().snapshot()["active_writers"],
            )
            return prism_context(
                "fresh-job",
                block_hash,
                worker=state.worker,
                difficulty=server.desired_client_share_difficulty(client),
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitAcceptingTemplateRpc(old_tip=old_tip, block_hash=block_hash, ledger=ledger)
            server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
            submission = SimpleNamespace(
                header_hex="aa" * 80,
                share_pass=True,
                block_pass=True,
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_request(
                    state,
                    {
                        "id": "submit-1",
                        "method": "mining.submit",
                        "params": ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    },
                )
                # The ack goes out before the block path runs; draining the
                # submitter queue lands the block and pushes fresh work.
                self.assertEqual(sent, [{"id": "submit-1", "result": True, "error": None}])
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(server.poll_qbit_tip_template_once(), 1)

        self.assertEqual(sent[0], {"id": "submit-1", "result": True, "error": None})
        self.assertEqual([payload.get("method") for payload in sent[1:]], ["mining.set_difficulty", "mining.notify"])
        self.assertEqual(sent[2]["params"][0], "fresh-job")
        self.assertTrue(sent[2]["params"][8])
        self.assertEqual(server.tip_refresh_job_count, 1)
        self.assertEqual(server.post_accept_refresh_failure_count, 0)
        self.assertEqual(server.accepted_block_count, 1)
        self.assertNotIn("job-1", server.jobs)
        self.assertIn("fresh-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"fresh-job"})
        self.assertIn(state, server.clients)
        server.stale_grace_seconds = 0

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )
        self.assertEqual(raised.exception.code, 21)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)

        fresh_submission = SimpleNamespace(
            header_hex="bb" * 80,
            block_hash_hex="bc" * 32,
            share_pass=True,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=fresh_submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "fresh-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(ledger.pending[-1].job_id, "fresh-job")

    def test_post_accept_notification_does_not_run_failing_template_build(self) -> None:
        old_tip = "00" * 32
        block_hash = "ad" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def unexpected_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise AssertionError("job build should not run when template refresh fails")

        server.build_job_for_client = unexpected_build  # type: ignore[method-assign]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitAcceptingTemplateRpc(
                old_tip=old_tip,
                block_hash=block_hash,
                fail_template_after_submit=True,
                ledger=ledger,
            )
            server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
            submission = SimpleNamespace(
                header_hex="aa" * 80,
                share_pass=True,
                block_pass=True,
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_request(
                    state,
                    {
                        "id": "submit-1",
                        "method": "mining.submit",
                        "params": ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    },
                )
                self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(sent, [{"id": "submit-1", "result": True, "error": None}])
        self.assertEqual(server.accepted_block_count, 1)
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(server.tip_refresh_job_count, 0)
        self.assertEqual(server.post_accept_refresh_failure_count, 0)
        self.assertEqual(state.active_job_ids, {"job-1"})
        self.assertIn("job-1", server.jobs)
        self.assertTrue(server.tip_refresh_is_pending())
        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertIn("qbit_prism_post_accept_refresh_failures_total 0", server.metrics_payload())

    def test_post_accept_refresh_preserves_pending_vardiff_difficulty_pair(self) -> None:
        old_tip = "00" * 32
        block_hash = "ae" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.vardiff_config = vardiff.VardiffConfig(
            enabled=True,
            target_share_interval_seconds=Decimal("15"),
            min_difficulty=Decimal("1"),
            max_difficulty=Decimal("1024"),
            retarget_interval_seconds=Decimal("90"),
            max_step_factor=Decimal("4"),
            startup_difficulty=Decimal("1"),
            max_step_down_factor=Decimal("4"),
            ewma_alpha=Decimal("1"),
            retarget_tolerance=Decimal("0"),
        )
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        state.share_difficulty = Decimal("1")
        state.pending_share_difficulty = Decimal("8")
        state.vardiff_window_started_monotonic = time.monotonic()
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            return prism_context(
                "fresh-vardiff-job",
                block_hash,
                worker=state.worker,
                difficulty=server.desired_client_share_difficulty(client),
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitAcceptingTemplateRpc(old_tip=old_tip, block_hash=block_hash, ledger=ledger)
            server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
            submission = SimpleNamespace(
                header_hex="aa" * 80,
                share_pass=True,
                block_pass=True,
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_request(
                    state,
                    {
                        "id": "submit-1",
                        "method": "mining.submit",
                        "params": ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    },
                )
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(server.poll_qbit_tip_template_once(), 1)

        self.assertEqual(sent[0], {"id": "submit-1", "result": True, "error": None})
        self.assertEqual(sent[1]["method"], "mining.set_difficulty")
        self.assertEqual(sent[1]["params"], [8.0])
        self.assertEqual(sent[2]["method"], "mining.notify")
        self.assertEqual(sent[2]["params"][0], "fresh-vardiff-job")
        self.assertTrue(sent[2]["params"][8])
        self.assertEqual(state.share_difficulty, Decimal("8"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.vardiff_window_submitted, 1)
        self.assertEqual(state.vardiff_window_accepted, 1)

    def test_rejected_candidate_never_creates_prepared_payout_state(self) -> None:
        server, state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "dd" * 32
            server.rpc = SubmitRpc(tip="00" * 32, block_hash=block_hash, submit_result="rejected")
            server.build_audit_bundle = lambda **_kwargs: {  # type: ignore[method-assign]
                "found_block": {"coinbase_value_sats": 50_00000000},
                "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
                "payout_policy_manifest": {"accounts": []},
                "signed_coinbase_manifest": {
                    "manifest": {
                        "coinbase_tx_hex": "c0ffee",
                        "payout_count": 1,
                    }
                },
            }
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(ledger.rejected, [])
        self.assertEqual(ledger.reversed, [])
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_SUBMITBLOCK_REJECTED], 1
        )

    def test_build_audit_bundle_passes_coinbase_output_policy_to_cli_payload(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="99")
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        with patch.dict(
            os.environ,
            {
                "PRISM_COINBASE_OUTPUT_POLICY": "pool-fee-first",
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
            server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
            )

        payout_policy = captured["payload"]["payout_policy"]
        self.assertEqual(payout_policy["coinbase_output_policy"], "pool-fee-first")
        self.assertEqual(payout_policy["pool_fee_policy"]["fee_bps"], 125)

    def test_ledger_artifact_not_built_when_publication_not_required(self) -> None:
        tip = "5b" * 32
        ledger = ReorgLedger([])
        server, build_calls = self._reconcile_coordinator_with_artifact_counter(
            ledger,
            ReorgRpc(
                tip=tip,
                template=gbt_template(tip, height=11),
                height=10,
                block_hashes={10: tip},
            ),
        )

        first = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(first["published_generation"], 1)
        self.assertEqual(len(build_calls), 1)
        self.assertFalse(build_calls[0][2])

        second = server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        self.assertIsNone(second["published_generation"])
        self.assertEqual(
            len(build_calls),
            1,
            "a pass without publication need must not build the ledger artifact",
        )
        self.assertEqual(
            ledger.events,
            [("watch", 10), ("mature", 10), ("watch", 10), ("mature", 10)],
        )

    def test_payout_change_forces_publication_and_builds_artifact(self) -> None:
        tip = "5d" * 32
        pool_block_hash = "ae" * 32
        ledger = ReorgLedger([])
        server, build_calls = self._reconcile_coordinator_with_artifact_counter(
            ledger,
            ReorgRpc(
                tip=tip,
                template=gbt_template(tip, height=11),
                height=10,
                block_hashes={10: tip},
            ),
        )

        first = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(first["published_generation"], 1)
        self.assertEqual(len(build_calls), 1)

        # An orphaned confirmed block appears without any new payout source:
        # payout_changed alone must still force a publication.
        ledger.rows.append(
            {
                "block_hash": pool_block_hash,
                "block_height": 12,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
        )

        second = server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        self.assertEqual(second["inactive_blocks"], 1)
        self.assertEqual(second["published_generation"], 2)
        self.assertEqual(len(build_calls), 2)
        self.assertTrue(build_calls[1][2])
        self.assertEqual(ledger.rows[0]["chain_state"], "inactive")
        self.assertIn(("inactive", pool_block_hash, 10), ledger.events)

    def test_block_submit_lands_as_issued_after_late_append_epoch_invalidation(self) -> None:
        # A late-visible replay append advances the live epoch and schedules
        # the refresh wave asynchronously; until that wave retires the job,
        # membership admission still lets the pre-append job submit. The
        # fast lane offers the solve to qbitd without ledger
        # synchronization, so once the node accepts it the landing keeps
        # the as-issued payout snapshot for accounting (the replayed share
        # rides the next window) instead of terminally abandoning an
        # accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "ea" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ea" * 32,
            block_hex="00",
        )
        pending = self._pending_append("append-epoch-stale").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        # The share was already accepted at submit time, so a lost block is a
        # block-abandonment, not a stale share rejection.
        self.assertEqual(server.stale_share_count, 0)
        # The node accepted the pre-append solve: accounting keeps the
        # as-issued snapshot instead of terminally abandoning it.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="append_epoch_stale"} 0',
            metrics,
        )

    def test_replayed_block_candidate_is_exempt_from_append_epoch_fence(self) -> None:
        # Epochs are process-local: a candidate reconstructed from durable
        # intent carries no meaningful stamp, so an epoch advanced by this
        # process's own share replay must not abandon the recovered block.
        # Cross-restart payout drift is governed by the durable share-window
        # replay and prior-balance fences instead.
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        block_hash = "cd" * 32
        pending = self._pending_append("replayed-epoch").pending_share
        original = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            ),
            pending_share=pending,
        )
        candidate = server.block_candidate_from_intent(
            server.block_candidate_intent(original)
        )
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            accepted = server.submit_block_candidate(candidate)

        self.assertTrue(accepted)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_offered_replay_keeps_as_issued_snapshot_despite_window_omission(
        self,
    ) -> None:
        # Revalidation guards an offer the node has not yet seen. The
        # dedicated submitter's fast lane offers the durable bytes before
        # the landing runs, so once the node accepts, the recorded coinbase
        # is settled reality: the landing must keep the as-issued snapshot
        # and skip the audit-window walk entirely instead of terminally
        # abandoning payout accounting for an accepted block (or defer-
        # looping behind the walk's deadline under saturation).
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]
        window_calls: list[tuple[int, int]] = []

        def durable_window(
            *,
            anchor_job_issued_at_ms: int,
            network_difficulty: int,
        ) -> list[dict[str, object]]:
            window_calls.append((anchor_job_issued_at_ms, network_difficulty))
            return [
                {"share_id": "recorded-window-share"},
                {"share_id": "late-appended-share"},
            ]

        ledger.audit_share_window = durable_window  # type: ignore[method-assign]
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "ec" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ec" * 32,
            block_hex="00",
        )
        pending = self._pending_append("replayed-window-omission").pending_share
        original = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(original)
        candidate = server.block_candidate_from_intent(intent)
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        ledger.append_batch([(pending, intent)])
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        # The fast lane offered first, so the walk never runs and the
        # as-issued snapshot is accounted.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(window_calls, [])
        self.assertEqual(len(bundle_builds), 1)
        self.assertEqual(server.stale_share_count, 0)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_unoffered_replay_still_rejects_window_omitting_durable_append(
        self,
    ) -> None:
        # The pre-offer half of the same fence: a direct embedder resumes a
        # reconstructed candidate with no node offer made (attempted=False,
        # as the compatibility probe reports when block bytes are not
        # retained), and the durable window surfaces a share the recorded
        # coinbase omitted — the landing must still refuse to mint that
        # coinbase, and the revalidation walk must run before any offer.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]
        window_calls: list[tuple[int, int]] = []

        def durable_window(
            *,
            anchor_job_issued_at_ms: int,
            network_difficulty: int,
        ) -> list[dict[str, object]]:
            window_calls.append((anchor_job_issued_at_ms, network_difficulty))
            return [
                {"share_id": "recorded-window-share"},
                {"share_id": "late-appended-share"},
            ]

        ledger.audit_share_window = durable_window  # type: ignore[method-assign]
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError(
                "audit bundle must not be built from a payout window omitting "
                "a durably appended share"
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ed" * 32,
            block_hex="00",
        )
        pending = self._pending_append("unoffered-window-omission").pending_share
        original = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(original)
        candidate = server.block_candidate_from_intent(intent)
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        ledger.append_batch([(pending, intent)])

        self.assertFalse(
            server.submit_block_candidate(
                candidate,
                node_submission=SimpleNamespace(
                    attempted=False,
                    error=None,
                    result=None,
                ),
            )
        )

        self.assertEqual(window_calls, [(12000, 1)])
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB],
            1,
        )
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 0, "balance_stale": 0, "append_epoch_stale": 1},
        )
        # Direct embedders do not use the outbox-finalization wrapper:
        # the terminal outcome is recorded and counted, while the durable
        # row stays pending for queue-driven replay to finalize.
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "pending")

    def test_replayed_block_candidate_lands_when_durable_window_replays_intact(
        self,
    ) -> None:
        # The durable revalidation is a fence against omitted appends, not a
        # new obstacle to recovery: when the audit window at the declared
        # anchor replays exactly the recorded shares, the reconstructed
        # candidate still lands -- even while this process's own live epoch
        # has advanced past the meaningless replayed stamp.
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        ledger.durable_payout_state = True
        ledger.audit_share_window = (  # type: ignore[attr-defined]
            lambda *, anchor_job_issued_at_ms, network_difficulty: [
                {"share_id": "recorded-window-share"}
            ]
        )
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        block_hash = "cf" * 32
        pending = self._pending_append("replayed-durable-window").pending_share
        original = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            ),
            pending_share=pending,
        )
        candidate = server.block_candidate_from_intent(
            server.block_candidate_intent(original)
        )
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            accepted = server.submit_block_candidate(candidate)

        self.assertTrue(accepted)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_block_submit_keeps_as_issued_accounting_when_append_races_the_landing(
        self,
    ) -> None:
        # Node propagation is the fast lane: the block is offered to qbitd
        # before payout accounting notices anything, so an append-side
        # invalidation racing the landing can no longer block submitblock.
        # The landing still observes the bump at its epoch fence, but the
        # node already accepted the block, so accounting proceeds with the
        # as-issued snapshot instead of terminally abandoning a live
        # block's payouts.
        # The bump here is driven through the REAL invalidation path from
        # the drain hook -- with no armed artifact or in-flight walk in the
        # harness, it fires only because the landing exposed its own
        # declared anchor.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        late_append = self._pending_append("late-during-landing").pending_share

        race_fired: list[bool] = []
        original_drain = server._await_unfenced_appends_predating_anchor

        def racing_drain(anchor_ms: int) -> None:
            original_drain(anchor_ms)
            if not race_fired:
                race_fired.append(True)
                server._invalidate_incremental_payout_window_for_append(
                    late_append
                )

        server._await_unfenced_appends_predating_anchor = racing_drain  # type: ignore[method-assign]

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "da" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="da" * 32,
            block_hex="00",
        )
        pending = self._pending_append("append-races-landing").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(race_fired, [True])
        # The fast lane offered the block before the race could be observed.
        self.assertEqual(submitblock_calls, [["00"]])
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
        self.assertEqual(server.stale_share_count, 0)
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)
        # The landing retires its exposed anchor on the way out.
        with server._job_cache_lock:
            self.assertEqual(server._payout_window_inflight_scan_anchors, {})

    def test_offered_replay_lands_as_issued_without_revalidation_walk(
        self,
    ) -> None:
        # The fast lane offers a reconstructed candidate's durable bytes
        # before the landing runs, so the slow audit-window walk is skipped
        # outright — even while this process's live epoch advances mid-
        # flight — and accounting keeps the as-issued snapshot rather than
        # abandoning (or re-walking) an accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]

        window_calls: list[tuple[int, int]] = []

        def recording_audit_share_window(
            *, anchor_job_issued_at_ms: int, network_difficulty: int
        ) -> list[dict[str, object]]:
            window_calls.append((anchor_job_issued_at_ms, network_difficulty))
            return [{"share_id": "recorded-window-share"}]

        ledger.audit_share_window = recording_audit_share_window  # type: ignore[method-assign]
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "db" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="db" * 32,
            block_hex="00",
        )
        pending = self._pending_append("replayed-append-race").pending_share
        original = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(original)
        candidate = server.block_candidate_from_intent(intent)
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        ledger.append_batch([(pending, intent)])
        server.enqueue_block_candidate(candidate)
        # A live epoch bump mid-flight is irrelevant to a reconstructed
        # candidate whose offer is already out: no revalidation rebases it
        # and the epoch fences stand down (no effective epoch).
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1

        self.assertTrue(server.submit_next_block_candidate())
        # The fast lane offered the replayed block; the walk never ran.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(window_calls, [])
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_landing_blocked_by_fenced_append_lands_as_issued_on_bumped_epoch(
        self,
    ) -> None:
        # A replay-shaped append holds the landing fence across the durable
        # commit itself, not only across its epoch bump. The node offer is
        # the fast lane and does not wait, but a landing whose offer is
        # already in must still wait at the fence before ACCOUNTING; with
        # the offer already accepted, the bumped epoch is recorded and
        # accounting proceeds with the as-issued coinbase (the durable
        # predating share rides the next window) instead of abandoning the
        # accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "dc" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dc" * 32,
            block_hex="00",
        )
        pending = self._pending_append("landing-vs-fenced-append").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        # Expose a live anchor the late row predates, as an armed window
        # would: the writer's pre-commit peek must fire before any landing
        # is in flight to declare its own anchor.
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)

        commit_entered = threading.Event()
        release_commit = threading.Event()
        self.addCleanup(release_commit.set)
        original_append_batch = ledger.append_batch

        def gated_append_batch(entries: list[object]) -> list[object]:
            commit_entered.set()
            release_commit.wait(timeout=10.0)
            return original_append_batch(entries)

        ledger.append_batch = gated_append_batch  # type: ignore[method-assign]
        late_entry = self._pending_append("late-fenced-append")
        writer = threading.Thread(
            target=server._append_share_batch,
            args=([late_entry],),
            daemon=True,
        )
        writer.start()
        self.assertTrue(commit_entered.wait(timeout=10.0))
        # The fence is held while the predating row's commit is in flight.
        self.assertTrue(server._payout_append_landing_fence_lock.locked())

        landing = threading.Thread(
            target=server.submit_next_block_candidate,
            daemon=True,
        )
        landing.start()
        # Give the landing time to run up against the fence: the fast-lane
        # node offer already went out, but with the commit still gated the
        # landing must not have reached a terminal accounting decision.
        time.sleep(0.05)
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(server.block_candidate_abandoned_counts, {})

        release_commit.set()
        writer.join(timeout=10.0)
        self.assertFalse(writer.is_alive())
        landing.join(timeout=10.0)
        self.assertFalse(landing.is_alive())

        # The bump landed together with the durable append; the landing
        # observed it, and with the offer already accepted it kept the
        # as-issued snapshot for accounting.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_lease_deferral_unwinds_landed_bar_before_submitblock(self) -> None:
        # The landed bar is armed before the lease fence to keep a transport
        # failure's uncertain submitblock outcome conservative. A
        # WriterLeaseRenewalDeferred refusal fires before the RPC, so the
        # outcome is not uncertain: qbitd provably never saw the block, and
        # leaving the bar armed in the (now surviving) process would bar
        # reconciliation and payout-state publication for as long as the
        # writer's own fenced write keeps the renewal deferred. The pending
        # preview must survive, though: child work keeps waiting for the
        # retry instead of snapshotting pre-accept balances.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.block_candidate_retry_initial_seconds = 0.0

        class NoSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    raise AssertionError(
                        "submitblock must not run after a deferral refusal"
                    )
                return super().call(method, params)

        server.rpc = NoSubmitRpc("00" * 32)
        # Force the fenced fallback lane: with no fast-lane node offer
        # (attempted=False), the disposition itself must clear the lease
        # fence before submitblock — the branch the deferral exits from.
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="de" * 32,
            block_hex="00",
        )
        pending = self._pending_append("deferral-unwind").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        with patch.object(
            server,
            "_require_fresh_ledger_lease_for_external_side_effect",
            side_effect=WriterLeaseRenewalDeferred("renewal is deferred"),
        ) as fence:
            self.assertTrue(server.submit_next_block_candidate())

        fence.assert_called_once_with("submitblock")
        block_hash = str(submission.block_hash_hex).lower()
        self.assertFalse(
            server._accepted_block_payout_transition_landed(block_hash)
        )
        with server._accepted_block_payout_preview_condition:
            self.assertIn(block_hash, server._accepted_block_payout_previews)

    def test_lease_deferral_keeps_prior_attempts_landed_bar(self) -> None:
        # A transition landed by an earlier attempt that DID reach
        # submitblock records a genuinely uncertain outcome (a lost RPC
        # ack); a later attempt's pre-RPC deferral must not withdraw that
        # bar, or payout state could publish without the possibly-active
        # coinbase's base preserved.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.block_candidate_retry_initial_seconds = 0.0

        class NoSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    raise AssertionError(
                        "submitblock must not run after a deferral refusal"
                    )
                return super().call(method, params)

        server.rpc = NoSubmitRpc("00" * 32)
        # Force the fenced fallback lane, as above.
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="df" * 32,
            block_hex="00",
        )
        pending = self._pending_append("deferral-keeps-bar").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        block_hash = str(submission.block_hash_hex).lower()
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.enqueue_block_candidate(candidate)

        with patch.object(
            server,
            "_require_fresh_ledger_lease_for_external_side_effect",
            side_effect=WriterLeaseRenewalDeferred("renewal is deferred"),
        ) as fence:
            self.assertTrue(server.submit_next_block_candidate())

        fence.assert_called_once_with("submitblock")
        self.assertTrue(
            server._accepted_block_payout_transition_landed(block_hash)
        )

    def test_landing_drains_unfenced_inflight_append_before_epoch_fences(
        self,
    ) -> None:
        # The unfenced classification is a one-time predicate: an append
        # checked while no anchor is exposed commits outside the landing
        # fence, so a landing that exposes its declared anchor afterwards
        # could hold the fence across submitblock while the row becomes
        # durable mid-RPC and its epoch bump queues behind that same
        # fence -- arriving too late to reject the block. The landing must
        # instead drain such in-flight commits right after exposing its
        # anchor, so the bump lands before its epoch fences run.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dc" * 32,
            block_hex="00",
        )
        pending = self._pending_append("landing-drains").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        late_entry = self._pending_append("unfenced-inflight")
        commit_entered = threading.Event()
        commit_release = threading.Event()
        original_append_batch = ledger.append_batch

        def gated_append_batch(batch: list[object]) -> list[object]:
            commit_entered.set()
            if not commit_release.wait(timeout=10.0):
                raise AssertionError("gated ledger commit was never released")
            return original_append_batch(batch)

        ledger.append_batch = gated_append_batch  # type: ignore[method-assign]

        # Release the gated commit exactly when the landing starts
        # draining, so the test exercises the wait itself rather than a
        # lucky ordering.
        drained_anchors: list[int] = []
        original_drain = server._await_unfenced_appends_predating_anchor

        def recording_drain(anchor_ms: int) -> None:
            drained_anchors.append(int(anchor_ms))
            commit_release.set()
            original_drain(anchor_ms)

        server._await_unfenced_appends_predating_anchor = recording_drain  # type: ignore[method-assign]

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "dc" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)

        append_thread = threading.Thread(
            target=server._append_share_entry,
            args=(late_entry,),
            daemon=True,
        )
        append_thread.start()
        # The append was classified with no anchor exposed and is now
        # committing outside the fence.
        self.assertTrue(commit_entered.wait(timeout=10.0))

        self.assertTrue(server.submit_next_block_candidate())

        append_thread.join(timeout=10.0)
        self.assertFalse(append_thread.is_alive())
        self.assertEqual(drained_anchors, [12000])
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
            self.assertEqual(
                server._payout_unfenced_append_inflight_stamps, {}
            )
        # The drained append's bump landed before the landing's epoch
        # fences; the landing observed it and, with the node offer already
        # accepted, accounted the as-issued window (the durable predating
        # share rides the next window).
        self.assertEqual(submitblock_calls, [["00"]])
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_landing_builds_audit_bundle_outside_balance_serializer(self) -> None:
        # The builder/verifier subprocess time must stay off the
        # payout-balance mutation lock: job delivery and reconciliation
        # queue behind that lock, and holding it across the bundle build was
        # the dominant term of the finalization delivery stall.
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        lock_held_during_build: list[bool] = []
        build_calls: list[int] = []

        def probe_lock_from_other_thread() -> bool:
            # An RLock re-acquires freely on the owning thread, so the probe
            # must come from a thread that cannot be the owner.
            acquired: list[bool] = []

            def attempt() -> None:
                got = server._payout_balance_mutation_lock.acquire(
                    blocking=False
                )
                if got:
                    server._payout_balance_mutation_lock.release()
                acquired.append(not got)

            prober = threading.Thread(target=attempt)
            prober.start()
            prober.join(5)
            return bool(acquired and acquired[0])

        def fake_build_audit_bundle(**_kwargs: object) -> dict[str, object]:
            build_calls.append(1)
            lock_held_during_build.append(probe_lock_from_other_thread())
            return verified_block_bundle()

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "cc" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            pending = SimpleNamespace(share_id="miner-a:" + block_hash)
            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission, pending_share=pending)
            )

        self.assertTrue(accepted)
        self.assertEqual(build_calls, [1])
        self.assertEqual(lock_held_during_build, [False])
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_block_submitted_before_audit_bundle_build(self) -> None:
        # The audit build runs with the balance serializer released, but
        # never before submitblock: announcement of a freshly solved block
        # must not wait on audit construction (a lost race is a lost round).
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        order: list[str] = []
        block_hash = "cc" * 32

        class OrderRecordingRpc(SubmitRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "submitblock":
                    order.append("submitblock")
                return super().call(method, params)

        def ordered_build(**_kwargs: object) -> dict[str, object]:
            order.append("build_audit_bundle")
            return verified_block_bundle()

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = OrderRecordingRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = ordered_build  # type: ignore[method-assign]
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            pending = SimpleNamespace(share_id="miner-a:" + block_hash)
            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission, pending_share=pending)
            )

        self.assertTrue(accepted)
        self.assertEqual(order, ["submitblock", "build_audit_bundle"])
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_successful_fast_lane_reconciles_observed_post_submit_tip(self) -> None:
        parent_hash = "00" * 32
        block_hash = "cd" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        server.stop_after_block = False
        server.max_blocks = 10
        server.rpc = SubmitAcceptingTemplateRpc(
            old_tip=parent_hash,
            block_hash=block_hash,
            ledger=ledger,
        )
        reconciled: list[tuple[str, bool]] = []
        landing_inputs: list[tuple[str, bool]] = []

        def reconcile(
            tip_hash: str,
            *,
            _coalesce_same_tip: bool = True,
        ) -> bool:
            reconciled.append((tip_hash, _coalesce_same_tip))
            return True

        original_land = server._land_and_confirm_block_candidate

        def observe_landing(
            candidate: PrismBlockCandidate,
            **kwargs: object,
        ) -> object:
            landing_inputs.append(
                (str(kwargs["current_tip"]), bool(kwargs["already_active"]))
            )
            return original_land(candidate, **kwargs)  # type: ignore[arg-type]

        server.ensure_reorg_reconciled_for_tip = reconcile  # type: ignore[method-assign]
        server._land_and_confirm_block_candidate = observe_landing  # type: ignore[method-assign]
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertTrue(accepted)
        self.assertEqual(landing_inputs, [(block_hash, False)])
        self.assertEqual(reconciled, [(block_hash, False)])

    def test_submitter_offers_block_before_writer_admission_with_rpc_deadline(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.block_submit_rpc_timeout_seconds = 0.75
        order: list[object] = []

        class TimeoutRecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    order.append(("submitblock", timeout))
                    return None
                return super().call(method, params)

        class WriterAdmission:
            def __enter__(self) -> None:
                order.append("writer-admission")

            def __exit__(self, *_args: object) -> None:
                return None

        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="ce" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )
        server.rpc = TimeoutRecordingRpc("00" * 32)
        server._writer_operation = lambda _component: WriterAdmission()  # type: ignore[method-assign]

        def account(
            _candidate: PrismBlockCandidate,
            *,
            node_submission: object,
        ) -> bool:
            order.append("accounting")
            self.assertIsNone(getattr(node_submission, "result"))
            return True

        server._submit_next_block_candidate_writer = account  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(
            order,
            [("submitblock", 0.75), "writer-admission", "accounting"],
        )

    def test_accepted_candidate_with_moved_tip_finalizes_as_submitted(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        block_hash = "cc" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("accepted-tip-race").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        submitted: list[str] = []
        abandoned: list[str] = []
        original_submitted = ledger.mark_block_candidate_submitted
        original_abandoned = ledger.mark_block_candidate_abandoned

        def mark_submitted(*, block_hash: str) -> bool:
            submitted.append(block_hash)
            return original_submitted(block_hash=block_hash)

        def mark_abandoned(*, block_hash: str, error: str) -> bool:
            abandoned.append(block_hash)
            return original_abandoned(block_hash=block_hash, error=error)

        ledger.mark_block_candidate_submitted = mark_submitted  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            block_hash,
            block_height=10,
        )
        with server.lock:
            # This is the process-local record written immediately before the
            # "qbit accepted direct PRISM block" log.
            server._accounted_accepted_block_hashes.add(block_hash)
        server.rpc = TipRpc(new_tip)
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("an accounted candidate must not be rebuilt")
            )
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(submitted, [block_hash])
        self.assertEqual(abandoned, [])
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_terminal_abandon_immediately_clears_payout_preview(self) -> None:
        server, _state, _ledger = submit_coordinator()
        block_hash = "a4" * 32
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            block_hash,
            block_height=10,
        )

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved",
            block_hash=block_hash,
            worker="miner-a",
        )

        self.assertFalse(accepted_race_won)
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        # The tombstone protects pending durable replay, but unlike the leaked
        # landed transition it does not block payout reconciliation.
        with server._payout_balance_mutation():
            pass

    def test_tip_moved_abandon_yields_to_acceptance_during_cleanup(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        block_hash = "a5" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        configure_temporary_audit_root(self, server)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("accepted-during-clear").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        submitted: list[str] = []
        abandoned: list[str] = []
        original_submitted = ledger.mark_block_candidate_submitted
        original_abandoned = ledger.mark_block_candidate_abandoned
        original_clear = server._clear_accepted_block_payout_preview

        def mark_submitted(*, block_hash: str) -> bool:
            submitted.append(block_hash)
            return original_submitted(block_hash=block_hash)

        def mark_abandoned(*, block_hash: str, error: str) -> bool:
            abandoned.append(block_hash)
            return original_abandoned(block_hash=block_hash, error=error)

        def clear_while_accepting(
            candidate_hash: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            original_clear(
                candidate_hash,
                invalidate_published=invalidate_published,
            )
            if invalidate_published:
                with server.lock:
                    server._accounted_accepted_block_hashes.add(
                        candidate_hash.lower()
                    )

        ledger.mark_block_candidate_submitted = mark_submitted  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[method-assign]
        server._clear_accepted_block_payout_preview = clear_while_accepting  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            block_hash,
            block_height=10,
        )
        server.rpc = RejectingSubmitTipRpc(new_tip)
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(submitted, [block_hash])
        self.assertEqual(abandoned, [])
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="tip_moved"} 0',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="balance_stale"} 0',
            metrics,
        )
        self.assertEqual(ledger.pending_block_candidates(), [])
        outbox_row = ledger._block_candidate_outbox[block_hash]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_crash_after_submit_before_attempt_mark_replays_duplicate_once(self) -> None:
        parent_hash = "00" * 32
        block_hash = "cf" * 32
        ledger = SingleWriterShareLedger()
        first, state, _recording = submit_coordinator(tip=parent_hash)
        first.ledger = ledger
        first.stop_after_block = False
        first.max_blocks = 10
        pending = self._pending_append("submit-before-mark-crash").pending_share
        candidate = block_candidate(
            first,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        intent = first.block_candidate_intent(candidate)
        ledger.append_batch([(pending, intent)])

        class RestartAwareRpc(FakeRpc):
            def __init__(self) -> None:
                self.submit_results: list[object] = []

            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    result = None if not self.submit_results else "duplicate"
                    self.submit_results.append(result)
                    return result
                if method == "getbestblockhash":
                    return parent_hash
                if method == "getblockhash":
                    return block_hash
                if method == "getblockcount":
                    return 9
                return super().call(method, params)

        rpc = RestartAwareRpc()
        first.rpc = rpc
        original_mark = ledger.mark_block_candidate_attempted

        def crash_before_mark(*, block_hash: str) -> bool:
            raise SystemExit(f"simulated crash before marking {block_hash}")

        ledger.mark_block_candidate_attempted = crash_before_mark  # type: ignore[method-assign]
        first.enqueue_block_candidate(candidate)
        with self.assertRaisesRegex(SystemExit, "simulated crash"):
            first.submit_next_block_candidate()

        self.assertEqual(rpc.submit_results, [None])
        self.assertEqual(ledger.pending_block_candidates(), [intent])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["attempt_count"],
            0,
        )

        ledger.mark_block_candidate_attempted = original_mark  # type: ignore[method-assign]
        persisted_calls = 0
        confirmed_calls = 0
        original_persist = ledger.persist_accepted_block
        original_confirm = ledger.confirm_accepted_block

        def persist_once(**kwargs: object) -> dict[str, object]:
            nonlocal persisted_calls
            persisted_calls += 1
            return original_persist(**kwargs)

        def confirm_once(**kwargs: object) -> dict[str, object]:
            nonlocal confirmed_calls
            confirmed_calls += 1
            return original_confirm(**kwargs)

        ledger.persist_accepted_block = persist_once  # type: ignore[method-assign]
        ledger.confirm_accepted_block = confirm_once  # type: ignore[method-assign]

        restarted, _restart_state, _recording = submit_coordinator(tip=parent_hash)
        restarted.ledger = ledger
        restarted.rpc = rpc
        restarted.stop_after_block = False
        restarted.max_blocks = 10
        with tempfile.TemporaryDirectory() as tempdir:
            restarted.audit_dir = Path(tempdir)
            restarted.evidence_path = Path(tempdir) / "evidence.json"
            restarted.ledger_writer_public_key_hex = "aa" * 32
            restarted.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            restarted.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )

            self.assertEqual(restarted.replay_pending_block_candidates(), 1)
            self.assertTrue(restarted.submit_next_block_candidate())
            self.assertEqual(restarted.replay_pending_block_candidates(), 0)

        self.assertEqual(rpc.submit_results, [None, "duplicate"])
        self.assertEqual(persisted_calls, 1)
        self.assertEqual(confirmed_calls, 1)
        self.assertEqual(restarted.accepted_block_count, 1)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["state"],
            "submitted",
        )

    def test_block_submitter_honors_stop_after_block_above_one_block_capacity(
        self,
    ) -> None:
        server, state, ledger = submit_coordinator()
        server.accepted_block_count = 1
        server.max_blocks = 2
        server.stop_after_block = True
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="de" * 32,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(
            block_candidate(server, state, submission)
        )

        self.assertFalse(accepted)
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_POOL_CLOSED],
            1,
        )
        self.assertEqual(ledger.persisted, [])

    @staticmethod
    def _reconcile_coordinator_with_artifact_counter(
        ledger: ReorgLedger,
        rpc: ReorgRpc,
    ) -> tuple[PrismCoordinator, list[tuple[int, int, bool]]]:
        """Coordinator armed so candidate preparation would build an artifact."""
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = rpc
        server._ensure_job_cache_state()
        # Keep the speculative background preparer out of the way so the
        # counter observes only builds requested by reconcile passes.
        server._payout_artifact_executor_shutdown = True
        server._pool_ready_latched = True
        server._template_artifacts = SimpleNamespace(network_difficulty=1)
        build_calls: list[tuple[int, int, bool]] = []

        def fake_build(
            expected_payout_state_generation: int,
            artifact_payout_state_generation: int,
            network_difficulty: int,
            force_full_rescan: bool = False,
            bypass_build_interval: bool = False,
            during_publication: bool = False,
        ) -> None:
            del network_difficulty, bypass_build_interval, during_publication
            build_calls.append(
                (
                    expected_payout_state_generation,
                    artifact_payout_state_generation,
                    force_full_rescan,
                )
            )
            return None

        server._build_payout_ledger_artifact = fake_build  # type: ignore[method-assign]
        return server, build_calls


class PrismStampedJobFloorTests(unittest.TestCase):
    """The listener floor must hold on the wire, not just in vardiff policy.

    Stamped jobs are the single choke point for every mining.set_difficulty
    the coordinator sends, and marketplace verification judges the first one.
    The regression here is a young chain: qbit network difficulty below the
    high-diff floor used to drag the advertised difficulty down with it.
    """

    def stamp_coordinator(self) -> PrismCoordinator:
        server = coordinator()
        server.job_counter = 0
        server.share_weights_by_username = {}
        server.default_share_weight = 1
        return server

    def cached_bundle(self) -> CachedJobBundle:
        # bits 207fffff: regtest-grade network difficulty (~4.7e-10), far
        # below the 500k marketplace floor.
        qbit_target = target_from_compact("207fffff")
        base_job = direct_stratum.DirectQbitStratumJob(
            job_id="prism-template-base",
            previousblockhash_display="00" * 32,
            prevhash="00" * 32,
            coinb1="",
            coinb2="",
            full_coinbase_prefix="",
            full_coinbase_suffix="",
            merkle_branch=(),
            transaction_hexes=(),
            version="20000000",
            nbits="207fffff",
            ntime="6553f100",
            qbit_target=qbit_target,
            share_target=qbit_target,
            share_difficulty=Decimal("1"),
            extranonce1_hex="ffffffff",
            extranonce2_size=8,
            clean_jobs=True,
        )
        return CachedJobBundle(
            key=("test",),
            template=gbt_template("00" * 32),
            template_fingerprint="fp",
            coinbase_manifest={},
            shares_json=[],
            prior_balances=[],
            found_block={"network_difficulty": 1},
            collection_only=False,
            issued_at_ms=12345,
            base_job=base_job,
            built_monotonic=time.monotonic(),
        )

    def highdiff_client(self) -> ClientState:
        state = client()
        state.worker = worker_identity()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.minimum_advertised_difficulty = Decimal("500000")
        state.share_difficulty = Decimal("500000")
        return state

    def test_block_worthy_submission_below_share_target_submits_synchronously(self) -> None:
        # With the floor above network difficulty a hash can solve a block
        # while missing the advertised share target. It is a valid share only
        # if the block lands, so it submits synchronously (not via the async
        # queue) and the share credit lands with it.
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=True,
        )
        submitted: list[object] = []

        def fake_submit(candidate: object) -> bool:
            submitted.append(candidate)
            server.append_accepted_share(
                candidate.client, candidate.context, candidate.submission, candidate.pending_share
            )
            return True

        server.submit_block_candidate = fake_submit  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 0)
        self.assertEqual(len(submitted), 1)
        self.assertTrue(submitted[0].credit_share_on_accept)
        # Nothing was queued to the async submitter; it landed inline.
        self.assertEqual(server.block_candidate_queue.qsize(), 0)
        self.assertEqual(len(ledger.pending), 1)

    def test_below_target_block_intent_is_durable_before_synchronous_submit(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="bc" * 32,
            share_pass=False,
            block_pass=True,
        )

        def fake_submit(candidate: PrismBlockCandidate) -> bool:
            pending = ledger.pending_block_candidates()
            self.assertEqual(len(pending), 1)
            self.assertTrue(pending[0]["credit_share_on_accept"])
            server.append_accepted_share(
                candidate.client,
                candidate.context,
                candidate.submission,
                candidate.pending_share,
                candidate_intent=server.block_candidate_intent(candidate),
            )
            return True

        server.submit_block_candidate = fake_submit  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_below_target_intent_failure_does_not_create_unsafe_retry_slot(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="be" * 32,
            share_pass=False,
            block_pass=True,
        )
        submit_calls = 0

        def fail_intent(_intent: dict[str, object]) -> bool:
            raise RuntimeError("outbox unavailable")

        def unsafe_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            return True

        ledger.persist_block_candidate_intent = fail_intent  # type: ignore[method-assign]
        server.submit_block_candidate = unsafe_submit  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaisesRegex(RuntimeError, "outbox unavailable"):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(submit_calls, 0)
        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_block_worthy_below_target_rejects_low_difficulty_when_block_fails(self) -> None:
        # If the block does not land, the below-share-target hash earns nothing
        # and the miner is rejected as low-difficulty -- never acked accepted
        # with no ledger row.
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=True,
        )
        def reject_candidate(_candidate: PrismBlockCandidate) -> bool:
            server._abandon_block_candidate(
                PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                "rejected",
                block_hash=_candidate.submission.block_hash_hex,
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = reject_candidate  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_LOW_DIFFICULTY)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(server.block_candidate_queue.qsize(), 0)
        # The reject is counted (globally and for the worker), not just the
        # block-abandonment reason -- this synchronous path used to skip it.
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 1)
        self.assertEqual(server.low_difficulty_share_count, 1)

    def test_below_target_transient_outcome_closes_without_definitive_reject(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="bd" * 32,
            share_pass=False,
            block_pass=True,
        )
        server.submit_block_candidate = lambda _candidate: False  # type: ignore[method-assign]

        submit_params = ["miner-a", "job-1", "00" * 8, "00000001", "00000002"]
        # Each retry rebuilds its candidate intent with a fresh acknowledgment
        # stamp. Force every call onto a new millisecond so the durable-outbox
        # idempotency is exercised across acknowledgment-stamp drift instead of
        # depending on both attempts landing within the same millisecond.
        clock_ms = iter(range(1_700_000_000_000, 1_700_000_070_000, 7))
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=clock_ms.__next__,
        ):
            for _attempt in range(2):
                with self.assertRaisesRegex(RuntimeError, "pending durable retry"):
                    server.handle_submit(state, submit_params)
                self.assertEqual(server.recent_share_keys, set())

        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 0)
        self.assertEqual(server.duplicate_share_count, 0)
        self.assertEqual(len(ledger), 0)
        self.assertEqual(len(ledger.pending_block_candidates()), 1)
        self.assertIsNotNone(server._retry_block_candidate)
        assert server._retry_block_candidate is not None
        self.assertEqual(
            server._retry_block_candidate.submission.block_hash_hex,
            submission.block_hash_hex,
        )

    def test_low_difficulty_submission_without_block_solve_is_rejected(self) -> None:
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.code, 23)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_LOW_DIFFICULTY)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 1)
        self.assertEqual(len(ledger.pending), 0)

    def test_collection_only_below_target_block_solve_submits_solver_pays_all(self) -> None:
        # A collection job's signed bootstrap manifest already commits the
        # whole coinbase to the submitting worker, so a solved block on a
        # collection job is submitted (synchronously here, since the share
        # missed its target) instead of being withheld -- the first block on a
        # fresh ledger must never be silently ledgered away.
        server, state, ledger = submit_coordinator()
        server.jobs["job-1"].collection_only = True
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=True,
        )
        submitted: list[object] = []

        def fake_submit(candidate: object) -> bool:
            submitted.append(candidate)
            server.append_accepted_share(
                candidate.client, candidate.context, candidate.submission, candidate.pending_share
            )
            return True

        server.submit_block_candidate = fake_submit  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 0)
        self.assertEqual(len(submitted), 1)
        self.assertTrue(submitted[0].credit_share_on_accept)
        self.assertTrue(submitted[0].context.collection_only)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(server.collection_block_submission_count, 1)

    def test_collection_job_block_solve_is_credited_and_enqueued_not_withheld(self) -> None:
        # A solved block that also met its share target on a collection job is
        # credited immediately and queued for the submitter thread, exactly
        # like a ready-window candidate.
        server, state, ledger = submit_coordinator()
        server.jobs["job-1"].collection_only = True
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        queued = server.block_candidate_queue.get_nowait()
        self.assertTrue(queued.context.collection_only)
        self.assertFalse(queued.credit_share_on_accept)
        self.assertEqual(server.collection_block_submission_count, 1)

    def test_block_candidate_intent_round_trips_compact_payout_state(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.jobs["job-1"].collection_only = True
        server.jobs["job-1"].prospective_prior_balances = (
            ("miner-a", "miner-a", "11" * 32, 25),
        )
        pending = PendingShare(
            share_id="miner-a:" + "dd" * 32,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=2,
            ntime=1,
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex="dd" * 32,
            block_hex="00",
            share_pass=True,
            block_pass=True,
        )
        candidate = block_candidate(server, state, submission, pending_share=pending)

        intent = server.block_candidate_intent(candidate)
        self.assertIs(intent["collection_only"], True)
        replayed = server.block_candidate_from_intent(intent)
        self.assertTrue(replayed.context.collection_only)
        self.assertEqual(
            replayed.context.prospective_prior_balances,
            (("miner-a", "miner-a", "11" * 32, 25),),
        )

        # Intents persisted before the flag existed replay as ready-window
        # candidates, which is all the outbox could ever have contained then.
        intent.pop("collection_only")
        intent.pop("prospective_prior_balances")
        replayed = server.block_candidate_from_intent(intent)
        self.assertFalse(replayed.context.collection_only)
        self.assertIsNone(replayed.context.prospective_prior_balances)

    def test_below_target_accepted_tail_serializes_moved_tip_replay(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        block_hash = "b7" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        server.max_blocks = 10
        server.stop_after_block = False
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.rpc = SubmitRpc(
            tip=old_tip,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        server.ledger_writer_public_key_hex = "aa" * 32
        server.refresh_jobs_after_pending_accepted_block = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: 0
        )
        submission = SimpleNamespace(
            header_hex="b7" * 80,
            coinbase_tx_hex="c0ffee",
            block_hex="00",
            block_hash_hex=block_hash,
            share_pass=False,
            block_pass=True,
        )

        durable_confirmation = threading.Event()
        accepted_tail_paused = threading.Event()
        release_accepted_tail = threading.Event()
        replay_guard_blocked = threading.Event()
        confirmation_calls: list[str] = []
        submitted: list[str] = []
        abandoned: list[str] = []
        synchronous_results: list[bool] = []
        replay_results: list[bool] = []
        errors: list[BaseException] = []
        replay_thread: threading.Thread | None = None
        original_confirm = ledger.confirm_accepted_block
        original_stats = server.accepted_share_stats
        original_submitted = ledger.mark_block_candidate_submitted
        original_abandoned = ledger.mark_block_candidate_abandoned

        class ObservedDispositionLock:
            def __init__(self) -> None:
                self.lock = threading.Lock()

            def acquire(
                self,
                blocking: bool = True,
                timeout: float = -1,
            ) -> bool:
                if threading.current_thread() is replay_thread:
                    if self.lock.acquire(blocking=False):
                        return True
                    # A failed non-blocking acquisition proves the accepted
                    # attempt still owns this exact same-hash guard.
                    replay_guard_blocked.set()
                if not blocking:
                    return self.lock.acquire(blocking=False)
                if timeout < 0:
                    return self.lock.acquire()
                return self.lock.acquire(timeout=timeout)

            def release(self) -> None:
                self.lock.release()

        server._ensure_block_candidate_disposition_state()
        with server._block_candidate_disposition_registry_lock:
            server._block_candidate_disposition_flights[block_hash] = (  # type: ignore[assignment]
                SimpleNamespace(lock=ObservedDispositionLock(), users=0)
            )

        def confirm_accepted_block(
            *,
            block_hash: str,
            active_tip_height: int,
        ) -> dict[str, int | str]:
            result = original_confirm(
                block_hash=block_hash,
                active_tip_height=active_tip_height,
            )
            confirmation_calls.append(block_hash)
            durable_confirmation.set()
            return result

        def pause_in_accepted_tail() -> tuple[int, int]:
            accepted_tail_paused.set()
            if not release_accepted_tail.wait(10):
                raise AssertionError("timed out waiting to release accepted success tail")
            return original_stats()

        def mark_submitted(*, block_hash: str) -> bool:
            submitted.append(block_hash)
            return original_submitted(block_hash=block_hash)

        def mark_abandoned(*, block_hash: str, error: str) -> bool:
            abandoned.append(block_hash)
            return original_abandoned(block_hash=block_hash, error=error)

        ledger.confirm_accepted_block = confirm_accepted_block  # type: ignore[method-assign]
        ledger.mark_block_candidate_submitted = mark_submitted  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[method-assign]
        server.accepted_share_stats = pause_in_accepted_tail  # type: ignore[method-assign]

        def submit_synchronously() -> None:
            try:
                synchronous_results.append(
                    server.handle_submit(
                        state,
                        [
                            "miner-a",
                            "job-1",
                            "00" * 8,
                            "00000001",
                            "00000002",
                        ],
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)

        def replay_pending_candidate() -> None:
            try:
                replay_results.append(server.submit_next_block_candidate())
            except BaseException as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)

        with tempfile.TemporaryDirectory() as tempdir, patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            synchronous_thread = threading.Thread(target=submit_synchronously)
            synchronous_thread.start()
            try:
                self.assertTrue(
                    accepted_tail_paused.wait(5),
                    msg=f"synchronous submit exited early: {errors!r}",
                )
                self.assertTrue(durable_confirmation.is_set())
                self.assertEqual(len(ledger), 1)
                self.assertEqual(len(ledger.pending_block_candidates()), 1)
                with server.lock:
                    self.assertNotIn(
                        block_hash,
                        server._accounted_accepted_block_hashes,
                    )

                self.assertEqual(server.replay_pending_block_candidates(), 1)
                server.rpc = TipRpc(new_tip)
                replay_thread = threading.Thread(target=replay_pending_candidate)
                replay_thread.start()
                self.assertTrue(replay_guard_blocked.wait(5))
                self.assertTrue(replay_thread.is_alive())
                self.assertEqual(submitted, [])
                self.assertEqual(abandoned, [])
            finally:
                release_accepted_tail.set()
                synchronous_thread.join(10)
                if replay_thread is not None:
                    replay_thread.join(10)

        self.assertFalse(synchronous_thread.is_alive())
        self.assertIsNotNone(replay_thread)
        assert replay_thread is not None
        self.assertFalse(replay_thread.is_alive())
        if errors:
            raise errors[0]
        self.assertEqual(synchronous_results, [False])
        self.assertEqual(replay_results, [True])
        self.assertEqual(confirmation_calls, [block_hash])
        self.assertGreaterEqual(len(submitted), 1)
        self.assertEqual(abandoned, [])
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(server.accepted_block_count, 1)
        self.assertIn(block_hash, server._accounted_accepted_block_hashes)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        self.assertEqual(server._block_candidate_disposition_flights, {})

    def _drive_interleaved_distinct_hash_landings(
        self,
        *,
        lease_error: BaseException | None = None,
        lease_error_only_inside_publication_guard: bool = False,
    ) -> SimpleNamespace:
        """Interleave two distinct-hash landings across the production tails.

        Block A lands on the synchronous miner-connection tail and pauses in
        its confirm->publish gap (the accepted-share-stats aggregate) with the
        payout balance lock and the publication order guard released. Block B
        lands through the production submitter->accounting-actor handoff
        inside that gap and publishes first, so A reaches publication already
        superseded on the publication ordinal. ``lease_error`` is raised from
        the coordinator's fresh-lease fence when provided, modelling a writer
        whose lease authority cannot be proven inside the gap. With
        ``lease_error_only_inside_publication_guard`` the restore-authority
        proof succeeds anywhere outside the store's publication order guard
        and fails inside it, modelling a writer deposed while queued on the
        publication locks.
        """
        old_tip = "00" * 32
        hash_a = "a1" * 32
        hash_b = "b2" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        server.max_blocks = 10
        server.stop_after_block = False
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **kwargs: verified_audit_report(
                block_height=int(kwargs["expected_block_height"])
            )
        )
        server.ledger_writer_public_key_hex = "aa" * 32
        server.refresh_jobs_after_pending_accepted_block = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: 0
        )
        publication_guard_holders: set[int] = set()
        if lease_error is not None:

            def failing_lease_fence(component: str) -> None:
                if lease_error_only_inside_publication_guard:
                    if component != "superseded_audit_envelope_restore":
                        return
                    if threading.get_ident() not in publication_guard_holders:
                        return
                raise lease_error

            server._require_fresh_ledger_lease_for_external_side_effect = (  # type: ignore[method-assign]
                failing_lease_fence
            )
        submission_a = SimpleNamespace(
            header_hex="a1" * 80,
            coinbase_tx_hex="c0ffee",
            block_hex="00",
            block_hash_hex=hash_a,
            share_pass=False,
            block_pass=True,
        )
        context_b = SimpleNamespace(
            job=SimpleNamespace(
                job_id="job-2",
                share_target=target_from_compact("207fffff"),
                share_difficulty=Decimal("1"),
                transaction_hexes=(),
            ),
            template={
                "previousblockhash": hash_a,
                "height": 11,
                "coinbasevalue": 50_00000000,
            },
            found_block={"network_difficulty": 1},
            issued_at_ms=12345,
            collection_only=False,
            worker=state.worker,
            shares_json=[],
            prior_balances=[],
        )
        server.jobs["job-2"] = context_b
        submission_b = SimpleNamespace(
            header_hex="b2" * 80,
            coinbase_tx_hex="c0ffee",
            block_hex="01",
            block_hash_hex=hash_b,
            share_pass=True,
            block_pass=True,
        )

        class TwoBlockRpc:
            def __init__(self) -> None:
                self.tip = old_tip
                self.height = 9
                self.hashes = {9: old_tip}
                self.submitted: list[str] = []

            def call(self, method: str, params: object = None) -> object:
                if method == "getbestblockhash":
                    return self.tip
                if method == "getblockcount":
                    return self.height
                if method == "submitblock":
                    block_hex = str((params or [""])[0])  # type: ignore[index]
                    block_hash = hash_a if block_hex == "00" else hash_b
                    self.height += 1
                    self.tip = block_hash
                    self.hashes[self.height] = block_hash
                    self.submitted.append(block_hash)
                    return None
                if method == "getblockhash":
                    return self.hashes[int((params or [0])[0])]  # type: ignore[index]
                raise RuntimeError(method)

        server.rpc = TwoBlockRpc()

        sync_gap_open = threading.Event()
        release_sync_gap = threading.Event()
        confirmed_hashes: list[str] = []
        synchronous_results: list[bool] = []
        sync_errors: list[BaseException] = []
        actor_errors: list[BaseException] = []
        synchronous_thread: threading.Thread | None = None
        original_confirm = ledger.confirm_accepted_block
        original_stats = server.accepted_share_stats

        def confirm_accepted_block(
            *,
            block_hash: str,
            active_tip_height: int,
        ) -> dict[str, int | str]:
            result = original_confirm(
                block_hash=block_hash,
                active_tip_height=active_tip_height,
            )
            confirmed_hashes.append(block_hash)
            return result

        def pause_synchronous_gap() -> tuple[int, int]:
            # Only A's synchronous tail pauses; B's accounting-actor landing
            # runs the same aggregate inside the gap unimpeded.
            if threading.current_thread() is synchronous_thread:
                sync_gap_open.set()
                if not release_sync_gap.wait(10):
                    raise AssertionError(
                        "timed out waiting to release the synchronous gap"
                    )
            return original_stats()

        ledger.confirm_accepted_block = confirm_accepted_block  # type: ignore[method-assign]
        server.accepted_share_stats = pause_synchronous_gap  # type: ignore[method-assign]

        def submit_synchronously() -> None:
            try:
                synchronous_results.append(
                    server.handle_submit(
                        state,
                        [
                            "miner-a",
                            "job-1",
                            "00" * 8,
                            "00000001",
                            "00000002",
                        ],
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - asserted below
                sync_errors.append(exc)

        with tempfile.TemporaryDirectory() as tempdir, patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission_a,
        ):
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            audit_store = server._ensure_audit_artifact_store()
            if lease_error_only_inside_publication_guard:
                real_publication_order_guard = audit_store.publication_order_guard

                @contextlib.contextmanager
                def tracking_publication_order_guard():
                    with real_publication_order_guard():
                        publication_guard_holders.add(threading.get_ident())
                        try:
                            yield
                        finally:
                            publication_guard_holders.discard(
                                threading.get_ident()
                            )

                audit_store.publication_order_guard = (  # type: ignore[method-assign]
                    tracking_publication_order_guard
                )
            envelope_a = audit_store.live_envelope_path(
                block_height=10,
                block_hash=hash_a,
            )
            envelope_b = audit_store.live_envelope_path(
                block_height=11,
                block_hash=hash_b,
            )
            synchronous_thread = threading.Thread(target=submit_synchronously)
            synchronous_thread.start()
            actor_thread: threading.Thread | None = None
            try:
                self.assertTrue(
                    sync_gap_open.wait(5),
                    msg=f"synchronous submit exited early: {sync_errors!r}",
                )
                # A is durably confirmed with ordinal 1 and both the payout
                # balance lock and the publication order guard are released,
                # but its live envelope is not yet written.
                self.assertEqual(confirmed_hashes, [hash_a])
                self.assertEqual(ledger.audit_publication_sequence_floor(), 1)
                self.assertFalse(envelope_a.exists())

                # B rides the production handoff: the submitter claims the
                # disposition, offers the block to the node, and transfers the
                # lease to the accounting actor, which lands and publishes.
                server.enqueue_block_candidate(
                    block_candidate(server, state, submission_b, job_id="job-2")
                )
                handoff: list[object] = []
                server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
                    lambda task: (handoff.append(task), True)[1]
                )
                self.assertTrue(
                    server.submit_next_block_candidate(defer_accounting=True)
                )
                self.assertEqual(len(handoff), 1)

                def run_accounting_actor() -> None:
                    try:
                        server._run_block_accounting_task(handoff[0])
                    except BaseException as exc:  # noqa: BLE001 - asserted below
                        actor_errors.append(exc)

                actor_thread = threading.Thread(target=run_accounting_actor)
                actor_thread.start()
                actor_thread.join(10)
                self.assertFalse(actor_thread.is_alive())
                self.assertEqual(actor_errors, [])
                self.assertEqual(confirmed_hashes, [hash_a, hash_b])
                self.assertEqual(ledger.audit_publication_sequence_floor(), 2)
                self.assertTrue(envelope_b.exists())
                # The interleave is real: B published while A had not.
                self.assertFalse(envelope_a.exists())
            finally:
                release_sync_gap.set()
                synchronous_thread.join(10)
                if actor_thread is not None:
                    actor_thread.join(10)

            self.assertFalse(synchronous_thread.is_alive())
            if sync_errors:
                raise sync_errors[0]
            # Whatever happened to A's envelope, the publication itself must
            # have completed on both tails.
            self.assertEqual(synchronous_results, [False])
            self.assertEqual(server.accepted_block_count, 2)

            # The current evidence reference still names the highest
            # published sequence; a superseded publication never moves it.
            self.assertTrue(envelope_b.exists())
            latest = audit_store.latest_evidence()
            assert latest is not None
            self.assertEqual(latest["block_hash"], hash_b)
            self.assertEqual(
                latest["audit_publication_identity"]["sequence"],
                2,
            )
            return SimpleNamespace(
                hash_a=hash_a,
                envelope_a_exists=envelope_a.exists(),
                landed_a=(
                    json.loads(envelope_a.read_text(encoding="utf-8"))
                    if envelope_a.exists()
                    else None
                ),
            )

    def test_interleaved_distinct_hash_landings_write_both_live_envelopes(self) -> None:
        """Both live envelopes survive an interleaved distinct-hash landing.

        A's own height+hash live envelope is its block's only published
        evidence pointer and must be written even though A publishes already
        superseded; skipping it was the silent permanent loss of the original
        defect. This pins the production opt-in wiring end to end: with
        ``restore_superseded_envelope=False`` at the publish call site, this
        test fails.
        """
        result = self._drive_interleaved_distinct_hash_landings()
        self.assertTrue(result.envelope_a_exists)
        assert result.landed_a is not None
        self.assertEqual(result.landed_a["block_hash"], result.hash_a)
        self.assertEqual(result.landed_a["block_height"], 10)

    def test_withheld_lease_authority_skips_restore_without_failing_publication(
        self,
    ) -> None:
        """A failed lease proof degrades the restore; it never fails the landing.

        The fresh-lease fence gates only the superseded-envelope restore
        authority. When it raises, the publication must still complete
        exactly as before the restore existed -- withholding degrades to the
        historical behaviour of skipping the superseded envelope write,
        because failing the publication on a lease hiccup would be a live
        regression on the found-block path.
        """
        from lab.prism.share_ledger import WriterLeaseRenewalDeferred

        result = self._drive_interleaved_distinct_hash_landings(
            lease_error=WriterLeaseRenewalDeferred(
                "writer lease renewal is deferred behind an in-flight write"
            ),
        )
        self.assertFalse(result.envelope_a_exists)

    def test_restore_authority_is_proven_after_the_publication_locks(
        self,
    ) -> None:
        """Deposal during the publication lock wait withholds the restore.

        The fresh-lease proof once ran before the payout balance lock and
        the publication order guard were acquired -- a check-then-act across
        an unbounded wait, so a writer deposed while queued on those locks
        still restored. This fence models that deposal: the proof succeeds
        anywhere outside the publication order guard and fails inside it,
        so the restore is withheld only if authority is evaluated at the
        moment of action, with the locks held.
        """
        from lab.prism.share_ledger import WriterLeaseRenewalDeferred

        result = self._drive_interleaved_distinct_hash_landings(
            lease_error=WriterLeaseRenewalDeferred(
                "writer lease was deposed during the publication lock wait"
            ),
            lease_error_only_inside_publication_guard=True,
        )
        self.assertFalse(result.envelope_a_exists)


class PrismCoordinatorAcceptedBlockGapTests(unittest.TestCase):
    """Blockwait-first acceptance must survive every abandon-capable path.

    Regression coverage for the accepted-block blind spot left open by #89:
    the accepted registry was only written in the direct-accept submit tail,
    so a candidate whose submitblock ack was lost -- acceptance arriving via
    a blockwait tip observation instead -- could be terminally abandoned as
    stale-job by a retry that read one racing chain snapshot. The abandon
    withdrew the landed payout preview, fenced payout publication, and left
    template refreshes coordination-blocked until the watchdog restart.
    """

    def _accepted_tail_scaffolding(self, server: PrismCoordinator, tempdir: str) -> tuple[list[str], list[str]]:
        server._ensure_job_cache_state()
        server.reorg_reconciler_enabled = False
        server.block_candidate_retry_initial_seconds = 0.0
        server.audit_dir = Path(tempdir)
        server.evidence_path = Path(tempdir) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **kwargs: verified_audit_report(
                block_height=int(kwargs["expected_block_height"])
            )
        )
        submitted: list[str] = []
        abandoned: list[str] = []
        server.ledger.mark_block_candidate_submitted = (  # type: ignore[attr-defined]
            lambda **kwargs: submitted.append(str(kwargs["block_hash"])) or True
        )
        server.ledger.mark_block_candidate_abandoned = (  # type: ignore[attr-defined]
            lambda **kwargs: abandoned.append(str(kwargs["block_hash"])) or True
        )
        return submitted, abandoned

    def _age_retained_acceptance_past_window(
        self, server: PrismCoordinator
    ) -> None:
        """Backdate any stashed definitive ack past the acceptance window.

        These interleavings pin the terminal machinery that runs once
        first-party acceptance evidence has gone stale; a fresh stash
        correctly vetoes the terminal decision and defers instead (see
        test_definitive_ack_with_unprovable_chain_view_defers_not_abandons).
        """
        real_stash = server._stash_retained_block_candidate_node_submission

        def stash_then_age(block_hash: str, node_submission: object) -> None:
            real_stash(block_hash, node_submission)
            with server.lock:
                stamped = getattr(
                    server,
                    "_block_candidate_retained_submission_monotonic",
                    None,
                )
                if stamped and block_hash.lower() in stamped:
                    stamped[block_hash.lower()] -= 400.0

        server._stash_retained_block_candidate_node_submission = stash_then_age  # type: ignore[method-assign]

    def _retained_candidate(self, server: PrismCoordinator) -> PrismBlockCandidate:
        with server.lock:
            candidate = server._retry_block_candidate
            server._retry_block_candidate = None
        self.assertIsNotNone(candidate)
        return candidate

    def _chained_context(
        self,
        server: PrismCoordinator,
        *,
        job_id: str,
        parent_hash: str,
        height: int,
    ) -> None:
        base = server.jobs["job-1"]
        server.jobs[job_id] = SimpleNamespace(
            job=SimpleNamespace(
                job_id=job_id,
                share_target=base.job.share_target,
                share_difficulty=base.job.share_difficulty,
                transaction_hexes=(),
            ),
            template={
                "previousblockhash": parent_hash,
                "height": height,
                "coinbasevalue": 50_00000000,
            },
            found_block={"network_difficulty": 1},
            issued_at_ms=12345,
            collection_only=False,
            worker=base.worker,
            shares_json=[],
            prior_balances=[],
        )

    def test_blockwait_observed_acceptance_survives_stale_chain_probe(self) -> None:
        # The 2026-07-25 mainnet interleaving, end to end: lost submitblock
        # ack, blockwait reports the pool's own hash as the new tip, and the
        # retry disposition reads a racing snapshot in which the tip moved on
        # and the header probe cannot prove the candidate active. The
        # candidate must defer (never terminally abandon), keep its landed
        # payout preview, and finalize as submitted once the view settles --
        # with no "qbit accepted direct" tail having run before the defer.
        parent = "00" * 32
        block_hash = "b1" * 32
        racing_tip = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )

            # Attempt 1: submitblock lands the block but the ack is lost.
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(rpc.submitblock_calls, 1)
            self.assertIn(block_hash, server._accepted_block_payout_previews)
            self.assertTrue(
                server._accepted_block_payout_previews[block_hash].landed
            )
            candidate = self._retained_candidate(server)

            # Blockwait sees the pool's own hash as the new tip. This is the
            # only acceptance signal: no direct-submit ack ever arrived and
            # the accepted success tail has not run.
            self.assertTrue(server.observe_tip_for_refresh(block_hash))
            self.assertIn(block_hash, server._tip_observed_accepted_block_hashes)
            self.assertNotIn(block_hash, server._accounted_accepted_block_hashes)

            # Attempt 2: the disposition reads a racing snapshot -- tip moved
            # past the candidate and the header probe reports not-found. #89
            # abandoned here; the observation evidence must defer instead.
            rpc.racing_tip = racing_tip
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertNotIn(
                PRISM_REJECTION_STALE_JOB,
                getattr(server, "block_candidate_abandoned_counts", {}),
            )
            self.assertEqual(abandoned, [])
            self.assertEqual(server.block_candidate_accept_pending_defer_count, 1)
            # The landed preview is preserved -- not withdrawn, not
            # tombstoned -- so payout publication stays unfenced and the
            # reconcile/template path stays unblocked.
            self.assertIn(block_hash, server._accepted_block_payout_previews)
            self.assertTrue(
                server._accepted_block_payout_previews[block_hash].landed
            )
            self.assertNotIn(
                block_hash,
                server._invalidated_accepted_block_payout_previews,
            )
            self.assertFalse(server._payout_state_publication_blocked)
            candidate = self._retained_candidate(server)

            # Attempt 3: the chain view settles with the pool's own block as
            # the tip; the resume path runs the full accepted success tail
            # and finalizes the durable outbox as submitted.
            rpc.racing_tip = None
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(submitted, [block_hash])
            self.assertEqual(abandoned, [])
            self.assertEqual(rpc.submitblock_calls, 3)
            self.assertIn(block_hash, server._accounted_accepted_block_hashes)
            self.assertEqual(server.accepted_block_count, 1)
            self.assertEqual(len(ledger.persisted), 1)
            self.assertEqual(len(ledger.confirmed), 1)
            self.assertNotIn(block_hash, server._accepted_block_payout_previews)
            self.assertNotIn(
                block_hash,
                server._invalidated_accepted_block_payout_previews,
            )
            self.assertFalse(server._payout_state_publication_blocked)
            self.assertIsNone(
                server._accepted_block_payout_transition_for_parent(
                    block_hash,
                    parent_height=10,
                )
            )
            self.assertNotIn(
                block_hash, server._outstanding_block_candidate_hashes
            )
            self.assertNotIn(
                block_hash, server._tip_observed_accepted_block_hashes
            )
            self.assertIn(
                "qbit_prism_block_candidate_accept_pending_defers_total 1",
                server.metrics_payload(),
            )

    def test_three_blocks_quick_succession_lost_acks_finalize_all(self) -> None:
        # Three pool blocks land back to back (the 20:58 / 21:06:58 /
        # 21:07:20 pattern), every submitblock ack is lost, and one retry
        # even reads a transient foreign-tip snapshot. All three candidates
        # must finalize as submitted with payout publication unfenced.
        parent = "00" * 32
        hashes = ["b1" * 32, "b2" * 32, "b3" * 32]
        block_hexes = ["0a", "0b", "0c"]
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 5
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex=dict(zip(block_hexes, hashes)),
            )
            server.rpc = rpc
            parents = [parent, hashes[0], hashes[1]]
            for index, (block_hash, block_hex) in enumerate(
                zip(hashes, block_hexes)
            ):
                job_id = f"job-chain-{index}"
                self._chained_context(
                    server,
                    job_id=job_id,
                    parent_hash=parents[index],
                    height=10 + index,
                )
                candidate = block_candidate(
                    server,
                    state,
                    SimpleNamespace(
                        coinbase_tx_hex="c0ffee",
                        block_hash_hex=block_hash,
                        block_hex=block_hex,
                        share_pass=True,
                        block_pass=True,
                    ),
                    job_id=job_id,
                )

                # Lost-ack landing.
                self.assertTrue(
                    server._submit_next_block_candidate_writer(candidate)
                )
                candidate = self._retained_candidate(server)
                # Blockwait-first acceptance.
                self.assertTrue(server.observe_tip_for_refresh(block_hash))

                if index == 1:
                    # One retry reads a transient foreign-tip snapshot in
                    # which nothing can be proven active; it must defer.
                    rpc.racing_tip = "77" * 32
                    self.assertTrue(
                        server._submit_next_block_candidate_writer(candidate)
                    )
                    self.assertEqual(abandoned, [])
                    rpc.racing_tip = None
                    candidate = self._retained_candidate(server)

                # Settled view: the resume path finalizes as submitted.
                self.assertTrue(
                    server._submit_next_block_candidate_writer(candidate)
                )
                self.assertIn(
                    block_hash, server._accounted_accepted_block_hashes
                )

            self.assertEqual(submitted, hashes)
            self.assertEqual(abandoned, [])
            self.assertEqual(server.accepted_block_count, 3)
            self.assertEqual(
                getattr(server, "block_candidate_abandoned_counts", {}),
                {},
            )
            self.assertEqual(len(ledger.persisted), 3)
            self.assertEqual(len(ledger.confirmed), 3)
            self.assertFalse(server._payout_state_publication_blocked)
            self.assertEqual(server._accepted_block_payout_previews, {})
            self.assertEqual(
                server._invalidated_accepted_block_payout_previews, {}
            )
            self.assertEqual(server._outstanding_block_candidate_hashes, set())
            self.assertEqual(server._tip_observed_accepted_block_hashes, {})

    def test_observed_window_expiry_keeps_genuinely_stale_abandon_terminal(self) -> None:
        # A candidate that was once observed as the tip but has stayed off
        # the active chain past the observation window is genuinely stale
        # (permanently reorged out): the terminal abandon and its payout
        # preview withdrawal must still happen.
        parent = "00" * 32
        block_hash = "d4" * 32
        server, state, _ledger = submit_coordinator(tip=parent)
        server.observed_tip_accept_window_seconds = 60.0
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            candidate = self._retained_candidate(server)
            self.assertTrue(server.observe_tip_for_refresh(block_hash))

            # The chain moved on without the candidate and the observation
            # aged out: the block was reorged away for good.
            rpc.racing_tip = "77" * 32
            rpc.active.pop(block_hash, None)
            with server.lock:
                server._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic() - 61.0
                )

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB],
                1,
            )
            self.assertNotIn(block_hash, server._accepted_block_payout_previews)
            self.assertNotIn(block_hash, server._accounted_accepted_block_hashes)

    def test_block_candidate_acceptance_pending_probe_semantics(self) -> None:
        block_hash = "e5" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.observed_tip_accept_window_seconds = 60.0
        server._register_outstanding_block_candidate(block_hash)

        def observed(age_seconds: float | None) -> None:
            with server.lock:
                server._tip_observed_accepted_block_hashes.pop(block_hash, None)
                if age_seconds is not None:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic() - age_seconds
                    )

        # Own hash is the fresh best tip: accepted, and the probe itself
        # registers the observation for later checks.
        observed(None)
        server.rpc = AcceptanceProbeRpc(tip=block_hash)
        self.assertTrue(server._block_candidate_acceptance_pending(block_hash))
        self.assertIn(block_hash, server._tip_observed_accepted_block_hashes)

        # Active header at the expected height: accepted.
        observed(None)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 10, "confirmations": 2},
        )
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Provably active at the wrong height: abandonable even when the
        # hash was recently observed.
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 9, "confirmations": 2},
        )
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Unprovable probe + fresh observation: accepted (defer).
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Unprovable probe + expired observation: genuinely stale.
        observed(61.0)
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Unprovable probe + no observation: genuinely stale.
        observed(None)
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Probe failure + fresh observation: acceptance evidence wins.
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, fail=True)
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Probe failure + no observation: unchanged legacy behavior.
        observed(None)
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # A best-tip lookup failure must not suppress the active-header
        # probe: the header alone proves acceptance, with no observation.
        observed(None)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 10, "confirmations": 2},
            fail_tip=True,
        )
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # ... and the header's wrong-height verdict also stands alone,
        # overriding even a fresh observation.
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 9, "confirmations": 2},
            fail_tip=True,
        )
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

    def test_post_persist_stale_view_defers_before_rejecting_prepared_rows(self) -> None:
        # Codex P1 / Bugbot: the post-persistence active-hash check could
        # reject the prepared payout rows and only then reach the abandon,
        # which now defers on acceptance evidence -- leaving rows
        # rejected/reversed that a later confirmation can never promote.
        # The acceptance check must run BEFORE reject_prepared_block.
        parent = "00" * 32
        block_hash = "c7" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            candidate = self._retained_candidate(server)
            self.assertTrue(server.observe_tip_for_refresh(block_hash))

            # Retry resumes on the own-hash tip, but the post-persist
            # getblockhash read races to a foreign winner for the height.
            rpc.getblockhash_override = racing_winner
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            # Deferred without touching the prepared payout rows.
            self.assertEqual(ledger.rejected, [])
            self.assertEqual(abandoned, [])
            self.assertNotIn(
                "block-stale",
                getattr(server, "block_candidate_abandoned_counts", {}),
            )
            candidate = self._retained_candidate(server)

            # The view settles with the pool's block active again: the
            # replayed tail confirms the still-prepared rows.
            rpc.getblockhash_override = None
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(submitted, [block_hash])
            self.assertEqual(ledger.rejected, [])
            self.assertEqual(len(ledger.confirmed), 1)
            self.assertIn(block_hash, server._accounted_accepted_block_hashes)

    def test_post_persist_stale_view_still_rejects_without_acceptance_evidence(self) -> None:
        # The terminal half of the same site: submitblock succeeds but a
        # sibling steals the height right after persistence and the pool's
        # block was never observed as the tip. With no FRESH acceptance
        # evidence (the retained ack has aged past the acceptance window),
        # the terminal decision seals first and only then are the prepared
        # rows rejected, exactly once.
        parent = "00" * 32
        block_hash = "c8" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            # The definitive ack retained at the offer would veto the
            # terminal decision while fresh; these tests pin the sealed
            # terminal machinery, so age it past the acceptance window.
            self._age_retained_acceptance_past_window(server)
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                # A sibling wins the height after persistence: the tip and
                # the height's active hash both belong to the foreign winner
                # and the pool's block never became the chain tip.
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(len(ledger.rejected), 1)
            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts["block-stale"],
                1,
            )
            self.assertNotIn(block_hash, server._accounted_accepted_block_hashes)

    def test_acceptance_evidence_arriving_during_withdrawal_defers(self) -> None:
        # Bugbot: the payout-preview withdrawal inside the abandon can block
        # long enough for a blockwait observation to arrive; the terminal
        # commitment must consult that late evidence, not only the
        # completed-tail registry.
        block_hash = "a9" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)
        real_clear = server._clear_accepted_block_payout_preview

        def observing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                # A blockwait observation lands while the withdrawal blocks.
                with server.lock:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic()
                    )
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = observing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(
            getattr(outcome, "reason", None),
            PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            getattr(server, "block_candidate_abandoned_counts", {}),
        )
        self.assertEqual(server.block_candidate_accept_pending_defer_count, 1)
        # The landed barrier is restored (and its fail-closed tombstone
        # popped) so descendant builders wait on the preview instead of
        # failing closed while the deferred retry heals it.
        self.assertIn(block_hash, server._accepted_block_payout_previews)
        self.assertTrue(
            server._accepted_block_payout_previews[block_hash].landed
        )
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_replay_restores_acceptance_evidence_from_durable_block_state(self) -> None:
        # Codex round 5: the observation registry dies with the process. A
        # durable prepared/confirmed pool-block row proves a prior process's
        # submitblock succeeded, so startup replay must restore acceptance
        # evidence before a replay disposition can race a transient fork
        # view and terminally abandon the accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        accepted_hash = "e7" * 32
        unproven_hash = "e8" * 32
        unreadable_hash = "e9" * 32
        for index, block_hash in enumerate(
            (accepted_hash, unproven_hash, unreadable_hash), start=1
        ):
            pending = PendingShare(
                share_id=f"miner-a:{block_hash}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=9,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=index,
                ntime=1,
            )
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
                pending_share=pending,
            )
            ledger.append_batch(
                [(pending, server.block_candidate_intent(candidate))]
            )
        def durable_block_state(*, block_hash: str) -> dict[str, object] | None:
            if block_hash == accepted_hash:
                return {"chain_state": "prepared", "maturity_state": "immature"}
            if block_hash == unreadable_hash:
                raise RuntimeError("postgres unavailable")
            return None

        ledger.pool_block_state = durable_block_state  # type: ignore[attr-defined]

        self.assertEqual(server.replay_pending_block_candidates(), 3)
        replayed = [
            server._block_replay_candidate_queue.get_nowait()
            for _ in range(3)
        ]
        for candidate in replayed:
            server._restore_replayed_candidate_acceptance_evidence(candidate)

        self.assertIn(accepted_hash, server._tip_observed_accepted_block_hashes)
        self.assertIn(accepted_hash, server._outstanding_block_candidate_hashes)
        # No durable acceptance proof: replays without synthetic evidence.
        self.assertNotIn(
            unproven_hash, server._tip_observed_accepted_block_hashes
        )
        # An unreadable durable state fails safe: the candidate replays
        # protected, bounded by the observation window, instead of exposed
        # to a transient fork view racing its first disposition.
        self.assertIn(
            unreadable_hash, server._tip_observed_accepted_block_hashes
        )

    def test_late_defer_republishes_withdrawn_preview_and_unfences(self) -> None:
        # Bugbot round 5: when the withdrawal's own publication loses its
        # race, delivery is fenced before the late-acceptance defer runs.
        # The defer must republish the withdrawn preview so admission
        # reopens immediately instead of staying coordination-blocked
        # across the deferral cycles.
        block_hash = "ba" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server._publish_accepted_block_payout_preview(block_hash, [])
        self.assertFalse(server._payout_state_publication_blocked)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)

        real_publish = server._publish_payout_state_candidate

        def withdrawal_publication_lost(candidate: object) -> object:
            if getattr(candidate, "accepted_block_withdrawal", False):
                return None
            return real_publish(candidate)

        server._publish_payout_state_candidate = withdrawal_publication_lost  # type: ignore[method-assign]
        real_clear = server._clear_accepted_block_payout_preview

        def observing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                with server.lock:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic()
                    )
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = observing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(
            getattr(outcome, "reason", None),
            PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
        )
        # The barrier is restored AND publication admission reopened: the
        # withdrawn preview was republished despite the withdrawal's own
        # publication loss having fenced delivery.
        self.assertIn(block_hash, server._accepted_block_payout_previews)
        self.assertTrue(
            server._accepted_block_payout_previews[block_hash].landed
        )
        self.assertIsNotNone(
            server._accepted_block_payout_previews[block_hash].preview
        )
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        self.assertFalse(server._payout_state_publication_blocked)

    def test_late_gate_reprobes_after_withdrawal_heals_buried_block(self) -> None:
        # Bugbot round 7: an unknown pre-withdrawal probe verdict must
        # re-probe after the (blocking) withdrawal. A buried accepted block
        # is not always re-observed as the tip -- blockwait reports only the
        # newest of rapid connects -- so the recovered probe alone, with no
        # observation evidence at all, must defer the terminal abandon.
        block_hash = "bc" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)
        server.rpc = rpc
        real_clear = server._clear_accepted_block_payout_preview

        def healing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                # The chain view heals while the withdrawal blocks: the
                # candidate is provably active again, two blocks deep.
                rpc.header = {"height": 10, "confirmations": 2}
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = healing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(
            getattr(outcome, "reason", None),
            PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            getattr(server, "block_candidate_abandoned_counts", {}),
        )
        self.assertIn(block_hash, server._accepted_block_payout_previews)
        self.assertTrue(
            server._accepted_block_payout_previews[block_hash].landed
        )
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_wrong_height_probe_verdict_overrules_late_observation(self) -> None:
        # Bugbot round 6: the probe must win both directions in the late
        # check too. An observation arriving during the withdrawal cannot
        # revive a candidate the pre-check probe proved active at the wrong
        # height -- header heights are immutable, so that verdict stands and
        # the abandon commits terminally instead of deferring to window
        # expiry.
        block_hash = "bb" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 9, "confirmations": 2},
        )
        real_clear = server._clear_accepted_block_payout_preview

        def observing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                with server.lock:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic()
                    )
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = observing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            "block-stale",
            "candidate active at unexpected height 9",
            block_hash=block_hash,
            worker=None,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(getattr(outcome, "reason", None), "block-stale")
        self.assertNotIn(
            "block-stale",
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "block_candidate_accept_pending_defer_count", 0),
            0,
        )

    def test_terminal_seal_excludes_observations_after_the_commit(self) -> None:
        # Codex round 3: acceptance evidence arriving after the terminal
        # commit but before the follow-up durable rejection must be excluded
        # deterministically. The terminal commit seals the hash (drops it
        # from outstanding) in the same critical section, so a later
        # observation is a no-op instead of contradicting the sealed
        # disposition mid-rejection.
        block_hash = "b8" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertNotIn(block_hash, server._outstanding_block_candidate_hashes)

        # A blockwait observation lands right after the sealed commit: it
        # must not register acceptance evidence for the sealed hash.
        self.assertTrue(server.observe_tip_for_refresh(block_hash))
        self.assertNotIn(block_hash, server._tip_observed_accepted_block_hashes)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertIsNotNone(outcome)
        server._record_committed_block_candidate_abandonment(block_hash, outcome)
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB],
            1,
        )

    def test_observation_during_prepared_row_rejection_cannot_split_state(self) -> None:
        # The exact round-3 interleaving at the post-persist site: a
        # blockwait observation fires inside the gap between the sealed
        # terminal decision and reject_prepared_block (during the
        # getblockcount call). The seal excludes it, so the rows are
        # rejected exactly once with no contradictory evidence registered
        # and no defer emitted after the seal.
        parent = "00" * 32
        block_hash = "c9" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            # The definitive ack retained at the offer would veto the
            # terminal decision while fresh; these tests pin the sealed
            # terminal machinery, so age it past the acceptance window.
            self._age_retained_acceptance_past_window(server)
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            race_state: dict[str, bool] = {"post_persist": False}
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                race_state["post_persist"] = True
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]
            real_call = rpc.call

            def observing_call(
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount" and race_state["post_persist"]:
                    # Blockwait reports the candidate as tip inside the
                    # seal -> reject gap.
                    server.observe_tip_for_refresh(block_hash)
                return real_call(method, params)

            rpc.call = observing_call  # type: ignore[method-assign]

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(len(ledger.rejected), 1)
            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts["block-stale"],
                1,
            )
            self.assertNotIn(
                block_hash, server._tip_observed_accepted_block_hashes
            )
            self.assertNotIn(
                block_hash, server._outstanding_block_candidate_hashes
            )
            self.assertEqual(
                getattr(server, "block_candidate_accept_pending_defer_count", 0),
                0,
            )

    def test_retained_candidate_reregisters_for_tip_observations(self) -> None:
        # Codex round 8: the terminal seal stops observation matching, but
        # when the terminal cleanup itself fails (reject_prepared_block
        # raising) the candidate is retained for retry -- and evidence
        # arriving during that backoff gap must register again, not vanish.
        parent = "00" * 32
        block_hash = "cd" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            # The definitive ack retained at the offer would veto the
            # terminal decision while fresh; these tests pin the sealed
            # terminal machinery, so age it past the acceptance window.
            self._age_retained_acceptance_past_window(server)
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]
            reject_attempts: list[dict[str, object]] = []

            def failing_reject(**kwargs: object) -> dict[str, object]:
                reject_attempts.append(kwargs)
                raise RuntimeError("psql briefly unavailable")

            ledger.reject_prepared_block = failing_reject  # type: ignore[method-assign]
            ledger.pool_block_state = (  # type: ignore[attr-defined]
                lambda *, block_hash: {
                    "chain_state": "prepared",
                    "maturity_state": "immature",
                }
            )

            # The sealed terminal pass aborts inside the rejection (site
            # attempt, then the writer's terminal-cleanup attempt): the
            # candidate is retained and must match observations again.
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(len(reject_attempts), 2)
            self.assertIsNotNone(getattr(server, "_retry_block_candidate", None))
            self.assertIn(
                block_hash, server._outstanding_block_candidate_hashes
            )
            self.assertNotIn(
                "block-stale",
                server.block_candidate_abandoned_counts,
            )

            # Blockwait reports the pool's own hash during the backoff gap:
            # the evidence registers instead of vanishing behind the seal.
            self.assertTrue(server.observe_tip_for_refresh(block_hash))
            self.assertIn(
                block_hash, server._tip_observed_accepted_block_hashes
            )
            self.assertEqual(abandoned, [])

            # The retry can subsequently prove the candidate active and
            # complete as submitted. The earlier reversible seals must never
            # leave a contradictory abandonment count behind.
            ledger.persist_accepted_block = real_persist  # type: ignore[method-assign]
            rpc.tip = block_hash
            rpc.height = 10
            rpc.active[block_hash] = 10
            rpc.getblockhash_override = None
            recovered_candidate = self._retained_candidate(server)
            self.assertTrue(
                server._submit_next_block_candidate_writer(recovered_candidate)
            )
            self.assertEqual(submitted, [block_hash])
            self.assertEqual(abandoned, [])
            self.assertEqual(server.accepted_block_count, 1)
            self.assertNotIn(
                "block-stale",
                server.block_candidate_abandoned_counts,
            )

    def test_definitive_ack_with_unprovable_chain_view_defers_not_abandons(
        self,
    ) -> None:
        # Release-review finding: a definitive submitblock success followed
        # by a chain view that cannot prove the block landed (foreign tip,
        # header unknown, height taken by a sibling in that instant's view)
        # must defer as accept-pending on the strength of the retained ack
        # -- never terminally abandon accounting for a block the node
        # accepted. The stash ages on the observation window, so a real
        # orphan still terminalizes once the ack is stale (the sealed
        # rejection tests above).
        parent = "00" * 32
        block_hash = "ce" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            # Deferred, not terminal: prepared rows untouched, candidate
            # retained for retry, observation matching still open.
            self.assertEqual(ledger.rejected, [])
            self.assertEqual(abandoned, [])
            self.assertEqual(submitted, [])
            self.assertGreaterEqual(
                getattr(server, "block_candidate_accept_pending_defer_count", 0),
                1,
            )
            self.assertIsNotNone(getattr(server, "_retry_block_candidate", None))
            self.assertNotIn(
                "block-stale",
                server.block_candidate_abandoned_counts,
            )
            self.assertIn(
                block_hash, server._outstanding_block_candidate_hashes
            )

    def test_retained_acceptance_evidence_ages_on_observation_window(
        self,
    ) -> None:
        # The stash is acceptance evidence only while fresh: past the
        # observation window it stands down so a genuinely orphaned block
        # (whose probes never prove anything either way) regains
        # abandonability instead of deferring forever.
        server, _state, _ledger = submit_coordinator()
        block_hash = "cf" * 32
        server._stash_retained_block_candidate_node_submission(
            block_hash,
            SimpleNamespace(attempted=True, error=None, result=None),
        )
        self.assertTrue(server._block_candidate_acceptance_retained(block_hash))
        with server.lock:
            server._block_candidate_retained_submission_monotonic[
                block_hash
            ] -= 400.0
        self.assertFalse(server._block_candidate_acceptance_retained(block_hash))
        # A non-positive window means evidence never expires, mirroring the
        # tip-observation semantics.
        server.observed_tip_accept_window_seconds = 0.0
        self.assertTrue(server._block_candidate_acceptance_retained(block_hash))

    def test_pool_closed_gate_requires_probe_proven_acceptance(self) -> None:
        # Bugbot: observation evidence alone must not open the pool-closed
        # gate -- an off-chain candidate would fall through toward
        # submitblock after the pool stopped accepting blocks. Unprovable
        # views defer via the abandon path; expired evidence goes terminal.
        parent = "00" * 32
        block_hash = "d9" * 32
        server, state, _ledger = submit_coordinator(tip=parent)
        server.max_blocks = 1
        server.stop_after_block = False
        server.accepted_block_count = 1
        server.observed_tip_accept_window_seconds = 60.0
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            server._register_outstanding_block_candidate(block_hash)
            with server.lock:
                server._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic()
                )
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )

            # Fresh observation, unprovable chain view: defer, and above all
            # never reach submitblock through the closed pool.
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(rpc.submitblock_calls, 0)
            self.assertEqual(abandoned, [])
            candidate = self._retained_candidate(server)

            # Evidence expires with the candidate still absent: terminal.
            with server.lock:
                server._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic() - 61.0
                )
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(rpc.submitblock_calls, 0)
            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts[
                    PRISM_REJECTION_POOL_CLOSED
                ],
                1,
            )

    def test_pool_closed_gate_yields_to_on_chain_candidate(self) -> None:
        # A candidate already on the active chain must complete its payout
        # accounting even after the pool stops accepting new blocks; the
        # pool-closed gate previously abandoned it terminally.
        parent = "00" * 32
        block_hash = "f6" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 1
        server.stop_after_block = False
        server.accepted_block_count = 1
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            # The block is already active with the pool's own hash as tip
            # (for example after a watchdog restart replayed the outbox).
            rpc.tip = block_hash
            rpc.height = 10
            rpc.active[block_hash] = 10
            server.rpc = rpc
            server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
                lambda _tip, *, _coalesce_same_tip: not _coalesce_same_tip
            )
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(submitted, [block_hash])
            self.assertEqual(abandoned, [])
            self.assertNotIn(
                PRISM_REJECTION_POOL_CLOSED,
                getattr(server, "block_candidate_abandoned_counts", {}),
            )
            self.assertIn(block_hash, server._accounted_accepted_block_hashes)
            self.assertEqual(server.accepted_block_count, 2)


class PrismCoordinatorReliabilityTests(unittest.TestCase):
    def _bare_coordinator(self) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.lock = threading.RLock()
        server.stop_event = threading.Event()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 120.0
        server.watchdog_interval_seconds = 15.0
        return server

    def test_block_candidate_progress_stamps_only_on_submitter_thread(self) -> None:
        server = self._bare_coordinator()
        clock = {"now": 1000.0}
        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            server._record_heartbeat("block_submitter")
            clock["now"] += server.watchdog_timeout_seconds + 1.0
            self.assertEqual(
                server._overdue_heartbeats(clock["now"]), ["block_submitter"]
            )
            # Without a registered owner the helper never stamps.
            server._record_block_candidate_progress()
            self.assertEqual(
                server._overdue_heartbeats(clock["now"]), ["block_submitter"]
            )
            # The owner thread's stamps clear the overdue state.
            server._block_submitter_thread_ident = threading.get_ident()
            server._record_block_candidate_progress()
            self.assertEqual(server._overdue_heartbeats(clock["now"]), [])

    def test_client_thread_disposition_cannot_mask_frozen_submitter(self) -> None:
        server = self._bare_coordinator()
        clock = {"now": 1000.0}
        owner_ready = threading.Event()
        release_owner = threading.Event()

        def frozen_owner() -> None:
            server._block_submitter_thread_ident = threading.get_ident()
            server._record_heartbeat("block_submitter")
            owner_ready.set()
            release_owner.wait(5)

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            owner = threading.Thread(target=frozen_owner)
            owner.start()
            try:
                self.assertTrue(owner_ready.wait(5))
                clock["now"] += server.watchdog_timeout_seconds + 1.0
                self.assertEqual(
                    server._overdue_heartbeats(clock["now"]), ["block_submitter"]
                )
                # A client connection thread reaching a disposition boundary
                # (synchronous below-target solve) must not refresh the
                # frozen dedicated thread's liveness budget.
                foreign = threading.Thread(
                    target=server._record_block_candidate_progress
                )
                foreign.start()
                foreign.join(5)
                self.assertEqual(
                    server._overdue_heartbeats(clock["now"]), ["block_submitter"]
                )
            finally:
                release_owner.set()
                owner.join(5)

    def test_audit_verifier_subprocess_has_explicit_timeout(self) -> None:
        server = self._bare_coordinator()
        server.bundle_build_timeout_seconds = 0.25
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir) / "audit"
            server.evidence_path = Path(tempdir) / "evidence.json"
            store = server._ensure_audit_artifact_store()
            try:
                # The verifier budget is the coordinator-configured bundle
                # build budget; the store must never silently fall back to a
                # fixed default while a shorter budget is configured.
                self.assertEqual(store.verifier_timeout_seconds, 0.25)
                started = time.monotonic()
                with patch(
                    "lab.prism.audit_artifacts.prism_tool_command",
                    lambda _name: ["python3", "-c", "import time; time.sleep(5)"],
                ):
                    with self.assertRaisesRegex(RuntimeError, "timed out"):
                        server.verify_bundle(
                            Path(tempdir) / "candidate.json",
                            "00",
                            "11" * 32,
                            expected_coinbase_value_sats=1,
                        )
                self.assertLess(time.monotonic() - started, 5.0)
            finally:
                store.close()

    def test_sixty_second_attempt_mark_stall_does_not_delay_rpc_or_heartbeat(self) -> None:
        server, state, recording = submit_coordinator()
        entered_mark = threading.Event()
        release_mark = threading.Event()
        submitted = threading.Event()

        class StallingLedger(RecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.mark_calls = 0

            def mark_block_candidate_attempted(self, *, block_hash: str) -> bool:
                self.mark_calls += 1
                entered_mark.set()
                release_mark.wait(60.0)
                return True

        class DeadlineRpc(SubmitRpc):
            def __init__(self, ledger: RecordingLedger) -> None:
                super().__init__(
                    tip="00" * 32,
                    block_hash="d2" * 32,
                    ledger=ledger,
                )
                self.timeouts: list[float | None] = []

            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    self.timeouts.append(timeout)
                    submitted.set()
                    if len(self.timeouts) > 1:
                        return "duplicate"
                return super().call(method, params)

        ledger = StallingLedger()
        server.ledger = ledger
        rpc = DeadlineRpc(ledger)
        server.rpc = rpc
        server.block_submit_rpc_timeout_seconds = 0.4
        server.block_submit_db_timeout_seconds = 0.05
        server.block_candidate_retry_initial_seconds = 0.01
        server.block_candidate_retry_max_seconds = 0.01
        server.watchdog_timeout_seconds = 0.2
        server._heartbeats = {}
        server._heartbeat_phases = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="d2" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )
        server.enqueue_block_candidate(candidate)
        started = time.monotonic()
        submitter = threading.Thread(target=server.block_submit_loop)
        with patch("builtins.print"):
            submitter.start()
            try:
                self.assertTrue(submitted.wait(2.0))
                self.assertLess(time.monotonic() - started, 2.0)
                self.assertTrue(entered_mark.wait(1.0))
                with server._heartbeats_lock:
                    first_heartbeat = server._heartbeats["block_submitter"]
                heartbeat_deadline = time.monotonic() + 1.0
                while time.monotonic() < heartbeat_deadline:
                    with server._heartbeats_lock:
                        latest_heartbeat = server._heartbeats["block_submitter"]
                    if latest_heartbeat > first_heartbeat:
                        break
                    time.sleep(0.01)
                self.assertGreater(latest_heartbeat, first_heartbeat)
                self.assertEqual(
                    server._overdue_heartbeats(time.monotonic()),
                    [],
                )
                self.assertEqual(ledger.mark_calls, 1)
                self.assertEqual(rpc.timeouts[0], 0.4)
            finally:
                server.stop_event.set()
                submitter.join(2.0)
                release_mark.set()
        self.assertFalse(submitter.is_alive())

    def test_accounting_retry_backoff_defers_instead_of_sleeping_under_admission(self) -> None:
        server, state, _recording = submit_coordinator()
        server.block_candidate_retry_initial_seconds = 30.0
        server.block_candidate_retry_max_seconds = 30.0

        class FailingMarkLedger(RecordingLedger):
            def mark_block_candidate_attempted(self, *, block_hash: str) -> bool:
                raise RuntimeError("attempt marker unavailable")

        server.ledger = FailingMarkLedger()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="ab" * 32,
                block_hex="ab",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = "ab" * 32
        server._block_accounting_thread_ident = threading.get_ident()
        server._block_accounting_holds_disposition = True
        started = time.monotonic()
        with patch("builtins.print"):
            handled = server._submit_next_block_candidate_writer(
                candidate,
                node_submission=SimpleNamespace(
                    attempted=False,
                    result=None,
                    error=None,
                ),
                disposition_held=True,
            )
        elapsed = time.monotonic() - started
        self.assertTrue(handled)
        # The 30s backoff must be recorded as a deadline, never slept while
        # the accounting thread holds admission and the disposition lease.
        self.assertLess(elapsed, 10.0)
        self.assertIs(
            getattr(server, "_block_accounting_deferred_retry_candidate", None),
            candidate,
        )
        with server.lock:
            deadline = server._block_candidate_retry_not_before.get(block_hash)
        self.assertIsNotNone(deadline)
        self.assertGreater(deadline, time.monotonic())

        # After the lease releases, the deferred candidate parks in the retry
        # slot and the dequeue honors the backoff deadline.
        server._block_accounting_holds_disposition = False
        with server.lock:
            server._block_accounting_deferred_retry_candidate = None
            server._merge_block_candidate_retry_locked(
                "_retry_block_candidate",
                candidate,
            )
        with patch("builtins.print"):
            self.assertFalse(
                server.submit_next_block_candidate(defer_accounting=True)
            )
        self.assertIs(getattr(server, "_retry_block_candidate", None), candidate)

        # Once the deadline passes the same candidate dequeues again.
        with server.lock:
            server._block_candidate_retry_not_before[block_hash] = (
                time.monotonic() - 1.0
            )
        with patch("builtins.print"):
            self.assertTrue(
                server.submit_next_block_candidate(defer_accounting=True)
            )

    def test_live_retry_reuses_definitive_node_acceptance_without_reoffer(self) -> None:
        parent_hash = "00" * 32
        block_hash = "ce" * 32
        moved_tip = "11" * 32
        ledger = SingleWriterShareLedger()
        server, state, _recording = submit_coordinator(tip=parent_hash)
        server.ledger = ledger
        server.stop_after_block = False
        server.max_blocks = 10
        server.block_candidate_retry_initial_seconds = 0.0
        server.block_candidate_retry_max_seconds = 0.0
        from lab.prism.share_ledger import PendingShare

        pending = PendingShare(
            share_id="miner-a:live-retry-duplicate",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=10,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=2,
            ntime=1_700_000_000,
        )
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        intent = server.block_candidate_intent(candidate)
        ledger.append_batch([(pending, intent)])

        expected_height = int(candidate.context.template["height"])
        header_state = {"confirmations": -1}

        class MovedTipRpc(FakeRpc):
            def __init__(self) -> None:
                self.submit_results: list[object] = []

            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    result = None if not self.submit_results else "duplicate"
                    self.submit_results.append(result)
                    return result
                if method == "getbestblockhash":
                    # The chain advances as soon as the first offer lands, so
                    # the in-process retry observes a moved tip.
                    return parent_hash if not self.submit_results else moved_tip
                if method == "getblockhash":
                    return block_hash
                if method == "getblockcount":
                    return 9
                if method == "getblockheader":
                    # Unprovable during the tip race (found, not yet active),
                    # then settled and active for the final pass.
                    return {
                        "height": expected_height,
                        "confirmations": header_state["confirmations"],
                    }
                return super().call(method, params)

        rpc = MovedTipRpc()
        server.rpc = rpc
        original_mark = ledger.mark_block_candidate_attempted
        mark_state = {"failures": 0}

        def flaky_mark(*, block_hash: str) -> bool:
            # Fail twice, starting at the first attempt marker after the
            # fresh node offer, mirroring statement timeouts under sustained
            # saturation. The retained acceptance must survive the failed
            # retry too, not just the first failure.
            if len(rpc.submit_results) == 1 and mark_state["failures"] < 2:
                mark_state["failures"] += 1
                raise RuntimeError("attempt marker timed out")
            return original_mark(block_hash=block_hash)

        ledger.mark_block_candidate_attempted = flaky_mark  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            with patch("builtins.print"):
                # First pass: fresh accept from the node, then the attempt
                # marker fails and the candidate is retained for an
                # in-process retry carrying the definitive node result.
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(rpc.submit_results, [None])
                self.assertEqual(
                    ledger._block_candidate_outbox[block_hash]["state"],
                    "pending",
                )
                # Retry 1 fails the marker again: the retained acceptance
                # must survive a consumed-and-failed retry, not just the
                # first failure.
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(rpc.submit_results, [None])
                self.assertEqual(
                    ledger._block_candidate_outbox[block_hash]["state"],
                    "pending",
                )
                # Retry 2: the stashed acceptance is still reused, so the
                # node is never asked again (no "duplicate" classification,
                # no chain probe reliance) and the landing tail finalizes
                # exactly as if the first pass had continued.
                self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(rpc.submit_results, [None])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["state"],
            "submitted",
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            getattr(server, "block_candidate_abandoned_counts", {}),
        )
        self.assertEqual(server.accepted_block_count, 1)

    def test_block_accounting_primary_queue_is_bounded_by_default(self) -> None:
        server, _state, _recording = submit_coordinator()
        server._ensure_block_accounting_state()
        # A bounded primary is what makes the documented result-preserving
        # spillover ordering reachable; the overflow queue stays unbounded.
        self.assertEqual(server._block_accounting_queue.maxsize, 8)
        self.assertEqual(server._block_accounting_overflow_queue.maxsize, 0)

    def test_accounting_saturation_does_not_convoy_node_offers(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 10
        server.stop_after_block = False
        server.block_candidate_retry_initial_seconds = 0.01
        server._block_accounting_queue = queue.PriorityQueue(maxsize=1)
        entered_accounting = threading.Event()
        release_accounting = threading.Event()
        submitted: list[str] = []

        class RecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitted.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = RecordingRpc("00" * 32)

        def blocked_accounting(
            _candidate: PrismBlockCandidate,
            **_kwargs: object,
        ) -> bool:
            entered_accounting.set()
            release_accounting.wait(5)
            return True

        server._call_block_candidate_writer = blocked_accounting  # type: ignore[method-assign]
        for tag in ("a1", "b2", "c3", "d4"):
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=tag * 32,
                    block_hex=tag,
                    share_pass=True,
                    block_pass=True,
                ),
            )
            server.enqueue_block_candidate(candidate)

        submitter = threading.Thread(target=server.block_submit_loop)
        with patch("builtins.print"):
            submitter.start()
            try:
                self.assertTrue(entered_accounting.wait(1))
                deadline = time.monotonic() + 2
                while len(submitted) < 4 and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(submitted, ["a1", "b2", "c3", "d4"])
            finally:
                server.stop_event.set()
                release_accounting.set()
                submitter.join(2)
                accounting = getattr(server, "_block_accounting_thread", None)
                if accounting is not None:
                    accounting.join(2)
        self.assertFalse(submitter.is_alive())

    def test_replay_batch_reaches_node_while_oldest_accounting_stalls(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.max_blocks = 10
        server.stop_after_block = False
        submitted: list[str] = []
        release_accounting = threading.Event()
        entered_accounting = threading.Event()

        for index, tag in enumerate(("a5", "b6"), start=1):
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=tag * 32,
                    block_hex=tag,
                    share_pass=True,
                    block_pass=True,
                ),
            )
            pending = PendingShare(
                share_id=f"miner-a:{tag * 32}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=9,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=index,
                ntime=1,
            )
            candidate = dataclass_replace(candidate, pending_share=pending)
            ledger.append_batch(
                [(pending, server.block_candidate_intent(candidate))]
            )

        self.assertEqual(server.replay_pending_block_candidates(), 2)

        class RecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitted.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = RecordingRpc("00" * 32)

        def blocked_accounting(
            _candidate: PrismBlockCandidate,
            **_kwargs: object,
        ) -> bool:
            entered_accounting.set()
            release_accounting.wait(5)
            return True

        server._call_block_candidate_writer = blocked_accounting  # type: ignore[method-assign]
        submitter = threading.Thread(target=server.block_submit_loop)
        with patch("builtins.print"):
            submitter.start()
            try:
                self.assertTrue(entered_accounting.wait(1))
                deadline = time.monotonic() + 2
                while len(submitted) < 2 and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(submitted, ["a5", "b6"])
            finally:
                server.stop_event.set()
                release_accounting.set()
                submitter.join(2)
                accounting = getattr(server, "_block_accounting_thread", None)
                if accounting is not None:
                    accounting.join(2)

    def test_startup_block_replay_timeout_starts_submitter_and_converges(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 10
        server.stop_after_block = False
        server.block_submit_db_timeout_seconds = 0.01
        # The startup enumeration is landing-class (issue #188 fix 4).
        server.block_landing_db_timeout_seconds = 0.01
        server.block_landing_db_timeout_max_seconds = 0.01
        server.block_candidate_retry_initial_seconds = 0.01
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "e5" * 32
        pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=1,
            ntime=1,
        )
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="e5",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )

        query_started = threading.Event()
        release_query = threading.Event()
        query_calls = 0
        startup_call: list[object] = []
        original_pending_rows = ledger.pending_block_candidate_rows

        def slow_pending_rows(*, limit: int = 32) -> list[dict[str, object]]:
            nonlocal query_calls
            query_calls += 1
            if query_calls == 1:
                with server._block_submitter_ledger_calls_lock:
                    startup_call.append(
                        server._block_submitter_ledger_calls[
                            ("replay-outbox-query", 32)
                        ]
                    )
                query_started.set()
                if not release_query.wait(5):
                    raise AssertionError("timed out waiting to release outbox query")
            return original_pending_rows(limit=limit)

        ledger.pending_block_candidate_rows = slow_pending_rows  # type: ignore[method-assign]
        original_record_wait = server._record_block_submitter_wait
        loop_reuse_waiting = threading.Event()

        def observed_record_wait(phase: str) -> None:
            original_record_wait(phase)
            if (
                phase == "replay-outbox-query"
                and getattr(server, "_block_submitter_thread_ident", None)
                == threading.get_ident()
            ):
                loop_reuse_waiting.set()

        server._record_block_submitter_wait = observed_record_wait  # type: ignore[method-assign]

        submitted: list[str] = []

        class RecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitted.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = RecordingRpc("00" * 32)
        accounted: list[str] = []

        def account_candidate(
            replayed: PrismBlockCandidate,
            **_kwargs: object,
        ) -> bool:
            replayed_hash = replayed.submission.block_hash_hex
            accounted.append(replayed_hash)
            ledger.mark_block_candidate_submitted(block_hash=replayed_hash)
            server.stop_event.set()
            return True

        server._call_block_candidate_writer = account_candidate  # type: ignore[method-assign]

        profile = SimpleNamespace(heartbeat_name="stratum_accept_default")
        listener_closed = threading.Event()
        listener = SimpleNamespace(close=listener_closed.set)
        server.listener_profiles = [profile]
        server.bind = "127.0.0.1"
        server.port = 0
        server.min_ready_miners = 1
        server.audit_bind = None
        server.audit_port = 0
        server.hot_path_log_enabled = False
        server.blockwait_enabled = False
        server.vardiff_idle_sweep_seconds = 0.0
        server.stratum_initial_job_timeout_seconds = 0.0
        server.watchdog_enabled = False
        server.watchdog_interval_seconds = 0.1
        server.template_refresh_failure_exit_seconds = 120.0
        server.coordination_blocked_exit_seconds = 120.0
        server.open_stratum_listeners = (  # type: ignore[method-assign]
            lambda _stack: [(listener, profile)]
        )
        server.validate_live_chain_identity = lambda: None  # type: ignore[method-assign]
        server.validate_live_template_and_fee_policy = lambda: None  # type: ignore[method-assign]
        server.prism_payout_policy = lambda: {}  # type: ignore[method-assign]
        server.prewarm_startup_jobs = lambda: None  # type: ignore[method-assign]
        server.watchdog_loop = lambda: None  # type: ignore[method-assign]
        server.blockpoll_loop = lambda: None  # type: ignore[method-assign]
        server.replay_recovered_shares = lambda: 0  # type: ignore[method-assign]
        server.share_append_loop = lambda: None  # type: ignore[method-assign]
        server.shutdown = lambda *, reason="graceful": True  # type: ignore[method-assign]

        def drain_threads(threads: list[tuple[threading.Thread, float]]) -> None:
            for thread, timeout in threads:
                thread.join(timeout)

        server.drain_non_writer_components = drain_threads  # type: ignore[method-assign]

        def accept_after_startup(_listener: object, _profile: object) -> None:
            self.assertTrue(loop_reuse_waiting.wait(2))
            self.assertIsNotNone(
                getattr(server, "_block_submitter_thread_ident", None)
            )
            self.assertTrue(query_started.wait(1))
            # The loop is waiting on the exact startup call while that worker
            # remains blocked; no replacement query was spawned.
            with server._block_submitter_ledger_calls_lock:
                self.assertIs(
                    server._block_submitter_ledger_calls.get(
                        ("replay-outbox-query", 32)
                    ),
                    startup_call[0],
                )
            self.assertEqual(query_calls, 1)
            release_query.set()
            deadline = time.monotonic() + 2
            while not accounted and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(accounted)

        server.accept_loop = accept_after_startup  # type: ignore[method-assign]

        try:
            with patch("builtins.print") as printed:
                server._serve_with_listener_stack(SimpleNamespace())  # type: ignore[arg-type]
        finally:
            server.stop_event.set()
            release_query.set()

        ledger.pending_block_candidate_rows = original_pending_rows  # type: ignore[method-assign]
        startup_logs = [
            " ".join(str(value) for value in call.args)
            for call in printed.call_args_list
        ]
        self.assertTrue(
            any(
                "prism coordinator: startup block candidate replay timed out "
                "phase=replay-outbox-query timeout=0.01s" in message
                for message in startup_logs
            )
        )
        self.assertTrue(query_started.is_set())
        self.assertTrue(listener_closed.is_set())
        self.assertEqual(submitted, ["e5"])
        self.assertEqual(accounted, [block_hash])
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_startup_block_replay_catches_psql_server_timeout(self) -> None:
        server, _state, _recording = submit_coordinator()
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._command = ["psql"]
        ledger._native = None
        ledger._operation_timeout_local = threading.local()

        def timed_out_rows(*, limit: int = 32) -> list[dict[str, object]]:
            ledger._run_sql(f"SELECT {limit};")
            return []

        ledger.pending_block_candidate_rows = timed_out_rows  # type: ignore[method-assign]
        server.ledger = ledger
        completed = subprocess.CompletedProcess(
            args=["psql"],
            returncode=3,
            stdout="",
            stderr=(
                "ERROR:  57014: canceling statement due to statement timeout"
            ),
        )
        with patch(
            "lab.prism.share_ledger.subprocess.run",
            return_value=completed,
        ), patch("builtins.print") as printed:
            self.assertTrue(server._run_startup_block_candidate_replay())

        startup_logs = [
            " ".join(str(value) for value in call.args)
            for call in printed.call_args_list
        ]
        self.assertTrue(
            any(
                "startup block candidate replay timed out "
                "phase=replay-outbox-query" in message
                for message in startup_logs
            )
        )

    def test_startup_block_replay_keeps_hard_database_errors_fatal(self) -> None:
        server, _state, _recording = submit_coordinator()

        def failed_rows(*, limit: int = 32) -> list[dict[str, object]]:
            raise RuntimeError(f"postgres failed limit={limit}")

        server.ledger = SimpleNamespace(
            pending_block_candidate_rows=failed_rows,
        )
        with self.assertRaisesRegex(RuntimeError, "postgres failed"):
            server._run_startup_block_candidate_replay()

    def test_startup_block_replay_preserves_shutdown_stop(self) -> None:
        server = self._bare_coordinator()

        def shutdown_replay() -> int:
            raise ShutdownInProgress("shutdown won")

        server.replay_pending_block_candidates = shutdown_replay  # type: ignore[method-assign]

        self.assertFalse(server._run_startup_block_candidate_replay())

    def test_stuck_rpc_worker_pool_requests_nonzero_restart(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered: list[str] = []

        class IgnoringRpc:
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                entered.append(str((params or [""])[0]))
                release.wait(5)
                return None

        server.rpc = IgnoringRpc()
        try:
            for tag in ("11", "22"):
                with self.assertRaises(TimeoutError):
                    server._run_submitblock_rpc_with_hard_deadline(
                        block_hash=tag * 32,
                        block_hex=tag,
                        timeout_seconds=0.01,
                    )
            time.sleep(0.05)
            with self.assertRaises(TimeoutError):
                server._run_submitblock_rpc_with_hard_deadline(
                    block_hash="33" * 32,
                    block_hex="33",
                    timeout_seconds=0.01,
                )
            self.assertEqual(entered, ["11", "22"])
            self.assertTrue(server.stop_event.is_set())
            self.assertTrue(server._fatal_exit_requested)
        finally:
            release.set()

    def test_block_landing_db_timeout_escalates_and_clears(self) -> None:
        server, _state, _recording = submit_coordinator()
        # The doubling sequence below reaches the reviewed 120s cap, which
        # only a tolerance of at least 240s can grant: the landing budget is
        # clamped to half the configured watchdog. Pin the production
        # tolerance explicitly so this test keeps asserting the escalation
        # ladder itself rather than the ceiling.
        server.watchdog_timeout_seconds = 300.0
        server.block_landing_db_timeout_seconds = 30.0
        server.block_landing_db_timeout_max_seconds = 120.0
        block_hash = "ab" * 32
        self.assertEqual(server._block_landing_db_timeout(block_hash), 30.0)
        server._note_block_landing_timeout(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 60.0)
        server._note_block_landing_timeout(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 120.0)
        server._note_block_landing_timeout(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 120.0)
        self.assertEqual(server._block_landing_db_timeout("cd" * 32), 30.0)
        server._clear_block_candidate_retry_state(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 30.0)

    def test_escalated_landing_budget_stays_inside_watchdog_tolerance(self) -> None:
        """Escalation may never grant a budget the watchdog will not wait for.

        A landing step is spent on the watchdog-monitored block-work thread,
        so a budget above the tolerance does not buy a longer attempt: the
        watchdog hard-exits mid-landing, the in-memory escalation counts die
        with the process, and the restart replays the same doomed attempt
        from the base budget forever (issue #125). The ceiling is derived
        from the configured tolerance, so raising the watchdog restores the
        reviewed cap without touching this clamp.
        """
        server, _state, _recording = submit_coordinator()
        server.watchdog_timeout_seconds = 120.0
        server.block_landing_db_timeout_seconds = 30.0
        server.block_landing_db_timeout_max_seconds = 120.0
        block_hash = "ab" * 32
        for _ in range(8):
            server._note_block_landing_timeout(block_hash)
        escalated = server._block_landing_db_timeout(block_hash)
        self.assertLessEqual(escalated, server.watchdog_timeout_seconds / 2.0)
        self.assertEqual(escalated, 60.0)
        # The production override is pinned too: a 300s tolerance leaves a
        # 150s ceiling, so the reviewed 120s cap is still granted in full and
        # deployments running that override see no behavior change.
        server.watchdog_timeout_seconds = 300.0
        self.assertEqual(server._block_landing_db_timeout(block_hash), 120.0)

    def test_watchdog_ceiling_clamps_the_configured_base_not_only_the_cap(self) -> None:
        """A tolerance below the base budget must lower the base too.

        The first landing attempt already spends the base budget on the
        watchdog-monitored thread, so a base above the ceiling trips the
        watchdog before any escalation has happened at all -- the clamp
        cannot be a cap-only concern.
        """
        server, _state, _recording = submit_coordinator()
        server.watchdog_timeout_seconds = 40.0
        server.block_landing_db_timeout_seconds = 30.0
        server.block_landing_db_timeout_max_seconds = 120.0
        block_hash = "ab" * 32
        service = server._ensure_block_candidate_service()
        self.assertEqual(service._block_landing_watchdog_ceiling(), 20.0)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(server._block_landing_db_timeout(block_hash), 20.0)
            for _ in range(8):
                server._note_block_landing_timeout(block_hash)
            self.assertEqual(server._block_landing_db_timeout(block_hash), 20.0)

    def test_disabled_watchdog_grants_the_configured_landing_budget(self) -> None:
        """No hard-exit hazard means no reason to spend the operator's cap.

        The clamp exists solely because a landing budget above the watchdog
        tolerance buys a hard exit instead of a longer attempt. A deployment
        that turned the watchdog off has no such exit to avoid, so halving
        its cap would be pure loss. The attribute is read defensively and
        defaults to enabled, since clamping is the safe answer.
        """
        server, _state, _recording = submit_coordinator()
        server.watchdog_timeout_seconds = 120.0
        server.block_landing_db_timeout_seconds = 30.0
        server.block_landing_db_timeout_max_seconds = 120.0
        block_hash = "ab" * 32
        for _ in range(8):
            server._note_block_landing_timeout(block_hash)

        server.watchdog_enabled = False
        service = server._ensure_block_candidate_service()
        self.assertEqual(service._block_landing_watchdog_ceiling(), float("inf"))
        with contextlib.redirect_stdout(io.StringIO()) as quiet:
            self.assertEqual(server._block_landing_db_timeout(block_hash), 120.0)
            self.assertEqual(server._block_landing_db_timeout(None), 30.0)
        # An infinite ceiling never compares below a configured value, so the
        # clamp notice must stay silent as well.
        self.assertEqual(quiet.getvalue(), "")

        server.watchdog_enabled = True
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(server._block_landing_db_timeout(block_hash), 60.0)

    def test_lifecycle_config_still_carries_the_watchdog_enabled_switch(self) -> None:
        """The clamp's defensive read must not outlive the attribute itself.

        ``getattr(..., True)`` would keep clamping silently if the setting
        were ever renamed, so pin the name the coordinator assigns from.
        """
        self.assertIn(
            "watchdog_enabled",
            {field.name for field in dataclasses.fields(LifecycleConfig)},
        )

    def test_landing_budget_clamp_is_announced_exactly_once(self) -> None:
        """A silently reduced budget is a config mismatch nobody can see.

        Without the notice the operator's configured cap simply does not
        happen -- at the 120s default tolerance a 120s cap becomes 60s -- and
        "escalation exhausted at my cap" reads identically to "escalation
        never reached my cap". It is emitted once because the budget is
        recomputed on every landing attempt, and the flag flips under the
        coordinator lock so concurrent first landings cannot both print.
        """
        server, _state, _recording = submit_coordinator()
        server.watchdog_timeout_seconds = 120.0
        server.block_landing_db_timeout_seconds = 30.0
        server.block_landing_db_timeout_max_seconds = 120.0
        block_hash = "ab" * 32
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            barrier = threading.Barrier(4, timeout=10)

            def landing_budget() -> None:
                barrier.wait()
                for _ in range(5):
                    server._block_landing_db_timeout(block_hash)

            threads = [threading.Thread(target=landing_budget) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
            server._block_landing_db_timeout(block_hash)
        lines = [
            line
            for line in captured.getvalue().splitlines()
            if "landing db budget clamped by watchdog" in line
        ]
        self.assertEqual(len(lines), 1)
        self.assertIn("configured_max=120s", lines[0])
        self.assertIn("granted_max=60s", lines[0])
        self.assertIn("ceiling=60s", lines[0])
        self.assertIn("watchdog_timeout=120s", lines[0])

    def test_landing_scope_stamps_progress_during_ledger_admission(self) -> None:
        """Waiting for the ledger's writer lock must not be heartbeat-silent.

        Admission happens before any statement is sent, so no server-side
        deadline bounds it and nothing reports until the gate opens. The
        landing scope runs on the block-work owner thread the watchdog
        monitors, so it installs the ledger's progress hook and the wait
        stamps its phase in watchdog-sized slices instead.
        """
        server, _state, _recording = submit_coordinator()
        server.watchdog_timeout_seconds = 120.0
        server.block_landing_db_timeout_seconds = 30.0
        server.block_submit_db_timeout_seconds = 1.0
        installed: list[float] = []
        stamped: list[str] = []

        @contextlib.contextmanager
        def operation_progress(on_progress, *, slice_seconds: float):
            installed.append(slice_seconds)
            on_progress()
            yield

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            yield

        server.ledger = SimpleNamespace(
            statement_timeout=statement_timeout,
            operation_progress=operation_progress,
        )
        server._record_block_submitter_wait = lambda phase: stamped.append(phase)
        with server._block_landing_ledger_statement_timeout_scope("ab" * 32):
            pass
        with server._block_submitter_ledger_statement_timeout_scope():
            pass
        self.assertEqual(len(installed), 2)
        for slice_seconds in installed:
            self.assertGreater(slice_seconds, 0.0)
            self.assertLessEqual(slice_seconds, server.watchdog_timeout_seconds)
        self.assertEqual(
            stamped,
            [
                "wait-ledger-admission:landing",
                "wait-ledger-admission:submit",
            ],
        )

    def test_landing_scope_accepts_ledgers_without_a_progress_hook(self) -> None:
        """Duck-typed ledgers predating the hook keep working unchanged."""
        server, _state, _recording = submit_coordinator()
        server.watchdog_timeout_seconds = 120.0
        server.block_landing_db_timeout_seconds = 30.0
        scopes: list[float] = []

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            scopes.append(seconds)
            yield

        server.ledger = SimpleNamespace(statement_timeout=statement_timeout)
        with server._block_landing_ledger_statement_timeout_scope("ab" * 32):
            pass
        self.assertEqual(scopes, [30.0])

    def _landing_phase_recorder(self, server: PrismCoordinator) -> list[str]:
        """Capture the block-work phases one landing pass stamps, in order."""
        phases: list[str] = []
        server._record_block_submitter_phase = phases.append  # type: ignore[method-assign]
        return phases

    def test_landing_brackets_the_reorg_walk_and_prior_balance_check(self) -> None:
        """Neither stretch may sit inside the landing scope unnamed.

        Between the current-tip RPC and the audit build the landing thread
        spends a full reorg reconciliation -- ledger statements and chain
        RPCs, one pair per watched block -- and then a prior-balances read.
        The budget granted to a landing step is derived from the watchdog on
        the assumption that a heartbeat-silent stretch is about one
        statement long, so an unstamped multi-statement stretch here is
        exactly the hazard the derivation was supposed to remove (issue
        #125).
        """
        parent_hash = "00" * 32
        block_hash = "cd" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        ledger.durable_payout_state = True
        phases = self._landing_phase_recorder(server)
        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            lambda _tip, **_kwargs: True
        )
        # A stale payout base abandons the candidate right after the check,
        # so the landing stops with both bracketed stretches behind it.
        server.prior_balances_match_current = (  # type: ignore[method-assign]
            lambda _balances: False
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )
        landed = server._land_and_confirm_block_candidate(
            block_candidate(server, state, submission),
            current_tip=block_hash,
            already_active=False,
            worker="miner-a",
            node_submission=_BlockCandidateNodeSubmission(attempted=False),
        )
        self.assertIsNone(landed)
        self.assertEqual(
            phases,
            [
                "reorg-reconcile",
                "reorg-reconcile:complete",
                "prior-balances-check",
                "prior-balances-check:complete",
            ],
        )

    def test_landing_brackets_the_tip_height_rpc(self) -> None:
        """A node round trip between two stamped stretches gets its own name."""

        class StaleHeightTipRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 42
                return super().call(method, params)

        parent_hash = "00" * 32
        block_hash = "cd" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        ledger.durable_payout_state = True
        server.rpc = StaleHeightTipRpc(parent_hash)
        phases = self._landing_phase_recorder(server)
        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            lambda _tip, **_kwargs: True
        )
        server.prior_balances_match_current = (  # type: ignore[method-assign]
            lambda _balances: True
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )
        landed = server._land_and_confirm_block_candidate(
            block_candidate(server, state, submission),
            current_tip=block_hash,
            already_active=False,
            worker="miner-a",
            node_submission=_BlockCandidateNodeSubmission(attempted=False),
        )
        # The reported tip is far ahead of the template, so the candidate
        # abandons as stale immediately after the bracketed RPC.
        self.assertIsNone(landed)
        self.assertEqual(
            phases[-2:],
            ["tip-height-rpc", "tip-height-rpc:complete"],
        )

    def test_block_submitter_phase_never_stamps_from_a_foreign_thread(self) -> None:
        """A stamp off the owner thread must not refresh a wedged owner.

        The landing brackets added around the reorg walk, the prior-balance
        check and the tip-height RPC all run through this stamper, and the
        same code paths also execute on client connection threads. If those
        stamps counted, a frozen dedicated thread would look alive for as
        long as clients kept solving.
        """
        server, _state, _ledger = submit_coordinator()
        server.watchdog_timeout_seconds = 120.0
        clock = {"now": 1000.0}
        owner_ready = threading.Event()
        release_owner = threading.Event()

        def frozen_owner() -> None:
            server._block_submitter_thread_ident = threading.get_ident()
            server._record_heartbeat("block_submitter")
            owner_ready.set()
            release_owner.wait(5)

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            owner = threading.Thread(target=frozen_owner)
            owner.start()
            try:
                self.assertTrue(owner_ready.wait(5))
                clock["now"] += server.watchdog_timeout_seconds + 1.0
                self.assertEqual(
                    server._overdue_heartbeats(clock["now"]),
                    ["block_submitter"],
                )
                foreign = threading.Thread(
                    target=lambda: server._record_block_submitter_phase(
                        "prior-balances-check"
                    )
                )
                foreign.start()
                foreign.join(5)
                self.assertEqual(
                    server._overdue_heartbeats(clock["now"]),
                    ["block_submitter"],
                )
                self.assertNotEqual(
                    getattr(
                        server._ensure_block_candidate_service(),
                        "_block_submitter_phase",
                        None,
                    ),
                    "prior-balances-check",
                )
            finally:
                release_owner.set()
                owner.join(5)

    def test_landing_scope_uses_landing_budget_from_first_attempt(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 120.0
        scopes: list[float] = []

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            scopes.append(seconds)
            yield

        server.ledger = SimpleNamespace(statement_timeout=statement_timeout)
        with server._block_landing_ledger_statement_timeout_scope("ab" * 32):
            pass
        self.assertEqual(scopes, [45.0])
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 1)
        self.assertEqual(metrics["landing"]["timeouts_total"], 0)
        self.assertEqual(metrics["landing"]["last_budget_seconds"], 45.0)

    def test_landing_scope_timeout_escalates_next_attempt_budget(self) -> None:
        server, _state, _recording = submit_coordinator()
        # The escalated 90s budget asserted below needs a tolerance of at
        # least 180s; the landing budget is otherwise clamped to half the
        # configured watchdog.
        server.watchdog_timeout_seconds = 300.0
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 120.0
        scopes: list[float] = []

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            scopes.append(seconds)
            yield

        server.ledger = SimpleNamespace(statement_timeout=statement_timeout)
        block_hash = "ab" * 32
        with self.assertRaises(TimeoutError):
            with server._block_landing_ledger_statement_timeout_scope(block_hash):
                raise LedgerOperationTimeout("postgres operation exceeded 45s")
        with server._block_landing_ledger_statement_timeout_scope(block_hash):
            pass
        self.assertEqual(scopes, [45.0, 90.0])
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 2)
        self.assertEqual(metrics["landing"]["timeouts_total"], 1)
        self.assertEqual(metrics["landing"]["last_budget_seconds"], 90.0)

    def test_landing_scope_ignores_non_ledger_timeouts(self) -> None:
        """The scope guards a body that also runs node RPCs; an RPC timeout
        is not a database cancellation and must not escalate the next
        PostgreSQL budget or fire the landing-timeout alert."""
        server, _state, _recording = submit_coordinator()
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 120.0
        scopes: list[float] = []

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            scopes.append(seconds)
            yield

        server.ledger = SimpleNamespace(statement_timeout=statement_timeout)
        block_hash = "ab" * 32
        with self.assertRaises(TimeoutError):
            with server._block_landing_ledger_statement_timeout_scope(block_hash):
                raise TimeoutError("getblockhash exceeded 1s")
        with server._block_landing_ledger_statement_timeout_scope(block_hash):
            pass
        self.assertEqual(scopes, [45.0, 45.0])
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 2)
        self.assertEqual(metrics["landing"]["timeouts_total"], 0)

    def test_fast_class_ledger_call_metrics_record_timeouts(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        release = threading.Event()

        def blocking_operation() -> None:
            release.wait(5)

        try:
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("metrics-key",),
                    "metrics-test",
                    blocking_operation,
                )
        finally:
            release.set()
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["fast"]["timeouts_total"], 1)
        self.assertEqual(metrics["fast"]["last_budget_seconds"], 0.01)

    def test_stuck_ledger_worker_pool_requests_nonzero_restart(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered: list[str] = []

        def blocking_operation(tag: str) -> str:
            entered.append(tag)
            release.wait(5)
            return tag

        try:
            for tag in ("one", "two"):
                with self.assertRaises(TimeoutError):
                    server._run_block_submitter_ledger_call(
                        (tag,),
                        f"test-{tag}",
                        lambda tag=tag: blocking_operation(tag),
                    )
            time.sleep(0.05)
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("three",),
                    "test-three",
                    lambda: blocking_operation("three"),
                )
            self.assertEqual(entered, ["one", "two"])
            self.assertTrue(server.stop_event.is_set())
            self.assertTrue(server._fatal_exit_requested)
        finally:
            release.set()

    def test_one_stuck_ledger_call_does_not_restart_with_spare_capacity(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered = threading.Event()

        def blocking_operation() -> None:
            entered.set()
            release.wait(5)

        try:
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("same-key",),
                    "same-key",
                    blocking_operation,
                )
            self.assertTrue(entered.wait(1))
            time.sleep(0.05)
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("same-key",),
                    "same-key",
                    blocking_operation,
                )
            self.assertFalse(server.stop_event.is_set())
            self.assertFalse(getattr(server, "_fatal_exit_requested", False))
        finally:
            release.set()

    def test_two_stuck_ledger_calls_restart_on_existing_key_retry(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered: list[str] = []

        def blocking_operation(tag: str) -> None:
            entered.append(tag)
            release.wait(5)

        try:
            for tag in ("one", "two"):
                with self.assertRaises(TimeoutError):
                    server._run_block_submitter_ledger_call(
                        (tag,),
                        tag,
                        lambda tag=tag: blocking_operation(tag),
                    )
            time.sleep(0.05)
            # No third key is required: a retry reusing either poisoned call
            # still observes that every bounded worker slot is exhausted.
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("one",),
                    "one",
                    lambda: blocking_operation("one"),
                )
            self.assertEqual(entered, ["one", "two"])
            self.assertTrue(server.stop_event.is_set())
            self.assertTrue(server._fatal_exit_requested)
        finally:
            release.set()

    def test_parent_retry_displacement_preserves_durable_descendant(self) -> None:
        server, state, _recording = submit_coordinator()

        def candidate_at(tag: str, height: int) -> PrismBlockCandidate:
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=tag * 32,
                    block_hex=tag,
                    share_pass=True,
                    block_pass=True,
                ),
            )
            context = SimpleNamespace(**vars(candidate.context))
            context.template = {**candidate.context.template, "height": height}
            return dataclass_replace(
                candidate,
                context=context,
                durable_replay=True,
            )

        descendant = candidate_at("b8", 11)
        parent = candidate_at("a8", 10)
        descendant_hash = descendant.submission.block_hash_hex
        server._ensure_block_candidate_disposition_state()
        server._ensure_block_replay_state()
        server._block_replay_inflight_hashes.add(descendant_hash)
        with server.lock:
            server._retry_block_candidate = descendant
            server._merge_block_candidate_retry_locked(
                "_retry_block_candidate",
                parent,
            )

        self.assertIs(server._retry_block_candidate, parent)
        self.assertIs(
            server._block_disposition_waiting_retries[descendant_hash],
            descendant,
        )

        # Once the parent reaches a terminal state, the descendant's preserved
        # wakeup is selected even though durable replay still deduplicates it.
        with server.lock:
            server._retry_block_candidate = None
        captured: list[object] = []
        server.max_blocks = 10
        server.stop_after_block = False
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=True,
                result="duplicate",
                error=None,
            )
        )
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda task: (captured.append(task), True)[1]
        )

        self.assertTrue(server.submit_next_block_candidate(defer_accounting=True))
        self.assertEqual(len(captured), 1)
        task = captured[0]
        self.assertIs(task.candidate, descendant)
        server._release_block_candidate_disposition(task.disposition_lease)

    def test_synchronous_waiter_joins_finalize_only_registry(self) -> None:
        server, state, _recording = submit_coordinator()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="c8" * 32,
                block_hex="c8",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        block_hash = candidate.submission.block_hash_hex
        server._block_candidate_finalize_retries = {}
        server._block_candidate_finalize_retries[block_hash] = (True, "")
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                AssertionError("finalize-only retry must not call qbitd")
            )
        )
        finalized: list[dict[str, object]] = []

        def finalize(
            _candidate: PrismBlockCandidate,
            **kwargs: object,
        ) -> bool:
            finalized.append(kwargs)
            return True

        server._finalize_block_candidate = finalize  # type: ignore[method-assign]

        self.assertTrue(server._submit_synchronous_block_candidate(candidate))
        self.assertEqual(len(finalized), 1)
        self.assertTrue(finalized[0]["accepted"])
        self.assertEqual(finalized[0]["block_hash"], block_hash)

    def test_synchronous_waiter_preserves_finalize_only_abandonment(self) -> None:
        server, state, _recording = submit_coordinator()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="c9" * 32,
                block_hex="c9",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        block_hash = candidate.submission.block_hash_hex
        server._block_candidate_finalize_retries = {
            block_hash: (False, "terminal rejection")
        }
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                AssertionError("finalize-only retry must not call qbitd")
            )
        )
        finalized: list[dict[str, object]] = []
        server._finalize_block_candidate = (  # type: ignore[method-assign]
            lambda _candidate, **kwargs: (finalized.append(kwargs), True)[1]
        )

        self.assertFalse(server._submit_synchronous_block_candidate(candidate))
        self.assertEqual(len(finalized), 1)
        self.assertFalse(finalized[0]["accepted"])
        self.assertEqual(finalized[0]["error"], "terminal rejection")

    def test_synchronous_abandon_rejects_prepared_state_before_outbox(self) -> None:
        server, state, ledger = submit_coordinator()
        block_hash = "ca" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="ca",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        events: list[str] = []
        prepared = {"value": True}

        def pool_block_state(*, block_hash: str) -> dict[str, str]:
            self.assertEqual(block_hash, candidate.submission.block_hash_hex)
            events.append("state")
            return {
                "chain_state": "prepared" if prepared["value"] else "rejected",
                "maturity_state": "immature",
            }

        def reject_prepared_block(**_kwargs: object) -> dict[str, object]:
            events.append("reject")
            prepared["value"] = False
            return {"backend": "fake", "rejected_count": 1}

        def mark_abandoned(**_kwargs: object) -> bool:
            events.append("abandon")
            return True

        ledger.pool_block_state = pool_block_state  # type: ignore[attr-defined]
        ledger.reject_prepared_block = reject_prepared_block  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[attr-defined]

        class CleanupRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                return super().call(method, params)

        server.rpc = CleanupRpc("00" * 32)
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        server._mark_block_candidate_attempted = (  # type: ignore[method-assign]
            lambda _block_hash: True
        )

        def stage_terminal_rejection(
            _candidate: PrismBlockCandidate,
            *,
            node_submission: object,
        ) -> bool:
            self.assertIsNotNone(node_submission)
            outcome = server._block_candidate_outcome
            outcome.reason = PRISM_REJECTION_POOL_CLOSED
            outcome.error = "pool is no longer accepting blocks"
            outcome.stale_job_class = None
            return False

        server._submit_block_candidate_serialized = (  # type: ignore[method-assign]
            stage_terminal_rejection
        )

        self.assertFalse(server._submit_synchronous_block_candidate(candidate))
        self.assertEqual(events, ["state", "reject", "abandon"])
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_REJECTION_POOL_CLOSED: 1},
        )
        self.assertFalse(server._block_candidate_terminal_outcome(block_hash))

    def test_synchronous_cleanup_failure_keeps_candidate_pending(self) -> None:
        server, state, ledger = submit_coordinator()
        block_hash = "cb" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="cb",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        events: list[str] = []
        ledger.pool_block_state = (  # type: ignore[attr-defined]
            lambda *, block_hash: {
                "chain_state": "prepared",
                "maturity_state": "immature",
            }
        )

        def fail_reject(**_kwargs: object) -> dict[str, object]:
            events.append("reject")
            raise RuntimeError("postgres unavailable")

        def unexpected_abandon(**_kwargs: object) -> bool:
            events.append("abandon")
            return True

        ledger.reject_prepared_block = fail_reject  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = unexpected_abandon  # type: ignore[attr-defined]

        class CleanupRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                return super().call(method, params)

        server.rpc = CleanupRpc("00" * 32)
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        server._mark_block_candidate_attempted = (  # type: ignore[method-assign]
            lambda _block_hash: True
        )

        def stage_terminal_rejection(
            _candidate: PrismBlockCandidate,
            *,
            node_submission: object,
        ) -> bool:
            self.assertIsNotNone(node_submission)
            outcome = server._block_candidate_outcome
            outcome.reason = PRISM_REJECTION_POOL_CLOSED
            outcome.error = "pool is no longer accepting blocks"
            outcome.stale_job_class = None
            return False

        server._submit_block_candidate_serialized = (  # type: ignore[method-assign]
            stage_terminal_rejection
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "could not reject prepared state for terminal candidate",
        ):
            server._submit_synchronous_block_candidate(candidate)

        self.assertEqual(events, ["reject"])
        self.assertIs(server._retry_block_candidate, candidate)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertIsNone(server._block_candidate_terminal_outcome(block_hash))
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
        )

    def test_restart_resubmit_honors_terminal_abandoned_outbox(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "e8" * 32
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="e8",
            share_pass=False,
            block_pass=True,
        )
        first_pending = server.pending_share_from_submission(
            context=server.jobs["job-1"],
            submission=submission,
            ntime_hex="00000001",
        )
        first_candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=first_pending,
            credit_share_on_accept=True,
        )
        self.assertTrue(
            ledger.persist_block_candidate_intent(
                server.block_candidate_intent(first_candidate)
            )
        )
        self.assertTrue(
            ledger.mark_block_candidate_abandoned(
                block_hash=block_hash,
                error="terminal before restart",
            )
        )
        server._finish_pending_share_commit(first_pending)
        submitblock_calls: list[str] = []

        class NoResubmitRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitblock_calls.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = NoResubmitRpc("00" * 32)
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.code, 23)
        self.assertEqual(submitblock_calls, [])
        self.assertEqual(ledger._block_candidate_outbox[block_hash]["state"], "abandoned")
        self.assertEqual(len(ledger), 0)

    def test_restart_resubmit_coalesces_terminal_submitted_outbox(self) -> None:
        first_server, first_state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        first_server.ledger = ledger
        block_hash = "f8" * 32
        submission = SimpleNamespace(
            header_hex="ab" * 80,
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="f8",
            share_pass=True,
            block_pass=True,
        )
        first_pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=7,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=12345,
            accepted_at_ms=1,
            ntime=1,
        )
        first_candidate = block_candidate(
            first_server,
            first_state,
            submission,
            pending_share=first_pending,
        )
        first_record = ledger.append_batch(
            [
                (
                    first_pending,
                    first_server.block_candidate_intent(first_candidate),
                )
            ]
        )[0]
        self.assertTrue(first_record.newly_inserted)
        self.assertTrue(
            ledger.mark_block_candidate_submitted(block_hash=block_hash)
        )

        # Model a clean restart: volatile duplicate/disposition state is gone,
        # while the ledger and terminal candidate outbox remain authoritative.
        server, state, _recording = submit_coordinator()
        server.ledger = ledger
        submitblock_calls: list[str] = []

        class NoResubmitRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitblock_calls.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = NoResubmitRpc("00" * 32)
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(submitblock_calls, [])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["state"], "submitted"
        )
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger._shares[0].accepted_at_ms, 1)
        self.assertTrue(server.block_candidate_queue.empty())
        self.assertEqual(
            server.worker_share_counts["miner-a"],
            {"submitted": 1, "accepted": 0, "grace": 0},
        )

    def test_exact_share_replay_does_not_repeat_process_credit(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "d8" * 32
        submission = SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="d8",
            share_pass=True,
            block_pass=True,
        )
        pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=12345,
            accepted_at_ms=12346,
            ntime=1,
        )
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(candidate)
        worker_credits: list[str] = []
        vardiff_credits: list[str] = []
        server.note_worker_accepted_share = (  # type: ignore[method-assign]
            lambda worker, _policy: worker_credits.append(worker)
        )
        server.note_vardiff_accepted_share = (  # type: ignore[method-assign]
            lambda _client, job: vardiff_credits.append(job.job_id)
        )

        self.assertEqual(
            server.append_accepted_share(
                state,
                candidate.context,
                submission,
                pending,
                candidate_intent=intent,
            ),
            "pending",
        )
        self.assertEqual(
            server.append_accepted_share(
                state,
                candidate.context,
                submission,
                pending,
                candidate_intent=intent,
            ),
            "pending",
        )

        self.assertEqual(len(ledger), 1)
        self.assertEqual(worker_credits, ["miner-a"])
        self.assertEqual(vardiff_credits, ["job-1"])

    def test_same_hash_busy_lease_preserves_retry_behind_live_work(self) -> None:
        server, state, _recording = submit_coordinator()
        server.block_candidate_retry_initial_seconds = 0.0
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a7" * 32,
                block_hex="a7",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = candidate.submission.block_hash_hex
        lease = server._claim_block_candidate_disposition(
            block_hash,
            blocking=True,
        )
        assert lease is not None
        server._retry_block_candidate = candidate
        server._ensure_block_replay_state()
        server._block_replay_inflight_hashes.add(block_hash)
        try:
            self.assertTrue(
                server.submit_next_block_candidate(defer_accounting=True)
            )
            self.assertIsNone(server._retry_block_candidate)
            self.assertIs(
                server._block_disposition_waiting_retries[block_hash],
                candidate,
            )
        finally:
            server._release_block_candidate_disposition(lease)

        captured: list[object] = []
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=True,
                result="duplicate",
                error=None,
            )
        )
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda task: (captured.append(task), True)[1]
        )
        self.assertTrue(server.submit_next_block_candidate(defer_accounting=True))
        self.assertEqual(len(captured), 1)
        task = captured[0]
        server._release_block_candidate_disposition(task.disposition_lease)

    def test_permanently_closed_pool_hands_outbox_to_accounting(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.accepted_block_count = 1
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f2" * 32,
                block_hex="f2",
                share_pass=True,
                block_pass=True,
            ),
        )
        captured: list[object] = []
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda task: (captured.append(task), True)[1]
        )
        server.rpc = SimpleNamespace(
            call=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("closed pool must not call submitblock")
            )
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate(defer_accounting=True))
        self.assertEqual(len(captured), 1)
        task = captured[0]
        self.assertFalse(task.node_submission.attempted)
        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        server._release_block_candidate_disposition(task.disposition_lease)

    def test_accounted_block_replaces_its_capacity_reservation(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "e2" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="e2",
                share_pass=True,
                block_pass=True,
            ),
        )
        server._ensure_block_candidate_disposition_state()
        server._block_fast_lane_reservations.add(block_hash)
        report = verified_audit_report()
        verification_identity = AuditArtifactStore.build_verification_identity(
            trust_source="embedded_test_only",
            trusted_writer_public_key_hex="aa" * 32,
            literal_sha256="cd" * 32,
            literal_byte_len=1,
            report=report,
        )
        _recording._audit_publication_sequences[block_hash] = 1
        server._land_and_confirm_block_candidate = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (
                verified_block_bundle(),
                report,
                {"persisted": True},
                {"confirmed_count": 1},
                AuditPublicationIdentity(1, 10, block_hash),
                verification_identity,
            )
        )
        server.accepted_share_stats = lambda: (0, 0)  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            self.assertTrue(
                server._submit_block_candidate_serialized(
                    candidate,
                    node_submission=SimpleNamespace(
                        attempted=True,
                        result=None,
                        error=None,
                    ),
                )
            )

        self.assertEqual(server.accepted_block_count, 1)
        self.assertNotIn(block_hash, server._block_fast_lane_reservations)
        self.assertTrue(server._reserve_block_fast_lane_slot("e3" * 32))

    def test_synchronous_candidate_waits_for_fast_lane_capacity(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.stop_after_block = True
        reserved_hash = "e4" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex="e5" * 32,
                block_hex="e5",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        server._ensure_block_candidate_disposition_state()
        self.assertTrue(server._reserve_block_fast_lane_slot(reserved_hash))
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                AssertionError("capacity-blocked candidate must not reach qbitd")
            )
        )

        with self.assertRaisesRegex(RuntimeError, "fast-lane capacity"):
            server._submit_synchronous_block_candidate(candidate)

        self.assertIs(server._retry_block_candidate, candidate)
        self.assertEqual(server._block_fast_lane_reservations, {reserved_hash})

    def test_node_offer_exception_retains_reserved_candidate(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.stop_after_block = True
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex="e6" * 32,
                block_hex="e6",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = candidate.submission.block_hash_hex
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                RuntimeError("node offer bookkeeping failed")
            )
        )
        server.enqueue_block_candidate(candidate)

        with self.assertRaisesRegex(RuntimeError, "node offer bookkeeping failed"):
            server.submit_next_block_candidate(defer_accounting=True)

        self.assertIs(server._retry_block_candidate, candidate)
        self.assertIn(block_hash, server._block_fast_lane_reservations)
        self.assertFalse(server._reserve_block_fast_lane_slot("e7" * 32))
        lease = server._claim_block_candidate_disposition(
            block_hash,
            blocking=False,
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        server._release_block_candidate_disposition(lease)

    def test_accounting_enqueue_exception_retains_reserved_candidate(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.stop_after_block = True
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex="e8" * 32,
                block_hex="e8",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = candidate.submission.block_hash_hex
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=True,
                result=None,
                error=None,
            )
        )
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda _task: (_ for _ in ()).throw(
                RuntimeError("accounting enqueue failed")
            )
        )
        server.enqueue_block_candidate(candidate)

        with self.assertRaisesRegex(RuntimeError, "accounting enqueue failed"):
            server.submit_next_block_candidate(defer_accounting=True)

        self.assertIs(server._retry_block_candidate, candidate)
        self.assertIn(block_hash, server._block_fast_lane_reservations)
        self.assertFalse(server._reserve_block_fast_lane_slot("e9" * 32))
        lease = server._claim_block_candidate_disposition(
            block_hash,
            blocking=False,
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        server._release_block_candidate_disposition(lease)


class PendingShareFloorSeamTests(unittest.TestCase):
    """S3 floor-holder seams used by the block-candidate credit paths.

    At this layer the pending-commit floor keeps the current id-keyed single
    holder per stamped share; the attempt/candidate release seams are the
    staged split points a later layer widens into independent holders. These
    cases pin the seam wiring so a lost release path cannot silently unhook
    the snapshot-anchor floor from candidate handling.
    """

    def _stamped_pending_share(self, server, share_id: str):
        share_writer = server._ensure_share_writer_service()
        pending = PendingShare(
            share_id=share_id,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=100,
            ntime=1_700_000_000,
        )
        with share_writer._pending_share_commit_lock:
            share_writer._pending_share_commit_floor[id(pending)] = [
                pending,
                time.monotonic(),
                False,
            ]
        return pending

    def test_attempt_and_candidate_seams_release_the_owner_floor(self) -> None:
        server = coordinator()
        share_writer = server._ensure_share_writer_service()

        attempt = self._stamped_pending_share(server, "miner-a:attempt")
        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 99)
        server._finish_pending_share_attempt(attempt)
        self.assertEqual(share_writer._pending_share_commit_floor, {})
        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 999)

        candidate = self._stamped_pending_share(server, "miner-a:candidate")
        server._finish_pending_share_candidate(candidate)
        self.assertEqual(share_writer._pending_share_commit_floor, {})

    def test_distinct_same_hash_stamps_hold_independent_floor_entries(self) -> None:
        server = coordinator()
        share_writer = server._ensure_share_writer_service()

        first = self._stamped_pending_share(server, "miner-a:same-hash")
        second = self._stamped_pending_share(server, "miner-a:same-hash")

        # Releasing one stamped object's holder cannot drop the other live
        # holder for the same durable hash: the anchor floor stays clamped.
        server._finish_pending_share_candidate(second)
        self.assertEqual(len(share_writer._pending_share_commit_floor), 1)
        self.assertIs(
            share_writer._pending_share_commit_floor[id(first)][0],
            first,
        )
        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 99)

        server._finish_pending_share_attempt(first)
        self.assertEqual(share_writer._pending_share_commit_floor, {})


class DuplicateDropSnapshotFloorTests(unittest.TestCase):
    """Duplicate-dropped credit candidates release their adopted floor holders.

    A replayed credit-bearing candidate adopts its reconstructed PendingShare
    onto the snapshot-anchor floor at decode, keyed by object identity, so a
    same-hash duplicate dropped on any path carries a holder no other object's
    disposition can release. Without a release at the drop, the job/payout
    snapshot anchor stays clamped below the replayed stamp until restart
    (#76 review finding).
    """

    def _durable_credit_candidate(
        self,
        server: PrismCoordinator,
        state: ClientState,
        ledger: SingleWriterShareLedger,
        tag: str,
        stamp: int,
    ) -> PrismBlockCandidate:
        pending = PendingShare(
            share_id=f"miner-a:{tag * 32}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=stamp,
            ntime=1,
        )
        value = dataclass_replace(
            block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=tag * 32,
                    block_hex="00",
                    share_pass=False,
                    block_pass=True,
                ),
                pending_share=pending,
            ),
            credit_share_on_accept=True,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(value))])
        return value

    def test_replay_dedupe_drop_releases_adopted_floor_holder(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        self._durable_credit_candidate(server, state, ledger, "a7", 100)
        share_writer = server._ensure_share_writer_service()

        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 1)
        first = server._block_replay_candidate_queue.get_nowait()
        self.assertEqual(len(share_writer._pending_share_commit_floor), 1)

        # The submitter holds the dequeued candidate mid-disposition, so the
        # steady-state poll re-reads the still-pending outbox row: the decode
        # adopts a fresh holder before the in-flight dedupe drops the object.
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(len(share_writer._pending_share_commit_floor), 1)

        # Once the surviving object's disposition releases its own holder, no
        # leaked duplicate keeps clamping the anchor below the replayed stamp.
        server._finish_pending_share_candidate(first.pending_share)
        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 999)

    def test_terminal_short_circuit_releases_replayed_duplicate_holder(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        candidate = self._durable_credit_candidate(server, state, ledger, "b7", 100)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 1)

        # A same-hash disposition (e.g. the miner's synchronous resubmit of
        # the below-target share) lands terminally while the replayed
        # duplicate is still queued behind live solves.
        server._record_block_candidate_terminal_outcome(
            candidate.submission.block_hash_hex,
            accepted=True,
        )
        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 99)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 999)

    def test_terminal_outcome_releases_parked_same_hash_duplicate_holder(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        candidate = self._durable_credit_candidate(server, state, ledger, "c7", 100)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 1)
        parked = server._block_replay_candidate_queue.get_nowait()

        # submit_next parks a candidate that lost the same-hash disposition
        # claim; the winning flight then records the terminal outcome and
        # discards the parked wakeup.
        server._ensure_block_candidate_disposition_state()
        with server.lock:
            server._block_disposition_waiting_retries[
                candidate.submission.block_hash_hex
            ] = parked
        server._record_block_candidate_terminal_outcome(
            candidate.submission.block_hash_hex,
            accepted=True,
        )

        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 999)

    def test_same_hash_retry_merge_releases_displaced_duplicate_holder(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        self._durable_credit_candidate(server, state, ledger, "d7", 100)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 1)
        displaced = server._block_replay_candidate_queue.get_nowait()
        server._retain_block_candidate_for_retry(displaced)

        # The miner resubmits the same below-target block: a distinct live
        # stamped object takes the retry slot and the replayed object dies.
        live = dataclass_replace(
            block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex="d7" * 32,
                    block_hex="00",
                    share_pass=False,
                    block_pass=True,
                ),
                pending_share=PendingShare(
                    share_id="miner-a:" + "d7" * 32,
                    miner_id="miner-a",
                    order_key="miner-a",
                    p2mr_program_hex="11" * 32,
                    share_difficulty=1,
                    network_difficulty=1,
                    template_height=9,
                    job_id="job-1",
                    job_issued_at_ms=1,
                    accepted_at_ms=100,
                    ntime=1,
                ),
            ),
            credit_share_on_accept=True,
        )
        server._retain_block_candidate_for_retry(live)

        self.assertIs(server._retry_block_candidate, live)
        self.assertEqual(server._job_snapshot_anchor_ms(1_000), 999)
