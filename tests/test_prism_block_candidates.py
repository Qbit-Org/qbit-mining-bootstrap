#!/usr/bin/env python3
"""Focused PRISM coordinator block candidates tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import contextlib
from dataclasses import replace as dataclass_replace
import unittest
from unittest import mock
from tests.prism_vardiff_test_support import *
from lab.prism.audit_artifacts import (
    AuditArtifactStore,
    AuditPublicationIdentity,
    RetentionResult,
)
from lab.prism.bundle_compiler import BundleCompiler
from lab.prism.block_candidates import (
    BlockCandidateAttemptResult,
    BlockCandidateRunResult,
    block_candidate_from_intent,
    block_candidate_intent,
)
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
                    payout_service.balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
                )
                self.assertEqual(store._publication_guard_owner, threading.get_ident())
                events.append("confirm")
                return real_confirm(**kwargs)

            def publication_identity(**kwargs: object) -> AuditPublicationIdentity:
                self.assertTrue(
                    payout_service.balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
                )
                self.assertEqual(store._publication_guard_owner, threading.get_ident())
                events.append("publication_identity")
                return real_identity(**kwargs)

            def publication_floor() -> int:
                self.assertTrue(
                    payout_service.balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
                )
                self.assertEqual(store._publication_guard_owner, threading.get_ident())
                events.append("publication_floor")
                return real_floor()

            def publish(**kwargs: object) -> object:
                self.assertTrue(
                    payout_service.balance_mutation_lock._is_owned(),  # type: ignore[attr-defined]
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
            "lab.prism.bundle_compiler.subprocess.Popen",
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
            "lab.prism.bundle_compiler.subprocess.Popen",
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
            "lab.prism.bundle_compiler.subprocess.Popen",
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
            "lab.prism.bundle_compiler.subprocess.Popen",
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

    def test_summary_build_records_serialization_phase_once(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        observed: list[tuple[str, float]] = []
        service = server._ensure_job_bundle_service()
        service._phase_local.tip_refresh_metrics = True
        compiler = server._ensure_bundle_compiler()
        compiler._ports = dataclass_replace(
            compiler._ports,
            record_tip_refresh_phase=(
                lambda phase, elapsed: observed.append((phase, elapsed))
            ),
        )
        phase_metrics = {
            "phases_seconds": {},
            "input_deserialization_seconds": 0.25,
            "output_serialization_seconds": 0.5,
        }

        with patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            fake_audit_bundle_popen(
                {},
                stderr_text=(
                    "qbit-prism-build-phase-metrics "
                    + json.dumps(phase_metrics, separators=(",", ":"))
                ),
            ),
        ):
            server.build_audit_bundle(
                shares=[],
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
            )

        serialization = [
            elapsed for phase, elapsed in observed if phase == "serialization_copy"
        ]
        self.assertEqual(len(serialization), 1)
        self.assertGreaterEqual(serialization[0], 0.75)
    def test_build_audit_bundle_removes_partial_output_after_builder_failure(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32

        captured: dict[str, object] = {}
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "candidate.audit.json"
            with patch(
                "lab.prism.bundle_compiler.subprocess.Popen",
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
            self.assertEqual(server._ensure_job_bundle_service()._worker_counts["crashes"], 1)

            with patch(
                "lab.prism.bundle_compiler.subprocess.Popen",
                fake_audit_bundle_popen(captured),
            ):
                recovered = server.build_audit_bundle(
                    shares=[],
                    found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                )

            self.assertEqual(recovered, {"ok": True})
            self.assertEqual(server._ensure_job_bundle_service()._worker_counts["restarts"], 1)

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
            "lab.prism.bundle_compiler.subprocess.Popen",
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
        self.assertEqual(server._ensure_job_bundle_service()._worker_counts["terminations"], 1)
        self.assertEqual(server._ensure_job_bundle_service()._worker_counts["restarts"], 1)
    def test_build_audit_bundle_does_not_unlink_preexisting_output_path(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.bundle_compiler.subprocess.Popen"
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
        server, state, ledger = submit_coordinator()
        ledger.durable_payout_state = True
        ledger.prior_balances = [
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

        accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        # The share was already accepted at submit time, so a lost block is a
        # block-abandonment, not a stale share rejection.
        self.assertEqual(server.stale_share_count, 0)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(ledger.pending, [])
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
        self.assertEqual(submit_calls, [])
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
        transition = server._payout_state_service._previews[descendant_hash]
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
        server, state, ledger = submit_coordinator(tip=old_tip)
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
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(len(ledger.pending), 1)
        # The tip moves before the submitter drains the candidate.
        server.rpc = TipRpc(new_tip)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(server.accepted_block_count, 0)
        # Counted as a block abandonment, not a share rejection.
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(server.stale_share_count, 0)
        # The credited share survives the lost block race.
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.persisted, [])
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
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = reject  # type: ignore[method-assign]
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

        self.assertEqual(server.replay_pending_block_candidates(), 2)
        first = server.block_candidate_queue.get_nowait()
        second = server.block_candidate_queue.get_nowait()
        self.assertEqual(
            [first.submission.block_hash_hex, second.submission.block_hash_hex],
            ["aa" * 32, "bb" * 32],
        )
        ledger.mark_block_candidate_submitted(block_hash="aa" * 32)
        ledger.mark_block_candidate_abandoned(block_hash="bb" * 32, error="stale")

        self.assertEqual(server.replay_pending_block_candidates(), 1)
        replayed = server.block_candidate_queue.get_nowait()
        self.assertEqual(replayed.submission.block_hash_hex, "cc" * 32)
        self.assertEqual(replayed.pending_share.share_id, "miner-a:" + "cc" * 32)
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
            server._payout_state_service._previews,
        )
        self.assertNotIn(
            candidate.submission.block_hash_hex,
            server._payout_state_service._invalidated_previews,
        )
    def test_terminal_abandonment_keeps_tombstone_when_outbox_update_fails(
        self,
    ) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("terminal-outbox-failure").pending_share
        candidate = dataclass_replace(
            block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex="a3" * 32,
                    block_hex="00",
                    share_pass=False,
                    block_pass=True,
                ),
                pending_share=pending,
            ),
            credit_share_on_accept=True,
        )
        block_hash = candidate.submission.block_hash_hex
        block_height = int(candidate.context.template["height"])
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server._ensure_share_writer_service().adopt_pending_share(pending)

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

        self.assertNotIn(block_hash, server._payout_state_service._previews)
        self.assertIn(block_hash, server._payout_state_service._invalidated_previews)
        self.assertEqual(
            [intent["block_hash_hex"] for intent in ledger.pending_block_candidates()],
            [block_hash],
        )
        self.assertEqual(server._pending_share_commit_floor, {})

        # Once the same durable row terminalizes normally, the reconstructed
        # credit source is no longer reachable and the S3 floor is released.
        server.enqueue_block_candidate(candidate)
        self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(server._pending_share_commit_floor, {})

    def test_finalize_failure_replays_with_candidate_backoff(self) -> None:
        server, state, _recording = submit_coordinator()
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
        submit_calls = 0

        def accepting_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            return True

        server.submit_block_candidate = accepting_submit  # type: ignore[method-assign]
        original_finish = ledger.mark_block_candidate_submitted
        finish_attempts = 0

        def flaky_finish(*, block_hash: str) -> bool:
            nonlocal finish_attempts
            finish_attempts += 1
            if finish_attempts <= 4:
                raise RuntimeError("ledger unavailable")
            return original_finish(block_hash=block_hash)

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
        self.assertEqual(submit_calls, 1)
        self.assertNotIn(
            candidate.submission.block_hash_hex,
            server.block_candidate_retry_delays,
        )
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
                "tip moved",
                worker="miner-a",
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
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_REJECTION_STALE_JOB: 1},
        )
        self.assertEqual(ledger.pending_block_candidates(), [])
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
        original_release = server._finish_pending_share_candidate

        def recording_release(pending_share: object) -> None:
            released_shares.append(pending_share)
            original_release(pending_share)

        server._finish_pending_share_candidate = recording_release  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            server.enqueue_block_candidate(candidate)
            self.assertTrue(server.submit_next_block_candidate())
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

                self.assertEqual(ledger.pending_block_candidates(), [])
                self.assertNotIn(durable_hash, server.block_candidate_retry_delays)
                self.assertIn(
                    "qbit_prism_block_candidate_poisoned_total 1",
                    server.metrics_payload(),
                )

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

        self.assertEqual(server.replay_pending_block_candidates(), 1)
        replayed = server.block_candidate_queue.get_nowait()
        self.assertEqual(
            replayed.submission.block_hash_hex,
            second.submission.block_hash_hex,
        )
        self.assertEqual(
            [intent["block_hash_hex"] for intent in ledger.pending_block_candidates()],
            [second.submission.block_hash_hex],
        )

    def test_credit_replay_preview_failure_releases_floor_only_after_quarantine(self) -> None:
        for quarantine_fails in (False, True):
            with self.subTest(quarantine_fails=quarantine_fails):
                server, state, _recording = submit_coordinator()
                ledger = SingleWriterShareLedger()
                server.ledger = ledger
                block_hash = "93" * 32
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
                    accepted_at_ms=123,
                    ntime=1,
                )
                candidate = dataclass_replace(
                    block_candidate(
                        server,
                        state,
                        SimpleNamespace(
                            coinbase_tx_hex="00",
                            block_hash_hex=block_hash,
                            block_hex="00",
                            share_pass=False,
                            block_pass=True,
                        ),
                        pending_share=pending,
                    ),
                    credit_share_on_accept=True,
                )
                ledger.persist_block_candidate_intent(
                    server.block_candidate_intent(candidate)
                )

                quarantine = (
                    patch.object(
                        ledger,
                        "mark_block_candidate_abandoned",
                        side_effect=RuntimeError("terminal update failed"),
                    )
                    if quarantine_fails
                    else contextlib.nullcontext()
                )
                with patch.object(
                    server,
                    "_begin_accepted_block_payout_preview",
                    side_effect=RuntimeError("preview failed"),
                ), quarantine:
                    self.assertEqual(server.replay_pending_block_candidates(), 0)

                if quarantine_fails:
                    self.assertEqual(len(ledger.pending_block_candidates()), 1)
                    self.assertEqual(server._job_snapshot_anchor_ms(10_000), 122)
                else:
                    self.assertEqual(ledger.pending_block_candidates(), [])
                    self.assertEqual(server._pending_share_commit_floor, {})

    def test_credit_append_failure_never_drops_durable_floor_before_retry_adoption(
        self,
    ) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = PendingShare(
            share_id="miner-a:" + "94" * 32,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=123,
            ntime=1,
        )
        candidate = dataclass_replace(
            block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex="94" * 32,
                    block_hex="00",
                    share_pass=False,
                    block_pass=True,
                ),
                pending_share=pending,
            ),
            credit_share_on_accept=True,
        )
        ledger.persist_block_candidate_intent(server.block_candidate_intent(candidate))
        writer = server._ensure_share_writer_service()
        writer.adopt_pending_share(pending)

        append_entered = threading.Event()
        release_append = threading.Event()
        readoption_entered = threading.Event()
        release_readoption = threading.Event()

        def failing_append(_entries: object) -> list[object]:
            append_entered.set()
            release_append.wait(timeout=2)
            raise RuntimeError("share commit failed")

        original_adopt = writer.adopt_pending_share

        def blocking_readopt(value: PendingShare) -> None:
            readoption_entered.set()
            release_readoption.wait(timeout=2)
            original_adopt(value)

        def credit_then_fail(value: PrismBlockCandidate) -> bool:
            server.append_accepted_share(
                value.client,
                value.context,
                value.submission,
                value.pending_share,
                candidate_intent=server.block_candidate_intent(value),
            )
            return True

        ledger.append_batch = failing_append  # type: ignore[method-assign]
        writer.adopt_pending_share = blocking_readopt  # type: ignore[method-assign]
        server.submit_block_candidate = credit_then_fail  # type: ignore[method-assign]
        thread = threading.Thread(
            target=server._submit_next_block_candidate_writer,
            args=(candidate,),
        )
        thread.start()
        self.assertTrue(append_entered.wait(timeout=1))
        self.assertIn(pending.share_id, server._pending_share_commit_floor)
        release_append.set()
        self.assertTrue(readoption_entered.wait(timeout=1))

        # S3's failed append has completed its attempt-only cleanup, while the
        # caller is deliberately stopped before retry adoption. The preexisting
        # durable holder must still cover this entire interval.
        self.assertIn(pending.share_id, server._pending_share_commit_floor)
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 122)
        release_readoption.set()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertIs(server._retry_block_candidate, candidate)
        server._finish_pending_share_commit(pending)
        self.assertEqual(server._pending_share_commit_floor, {})
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
        server._ensure_job_bundle_service()._bundle_cache[stale_bundle_key] = object()  # type: ignore[assignment]
        active_fanout = _FanoutCancellation()
        server._ensure_tip_refresh_service().seed_active_refresh_for_test(
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
        self.assertEqual(server._payout_state_service._generation, 1)
        self.assertEqual(server._ensure_job_bundle_service()._bundle_cache, {})
        self.assertTrue(active_fanout.is_set())
        self.assertTrue(server._ensure_tip_refresh_service().snapshot().retry_requested)
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

        self.assertEqual(server._payout_state_service._generation, 2)
        self.assertEqual(server._payout_state_service._previews, {})
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
        mutation_lock = server._payout_state_service._balance_mutation_lock

        class ObservedBalanceLock:
            def __enter__(self) -> ObservedBalanceLock:
                if threading.current_thread() is reconcile_thread:
                    reconcile_lock_attempted.set()
                mutation_lock.acquire()
                return self

            def __exit__(self, *_args: object) -> None:
                mutation_lock.release()

        server._payout_state_service._balance_mutation_lock = ObservedBalanceLock()  # type: ignore[assignment]

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
                    with server._payout_state_service._delivery_gate.delivery():
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
            self.assertEqual(server._payout_state_service._generation, 2)
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
        server._ensure_tip_refresh_service().refresh_after_accepted_block = (  # type: ignore[method-assign]
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
            context.payout_state_generation = server._payout_state_service._generation
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
                    server._payout_state_service._generation,
                )
                preview_generation = server._payout_state_service._generation
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
            self.assertEqual(server._payout_state_service._generation, preview_generation)
            self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(rpc.submitted, [parent_hash, child_hash])
        self.assertEqual(server.accepted_block_count, 2)
        self.assertEqual(len(ledger.persisted), 2)
        self.assertEqual(len(ledger.confirmed), 2)
        self.assertEqual(
            server.block_candidate_abandoned_counts.get(PRISM_REJECTION_STALE_JOB, 0),
            0,
        )
        self.assertEqual(server._payout_state_service._previews, {})
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
                server._payout_state_service._delivery_gate._mutation_owner
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
                with server._payout_state_service._delivery_gate.delivery_cancelable(
                    lambda: False,
                    generation=server._payout_state_service._generation,
                    priority=True,
                ) as admission:
                    self.assertTrue(admission)
                    release.set()
                    thread.join(5)
                    self.assertFalse(thread.is_alive())
                    self.assertTrue(
                        server._payout_state_service._prepare_lock.acquire(timeout=1)
                    )
                    server._payout_state_service._prepare_lock.release()
                    admission.mark_delivered()
            finally:
                release.set()
                thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(accepted, [True])
        self.assertEqual(server._payout_state_service._generation, 1)
    def test_failed_direct_block_audit_does_not_reserve_payout_source(self) -> None:
        for failure_phase in ("build", "verify"):
            with self.subTest(failure_phase=failure_phase):
                server, state, ledger = submit_coordinator()
                server._ensure_job_cache_state()
                initial_source = server._payout_state_service._source
                initial_published = server._payout_state_service._published
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

                self.assertEqual(server._payout_state_service._source, initial_source)
                self.assertEqual(server._payout_state_service._published, initial_published)
                self.assertEqual(server._payout_state_service._generation, 0)
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
        self.assertEqual(server._payout_state_service._generation, 1)
        self.assertEqual(server._payout_state_service._source[0], 2)
        self.assertEqual(server._payout_state_service._published.source_generation, 1)
        self.assertTrue(server._payout_state_service._publication_blocked)
        self.assertTrue(server._payout_state_service._delivery_gate._delivery_blocked)
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

        self.assertEqual(server._payout_state_service._generation, 2)
        self.assertEqual(server._payout_state_service._published.source_generation, 2)
        self.assertEqual(server._payout_state_service._source[0], 3)
        self.assertEqual(server._payout_state_service._source[1], newer_tip)
        self.assertEqual(
            server._payout_state_service._source[2],
            "direct_block_uncertain",
        )
        self.assertTrue(server._payout_state_service._publication_blocked)
        self.assertTrue(server._payout_state_service._delivery_gate._delivery_blocked)
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
        service = server._payout_state_service
        service.publish_current_with_retry_budget = (  # type: ignore[method-assign]
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
        self.assertTrue(server._payout_state_service._publication_blocked)
        self.assertTrue(server._payout_state_service._delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())

        # The scheduled refresh publishes the pending source and reopens
        # delivery without any candidate replay.
        del service.publish_current_with_retry_budget
        self.assertIsNotNone(server._publish_current_payout_state_with_retry_budget())
        self.assertFalse(server._payout_state_service._publication_blocked)
        self.assertFalse(server._payout_state_service._delivery_gate._delivery_blocked)
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
            self.assertEqual(server._payout_state_service._generation, 1)
            self.assertEqual(
                server._payout_state_service._published.source_tip_hash,
                block_hash,
            )
            self.assertEqual(server._payout_state_service._source[0], 1)

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
            server._ensure_job_bundle_service()._bundle_cache[cache_key] = object()
            discarded_before = server._payout_state_service.metrics_snapshot()["discarded_candidates"]
            pending_marks_before = server._ensure_tip_refresh_service().snapshot().pending_counter

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )

        self.assertEqual(server._payout_state_service._generation, 1)
        self.assertEqual(server._payout_state_service._published.source_generation, 1)
        self.assertEqual(server._payout_state_service._published.source_tip_hash, block_hash)
        self.assertEqual(server._payout_state_service._source[0], 1)
        self.assertIn(cache_key, server._ensure_job_bundle_service()._bundle_cache)
        self.assertEqual(retry_calls, 0)
        self.assertEqual(
            server._ensure_tip_refresh_service().snapshot().pending_counter,
            pending_marks_before,
        )
        self.assertEqual(
            server._payout_state_service.metrics_snapshot()["discarded_candidates"],
            discarded_before,
        )
        self.assertFalse(server._payout_state_service._publication_blocked)
        self.assertFalse(server._payout_state_service._delivery_gate._delivery_blocked)
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
            self.assertEqual(server._payout_state_service._generation, 1)

            # Simulate the exception tail a replay must heal: a prior attempt
            # force-blocked delivery while its source generation already
            # matched the published source, then failed before republishing.
            # The leaked fence blocks every job build until a publication
            # lands, so an exact confirmed replay must heal the leaked fence
            # before taking the covered-state skip.
            server._block_payout_state_publication(force=True)
            self.assertTrue(server._payout_state_service._publication_blocked)
            self.assertEqual(
                server._payout_state_service._source[0],
                server._payout_state_service._published.source_generation,
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

        self.assertEqual(server._payout_state_service._generation, 2)
        # Fence healing republishes identical covered state. It advances the
        # delivery generation without inventing a logical invalidation source.
        self.assertEqual(server._payout_state_service._published.source_generation, 1)
        self.assertEqual(server._payout_state_service._source[0], 1)
        self.assertFalse(server._payout_state_service._publication_blocked)
        self.assertFalse(server._payout_state_service._delivery_gate._delivery_blocked)
    def test_direct_block_disabled_reconciler_bounds_publish_supersession(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        service = server._payout_state_service
        service.set_reconcile_retries_for_test(2)
        block_hash = "d3" * 32
        real_publish = service.publish_candidate
        publish_attempts = 0

        def supersede_before_publish(candidate: object) -> int | None:
            nonlocal publish_attempts
            publish_attempts += 1
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=f"{publish_attempts + 20:064x}",
            )
            return real_publish(candidate)  # type: ignore[arg-type]

        service.publish_candidate = supersede_before_publish  # type: ignore[method-assign]
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
        self.assertEqual(server._payout_state_service._generation, 0)
        self.assertTrue(server._payout_state_service._publication_blocked)
        self.assertTrue(server._payout_state_service._delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())
    def test_untrusted_direct_block_reconcile_publishes_newer_source_once(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.reorg_reconciler_enabled = True
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
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
        self.assertEqual(server._payout_state_service._generation, 2)
        self.assertEqual(server._payout_state_service._published.source_tip_hash, newer_tip)
        self.assertEqual(server._payout_state_service.metrics_snapshot()["discarded_candidates"], 0)
        self.assertEqual(server.reorg_reconcile_skip_count, 1)
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
        server._ensure_tip_refresh_service().refresh_after_accepted_block = (  # type: ignore[method-assign]
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
            server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
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
                # submitter queue lands the block and its admitted scheduler
                # trigger pushes fresh work before returning.
                self.assertEqual(sent, [{"id": "submit-1", "result": True, "error": None}])
                self.assertTrue(server.submit_next_block_candidate())

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
    def test_post_accept_scheduler_reports_failing_template_build(self) -> None:
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
        self.assertEqual(server.post_accept_refresh_failure_count, 1)
        self.assertEqual(state.active_job_ids, {"job-1"})
        self.assertIn("job-1", server.jobs)
        self.assertTrue(server.tip_refresh_is_pending())
        self.assertTrue(
            server._ensure_tip_refresh_service().snapshot().retry_requested
        )
        self.assertIn("qbit_prism_post_accept_refresh_failures_total 1", server.metrics_payload())
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

    def test_durable_intent_promotion_failure_keeps_attempt_floor_for_replay(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="bf" * 32,
            share_pass=False,
            block_pass=True,
        )
        submit_calls = 0

        def unsafe_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            return True

        server.submit_block_candidate = unsafe_submit  # type: ignore[method-assign]
        writer = server._ensure_share_writer_service()
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ), patch.object(
            writer,
            "begin_candidate_actor",
            side_effect=RuntimeError("promotion wiring failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "promotion wiring failed"):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(submit_calls, 0)
        self.assertEqual(len(ledger.pending_block_candidates()), 1)
        self.assertNotEqual(server._pending_share_commit_floor, {})
        pending = next(iter(server._pending_share_commit_floor.values()))[0]
        self.assertEqual(pending.share_id, "miner-a:" + submission.block_hash_hex)
        server._finish_pending_share_commit(PendingShare(**pending.__dict__))
        self.assertEqual(server._pending_share_commit_floor, {})
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

    def test_below_target_terminal_update_failure_retains_floor_until_replay(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="b9" * 32,
            share_pass=False,
            block_pass=True,
        )

        def reject_candidate(_candidate: PrismBlockCandidate) -> bool:
            server._abandon_block_candidate(
                PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                "rejected",
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = reject_candidate  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ), patch.object(
            ledger,
            "mark_block_candidate_abandoned",
            side_effect=RuntimeError("terminal update failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "terminal update failed"):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        (intent,) = ledger.pending_block_candidates()
        self.assertEqual(intent["block_hash_hex"], submission.block_hash_hex)
        self.assertNotEqual(server._pending_share_commit_floor, {})

        # Exact durable replay reconstructs the candidate, reaches a successful
        # terminal outbox return, and releases the same stable share-ID floor.
        replayed = server.block_candidate_from_intent(intent)
        server.enqueue_block_candidate(replayed)
        self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(server._pending_share_commit_floor, {})
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
        # Same-hash retries have different acceptance stamps but one durable
        # share identity. The stable S3 lease keeps the earliest stamp and a
        # reconstructed terminal can release it by share_id.
        (floor_entry,) = server._pending_share_commit_floor.values()
        self.assertEqual(len(floor_entry), 3)
        self.assertEqual(server._job_snapshot_anchor_ms(1_800_000_000_000), 1_699_999_999_999)
        server._finish_pending_share_commit(
            PendingShare(**server._retry_block_candidate.pending_share.__dict__)
        )
        self.assertEqual(server._pending_share_commit_floor, {})

    def test_retry_slot_is_not_published_before_floor_adoption(self) -> None:
        server, state, _ledger = submit_coordinator()
        pending = PendingShare(
            share_id="miner-a:" + "91" * 32,
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
        candidate = dataclass_replace(
            block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex="91" * 32,
                    block_hex="00",
                    share_pass=False,
                    block_pass=True,
                ),
                pending_share=pending,
            ),
            credit_share_on_accept=True,
        )
        writer = server._ensure_share_writer_service()
        original_adopt = writer.adopt_pending_share
        adoption_entered = threading.Event()
        release_adoption = threading.Event()

        def blocking_adopt(value: PendingShare) -> None:
            adoption_entered.set()
            release_adoption.wait(timeout=2)
            original_adopt(value)

        writer.adopt_pending_share = blocking_adopt  # type: ignore[method-assign]
        thread = threading.Thread(
            target=server._retain_block_candidate_for_retry,
            args=(candidate,),
        )
        thread.start()
        self.assertTrue(adoption_entered.wait(timeout=1))
        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        release_adoption.set()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertIs(server._retry_block_candidate, candidate)

        # Simulate the submitter winning immediately after publication. No
        # delayed post-publication adoption remains to resurrect the floor.
        server._finish_pending_share_commit(candidate.pending_share)
        self.assertEqual(server._pending_share_commit_floor, {})

    def test_failed_same_hash_attempt_cannot_release_older_durable_holder(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="95" * 32,
            share_pass=False,
            block_pass=True,
        )
        server.submit_block_candidate = lambda _candidate: False  # type: ignore[method-assign]
        submit_params = ["miner-a", "job-1", "00" * 8, "00000001", "00000002"]
        persist_entered = threading.Event()
        release_persist = threading.Event()
        errors: list[BaseException] = []
        clock_ms = iter([100, 200])

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=clock_ms.__next__,
        ):
            with self.assertRaisesRegex(RuntimeError, "pending durable retry"):
                server.handle_submit(state, submit_params)
            self.assertEqual(server._job_snapshot_anchor_ms(10_000), 99)

            def fail_second_persist(_intent: dict[str, object]) -> bool:
                persist_entered.set()
                release_persist.wait(timeout=2)
                raise RuntimeError("outbox unavailable")

            def second_attempt() -> None:
                try:
                    server.handle_submit(state, submit_params)
                except BaseException as exc:
                    errors.append(exc)

            with patch.object(
                ledger,
                "persist_block_candidate_intent",
                side_effect=fail_second_persist,
            ):
                thread = threading.Thread(target=second_attempt)
                thread.start()
                self.assertTrue(persist_entered.wait(timeout=1))
                self.assertEqual(server._job_snapshot_anchor_ms(10_000), 99)
                release_persist.set()
                thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertRegex(str(errors[0]), "outbox unavailable")
        # The failed 200ms attempt released only its object holder. The first
        # durable 100ms candidate remains reachable without re-adoption.
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 99)
        self.assertEqual(
            set(server._pending_share_commit_floor),
            {server._retry_block_candidate.pending_share.share_id},
        )
        server._finish_pending_share_commit(
            PendingShare(**server._retry_block_candidate.pending_share.__dict__)
        )
        self.assertEqual(server._pending_share_commit_floor, {})

    def test_same_hash_terminal_actor_cannot_drop_live_actor_or_failure_retry_floor(
        self,
    ) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "96" * 32
        share_id = f"miner-a:{block_hash}"

        def pending(accepted_at_ms: int) -> PendingShare:
            return PendingShare(
                share_id=share_id,
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=9,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1,
            )

        submission = SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="00",
            share_pass=False,
            block_pass=True,
        )
        actor_a = dataclass_replace(
            block_candidate(
                server,
                state,
                submission,
                pending_share=pending(100),
            ),
            credit_share_on_accept=True,
        )
        actor_b = dataclass_replace(
            block_candidate(
                server,
                state,
                submission,
                pending_share=pending(200),
            ),
            credit_share_on_accept=True,
        )
        self.assertTrue(
            ledger.persist_block_candidate_intent(server.block_candidate_intent(actor_a))
        )
        self.assertFalse(
            ledger.persist_block_candidate_intent(server.block_candidate_intent(actor_b))
        )
        a_entered = threading.Event()
        release_a = threading.Event()
        append_failed = threading.Event()
        a_results: list[bool] = []

        def controlled_submit(candidate: PrismBlockCandidate) -> bool:
            if candidate is actor_a:
                a_entered.set()
                release_a.wait(timeout=2)
                server.append_accepted_share(
                    candidate.client,
                    candidate.context,
                    candidate.submission,
                    candidate.pending_share,
                    candidate_intent=server.block_candidate_intent(candidate),
                )
                return True
            server._abandon_block_candidate(
                PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                "same-hash retry rejected",
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = controlled_submit  # type: ignore[method-assign]
        server._next_block_candidate_retry_delay = (  # type: ignore[method-assign]
            lambda _block_hash: 0.0
        )
        actor_a_thread = threading.Thread(
            target=lambda: a_results.append(
                server._submit_next_block_candidate_writer(actor_a)
            )
        )
        actor_a_thread.start()
        self.assertTrue(a_entered.wait(timeout=1))

        # B can terminalize the one durable row, but its terminal holder and
        # actor release must not erase A's independent active acceptance floor.
        self.assertTrue(server._submit_next_block_candidate_writer(actor_b))
        self.assertTrue(actor_a_thread.is_alive())
        self.assertEqual(len(ledger), 0)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(set(server._pending_share_commit_floor), {share_id})
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 99)
        writer = server._ensure_share_writer_service()
        self.assertIn(id(actor_a.pending_share), writer._candidate_actor_holders)
        self.assertNotIn(id(actor_b.pending_share), writer._candidate_actor_holders)

        def fail_actor_a_append(
            _entries: list[tuple[PendingShare, dict[str, object] | None]],
        ) -> list[object]:
            append_failed.set()
            raise RuntimeError("share commit failed after competing terminal")

        ledger.append_batch = fail_actor_a_append  # type: ignore[method-assign]
        release_a.set()
        actor_a_thread.join(timeout=2)

        self.assertFalse(actor_a_thread.is_alive())
        self.assertTrue(append_failed.is_set())
        self.assertEqual(a_results, [True])
        self.assertEqual(len(ledger), 0)
        self.assertIs(server._retry_block_candidate, actor_a)
        self.assertEqual(set(server._pending_share_commit_floor), {share_id})
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 99)
        self.assertEqual(writer._candidate_actor_holders, {})
        server._finish_pending_share_commit(actor_a.pending_share)
        self.assertEqual(server._pending_share_commit_floor, {})

    def test_sync_same_hash_terminal_cannot_drop_async_actor_before_credit(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "97" * 32
        pending_a = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=7,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=12_345,
            accepted_at_ms=100,
            ntime=1,
        )
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="00",
            share_pass=False,
            block_pass=True,
        )
        actor_a = dataclass_replace(
            block_candidate(
                server,
                state,
                submission,
                pending_share=pending_a,
            ),
            credit_share_on_accept=True,
        )
        self.assertTrue(
            ledger.persist_block_candidate_intent(server.block_candidate_intent(actor_a))
        )
        a_entered = threading.Event()
        release_a = threading.Event()
        a_results: list[bool] = []

        def controlled_submit(candidate: PrismBlockCandidate) -> bool:
            if candidate is actor_a:
                a_entered.set()
                release_a.wait(timeout=2)
                server.append_accepted_share(
                    candidate.client,
                    candidate.context,
                    candidate.submission,
                    candidate.pending_share,
                    candidate_intent=server.block_candidate_intent(candidate),
                )
                return True
            server._abandon_block_candidate(
                PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                "synchronous same-hash retry rejected",
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = controlled_submit  # type: ignore[method-assign]
        actor_a_thread = threading.Thread(
            target=lambda: a_results.append(
                server._submit_next_block_candidate_writer(actor_a)
            )
        )
        actor_a_thread.start()
        self.assertTrue(a_entered.wait(timeout=1))

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ), patch("lab.prism.prism_coordinator.now_ms", return_value=200):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_LOW_DIFFICULTY)
        self.assertTrue(actor_a_thread.is_alive())
        self.assertEqual(len(ledger), 0)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(
            server._job_snapshot_anchor_ms(10_000),
            pending_a.accepted_at_ms - 1,
        )
        writer = server._ensure_share_writer_service()
        self.assertIn(id(pending_a), writer._candidate_actor_holders)

        release_a.set()
        actor_a_thread.join(timeout=2)

        self.assertFalse(actor_a_thread.is_alive())
        self.assertEqual(a_results, [True])
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger.all_shares()[0].accepted_at_ms, 100)
        self.assertEqual(server._pending_share_commit_floor, {})
        self.assertEqual(writer._candidate_actor_holders, {})

    def test_lower_parent_retry_replacement_keeps_both_durable_floors(self) -> None:
        server, state, _ledger = submit_coordinator()

        def candidate(tag: str, height: int, accepted_at_ms: int) -> PrismBlockCandidate:
            context = SimpleNamespace(**vars(server.jobs["job-1"]))
            context.template = {**context.template, "height": height}
            pending = PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=height,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1,
            )
            return dataclass_replace(
                block_candidate(
                    server,
                    state,
                    SimpleNamespace(block_hash_hex=tag, block_hex="00"),
                    pending_share=pending,
                ),
                context=context,
                credit_share_on_accept=True,
            )

        descendant = candidate("d1" * 32, 11, 200)
        parent = candidate("a1" * 32, 10, 100)

        server._retain_block_candidate_for_retry(descendant)
        server._retain_block_candidate_for_retry(parent)

        self.assertIs(server._retry_block_candidate, parent)
        self.assertEqual(
            set(server._pending_share_commit_floor),
            {descendant.pending_share.share_id, parent.pending_share.share_id},
        )
        for retained in (parent, descendant):
            server._finish_pending_share_commit(
                PendingShare(**retained.pending_share.__dict__)
            )
        self.assertEqual(server._pending_share_commit_floor, {})

    def test_equal_height_nonselection_keeps_competitor_durable_floor(self) -> None:
        server, state, _ledger = submit_coordinator()

        def candidate(tag: str, accepted_at_ms: int) -> PrismBlockCandidate:
            context = SimpleNamespace(**vars(server.jobs["job-1"]))
            context.template = {**context.template, "height": 10}
            pending = PendingShare(
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
                ntime=1,
            )
            return dataclass_replace(
                block_candidate(
                    server,
                    state,
                    SimpleNamespace(block_hash_hex=tag, block_hex="00"),
                    pending_share=pending,
                ),
                context=context,
                credit_share_on_accept=True,
            )

        selected = candidate("b1" * 32, 100)
        competitor = candidate("c1" * 32, 200)

        server._retain_block_candidate_for_retry(selected)
        server._retain_block_candidate_for_retry(competitor)

        self.assertIs(server._retry_block_candidate, selected)
        self.assertEqual(
            set(server._pending_share_commit_floor),
            {selected.pending_share.share_id, competitor.pending_share.share_id},
        )
        for retained in (selected, competitor):
            server._finish_pending_share_commit(
                PendingShare(**retained.pending_share.__dict__)
            )
        self.assertEqual(server._pending_share_commit_floor, {})
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

        static_replayed = PrismCoordinator.block_candidate_from_intent(intent)
        self.assertTrue(static_replayed.context.collection_only)
        self.assertEqual(
            static_replayed.context.prospective_prior_balances,
            (("miner-a", "miner-a", "11" * 32, 25),),
        )

        # Intents persisted before the flag existed replay as ready-window
        # candidates, which is all the outbox could ever have contained then.
        intent.pop("collection_only")
        intent.pop("prospective_prior_balances")
        replayed = server.block_candidate_from_intent(intent)
        self.assertFalse(replayed.context.collection_only)
        self.assertIsNone(replayed.context.prospective_prior_balances)

    def test_replayed_credit_candidate_adopts_floor_before_job_anchor(self) -> None:
        server, state, _ledger = submit_coordinator()
        pending = PendingShare(
            share_id="miner-a:" + "92" * 32,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=123,
            ntime=1,
        )
        candidate = dataclass_replace(
            block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex="92" * 32,
                    block_hex="00",
                    share_pass=False,
                    block_pass=True,
                ),
                pending_share=pending,
            ),
            credit_share_on_accept=True,
        )

        replayed = server.block_candidate_from_intent(
            server.block_candidate_intent(candidate)
        )

        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 122)
        server._finish_pending_share_commit(
            PendingShare(**replayed.pending_share.__dict__)
        )
        self.assertEqual(server._pending_share_commit_floor, {})
