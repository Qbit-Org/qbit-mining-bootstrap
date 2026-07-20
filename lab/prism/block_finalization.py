"""Measured, replay-safe PRISM accepted-block finalization."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import threading
import time
import traceback
from typing import Any, Iterator

from lab.prism import direct_stratum, public_api
from lab.prism.audit_artifacts import AuditPublicationIdentity
from lab.prism.block_candidates import PrismBlockCandidate
from lab.prism.share_ledger import sha256_json_hex
from lab.prism.share_submission import (
    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
    PRISM_REJECTION_POOL_CLOSED,
    PRISM_REJECTION_STALE_JOB,
)


PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH = "candidate-audit-mismatch"
PRISM_REJECTION_SUBMITBLOCK_REJECTED = "submitblock-rejected"
PRISM_REJECTION_BLOCK_STALE = "block-stale"
PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED = "ledger-confirmation-failed"
FINALIZATION_PHASES = (
    "admission",
    "land_confirm",
    "ctv_credit",
    "evidence",
    "audit_publish",
    "accounting",
)


@dataclass(frozen=True)
class FinalizationAdmission:
    """Immutable result of candidate admission and active-chain classification."""

    candidate: PrismBlockCandidate
    context: Any
    submission: direct_stratum.DirectQbitSubmission
    worker: str | None
    expected_height: int
    block_hash: str
    parent_hash: str
    current_tip: str
    already_active: bool


@dataclass(frozen=True)
class LandedCandidate:
    """Durable landing outputs consumed by the remaining ordered phases."""

    final_bundle: dict[str, Any]
    report: dict[str, Any]
    persistence: dict[str, Any]
    confirmation: dict[str, Any]
    audit_publication_identity: AuditPublicationIdentity
    audit_verification_identity: dict[str, Any]


@dataclass(frozen=True)
class FinalizationEvidence:
    """Evidence body and its normalized publication persistence identity."""

    evidence: dict[str, Any]
    publication_persistence: dict[str, Any]


class BlockFinalizationService:
    """Own accepted-block finalization while forwarding infrastructure ports."""

    def __init__(self, runtime: Any) -> None:
        object.__setattr__(self, "runtime", runtime)
        object.__setattr__(self, "_metrics_lock", threading.Lock())
        object.__setattr__(
            self,
            "_phase_metrics",
            {
                phase: {"count": 0, "sum": 0.0, "max": 0.0}
                for phase in FINALIZATION_PHASES
            },
        )
        object.__setattr__(self, "_last_candidate_started", None)
        object.__setattr__(
            self,
            "_candidate_intervals",
            {"count": 0, "sum": 0.0, "min": None},
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self.runtime, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in {
            "runtime",
            "_metrics_lock",
            "_phase_metrics",
            "_last_candidate_started",
            "_candidate_intervals",
        }:
            object.__setattr__(self, name, value)
        else:
            setattr(self.runtime, name, value)

    @contextmanager
    def _phase(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        try:
            yield
        finally:
            elapsed = max(0.0, time.monotonic() - started)
            with self._metrics_lock:
                metric = self._phase_metrics[name]
                metric["count"] = int(metric["count"]) + 1
                metric["sum"] = float(metric["sum"]) + elapsed
                metric["max"] = max(float(metric["max"]), elapsed)

    def _note_candidate_started(self) -> None:
        now = time.monotonic()
        with self._metrics_lock:
            previous = self._last_candidate_started
            self._last_candidate_started = now
            if previous is None:
                return
            interval = max(0.0, now - previous)
            metric = self._candidate_intervals
            metric["count"] = int(metric["count"]) + 1
            metric["sum"] = float(metric["sum"]) + interval
            current_min = metric["min"]
            metric["min"] = (
                interval if current_min is None else min(float(current_min), interval)
            )

    def metrics_snapshot(self) -> dict[str, Any]:
        with self._metrics_lock:
            return {
                "phases": {
                    name: dict(value) for name, value in self._phase_metrics.items()
                },
                "candidate_intervals": dict(self._candidate_intervals),
            }

    def metrics_lines(self) -> list[str]:
        snapshot = self.metrics_snapshot()
        phases = snapshot["phases"]
        intervals = snapshot["candidate_intervals"]
        lines = [
            "# HELP qbit_prism_block_finalization_phase_seconds Accepted-block finalization wall time by ordered phase.",
            "# TYPE qbit_prism_block_finalization_phase_seconds summary",
        ]
        for phase in FINALIZATION_PHASES:
            metric = phases[phase]
            lines.extend(
                [
                    f'qbit_prism_block_finalization_phase_seconds_sum{{phase="{phase}"}} {float(metric["sum"]):.6f}',
                    f'qbit_prism_block_finalization_phase_seconds_count{{phase="{phase}"}} {int(metric["count"])}',
                    f'qbit_prism_block_finalization_phase_seconds_max{{phase="{phase}"}} {float(metric["max"]):.6f}',
                ]
            )
        interval_count = int(intervals["count"])
        interval_sum = float(intervals["sum"])
        interval_min = intervals["min"]
        lines.extend(
            [
                "# HELP qbit_prism_block_candidate_interarrival_seconds Time between finalization starts.",
                "# TYPE qbit_prism_block_candidate_interarrival_seconds summary",
                f"qbit_prism_block_candidate_interarrival_seconds_sum {interval_sum:.6f}",
                f"qbit_prism_block_candidate_interarrival_seconds_count {interval_count}",
                "qbit_prism_block_candidate_interarrival_seconds_min "
                + ("0.000000" if interval_min is None else f"{float(interval_min):.6f}"),
            ]
        )
        return lines

    def _land_and_confirm_block_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        current_tip: str,
        already_active: bool,
        worker: str | None,
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
        AuditPublicationIdentity,
        dict[str, Any],
    ] | None:
        """Land, verify, publish, persist, and confirm one candidate.

        The balance serializer spans the last prior-state check through durable
        confirmation. Reconciliation therefore cannot change the base beneath
        the accepted coinbase, while ordinary job delivery remains unblocked.
        """
        context = candidate.context
        submission = candidate.submission
        expected_height = int(context.template["height"])
        block_hash = str(submission.block_hash_hex).lower()
        parent_hash = str(context.template["previousblockhash"])
        self._ensure_job_cache_state()
        durable_payout_state = bool(
            getattr(self.ledger, "durable_payout_state", False)
        )
        with self._ensure_payout_state_service().balance_mutation_lock:
            if self._defer_for_pending_parent_payout_transition(
                parent_hash=parent_hash,
                parent_height=expected_height - 1,
                worker=worker,
                active_candidate_hash=block_hash if already_active else None,
                active_candidate_height=expected_height if already_active else None,
            ):
                return None
            block_state: dict[str, object] | None = None
            block_state_reader = getattr(self.ledger, "pool_block_state", None)
            transition_already_landed = self._accepted_block_payout_transition_landed(
                block_hash
            )
            reorg_reconciled: bool | None = None
            if already_active and not transition_already_landed:
                # A replayed active ancestor may coexist with balances from an
                # orphaned pool block. Reconcile that global state before this
                # transition becomes a landed barrier and before validating its
                # payout base.
                try:
                    reorg_reconciled = self.ensure_reorg_reconciled_for_tip(current_tip)
                except Exception:
                    traceback.print_exc()
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation failed before block replay",
                        worker=worker,
                    )
                    return None
                if not reorg_reconciled:
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation reported an untrusted chain view",
                        worker=worker,
                    )
                    return None
            if already_active and callable(block_state_reader):
                block_state = block_state_reader(block_hash=block_hash)
            already_confirmed = bool(
                block_state is not None
                and str(block_state.get("chain_state", "")) == "confirmed"
                and str(block_state.get("maturity_state", "")) != "reversed"
            )
            if already_confirmed:
                # The outbox terminal update can fail after a fully durable
                # confirmation. Do not replace later global balances with an
                # ancestor-only preview during exact-idempotent replay.
                self._clear_accepted_block_payout_preview(block_hash)
                reorg_reconciled = True
            elif already_active:
                self._begin_accepted_block_payout_preview(
                    block_hash,
                    block_height=expected_height,
                )
                self._mark_accepted_block_payout_landed(
                    block_hash,
                    block_height=expected_height,
                )
                reorg_reconciled = True
            elif transition_already_landed:
                # A prior attempt reached submitblock while holding this
                # serializer. External reconciliation is barred until it
                # confirms or is withdrawn, so retry its durable steps directly.
                reorg_reconciled = True
            else:
                try:
                    reorg_reconciled = self.ensure_reorg_reconciled_for_tip(current_tip)
                except Exception:
                    traceback.print_exc()
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation failed before block submit",
                        worker=worker,
                    )
                    return None
            if not reorg_reconciled:
                self._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "reorg reconciliation reported an untrusted chain view",
                    worker=worker,
                )
                return None
            if (
                already_active
                and not already_confirmed
                and self._defer_for_pending_parent_payout_transition(
                    parent_hash=parent_hash,
                    parent_height=expected_height - 1,
                    worker=worker,
                )
            ):
                return None
            if (
                durable_payout_state
                and not already_active
                and not self.prior_balances_match_current(context.prior_balances)
            ):
                self._clear_accepted_block_payout_preview(
                    block_hash,
                    invalidate_published=True,
                )
                self._abandon_block_candidate(
                    PRISM_REJECTION_STALE_JOB,
                    "prior balances changed since the job was issued",
                    worker=worker,
                )
                return None
            if not already_active:
                before_height = int(self.rpc.call("getblockcount"))
                if before_height + 1 != expected_height:
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BLOCK_STALE,
                        f"stale block height: template={expected_height} tip={before_height}",
                        worker=worker,
                    )
                    return None
                # Register before submitblock can expose this hash as the new
                # tip. Child builders will wait for the verified preview rather
                # than reading balances that omit their new parent.
                self._begin_accepted_block_payout_preview(
                    block_hash,
                    block_height=expected_height,
                )
                # Treat the submit outcome as uncertain before entering RPC.
                # If transport fails after qbitd accepted the block, this
                # conservative barrier preserves the coinbase's payout base.
                self._mark_accepted_block_payout_landed(
                    block_hash,
                    block_height=expected_height,
                )
                self._record_heartbeat("block_submitter")
                result = self.rpc.call("submitblock", [submission.block_hex])
                self._record_heartbeat("block_submitter")
                if result not in (None, "duplicate"):
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                        f"submitblock rejected candidate: {result}",
                        worker=worker,
                    )
                    return None
                active_hash = str(
                    self.rpc.call("getblockhash", [expected_height])
                ).lower()
                if active_hash != block_hash:
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                        f"submitted block is not active at height {expected_height}",
                        worker=worker,
                    )
                    return None
                self._cancel_obsolete_job_builds("direct PRISM block accepted")
                self._mark_tip_refresh_pending(block_hash)
                self._schedule_tip_refresh_retry()

            preview: list[dict[str, object]] | None = None
            issued_preview = getattr(context, "prospective_prior_balances", None)
            if not already_confirmed and issued_preview is not None:
                # The compact preview came from the immutable issued job
                # summary. Publish it before rebuilding/canonicalizing the full
                # audit bundle, without retaining that bundle's shares tree.
                preview = self._materialize_prior_balance_preview(issued_preview)
                if durable_payout_state and not self.prior_balances_match_current(
                    context.prior_balances
                ):
                    self.request_shutdown()
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                        "accepted block payout base changed before preview publication",
                        worker=worker,
                    )
                    return None
                self._publish_accepted_block_payout_preview(block_hash, preview)

            self._record_heartbeat("block_submitter")
            audit_store = self._ensure_audit_artifact_store()
            candidate_artifact = audit_store.issue_candidate(
                block_hash=submission.block_hash_hex
            )
            candidate_bundle_path = candidate_artifact.path
            compiler_transferred_candidate = False

            def adopt_compiler_output(path: Path, value: os.stat_result) -> None:
                nonlocal compiler_transferred_candidate
                audit_store.adopt_compiler_candidate(
                    candidate_artifact,
                    path=path,
                    value=value,
                )
                compiler_transferred_candidate = True

            compiler_parent_fd = audit_store.duplicate_root_directory_fd()
            try:
                final_bundle = self.build_audit_bundle(
                    shares=context.shares_json,
                    found_block=context.found_block,
                    prior_balances=context.prior_balances,
                    coinbase_script_sig_suffix_hex=self.coinbase_script_sig_suffix_hex(
                        candidate.extranonce1_hex,
                        candidate.extranonce2_hex,
                    ),
                    witness_merkle_leaves_hex=list(
                        getattr(context.job, "witness_merkle_leaves_hex", ())
                    )
                    or direct_stratum.witness_merkle_leaves_hex(
                        getattr(context.job, "transaction_hexes", ())
                    ),
                    ctv_fee_parent_hash=parent_hash,
                    canonical_output_path=candidate_bundle_path,
                    canonical_output_parent_fd=compiler_parent_fd,
                    canonical_output_adopter=adopt_compiler_output,
                )
            except BaseException:
                audit_store.discard_candidate(candidate_artifact)
                raise
            finally:
                os.close(compiler_parent_fd)
            # Compatibility builders used by tests and older integrations may
            # ignore canonical_output_path. Persist their logical bundle via
            # the normal canonicalization fallback without mislabeling bytes.
            try:
                if not candidate_bundle_path.exists():
                    candidate_bundle_path = audit_store.write_compatibility_candidate(
                        candidate_artifact,
                        final_bundle,
                    )
                else:
                    if not compiler_transferred_candidate:
                        raise RuntimeError(
                            "audit builder created an output path without exact inode transfer"
                        )
                final_manifest = final_bundle["signed_coinbase_manifest"]["manifest"]
                final_coinbase_tx_hex_raw = final_manifest["coinbase_tx_hex"]
                if not isinstance(final_coinbase_tx_hex_raw, str):
                    raise ValueError(
                        "final audit bundle coinbase_tx_hex is not a string"
                    )
                final_coinbase_tx_hex = final_coinbase_tx_hex_raw.lower()
            except BaseException:
                audit_store.discard_candidate(candidate_artifact)
                raise
            if final_coinbase_tx_hex != submission.coinbase_tx_hex.lower():
                audit_store.discard_candidate(candidate_artifact)
                self.request_shutdown()
                self._clear_accepted_block_payout_preview(
                    block_hash,
                    invalidate_published=True,
                )
                self._abandon_block_candidate(
                    PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                    "final audit bundle coinbase does not match submitted coinbase",
                    worker=worker,
                )
                return None
            payout_commit_started: float | None = None
            payout_commit_source: int | None = None
            try:
                verifier_override = self.runtime.__dict__.get("verify_bundle")
                configured_writer_key = getattr(
                    self,
                    "ledger_writer_public_key_hex",
                    None,
                )
                verified_audit = audit_store.verify_candidate(
                    candidate_artifact,
                    coinbase_tx_hex=submission.coinbase_tx_hex,
                    expected_coinbase_value_sats=int(context.template["coinbasevalue"]),
                    expected_block_height=expected_height,
                    trusted_writer_public_key_hex=(
                        self.trusted_ledger_writer_public_key_hex(final_bundle)
                    ),
                    trust_source=(
                        "configured"
                        if configured_writer_key is not None
                        else "embedded_test_only"
                    ),
                    verifier=(
                        verifier_override
                        if callable(verifier_override)
                        else None
                    ),
                )
                audit_store.require_current_verified_candidate(
                    verified_audit,
                    candidate_artifact,
                )
                report = dict(verified_audit.report)
                persistence_canonical_bundle_path = (
                    candidate_bundle_path
                    if verified_audit.canonical_copy_eligible
                    else None
                )
                self._record_heartbeat("block_submitter")
                verified_preview = self._accepted_block_payout_preview_from_bundle(
                    final_bundle,
                    prior_balances=context.prior_balances,
                )
                if not already_confirmed:
                    if preview is None and durable_payout_state:
                        live_prior_balances = self.normalized_prior_balances(
                            self.ledger.current_prior_balances()
                        )
                        expected_prior_balances = self.normalized_prior_balances(
                            context.prior_balances
                        )
                        if live_prior_balances != expected_prior_balances:
                            self.request_shutdown()
                            self._clear_accepted_block_payout_preview(
                                block_hash,
                                invalidate_published=True,
                            )
                            self._abandon_block_candidate(
                                PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                                "accepted block payout base changed before preview publication",
                                worker=worker,
                            )
                            return None
                    try:
                        self._publish_accepted_block_payout_preview(
                            block_hash,
                            verified_preview,
                        )
                    except RuntimeError as exc:
                        self.request_shutdown()
                        self._clear_accepted_block_payout_preview(
                            block_hash,
                            invalidate_published=True,
                        )
                        self._abandon_block_candidate(
                            PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                            "verified final payout preview does not match the "
                            f"issued block job: {exc}",
                            worker=worker,
                        )
                        return None
                preview = verified_preview

                # The verified preview is now the effective balance snapshot,
                # so persistence can do canonicalization, body writes, copies,
                # and bulk SQL without owning the delivery gate.
                payout_commit_started = time.monotonic()
                payout_commit_source = self._capture_payout_state_source()[1]
                persistence = self.ledger.persist_accepted_block(
                    block_hash=submission.block_hash_hex,
                    block_height=expected_height,
                    parent_hash=parent_hash,
                    final_bundle=final_bundle,
                    audit_report=report,
                    canonical_bundle_path=persistence_canonical_bundle_path,
                )
                self._record_heartbeat("block_submitter")
                active_hash = str(
                    self.rpc.call("getblockhash", [expected_height])
                ).lower()
                if active_hash != block_hash:
                    if already_confirmed:
                        self._abandon_block_candidate(
                            PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                            "accepted ancestor left the active chain during replay",
                            worker=worker,
                        )
                        return None
                    active_tip_height = int(self.rpc.call("getblockcount"))
                    self.reject_prepared_block(
                        block_hash=block_hash,
                        active_tip_height=active_tip_height,
                    )
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_BLOCK_STALE,
                        "accepted block left the active chain before ledger confirmation",
                        worker=worker,
                    )
                    return None
                with audit_store.publication_order_guard():
                    confirmation = self.ledger.confirm_accepted_block(
                        block_hash=block_hash,
                        # The ledger confirmation function matches this value
                        # against the candidate row's own height. An accepted
                        # ancestor can be finalized after newer blocks arrive.
                        active_tip_height=expected_height,
                    )
                    confirmed_count = int(confirmation.get("confirmed_count", 0))
                    if confirmed_count == 1:
                        audit_publication_identity = (
                            self._audit_publication_identity(
                                block_hash=block_hash,
                                block_height=expected_height,
                                confirmation=confirmation,
                            )
                        )
                if confirmed_count != 1:
                    self.request_shutdown()
                    self._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self._abandon_block_candidate(
                        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                        f"ledger did not confirm accepted block {block_hash}",
                        worker=worker,
                    )
                    return None

                if durable_payout_state:
                    # Compare the durable active-chain view as of this block,
                    # not the global latest view: an exact replay may finalize
                    # ancestor A after later pool block B is already confirmed.
                    # This also preserves the invariant across restart after a
                    # prior post-confirm mismatch instead of silently accepting
                    # the already-confirmed row on the next attempt.
                    as_of_reader = getattr(
                        self.ledger,
                        "prior_balances_after_pool_block",
                        None,
                    )
                    confirmed_balances = self.normalized_prior_balances(
                        as_of_reader(block_hash=block_hash)
                        if callable(as_of_reader)
                        else self.ledger.current_prior_balances()
                    )
                    if confirmed_balances != preview:
                        self.request_shutdown()
                        self._clear_accepted_block_payout_preview(
                            block_hash,
                            invalidate_published=True,
                        )
                        self._abandon_block_candidate(
                            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                            "confirmed payout balances do not match the published "
                            f"preview for accepted block {block_hash}",
                            worker=worker,
                        )
                        return None
                # Durability caught up to the already-published logical state;
                # clearing the parent override needs no second generation bump.
                self._clear_accepted_block_payout_preview(block_hash)
                self._schedule_current_payout_ledger_artifact_if_missing()
                payout_publication_required = (
                    self._payout_source_requires_publication()
                )
                payout_publication_fenced = (
                    self._payout_state_publication_fenced()
                )
                if payout_publication_required or payout_publication_fenced:
                    # A covered replay normally has no publication work. The
                    # exception is a leaked delivery fence whose source already
                    # published: force one republish so the replay heals it.
                    covered_replay_fence = (
                        payout_publication_fenced
                        and not payout_publication_required
                    )
                    with self.lock:
                        pending_cause = self._ensure_payout_state_service().snapshot().source[2]
                    # A bounded preview-publication loss already left the gate
                    # fenced and its retry scheduled. Do not monopolize the
                    # submitter with a second retry budget. Uncertain commits,
                    # ordinary unfenced tip sources, and a covered replay's
                    # leaked fence still reconcile now.
                    publish_now = (
                        covered_replay_fence
                        or pending_cause == "direct_block_uncertain"
                        or not payout_publication_fenced
                    )
                    published: int | None = None
                    if publish_now and getattr(
                        self,
                        "reorg_reconciler_enabled",
                        True,
                    ):
                        with self.lock:
                            latest_tip = self._ensure_payout_state_service().snapshot().source[1]
                        summary = self.reconcile_prism_pool_blocks_once(
                            tip_hash=latest_tip,
                            _force_publish=True,
                            _source_reserved=True,
                        )
                        reconciled_generation = summary.get("published_generation")
                        if isinstance(reconciled_generation, int):
                            published = reconciled_generation
                    elif publish_now:
                        published = (
                            self._publish_current_payout_state_with_retry_budget()
                        )
                    if publish_now and published is None:
                        # The block is durably confirmed; only the payout
                        # publication lost its race. Aborting would keep the
                        # outbox row pending and replay persist/confirm churn
                        # for an already-final block. Keep delivery fenced and
                        # let the scheduled tip refresh publish the newest
                        # source; this candidate's durable work is complete.
                        self._block_payout_state_publication()
                        print(
                            "prism coordinator: accepted block confirmed "
                            "durably; payout publication deferred to the "
                            f"scheduled refresh hash={block_hash}",
                            flush=True,
                        )
                return (
                    final_bundle,
                    report,
                    persistence,
                    confirmation,
                    audit_publication_identity,
                    dict(verified_audit.verification_identity),
                )
            except Exception:
                if payout_commit_started is not None and payout_commit_source is not None:
                    # Persistence/confirmation can report failure after a
                    # durable partial commit. Supersede every prepared source
                    # and keep all delivery fenced until replay/reconciliation
                    # proves the resulting ledger state.
                    self._block_payout_state_publication(
                        supersede_with=(
                            payout_commit_source,
                            block_hash,
                            "direct_block_uncertain",
                            payout_commit_started,
                        )
                    )
                raise
            finally:
                if payout_commit_started is not None:
                    self._observe_payout_state_seconds(
                        "preparation",
                        max(0.0, time.monotonic() - payout_commit_started),
                    )
                audit_store.discard_candidate(candidate_artifact)

    def _admit_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> FinalizationAdmission | None:
        outcome = getattr(self, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self._block_candidate_outcome = outcome
        outcome.reason = None
        context = candidate.context
        submission = candidate.submission
        worker = candidate.client.username or None
        expected_height = int(context.template["height"])
        block_hash = str(submission.block_hash_hex).lower()
        parent_hash = str(context.template["previousblockhash"])
        self._ensure_job_cache_state()
        with self.lock:
            pool_closed = (
                self.accepted_block_count >= self.max_blocks
                and block_hash not in self._accounted_accepted_block_hashes
            )
        if pool_closed:
            self._clear_accepted_block_payout_preview(
                block_hash,
                invalidate_published=True,
            )
            self._abandon_block_candidate(
                PRISM_REJECTION_POOL_CLOSED,
                "pool is no longer accepting blocks",
                worker=worker,
            )
            return None
        current_tip = str(self.rpc.call("getbestblockhash"))
        landed_height: int | None = None
        if current_tip.lower() == block_hash:
            landed_height = expected_height
        elif current_tip != parent_hash:
            try:
                landed_height = self.active_block_candidate_height(block_hash)
            except Exception:
                traceback.print_exc()
                self._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "could not determine whether a prior candidate is active",
                    worker=worker,
                )
                return None
        already_active = landed_height == expected_height
        if landed_height is not None and not already_active:
            self._clear_accepted_block_payout_preview(
                block_hash,
                invalidate_published=True,
            )
            self._abandon_block_candidate(
                PRISM_REJECTION_BLOCK_STALE,
                f"candidate active at unexpected height {landed_height}",
                worker=worker,
            )
            return None
        if already_active:
            print(
                "prism coordinator: resuming finalization for active block candidate "
                f"height={landed_height} hash={submission.block_hash_hex}",
                flush=True,
            )
        elif parent_hash != current_tip:
            self._clear_accepted_block_payout_preview(
                block_hash,
                invalidate_published=True,
            )
            self._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                f"tip moved before submit: {current_tip}",
                worker=worker,
            )
            return None
        return FinalizationAdmission(
            candidate=candidate,
            context=context,
            submission=submission,
            worker=worker,
            expected_height=expected_height,
            block_hash=block_hash,
            parent_hash=parent_hash,
            current_tip=current_tip,
            already_active=already_active,
        )

    def _land_candidate(
        self,
        admission: FinalizationAdmission,
    ) -> LandedCandidate | None:
        landed = self._land_and_confirm_block_candidate(
            admission.candidate,
            current_tip=admission.current_tip,
            already_active=admission.already_active,
            worker=admission.worker,
        )
        if landed is None:
            return None
        return LandedCandidate(*landed)

    def _candidate_already_accounted(self, block_hash: str) -> bool:
        with self.lock:
            return block_hash in self._accounted_accepted_block_hashes

    def _persist_ctv_and_credit(
        self,
        admission: FinalizationAdmission,
        landed: LandedCandidate,
    ) -> dict[str, Any] | None:
        ctv_persistence = None
        ctv_manifest_set = landed.final_bundle.get("ctv_fanout_manifest_set")
        if isinstance(ctv_manifest_set, dict):
            ctv_persistence = self.ledger.persist_ctv_fanout_manifest_set(
                block_hash=admission.block_hash,
                manifest_set=ctv_manifest_set,
                manifest_set_sha256=sha256_json_hex(ctv_manifest_set),
            )
        candidate = admission.candidate
        if candidate.credit_share_on_accept:
            self.append_accepted_share(
                candidate.client,
                admission.context,
                admission.submission,
                candidate.pending_share,
                candidate_intent=self.block_candidate_intent(candidate),
            )
        return ctv_persistence

    def _build_finalization_evidence(
        self,
        admission: FinalizationAdmission,
        landed: LandedCandidate,
        ctv_persistence: dict[str, Any] | None,
    ) -> FinalizationEvidence:
        # Aggregate counts only: materializing the whole share history
        # (all_shares) here would scan the full ledger twice per block,
        # and would grow without bound as the ledger grows.
        evidence_share_count, evidence_distinct_miners = self.accepted_share_stats()
        evidence = {
            "schema": "qbit.prism.live-stratum-evidence.v1",
            "block_hash": admission.block_hash,
            "block_height": admission.expected_height,
            "coinbase_tx_hex": admission.submission.coinbase_tx_hex,
            "audit_report": landed.report,
            "ledger_backend": self.ledger.backend_name,
            "persistence": landed.persistence,
            "confirmation": landed.confirmation,
            "audit_verification_identity": landed.audit_verification_identity,
            "ctv_persistence": ctv_persistence,
            "accepted_share_count": evidence_share_count,
            "distinct_miner_count": evidence_distinct_miners,
            "job_share_count": len(admission.context.shares_json),
        }
        publication_persistence = dict(landed.persistence)
        publication_persistence.setdefault(
            "audit_bundle_sha256",
            landed.report.get("audit_bundle_sha256_hex"),
        )
        publication_persistence.setdefault("body_uri", "")
        evidence["persistence"] = publication_persistence
        return FinalizationEvidence(
            evidence=evidence,
            publication_persistence=publication_persistence,
        )

    def _publish_finalization_evidence(
        self,
        landed: LandedCandidate,
        prepared: FinalizationEvidence,
    ) -> dict[str, Any]:
        audit_store = self._ensure_audit_artifact_store()
        with self._ensure_payout_state_service().balance_mutation_lock:
            with audit_store.publication_order_guard():
                publication_floor_reader = getattr(
                    self.ledger,
                    "audit_publication_sequence_floor",
                    None,
                )
                if callable(publication_floor_reader):
                    # This is deliberately a fresh durable-row read immediately
                    # before A1 publication. Confirmation-time state or a raw
                    # sequence value cannot fence rollback gaps and restart
                    # replays. P1's local serializer plus A1's process guard
                    # prevent another confirmation/reactivation from allocating
                    # between this read and the durable publication decision.
                    publication_floor_sequence = publication_floor_reader()
                else:
                    # Compatibility-only ledgers used by legacy embeddings/tests
                    # do not own durable ordinal state. Production memory/Postgres
                    # backends implement the reader above.
                    publication_floor_sequence = (
                        landed.audit_publication_identity.sequence
                    )
                publication = audit_store.publish_success(
                    identity=landed.audit_publication_identity,
                    publication_floor_sequence=publication_floor_sequence,
                    report=landed.report,
                    persistence=prepared.publication_persistence,
                    evidence=prepared.evidence,
                    verification_identity=landed.audit_verification_identity,
                    created_at=public_api.utc_now_iso(),
                )
        return dict(publication.evidence)

    def _account_finalized_candidate(
        self,
        admission: FinalizationAdmission,
        landed: LandedCandidate,
        published_evidence: dict[str, Any],
    ) -> bool:
        # The copied publication is intentionally consumed before accounting;
        # converting an invalid publication remains a finalization failure.
        del published_evidence
        with self.lock:
            newly_accounted = (
                admission.block_hash not in self._accounted_accepted_block_hashes
            )
            if newly_accounted:
                self._accounted_accepted_block_hashes.add(admission.block_hash)
                self.accepted_block_count += 1
            self.latest_coinbase_size_bytes = len(
                str(
                    landed.final_bundle["signed_coinbase_manifest"]["manifest"][
                        "coinbase_tx_hex"
                    ]
                )
            ) // 2
            should_stop = newly_accounted and (
                self.stop_after_block or self.accepted_block_count >= self.max_blocks
            )
        if not newly_accounted:
            return True
        print(
            "prism coordinator: qbit accepted direct PRISM block "
            f"height={admission.expected_height} hash={admission.block_hash}",
            flush=True,
        )
        if should_stop:
            self.request_shutdown()
        else:
            # The public submitter wrapper performs this fanout only after its
            # writer scope (including outbox finalization) exits. The rare
            # synchronous share path consumes the same marker after sending
            # the Stratum result.
            admission.candidate.client.post_accept_refresh_block = (
                admission.expected_height,
                admission.block_hash,
            )
        return True

    def submit_block_candidate(self, candidate: PrismBlockCandidate) -> bool:
        """Run the ordered, replay-safe accepted-block finalization phases."""
        self._note_candidate_started()
        with self._phase("admission"):
            admission = self._admit_candidate(candidate)
        if admission is None:
            return False
        with self._phase("land_confirm"):
            landed = self._land_candidate(admission)
        if landed is None:
            return False
        if self._candidate_already_accounted(admission.block_hash):
            # A previous attempt completed every success side effect but its
            # durable outbox terminal update failed. Retrying must not duplicate
            # CTV persistence, share credit, publication, or block accounting.
            return True
        with self._phase("ctv_credit"):
            ctv_persistence = self._persist_ctv_and_credit(admission, landed)
        with self._phase("evidence"):
            prepared = self._build_finalization_evidence(
                admission,
                landed,
                ctv_persistence,
            )
        with self._phase("audit_publish"):
            published_evidence = self._publish_finalization_evidence(
                landed,
                prepared,
            )
        with self._phase("accounting"):
            return self._account_finalized_candidate(
                admission,
                landed,
                published_evidence,
            )
