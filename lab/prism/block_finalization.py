"""Measured, replay-safe PRISM accepted-block finalization.

B3 owner module: correctness after node-offer evidence exists. It owns the
post-offer admission/finalization policy of ``submit_block_candidate`` and
``_submit_block_candidate_serialized``, the replay-window decision that
consumes P1's durable window proof, ``_land_and_confirm_block_candidate``,
the CTV/share-credit tail, ordered PR 75 audit evidence publication, and
accepted-block process accounting. The node offer, same-hash disposition,
accounting handoff, and durable outbox terminalization stay with B1
(``lab/prism/block_candidates.py``); payout previews, balance serialization,
append/anchor fences, and window proofs stay with P1
(``lab/prism/payout_state.py``). The service accepts B1's prepared
``node_submission`` and never creates a second transport path — its rare
fenced fallback calls B1's node-offer seam through the runtime.
"""

from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import threading
import time
import traceback
from typing import Any, Callable, Iterator, Mapping, Protocol

from lab.prism import direct_stratum, public_api
from lab.prism.audit_artifacts import AuditArtifactStore, AuditPublicationIdentity
from lab.prism.block_candidates import (
    PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED,
    PrismBlockCandidate,
    _BlockCandidateNodeSubmission,
)
from lab.prism.share_ledger import WriterLeaseRenewalDeferred, sha256_json_hex
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
    """Immutable result of candidate admission and active-chain classification.

    ``current_tip`` records the observed post-probe tip that payout
    reconciliation must follow; the fresh-attempt classification against the
    stamped parent stays local to admission. ``node_submission`` carries B1's
    fast-lane offer evidence into the landing phase unchanged.
    """

    candidate: PrismBlockCandidate
    context: Any
    submission: direct_stratum.DirectQbitSubmission
    worker: str | None
    expected_height: int
    block_hash: str
    parent_hash: str
    current_tip: str
    already_active: bool
    node_submission: _BlockCandidateNodeSubmission


@dataclass(frozen=True)
class LandedCandidate:
    """Durable landing outputs consumed by the remaining ordered phases.

    Combines the landing's four accounting mappings with PR 75's ordered
    audit publication identity and the verifier's identity evidence.
    """

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


class BlockFinalizationPort(Protocol):
    """Explicit coordinator capabilities required by finalization.

    Every runtime attribute and method the service touches is declared here,
    with one intentional exception: the ``verify_bundle`` verifier override is
    consumed only through ``runtime.__dict__`` as a per-instance escape hatch
    and deliberately stays out of the port.
    """

    ledger: Any
    lock: Any
    rpc: Any
    accepted_block_count: int
    latest_coinbase_size_bytes: int | None
    ledger_writer_public_key_hex: str | None
    max_blocks: int
    reorg_reconciler_enabled: bool
    stop_after_block: bool
    _accounted_accepted_block_hashes: set[str]
    _block_candidate_outcome: threading.local | None
    _block_fast_lane_reservations: set[str]
    _job_cache_lock: Any
    _payout_append_landing_fence_lock: Any
    _payout_balance_mutation_lock: Any
    _payout_ledger_append_invalidation_epoch: int
    _payout_state_generation: int
    _payout_state_source: tuple[int, str | None, str, float]
    _template_artifacts: Any

    def _abandon_block_candidate(
        self,
        reason: str,
        message: str,
        *,
        block_hash: str,
        worker: str | None,
        preserve_if_accepted: bool = False,
        expected_height: int | None = None,
        stale_job_class: str | None = None,
    ) -> bool: ...

    def _accepted_block_payout_preview_from_bundle(
        self,
        final_bundle: dict[str, Any],
        *,
        prior_balances: list[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]: ...

    def _accepted_block_payout_transition_landed(self, block_hash: str) -> bool: ...

    def _audit_publication_identity(
        self,
        *,
        block_hash: str,
        block_height: int,
        confirmation: Mapping[str, Any],
    ) -> AuditPublicationIdentity: ...

    def _await_unfenced_appends_predating_anchor(self, anchor_ms: int) -> None: ...

    def _begin_accepted_block_payout_preview(
        self,
        block_hash: str,
        *,
        block_height: int | None = None,
    ) -> None: ...

    def _block_candidate_acceptance_recorded(self, block_hash: str) -> bool: ...

    def _block_candidate_chain_probe(
        self,
        block_hash: str,
        *,
        expected_height: int | None = None,
    ) -> bool | None: ...

    def _block_candidate_disposition(
        self,
        block_hash: str,
    ) -> AbstractContextManager[Any]: ...

    def _block_candidate_terminal_outcome(self, block_hash: str) -> bool | None: ...

    def _block_payout_state_publication(
        self,
        *,
        force: bool = False,
        supersede_with: tuple[int, str | None, str, float] | None = None,
    ) -> None: ...

    def _block_submitter_lock(
        self,
        lock: Any,
        name: str,
    ) -> AbstractContextManager[None]: ...

    def _cancel_obsolete_job_builds(
        self,
        reason: str,
        *,
        keep_published_snapshot: bool = False,
    ) -> None: ...

    def _capture_payout_state_source(
        self,
    ) -> tuple[int, int, str | None, str, float]: ...

    def _clear_accepted_block_payout_preview(
        self,
        block_hash: str,
        *,
        invalidate_published: bool = False,
    ) -> None: ...

    def _defer_for_pending_parent_payout_transition(
        self,
        *,
        block_hash: str,
        parent_hash: str,
        parent_height: int,
        worker: str | None,
        active_candidate_hash: str | None = None,
        active_candidate_height: int | None = None,
    ) -> bool: ...

    def _ensure_audit_artifact_store(self) -> AuditArtifactStore: ...

    def _ensure_job_cache_state(self) -> None: ...

    def _expose_inflight_scan_anchor(self, anchor_ms: int) -> int: ...

    def _land_and_confirm_block_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        current_tip: str,
        already_active: bool,
        worker: str | None,
        node_submission: _BlockCandidateNodeSubmission,
        revalidated_append_epoch: int | None = None,
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
        AuditPublicationIdentity,
        dict[str, Any],
    ] | None: ...

    def _mark_accepted_block_payout_landed(
        self,
        block_hash: str,
        *,
        block_height: int,
    ) -> None: ...

    def _mark_tip_refresh_pending(self, _observation: object) -> int: ...

    def _materialize_prior_balance_preview(
        self,
        preview: tuple[tuple[str, str, str, int], ...],
    ) -> list[dict[str, object]]: ...

    def _node_submission_for_direct_candidate(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission: ...

    def _note_tip_observation_for_candidates(self, tip_hash: str) -> None: ...

    def _observe_payout_state_seconds(
        self,
        name: str,
        elapsed_seconds: float,
        *,
        relation: str | None = None,
    ) -> None: ...

    def _payout_balance_serializer_released(
        self,
    ) -> AbstractContextManager[None]: ...

    def _payout_source_requires_publication(self, candidate: Any = None) -> bool: ...

    def _payout_state_publication_fenced(self) -> bool: ...

    def _publish_accepted_block_payout_preview(
        self,
        block_hash: str,
        balances: list[dict[str, object]],
    ) -> list[dict[str, object]]: ...

    def _publish_current_payout_state_with_retry_budget(
        self,
        *,
        initial_attempted: bool = False,
    ) -> int | None: ...

    def _record_block_candidate_progress(
        self,
        phase: str = "accounting-progress",
    ) -> None: ...

    def _record_block_submitter_phase(self, phase: str) -> None: ...

    def _record_committed_block_candidate_abandonment(
        self,
        block_hash: str,
        outcome: threading.local,
    ) -> None: ...

    def _register_outstanding_block_candidate(self, block_hash: str) -> None: ...

    def _replayed_payout_window_reproducible(self, context: Any) -> bool: ...

    def _require_fresh_ledger_lease_for_external_side_effect(
        self,
        component: str,
    ) -> None: ...

    def _retire_inflight_scan_anchor(self, token: int | None) -> None: ...

    def _schedule_current_payout_ledger_artifact_if_missing(self) -> None: ...

    def _schedule_payout_ledger_artifact_preparation(
        self,
        payout_state_generation: int,
        network_difficulty: int,
        *,
        min_interval_seconds: float | None = None,
        bypass_build_interval: bool = False,
    ) -> None: ...

    def _schedule_tip_refresh_retry(self) -> None: ...

    def _submit_block_candidate_serialized(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> bool: ...

    def _submit_block_candidate_to_node(
        self,
        candidate: PrismBlockCandidate,
    ) -> _BlockCandidateNodeSubmission: ...

    def _unmark_accepted_block_payout_landed(self, block_hash: str) -> None: ...

    def accepted_share_stats(self) -> tuple[int, int]: ...

    def active_block_candidate_height(self, block_hash: str) -> int | None: ...

    def append_accepted_share(
        self,
        client: Any,
        context: Any,
        submission: direct_stratum.DirectQbitSubmission,
        pending_share: Any,
        *,
        credit_policy: str | None = None,
        candidate_intent: dict[str, Any] | None = None,
    ) -> str | None: ...

    def block_candidate_intent(
        self,
        candidate: PrismBlockCandidate,
    ) -> dict[str, Any]: ...

    def build_audit_bundle(
        self,
        *,
        shares: list[dict[str, object]],
        found_block: dict[str, object],
        prior_balances: list[dict[str, object]],
        coinbase_script_sig_suffix_hex: str,
        witness_merkle_leaves_hex: list[str] | None = None,
        ctv_fee_parent_hash: str | None = None,
        canonical_output_path: Path | None = None,
        canonical_output_parent_fd: int | None = None,
        canonical_output_adopter: Callable[[Path, os.stat_result], None] | None = None,
    ) -> dict[str, Any]: ...

    def coinbase_script_sig_suffix_hex(
        self,
        extranonce1_hex: str,
        extranonce2_hex: str,
    ) -> str: ...

    def ensure_reorg_reconciled_for_tip(
        self,
        tip_hash: str,
        *,
        _coalesce_same_tip: bool = True,
    ) -> bool: ...

    def normalized_prior_balances(
        self,
        balances: list[dict[str, object]],
    ) -> list[dict[str, object]]: ...

    def prior_balances_match_current(
        self,
        prior_balances: list[dict[str, object]],
    ) -> bool: ...

    def reconcile_prism_pool_blocks_once(
        self,
        *,
        tip_hash: str | None = None,
        _force_publish: bool = False,
        _source_reserved: bool = False,
        _wait_for_same_tip_flight: bool = True,
    ) -> dict[str, object]: ...

    def reject_prepared_block(
        self,
        *,
        block_hash: str,
        active_tip_height: int,
    ) -> dict[str, int | str]: ...

    def request_shutdown(self, signum: int | None = None) -> None: ...

    def settlement_balances_by_program(
        self,
        balances: list[dict[str, object]],
    ) -> dict[str, int]: ...

    def trusted_ledger_writer_public_key_hex(
        self,
        bundle: dict[str, Any],
    ) -> str: ...


class BlockFinalizationService:
    """Own accepted-block finalization over explicit infrastructure ports.

    The service deliberately receives the whole coordinator as its runtime:
    the finalization tail touches many coordinator-owned seams that focused
    tests monkeypatch per instance, so every runtime capability is reached
    explicitly through ``self.runtime`` against the typed
    ``BlockFinalizationPort`` while the bounded phase metrics stay
    service-owned.
    """

    runtime: BlockFinalizationPort

    def __init__(self, runtime: BlockFinalizationPort) -> None:
        self.runtime = runtime
        self._metrics_lock = threading.Lock()
        self._phase_metrics = {
            phase: {"count": 0, "sum": 0.0, "max": 0.0}
            for phase in FINALIZATION_PHASES
        }
        self._last_candidate_started: float | None = None
        self._candidate_intervals: dict[str, int | float | None] = {
            "count": 0,
            "sum": 0.0,
            "min": None,
        }

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

    def _record_block_candidate_progress(
        self,
        phase: str = "accounting-progress",
    ) -> None:
        # Service-level compatibility seam: progress marks stay dispatched
        # through the service so focused tests can intercept them with a
        # subclass override; the base implementation forwards to the port.
        self.runtime._record_block_candidate_progress(phase)

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
        node_submission: _BlockCandidateNodeSubmission,
        revalidated_append_epoch: int | None = None,
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
        The caller has already run submitblock on the lock/DB-free fast lane.
        The audit bundle build and verification execute with the serializer
        temporarily released (the landed fence stays armed), so neither block
        announcement nor job delivery waits on audit construction.
        """
        context = candidate.context
        submission = candidate.submission
        expected_height = int(context.template["height"])
        block_hash = str(submission.block_hash_hex).lower()
        parent_hash = str(context.template["previousblockhash"])
        self.runtime._ensure_job_cache_state()
        durable_payout_state = bool(
            getattr(self.runtime.ledger, "durable_payout_state", False)
        )
        with self.runtime._block_submitter_lock(
            self.runtime._payout_balance_mutation_lock,
            "payout-balance-mutation",
        ):
            if self.runtime._defer_for_pending_parent_payout_transition(
                block_hash=block_hash,
                parent_hash=parent_hash,
                parent_height=expected_height - 1,
                worker=worker,
                active_candidate_hash=block_hash if already_active else None,
                active_candidate_height=expected_height if already_active else None,
            ):
                return None
            block_state: dict[str, object] | None = None
            block_state_reader = getattr(self.runtime.ledger, "pool_block_state", None)
            transition_already_landed = self.runtime._accepted_block_payout_transition_landed(
                block_hash
            )
            reorg_reconciled: bool | None = None
            if already_active and not transition_already_landed:
                # A replayed active ancestor may coexist with balances from an
                # orphaned pool block. Reconcile that global state before this
                # transition becomes a landed barrier and before validating its
                # payout base.
                try:
                    reorg_reconciled = self.runtime.ensure_reorg_reconciled_for_tip(
                        current_tip,
                        _coalesce_same_tip=False,
                    )
                except Exception:
                    traceback.print_exc()
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation failed before block replay",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
                if not reorg_reconciled:
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation reported an untrusted chain view",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
            if already_active and callable(block_state_reader):
                self.runtime._record_block_submitter_phase("pool-block-state")
                block_state = block_state_reader(block_hash=block_hash)
                self.runtime._record_block_submitter_phase("pool-block-state:complete")
            already_confirmed = bool(
                block_state is not None
                and str(block_state.get("chain_state", "")) == "confirmed"
                and str(block_state.get("maturity_state", "")) != "reversed"
            )
            if already_confirmed:
                # The outbox terminal update can fail after a fully durable
                # confirmation. Do not replace later global balances with an
                # ancestor-only preview during exact-idempotent replay.
                self.runtime._clear_accepted_block_payout_preview(block_hash)
                reorg_reconciled = True
            elif already_active:
                self.runtime._begin_accepted_block_payout_preview(
                    block_hash,
                    block_height=expected_height,
                )
                self.runtime._mark_accepted_block_payout_landed(
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
                    reorg_reconciled = self.runtime.ensure_reorg_reconciled_for_tip(
                        current_tip,
                        _coalesce_same_tip=False,
                    )
                except Exception:
                    traceback.print_exc()
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "reorg reconciliation failed before block submit",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
            if not reorg_reconciled:
                self.runtime._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "reorg reconciliation reported an untrusted chain view",
                    block_hash=block_hash,
                    worker=worker,
                )
                return None
            if (
                already_active
                and not already_confirmed
                and self.runtime._defer_for_pending_parent_payout_transition(
                    block_hash=block_hash,
                    parent_hash=parent_hash,
                    parent_height=expected_height - 1,
                    worker=worker,
                )
            ):
                return None
            # A late-visible append advances only the append-invalidation
            # epoch: the tip, payout generation, and balance digest all
            # survive, yet this candidate's coinbase pays from a window that
            # omitted the late row. The refresh wave retires the job
            # asynchronously; until it does, membership admission still lets
            # the pre-append job submit, so landing fails closed here.
            # Collection candidates settle solver-pays-all and are exempt, as
            # at every other epoch fence. A negative stamp is a candidate
            # reconstructed from durable intent: the caller revalidated its
            # recorded window against the durable ledger before this landing
            # and rebased it onto the live epoch sequence at that read, so
            # from here both kinds of candidate share one epoch fence. A
            # reconstructed candidate on a backend that cannot revalidate
            # carries no epoch and the fence stands down.
            context_append_epoch = int(
                getattr(context, "payout_append_invalidation_epoch", 0)
            )
            effective_append_epoch = (
                context_append_epoch
                if context_append_epoch >= 0
                else revalidated_append_epoch
            )
            with self.runtime._job_cache_lock:
                live_append_epoch = int(
                    self.runtime._payout_ledger_append_invalidation_epoch
                )
            if (
                not already_active
                and not getattr(context, "collection_only", False)
                and not node_submission.attempted
                and effective_append_epoch is not None
                and effective_append_epoch != live_append_epoch
            ):
                # Fail closed only while nothing has been offered: a
                # fast-lane candidate already reached qbitd, so a moved
                # epoch can no longer withhold its coinbase — the offer's
                # own classification below decides (an error retries
                # duplicate-safe, a rejection stays terminal, and an
                # accepted or duplicate result proceeds to the post-offer
                # fence, which keeps the as-issued snapshot for
                # accounting).
                self.runtime._abandon_block_candidate(
                    PRISM_REJECTION_STALE_JOB,
                    "payout window was invalidated by a late-visible share append",
                    block_hash=block_hash,
                    worker=worker,
                    expected_height=expected_height,
                    stale_job_class="append_epoch_stale",
                )
                return None
            if (
                durable_payout_state
                and not already_active
                and not self.runtime.prior_balances_match_current(context.prior_balances)
            ):
                self.runtime._abandon_block_candidate(
                    PRISM_REJECTION_STALE_JOB,
                    "prior balances changed since the job was issued",
                    block_hash=block_hash,
                    worker=worker,
                    expected_height=expected_height,
                    stale_job_class="balance_stale",
                )
                return None
            if not already_active and not node_submission.attempted:
                before_height = int(self.runtime.rpc.call("getblockcount"))
                if before_height + 1 != expected_height:
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_BLOCK_STALE,
                        f"stale block height: template={expected_height} tip={before_height}",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    return None
            if not already_active:
                # Register before a fallback submitblock can expose this hash as the new
                # tip. Child builders will wait for the verified preview rather
                # than reading balances that omit their new parent.
                self.runtime._begin_accepted_block_payout_preview(
                    block_hash,
                    block_height=expected_height,
                )
                # Treat the submit outcome as uncertain before entering RPC.
                # If transport fails after qbitd accepted the block, this
                # conservative barrier preserves the coinbase's payout base.
                self.runtime._mark_accepted_block_payout_landed(
                    block_hash,
                    block_height=expected_height,
                )
                fallback_submit_under_fence = not node_submission.attempted
                if not node_submission.attempted:

                    def _verify_lease_before_submitblock() -> None:
                        # Runs at the RPC boundary — after any wait on the
                        # landing fence lock below — so the lease runway the
                        # verification proves is still intact when the RPC
                        # starts. A predating append_batch can hold that
                        # lock across its ledger commit and epoch bump far
                        # longer than any scheduling headroom, and a
                        # verification taken before the wait would measure
                        # runway the wait then consumes.
                        try:
                            self.runtime._require_fresh_ledger_lease_for_external_side_effect(
                                "submitblock"
                            )
                        except WriterLeaseRenewalDeferred:
                            if not transition_already_landed:
                                # The refusal fired before submitblock, so
                                # this attempt's outcome is not uncertain:
                                # qbitd provably never saw the block (no
                                # fast-lane node offer either — attempted is
                                # False). Unwind the landed bar this attempt
                                # armed — leaving it would bar reconciliation
                                # and payout-state publication for as long as
                                # the writer's own fenced write keeps the
                                # renewal deferred, though nothing needs
                                # preserving. The begun preview stays and the
                                # retry re-arms both. A bar landed by an
                                # earlier attempt that did reach submitblock
                                # keeps standing for that attempt's uncertain
                                # outcome.
                                self.runtime._unmark_accepted_block_payout_landed(
                                    block_hash
                                )
                            raise

                    # The epoch fence above is advisory: it releases the lock
                    # after one read, so an append-side bump could still commit
                    # between that read and the RPC below. This one is
                    # authoritative -- the bump acquires the same fence lock, so
                    # holding it across submitblock means no late-visible append
                    # can advance the epoch between this comparison and the
                    # block entering qbitd. The lock spans the lease
                    # verification and one RPC, and only on this boundary;
                    # ordinary share commits never touch it (the append side
                    # takes it only for rows that predate a live anchor, and
                    # this landing's own declared anchor stays exposed for
                    # the landing's duration).
                    append_epoch_raced = False
                    if (
                        effective_append_epoch is None
                        or getattr(context, "collection_only", False)
                    ):
                        _verify_lease_before_submitblock()
                        node_submission = self.runtime._submit_block_candidate_to_node(
                            candidate
                        )
                    else:
                        with self.runtime._payout_append_landing_fence_lock:
                            with self.runtime._job_cache_lock:
                                live_append_epoch = int(
                                    self.runtime._payout_ledger_append_invalidation_epoch
                                )
                            if live_append_epoch != effective_append_epoch:
                                append_epoch_raced = True
                            else:
                                _verify_lease_before_submitblock()
                                node_submission = (
                                    self.runtime._submit_block_candidate_to_node(candidate)
                                )
                    if append_epoch_raced:
                        self.runtime._abandon_block_candidate(
                            PRISM_REJECTION_STALE_JOB,
                            "payout window was invalidated by a late-visible share append",
                            block_hash=block_hash,
                            worker=worker,
                            expected_height=expected_height,
                            stale_job_class="append_epoch_stale",
                        )
                        return None
                if node_submission.error is not None:
                    raise node_submission.error
                result = node_submission.result
                if result not in (None, "duplicate"):
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                        f"submitblock rejected candidate: {result}",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    return None
                if (
                    not fallback_submit_under_fence
                    and effective_append_epoch is not None
                    and not getattr(context, "collection_only", False)
                ):
                    # The fast lane offered this block to qbitd without any
                    # ledger synchronization, so the landing fence can no
                    # longer gate submitblock itself. It still orders the
                    # accounting decision: wait out any fenced predating
                    # append whose durable commit is in flight (the commit
                    # holds this lock across its epoch bump). A moved epoch
                    # is no longer terminal here, though: every candidate
                    # reaching this point was accepted or already known by
                    # the node (rejections returned above), so its coinbase
                    # paid the as-issued window the moment it landed and no
                    # rebuild can change it. Abandoning would permanently
                    # discard payout accounting for a live block — the
                    # ledger would then re-pay the same carried balances in
                    # a later block — so the as-issued snapshot is kept for
                    # accounting and the predating share simply rides the
                    # next window (its ledger credit is untouched). The
                    # active-height check below still owns the
                    # did-it-actually-land verdict, and the pre-offer fence
                    # above still fails closed while nothing has been
                    # submitted.
                    with self.runtime._payout_append_landing_fence_lock:
                        with self.runtime._job_cache_lock:
                            live_append_epoch = int(
                                self.runtime._payout_ledger_append_invalidation_epoch
                            )
                    if live_append_epoch != effective_append_epoch:
                        print(
                            "prism coordinator: payout window was "
                            "invalidated by a late-visible share append "
                            f"after the node offer hash={block_hash}; "
                            "keeping the as-issued payout snapshot for "
                            "accounting",
                            flush=True,
                        )
                active_hash = str(
                    self.runtime.rpc.call("getblockhash", [expected_height])
                ).lower()
                if active_hash != block_hash:
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                        f"submitted block is not active at height {expected_height}",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    return None
                self.runtime._cancel_obsolete_job_builds("direct PRISM block accepted")
                self.runtime._mark_tip_refresh_pending(block_hash)
                self.runtime._schedule_tip_refresh_retry()

            preview: list[dict[str, object]] | None = None
            issued_preview = getattr(context, "prospective_prior_balances", None)
            if not already_confirmed and issued_preview is not None:
                # The compact preview came from the immutable issued job
                # summary. Publish it before rebuilding/canonicalizing the full
                # audit bundle, without retaining that bundle's shares tree.
                preview = self.runtime._materialize_prior_balance_preview(issued_preview)
                if durable_payout_state and not self.runtime.prior_balances_match_current(
                    context.prior_balances
                ):
                    self.runtime.request_shutdown()
                    self.runtime._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                        "accepted block payout base changed before preview publication",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
                self.runtime._publish_accepted_block_payout_preview(block_hash, preview)

            self.runtime._record_block_submitter_phase("audit-build")
            # The bundle derives only from inputs frozen on the candidate
            # (share window, prior balances, extranonces, template fields),
            # so the serializer is released around the builder/verifier
            # subprocess work: submitblock above has already run for fresh
            # candidates -- announcement is never delayed by audit
            # construction -- and the landed fence keeps reconciliation out
            # while job delivery proceeds. Child builds consume the compact
            # preview published above until the verified preview lands.
            with self.runtime._payout_balance_serializer_released():
                audit_store = self.runtime._ensure_audit_artifact_store()
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
                    final_bundle = self.runtime.build_audit_bundle(
                        shares=context.shares_json,
                        found_block=context.found_block,
                        prior_balances=context.prior_balances,
                        coinbase_script_sig_suffix_hex=self.runtime.coinbase_script_sig_suffix_hex(
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
                # Compatibility builders used by tests and older integrations
                # may ignore canonical_output_path. Persist their logical
                # bundle via the normal canonicalization fallback without
                # mislabeling bytes.
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
                    self.runtime.request_shutdown()
                    self.runtime._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                        "final audit bundle coinbase does not match submitted coinbase",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
            payout_commit_started: float | None = None
            payout_commit_source: int | None = None
            try:
                with self.runtime._payout_balance_serializer_released():
                    self.runtime._record_block_submitter_phase("audit-verify")
                    # Intentional escape hatch outside the typed port: only a
                    # per-instance override placed in the runtime's __dict__
                    # replaces the verifier, never a class-level attribute.
                    verifier_override = self.runtime.__dict__.get("verify_bundle")
                    configured_writer_key = getattr(
                        self.runtime,
                        "ledger_writer_public_key_hex",
                        None,
                    )
                    verified_audit = audit_store.verify_candidate(
                        candidate_artifact,
                        coinbase_tx_hex=submission.coinbase_tx_hex,
                        expected_coinbase_value_sats=int(
                            context.template["coinbasevalue"]
                        ),
                        expected_block_height=expected_height,
                        trusted_writer_public_key_hex=(
                            self.runtime.trusted_ledger_writer_public_key_hex(final_bundle)
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
                    verified_preview = (
                        self.runtime._accepted_block_payout_preview_from_bundle(
                            final_bundle,
                            prior_balances=context.prior_balances,
                        )
                    )
                self.runtime._record_block_submitter_phase("audit-verify:complete")
                if not already_confirmed:
                    if preview is None and durable_payout_state:
                        live_prior_balances = self.runtime.settlement_balances_by_program(
                            self.runtime.ledger.current_prior_balances()
                        )
                        expected_prior_balances = self.runtime.settlement_balances_by_program(
                            context.prior_balances
                        )
                        if live_prior_balances != expected_prior_balances:
                            self.runtime.request_shutdown()
                            self.runtime._clear_accepted_block_payout_preview(
                                block_hash,
                                invalidate_published=True,
                            )
                            self.runtime._abandon_block_candidate(
                                PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                                "accepted block payout base changed before preview publication",
                                block_hash=block_hash,
                                worker=worker,
                            )
                            return None
                    try:
                        self.runtime._publish_accepted_block_payout_preview(
                            block_hash,
                            verified_preview,
                        )
                    except RuntimeError as exc:
                        self.runtime.request_shutdown()
                        self.runtime._clear_accepted_block_payout_preview(
                            block_hash,
                            invalidate_published=True,
                        )
                        self.runtime._abandon_block_candidate(
                            PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
                            "verified final payout preview does not match the "
                            f"issued block job: {exc}",
                            block_hash=block_hash,
                            worker=worker,
                        )
                        return None
                preview = verified_preview

                # The verified preview is now the effective balance snapshot,
                # so persistence can do canonicalization, body writes, copies,
                # and bulk SQL without owning the delivery gate.
                payout_commit_started = time.monotonic()
                payout_commit_source = self.runtime._capture_payout_state_source()[1]
                self.runtime._record_block_submitter_phase("persist-accepted-block")
                persistence = self.runtime.ledger.persist_accepted_block(
                    block_hash=submission.block_hash_hex,
                    block_height=expected_height,
                    parent_hash=parent_hash,
                    final_bundle=final_bundle,
                    audit_report=report,
                    canonical_bundle_path=persistence_canonical_bundle_path,
                )
                self.runtime._record_block_submitter_phase("persist-accepted-block:complete")
                active_hash = str(
                    self.runtime.rpc.call("getblockhash", [expected_height])
                ).lower()
                if active_hash != block_hash:
                    if already_confirmed:
                        self.runtime._abandon_block_candidate(
                            PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                            "accepted ancestor left the active chain during replay",
                            block_hash=block_hash,
                            worker=worker,
                        )
                        return None
                    # Seal the disposition BEFORE touching the prepared
                    # payout rows: a rejected row can never be promoted by a
                    # later confirmation (and reactivation only covers
                    # inactive rows), so rejection must follow -- never
                    # precede -- a terminal decision. The abandon defers on
                    # live acceptance evidence; its terminal commit consults
                    # that evidence atomically with the commit AND seals the
                    # hash against further observation matching, so no
                    # evidence can register anywhere in the gap between the
                    # sealed decision and this rejection. A crash in between
                    # leaves the outbox row pending; restart replay
                    # re-registers the hash and re-evaluates from live chain
                    # state before rejecting.
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_BLOCK_STALE,
                        "accepted block left the active chain before ledger confirmation",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                    )
                    outcome = getattr(
                        self.runtime, "_block_candidate_outcome", None
                    )
                    sealed_reason = (
                        getattr(outcome, "reason", None)
                        if outcome is not None
                        else None
                    )
                    if sealed_reason == PRISM_REJECTION_BLOCK_STALE:
                        active_tip_height = int(self.runtime.rpc.call("getblockcount"))
                        self.runtime.reject_prepared_block(
                            block_hash=block_hash,
                            active_tip_height=active_tip_height,
                        )
                    return None
                self.runtime._record_block_submitter_phase("confirm-accepted-block")
                with audit_store.publication_order_guard():
                    confirmation = self.runtime.ledger.confirm_accepted_block(
                        block_hash=block_hash,
                        # The ledger confirmation function matches this value
                        # against the candidate row's own height. An accepted
                        # ancestor can be finalized after newer blocks arrive.
                        active_tip_height=expected_height,
                    )
                    confirmed_count = int(confirmation.get("confirmed_count", 0))
                    if confirmed_count == 1:
                        audit_publication_identity = (
                            self.runtime._audit_publication_identity(
                                block_hash=block_hash,
                                block_height=expected_height,
                                confirmation=confirmation,
                            )
                        )
                self.runtime._record_block_submitter_phase(
                    "confirm-accepted-block:complete"
                )
                if confirmed_count != 1:
                    # -1 is the ledger's superseded disposition: the row was
                    # terminally disposed (reorg quarantine, rejection, or
                    # reversal) before this confirmation landed. That is
                    # terminal for the candidate but benign for the pool, so
                    # the coordinator keeps serving; the shutdown escalation
                    # is reserved for a genuinely unexplained failure.
                    superseded = confirmed_count == -1
                    if not superseded:
                        self.runtime.request_shutdown()
                    self.runtime._clear_accepted_block_payout_preview(
                        block_hash,
                        invalidate_published=True,
                    )
                    if superseded:
                        self.runtime._abandon_block_candidate(
                            PRISM_REJECTION_LEDGER_CONFIRMATION_SUPERSEDED,
                            "ledger row for accepted block "
                            f"{block_hash} was superseded before confirmation",
                            block_hash=block_hash,
                            worker=worker,
                        )
                    else:
                        self.runtime._abandon_block_candidate(
                            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                            f"ledger did not confirm accepted block {block_hash}",
                            block_hash=block_hash,
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
                        self.runtime.ledger,
                        "prior_balances_after_pool_block",
                        None,
                    )
                    confirmed_balances = self.runtime.normalized_prior_balances(
                        as_of_reader(block_hash=block_hash)
                        if callable(as_of_reader)
                        else self.runtime.ledger.current_prior_balances()
                    )
                    if self.runtime.settlement_balances_by_program(
                        confirmed_balances
                    ) != self.runtime.settlement_balances_by_program(preview):
                        self.runtime.request_shutdown()
                        self.runtime._clear_accepted_block_payout_preview(
                            block_hash,
                            invalidate_published=True,
                        )
                        self.runtime._abandon_block_candidate(
                            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
                            "confirmed payout balances do not match the published "
                            f"preview for accepted block {block_hash}",
                            block_hash=block_hash,
                            worker=worker,
                        )
                        return None
                # Durability caught up to the already-published logical state;
                # clearing the parent override needs no second generation bump.
                self.runtime._clear_accepted_block_payout_preview(block_hash)
                self.runtime._schedule_current_payout_ledger_artifact_if_missing()
                payout_publication_required = (
                    self.runtime._payout_source_requires_publication()
                )
                payout_publication_fenced = (
                    self.runtime._payout_state_publication_fenced()
                )
                if payout_publication_required or payout_publication_fenced:
                    # A covered replay normally has no publication work. The
                    # exception is a leaked delivery fence whose source already
                    # published: force one republish so the replay heals it.
                    covered_replay_fence = (
                        payout_publication_fenced
                        and not payout_publication_required
                    )
                    with self.runtime.lock:
                        pending_cause = self.runtime._payout_state_source[2]
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
                        self.runtime,
                        "reorg_reconciler_enabled",
                        True,
                    ):
                        with self.runtime.lock:
                            latest_tip = self.runtime._payout_state_source[1]
                        summary = self.runtime.reconcile_prism_pool_blocks_once(
                            tip_hash=latest_tip,
                            _force_publish=True,
                            _source_reserved=True,
                        )
                        reconciled_generation = summary.get("published_generation")
                        if isinstance(reconciled_generation, int):
                            published = reconciled_generation
                    elif publish_now:
                        published = (
                            self.runtime._publish_current_payout_state_with_retry_budget()
                        )
                    if publish_now and published is None:
                        # The block is durably confirmed; only the payout
                        # publication lost its race. Aborting would keep the
                        # outbox row pending and replay persist/confirm churn
                        # for an already-final block. Keep delivery fenced and
                        # let the scheduled tip refresh publish the newest
                        # source; this candidate's durable work is complete.
                        self.runtime._block_payout_state_publication()
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
                    self.runtime._block_payout_state_publication(
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
                    self.runtime._observe_payout_state_seconds(
                        "preparation",
                        max(0.0, time.monotonic() - payout_commit_started),
                    )
                audit_store.discard_candidate(candidate_artifact)

    def _admit_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> FinalizationAdmission | bool | None:
        """Classify one post-offer candidate against pool and chain state.

        Returns an admission for the ordered phases, a terminal bool for an
        outcome the disposition can finalize directly (an already-recorded
        acceptance or an accepted race win), or ``None`` for a terminal
        abandonment already recorded through the outcome seam.
        """
        outcome = getattr(self.runtime, "_block_candidate_outcome", None)
        if outcome is None:
            outcome = threading.local()
            self.runtime._block_candidate_outcome = outcome
        outcome.reason = None
        outcome.error = None
        outcome.stale_job_class = None
        context = candidate.context
        submission = candidate.submission
        worker = candidate.client.username or None
        expected_height = int(context.template["height"])
        block_hash = str(submission.block_hash_hex).lower()
        parent_hash = str(context.template["previousblockhash"])
        self.runtime._ensure_job_cache_state()
        # Every disposition (queue drain, synchronous below-target submit,
        # outbox replay, retained retry) marks its hash outstanding so tip
        # observations arriving on other threads can register acceptance.
        self.runtime._register_outstanding_block_candidate(block_hash)
        self._record_block_candidate_progress("disposition-start")
        if (
            self.runtime._block_candidate_acceptance_recorded(block_hash)
            and node_submission.error is not None
        ):
            # A concurrent same-hash pass completed the success tail while
            # this duplicate-safe node offer waited for disposition. Do not
            # recreate its payout transition or accounting work.
            self.runtime._clear_accepted_block_payout_preview(block_hash)
            return True
        with self.runtime.lock:
            accepted_count = int(self.runtime.accepted_block_count)
            pool_closed = (
                block_hash not in self.runtime._accounted_accepted_block_hashes
                and (
                    accepted_count >= int(self.runtime.max_blocks)
                    or (bool(self.runtime.stop_after_block) and accepted_count >= 1)
                )
            )
        if pool_closed and self.runtime._block_candidate_chain_probe(
            block_hash,
            expected_height=expected_height,
        ) is True:
            # The chain provably contains this block; its payout accounting
            # must complete regardless of when the pool stopped accepting
            # new work. Fall through to the normal disposition below, which
            # resumes the accepted success tail. Observation evidence alone
            # deliberately does not open this gate -- an unprovable view
            # defers via the abandon path instead, so a closed pool can
            # never fall through to submitblock on stale evidence.
            pool_closed = False
        if pool_closed:
            self.runtime._abandon_block_candidate(
                PRISM_REJECTION_POOL_CLOSED,
                "pool is no longer accepting blocks",
                block_hash=block_hash,
                worker=worker,
                expected_height=expected_height,
            )
            return None
        self._record_block_candidate_progress("current-tip-rpc")
        observed_tip = str(self.runtime.rpc.call("getbestblockhash"))
        self._record_block_candidate_progress("current-tip-rpc:complete")
        # A successful or transport-ambiguous fast-lane call can change the
        # tip before this post-submit probe. It is still a *fresh* attempt,
        # not an active replay: run the normal validation/persistence tail
        # against the candidate's stamped parent. A later getblockhash check
        # proves a successful acknowledgement, while an ambiguous transport
        # outcome stays pending for duplicate-safe replay. Duplicate replies
        # are replay evidence and retain the live-tip classification; the
        # abandonment tie-breaker separately consults this process's own
        # recorded offer evidence so an unprovable chain view cannot
        # terminally discard a block the node already told us it has.
        fresh_or_uncertain_submit = bool(
            node_submission.attempted
            and (
                node_submission.error is not None
                or node_submission.result is None
            )
        )
        current_tip = parent_hash if fresh_or_uncertain_submit else observed_tip
        landed_height: int | None = None
        if current_tip.lower() == block_hash:
            landed_height = expected_height
        elif current_tip != parent_hash:
            try:
                landed_height = self.runtime.active_block_candidate_height(block_hash)
            except Exception:
                traceback.print_exc()
                self.runtime._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "could not determine whether a prior candidate is active",
                    block_hash=block_hash,
                    worker=worker,
                )
                return None
        already_active = landed_height == expected_height
        if landed_height is not None and not already_active:
            self.runtime._abandon_block_candidate(
                PRISM_REJECTION_BLOCK_STALE,
                f"candidate active at unexpected height {landed_height}",
                block_hash=block_hash,
                worker=worker,
                expected_height=expected_height,
            )
            return None
        if already_active:
            # A disposition probe is a tip observation too: remember it so a
            # later attempt cannot terminally abandon this hash on a racing
            # chain snapshot after this attempt fails mid-tail.
            self.runtime._note_tip_observation_for_candidates(block_hash)
            print(
                "prism coordinator: resuming finalization for active block candidate "
                f"height={landed_height} hash={submission.block_hash_hex}",
                flush=True,
            )
        elif parent_hash != current_tip:
            if self.runtime._block_candidate_acceptance_recorded(block_hash):
                # A duplicate wakeup can reach this check after the accepted
                # success tail but after a newer tip hides the candidate from
                # the active-header probe. Its durable work is already done;
                # let the caller finalize the outbox as submitted.
                self.runtime._clear_accepted_block_payout_preview(block_hash)
                return True
            accepted_race_won = self.runtime._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                f"tip moved before submit: {current_tip}",
                block_hash=block_hash,
                worker=worker,
                preserve_if_accepted=True,
                expected_height=expected_height,
                stale_job_class="tip_moved",
            )
            return bool(accepted_race_won)
        if (
            node_submission.attempted
            and node_submission.error is None
            and node_submission.result not in (None, "duplicate")
            and not already_active
        ):
            self.runtime._abandon_block_candidate(
                PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                f"submitblock rejected candidate: {node_submission.result}",
                block_hash=block_hash,
                worker=worker,
                expected_height=expected_height,
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
            # Fresh-attempt classification may use the stamped parent, but
            # payout reconciliation must follow the observed post-submit tip.
            current_tip=observed_tip,
            already_active=already_active,
            node_submission=node_submission,
        )

    def _land_candidate(
        self,
        admission: FinalizationAdmission,
    ) -> LandedCandidate | None:
        """Decide replay validity, fence the anchor, and land the candidate."""
        candidate = admission.candidate
        context = admission.context
        worker = admission.worker
        expected_height = admission.expected_height
        block_hash = admission.block_hash
        node_submission = admission.node_submission
        # A reconstructed candidate revalidates BEFORE the balance
        # serializer: the audit share-window replay is the slow oracle walk
        # and takes the ledger writer lock, so running it inside
        # _payout_balance_mutation_lock would stall share persistence and
        # every other landing for the walk's duration. The read is safe out
        # here because the window is append-only content -- a pass can only
        # be invalidated by a later append, and any such append after the
        # baseline epoch captured below advances the live epoch (this
        # landing's declared anchor stays exposed to the append-side
        # predates() checks), which the landing's fences compare against.
        collection_only = bool(getattr(context, "collection_only", False))
        context_append_epoch = int(
            getattr(context, "payout_append_invalidation_epoch", 0)
        )
        revalidated_append_epoch: int | None = None
        landing_anchor_token: int | None = None
        if not admission.already_active and not collection_only:
            found_block = getattr(context, "found_block", None)
            declared_anchor_ms = (
                found_block.get("anchor_job_issued_at_ms")
                if isinstance(found_block, dict)
                else None
            )
            if declared_anchor_ms is not None:
                # With no armed artifact and no in-flight walk (an outbox
                # replay at startup, or a landing after the artifact was
                # disarmed), a replay-shaped append would skip the epoch
                # bump entirely; exposing the landing window's own anchor
                # guarantees the bump the fences below check for.
                landing_anchor_token = self.runtime._expose_inflight_scan_anchor(
                    int(declared_anchor_ms)
                )
                # An append classified as unfenced before that exposure
                # commits outside the landing fence, so its row could
                # become durable mid-submitblock while its epoch bump
                # queues behind the fence this landing holds across the
                # RPC. Wait those commits (and their bump attempts
                # against the now-exposed anchor) out before any window
                # revalidation or epoch fence runs; no fence is held
                # here, so the bump can always proceed.
                self.runtime._await_unfenced_appends_predating_anchor(
                    int(declared_anchor_ms)
                )
        try:
            reconstructed_needs_revalidation = (
                not admission.already_active
                and not collection_only
                and context_append_epoch < 0
                and bool(getattr(self.runtime.ledger, "durable_payout_state", False))
            )
            if reconstructed_needs_revalidation and node_submission.attempted:
                # Revalidation guards an offer the node has not yet seen: a
                # reconstructed window that omits a durably appended share
                # must not mint a coinbase. Once the fast lane has offered
                # the durable bytes, the coinbase is the node's to judge —
                # an accepted or duplicate result proceeds with the
                # as-issued snapshot (the post-offer epoch fences log
                # rather than abandon), a rejection stayed terminal above,
                # and an ambiguous transport error re-offers duplicate-
                # safely on retry. Abandoning an already-offered candidate
                # here would permanently discard payout accounting for a
                # block qbitd may have accepted, and the audit walk is the
                # slow oracle whose deadline under saturation would
                # otherwise defer-loop an accepted block forever.
                print(
                    "prism coordinator: reconstructed candidate was already "
                    f"offered to the node hash={block_hash}; keeping the "
                    "as-issued payout snapshot (window revalidation "
                    "skipped)",
                    flush=True,
                )
            if (
                reconstructed_needs_revalidation
                and not node_submission.attempted
            ):
                with self.runtime._job_cache_lock:
                    revalidation_base_epoch = int(
                        self.runtime._payout_ledger_append_invalidation_epoch
                    )
                try:
                    window_reproducible = (
                        self.runtime._replayed_payout_window_reproducible(context)
                    )
                except Exception:
                    traceback.print_exc()
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                        "durable share-window replay failed for the "
                        "reconstructed candidate",
                        block_hash=block_hash,
                        worker=worker,
                    )
                    return None
                if not window_reproducible:
                    self.runtime._abandon_block_candidate(
                        PRISM_REJECTION_STALE_JOB,
                        "replayed payout window omits a durably appended share",
                        block_hash=block_hash,
                        worker=worker,
                        expected_height=expected_height,
                        stale_job_class="append_epoch_stale",
                    )
                    return None
                revalidated_append_epoch = revalidation_base_epoch
            # Land through the coordinator/runtime seam so per-instance
            # monkeypatches on _land_and_confirm_block_candidate intercept.
            landed = self.runtime._land_and_confirm_block_candidate(
                candidate,
                current_tip=admission.current_tip,
                already_active=admission.already_active,
                worker=worker,
                node_submission=node_submission,
                revalidated_append_epoch=revalidated_append_epoch,
            )
        finally:
            self.runtime._retire_inflight_scan_anchor(landing_anchor_token)
        if landed is None:
            return None
        return LandedCandidate(*landed)

    def _candidate_already_accounted(self, block_hash: str) -> bool:
        with self.runtime.lock:
            return block_hash in self.runtime._accounted_accepted_block_hashes

    def _persist_ctv_and_credit(
        self,
        admission: FinalizationAdmission,
        landed: LandedCandidate,
    ) -> dict[str, Any] | None:
        candidate = admission.candidate
        block_hash = admission.block_hash
        ctv_persistence = None
        ctv_manifest_set = landed.final_bundle.get("ctv_fanout_manifest_set")
        if isinstance(ctv_manifest_set, dict):
            self._record_block_candidate_progress("ctv-manifest-persist")
            ctv_persistence = self.runtime.ledger.persist_ctv_fanout_manifest_set(
                block_hash=block_hash,
                manifest_set=ctv_manifest_set,
                manifest_set_sha256=sha256_json_hex(ctv_manifest_set),
            )
            self._record_block_candidate_progress("ctv-manifest-persist:complete")
        if candidate.credit_share_on_accept:
            self._record_block_candidate_progress("accepted-share-credit")
            self.runtime.append_accepted_share(
                candidate.client,
                admission.context,
                admission.submission,
                candidate.pending_share,
                candidate_intent=self.runtime.block_candidate_intent(candidate),
            )
            # The preview window was intentionally clamped below this pending
            # winning share. Once the append is durable, enqueue an urgent
            # delta fold so a rapidly found child does not wait for the normal
            # 60-second cadence to include its payout obligation.
            with self.runtime._job_cache_lock:
                payout_generation = self.runtime._payout_state_generation
                template_artifacts = self.runtime._template_artifacts
            if template_artifacts is not None:
                self.runtime._schedule_payout_ledger_artifact_preparation(
                    payout_generation,
                    template_artifacts.network_difficulty,
                    bypass_build_interval=True,
                )
            self._record_block_candidate_progress("accepted-share-credit:complete")
        return ctv_persistence

    def _build_finalization_evidence(
        self,
        admission: FinalizationAdmission,
        landed: LandedCandidate,
        ctv_persistence: dict[str, Any] | None,
    ) -> FinalizationEvidence:
        # Aggregate counts only: materializing the whole share history
        # (all_shares) here would scan the full ledger twice per block,
        # and would grow without bound as the ledger grows. The counters are
        # served from the ledger's maintained cache; a cold cache (first
        # read after process start) runs the exact aggregate synchronously
        # and stays watchdog-eligible on purpose, so a wedged read keeps the
        # exit-and-replay recovery path instead of hanging the disposition
        # invisibly.
        self._record_block_candidate_progress("accepted-share-stats")
        evidence_share_count, evidence_distinct_miners = self.runtime.accepted_share_stats()
        self._record_block_candidate_progress("accepted-share-stats:complete")
        evidence = {
            "schema": "qbit.prism.live-stratum-evidence.v1",
            "block_hash": admission.block_hash,
            "block_height": admission.expected_height,
            "coinbase_tx_hex": admission.submission.coinbase_tx_hex,
            "audit_report": landed.report,
            "ledger_backend": self.runtime.ledger.backend_name,
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
        audit_store = self.runtime._ensure_audit_artifact_store()
        self._record_block_candidate_progress("evidence-write")
        with self.runtime._payout_balance_mutation_lock:
            with audit_store.publication_order_guard():
                publication_floor_reader = getattr(
                    self.runtime.ledger,
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
        published_evidence = dict(publication.evidence)
        self._record_block_candidate_progress("evidence-write:complete")
        return published_evidence

    def _account_finalized_candidate(
        self,
        admission: FinalizationAdmission,
        landed: LandedCandidate,
        published_evidence: dict[str, Any],
    ) -> bool:
        # The copied publication is intentionally consumed before accounting;
        # converting an invalid publication remains a finalization failure.
        del published_evidence
        block_hash = admission.block_hash
        with self.runtime.lock:
            newly_accounted = block_hash not in self.runtime._accounted_accepted_block_hashes
            if newly_accounted:
                self.runtime._accounted_accepted_block_hashes.add(block_hash)
                self.runtime.accepted_block_count += 1
                # Replace this hash's provisional capacity reservation with
                # its durable accounted slot atomically. Keeping both until
                # the outbox terminal write would double-count the block and
                # unnecessarily reject an unrelated next solve.
                self.runtime._block_fast_lane_reservations.discard(block_hash)
            self.runtime.latest_coinbase_size_bytes = len(
                str(
                    landed.final_bundle["signed_coinbase_manifest"]["manifest"][
                        "coinbase_tx_hex"
                    ]
                )
            ) // 2
            should_stop = (
                newly_accounted
                and (self.runtime.stop_after_block or self.runtime.accepted_block_count >= self.runtime.max_blocks)
            )
        if not newly_accounted:
            return True
        print(
            "prism coordinator: qbit accepted direct PRISM block "
            f"height={admission.expected_height} hash={block_hash}",
            flush=True,
        )
        if should_stop:
            self.runtime.request_shutdown()
        else:
            # The public submitter wrapper performs this fanout only after its
            # writer scope (including outbox finalization) exits. The rare
            # synchronous share path consumes the same marker after sending
            # the Stratum result.
            admission.candidate.client.post_accept_refresh_block = (
                admission.expected_height,
                block_hash,
            )
        return True

    def _submit_block_candidate_serialized(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission,
    ) -> bool:
        """Process a candidate while its same-hash disposition guard is held.

        Runs the ordered, replay-safe post-offer finalization phases. B1's
        accounting actor enters here directly with the disposition already
        held; the decorated public wrapper reaches the same runner through
        the coordinator seam.
        """
        self._note_candidate_started()
        with self._phase("admission"):
            admission = self._admit_candidate(
                candidate,
                node_submission=node_submission,
            )
        if admission is None:
            return False
        if isinstance(admission, bool):
            return admission
        with self._phase("land_confirm"):
            landed = self._land_candidate(admission)
        if landed is None:
            return False
        self._record_block_candidate_progress("durable-accounting:complete")
        if self._candidate_already_accounted(admission.block_hash):
            # The previous attempt completed every success side effect but its
            # durable outbox terminal update failed. submit_next will retry that
            # update after this exact-idempotent confirmation without double
            # counting the block or replacing newer evidence/work.
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

    def submit_block_candidate(
        self,
        candidate: PrismBlockCandidate,
        *,
        node_submission: _BlockCandidateNodeSubmission | None = None,
    ) -> bool:
        """Land one block candidate, then finalize its audit and payout state.

        Runs on the block-submitter thread (tests call it synchronously). It
        never raises for a lost race and holds self.runtime.lock only for short
        in-memory state mutation -- never across RPC, psql, subprocess, or
        file I/O -- so share acks and job pushes stay fast while a block
        lands. The durable candidate outbox is the pre-submit recovery boundary;
        full audit and payout persistence happens after the latency-sensitive
        ``submitblock`` call and is replayable after a crash. Returns True only
        after that finalization completes.
        """
        block_hash = str(candidate.submission.block_hash_hex).lower()
        with self.runtime._block_candidate_disposition(block_hash):
            terminal_outcome = self.runtime._block_candidate_terminal_outcome(block_hash)
            if terminal_outcome is not None:
                return terminal_outcome
            if node_submission is None:
                node_submission = self.runtime._node_submission_for_direct_candidate(candidate)
            accepted = self.runtime._submit_block_candidate_serialized(
                candidate,
                node_submission=node_submission,
            )
            if not accepted:
                outcome = getattr(self.runtime, "_block_candidate_outcome", None)
                if outcome is not None:
                    # Direct embedders do not use the outbox-finalization
                    # wrapper. A normal return means the serialized path also
                    # completed any prepared-state rejection it initiated.
                    self.runtime._record_committed_block_candidate_abandonment(
                        block_hash,
                        outcome,
                    )
            return accepted
