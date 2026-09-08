"""Shared telemetry contract for accepted-preview latency attribution.

Why this module exists
----------------------
Issue #224: ``PrismAcceptedPreviewPublicationLatencyHigh`` fired on Union
mainnet while the coordinator stayed healthy. The publication histogram
(``qbit_prism_accepted_block_preview_publication_seconds``) proves that a
tail exists between definitive node acceptance and the visible payout
preview, but nothing on ``/metrics`` says *which stretch of the landing*
owns a sample above the 4 s warning or the 5 s child wait budget. Logs
around the alert showed reconcile-driven full payout-window rescans and
publication-blocked retries, which is correlated evidence, not
attribution.

Three implementation lanes fix that in parallel: the core landing path,
landing attribution, and ledger attribution. They must agree on metric
names, label vocabularies, snapshot shapes, and neutral record types
before any of them instruments a call site, or the families they export
drift and the operator gets three partial answers. This module is that
agreement. It is deliberately behavior-neutral: nothing here is called on
a hot path yet, nothing here changes payout, reconciliation,
finalization, candidate, ledger, queue, lock, timeout, publication, or
retry behavior, and the renderer exports every family from an empty
owner so the series exist from process start.

Contract at a glance
--------------------
Every vocabulary below is a closed tuple. A metric label may only ever be
one of its members: the recorder raises on an unknown programmer-chosen
label (phase, caller, step, path) exactly as
:meth:`ReorgReconcilerService.record_lookup` does, and *normalizes* the two
values that arrive as runtime strings (a payout-window rescan reason, a
ledger read operation name) onto a fixed ``other`` member rather than
growing a series. Block hashes, heights, miner IDs, exception text and
caller-supplied operation strings never become labels; the per-publication
diagnostic record is where a hash or height may appear, bounded by a
small ring.

``PRISM_ACCEPTED_LANDING_PHASES``
    Sub-phases of one accepted-block landing, in the order the landing
    runs them (``BlockFinalizationService._land_and_confirm_block_candidate``
    and the accounting lane around it):

    ``lane_wait``
        definitive node acceptance stamped on the submitter thread
        (``_note_accepted_block_preview_acceptance``) to the accounting
        lane starting the landing task.
    ``balance_lock_wait``
        waiting for ``_payout_balance_mutation_lock`` under
        ``_block_submitter_lock(..., "payout-balance-mutation")``.
    ``reconcile``
        the inline ``ensure_reorg_reconciled_for_tip`` stretch, the
        ``reorg-reconcile`` heartbeat phase.
    ``prior_balances_check``
        ``_stamped_prior_balances_match_current`` and the live
        ``current_prior_balances`` comparison before the verified preview.
        This is a prior-balances *reread*; it is never a payout-window
        rescan (see the rescan family below).
    ``chain_probe``
        the active-chain reads: ``pool-block-state``, ``tip-height-rpc``
        and ``_block_candidate_chain_probe``.
    ``preview_prepare``
        materializing the compact preview from the issued job summary or
        deriving the verified preview from the audit bundle.
    ``preview_publish``
        ``_publish_accepted_block_payout_preview`` itself, both the
        generation publication and the degraded fenced branch.

    Family: ``qbit_prism_accepted_block_landing_phase_seconds{phase}``
    as ``_sum`` / ``_count`` / ``_max``, the same shape as
    ``qbit_prism_block_finalization_phase_seconds``.

``PRISM_REORG_RECONCILE_CALLERS`` / ``PRISM_REORG_RECONCILE_STEPS``
    Which path asked for a reconcile pass and where the pass spent its
    wall-clock. Callers: ``landing`` (the in-lock
    ``ensure_reorg_reconciled_for_tip(..., _coalesce_same_tip=False)``),
    ``post_confirm`` (the forced-publish
    ``reconcile_prism_pool_blocks_once(_force_publish=True)`` after a
    durable confirmation), ``tip_refresh``, ``job_build`` (spelled exactly
    as ``PRISM_REORG_RECONCILE_LOOKUP_PATHS`` spells them, so the two
    families join), and ``other`` for tools, benchmarks and periodic
    passes. Steps follow ``ReorgReconcilerService.reconcile``:
    ``admission_wait`` (the serialized adapter's writer admission plus the
    payout-balance mutation lock, added because that wait is the one
    stretch a tip-refresh caller spends behind a landing and it is not a
    step of the pass itself), ``watch_query`` (``reorg_watch_blocks``),
    ``chain_probe`` (``getblockcount`` / ``getblockhash`` per watched or
    stranded row), ``mutations`` (``mark_pool_block_inactive``,
    ``reactivate_pool_block``, ``reject_prepared_block``),
    ``candidate_prepare`` (``prepared_candidate``, which is where a
    ``reconcile_invalidation`` full rescan actually executes) and
    ``publish`` (``publish_candidate``).

    Families: ``qbit_prism_reorg_reconcile_pass_seconds{caller}`` and
    ``qbit_prism_reorg_reconcile_step_seconds{caller,step}``, each as
    ``_sum`` / ``_count`` / ``_max``. The pass count by caller is the
    pass family's ``_count``; pass minus the sum of its steps is the
    unattributed remainder.

``PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS`` / ``..._PATHS``
    Full payout-window oracle rescans by the reason that forced them and
    the window pipeline that executed the fold. The reason vocabulary is
    the set of literals ``lab.prism.payout_state`` hands to
    ``_full_payout_window_materialization(reason=...)`` or records as the
    periodic self-check reason; it lives here only because that module
    keeps them as scattered literals rather than a tuple, and
    ``tests.test_prism_accepted_preview_telemetry`` parses that module to
    fail the moment a literal appears that this tuple does not carry.
    ``normalize_payout_window_full_rescan_reason`` maps anything else to
    ``other``. Paths: ``daemon`` (the Rust ``--serve`` window pipeline) and
    ``in_process`` (the Python fold, including the legacy whole-snapshot
    read for ledgers without the incremental record contract).

    Family: ``qbit_prism_payout_window_full_rescan_seconds{reason,path}``
    as ``_sum`` / ``_count`` / ``_max``. A prior-balances reread is *not*
    an observation of this family: it is a ledger read operation
    (``current_prior_balances``) and, on the landing, the
    ``prior_balances_check`` phase. Wave 1 must never encode the two as
    one event.

``PRISM_LEDGER_READ_OPERATIONS``
    The closed operation vocabulary for the existing
    ``qbit_prism_ledger_read_*`` families, which already split
    coordinator-local admission (``gate_wait``) from PostgreSQL execution
    (``execute``) per operation. ``pending_block_candidate_rows`` is the
    one operation shipped today; ``payout_window_snapshot``
    (``snapshot_at_job_issue``), ``payout_window_delta``
    (``snapshot_between_job_issues``), ``current_prior_balances`` and
    ``prior_balances_after_pool_block`` are the names the ledger lane must
    use when it routes those statements through the attributed read path.
    ``fold_ledger_read_stats`` merges any other operation name into
    ``other`` before rendering so the label stays bounded even if a call
    site drifts.

``AcceptedPreviewPublicationDiagnostic``
    One bounded per-publication record for structured logging or the
    small in-process ring (``PRISM_ACCEPTED_PREVIEW_DIAGNOSTICS_CAPACITY``
    entries, oldest evicted). It carries the block hash and height that
    the metrics must not, the publication result, the acceptance-to-
    publication interval, the per-phase seconds, the reconcile caller, the
    rescan reason/path/seconds and the landing's ledger gate/execute
    totals. Every field is a fixed-vocabulary string, a number, or None.

Verified invariant: reconciliation and the share ledger
-------------------------------------------------------
A reconcile pass may change pool-block and payout balance state. Its
three mutators -- ``mark_pool_block_inactive``, ``reactivate_pool_block``
and ``reject_prepared_block`` -- call ``qbit_mark_pool_block_inactive``,
``qbit_reactivate_pool_block`` and ``qbit_reject_prepared_pool_block``,
which update ``qbit_pool_blocks``, ``qbit_pool_payout_entries``,
``qbit_payout_carry_forward``, ``qbit_ctv_fanout_artifacts`` and the
writer-lease row. Neither those functions, the ``qbit_confirm_pool_block``
and ``qbit_reverse_immature_pool_block`` helpers they call, nor any other
statement in ``crates/qbit-prism/sql/001_share_ledger.sql`` updates or
deletes from ``qbit_share_ledger``; the only writers of that table are the
share-append inserts in ``lab.prism.share_ledger``. So a reconcile pass
can move the prior-balances base the next preview must observe, but it
cannot change the contents of an accepted-share window at a fixed anchor.

The optimization that follows from this -- answering a
``reconcile_invalidation`` with a prior-balances reread instead of a full
window rescan -- relies on exactly that invariant. It is **not**
implemented in this wave. This module only makes sure the two events are
distinguishable once it is.

Handoff
-------
Every lane obtains the shared owner with
``ensure_accepted_preview_telemetry(runtime)`` where ``runtime`` is the
coordinator (the object every service already holds as ``self.runtime`` /
``self._runtime`` / ``self._coordinator``); the renderer obtains it from
its port the same way, so no coordinator attribute or ``_ensure_*`` method
needs adding and the lanes do not collide in ``prism_coordinator.py``.
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field, replace as dataclass_replace
import math
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping


# --- closed vocabularies ---------------------------------------------------

LANDING_PHASE_LANE_WAIT = "lane_wait"
LANDING_PHASE_BALANCE_LOCK_WAIT = "balance_lock_wait"
LANDING_PHASE_RECONCILE = "reconcile"
LANDING_PHASE_PRIOR_BALANCES_CHECK = "prior_balances_check"
LANDING_PHASE_CHAIN_PROBE = "chain_probe"
LANDING_PHASE_PREVIEW_PREPARE = "preview_prepare"
LANDING_PHASE_PREVIEW_PUBLISH = "preview_publish"
PRISM_ACCEPTED_LANDING_PHASES = (
    LANDING_PHASE_LANE_WAIT,
    LANDING_PHASE_BALANCE_LOCK_WAIT,
    LANDING_PHASE_RECONCILE,
    LANDING_PHASE_PRIOR_BALANCES_CHECK,
    LANDING_PHASE_CHAIN_PROBE,
    LANDING_PHASE_PREVIEW_PREPARE,
    LANDING_PHASE_PREVIEW_PUBLISH,
)

RECONCILE_CALLER_LANDING = "landing"
RECONCILE_CALLER_POST_CONFIRM = "post_confirm"
RECONCILE_CALLER_TIP_REFRESH = "tip_refresh"
RECONCILE_CALLER_JOB_BUILD = "job_build"
RECONCILE_CALLER_OTHER = "other"
PRISM_REORG_RECONCILE_CALLERS = (
    RECONCILE_CALLER_LANDING,
    RECONCILE_CALLER_POST_CONFIRM,
    RECONCILE_CALLER_TIP_REFRESH,
    RECONCILE_CALLER_JOB_BUILD,
    RECONCILE_CALLER_OTHER,
)

RECONCILE_STEP_ADMISSION_WAIT = "admission_wait"
RECONCILE_STEP_WATCH_QUERY = "watch_query"
RECONCILE_STEP_CHAIN_PROBE = "chain_probe"
RECONCILE_STEP_MUTATIONS = "mutations"
RECONCILE_STEP_CANDIDATE_PREPARE = "candidate_prepare"
RECONCILE_STEP_PUBLISH = "publish"
PRISM_REORG_RECONCILE_STEPS = (
    RECONCILE_STEP_ADMISSION_WAIT,
    RECONCILE_STEP_WATCH_QUERY,
    RECONCILE_STEP_CHAIN_PROBE,
    RECONCILE_STEP_MUTATIONS,
    RECONCILE_STEP_CANDIDATE_PREPARE,
    RECONCILE_STEP_PUBLISH,
)

FULL_RESCAN_REASON_OTHER = "other"
# Mirrors the literals in lab.prism.payout_state (see the module docstring
# and the AST drift test). Order is render order: the landing-forced reason
# first, then cache-lifecycle reasons, daemon reasons, incremental fallbacks,
# the periodic self-check family, and the fold-in bucket last.
PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS = (
    "reconcile_invalidation",
    "cold_start",
    "cache_invalidated",
    "late_visible_append",
    "anchor_regression",
    "snapshot_window_weight_out_of_band",
    "window_pipeline_mode_changed",
    "window_mirror_divergence",
    "window_daemon_unavailable",
    "window_daemon_busy",
    "window_daemon_state_lost",
    "window_value_out_of_range",
    "delta_api_unavailable",
    "incremental_invariant_failed",
    "periodic_self_check",
    "periodic_self_check_failed",
    "periodic_self_check_balance_check_failed",
    FULL_RESCAN_REASON_OTHER,
)

FULL_RESCAN_PATH_DAEMON = "daemon"
FULL_RESCAN_PATH_IN_PROCESS = "in_process"
PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS = (
    FULL_RESCAN_PATH_DAEMON,
    FULL_RESCAN_PATH_IN_PROCESS,
)

LEDGER_READ_OPERATION_PENDING_BLOCK_CANDIDATE_ROWS = "pending_block_candidate_rows"
LEDGER_READ_OPERATION_PAYOUT_WINDOW_SNAPSHOT = "payout_window_snapshot"
LEDGER_READ_OPERATION_PAYOUT_WINDOW_DELTA = "payout_window_delta"
LEDGER_READ_OPERATION_CURRENT_PRIOR_BALANCES = "current_prior_balances"
LEDGER_READ_OPERATION_PRIOR_BALANCES_AFTER_POOL_BLOCK = (
    "prior_balances_after_pool_block"
)
LEDGER_READ_OPERATION_OTHER = "other"
PRISM_LEDGER_READ_OPERATIONS = (
    LEDGER_READ_OPERATION_PENDING_BLOCK_CANDIDATE_ROWS,
    LEDGER_READ_OPERATION_PAYOUT_WINDOW_SNAPSHOT,
    LEDGER_READ_OPERATION_PAYOUT_WINDOW_DELTA,
    LEDGER_READ_OPERATION_CURRENT_PRIOR_BALANCES,
    LEDGER_READ_OPERATION_PRIOR_BALANCES_AFTER_POOL_BLOCK,
    LEDGER_READ_OPERATION_OTHER,
)

# How many per-publication diagnostic records the in-process ring keeps.
# Sized for an operator reading the last hour of a dense accepted-tip
# cadence (the #224 alert hour held 35 accepted blocks), not as a log.
PRISM_ACCEPTED_PREVIEW_DIAGNOSTICS_CAPACITY = 64


def normalize_payout_window_full_rescan_reason(reason: object) -> str:
    """Bound a payout_state reason string to the closed vocabulary."""
    if isinstance(reason, str) and reason in PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS:
        return reason
    return FULL_RESCAN_REASON_OTHER


def normalize_ledger_read_operation(operation: object) -> str:
    """Bound a ledger read operation name to the closed vocabulary."""
    if isinstance(operation, str) and operation in PRISM_LEDGER_READ_OPERATIONS:
        return operation
    return LEDGER_READ_OPERATION_OTHER


def fold_ledger_read_stats(
    stats_by_operation: Mapping[str, Mapping[str, float | int]],
) -> dict[str, dict[str, float | int]]:
    """Merge out-of-contract operation names into ``other`` for rendering.

    Operations in :data:`PRISM_LEDGER_READ_OPERATIONS` pass through as
    copies, so the shipped ``pending_block_candidate_rows`` series render
    exactly as before. Anything else is summed into ``other`` -- ``*_max``
    fields take the maximum, every other field adds -- so an operation
    name the ledger lane did not take from this contract still cannot open
    a new series.
    """
    folded: dict[str, dict[str, float | int]] = {}
    for operation, stats in stats_by_operation.items():
        key = normalize_ledger_read_operation(operation)
        target = folded.get(key)
        if target is None:
            folded[key] = dict(stats)
            continue
        for name, value in stats.items():
            if name.endswith("_max"):
                target[name] = max(float(target.get(name, 0.0)), float(value))
            else:
                target[name] = target.get(name, 0) + value
    return folded


def empty_duration_stats() -> dict[str, float | int]:
    """The sum/count/max cell every family here accumulates into."""
    return {"count": 0, "sum": 0.0, "max": 0.0}


def _accumulate(stats: dict[str, float | int], seconds: float) -> None:
    elapsed = float(seconds)
    if not math.isfinite(elapsed) or elapsed < 0.0:
        elapsed = 0.0
    stats["count"] = int(stats["count"]) + 1
    stats["sum"] = float(stats["sum"]) + elapsed
    stats["max"] = max(float(stats["max"]), elapsed)


def _publication_results() -> tuple[str, ...]:
    # Imported lazily: block_candidates is the B1 owner module and will
    # import this contract from its landing paths; a module-level import
    # here would make that a cycle.
    from lab.prism.block_candidates import (
        PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS,
    )

    return PRISM_ACCEPTED_BLOCK_PREVIEW_PUBLICATION_RESULTS


def _bounded_seconds(value: object) -> float:
    seconds = float(value)  # type: ignore[arg-type]
    if not math.isfinite(seconds) or seconds < 0.0:
        return 0.0
    return seconds


# --- neutral data types ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class AcceptedPreviewPublicationDiagnostic:
    """One bounded record of an accepted block's preview publication.

    This is the only place in the contract where a block hash or height
    may appear. It is a value for a structured log line or the diagnostics
    ring, never a metric label. Construction validates every vocabulary
    field and clamps every duration, so a record that exists is already
    safe to log.
    """

    block_hash: str
    block_height: int | None
    result: str
    acceptance_to_publication_seconds: float
    phase_seconds: Mapping[str, float] = field(default_factory=dict)
    reconcile_caller: str | None = None
    full_rescan_reason: str | None = None
    full_rescan_path: str | None = None
    full_rescan_seconds: float = 0.0
    ledger_gate_wait_seconds: float = 0.0
    ledger_execute_seconds: float = 0.0
    recorded_monotonic: float | None = None

    def __post_init__(self) -> None:
        block_hash = str(self.block_hash).strip().lower()
        if not block_hash:
            raise ValueError("diagnostic block_hash must be non-empty")
        object.__setattr__(self, "block_hash", block_hash)
        if self.block_height is not None:
            object.__setattr__(self, "block_height", int(self.block_height))
        if self.result not in _publication_results():
            raise ValueError(
                f"unknown accepted preview publication result: {self.result!r}"
            )
        phases: dict[str, float] = {
            phase: 0.0 for phase in PRISM_ACCEPTED_LANDING_PHASES
        }
        for phase, seconds in dict(self.phase_seconds).items():
            if phase not in phases:
                raise ValueError(f"unknown accepted landing phase: {phase!r}")
            phases[phase] = _bounded_seconds(seconds)
        object.__setattr__(self, "phase_seconds", MappingProxyType(phases))
        if (
            self.reconcile_caller is not None
            and self.reconcile_caller not in PRISM_REORG_RECONCILE_CALLERS
        ):
            raise ValueError(
                f"unknown reorg reconcile caller: {self.reconcile_caller!r}"
            )
        if self.full_rescan_reason is not None:
            object.__setattr__(
                self,
                "full_rescan_reason",
                normalize_payout_window_full_rescan_reason(self.full_rescan_reason),
            )
        if (
            self.full_rescan_path is not None
            and self.full_rescan_path not in PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS
        ):
            raise ValueError(
                f"unknown payout window full rescan path: {self.full_rescan_path!r}"
            )
        for name in (
            "acceptance_to_publication_seconds",
            "full_rescan_seconds",
            "ledger_gate_wait_seconds",
            "ledger_execute_seconds",
        ):
            object.__setattr__(self, name, _bounded_seconds(getattr(self, name)))
        if self.recorded_monotonic is not None:
            object.__setattr__(
                self, "recorded_monotonic", float(self.recorded_monotonic)
            )

    def log_fields(self) -> dict[str, object]:
        """Flat JSON-serializable fields for ``_payout_artifact_log`` style lines."""
        fields: dict[str, object] = {
            "block_hash": self.block_hash,
            "block_height": self.block_height,
            "result": self.result,
            "acceptance_to_publication_seconds": round(
                self.acceptance_to_publication_seconds, 6
            ),
            "reconcile_caller": self.reconcile_caller,
            "full_rescan_reason": self.full_rescan_reason,
            "full_rescan_path": self.full_rescan_path,
            "full_rescan_seconds": round(self.full_rescan_seconds, 6),
            "ledger_gate_wait_seconds": round(self.ledger_gate_wait_seconds, 6),
            "ledger_execute_seconds": round(self.ledger_execute_seconds, 6),
        }
        for phase in PRISM_ACCEPTED_LANDING_PHASES:
            fields[f"phase_{phase}_seconds"] = round(self.phase_seconds[phase], 6)
        return fields

    def summary(self) -> str:
        """One line naming the owner of the interval, for plain prints."""
        phases = " ".join(
            f"{phase}={self.phase_seconds[phase]:.3f}s"
            for phase in PRISM_ACCEPTED_LANDING_PHASES
        )
        rescan = (
            f" rescan={self.full_rescan_reason}/{self.full_rescan_path}"
            f"={self.full_rescan_seconds:.3f}s"
            if self.full_rescan_reason is not None
            else ""
        )
        return (
            f"hash={self.block_hash} height={self.block_height} "
            f"result={self.result} "
            f"total={self.acceptance_to_publication_seconds:.3f}s {phases}"
            f"{rescan} ledger_gate_wait={self.ledger_gate_wait_seconds:.3f}s "
            f"ledger_execute={self.ledger_execute_seconds:.3f}s"
        )


# --- the shared owner ------------------------------------------------------


class AcceptedPreviewTelemetry:
    """Fixed-cardinality accumulators plus the bounded diagnostics ring.

    One lock guards every cell, the same model as the B1 owner's
    publication histogram and the finalization service's phase metrics:
    recorders take it for one dictionary update, ``snapshot`` copies every
    cell under it and returns plain dicts the renderer may read without
    it. Every cell of every closed product exists from construction, so an
    empty owner renders every series at zero and a scrape never sees a
    family appear mid-life.
    """

    def __init__(
        self,
        *,
        diagnostics_capacity: int = PRISM_ACCEPTED_PREVIEW_DIAGNOSTICS_CAPACITY,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        capacity = int(diagnostics_capacity)
        if capacity <= 0:
            raise ValueError("diagnostics_capacity must be positive")
        self._monotonic = monotonic or time.monotonic
        self._lock = threading.Lock()
        self._landing_phases = {
            phase: empty_duration_stats() for phase in PRISM_ACCEPTED_LANDING_PHASES
        }
        self._reconcile_passes = {
            caller: empty_duration_stats() for caller in PRISM_REORG_RECONCILE_CALLERS
        }
        self._reconcile_steps = {
            (caller, step): empty_duration_stats()
            for caller in PRISM_REORG_RECONCILE_CALLERS
            for step in PRISM_REORG_RECONCILE_STEPS
        }
        self._full_rescans = {
            (reason, path): empty_duration_stats()
            for reason in PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS
            for path in PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS
        }
        self._diagnostics: deque[AcceptedPreviewPublicationDiagnostic] = deque(
            maxlen=capacity
        )
        self._diagnostics_recorded = 0

    # -- recorders ---------------------------------------------------------

    def observe_landing_phase(self, phase: str, seconds: float) -> None:
        if phase not in self._landing_phases:
            raise ValueError(f"unknown accepted landing phase: {phase!r}")
        with self._lock:
            _accumulate(self._landing_phases[phase], seconds)

    @contextmanager
    def landing_phase(self, phase: str) -> Iterator[None]:
        """Time one landing sub-phase; records on every exit, raising or not."""
        if phase not in self._landing_phases:
            raise ValueError(f"unknown accepted landing phase: {phase!r}")
        started = self._monotonic()
        try:
            yield
        finally:
            self.observe_landing_phase(phase, self._monotonic() - started)

    def observe_reconcile_pass(self, caller: str, seconds: float) -> None:
        if caller not in self._reconcile_passes:
            raise ValueError(f"unknown reorg reconcile caller: {caller!r}")
        with self._lock:
            _accumulate(self._reconcile_passes[caller], seconds)

    def observe_reconcile_step(self, caller: str, step: str, seconds: float) -> None:
        key = (caller, step)
        if key not in self._reconcile_steps:
            if caller not in PRISM_REORG_RECONCILE_CALLERS:
                raise ValueError(f"unknown reorg reconcile caller: {caller!r}")
            raise ValueError(f"unknown reorg reconcile step: {step!r}")
        with self._lock:
            _accumulate(self._reconcile_steps[key], seconds)

    @contextmanager
    def reconcile_step(self, caller: str, step: str) -> Iterator[None]:
        """Time one reconcile step for one caller; records on every exit."""
        if (caller, step) not in self._reconcile_steps:
            if caller not in PRISM_REORG_RECONCILE_CALLERS:
                raise ValueError(f"unknown reorg reconcile caller: {caller!r}")
            raise ValueError(f"unknown reorg reconcile step: {step!r}")
        started = self._monotonic()
        try:
            yield
        finally:
            self.observe_reconcile_step(caller, step, self._monotonic() - started)

    def observe_payout_window_full_rescan(
        self,
        reason: object,
        path: str,
        seconds: float,
    ) -> None:
        """Record one executed full oracle rescan.

        ``reason`` is the payout_state string and is normalized; ``path``
        is chosen by the recording code and must be a vocabulary member.
        A prior-balances reread must not be recorded here.
        """
        if path not in PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS:
            raise ValueError(f"unknown payout window full rescan path: {path!r}")
        key = (normalize_payout_window_full_rescan_reason(reason), path)
        with self._lock:
            _accumulate(self._full_rescans[key], seconds)

    def record_publication_diagnostic(
        self,
        diagnostic: AcceptedPreviewPublicationDiagnostic,
    ) -> AcceptedPreviewPublicationDiagnostic:
        """Retain one publication record, stamping it if the caller did not."""
        if not isinstance(diagnostic, AcceptedPreviewPublicationDiagnostic):
            raise TypeError("expected an AcceptedPreviewPublicationDiagnostic")
        if diagnostic.recorded_monotonic is None:
            diagnostic = dataclass_replace(
                diagnostic, recorded_monotonic=self._monotonic()
            )
        with self._lock:
            self._diagnostics.append(diagnostic)
            self._diagnostics_recorded += 1
        return diagnostic

    # -- readers -----------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Copied cells for one render; keys are the closed products."""
        with self._lock:
            return {
                "landing_phases": {
                    phase: dict(stats)
                    for phase, stats in self._landing_phases.items()
                },
                "reconcile_passes": {
                    caller: dict(stats)
                    for caller, stats in self._reconcile_passes.items()
                },
                "reconcile_steps": {
                    key: dict(stats) for key, stats in self._reconcile_steps.items()
                },
                "full_rescans": {
                    key: dict(stats) for key, stats in self._full_rescans.items()
                },
                "diagnostics_retained": len(self._diagnostics),
                "diagnostics_recorded": self._diagnostics_recorded,
            }

    def diagnostics_snapshot(
        self,
    ) -> tuple[AcceptedPreviewPublicationDiagnostic, ...]:
        """Retained publication records, oldest first."""
        with self._lock:
            return tuple(self._diagnostics)


_OWNER_ATTRIBUTE = "_accepted_preview_telemetry"
_OWNER_BOOTSTRAP_LOCK = threading.Lock()


def ensure_accepted_preview_telemetry(owner: object) -> AcceptedPreviewTelemetry:
    """The one telemetry owner attached to ``owner`` (normally the coordinator).

    Idempotent and race-free: the first caller from any thread creates
    the instance under a module bootstrap lock, every later caller gets
    that same instance. State lives in the owner's instance ``__dict__``
    exactly as the coordinator's lazily built renderer does, so no
    coordinator attribute, descriptor or ``_ensure_*`` method has to be
    added for the lanes or the renderer to share it.
    """
    try:
        state = vars(owner)
    except TypeError as exc:
        raise TypeError(
            "accepted preview telemetry needs an owner with an instance "
            f"__dict__, got {type(owner).__name__}"
        ) from exc
    telemetry = state.get(_OWNER_ATTRIBUTE)
    if telemetry is not None:
        return telemetry
    with _OWNER_BOOTSTRAP_LOCK:
        telemetry = state.get(_OWNER_ATTRIBUTE)
        if telemetry is None:
            telemetry = AcceptedPreviewTelemetry()
            state[_OWNER_ATTRIBUTE] = telemetry
    return telemetry


__all__ = [
    "AcceptedPreviewPublicationDiagnostic",
    "AcceptedPreviewTelemetry",
    "FULL_RESCAN_PATH_DAEMON",
    "FULL_RESCAN_PATH_IN_PROCESS",
    "FULL_RESCAN_REASON_OTHER",
    "LANDING_PHASE_BALANCE_LOCK_WAIT",
    "LANDING_PHASE_CHAIN_PROBE",
    "LANDING_PHASE_LANE_WAIT",
    "LANDING_PHASE_PREVIEW_PREPARE",
    "LANDING_PHASE_PREVIEW_PUBLISH",
    "LANDING_PHASE_PRIOR_BALANCES_CHECK",
    "LANDING_PHASE_RECONCILE",
    "LEDGER_READ_OPERATION_CURRENT_PRIOR_BALANCES",
    "LEDGER_READ_OPERATION_OTHER",
    "LEDGER_READ_OPERATION_PAYOUT_WINDOW_DELTA",
    "LEDGER_READ_OPERATION_PAYOUT_WINDOW_SNAPSHOT",
    "LEDGER_READ_OPERATION_PENDING_BLOCK_CANDIDATE_ROWS",
    "LEDGER_READ_OPERATION_PRIOR_BALANCES_AFTER_POOL_BLOCK",
    "PRISM_ACCEPTED_LANDING_PHASES",
    "PRISM_ACCEPTED_PREVIEW_DIAGNOSTICS_CAPACITY",
    "PRISM_LEDGER_READ_OPERATIONS",
    "PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS",
    "PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS",
    "PRISM_REORG_RECONCILE_CALLERS",
    "PRISM_REORG_RECONCILE_STEPS",
    "RECONCILE_CALLER_JOB_BUILD",
    "RECONCILE_CALLER_LANDING",
    "RECONCILE_CALLER_OTHER",
    "RECONCILE_CALLER_POST_CONFIRM",
    "RECONCILE_CALLER_TIP_REFRESH",
    "RECONCILE_STEP_ADMISSION_WAIT",
    "RECONCILE_STEP_CANDIDATE_PREPARE",
    "RECONCILE_STEP_CHAIN_PROBE",
    "RECONCILE_STEP_MUTATIONS",
    "RECONCILE_STEP_PUBLISH",
    "RECONCILE_STEP_WATCH_QUERY",
    "empty_duration_stats",
    "ensure_accepted_preview_telemetry",
    "fold_ledger_read_stats",
    "normalize_ledger_read_operation",
    "normalize_payout_window_full_rescan_reason",
]
