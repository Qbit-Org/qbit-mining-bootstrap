#!/usr/bin/env python3
"""Monotonic mining-progress readiness state for the PRISM coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
import math
import threading
import time
from typing import Callable, Mapping


PROGRESS_HEALTH_REASONS = (
    "tip_poll_stale",
    "refresh_pending_too_long",
    "current_generation_not_published",
    "current_generation_not_delivered",
    "bundle_build_stuck",
)

# --- Router-facing mining readiness (issue #186) ----------------------------
#
# /healthz answers "is this process alive and delivering?" and is deliberately
# quick to fail. A load balancer deciding where to send *new* miners needs a
# slower, latched answer: one that ignores the coverage dip every accepted tip
# causes, degrades only when a bad condition is sustained, and recovers only
# after the fleet has been demonstrably stable. The tracker below is that
# hysteresis, pure and scripted-clock testable; the observability owner feeds
# it from the background health refresh and caches its snapshot.

MINING_READINESS_SCHEMA = "qbit.prism.mining-readiness.v1"
MINING_READINESS_STATE_READY = "ready"
MINING_READINESS_STATE_DEGRADED = "degraded"
MINING_READINESS_STATES = (
    MINING_READINESS_STATE_READY,
    MINING_READINESS_STATE_DEGRADED,
)

# The closed reason vocabulary. Each name is one series of the fixed reason
# gauge and one possible member of the response's ``reasons`` list; nothing
# here carries a block hash, a client id, or any other unbounded value.
MINING_READINESS_REASON_WARMING_UP = "warming_up"
MINING_READINESS_REASON_SEMANTIC_COVERAGE_LOW = "semantic_coverage_low"
MINING_READINESS_REASON_REFRESH_PENDING_TOO_LONG = "refresh_pending_too_long"
MINING_READINESS_REASON_REFRESH_PENDING = "refresh_pending"
MINING_READINESS_REASON_RECOVERY_WINDOW_PENDING = "recovery_window_pending"
MINING_READINESS_REASON_DURABLE_CANDIDATE_OLD = "durable_candidate_old"
MINING_READINESS_REASON_PREVIEW_TIMEOUTS = "accepted_parent_preview_timeouts"
MINING_READINESS_REASONS = (
    MINING_READINESS_REASON_WARMING_UP,
    MINING_READINESS_REASON_SEMANTIC_COVERAGE_LOW,
    MINING_READINESS_REASON_REFRESH_PENDING_TOO_LONG,
    MINING_READINESS_REASON_REFRESH_PENDING,
    MINING_READINESS_REASON_RECOVERY_WINDOW_PENDING,
    MINING_READINESS_REASON_DURABLE_CANDIDATE_OLD,
    MINING_READINESS_REASON_PREVIEW_TIMEOUTS,
)

# Coverage thresholds are named policy, not tuning knobs. Entry mirrors the
# delivery-health predicate's five-percent loss; recovery demands the fleet
# be nearly whole so a partially refreshed fleet cannot count as stable.
MINING_READINESS_ENTRY_COVERAGE_RATIO = 0.95
MINING_READINESS_RECOVERY_COVERAGE_RATIO = 0.99

# A durable pending block candidate older than this annotates a degraded or
# recovering snapshot. It is the documented warning threshold on
# qbit_prism_block_candidate_oldest_pending_seconds (docs/prism-overload-
# alerts.md) and never latches a transition on its own.
MINING_READINESS_OLD_CANDIDATE_AGE_SECONDS = 60.0

# Defaults for the two hysteresis windows. The entry dwell exceeds the
# 2026-08-21 normal tip-refresh cycle (pending age peaked near 36.77s), so the
# 0 -> partial -> 1 -> 0 coverage sweep at each accepted tip cannot flap the
# signal. The recovery window exceeds the 213-second 2026-08-20 oscillation
# between the apparent healthy point at 20:10:27Z and stability at 20:14:00Z.
DEFAULT_PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS = 60.0
DEFAULT_PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS = 240.0


@dataclass(frozen=True)
class WorkGeneration:
    template_generation: int
    template_fingerprint: str | None
    payout_generation: int


@dataclass(frozen=True)
class DeliveryProof:
    connection_id: int
    delivered_work: WorkGeneration
    collection_only: bool
    delivered_monotonic: float


@dataclass(frozen=True)
class EligibilitySnapshot:
    eligible_connection_ids: tuple[int, ...]
    delivery_proofs: tuple[DeliveryProof, ...]
    ready_mode_required: bool


# Adapter signature used by the coordinator to supply live client facts:
# ``(template_fingerprint, payout_generation) -> (eligible, requiring_refresh)``.
EligibleClientCounts = Callable[[str | None, int], tuple[int, int]]


@dataclass(frozen=True)
class ProgressHealthConfig:
    pending_refresh_deadline_seconds: float
    tip_poll_deadline_seconds: float
    bundle_build_deadline_seconds: float


@dataclass(frozen=True)
class ProgressHealthSnapshot:
    ok: bool
    reason: str | None
    reasons: tuple[str, ...]
    pending_refresh: bool
    pending_refresh_age_seconds: float | None
    tip_poll_age_seconds: float
    tip_refresh_in_progress: bool
    tip_refresh_progress_age_seconds: float | None
    current_template_generation: int
    published_template_generation: int
    current_payout_generation: int
    published_payout_generation: int
    last_valid_delivery_age_seconds: float | None
    eligible_client_count: int
    eligible_clients_requiring_refresh: int
    bundle_build_oldest_age_seconds: float

    def as_mapping(self) -> dict[str, object]:
        """Return the existing mutable HTTP/test compatibility representation."""

        return {
            "ok": self.ok,
            "reason": self.reason,
            "reasons": list(self.reasons),
            "pending_refresh": self.pending_refresh,
            "pending_refresh_age_seconds": self.pending_refresh_age_seconds,
            "tip_poll_age_seconds": self.tip_poll_age_seconds,
            "tip_refresh_in_progress": self.tip_refresh_in_progress,
            "tip_refresh_progress_age_seconds": self.tip_refresh_progress_age_seconds,
            "current_template_generation": self.current_template_generation,
            "published_template_generation": self.published_template_generation,
            "current_payout_generation": self.current_payout_generation,
            "published_payout_generation": self.published_payout_generation,
            "last_valid_delivery_age_seconds": self.last_valid_delivery_age_seconds,
            "eligible_client_count": self.eligible_client_count,
            "eligible_clients_requiring_refresh": (
                self.eligible_clients_requiring_refresh
            ),
            "bundle_build_oldest_age_seconds": self.bundle_build_oldest_age_seconds,
        }


@dataclass(frozen=True)
class _ProgressStateCopy:
    current_template_generation: int
    current_template_fingerprint: str | None
    current_payout_generation: int
    published_template_generation: int
    published_template_fingerprint: str | None
    published_payout_generation: int
    has_published_work: bool
    last_tip_poll_monotonic: float | None
    last_delivery_template_fingerprint: str | None
    last_delivery_payout_generation: int
    last_delivery_monotonic: float | None
    pending_since_monotonic: float | None
    publication_divergence_since_monotonic: float | None
    refresh_signal_pending: bool
    active_refresh_count: int
    last_refresh_activity_monotonic: float | None
    bundle_build_starts: tuple[float, ...]


class RefreshActivityToken:
    """One idempotent, context-managed refresh activity lifetime."""

    def __init__(self, service: ProgressHealthService) -> None:
        self._service = service
        self._state_lock = threading.Lock()
        self._finished = False

    def note_activity(self, observed_monotonic: float | None = None) -> None:
        with self._state_lock:
            if self._finished:
                return
            self._service.note_refresh_activity(observed_monotonic)

    def finish(self) -> None:
        with self._state_lock:
            if self._finished:
                return
            self._finished = True
            self._service.refresh_finished()

    cancel = finish

    def __enter__(self) -> RefreshActivityToken:
        return self

    def __exit__(self, *_args: object) -> None:
        self.finish()


class BundleBuildToken:
    """One idempotent, context-managed bundle construction lifetime."""

    def __init__(self, service: ProgressHealthService, token_id: int) -> None:
        self._service = service
        self._token_id = token_id
        self._state_lock = threading.Lock()
        self._finished = False

    def finish(self) -> None:
        with self._state_lock:
            if self._finished:
                return
            self._finished = True
            self._service.bundle_build_finished(self._token_id)

    cancel = finish

    def __enter__(self) -> BundleBuildToken:
        return self

    def __exit__(self, *_args: object) -> None:
        self.finish()


class ProgressHealthService:
    """Own aggregate mining-progress state and evaluate bounded readiness.

    State fields are deliberately plain attributes matching the historical
    coordinator layout one-for-one: the coordinator routes its legacy raw
    ``_progress_*`` names here through class descriptors, and the historical
    white-box protocol mutates them directly while holding ``_lock``. Service
    methods take the lock themselves.
    """

    def __init__(
        self,
        *,
        started_monotonic: float,
        initial_payout_generation: int = 0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._current_template_generation = 0
        self._current_template_fingerprint: str | None = None
        self._current_payout_generation = int(initial_payout_generation)
        self._published_template_generation = 0
        self._published_template_fingerprint: str | None = None
        self._published_payout_generation = 0
        self._has_published_work = False
        self._last_tip_poll_monotonic: float | None = None
        self._last_delivery_template_generation = 0
        self._last_delivery_template_fingerprint: str | None = None
        self._last_delivery_payout_generation = 0
        self._last_delivery_monotonic: float | None = None
        self._pending_since_monotonic: float | None = float(started_monotonic)
        # Start of the current detected-vs-published divergence epoch. Cleared
        # only by current-work publication/delivery that matches the latest
        # detected tip, so old delivery backlog cannot renew or expire a later
        # legitimate divergence (the publication watchdog reads it).
        self._publication_divergence_since_monotonic: float | None = float(
            started_monotonic
        )
        self._refresh_signal_pending = False
        self._active_refresh_count = 0
        self._last_refresh_activity_monotonic: float | None = None
        self._bundle_build_counter = 0
        self._bundle_builds: dict[int, float] = {}

    def now(self) -> float:
        return float(self._monotonic())

    def mark_refresh_pending(self, started_monotonic: float | None = None) -> None:
        started = self.now() if started_monotonic is None else started_monotonic
        with self._lock:
            pending_since = self._pending_since_monotonic
            if pending_since is None or started < pending_since:
                self._pending_since_monotonic = started
            divergence_since = self._publication_divergence_since_monotonic
            if divergence_since is None or started < divergence_since:
                self._publication_divergence_since_monotonic = started
            self._refresh_signal_pending = True

    def observe_tip(
        self,
        work: WorkGeneration,
        observed_monotonic: float | None = None,
    ) -> None:
        """Publish a coherent qbit tip/template observation to health state."""
        observed = self.now() if observed_monotonic is None else observed_monotonic
        with self._lock:
            if work.template_generation < self._current_template_generation:
                # A slower concurrent poll can finish after a newer coherent
                # observation. It proves only that the obsolete generation was
                # coherent, so it must not renew freshness for current work.
                return
            self._current_template_generation = work.template_generation
            self._current_template_fingerprint = work.template_fingerprint
            self._current_payout_generation = max(
                self._current_payout_generation,
                work.payout_generation,
            )
            self._last_tip_poll_monotonic = observed
            same_published_work = bool(
                self._has_published_work
                and self._published_template_fingerprint
                == work.template_fingerprint
                and self._published_payout_generation
                == self._current_payout_generation
            )
            if same_published_work:
                # Observation generations order RPC races. When the semantic
                # template fingerprint and payout generation are unchanged,
                # already-issued work remains current and can be reconciled to
                # the latest observation without a needless socket delivery.
                self._published_template_generation = work.template_generation
            else:
                pending_since = self._pending_since_monotonic
                if pending_since is None or observed < pending_since:
                    self._pending_since_monotonic = observed
                divergence_since = self._publication_divergence_since_monotonic
                if divergence_since is None or observed < divergence_since:
                    self._publication_divergence_since_monotonic = observed

    def observe_payout_generation(
        self,
        generation: int,
        invalidated_monotonic: float | None = None,
    ) -> None:
        invalidated = (
            self.now()
            if invalidated_monotonic is None
            else invalidated_monotonic
        )
        with self._lock:
            if generation < self._current_payout_generation:
                return
            self._current_payout_generation = generation
            if generation != self._published_payout_generation:
                pending_since = self._pending_since_monotonic
                if pending_since is None or invalidated < pending_since:
                    self._pending_since_monotonic = invalidated
                divergence_since = self._publication_divergence_since_monotonic
                if divergence_since is None or invalidated < divergence_since:
                    self._publication_divergence_since_monotonic = invalidated
                self._refresh_signal_pending = True

    def publish_work(
        self,
        work: WorkGeneration,
        *,
        matches_latest_tip: bool = True,
    ) -> bool:
        """Record that current in-memory work is available for delivery."""
        with self._lock:
            if (
                work.template_generation < self._published_template_generation
                or work.payout_generation < self._published_payout_generation
            ):
                return False
            self._published_template_generation = max(
                self._published_template_generation,
                work.template_generation,
            )
            self._published_template_fingerprint = work.template_fingerprint
            self._published_payout_generation = max(
                self._published_payout_generation,
                work.payout_generation,
            )
            self._has_published_work = True
            if (
                matches_latest_tip
                and self._current_template_fingerprint == work.template_fingerprint
                and self._current_payout_generation == work.payout_generation
            ):
                self._refresh_signal_pending = False
                self._publication_divergence_since_monotonic = None
        return True

    def record_delivery(
        self,
        proof: DeliveryProof,
        ready_mode_required: bool,
        *,
        matches_latest_tip: bool = True,
    ) -> bool:
        """Record a completed current-generation socket delivery."""
        work = proof.delivered_work
        with self._lock:
            if (
                work.template_fingerprint == self._current_template_fingerprint
                and work.payout_generation == self._current_payout_generation
                and not (ready_mode_required and proof.collection_only)
            ):
                self._last_delivery_template_generation = work.template_generation
                self._last_delivery_template_fingerprint = work.template_fingerprint
                self._last_delivery_payout_generation = work.payout_generation
                self._last_delivery_monotonic = proof.delivered_monotonic
                # A successful delivery proves publication for its coherent
                # generation. Only the latest observed tip can close the
                # outstanding divergence below.
                self._published_template_generation = max(
                    self._published_template_generation,
                    self._current_template_generation,
                )
                self._published_template_fingerprint = work.template_fingerprint
                self._published_payout_generation = max(
                    self._published_payout_generation,
                    work.payout_generation,
                )
                self._has_published_work = True
                self._refresh_signal_pending = False
                if matches_latest_tip:
                    self._publication_divergence_since_monotonic = None
                return True
        return False

    def refresh_started(self) -> None:
        """Track a coherent poll pass while it continues making progress."""
        started = self.now()
        with self._lock:
            self._active_refresh_count += 1
            self._last_refresh_activity_monotonic = started

    def note_refresh_activity(
        self,
        observed_monotonic: float | None = None,
    ) -> None:
        observed = (
            self.now()
            if observed_monotonic is None
            else observed_monotonic
        )
        with self._lock:
            if self._active_refresh_count > 0:
                last_activity = self._last_refresh_activity_monotonic
                if last_activity is None or observed > last_activity:
                    self._last_refresh_activity_monotonic = observed

    def refresh_finished(self) -> None:
        with self._lock:
            self._active_refresh_count = max(0, self._active_refresh_count - 1)

    def start_refresh(self) -> RefreshActivityToken:
        self.refresh_started()
        return RefreshActivityToken(self)

    def bundle_build_started(self) -> int:
        started = self.now()
        with self._lock:
            self._bundle_build_counter += 1
            token = self._bundle_build_counter
            self._bundle_builds[token] = started
            return token

    def bundle_build_finished(self, token: int) -> None:
        with self._lock:
            self._bundle_builds.pop(token, None)

    def start_bundle_build(self) -> BundleBuildToken:
        return BundleBuildToken(self, self.bundle_build_started())

    @staticmethod
    def requiring_refresh(
        eligibility: EligibilitySnapshot,
        work: WorkGeneration,
    ) -> int:
        proofs = {
            proof.connection_id: proof
            for proof in eligibility.delivery_proofs
        }
        requiring_refresh = 0
        for connection_id in eligibility.eligible_connection_ids:
            proof = proofs.get(connection_id)
            if (
                proof is None
                or proof.delivered_work.template_fingerprint
                != work.template_fingerprint
                or proof.delivered_work.payout_generation
                != work.payout_generation
                or (
                    eligibility.ready_mode_required
                    and proof.collection_only
                )
            ):
                requiring_refresh += 1
        return requiring_refresh

    def _copy_state_locked(self) -> _ProgressStateCopy:
        return _ProgressStateCopy(
            current_template_generation=self._current_template_generation,
            current_template_fingerprint=self._current_template_fingerprint,
            current_payout_generation=self._current_payout_generation,
            published_template_generation=self._published_template_generation,
            published_template_fingerprint=self._published_template_fingerprint,
            published_payout_generation=self._published_payout_generation,
            has_published_work=self._has_published_work,
            last_tip_poll_monotonic=self._last_tip_poll_monotonic,
            last_delivery_template_fingerprint=(
                self._last_delivery_template_fingerprint
            ),
            last_delivery_payout_generation=self._last_delivery_payout_generation,
            last_delivery_monotonic=self._last_delivery_monotonic,
            pending_since_monotonic=self._pending_since_monotonic,
            publication_divergence_since_monotonic=(
                self._publication_divergence_since_monotonic
            ),
            refresh_signal_pending=self._refresh_signal_pending,
            active_refresh_count=self._active_refresh_count,
            last_refresh_activity_monotonic=(
                self._last_refresh_activity_monotonic
            ),
            bundle_build_starts=tuple(self._bundle_builds.values()),
        )

    def publication_divergence_since(self) -> float | None:
        """Locked read of the divergence stamp for the publication watchdog."""
        with self._lock:
            return self._publication_divergence_since_monotonic

    def reconcile_pending(
        self,
        eligible_counts: EligibleClientCounts,
        *,
        now: float | None = None,
    ) -> None:
        """Clear pending state exactly when publication/delivery is sufficient."""
        current = self.now() if now is None else now
        with self._lock:
            state_key = (
                self._current_template_fingerprint,
                self._current_payout_generation,
            )
        _, requiring_refresh = eligible_counts(*state_key)
        with self._lock:
            if state_key != (
                self._current_template_fingerprint,
                self._current_payout_generation,
            ):
                return
            published_current = bool(
                self._has_published_work
                and self._published_template_fingerprint == state_key[0]
                and self._published_payout_generation == state_key[1]
            )
            refresh_required = bool(
                self._refresh_signal_pending
                or not published_current
                or requiring_refresh > 0
            )
            if refresh_required:
                if self._pending_since_monotonic is None:
                    self._pending_since_monotonic = current
            else:
                self._pending_since_monotonic = None

    def snapshot(
        self,
        eligible_counts: EligibleClientCounts,
        current_payout_generation: int,
        config: ProgressHealthConfig,
        started_monotonic: float | None = None,
        *,
        now: float | None = None,
    ) -> ProgressHealthSnapshot:
        """Return a bounded, monotonic-only mining progress health snapshot."""
        current = self.now() if now is None else now
        with self._lock:
            tracked_payout_generation = self._current_payout_generation
        if current_payout_generation > tracked_payout_generation:
            self.observe_payout_generation(current_payout_generation, current)
        self.reconcile_pending(eligible_counts, now=current)

        with self._lock:
            state = self._copy_state_locked()

        eligible_count, requiring_refresh = eligible_counts(
            state.current_template_fingerprint,
            state.current_payout_generation,
        )
        started = (
            current if started_monotonic is None else float(started_monotonic)
        )
        tip_poll_reference = (
            started
            if state.last_tip_poll_monotonic is None
            else state.last_tip_poll_monotonic
        )
        tip_poll_age = max(0.0, current - tip_poll_reference)
        refresh_activity_age = (
            None
            if (
                state.active_refresh_count <= 0
                or state.last_refresh_activity_monotonic is None
            )
            else max(0.0, current - state.last_refresh_activity_monotonic)
        )
        pending_age = (
            None
            if state.pending_since_monotonic is None
            else max(0.0, current - state.pending_since_monotonic)
        )
        oldest_bundle_age = (
            0.0
            if not state.bundle_build_starts
            else max(0.0, current - min(state.bundle_build_starts))
        )
        published_current = bool(
            state.has_published_work
            and state.published_template_fingerprint
            == state.current_template_fingerprint
            and state.published_payout_generation
            == state.current_payout_generation
        )
        delivered_current = bool(
            state.current_template_fingerprint is not None
            and state.last_delivery_template_fingerprint
            == state.current_template_fingerprint
            and state.last_delivery_payout_generation
            == state.current_payout_generation
        )
        delivery_age = (
            max(0.0, current - state.last_delivery_monotonic)
            if delivered_current and state.last_delivery_monotonic is not None
            else None
        )
        reasons: list[str] = []
        refresh_is_progressing = bool(
            state.active_refresh_count > 0
            and refresh_activity_age is not None
            and refresh_activity_age <= config.tip_poll_deadline_seconds
        )
        if (
            tip_poll_age > config.tip_poll_deadline_seconds
            and not refresh_is_progressing
        ):
            reasons.append("tip_poll_stale")
        if oldest_bundle_age > config.bundle_build_deadline_seconds:
            reasons.append("bundle_build_stuck")
        if not state.has_published_work:
            reasons.append("current_generation_not_published")
        elif (
            pending_age is not None
            and pending_age > config.pending_refresh_deadline_seconds
        ):
            reasons.append("refresh_pending_too_long")
            if state.refresh_signal_pending or not published_current:
                reasons.append("current_generation_not_published")
            elif requiring_refresh > 0:
                reasons.append("current_generation_not_delivered")
        reasons = list(dict.fromkeys(reasons))

        def rounded(value: float | None) -> float | None:
            return None if value is None else round(value, 3)

        return ProgressHealthSnapshot(
            ok=not reasons,
            reason=reasons[0] if reasons else None,
            reasons=tuple(reasons),
            pending_refresh=state.pending_since_monotonic is not None,
            pending_refresh_age_seconds=rounded(pending_age),
            tip_poll_age_seconds=rounded(tip_poll_age),
            tip_refresh_in_progress=state.active_refresh_count > 0,
            tip_refresh_progress_age_seconds=rounded(refresh_activity_age),
            current_template_generation=state.current_template_generation,
            published_template_generation=state.published_template_generation,
            current_payout_generation=state.current_payout_generation,
            published_payout_generation=state.published_payout_generation,
            last_valid_delivery_age_seconds=rounded(delivery_age),
            eligible_client_count=eligible_count,
            eligible_clients_requiring_refresh=requiring_refresh,
            bundle_build_oldest_age_seconds=rounded(oldest_bundle_age),
        )

    @staticmethod
    def overlay(
        base_health: Mapping[str, object],
        snapshot: ProgressHealthSnapshot,
    ) -> Mapping[str, object]:
        return overlay_progress_health(base_health, snapshot.as_mapping())

    @staticmethod
    def metrics_lines(
        snapshot: ProgressHealthSnapshot,
        *,
        coordination_blocked_age_seconds: float,
    ) -> tuple[str, ...]:
        pending_age = snapshot.pending_refresh_age_seconds
        delivery_age = snapshot.last_valid_delivery_age_seconds
        active_reasons = set(snapshot.reasons)
        return (
            "# HELP qbit_prism_refresh_pending Whether current template or payout work still requires publication or delivery.",
            "# TYPE qbit_prism_refresh_pending gauge",
            f"qbit_prism_refresh_pending {1 if snapshot.pending_refresh else 0}",
            "# HELP qbit_prism_refresh_pending_age_seconds Monotonic age of the oldest unresolved current-work refresh.",
            "# TYPE qbit_prism_refresh_pending_age_seconds gauge",
            f"qbit_prism_refresh_pending_age_seconds {float(pending_age or 0.0):.6f}",
            "# HELP qbit_prism_tip_poll_age_seconds Monotonic age of the last coherent qbit tip/template poll.",
            "# TYPE qbit_prism_tip_poll_age_seconds gauge",
            f"qbit_prism_tip_poll_age_seconds {float(snapshot.tip_poll_age_seconds):.6f}",
            "# HELP qbit_prism_current_generation_delivery_age_seconds Monotonic age of the last valid current-generation delivery, or -1 when none exists.",
            "# TYPE qbit_prism_current_generation_delivery_age_seconds gauge",
            f"qbit_prism_current_generation_delivery_age_seconds {float(delivery_age) if delivery_age is not None else -1.0:.6f}",
            "# HELP qbit_prism_bundle_build_oldest_age_seconds Monotonic age of the oldest active bundle build.",
            "# TYPE qbit_prism_bundle_build_oldest_age_seconds gauge",
            f"qbit_prism_bundle_build_oldest_age_seconds {float(snapshot.bundle_build_oldest_age_seconds):.6f}",
            "# HELP qbit_prism_template_refresh_coordination_blocked_age_seconds Monotonic age of the continuous coordination-blocked template-refresh streak, or 0 when clear.",
            "# TYPE qbit_prism_template_refresh_coordination_blocked_age_seconds gauge",
            f"qbit_prism_template_refresh_coordination_blocked_age_seconds {coordination_blocked_age_seconds:.6f}",
            "# HELP qbit_prism_health_state Current progress-health state by bounded reason.",
            "# TYPE qbit_prism_health_state gauge",
            f'qbit_prism_health_state{{reason="healthy"}} {1 if snapshot.ok else 0}',
            *(
                f'qbit_prism_health_state{{reason="{reason}"}} {1 if reason in active_reasons else 0}'
                for reason in PROGRESS_HEALTH_REASONS
            ),
        )


@dataclass(frozen=True)
class MiningReadinessConfig:
    """The two hysteresis windows, validated once at construction."""

    entry_dwell_seconds: float = DEFAULT_PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS
    recovery_window_seconds: float = (
        DEFAULT_PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS
    )

    def __post_init__(self) -> None:
        for name in ("entry_dwell_seconds", "recovery_window_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"mining readiness {name} must be a number")
            if not math.isfinite(value):
                raise ValueError(f"mining readiness {name} must be finite")
            if value < 0:
                raise ValueError(f"mining readiness {name} must be nonnegative")
            object.__setattr__(self, name, float(value))
        if self.recovery_window_seconds < self.entry_dwell_seconds:
            raise ValueError(
                "mining readiness recovery_window_seconds must be at least "
                "entry_dwell_seconds"
            )


@dataclass(frozen=True)
class MiningReadinessSample:
    """One background observation, already reduced to the facts policy needs.

    Every field is copied from snapshots the refresher already maintains: the
    delivery snapshot's semantic ratio, the progress-health mapping's refresh
    facts, the candidate outbox aggregate, and the accepted-parent preview
    timeout counter. Nothing here is read on a request thread.
    """

    monotonic: float
    semantic_current_work_ratio: float
    refresh_pending: bool
    refresh_pending_age_seconds: float | None
    refresh_pending_too_long: bool
    eligible_clients_requiring_refresh: int
    # None when the durable outbox aggregate is unavailable (the metrics
    # gauge reports -1 for the same condition).
    oldest_durable_candidate_age_seconds: float | None
    # Monotonic process counter; the tracker differences consecutive samples.
    accepted_parent_preview_wait_timeouts: int

    @property
    def entry_condition(self) -> bool:
        """True when this sample alone argues for degrading."""

        return bool(
            self.semantic_current_work_ratio
            < MINING_READINESS_ENTRY_COVERAGE_RATIO
            or self.refresh_pending_too_long
        )

    @property
    def recovery_condition(self) -> bool:
        """True when this sample alone argues the fleet is stable."""

        return bool(
            self.semantic_current_work_ratio
            >= MINING_READINESS_RECOVERY_COVERAGE_RATIO
            and not self.refresh_pending_too_long
            and self.eligible_clients_requiring_refresh <= 0
            and not self.refresh_pending
        )


@dataclass(frozen=True)
class MiningReadinessSnapshot:
    """One immutable latched answer plus the diagnostics that qualify it."""

    state: str
    state_since_monotonic: float
    sample_monotonic: float
    reasons: tuple[str, ...]
    transitions: int
    entry_streak_seconds: float
    recovery_streak_seconds: float
    semantic_current_work_ratio: float
    refresh_pending: bool
    refresh_pending_age_seconds: float | None
    refresh_pending_too_long: bool
    eligible_clients_requiring_refresh: int
    oldest_durable_candidate_age_seconds: float | None
    accepted_parent_preview_timeout_rate_per_second: float
    entry_dwell_seconds: float
    recovery_window_seconds: float

    @property
    def ready(self) -> bool:
        return self.state == MINING_READINESS_STATE_READY

    def state_age_seconds(self, now: float) -> float:
        return max(0.0, now - self.state_since_monotonic)

    def sample_age_seconds(self, now: float) -> float:
        return max(0.0, now - self.sample_monotonic)


class MiningReadinessTracker:
    """Latch mining readiness with independent entry and recovery windows.

    Not thread-safe by design: the observability owner drives ``observe`` from
    its single background refresher and publishes the returned snapshot under
    its own lock. The tracker keeps exactly two timers and the last counter
    sample -- no history that can grow with uptime.

    Policy, stated once:

    - Ready is the initial latched state at the first sample. Before that
      there is no snapshot, and the owner answers fail-closed warming-up.
    - While ready, a sample whose ``entry_condition`` holds extends the entry
      streak; any other sample resets it. The state changes to degraded
      exactly when the streak reaches ``entry_dwell_seconds``.
    - While degraded, a sample whose ``recovery_condition`` holds extends the
      recovery streak; any other sample resets it. The state changes to ready
      exactly when the streak reaches ``recovery_window_seconds``.
    - The sampling owner may supply its staleness budget to ``observe``. A
      larger gap between successful observations resets both streaks before
      the new sample is applied, so elapsed wall time without evidence can
      never satisfy either continuous window.
    - The candidate age and the preview-timeout rate annotate a degraded or
      recovering snapshot's reasons. They never start or extend a streak.
    """

    def __init__(self, config: MiningReadinessConfig) -> None:
        self.config = config
        self._state = MINING_READINESS_STATE_READY
        self._state_since: float | None = None
        self._entry_since: float | None = None
        self._recovery_since: float | None = None
        self._last_sample_monotonic: float | None = None
        self._transitions = 0
        self._last_timeout_count: int | None = None
        self._last_timeout_monotonic: float | None = None

    @property
    def state(self) -> str:
        return self._state

    @property
    def transitions(self) -> int:
        return self._transitions

    def _timeout_rate(self, sample: MiningReadinessSample) -> float:
        """Per-second rate from consecutive counter samples; zero at first."""

        previous_count = self._last_timeout_count
        previous_monotonic = self._last_timeout_monotonic
        self._last_timeout_count = int(sample.accepted_parent_preview_wait_timeouts)
        self._last_timeout_monotonic = sample.monotonic
        if previous_count is None or previous_monotonic is None:
            return 0.0
        elapsed = sample.monotonic - previous_monotonic
        if elapsed <= 0.0:
            return 0.0
        # A counter that appears to run backwards (a test override, or a
        # replaced owner) is read as no activity rather than a negative rate.
        delta = max(0, self._last_timeout_count - previous_count)
        return delta / elapsed

    def observe(
        self,
        sample: MiningReadinessSample,
        *,
        max_sample_gap_seconds: float | None = None,
    ) -> MiningReadinessSnapshot:
        now = float(sample.monotonic)
        previous_sample_monotonic = self._last_sample_monotonic
        sample_gap_exceeded = False
        if max_sample_gap_seconds is not None:
            max_gap = float(max_sample_gap_seconds)
            if not math.isfinite(max_gap) or max_gap < 0.0:
                raise ValueError(
                    "mining readiness max_sample_gap_seconds must be finite "
                    "and nonnegative"
                )
            sample_gap_exceeded = (
                previous_sample_monotonic is not None
                and now - previous_sample_monotonic > max_gap
            )
        self._last_sample_monotonic = now
        if sample_gap_exceeded:
            # ``observe`` is called only after a complete health refresh,
            # so a larger interval is a period with no successful sample,
            # not evidence that either condition held continuously.
            self._entry_since = None
            self._recovery_since = None
        if self._state_since is None:
            self._state_since = now
        timeout_rate = self._timeout_rate(sample)

        if self._state == MINING_READINESS_STATE_READY:
            if sample.entry_condition:
                if self._entry_since is None:
                    self._entry_since = now
                if now - self._entry_since >= self.config.entry_dwell_seconds:
                    self._state = MINING_READINESS_STATE_DEGRADED
                    self._state_since = now
                    self._transitions += 1
                    self._entry_since = None
                    # A sample that just degraded cannot also be stable.
                    self._recovery_since = None
            else:
                self._entry_since = None
        else:
            if sample.recovery_condition:
                if self._recovery_since is None:
                    self._recovery_since = now
                if now - self._recovery_since >= self.config.recovery_window_seconds:
                    self._state = MINING_READINESS_STATE_READY
                    self._state_since = now
                    self._transitions += 1
                    self._recovery_since = None
                    self._entry_since = None
            else:
                self._recovery_since = None

        entry_streak = (
            0.0 if self._entry_since is None else max(0.0, now - self._entry_since)
        )
        recovery_streak = (
            0.0
            if self._recovery_since is None
            else max(0.0, now - self._recovery_since)
        )
        return MiningReadinessSnapshot(
            state=self._state,
            state_since_monotonic=self._state_since,
            sample_monotonic=now,
            reasons=self._reasons(sample, timeout_rate),
            transitions=self._transitions,
            entry_streak_seconds=entry_streak,
            recovery_streak_seconds=recovery_streak,
            semantic_current_work_ratio=float(sample.semantic_current_work_ratio),
            refresh_pending=bool(sample.refresh_pending),
            refresh_pending_age_seconds=sample.refresh_pending_age_seconds,
            refresh_pending_too_long=bool(sample.refresh_pending_too_long),
            eligible_clients_requiring_refresh=int(
                sample.eligible_clients_requiring_refresh
            ),
            oldest_durable_candidate_age_seconds=(
                sample.oldest_durable_candidate_age_seconds
            ),
            accepted_parent_preview_timeout_rate_per_second=timeout_rate,
            entry_dwell_seconds=self.config.entry_dwell_seconds,
            recovery_window_seconds=self.config.recovery_window_seconds,
        )

    def _reasons(
        self,
        sample: MiningReadinessSample,
        timeout_rate: float,
    ) -> tuple[str, ...]:
        """Reasons in vocabulary order, judged against the latched state.

        Ready: the entry conditions currently being dwelled on, so an
        operator can see a dwell counting before it lands. Degraded: every
        condition currently blocking recovery, or the open recovery window
        when nothing blocks it, plus the two corroborating annotations.
        """

        reasons: list[str] = []
        if self._state == MINING_READINESS_STATE_READY:
            if (
                sample.semantic_current_work_ratio
                < MINING_READINESS_ENTRY_COVERAGE_RATIO
            ):
                reasons.append(MINING_READINESS_REASON_SEMANTIC_COVERAGE_LOW)
            if sample.refresh_pending_too_long:
                reasons.append(MINING_READINESS_REASON_REFRESH_PENDING_TOO_LONG)
            return tuple(reasons)
        if (
            sample.semantic_current_work_ratio
            < MINING_READINESS_RECOVERY_COVERAGE_RATIO
        ):
            reasons.append(MINING_READINESS_REASON_SEMANTIC_COVERAGE_LOW)
        if sample.refresh_pending_too_long:
            reasons.append(MINING_READINESS_REASON_REFRESH_PENDING_TOO_LONG)
        elif sample.refresh_pending or sample.eligible_clients_requiring_refresh > 0:
            reasons.append(MINING_READINESS_REASON_REFRESH_PENDING)
        if sample.recovery_condition:
            reasons.append(MINING_READINESS_REASON_RECOVERY_WINDOW_PENDING)
        candidate_age = sample.oldest_durable_candidate_age_seconds
        if (
            candidate_age is not None
            and candidate_age >= MINING_READINESS_OLD_CANDIDATE_AGE_SECONDS
        ):
            reasons.append(MINING_READINESS_REASON_DURABLE_CANDIDATE_OLD)
        if timeout_rate > 0.0:
            reasons.append(MINING_READINESS_REASON_PREVIEW_TIMEOUTS)
        return tuple(reasons)


def overlay_progress_health(
    base_health: Mapping[str, object],
    progress: Mapping[str, object],
) -> Mapping[str, object]:
    """Overlay current progress without masking an independent base failure."""

    base_ok = bool(base_health.get("mining_ready", base_health.get("ok")))
    result = dict(base_health)
    result.update(progress)
    result["ok"] = base_ok and bool(progress["ok"])
    if progress["ok"]:
        result.pop("reason", None)
        result.pop("reasons", None)
    return MappingProxyType(result)


__all__ = [
    "BundleBuildToken",
    "DEFAULT_PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS",
    "DEFAULT_PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS",
    "DeliveryProof",
    "EligibilitySnapshot",
    "MINING_READINESS_ENTRY_COVERAGE_RATIO",
    "MINING_READINESS_OLD_CANDIDATE_AGE_SECONDS",
    "MINING_READINESS_REASONS",
    "MINING_READINESS_REASON_DURABLE_CANDIDATE_OLD",
    "MINING_READINESS_REASON_PREVIEW_TIMEOUTS",
    "MINING_READINESS_REASON_RECOVERY_WINDOW_PENDING",
    "MINING_READINESS_REASON_REFRESH_PENDING",
    "MINING_READINESS_REASON_REFRESH_PENDING_TOO_LONG",
    "MINING_READINESS_REASON_SEMANTIC_COVERAGE_LOW",
    "MINING_READINESS_REASON_WARMING_UP",
    "MINING_READINESS_RECOVERY_COVERAGE_RATIO",
    "MINING_READINESS_SCHEMA",
    "MINING_READINESS_STATES",
    "MINING_READINESS_STATE_DEGRADED",
    "MINING_READINESS_STATE_READY",
    "MiningReadinessConfig",
    "MiningReadinessSample",
    "MiningReadinessSnapshot",
    "MiningReadinessTracker",
    "PROGRESS_HEALTH_REASONS",
    "ProgressHealthConfig",
    "ProgressHealthService",
    "ProgressHealthSnapshot",
    "RefreshActivityToken",
    "WorkGeneration",
    "overlay_progress_health",
]
