#!/usr/bin/env python3
"""Monotonic mining-progress readiness state for the PRISM coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
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
    "DeliveryProof",
    "EligibilitySnapshot",
    "PROGRESS_HEALTH_REASONS",
    "ProgressHealthConfig",
    "ProgressHealthService",
    "ProgressHealthSnapshot",
    "RefreshActivityToken",
    "WorkGeneration",
    "overlay_progress_health",
]
