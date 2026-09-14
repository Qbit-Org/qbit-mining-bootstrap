"""Bounded decision attribution; never a source of window validity."""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

REUSE_REASONS = (
    "disabled",
    "absent",
    "payout_generation",
    "difficulty",
    "append_epoch",
    "anchor_missing",
    "audit_ceiling",
    "published_unavailable",
    "publication_race",
    "artifact_replaced",
    "balances",
    "not_probed",
    "unknown",
)
EVENT_REASONS = {
    "spawn": ("started", "unknown"),
    "retire": (
        "shutdown",
        "exited",
        "cancelled",
        "timeout",
        "eof",
        "io",
        "protocol",
        "request_error",
        "partial_request",
        "handshake",
        "unknown",
    ),
    "prepare_full": (
        "prepared",
        "busy",
        "unavailable",
        "fold_invalid",
        "out_of_range",
        "unknown",
    ),
    "prepare_advance": (
        "prepared",
        "busy",
        "unavailable",
        "fallback",
        "out_of_range",
        "evicted_by_build",
        "evicted_by_prepare",
        "uploaded_only",
        "not_held",
        "unknown",
    ),
    "ready_snapshot": REUSE_REASONS,
}


@dataclass
class ArtifactReuseProbe:
    reason: str = "not_probed"


_reuse_probe: ContextVar[ArtifactReuseProbe | None] = ContextVar(
    "window_reuse_probe", default=None
)


@contextmanager
def capture_artifact_reuse():
    """Carry this request's refusal through admission, without shared state."""
    probe = ArtifactReuseProbe()
    token = _reuse_probe.set(probe)
    try:
        yield probe
    finally:
        _reuse_probe.reset(token)


def refuse_artifact_reuse(reason: str) -> None:
    probe = _reuse_probe.get()
    if probe is not None:
        probe.reason = reason if reason in REUSE_REASONS else "unknown"


class WindowLifecycleTelemetry:
    """Closed counters, a 64-event metadata ring, and rate-limited logs.

    Digests/epochs/PIDs are scalar diagnostic fields, never metric labels.
    Each event/reason logs at most once per 30 seconds. Counters include all
    events; suppressed logs are explicitly counted. No payload or traceback
    is retained. The ring is useful in isolated replays, not a heap census.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.counts = {
            (event, reason): 0
            for event, reasons in EVENT_REASONS.items()
            for reason in reasons
        }
        self._last_log = {}
        self._events = deque(maxlen=64)
        self.suppressed_logs = 0
        self._sequence = 0

    def note(self, event: str, reason: str, **fields):
        if event not in EVENT_REASONS:
            return
        if reason not in EVENT_REASONS[event]:
            reason = "unknown"
        # Only bounded scalar metadata is accepted, even from an old daemon.
        allowed = {
            "daemon_generation",
            "pid",
            "returncode",
            "base_digest",
            "digest",
            "anchor_ms",
            "append_epoch",
            "payout_generation",
            "template_generation",
            "rejection",
            "base_anchor_ms",
            "base_window_weight",
            "base_epoch",
        }
        metadata = {
            key: value[:64] if isinstance(value, str) else value
            for key, value in fields.items()
            if key in allowed and (value is None or type(value) in (str, int, bool))
        }
        now = time.monotonic()
        with self._lock:
            key = (event, reason)
            self.counts[key] += 1
            self._sequence += 1
            entry = dict(
                event="window_lifecycle",
                operation=event,
                reason=reason,
                sequence=self._sequence,
                **metadata,
            )
            self._events.append(entry)
            emit = now - self._last_log.get(key, float("-inf")) >= 30.0
            if emit:
                self._last_log[key] = now
            else:
                self.suppressed_logs += 1
        if emit:
            print("prism coordinator: " + json.dumps(entry, sort_keys=True), flush=True)

    def snapshot(self):
        with self._lock:
            return dict(self.counts), list(self._events), self.suppressed_logs

    def metrics_lines(self):
        counts, _, suppressed = self.snapshot()
        return [
            "# HELP qbit_prism_window_lifecycle_total Window preparation, process retirement and ready snapshot decisions (not crash attribution).",
            "# TYPE qbit_prism_window_lifecycle_total counter",
            *[
                f'qbit_prism_window_lifecycle_total{{event="{event}",reason="{reason}"}} {count}'
                for (event, reason), count in counts.items()
            ],
            "# HELP qbit_prism_window_lifecycle_logs_suppressed_total Rate-limited window decision log records; event counters remain complete.",
            "# TYPE qbit_prism_window_lifecycle_logs_suppressed_total counter",
            f"qbit_prism_window_lifecycle_logs_suppressed_total {suppressed}",
        ]
