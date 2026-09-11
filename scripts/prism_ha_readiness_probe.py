#!/usr/bin/env python3
"""Deterministic simulator for the documented external HA readiness probe.

This is qualification evidence only; it performs no network I/O or routing.
"""
from dataclasses import dataclass
import json
from typing import Any


@dataclass(frozen=True)
class ProbeConfig:
    interval_s: float = 2.0
    timeout_s: float = 1.0
    fall: int = 6
    rise: int = 2

    def __post_init__(self) -> None:
        if self.interval_s <= 0 or self.timeout_s <= 0:
            raise ValueError("probe interval and timeout must be positive")
        if self.fall < 1 or self.rise < 1:
            raise ValueError("fall and rise thresholds must be positive")


class ReadinessProbe:
    """Monotonic hysteresis state machine: unknown, up, or down."""

    def __init__(self, config: ProbeConfig = ProbeConfig()) -> None:
        self.config = config
        self.state = "unknown"
        self.failures = 0
        self.successes = 0

    @staticmethod
    def classify(response: Any, elapsed_s: float, timeout_s: float) -> bool:
        if elapsed_s > timeout_s or not isinstance(response, dict):
            return False
        return response.get("status") == 200 and response.get("ok") is True

    def observe(self, response: Any, elapsed_s: float = 0.0) -> str:
        good = self.classify(response, elapsed_s, self.config.timeout_s)
        if good:
            self.failures = 0
            self.successes += 1
            if self.state in ("unknown", "down") and self.successes >= self.config.rise:
                self.state = "up"
        else:
            self.successes = 0
            self.failures += 1
            if self.failures >= self.config.fall:
                self.state = "down"
        return self.state


def run(sequence: list[tuple[Any, float]], config: ProbeConfig = ProbeConfig()) -> list[str]:
    probe = ReadinessProbe(config)
    return [probe.observe(response, elapsed) for response, elapsed in sequence]


def run_timeline(
    sequence: list[tuple[float, Any, float]], config: ProbeConfig = ProbeConfig()
) -> list[str]:
    """Run starts on a monotonic schedule; rejects overlapping/early probes."""
    probe = ReadinessProbe(config)
    previous = None
    states = []
    for started_at, response, elapsed in sequence:
        if previous is not None and started_at - previous < config.interval_s:
            raise ValueError("probe starts must honor the configured monotonic interval")
        previous = started_at
        states.append(probe.observe(response, elapsed))
    return states


def main() -> None:
    # JSON-lines adapter for reproducible qualification, intentionally no I/O
    # beyond stdin/stdout and no claim of real load-balancer behavior.
    config = ProbeConfig()
    for line in __import__("sys").stdin:
        item = json.loads(line)
        print(json.dumps({"state": ReadinessProbe(config).observe(item["response"], item.get("elapsed_s", 0.0))}))


if __name__ == "__main__":
    main()
