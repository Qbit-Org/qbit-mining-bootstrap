from __future__ import annotations

from types import SimpleNamespace
import unittest

from lab.prism.metrics import MetricsRenderer


class MetricsRendererTests(unittest.TestCase):
    def test_shutdown_formatter_consumes_one_owner_snapshot(self) -> None:
        snapshot_calls = 0

        def snapshot() -> dict[str, object]:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return {
                "shutdowns_total": 1,
                "writer_quiescence_outcomes": {"success": 1, "timeout": 0},
                "lease_release_outcomes": {
                    "success": 1,
                    "not_held": 0,
                    "unsupported": 0,
                    "failure": 0,
                },
                "active_writers": {"candidate": 2},
                "writer_quiescence_seconds": 0.25,
                "lease_release_attempts_total": 1,
                "lease_release_seconds": 0.125,
                "sigterm_release_observed": True,
                "sigterm_to_lease_release_seconds": 0.5,
                "release_withheld_total": 0,
                "non_writer_drain_seconds": 0.75,
            }

        port = SimpleNamespace(
            _ensure_shutdown_controller=lambda: SimpleNamespace(
                snapshot=snapshot
            ),
            prometheus_label_value=lambda value: value,
        )
        lines = MetricsRenderer(port).shutdown_metrics_lines()  # type: ignore[arg-type]

        self.assertEqual(snapshot_calls, 1)
        self.assertIn("qbit_prism_shutdowns_total 1", lines)
        self.assertIn(
            'qbit_prism_shutdown_writer_operations{component="candidate"} 2',
            lines,
        )
        self.assertIn(
            "qbit_prism_shutdown_sigterm_to_lease_release_seconds 0.500000",
            lines,
        )


if __name__ == "__main__":
    unittest.main()
