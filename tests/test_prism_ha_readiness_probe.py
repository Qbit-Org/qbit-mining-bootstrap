import unittest

from scripts.prism_ha_readiness_probe import ProbeConfig, ReadinessProbe, run, run_timeline


OK = {"status": 200, "ok": True, "schema": "qbit.prism.audit-health.v1"}
BAD = {"status": 503, "ok": False}


class ReadinessProbeTests(unittest.TestCase):
    def test_initial_unknown_and_two_success_rise(self):
        self.assertEqual(run([(OK, 0), (OK, 0)]), ["unknown", "up"])

    def test_contract_rejects_false_timeout_unreachable_and_malformed(self):
        p = ReadinessProbe()
        for response, elapsed in [(BAD, 0), (OK, 1.01), (None, 0), ({"status": 200}, 0)]:
            self.assertEqual(p.observe(response, elapsed), "unknown")
        self.assertEqual(p.failures, 4)

    def test_six_failures_eject_and_two_successes_recover(self):
        p = ReadinessProbe()
        for _ in range(2):
            p.observe(OK)
        self.assertEqual(p.state, "up")
        self.assertEqual(run([(BAD, 0)] * 5, ProbeConfig()), ["unknown"] * 5)  # initial state is explicit
        for _ in range(6):
            p.observe(BAD)
        self.assertEqual(p.state, "down")
        p.observe(OK)
        self.assertEqual(p.state, "down")
        p.observe(OK)
        self.assertEqual(p.state, "up")

    def test_timing_bound_and_monotonic_schedule(self):
        events = [(float(i * 2), BAD, 1.0) for i in range(6)]
        self.assertEqual(run_timeline(events), ["unknown"] * 5 + ["down"])
        self.assertLessEqual(events[-1][0] + 1.0, 13.0)
        with self.assertRaises(ValueError):
            run_timeline([(0.0, BAD, 0), (1.0, BAD, 0)])

    def test_transient_rebuild_does_not_eject(self):
        p = ReadinessProbe()
        p.observe(OK); p.observe(OK)
        for _ in range(5):
            p.observe(BAD)
        self.assertEqual(p.state, "up")

    def test_nondefaults_and_invalid_config(self):
        self.assertEqual(run([(BAD, 0)] * 3 + [(OK, 0)] * 3, ProbeConfig(fall=3, rise=3)), ["unknown", "unknown", "down", "down", "down", "up"])
        for kwargs in ({"interval_s": 0}, {"timeout_s": 0}, {"fall": 0}, {"rise": 0}):
            with self.assertRaises(ValueError):
                ProbeConfig(**kwargs)


if __name__ == "__main__":
    unittest.main()
