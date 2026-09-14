# HA readiness probe harness

This standalone simulator exercises the documented operator-supplied Stratum
TCP load-balancer readiness contract without network access, deployment
services, or production traffic. The defaults are the documented policy:
2-second monotonic probe cadence, 1-second outer timeout, six consecutive
failures to mark a frontend down, and two consecutive successes to allow new
sessions again. Initial state is explicitly `unknown`; failures, timeouts,
unreachable endpoints, malformed responses, non-200 status, and `ok != true`
never count as healthy.

`tests/test_prism_ha_readiness_probe.py` provides positive and negative
controls, including transient rebuild preservation, bounded ejection, recovery
gating, timeout handling, and validation of non-default thresholds. It is a
deterministic health-check and hysteresis simulator, not an integration,
deployment, live load-balancer, or performance qualification.
