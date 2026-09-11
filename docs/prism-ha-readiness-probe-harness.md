# HA readiness probe harness

This standalone simulator qualifies the external hashrouter contract in
`prism-ha-reference-architecture.md` without network access, a load balancer,
Compose services, or production traffic. The defaults are the documented
policy: 2-second monotonic probe cadence, 1-second outer timeout, six
consecutive failures to mark a frontend down, and two consecutive successes to
allow new sessions again. Initial state is explicitly `unknown`; failures,
timeouts, unreachable endpoints, malformed responses, non-200 status, and
`ok != true` never count as healthy.

`tests/test_prism_ha_readiness_probe.py` provides positive and negative
controls, including transient rebuild preservation, bounded ejection, recovery
gating, timeout handling, and validation of non-default thresholds. It is a
deterministic state-machine qualification aid, not evidence of real routing or
500k-share performance.
