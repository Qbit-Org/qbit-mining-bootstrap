# Issue #279 implementation verification

Workstream B, Dan (`djh58`); verified 2026-09-11 on branch
`djh58/prism-b7-alert-migration`, based on `5355b69462663e9b9e666589147f74e7aeb268dc`
(#308), which matched freshly fetched `origin/3.x.x` at task start.

## Acceptance results

| Acceptance | Result | Evidence |
| --- | --- | --- |
| `cargo +1.89.0 fmt --all --check` | Executed and passed | Final formatter check clean |
| `git diff --check` and staged diff check | Executed and passed | No whitespace errors, including the patch artifact |
| Inventory, both directions, both roles, ungated | Executed and passed | `inventory_scrapes_both_roles_in_both_directions`: real loopback HTTP listeners for the production role routers; coordinator startup and published states, public replica off and require; no PostgreSQL prerequisite or skip |
| Registry-generated inventory | Executed and passed | `inventory_is_generated_from_registry`; 38 coordinator + 14 public = 52 families |
| Rules versus inventory grep | Executed and passed | `native_rules_reference_only_inventory_families_for_their_role`; histogram suffixes resolve only to declared histogram families; public request counter and deferred first-offer/lock timing are rejected |
| Complete migration coverage | Executed and passed | All 46 historical metric tokens and 78 deployed definitions: 22 migrated, 22 no replacement, 34 external rules preserved |
| Entry-point documentation | Executed and passed | `PRISM.md`, crate README and the replacement-series section of `prism-rust-migration.md` updated; historical appendix links to the new contract |
| Deployment diff applies cleanly | Executed and passed | `git apply --check` and actual application in a disposable copy of the snapshot path; applied bytes equal generator output; original snapshot checksum unchanged |

The required Cargo command was executed with
`CARGO_TARGET_DIR=/tmp/prism-b7-target`:

```sh
cargo +1.89.0 test --locked -p qbit-prism-server --test observability -- --nocapture
```

All **12** observability tests passed, none ignored or skipped. The existing
`rust-tests` CI job runs `cargo test --locked --workspace --all-targets`, so these
tests execute without new environment gates. The existing collector/runtime
tests also verify real zero versus failed observations and blocked runtime HTTP
behavior. No production metric producer, name, label or version was changed.

## Review corrections and consumer verification

This report supersedes the original `3f4caa9` D3 verification: the first draft's
NULL replay-lag contract produced a false alert on a healthy idle standby.
The [review ledger](prism-alert-review-ledger.json) retains the findings, sources,
local commits and incomplete review lanes without claiming publication.

| Finding | Local correction | Evidence |
| --- | --- | --- |
| Healthy idle NULL replay latency alerted; retained latency did not describe current backlog | `8e5e8d8`: use exact high/low durable WAL positions observed five seconds apart; retain explicit unknown/error states | Exact query tested on disposable PostgreSQL 16.14 primary plus one async standby; idle, paused replay, dwell, recovery and disconnect all pass |
| Deployed network-wide floor of ten clients became ten per instance | `b85a137`: sum only a complete set of fresh coordinator observations | Regression failed before correction; six plus six is healthy, six plus three alerts, partial stale data is unknown |
| Disabled gates omitted reused legacy UIDs without deleting persisted rules | `d1d6c81`: conditionally delete every disabled native UID | Regression failed before correction; ten rendered gate combinations pass, including all disabled and each gate toggled |
| Synchronous standby configuration could satisfy the async D3 health contract | `2bf64f2`: expose explicit async topology status and reject missing/stale/invalid values | Four synthetic regressions failed before correction; real SQL tests cover a selected synchronous standby, a primary waiting for another standby, and restoration to async |

Executed and passed on corrective implementation `2bf64f2fbf6861320e05e62e07a9c741abd5953e`:

- Both generators with `--check`, including the exact frozen deployment snapshot
  and specification digest; `cargo +1.89.0 fmt --all --check` and whitespace checks.
- All 12 observability tests, covering both-role/both-direction inventory, rule
  family references, migration completeness and real failed/zero collector states.
- Prometheus `promtool` 3.14.0 parsed all **27** authored expressions plus the two
  D3 alert conditions used to test dwell. The retained synthetic fixtures passed
  **38 scenarios / 86 predicate assertions**, plus **12 alert-state assertions**.
  They cover native stale/unknown/zero signals, the network-wide client floor,
  partial scrape loss, D3 failed/missing exporter and query status, stale history,
  exact five-second boundaries, 64-bit LSN precision and low-word rollover.
- The exact SQL from `prism-postgres-exporter-queries.yaml` ran against two
  disposable PostgreSQL **16.14** clusters with one dedicated **asynchronous**
  standby. The resulting samples passed **9 PromQL scenarios / 18 predicate
  assertions**, plus **12 alert-state assertions**. Idle caught-up SQL returned
  zero only with equal WAL positions while raw `replay_lag` was NULL; paused
  replay exceeded the allowance and one-minute dwell; recovery cleared with raw
  latency still **62.843997 seconds**; disconnect returned count zero and unknown
  positions/latency. Synchronous configuration of this standby or another
  required standby returned explicit async status zero while preserving valid
  WAL positions; restoring the asynchronous topology cleared the predicate.
  Running the primary-only query on the standby failed as
  expected. Both clusters were stopped and removed. The harness records real SQL
  samples at nominal one-second cadence; exporter status inputs are controlled
  by the harness, not a live postgres_exporter.
- The retained provisioning check applies the unified patch in a disposable
  copy, compares resulting bytes to the generator, renders with Jinja2 and the
  unchanged shared macro, then parses the provisioning YAML with PyYAML. All
  **10** gate combinations passed. All enabled: **61** alerts (25 native/public,
  34 external, two D3) and **27** retired UID deletions. Additional disabled native
  UIDs are deleted conditionally; all disabled: two mandatory D3 alerts and 52
  deletions. No duplicate UID, create/delete overlap, or altered external rule
  dictionary was observed. Original snapshot bytes remain unchanged.

Reproduce the added consumer checks:

```sh
python3 scripts/test_prism_alert_promql.py --promtool /path/to/promtool
PRISM_TEST_PG_BIN_DIR=/path/to/postgresql/bin python3 scripts/test_prism_postgres_alerts.py --promtool /path/to/promtool
python3 scripts/test_prism_alert_deployment.py --snapshot /path/to/read-only-snapshot
```

The SQL/provisioning scripts require PyYAML; provisioning also requires Jinja2.
The static PromQL runner only needs Python and promtool. The inventory test
remains ungated in the existing Rust CI job. Provisioning deletion is validated
at the rendered contract level against Grafana's documented
[`deleteRules` mechanism](https://grafana.com/docs/grafana/latest/alerting/set-up/provision-alerting-resources/file-provisioning/);
no running Grafana instance was used.

## Review lane status

| Lane | Evidence and state |
| --- | --- |
| Local Codex CLI review, medium effort | Executed on `5355b69..3f4caa9` with gpt-6-astra; no actionable findings; this lane checked generators but did not rerun Cargo/PromQL |
| Local adversarial Codex | Executed on the original diff with gpt-5.6-sol; reported the idle-standby and client-floor findings above |
| Local corrective-diff adversarial review | Executed with gpt-5.6-sol on `3f4caa9..8e5e8d8`; independently reported gate deletion and synchronous-topology findings; both have local corrections and regression evidence |
| Independent Fable adversarial | Requested from coordinator; not executed locally; result pending |
| Thermo quality lane | Installed skill read at coordinator-provided path; independent execution pending coordinator routing |
| GitHub CI, Tenki and PR review bots | Not executed: branch unpushed and no PR opened |

The main checkout's `AGENTS.md` was sought; the coordinator confirmed it is
absent. Review used the supplied session instructions and engineering-practices
EP-COMPAT, EP-OBSERVABILITY and EP-ERRORS. The local medium CLI review is recorded
as the actual command lane, not an unavailable `/code-review` skill invocation.
No unavailable independent lane is counted as complete.

## Remaining coordinator and deployment work

The coordinator must review the four findings, their local corrections and the
final diff (including the final async correction, which has not received a
further independent review); obtain or explicitly resolve the pending independent Fable/thermo
lanes; then authorize publishing and PR creation. No push, PR, production change,
or qbit-tools modification was performed. Issue #311 was left untouched.

Not executed: production rendering with actual Ansible variables, live Grafana
provisioning/deletion, an actual postgres_exporter scrape/failure test, production
HA failover, qbitd integration, the **400k regression / 500k headroom** load checks,
or the #291 soak. Those capacity targets are preserved as qualification gates;
this documentation/test correction supplies no new load-capacity evidence.

The D3 correction retains the approved five-second allowance and one-minute dwell
for one primary plus one asynchronous dedicated HA standby, separate from the
public read replica. It requires primary exporter query metrics, one-second
scrapes and standby feedback, exact WAL components, and explicit failed/missing/
stale status; #281/#291 must verify permissions, collector cadence/cost, the
chosen exporter version and its real error behavior. ACKs still require local
durability without waiting for standby replay. Runtime metric producers,
VERSION and CHANGELOG were not changed.

Native numeric thresholds without measurements remain **provisional, measure in
#291**; proof-to-first-offer and advisory-lock rules remain deferred to A/#266
and #283. These are recorded integration/qualification dependencies, not
completed production checks or an authorization to deploy.
