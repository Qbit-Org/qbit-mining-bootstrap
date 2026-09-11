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

## Additional consumer verification

- Both generators passed `--check`; the alert generator checked the exact
  deployment snapshot and patch, including the specification digest.
- Prometheus `promtool` **3.14.0** parsed all **27** authored expressions (25
  native/public rules and two external D3 rules). The retained scenario runner,
  `scripts/test_prism_alert_promql.py`, passed **17 scenarios / 47 assertions**:
  successful zeros, negative unknowns, stale-body suppression, runtime evidence
  during publisher staleness, native pressure, missing series, sampler startup,
  partial public-instance scrape loss, and D3 healthy/boundary/lag/disconnection/
  query-failure/missing-exporter cases.
- Jinja rendering with synthetic deployment variables and all flags enabled
  produced valid Grafana provisioning YAML: **61** alerts (25 native/public,
  34 external, two D3) and **27** `deleteRules` entries, with no duplicate UIDs
  or simultaneous creation/deletion. All 34 external rule dictionaries matched
  the original rendered definitions exactly. Disabling the native/external gates
  still rendered a valid separate, mandatory D3 group. The synthetic variables
  are render fixtures, not a record of live deployment settings.

## Remaining operator and qualification work

Not executed: production rendering with actual Ansible variables, qbit-tools
application/push, deployment, a live postgres_exporter scrape, a PostgreSQL HA
drill, qbitd integration, load measurement, or the #291 soak. This task changes
documentation/specifications/test harnesses; it neither provisions an exporter
nor requires a database to verify inventory coverage.

The coordinator approved D3's database-side contract: primary exporter query
metrics `pg_stat_replication_replay_lag{application_name}` and
`pg_stat_replication_count{application_name}`, plus `pg_up`, with missing and
negative data firing. The exact query extension is retained in
`prism-postgres-exporter-queries.yaml` and included in the rules-file patch.
The two D3 rules use the approved 5-second / 1-minute and disconnected / 1-minute
bounds; #281/#291 must verify instrumentation on the dedicated HA primary,
separate from the public read replica. Stock exporter WAL byte-lag metrics are
not a substitute for seconds.

Native numeric thresholds without measured behavior remain explicitly
**provisional, measure in #291**. First-offer and advisory-lock rules remain
deferred to A/#266 and #283. There are no unanswered coordinator decisions;
threshold qualification and exporter provisioning are recorded dependencies,
not claims of completed production validation. The branch is for coordinator
review and integration only: no push or pull request was performed.
