# Validation Strategy

Validation follows risk and invalidation scope. Focused checks run per slice;
broad checks run at cumulative milestones and once more on the final tree.

## Per slice

Run:

```sh
python3 -m py_compile <changed Python modules and tests>
python3 -m unittest <direct owner tests> <tight coordinator regressions>
git diff --check
```

Inspect the complete slice diff and confirm no leaf module imports
`lab.prism.prism_coordinator`. Add PostgreSQL, Rust, or process tests immediately
when the slice changes those boundaries; do not defer a directly relevant
failure to a milestone.

## Cumulative PRISM regression

Use the current shard names discovered in `tests/`. At minimum include the
candidate, vardiff, retained-job, share-writer, payout, job-builder, metrics,
tip-refresh, publication-boundary, initial/reconnect, hot-path, shutdown, CTV,
progress-health, audit, and public-API suites. The exact module list may grow as
new direct owner suites are added.

## Full Python discovery

Full discovery includes fixtures that create temporary Git repositories, so
scope the global-config override to this command only:

```sh
GIT_CONFIG_GLOBAL=/dev/null \
  python3 -m unittest discover -s tests -p 'test_*.py'
```

Never export `GIT_CONFIG_GLOBAL` and never use it for repository commits.

## PostgreSQL, Rust, and live validation

Run when a milestone touches persistence, payout, shares, audit, candidates,
vardiff, or finalization:

```sh
make test-prism-postgres-ledger
cargo test -p qbit-prism
```

At the concurrency/finalization and final milestones, run:

```sh
make test-prism-stratum-regtest-live
make test-prism-stratum-postgres-regtest-live
```

If qbitd, qbit-cli, PostgreSQL, Docker, Rust, credentials, or another real
prerequisite is unavailable, record `UNAVAILABLE`, the missing prerequisite,
and the exact unrun command. Do not call it a pass.

## Docker compile, lint, and build

Lint only in Docker:

```sh
docker run --rm \
  -e PYTHONPYCACHEPREFIX=/tmp/pycache \
  -v "$PWD:/work:ro" -w /work \
  python:3.12-slim \
  python -m compileall -q docker lab tests examples scripts

docker run --rm \
  -v "$PWD:/work" -w /work \
  ghcr.io/astral-sh/ruff:0.12.5 \
  check --select E4,E7,E9,F lab/prism tests

docker build -f lab/prism/Dockerfile -t qbit-prism-refactor-check .
```

Focused slices may lint only their changed Python paths. Milestones lint the
complete PRISM paths. The image build is required after B2 and on the final
tree.

## Final structural and diff audit

Before declaring completion:

- no leaf service imports the coordinator;
- mutable domain state has one owner and no drifting scalar mirror;
- coordinator worker loops and large domain state machines have moved;
- compatibility re-exports/delegates have a demonstrated caller or are gone;
- no blocking I/O occurs under coordinator/session locks;
- queues, executors, cancellation, and shutdown joins are bounded;
- public schemas, metrics, environment defaults, and wire payloads are stable;
- `git diff --check`, status, target-branch diff, and generated-artifact hygiene
  are clean;
- cumulative regression, full discovery, PostgreSQL/Rust, Docker lint/build,
  and live-or-unavailable evidence all reflect the final tree.

## Final evidence

Recorded on 2026-07-22 after X1, integration through `origin/1.x.x` at
`b002caa`, stack reconstruction, upstream hot-path reconciliation, and a
thread-aware review audit showing no unresolved comments:

| Check | Result |
| --- | --- |
| PRISM Python discovery | 1,251 passed |
| full Python discovery | 1,585 passed; `GIT_CONFIG_GLOBAL=/dev/null` was scoped only to this command |
| PostgreSQL ledger integration | `UNAVAILABLE`; `make test-prism-postgres-ledger` could not reach the stopped OrbStack Docker daemon |
| Rust `qbit-prism` | 182 passed |
| Docker Python compile | `UNAVAILABLE`; Docker daemon stopped |
| Docker Ruff | `UNAVAILABLE`; Docker daemon stopped; lint was not run outside Docker |
| PRISM image | `UNAVAILABLE`; Docker daemon stopped |
| Compose | PRISM target and permissionless, real-miner-smoke, auxpow, and prism profiles passed configuration validation |
| structural and target diff audit | passed; see [Structure](structure.md) |

`make test-prism-stratum-regtest-live` and
`make test-prism-stratum-postgres-regtest-live` are `UNAVAILABLE`: their doctor
reported that the Docker daemon is not reachable before either live test could
run. `qbitd` and `qbit-cli` are also absent from the host path.
