# qbit-mining-bootstrap 3.x.x development line

Release date: none. This is a development line, not a release.

## Status

The `3.x.x` branch is not production-ready. The production line is `2.x.x`;
its tip is version 2.0.2 (see `release-notes-2.0.2.md`, whose release date is still pending). Nothing on this
branch has been through a production release, and no production deployment
should run it or point a production database at it.

This line exists so that three engineers can work in parallel on the native
Rust PRISM server without sharing one open pull request. Work is split into
workstreams A, B, and C as decided in #260 and tracked in #263. The README
carries a banner with the same statement.

## What landed

- PR #244, merged at `1398bbc1`: the multi-instance Rust PRISM server
  (`qbit-prism-server`) replaces the Python coordinator runtime under
  `lab/prism/`. Every instance coordinates through PostgreSQL; only the
  canonical share order and each job's accounting boundary are serialized.
- PR #296: borrowed audit body build and verify entry points in `qbit-prism`.
- The integration of the 2.x.x tip v2.0.2 (`504846cc`, PR #258) into this
  line, per #283. It brings:
  - the ckpool version-mask probe (#257), with the settings
    `CKPOOL_VERSION_MASK_PROBE_ATTEMPTS` and
    `CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS` in `.env.example` and
    `compose.yaml`, read by `docker/ckpool/ckpool-version-mask.py` and
    `docker/ckpool/start-ckpool.sh`;
  - the CI push-branch restriction (#295): push builds run only on `main`,
    `1.x.x`, `2.x.x`, and `3.x.x`, while the sharded lint, compile, and test
    layout from #243 is kept;
  - the 2.0.0 and 2.0.2 release notes, as 2.x.x history;
  - the schema file `crates/qbit-prism/sql/002_candidate_bodies.sql`, carried
    byte-identical to v2.0.2 and digest-pinned by the upgrade tests (#285).
    The Rust migrator enumerates its SQL files explicitly and applies
    `001_share_ledger.sql` plus its own migrations; it never applies
    `002_candidate_bodies.sql`. It accepts a #258 source after the drain
    check, keeping the 002 objects and the capability row, refuses a
    partial 001 (a leftover 001 object without `qbit_share_ledger`), a
    partial 002 or a newer source before any DDL, and after 001 has run
    compares what its `IF NOT EXISTS` left alone, sequences included,
    against a scratch apply of the release SQL, refusing a drifted
    definition transactionally; see `docs/prism-rust-migration.md`.
- `crates/qbit-prism-server/src/ledger.rs` is split into ownership
  submodules, `ledger/candidates.rs`, `ledger/jobs.rs`, `ledger/window.rs`,
  and `ledger/connect.rs`, with no behaviour change and no signature change,
  so parallel workstream pull requests stop colliding on one file.
- One release identity, `3.0.0`, per #284. The single source is
  `[workspace.package].version` in the root `Cargo.toml`, which every crate
  inherits with `version.workspace = true`, plus the `VERSION` file; bump the
  two together. `scripts/check_version_skew.py` (also `make
  check-version-skew`) fails when `VERSION` differs from the version Cargo
  resolves for any workspace crate, and CI runs it in the Rust tests job that
  the merge gate requires.

The following 2.x.x changes were deliberately not carried, because the code
they fixed or configured no longer exists on this line:

- The 2.x.x Python coordinator modules under `lab/prism/` and the tests that
  import them. PRs #245, #248, #250, #252, #253, #256, #259, and #292 fixed
  that runtime; their commits are ancestors of this line, but their Python
  files are removed. Only `lab/prism/Dockerfile` remains.
- The `PRISM_BLOCK_REPLAY_PAGE_SIZE` knob. Its only reader was the Python
  coordinator configuration, and the native lane claims one candidate at a
  time.
- The `PRISM_CANDIDATE_STORAGE_VERSION`, `PRISM_CANDIDATE_SPOOL_DIR`, and
  `PRISM_CANDIDATE_SPOOL_RESERVATION_BYTES` settings. No file on this line
  reads them.
- The Python offline pending-block recovery command
  (`lab.prism.recover_pending_blocks`) and its runbook steps.
- `docs/prism-candidate-storage.md`, which documented the Python candidate
  storage lane end to end.

## Not done yet

- There is no production release of the Rust server. The Postgres-backed and
  live-regtest Rust suites need a PostgreSQL server and a `qbitd` binary; see
  `test/prism-native-tests.sh`. They are not part of this note's verification.
- Real release notes are #291. The tree reads 3.0.0, but nothing on this
  line has been released under that version; `doc/release-notes-3.0.0.md` and
  the rollout notes are written there.
- Native candidate recovery is #268.

## Upgrade and rollback

None is supported. Do not point a production database at this line. The Rust
migrator refuses to start against a live legacy Python writer lease and
against an undrained legacy block outbox, but that is a guard against
accidental cutover, not a supported migration path. There is no rollback
procedure from a database this line has migrated.
