# sqlx-core 0.8.6 with launchbadge/sqlx#4251 (#771)

This is sqlx-core 0.8.6 as published on crates.io: the `.crate` whose
SHA-256 is `ee6798b1838b6a0f69c007c133b8df5866302197e404e8b6ee8ed3e3a5e68dc6`,
built from upstream commit `bab1b022bd56a64f9a08b46b36b97c5cff19d77e`
(`.cargo_vcs_info.json`). It is licensed MIT OR Apache-2.0; see
`LICENSE-MIT` and `LICENSE-APACHE`.

One upstream fix is applied, and nothing else changes:
[launchbadge/sqlx#4251](https://github.com/launchbadge/sqlx/pull/4251),
"fix(tls): potential deadlock in `StdSocket::poll_ready()`" (merge commit
`c0ec9c0b5042767cd30f569874e02dfb3d92668b`, first released in sqlx 0.9.0;
no 0.8.x release has it). It changes `src/net/tls/util.rs` only. When
rustls's `complete_io` has found both its write and its read blocked,
`poll_ready` now waits on both directions and returns when either is ready.
0.8.6 waited for write readiness and then for read readiness, so a large
statement over TLS could park on read readiness while the server waited for
the rest of the statement.

The diff applies as upstream wrote it. Its first hunk's context differs only
because 0.8.6 imports `future` from `futures_util` rather than `std`, and
that import stays. The registry's `.cargo-ok` marker is not copied, and this
README and `reviewed-advisories.toml` are added. Diffing this directory
against the extracted crate shows nothing else.

The root `Cargo.toml` and `crates/qbit-prism-server/fuzz/Cargo.toml` patch
sqlx-core to this directory, and `lab/prism/Dockerfile` copies it into the
image build. `crates/qbit-prism-server/tests/sqlx_tls_readiness.rs` stalls
against unpatched 0.8.6.

`scripts/check_vendored_sqlx_core.py` guards both in CI:

- **The patch.** sqlx pins its sqlx-core with `=`, so moving sqlx to another
  version leaves the patch unused, and Cargo only warns. The script fails
  unless both lock files build this copy at this version.
- **Advisories.** cargo-deny matches RustSec advisories only against
  registry crates, so it no longer checks sqlx-core. Right after
  `cargo deny check`, the script lists every advisory the database it fetched
  holds for sqlx-core. It fails on any advisory that is not withdrawn and
  that `reviewed-advisories.toml` does not name.

When an advisory appears, check it against this copy and fix or track it.
Then add its ID to `reviewed-advisories.toml` with a one-line reason.

Cargo builds a path dependency as local code, so the crate's own rustc
warnings (three `mismatched_lifetime_syntaxes` on Rust 1.98) show in builds.
They are left as upstream shipped them.

When sqlx moves to a release that carries the fix, remove these together:
- this directory;
- both `[patch.crates-io]` entries;
- the Dockerfile `COPY`;
- the script, its tests and its two CI steps.

Keep the regression test, with its sqlx-core dev-dependency moved to the new
version.
