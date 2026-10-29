# sqlx-core 0.8.6 with launchbadge/sqlx#4251 (#771) and #3861 (#713), and verify-ca trusting only sslrootcert

This is sqlx-core 0.8.6 as published on crates.io: the `.crate` whose
SHA-256 is `ee6798b1838b6a0f69c007c133b8df5866302197e404e8b6ee8ed3e3a5e68dc6`,
built from upstream commit `bab1b022bd56a64f9a08b46b36b97c5cff19d77e`
(`.cargo_vcs_info.json`). It is licensed MIT OR Apache-2.0; see
`LICENSE-MIT` and `LICENSE-APACHE`.

Two upstream fixes are applied, and one local change that is not
upstream's; nothing else changes. Both fixes were first released in sqlx
0.9.0; no 0.8.x release has either.

- [launchbadge/sqlx#4251](https://github.com/launchbadge/sqlx/pull/4251),
  "fix(tls): potential deadlock in `StdSocket::poll_ready()`" (merge commit
  `c0ec9c0b5042767cd30f569874e02dfb3d92668b`), for #771. It changes
  `src/net/tls/util.rs` only. When rustls's `complete_io` has found both its
  write and its read blocked, `poll_ready` now waits on both directions and
  returns when either is ready. 0.8.6 waited for write readiness and then
  for read readiness, so a large statement over TLS could park on read
  readiness while the server waited for the rest of the statement.
- [launchbadge/sqlx#3861](https://github.com/launchbadge/sqlx/pull/3861),
  "Fix NoHostnameTlsVerifier for rustls 0.23.24 and above" (merge commit
  `2970559e256ff11194cdc7c7aa18eb9d64fce494`), for #713. sqlx-postgres uses
  `NoHostnameTlsVerifier` in `src/net/tls/tls_rustls.rs` for
  `sslmode=verify-ca` only. It runs rustls's WebPKI checks, which verify the
  chain before the name, and accepts the certificate when the name is the
  only failure. rustls 0.23.24 and later report that failure as
  `NotValidForNameContext`, which 0.8.6 did not match, so verify-ca refused
  a CA-signed certificate for another name, as `verify-full` does. The
  verifier now accepts either `NotValidForName` or `NotValidForNameContext`.
  Every other error and every other sslmode are unchanged, and `verify-full`
  still checks the name. The fix also raises sqlx-core's rustls requirement from
  0.23.15 to 0.23.24, the first release with that variant: in
  `Cargo.toml.orig`, and on the same requirement in `Cargo.toml`. Both lock
  files already build rustls 0.23.46, so neither changes.
- **Local: verify-ca trusts only `sslrootcert`**, in
  `src/net/tls/tls_rustls.rs`. Upstream seeds the trust store with Mozilla's
  public roots (`webpki-roots`, or the system's with the native-roots
  features) for every mode that verifies certificates, and adds the
  `sslrootcert` certificates to them. verify-ca checks no name, so with
  #3861 any certificate that chains to a public root passed for any name,
  for example a Let's Encrypt certificate for an attacker's own domain, and
  sqlx-postgres answers a cleartext password request on that connection.
  As libpq does, verify-ca's verifier now starts from an empty store and
  holds only the certificates in `sslrootcert` (or `PGSSLROOTCERT`).
  verify-ca without `sslrootcert`, or with one holding no certificate, fails
  with a configuration error naming `sslrootcert` before the TLS handshake
  starts. libpq instead reads `~/.postgresql/root.crt` when `sslrootcert` is
  unset, and refuses when that is missing; sqlx-postgres has no such
  default. verify-full, require, prefer and allow are unchanged: verify-full
  still adds `sslrootcert` to the public roots and checks the name.

Each upstream diff applies as upstream wrote it, except that #3861's
`Cargo.lock` hunk, which belongs to upstream's workspace, is left out.
#4251's first hunk's context differs only because 0.8.6 imports `future`
from `futures_util` rather than `std`, and that import stays. The
registry's `.cargo-ok` marker is not copied, and this README and
`reviewed-advisories.toml` are added. Diffing this directory against the
extracted crate shows those three changes and nothing else.

The root `Cargo.toml` and `crates/qbit-prism-server/fuzz/Cargo.toml` patch
sqlx-core to this directory, and `lab/prism/Dockerfile` copies it into the
image build. Each change has regression tests that fail without it.
Against unpatched 0.8.6,
`crates/qbit-prism-server/tests/sqlx_tls_readiness.rs` stalls, and
`crates/qbit-prism-server/tests/sqlx_tls_verify_ca.rs` sees verify-ca
refuse a CA-signed certificate for another name. Without the local change,
the same file sees verify-ca without a usable `sslrootcert` report an
unknown issuer rather than the configuration error, and take a public root,
ISRG Root X1, as a certificate's issuer, as verify-full does.

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

When sqlx moves to a release that carries both fixes (0.9.0 or later),
first check that release's verify-ca trust store in
`src/net/tls/tls_rustls.rs`. The local change is not upstream's: if the
release still adds the public roots under verify-ca, carry the change
forward (for example in a vendored copy of that release) rather than drop
it. Only when it is no longer needed, remove these together:
- this directory;
- both `[patch.crates-io]` entries;
- the Dockerfile `COPY`;
- the script, its tests and its two CI steps.

Keep both regression test files, with their sqlx-core dev-dependency moved to
the new version: `sqlx_tls_verify_ca.rs` fails if the local change is lost.
