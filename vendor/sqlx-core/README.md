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
  `c0ec9c0b5042767cd30f569874e02dfb3d92668b`), for #771, recorded as
  `0001-sqlx-4251-poll-ready-waits-on-either-direction.patch`. It changes
  `src/net/tls/util.rs` only. When rustls's `complete_io` has found both its
  write and its read blocked, `poll_ready` now waits on both directions and
  returns when either is ready. 0.8.6 waited for write readiness and then
  for read readiness, so a large statement over TLS could park on read
  readiness while the server waited for the rest of the statement.
- [launchbadge/sqlx#3861](https://github.com/launchbadge/sqlx/pull/3861),
  "Fix NoHostnameTlsVerifier for rustls 0.23.24 and above" (merge commit
  `2970559e256ff11194cdc7c7aa18eb9d64fce494`), for #713, recorded as
  `0002-sqlx-3861-verify-ca-accepts-not-valid-for-name-context.patch`.
  sqlx-postgres uses
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
  `src/net/tls/tls_rustls.rs`, recorded as
  `0003-verify-ca-trusts-only-sslrootcert.patch`. Upstream seeds the trust store with Mozilla's
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

Each change is a `diff -u` patch in `vendor/sqlx-core.patches/`, applied
to upstream's files in name order. Each upstream diff applies as upstream
wrote it, except that #3861's `Cargo.lock` hunk, which belongs to
upstream's workspace, is left out. #4251's first hunk's context differs
only because 0.8.6 imports `future` from `futures_util` rather than `std`,
and that import stays. The registry's `.cargo-ok` marker is not copied,
and this README and `reviewed-advisories.toml` are added. The build reads
neither: `Cargo.toml` names no `readme` or build script, and no source
includes them.

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

`scripts/check_vendored_sqlx_core.py` guards this copy in CI:

- **The patch.** sqlx pins its sqlx-core with `=`, so moving sqlx to another
  version leaves the patch unused, and Cargo only warns. The script fails
  unless both lock files build this copy at this version.
- **Advisories.** cargo-deny matches RustSec advisories only against
  registry crates, so it no longer checks sqlx-core. Right after
  `cargo deny check`, the script lists every advisory the database it fetched
  holds for sqlx-core. It fails on any advisory that is not withdrawn and
  that `reviewed-advisories.toml` does not name.
- **The source.** cargo-deny doesn't check a path crate's files either.
  CI downloads upstream's `sqlx-core-0.8.6.crate` from crates.io, and the
  script refuses it unless its SHA-256 is the one recorded above. It
  applies the patches in `vendor/sqlx-core.patches/` to the crate's files,
  each hunk only at the line it names and on exact context, and fails
  unless the result is this directory byte for byte. No file may differ,
  be missing, be a symbolic link, or be added but this README and
  `reviewed-advisories.toml`. Any change to the code here therefore fails
  CI until a patch records it.

  This proves only that this directory is upstream 0.8.6 plus the recorded
  patches. It says nothing about what a patch does: a patch that trusted
  the public roots again would pass, as would a commit that changed both
  the code and its patch. The control is review of every change under
  `vendor/sqlx-core.patches/`, which shows each change to the vendored code
  as a diff against upstream, and the regression tests above, which check
  what the three changes do. Edits to `reviewed-advisories.toml` are not
  checked, nor are this README's, except that it must record the `.crate`'s
  SHA-256 exactly once and name every patch, so that the change list keeps
  up with them. Locally, the script uses Cargo's cached copy of the
  `.crate` when there is one, and otherwise skips this part.

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
- this directory and `vendor/sqlx-core.patches/`;
- both `[patch.crates-io]` entries;
- the Dockerfile `COPY`;
- the script, its tests, and its CI steps: the check, the `.crate`
  download for the Python tests, and the advisory review.

Keep both regression test files, with their sqlx-core dev-dependency moved to
the new version: `sqlx_tls_verify_ca.rs` fails if the local change is lost.

## Changing this copy

1. Make the change here, and describe it in the change list above, naming
   the patch file that will record it.
2. Print it as a patch against upstream with the recorded patches. This
   needs upstream's `.crate`, refused unless its SHA-256 is the one
   recorded above:

   ```sh
   curl -sSfL -o /tmp/sqlx-core-0.8.6.crate \
     https://static.crates.io/crates/sqlx-core/sqlx-core-0.8.6.crate
   python3 scripts/check_vendored_sqlx_core.py \
     --upstream-crate /tmp/sqlx-core-0.8.6.crate --unrecorded-diff
   ```

3. Save it as the next patch, `vendor/sqlx-core.patches/NNNN-<name>.patch`
   (lower case, digits and hyphens), with a description above its first
   `---` line, or fold it into the patch whose change it extends. Free text
   is allowed only there; the script refuses anything else outside the
   hunks, and a hunk that does not apply exactly where it says.
4. Run the script with `--upstream-crate` alone, and commit the change, the
   patch and this README together.
