# Bitcoin Core Release Integrity

The local parent-chain image installs Bitcoin Core release binaries. A download
that is merely versioned or served over HTTPS is not a sufficient supply-chain
control, so the image build requires a reviewed SHA256 for each supported
architecture and verifies the archive before extraction.

## Build Contract

The checked-in release coordinates live in `config/upstream.env`:

```text
BITCOIN_RELEASE_VERSION
BITCOIN_RELEASE_BASE_URL
BITCOIN_RELEASE_URL
BITCOIN_RELEASE_SHA256_AMD64
BITCOIN_RELEASE_SHA256_ARM64
```

`BITCOIN_RELEASE_URL` may point to a controlled mirror. It does not bypass the
digest check: the mirrored archive must match the pinned digest for Docker's
`TARGETARCH`. Builds fail for unsupported architectures, missing or malformed
digests, and digest mismatches.

## Updating Bitcoin Core

1. Download the release's `SHA256SUMS` and detached `SHA256SUMS.asc` from the
   official Bitcoin Core release directory over an independently verified URL.
2. Follow Bitcoin Core's official binary verification procedure. Verify the
   manifest against release-builder keys whose fingerprints were obtained from
   a separate trusted source; downloading a key and the manifest from the same
   location does not establish trust.
3. Select the `x86_64-linux-gnu` and `aarch64-linux-gnu` tarball digests from
   that verified manifest. Do not calculate new expected digests from the
   downloaded tarballs themselves.
4. Update the version, base URL, and both architecture digests together in
   `config/upstream.env` and `config/upstream.env.example`.
5. Build both target architectures. Confirm a deliberately incorrect digest
   fails before extraction, then run `bitcoin-cli --version` in each successful
   image and compare it with the intended release.

For reproducible deployments, also pin the resulting image by immutable digest
after the reviewed build has completed. Release-archive verification protects
the input binary; image-digest pinning protects the exact built artifact used by
the deployment.
