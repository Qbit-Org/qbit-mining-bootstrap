# syntax=docker/dockerfile:1.7

# The builder is the toolchain pinned in rust-toolchain.toml, by version and by
# the multi-arch index digest of that tag on Docker Hub; the docker-builds CI
# job asserts the stage's cargo reports the pin, and
# scripts/check_toolchain_pin.py holds the version here to the pin.
FROM rust:1.98.1-bookworm@sha256:9a73a5088750b4c95158ab26629c854c3d6fc4b173cb7bc8079ad252d8ed7bfa AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --locked --release -p qbit-prism-server -p qbit-prism --bins

# The runtime base is pinned by the multi-arch index digest of bookworm-slim;
# move the digest deliberately to take a Debian point or security release.
# Packages still resolve from the archive at build time: Debian drops
# superseded versions, so exact apt pins would break the next rebuild.
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 prism \
    && useradd --system --uid 10001 --gid prism --home-dir /nonexistent \
        --no-create-home --shell /usr/sbin/nologin prism \
    && install -d -m 0750 -o prism -g prism /var/lib/qbit-prism/audit
WORKDIR /app
COPY --from=build /build/target/release/qbit-prism-server /usr/local/bin/
COPY --from=build /build/target/release/qbit-prism-miner /usr/local/bin/
COPY --from=build /build/target/release/qbit-prism-audit-canonicalize /usr/local/bin/
COPY --from=build /build/target/release/qbit-prism-audit-verify /usr/local/bin/
COPY --from=build /build/target/release/qbit-prism-build-audit-bundle /usr/local/bin/
COPY --from=build /build/target/release/qbit-prism-reorg-verify /usr/local/bin/
ENV RUST_LOG=info
EXPOSE 3340 3341
STOPSIGNAL SIGTERM
# A numeric UID lets orchestrators verify the non-root policy. Mounted secrets
# and the audit directory must be readable by UID/GID 10001.
USER 10001:10001
# Mining frontends hold signing seeds in memory (#260 D4), so a core dump could
# write them to disk. Lowering the soft and hard limits here holds under plain
# `docker run` too; exec keeps the command as PID 1 to receive SIGTERM.
ENTRYPOINT ["/bin/sh", "-c", "ulimit -c 0 && exec \"$@\"", "qbit-prism"]
# Probes the operator listener, sending PRISM_OPERATOR_BEARER_TOKEN when set,
# or Stratum when that listener is disabled. The public-api role overrides it
# with `healthcheck --public-api`; run qbit-prism-miner and one-shot tools with
# --no-healthcheck.
HEALTHCHECK --interval=5s --timeout=5s --start-period=15s --retries=3 \
    CMD ["qbit-prism-server", "healthcheck"]
CMD ["qbit-prism-server"]
