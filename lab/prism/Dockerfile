# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --locked --release -p qbit-prism-server -p qbit-prism --bins

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
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
CMD ["qbit-prism-server"]
