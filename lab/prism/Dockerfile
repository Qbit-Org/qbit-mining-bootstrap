# syntax=docker/dockerfile:1.7

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cargo \
        postgresql-client \
        rustc \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY lab/auxpow/ ./lab/auxpow/
COPY lab/prism/ ./lab/prism/

ENV PYTHONPATH=/app

# Release profile plus a direct-binary path: the coordinator shells out to
# these tools on the job build hot path, so it must not pay `cargo run`
# build-graph checks per invocation nor debug-profile execution time.
RUN cargo build --locked --release -p qbit-prism --bins

ENV PRISM_TOOL_BIN_DIR=/app/target/release

CMD ["python3", "-m", "lab.prism.prism_coordinator"]
