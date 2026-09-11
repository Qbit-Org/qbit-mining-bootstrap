# syntax=docker/dockerfile:1.7

FROM python:3.14-slim-trixie

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cargo \
        postgresql-client \
        rustc \
    && rm -rf /var/lib/apt/lists/*

# Persistent pooled Postgres client for the share-ledger hot path. The psql
# binary stays installed as the fallback backend (PRISM_POSTGRES_NATIVE_CLIENT=0).
RUN pip install --no-cache-dir "psycopg[binary]==3.2.*"

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

# Issue #226: glibc malloc arena policy, set deliberately rather than left to
# glibc. The mainnet coordinator runs about 64 threads and its memory map is
# hundreds of 4-64 MiB anonymous regions -- the per-thread arena signature --
# on a host where glibc's default arena limit is 8 x cores = 256, so every
# thread that ever mallocs owns an arena and that arena's retained free lists.
# Two arenas is the default because it is safe before the soak decides
# between 2 and 4: under the GIL at most one Python thread allocates at a
# time, so arena-lock contention can only come from GIL-released native code
# (libpq I/O on a handful of pooled connections), and the audit-builder
# daemon this process spawns inherits the setting but is single-threaded. The
# failure mode of too few arenas is measurable lock wait on /metrics share-ack
# latency; the failure mode of too many is the 17.7 GB resident set. Override
# at build time with --build-arg MALLOC_ARENA_MAX=4 or at run time with
# -e MALLOC_ARENA_MAX=4; docs/mainnet-deployment.md records the decision and
# docs/prism-capacity-readiness.md the soak that revisits it.
#
# PYTHONMALLOC is deliberately unset: pymalloc keeps objects of 512 bytes and
# under out of glibc entirely, and routing them through glibc would enlarge
# the very arenas this bounds. No other MALLOC_* variable is set here. Never
# set one to an empty value: glibc parses "" as 0, and MALLOC_MMAP_THRESHOLD_=0
# would send every allocation through mmap. tests/test_prism_heap_census.py
# pins exactly this allocator environment.
ARG MALLOC_ARENA_MAX=2
ENV MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX}

# Keep Python as PID 1 so watchdog os._exit(1) reaches Docker unchanged. Any
# future wrapper must exec the coordinator or explicitly propagate its status.
CMD ["python3", "-m", "lab.prism.prism_coordinator"]
