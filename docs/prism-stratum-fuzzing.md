# Stratum fuzzing and property tests

Issue #575 item 4. Three cargo-fuzz targets search the Stratum front door for
hours; a small property-test suite holds the same contracts on every PR.

## What runs where

| What | Where | When | Budget |
| --- | --- | --- | --- |
| `tests/stratum_properties.rs` (proptest) | `rust-tests` CI job (`cargo test --workspace`) and the database job's workspace run | every PR | well under a second of run time; one more test binary to build |
| `stratum_session`, `stratum_lines`, `codec_parsers` (cargo-fuzz) | `stratum-fuzz` job of `prism-load-nightly.yml` | nightly and on dispatch | 20 minutes, all three at once |
| the same targets | a developer machine | on demand | as long as you like |

The property tests read no environment input, so they are not gated and not
listed in `test/prism-gated-tests.txt`; see
[the integration test gate](prism-integration-test-gate.md).

## The seam

The targets drive the production per-connection loop,
`qbit_prism_server::stratum::serve_connection`, over an in-memory pipe
(`tokio::io::duplex`). `run_listener` calls the same function with an accepted
socket's halves after `set_nodelay`, so framing, timeouts, buffer bounds,
budgets and shutdown are the production ones. Admission (the global, per-source
and per-username limits taken at accept) and `StratumConfig::validate` stay with
the caller; the harness validates its configuration itself.

The backend is `FuzzBackend` in `crates/qbit-prism-server/fuzz/src/backend.rs`:
no database, node or network. It builds real jobs from real payout manifests,
remembers every job it built, and credits a share by the coordinator's gate
(not block-only, current tip or inside stale grace, meets the share target, not
a duplicate). Everything the coordinator decides against PostgreSQL is out of
scope here; the ledger and live suites cover it.

## Targets

- **`stratum_lines`**: arbitrary bytes, cut into arbitrary fragments, into one
  connection. Byte 0 picks the message bound (256 B to 16 KiB), byte 1 the
  malformed-frame budget, byte 2 the fragment sizes; the rest is the stream.
- **`stratum_session`**: a script of requests and backend events. Each line is
  a frame, sent after placeholders such as `$job`, `$en2` and `$nonce` are
  replaced with live values (a nonce that meets the job's target, when one is
  near), or an event: `#refresh` (new tip), `#revision` (same-parent payout
  replacement), `#reconnect`, `#fail-build`, `#fail-submit`, `#pipeline`,
  `#sleep`, `#again`. An optional `#cfg` first line sets the listener: message
  bound, extranonce2 size, stale grace, retention, per-connection job cap,
  per-username limit, the three session budgets, vardiff and its retarget
  timing, difficulties, network bits, template transactions and the
  version-rolling mask. The backend keeps retained vardiff evidence like the
  ledger does, so reconnects resume a difficulty. `fuzz/src/script.rs`
  documents the grammar.
- **`codec_parsers`**: the codec's transaction, block, coinbase-split, compact
  target, difficulty, hex and template parsers on arbitrary bytes.

## Invariants

A run fails, and libFuzzer saves the input, when any of these breaks:

- **No panic**, in the session task or the harness.
- **Bounded memory.** A counting global allocator fails an iteration whose
  heap grows past a fixed allowance plus a small multiple of its input, and
  libFuzzer's `-malloc_limit_mb` and `-rss_limit_mb` catch single huge
  allocations and leaks. No line the server writes may exceed four times the
  message bound plus 16 KiB.
- **Framing.** Every complete frame within the bound is answered exactly once,
  in order, with the frame's own `id` (any JSON type, compared exactly); a frame
  that is not a JSON object gets a null-id `malformed-submit`; a frame over the
  bound gets one null-id `Stratum message exceeds size limit` and the
  connection closes; a trailing partial frame is dropped. How the stream is
  fragmented changes none of it. The server closes only for an oversize frame or
  a spent budget, and writes nothing after that answer.
- **Documented errors only.** Every error is `[code, message, data]` with a
  reason ID from [the rejection reference](prism-rejections.md) and its code
  (21 stale or unknown job, 22 duplicate, 23 low difficulty, 20 otherwise), or
  one of the reason-less refusals (spent authorize or unknown-job budget,
  username limit). `internal-error` never appears, and `backend-rpc-unavailable`
  only after an injected fault. The answers that are fully determined by the
  connection state and the fields are predicted exactly: submit before
  subscribe and authorize, short or non-string params, another username, a
  wrong-size extranonce2, a wrong-size ntime or nonce, a job never built.
- **Honest work.** Every `mining.notify` names a job the backend built for this
  connection's extranonce1, after subscribe and authorize, matches it field for
  field, and follows a `set_difficulty` equal to its share difficulty.
  `build_job` is never asked for a non-finite or non-positive difficulty.
- **No credit for an invalid share.** The backend's credits and the submits
  answered `true` correspond one to one, in order. Each credited share is
  re-verified from the job as built and the raw wire fields, by code that shares
  nothing with `Job::assemble_submission` but SHA-256: extranonce2 size, 4-byte
  hex time and nonce, time not before `mintime`, version bits inside the mask
  the job was persisted with, and the rebuilt header's hash at or below the
  job's share target. The credit goes to the worker the job was issued to, and
  never to work that a same-parent payout replacement delivered on the same
  connection retired to block-only (#478).
- **Parsers.** Accepted transactions strip their witness idempotently, coinbase
  splits reassemble, compact targets decode into range and re-encode, and a
  higher difficulty never gives a larger target.

## Running locally

```sh
rustup toolchain install nightly-2026-09-24 --profile minimal
cargo install cargo-fuzz --locked --version 0.13.2
cd crates/qbit-prism-server/fuzz
cargo +nightly-2026-09-24 fuzz run stratum_session corpus/stratum_session seeds/stratum_session -- \
  -dict=stratum.dict -max_len=16384 -rss_limit_mb=2048 -malloc_limit_mb=512 -timeout=30
```

`corpus/` and `artifacts/` are ignored by git. To reproduce a finding, pass the
artifact instead of the corpus directories, with `PRISM_FUZZ_TRACE=1` to print
every frame and line. `bash .github/scripts/prism-stratum-fuzz.sh <corpus>
<output> [seconds]` runs exactly what the nightly job runs.

`cargo test` in `fuzz/` (stable toolchain) replays every seed through its
target's checks without libFuzzer, checks that each session seed reaches the
state it is named for, and fails when `seeds/` drifts from the generator in
`fuzz/tests/seeds.rs`; `PRISM_FUZZ_REGENERATE_SEEDS=1 cargo test` rewrites it.
The seeds are the requests and codec vectors of `tests/stratum_protocol.rs` and
`tests/stratum_codec.rs`.

## Nightly job

`stratum-fuzz` in `prism-load-nightly.yml` restores the previous night's
corpus from the Actions cache, runs `.github/scripts/prism-stratum-fuzz.sh` for
1,200 seconds per target on an 8 vCPU runner, merges each corpus down to the
inputs that add coverage, saves it under a new cache key, and uploads the logs
and any crashing, leaking, timing-out or out-of-memory inputs. It fails when any
target found one. The script pins the nightly toolchain and the cargo-fuzz
release (by sha256) it installs.
