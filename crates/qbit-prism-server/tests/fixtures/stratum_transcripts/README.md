# Stratum transcript corpus

Recorded Stratum v1 client sessions that
`live_regtest::transcript_replay_tests` replays against a live regtest server
on every PR (#575). Each `*.jsonl` file is one session between one miner and
the pool it was recorded against; the replay checks that PRISM answers every
request in a way that client accepts. The harness and its compatibility rules
are documented in
[`tests/support/live_transcript_replay.rs`](../../support/live_transcript_replay.rs).

Every file here is **synthetic**: written by hand from the conventions each
client dialect follows (BIP 310 for `mining.configure`, the published
behaviour of the named firmware and marketplaces), not captured from a device
or a rental. The header of each file says so.

| file | dialect |
| --- | --- |
| `cgminer-plain.jsonl` | cgminer 4.x without version rolling |
| `bmminer-version-rolling.jsonl` | bmminer (Antminer): `mining.configure` first, mask `1fffe000`, six-field submits |
| `nicehash-extranonce-subscribe.jsonl` | NiceHash-style proxy: `mining.extranonce.subscribe` after authorize |
| `braiins-configure-extensions.jsonl` | Braiins OS: one `mining.configure` naming several extensions |
| `esp-miner-suggest-difficulty.jsonl` | esp-miner (Bitaxe): `mining.suggest_difficulty` before authorize, an unsupported method |

## Format

The first line is a header; every other line is one message.

```json
{"transcript":{"name":"<file name without .jsonl>","source":"synthetic|captured","dialect":"<client, firmware, version and what it exercises>","notes":"<where it came from and what was redacted>"}}
{"direction":"c2s","timestamp":0.0,"message":{"id":1,"method":"mining.subscribe","params":["bmminer/2.0.0"]}}
{"direction":"s2c","timestamp":0.038,"message":{"id":1,"result":[[],"0800000a",4],"error":null}}
```

- `direction` is `c2s` (sent by the miner) or `s2c` (sent by the pool).
- `timestamp` is seconds since the session started, non-decreasing. It orders
  the lines; the replay does not wait out recorded gaps.
- `message` is the JSON-RPC object exactly as it crossed the wire.
- Every `c2s` request with a non-null `id` needs its `s2c` answer (same
  `id`) in the file: that answer is the expectation.

`stratum_transcript_corpus_is_well_formed_and_labelled` checks every file
without a server; run it after adding one.

## Adding a captured transcript

1. Capture one session between a real miner and a pool with a logging TCP
   proxy (or `tcpdump` and a Stratum dissector) that records each line and
   its direction and time. Capture only on hardware and accounts you control.
2. Convert it to the format above: one header, then one line per message,
   with `timestamp` in seconds from the first message.
3. Redact before committing. Replace payout addresses in worker names with a
   placeholder (the replay substitutes the fixture's regtest address and keeps
   the worker suffix after the last `.`), and the `mining.authorize` password
   with `x`. Remove hostnames, IP addresses, account names and anything else
   that identifies the operator. The jobs, extranonces and nonces are not
   secret and may stay.
4. Set `"source":"captured"` and name the miner, firmware version, pool
   dialect and capture date in `dialect` and `notes`.
5. Name the file `<client>-<what it exercises>.jsonl`, matching
   `transcript.name`, and run
   `cargo test -p qbit-prism-server --test live_regtest transcript_replay`
   with the live inputs set (see `docs/prism-integration-test-gate.md`).

A captured session may contain answers PRISM gives differently: the replay
compares compatibility, not bytes. A submit the recorded pool accepted is
solved again on live work; one it refused is sent as recorded and must be
refused again. Notifications the recorded pool sent that PRISM never sends
(for example `mining.set_extranonce`) are reported, not failed; a missing
`mining.notify` or `mining.set_difficulty` fails.
