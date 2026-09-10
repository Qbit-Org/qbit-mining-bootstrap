# Frozen payout-window corpus

`reference.json` is a frozen corpus of 35 payout-window cases: inputs,
rejection categories, and the exact output bytes (canonical window bytes and
digest, per-record stream, compact spool tail, advance stats). The Python
differential oracle generated it on 2.x.x. 3.x.x removed that oracle, and two
Rust tests read the corpus instead:

- `crates/qbit-prism/tests/window_frozen_vectors.rs` replays every case
  against `qbit_prism::window::PayoutWindow`;
- `crates/qbit-prism/tests/window_daemon_gate.rs` replays every case through
  the real `qbit-prism-build-audit-bundle --serve` daemon.

Both run under `cargo test` and need no Python or other inputs.

## Pins

- `reference.json` is pinned by its sha256,
  `017c787d3b894d92702d65774e47224ce5a09838bcd1118549f740a21b406142`, in
  `crates/qbit-prism/tests/support/window_corpus.rs`.
- Every case's input document is pinned by its `input_sha256` in
  `reference.json`. That is Python's
  `json.dumps(doc, sort_keys=True, separators=(",", ":"))` with its default
  `ensure_ascii`. Both tests recompute it for all 35 cases before comparing any
  output.

Changing either file needs a reviewed commit that updates these pins. Never
edit expected values to make a test pass: a disagreement may be a payout bug.

## `inputs-unpinned.json`

Thirty cases carry their input in `pinned_literals.input`. The other five
(`bulk-seeded`, `multi-page-interior-cutoff`, `page-boundary-511`,
`page-boundary-512`, `page-boundary-513`) don't. The 2.x.x oracle generated
them with Python's seeded `random.Random`, which isn't ported to Rust.

The sidecar holds those five input documents, exported from 2.x.x at
`504846cc0b72e8f86ed17f896d4ccbbe196a31dc`. Its `command` field records the
exact export command. That command checks each document against the frozen
`input_sha256` before writing anything.

## Regenerating

In an extract of 2.x.x at the pinned commit
(`git archive 504846cc0b72e8f86ed17f896d4ccbbe196a31dc | tar -x -C <dir>`),
with Python 3.14 and no database:

1. Run the no-op test. It must report 29 tests OK:
   `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_window_pipeline_parity -v`.
2. Regenerate the corpus with the file's own `regenerate_with` command:
   `python3 -m tests.window_pipeline_parity regenerate`.
3. Export the sidecar with the `command` recorded in `inputs-unpinned.json`.
   It writes `inputs-unpinned.json` in the extract's root.
4. Copy both files here and update the pins above.

## Integer domain

3.x.x carries integers at fixed widths: `share_seq` and `template_height` are
u64, difficulties and `window_weight` are u128, `ntime` is u32, timestamps are
i64, `page_size` is u64, and the total difficulty is u128. 2.x.x's Python
oracle had unbounded integers. Two cases lie outside these widths:

| Case | First out-of-domain value | Width |
| --- | --- | --- |
| `difficulty-beyond-u128` | `window_weight` = 2^128 + 2^64 | u128 |
| `ntime-beyond-u32` | `records[0].ntime` = 2^32 | u32 |

On 3.x.x their inputs can't be represented. The typed parse fails, and the
daemon answers `out_of_range` with that field and width. The tests assert this
decline explicitly, but
the frozen output bytes for these two cases came from unbounded integers and
aren't compared.

`wide-integers` sits at the edges of the widths (2^64-1, 2^128-1) and must
match byte for byte.
