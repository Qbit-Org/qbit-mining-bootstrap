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

Both run under `cargo test` and need no Python or other inputs. Both also
replay the cases in `supplementary.json` (see below) under a separate tally.

## Pins

- `reference.json` is pinned by its sha256,
  `017c787d3b894d92702d65774e47224ce5a09838bcd1118549f740a21b406142`, in
  `crates/qbit-prism/tests/support/window_corpus.rs`.
- `supplementary.json` is pinned there too, by its sha256,
  `4a89967643938cfbec933b7f5582118e583245986eb2adf9e533d017ea252fc6`.
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

## `supplementary.json`

The corpus never expires a whole pre-existing page at exactly
`window_weight`, so a `>` in place of that `>=` passes every corpus case.
`supplementary.json` holds four cases that pin the boundary. They were
exported from the same 2.x.x oracle at
`504846cc0b72e8f86ed17f896d4ccbbe196a31dc`, and each entry has exactly the
`reference.json` v2 shape: `why`, `input_sha256`, `pinned_literals` (input,
record JSONs, canonical bytes and spool tail), and every output field.

| Case | Window before the advance | What the advance pins |
| --- | --- | --- |
| `advance-page-expiry-at-weight` | weight 2, page_size 2, one page | the old page expires with exactly the weight left |
| `advance-page-expiry-at-weight-second-page` | weight 4, page_size 2, two pages | the first page expires above the weight, the second with exactly the weight left |
| `advance-page-expiry-one-below-weight` | weight 3, page_size 2, one page | expiring the page would leave one below the weight, so it stays and one row expires |
| `advance-page-expiry-one-above-weight` | weight 3, page_size 2, one page | the page expires with one above the weight left |

Every expected value, `touched_pages` included, is the oracle's output; none
is computed by hand. Both tests replay these cases with the same comparisons
as the corpus, under their own tally
(`4 cases, 0 rejections, 4 byte-compared, 0 declined, 4 with advance stats`),
so the corpus counts stay as frozen.

The file's `command` field is the exact export. Run it in the extract's root.
It writes `supplementary.json` there, and exits non-zero if the oracle
rejects any case. Two runs produce identical bytes.

## Compact spool tail

Each frozen `spool_tail` is the compact build-request suffix the 2.x.x
coordinator spooled:
`,"compact_share_identities":[...],"compact_shares":[...]}`. The native test
compares it with the test's own encoder. The daemon gate also makes the
daemon's real decoder consume it:

- It prefixes the frozen bytes with a `found_block` and a `window_key` and
  sends the result as a `--serve` build request.
- The build summary must equal the one the same daemon builds from the window
  it prepared from the case's full records.
- The summary's reward manifest lists every counted share with the fields the
  compact format carries: `share_seq`, `share_id`, `miner_id`, `order_key`,
  `p2mr_program_hex`, `share_difficulty`, `job_issued_at_ms`,
  `accepted_at_ms` and `credit_policy`. The fields it doesn't carry
  (`network_difficulty`, `template_height`, `job_id`, `ntime`) never reach the
  summary.

The five sidecar cases pin no spool literal. For them the gate sends the test
encoder's bytes, and only once those hash to the frozen `spool_tail_sha256`.
The empty window has no rows to upload and isn't sent.

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
4. Export the supplementary cases with the `command` recorded in
   `supplementary.json`. It writes `supplementary.json` in the extract's root.
5. Copy the three files here and update the pins above.

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
