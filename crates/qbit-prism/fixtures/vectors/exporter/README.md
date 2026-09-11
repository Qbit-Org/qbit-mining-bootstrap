# Money-path vector exporter

These two files produced the frozen vectors one directory up
(`crates/qbit-prism/fixtures/vectors/*.json`). They are kept outside any cargo target, so 3.x.x
never compiles them. `export_rule_decisions.py` runs the 2.x.x Python rule code for the bootstrap
transition and below-target block credit. `export_money_path_vectors.rs` calls the 2.x.x
`qbit-prism` engine for every topic and computes each rule's payout consequence. The 3.x.x test
`crates/qbit-prism/tests/money_path_vectors.rs` replays every case. To regenerate, extract the 2.x.x
source commit `504846cc0b72e8f86ed17f896d4ccbbe196a31dc` into an empty directory:
`git archive 504846cc0b72e8f86ed17f896d4ccbbe196a31dc | tar -x -C "$D"`. Copy both files into
`crates/qbit-prism/examples/` there. From the root of that tree, run
`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 crates/qbit-prism/examples/export_rule_decisions.py | cargo run --locked -q -p qbit-prism --example export_money_path_vectors -- vectors-out`.
Copy `vectors-out/*.json` here. The export is deterministic: two runs must be byte-identical. The
exporter refuses to write a bootstrap or credit case where the two rules pay differently unless
that case is mapped to a D2 entry in `docs/prism-rust-migration.md`.

The test pins the sha256 of every vector file (`VECTOR_SHA256`). A re-export that changes any file
must update those pins in the same reviewed commit; never edit a vector by hand.

Known gap: the TRUC per-transaction cap (1160 fanout recipients) is frozen only as a configuration
bound (`fanout-size-at-the-truc-cap` and `fanout-size-one-over-the-truc-cap-is-an-error`, each with
three recipients). A real 1160-recipient chunk, and a 1161-recipient one, would add about 1.6 MB to
`settlement_chunks.json`, so they are not exported.
