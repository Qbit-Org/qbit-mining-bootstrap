# beyond-v3 models

These are research models behind
[the HA recommendation](../ha-recommendation.md). They are not PRISM code, and
nothing imports them. Each runs with the Python 3 standard library only.

## `payout/`: the chain-anchored payout rules

This is red team A's model of the payout rules in Appendix A of the
recommendation. It looks for double payouts and lost payout data under any
number of writers, promotions, partitions and reorgs.

| File | What it does |
|---|---|
| `policy.py` | A Python port of PRISM 3's `apply_payout_policy` and `apply_proportional_fanout_fee` (v3.0.0-rc.6). `python3 -I policy.py <repo-root>` replays every case for those two functions in `crates/qbit-prism/fixtures/vectors/*.json`: 25 of 25 match. |
| `core.py` | Jobs and manifests, the R1 variants (sum of deltas, or carry vector), the R4 variants (strict, relaxed, latest-only, none), carry-free work, checkpoints, and the overpay check. Overpay means an increase in `max(0, −truth)`, PRISM's own debt definition. |
| `exhaustive.py` | Enumerates every final chain × what each block's builder knew × rule variant. `pretty.py` prints the shortest counterexamples. |
| `sim.py`, `batch.py` | A timed two-node simulator, for the 3.x single-writer shape and the 4.0 two-writer shape, and its scenario matrix: deaths, disk loss, partitions, split brain, standby lag, spoofed markers. |
| `econ.py` | Carry-free economics at production-like magnitudes. |
| `targeted.py` | Checkpoint convergence, and debt recovered twice. |

**Running it:**

```sh
cd docs/beyond-v3/models/payout
python3 -I policy.py <v3.0.0-rc.6 checkout>   # expect "vectors: 25 match, 0 mismatch"
python3 -I exhaustive.py P2 X1       # a few minutes; P1..P4, C1..C3, X0..X8, S1 are configurations
python3 -I pretty.py                 # shortest counterexample per configuration
python3 -I batch.py 300 12           # simulator matrix, as proposed
python3 -I batch.py 300 12 fixed     # simulator matrix, with the data fixes
```

Results are written to `out/`, which is ignored by git.

**Headline results:**
- **Sum plus strict R4:** zero double payouts in 6,388,553 enumerated chains.
  Across the sum-based configurations (P1–P3), zero in 19.15 million chains,
  and zero in 21,600 timed runs.
- **Readings that double-pay:**
  - a latest-only check;
  - a missed pool block;
  - a misread mode byte with the relaxed rule;
  - a bounded lookback;
  - R1 over the database's "confirmed" set;
  - one builder without the rules.
- **Data loss in single faults:** appears only through a non-durable carry-free
  fallback and a rejoin that discards the loser's records. Both disappear with
  the fixes.

Run counts are existence proofs and comparisons. The model's lag, block and
failure rates are not calibrated to production.

## `availability/`: expected pool blocks lost per year

`blocks_lost.py` is red team D's back-of-envelope model:
- per-option recovery times;
- ~100 pool blocks a day;
- a per-node failure rate λ.

Every input is an assumption, and the output is for comparing options, not a
forecast.
