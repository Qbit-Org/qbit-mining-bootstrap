# prism-load-trend fixtures (#551)

`regression/` is a synthetic series for `scripts/prism_load_regress.py`:
twelve nightly L2 rows of `throughput-20k-window-1fe` on one runner class and
fsync band (`history/trend/L2/2026-09.jsonl`, runs 1001 to 1012, within a few
percent of each other), and run 1013 (`current/`) as it would look with a
10 ms sleep inside the share append: server-side ACK mean 2 ms to 12.1 ms,
client ACK p50 10 ms to 22.75 ms and p99 40 ms to 91 ms. The rule must flag
those three numbers, and only those, with the commit range from run 1012's
commit to run 1013's.

    python3 scripts/prism_load_regress.py \
      --history tests/fixtures/prism-load-trend/regression/history \
      --rows tests/fixtures/prism-load-trend/regression/current
