# ckpool Notes For qbit

ckpool is the cleanest open-source fit for qbit's permissionless mining path because it speaks standard GBT and `submitblock`. It does not add anything for qbit's AuxPoW path.

## Current status

- This repo's patched `ckpool` path is the validated permissionless reference integration for qbit.
- Wider upstream `ckpool` compatibility still needs explicit testing outside this overlay.
- Public-chain payout handling must follow qbit's P2MR-only address policy.

## qbit values ckpool needs

- Mainnet RPC / P2P: `8352` / `8355`
- Address HRP: `qb`
- Public-chain payout addresses: P2MR only
- Coinbase maturity: `1000`
- Witness scale factor: `1`
- Max block weight: `2000000`
- Permissionless version mask: use `getblocktemplate.versionrollingmask` when
  qbitd advertises it; fall back to `CKPOOL_VERSION_MASK=1fffe000`

## Likely friction points

### Address validation

Do not force `bech32`, taproot, or legacy payout types through the pool config. Use qbit's default `getnewaddress` behavior and let the node hand back the chain-correct payout address. On public chains that means P2MR.

### Payout maturity

Do not leave maturity at Bitcoin defaults. qbit rewards stay immature for `1000` blocks.

### Version rolling

Keep miner-controlled version rolling inside the mask advertised by qbitd. The
container defaults `CKPOOL_VERSION_MASK_MODE=dynamic`, so ckpool probes
`getblocktemplate` at startup and writes the selected `version_mask` into
`/etc/ckpool/ckpool.conf`. Older qbitd builds that do not return
`versionrollingmask` fall back to `CKPOOL_VERSION_MASK=1fffe000`. If qbitd
returns `versionrollingmask=00000000`, ckpool disables BIP310 version rolling.
For normal production qbit, the node-advertised mask is the source of truth.
During explicitly authorized mainnet prelaunch, the mask probe uses the
configured fallback and preflight defers only its live GBT shape check. CKPool's
generator then keeps retrying GBT every five seconds rather than exiting. Its
Stratum listener remains bound, but it does not serve mining work until qbitd
returns a valid template.

### Preflight interfaces

`qbit-ckpool-preflight` with no arguments performs the existing full one-shot
check. `--production-gate-only` performs stateless production, CKPool-knob, and
public difficulty-policy checks without constructing an RPC client. Private
wrappers can run it before creating wallets or files. `--supervise <command>
[args...]` performs full initial checks, starts the child without shell
interpolation, forwards SIGTERM/SIGINT, and continuously checks qbit while the
child lives.

Strict public-chain supervision requires the expected RPC chain, completed IBD,
`CKPOOL_MIN_PEERS`, a fresh live template, and a template parent matching the
active tip. `QBIT_EXPECTED_GENESIS_HASH` is mandatory on mainnet and optional on
other chains. The watchdog interval, failure grace, maximum template age, and
maximum future time are configured by
`CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS`,
`CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS`, `CKPOOL_TEMPLATE_MAX_AGE_SECONDS`, and
`CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS`.

The only relaxed mode requires mainnet, both production flags set to `1`, and
both launch/readiness flags set to `0`. It continuously checks static policy,
chain/genesis identity, and the payout address, while deferring only IBD, peer,
GBT, freshness, and active-tip requirements. Switching both readiness flags to
`1` and restarting or redeploying CKPool restores strict checks. A running
supervisor intentionally uses the environment captured at process startup.

### Fee and size accounting

If your ckpool fork assumes `WITNESS_SCALE_FACTOR=4`, it will mis-estimate block size and fee density on qbit. Use serialized size or qbit's weight values directly.

### Public-network payout policy

Public qbit networks currently run with `fP2MROnly=true` for non-coinbase outputs. Even if ckpool can mine blocks, verify that the payout path produces outputs qbit will relay and mine.

## Regtest walkthrough

1. Start qbitd on regtest with `-asert` and RPC enabled.
2. Create a regtest payout address with `qbit-cli -regtest getnewaddress`. Add `-p2mronly=1` if you want the restricted-output path to match public-chain wallet behavior.
3. Confirm `getblocktemplate '{"rules":["segwit"]}'` succeeds against regtest qbitd.
4. Merge the qbit values from [`ckpool.conf.example`](./ckpool.conf.example) into your ckpool config.
5. Start ckpool pointed at qbit's RPC port, not the P2P port.
6. Submit a block on regtest and confirm qbit accepts it through `submitblock`.

## Signet note

- qbit signet uses RPC `38352`, P2P `38355`, and `tq...` P2MR payout addresses.
- qbit signet also requires `getblocktemplate` callers to request `{"rules":["segwit","signet"]}`.
- This repo keeps the generic ckpool fractional-difficulty compatibility patch on signet, but only injects the `1/256` floor on regtest.
- If your signet needs a lower starting share difficulty, set `CKPOOL_MINDIFF` and `CKPOOL_STARTDIFF` explicitly instead of reusing the regtest floor.
- Set `CKPOOL_MAXDIFF` when operators need an upper bound on ckpool's vardiff-selected share difficulty.

## Rejects observability

The patched ckpool aggregates every Stratum `mining.submit` by worker and
reason and writes the tallies to `<logdir>/pool/rejects.status` (in the
container: `/var/log/ckpool/pool/rejects.status`) once per 60-second stats
cycle. The write is staged to `rejects.status.tmp` and atomically renamed, so
exporters never observe a partial document. Share validation, share responses,
and block submission behavior are unchanged; the file is purely additive
observability from `docker/ckpool/qbit-rejects-observability.patch`.

Document shape (version 1):

```json
{
 "version": 1,
 "poolinstance": "qbitlab",
 "runtime": 120,
 "lastupdate": 1712345678,
 "interval": 60,
 "pool":    { "accepted": {"count": 10, "diff": 9.5e-06}, "...": {} },
 "workers": [ { "workername": "qb1....rig0", "accepted": {"count": 10, "diff": 9.5e-06}, "...": {} } ]
}
```

Every submission lands in exactly one reason bucket, so bucket counts sum to
total submissions per worker:

- `accepted`: share met its assigned difficulty and was accounted.
- `above_target`: share hash above the target for the assigned difficulty
  (upstream's "Above target" reject).
- `stale`: share against a job retired by a block change.
- `duplicate`: byte-identical resubmission of an already-accepted share.
- `invalid_job`: unknown job id.
- `invalid_ntime`: ntime outside the allowed rolling window.
- `invalid_version`: version bits outside the negotiated BIP310 mask.
- `malformed`: every other unprocessable submission (bad parameter shape,
  worker mismatch, or no current workbase).

`count` is the number of submissions; `diff` sums the difficulty *assigned* to
the worker at each submission (the accounting diff), not the achieved share
difficulty. Counters are cumulative since ckpool startup and reset on restart;
exporters should treat them as monotonic counters and derive rates from
deltas.

Block-candidate outcomes are tracked in two additional buckets,
`block_accepted` and `block_rejected`, counting local `submitblock` results
for shares that met network difficulty. A candidate is also classified as a
share in the reason buckets above, so block buckets are intentionally not part
of the per-worker submission sum. Their `diff` sums the achieved share
difficulty of each candidate. Candidates arriving without a worker attribution
(node-relayed blocks) count toward the pool totals only.

These buckets are the ckpool-lane counterpart of the PRISM coordinator's
rejection reason IDs documented in
[`docs/prism-rejections.md`](../docs/prism-rejections.md); the two lanes keep
their own taxonomies because they classify at different layers.

Regression and load coverage lives in
`tests/test_ckpool_rejects_observability.py`; run it against a locally built
patched binary with `make test-ckpool-rejects-observability`.

## Vardiff note

ckpool caps vardiff at current network difficulty. Under qbit regtest, the patched ckpool path floors network difficulty at `1/256`, so Stratum difficulty is expected to stay at `0.00390625` even when a high-rate CPU miner solves hundreds of blocks. This is a regtest artifact, not evidence that vardiff is broken on signet/mainnet-like targets.

For a controlled non-regtest-style probe, run ckpool against [`tests/fake_qbit_rpc.py`](../tests/fake_qbit_rpc.py), which advertises diff-1 work without accepting every share as a qbit block.

## Recommendation

Treat this patched ckpool overlay as the default single-address
permissionless-mining story for qbit. It remains useful as a Stratum/BIP310
comparison target and as a smoke test for qbit P2MR-only block acceptance.

For non-custodial PRISM mining, use the direct PRISM Stratum path documented in
[`PRISM.md`](../PRISM.md). ckpool remains the single-address permissionless
comparison path rather than the multi-output PRISM payout engine.
