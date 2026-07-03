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

## Vardiff note

ckpool caps vardiff at current network difficulty. Under qbit regtest, the patched ckpool path floors network difficulty at `1/256`, so Stratum difficulty is expected to stay at `0.00390625` even when a high-rate CPU miner solves hundreds of blocks. This is a regtest artifact, not evidence that vardiff is broken on signet/mainnet-like targets.

For a controlled non-regtest-style probe, run ckpool against [`tests/fake_qbit_rpc.py`](../tests/fake_qbit_rpc.py), which advertises diff-1 work without accepting every share as a qbit block.

## Recommendation

Treat this patched ckpool overlay as the default single-address
permissionless-mining story for qbit. It remains useful as a Stratum/BIP310
comparison target and as a smoke test for qbit P2MR-only block acceptance.

For non-custodial PRISM mining, use the builder-owned transport decision in
[`docs/pool-base-decision.md`](../docs/pool-base-decision.md). ckpool should not
own the multi-output payout split unless a later spike proves that external
coinbase injection is less invasive than the repo-owned Stratum path.
