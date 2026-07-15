# qbit Chain Parameters For Miners

This reference isolates the parameters a pool or solo-mining operator has to get right.

## Network defaults

| Network | RPC | P2P | Address HRP |
| --- | ---: | ---: | --- |
| Mainnet | 8352 | 8355 | `qb` |
| Testnet3 | 18352 | 18355 | `tq` |
| Testnet4 | 48352 | 48355 | `tq` |
| Signet | 38352 | 38355 | `tq` |
| Regtest | 18452 | 18460 | `qbrt` |

## Compose presets

Compose defaults to regtest. For signet, override:

```bash
QBIT_CHAIN=signet
QBIT_CHAIN_FLAG=-signet
QBIT_NODE_EXTRA_ARG=-signetchallenge=<your_signet_challenge_hex>
QBIT_LISTEN=0
QBIT_RPC_PORT=38352
QBIT_P2P_PORT=38355
QBIT_RPC_PORT_HOST=127.0.0.1:38352
QBIT_P2P_PORT_HOST=127.0.0.1:38355
QBIT_MINER_ADDRESS=auto
CKPOOL_MINDIFF=<reviewed-share-floor>
CKPOOL_STARTDIFF=<reviewed-starting-share-difficulty>
CKPOOL_MAXDIFF=<optional-share-difficulty-cap>
```

Notes:

- `QBIT_MINER_ADDRESS=auto` asks the helper services to derive the chain-default payout address from qbit. On public chains that means P2MR.
- `QBIT_LISTEN=0`, loopback-only `QBIT_RPC_PORT_HOST`, and loopback-only `QBIT_P2P_PORT_HOST` keep qbitd from exposing RPC or advertising as a reachable peer by default.
- If you set the payout address manually on signet, use a signet P2MR address with the `tq` HRP.
- Non-regtest ckpool startup requires explicit `CKPOOL_MINDIFF` and `CKPOOL_STARTDIFF` so share-difficulty policy remains deployment-reviewed.
- The bundled local shell tests remain on regtest even when compose is switched to signet.

## Consensus and mining constants

| Parameter | Value |
| --- | --- |
| Aggregate target spacing | `60` seconds |
| Permissionless lane spacing | `75` seconds |
| AuxPoW lane spacing | `300` seconds |
| Difficulty algorithm | ASERT |
| ASERT half-life | `7200` seconds |
| Difficulty timespan compatibility value | `1209600` seconds on public nets, `86400` on regtest |
| AuxPoW chain ID | `47` mainnet, `31430` testnet4 |
| Max block serialized size | `2,000,000` bytes |
| Max block weight | `2,000,000` |
| Max sigops cost | `80,000` |
| Witness scale factor | `1` |
| Coinbase maturity | `1000` blocks |

## qbit block version layout

qbit reuses BIP9 top bits but repacks the rest of the 32-bit version field:

```text
[top_bits:3][chain_id:16][reserved:4][auxpow_flag:1][version_bits:8]
```

Constants:

- BIP9 top bits: `0x20000000`
- Chain ID shift: `13`
- AuxPoW flag: `0x00000100`
- Low signalling bits: low `8` bits

Operational consequences:

- Permissionless blocks use `chain_id=0` and `auxpow=false`
- Mainnet AuxPoW blocks use `chain_id=47` and `auxpow=true`
- Testnet4 AuxPoW blocks use `chain_id=31430` and `auxpow=true`
- Pools should preserve the returned `version` from `getblocktemplate`
- Manually setting reserved bits or the wrong chain ID causes header rejection

## RPC expectations

Mining-relevant `getblocktemplate` fields returned by qbit:

- `version`
- `rules`
- `vbavailable`
- `vbrequired`
- `previousblockhash`
- `transactions`
- `coinbasevalue`
- `target`
- `mintime`
- `noncerange`
- `sigoplimit`
- `sizelimit`
- `weightlimit`
- `curtime`
- `bits`
- `height`
- `default_witness_commitment`

qbit still follows normal BIP22/BIP23 semantics:

- request `rules=["segwit"]`
- proposal mode uses `mode="proposal"` plus `data`
- accepted `submitblock` or proposal results come back as JSON `null`

`createauxblock(address)` returns:

- `hash`
- `chainid`
- `previousblockhash`
- `coinbasevalue`
- `bits`
- `height`
- `target`

`submitauxblock(hash, auxpow_hex)` returns:

- JSON `null` on accept
- `stale-prevblk` when the cached candidate's qbit tip changed or the candidate aged past `-auxpowtemplateexpiry`
- a qbit rejection string for payload or PoW failures

## Operational gotchas

- `8355` is mainnet P2P, not RPC
- qbit RPC defaults are `8352`, `18352`, `48352`, `38352`, and `18452`
- Signet deployments must use the challenge configured for that signet.
- `WITNESS_SCALE_FACTOR=1` means weight tracks serialized size
- `COINBASE_MATURITY=1000` must be configured explicitly in pool software
- Public qbit networks use P2MR-only wallet output types; use default wallet-generated payout addresses and keep payout transactions inside qbit's allowed output policy
- Non-test chains require both peer connectivity and `initialblockdownload=false` before mining RPCs succeed
