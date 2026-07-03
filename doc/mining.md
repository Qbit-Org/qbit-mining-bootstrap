# qbit Mining Operator Guide

This guide covers the two mining paths qbit exposes today:

- Standard permissionless mining over `getblocktemplate` and `submitblock`
- Namecoin-style merge mining over `createauxblock` and `submitauxblock`

No qbit-specific wire protocol is required. From a pool stack's perspective qbit behaves like a Bitcoin Core fork on the permissionless path, plus an additional AuxPoW RPC pair for merged mining.

This guide is written in two layers:

- `lab` mode, which uses the repo's simulator/coordinator helpers to prove the RPCs and consensus behavior
- `operator` mode, which is the real deployment shape: `qbitd` plus a real miner, pool, or merge-mining bridge

The repo currently supports two development-time qbit providers:

- `source`: use a local qbit checkout via `QBIT_SRC_DIR`
- `git`: clone the configured qbit repo/ref from `config/upstream.env`, with optional exact commit pinning

Each `make` target stages a clean qbit tree before building containers, so host build artifacts do not leak into the lab. Once qbit publishes release artifacts, the same operator flow can point at a pinned image or tarball without changing the mining steps below.

## Chain parameter cheat sheet

| Parameter | qbit value | Pool impact |
| --- | --- | --- |
| Mainnet RPC / P2P ports | `8352` / `8355` | Point pool RPC at `8352`, not `8355` |
| Aggregate target spacing | `60s` | Faster block cadence than Bitcoin |
| Permissionless lane spacing | `75s` | Share difficulty should start below Bitcoin defaults |
| AuxPoW lane spacing | `300s` | Merge-mined blocks arrive about every 5 minutes |
| Public-chain payout policy | P2MR only | Use qbit's default `getnewaddress`; do not force `bech32`, taproot, or legacy types |
| Address HRPs | `qb` mainnet, `tq` testnet/signet, `qbrt` regtest | Manual payout addresses must match the chain |
| Max block serialized size / weight | `2,000,000` / `2,000,000` | No witness discount; size and weight track together |
| Witness scale factor | `1` | Software assuming `4` will overestimate vsize and fee rates |
| Coinbase maturity | `1000` blocks | Do not hardcode `100` |
| AuxPoW chain ID | `31430` | Required for merge-mining slot selection and version layout |
| ASERT half-life | `2 hours` | Difficulty reacts faster than Bitcoin's epoch retarget |

## Address formats

Public qbit chains use P2MR-only wallet output types. The bootstrap repo therefore treats qbit itself as the source of truth for payout-address derivation.

Recommended default:

- Use `qbit-cli getnewaddress` for payout addresses
- If you need to force the type explicitly, use `qbit-cli getnewaddress "" p2mr`
- Do not force `bech32`, taproot, or legacy output types on public chains

Chain HRPs:

- Mainnet: `qb`
- Testnet/signet: `tq`
- Regtest: `qbrt`

## Regtest vs Signet

The compose stack supports two qbit modes:

- `regtest` is the default and keeps the built-in `make test-*` flows deterministic.
- `signet` points qbit at a configured signet while reusing the same compose services and helper scripts.

Signet quickstart:

```bash
export QBIT_CHAIN=signet
export QBIT_CHAIN_FLAG=-signet
export QBIT_NODE_EXTRA_ARG=-signetchallenge=<your_signet_challenge_hex>
export QBIT_LISTEN=0
export QBIT_RPC_PORT=38352
export QBIT_P2P_PORT=38355
export QBIT_RPC_PORT_HOST=127.0.0.1:38352
export QBIT_P2P_PORT_HOST=127.0.0.1:38355
export QBIT_MINER_ADDRESS=auto
export CKPOOL_MINDIFF=<reviewed-share-floor>
export CKPOOL_STARTDIFF=<reviewed-starting-share-difficulty>
export CKPOOL_MAXDIFF=<optional-share-difficulty-cap>
make up-permissionless-pool
```

What those settings do:

- `QBIT_CHAIN_FLAG=-signet` starts qbit on signet.
- `QBIT_NODE_EXTRA_ARG=-signetchallenge=<your_signet_challenge_hex>` selects your signet challenge.
- `QBIT_RPC_PORT=38352` and `QBIT_P2P_PORT=38355` align the repo with qbit signet defaults.
- `QBIT_LISTEN=0`, loopback-only `QBIT_RPC_PORT_HOST`, and loopback-only `QBIT_P2P_PORT_HOST` keep qbitd from exposing RPC or advertising as a reachable peer by default.
- `QBIT_MINER_ADDRESS=auto` lets the helpers derive the chain-default payout address. On public chains that resolves to P2MR.
- `CKPOOL_MINDIFF` and `CKPOOL_STARTDIFF` must be explicit on non-regtest chains so bootstrap does not silently choose launch share-difficulty policy.

Keep the separation clear:

- `make test-permissionless`, `make test-auxpow`, and the local `test/` shell smokes remain regtest-only.
- Signet mode is for operator bring-up against a qbit signet, not for deterministic local mining proofs.

## Permissionless mining quickstart

### Node config

Start `qbitd` with RPC enabled and pool-friendly hooks:

```ini
server=1
txindex=0
rpcuser=qbitrpc
rpcpassword=change-this
rpcbind=127.0.0.1
rpcallowip=127.0.0.1
blocknotify=/path/to/on-block %s
zmqpubrawblock=tcp://127.0.0.1:28332
zmqpubrawtx=tcp://127.0.0.1:28333
```

Production notes:

- Use `rpcauth` instead of `rpcuser` and `rpcpassword` when you move beyond local testing.
- Mainnet JSON-RPC defaults to `8352`.
- Keep `listen=0` unless this pool is also meant to be a public qbit peer; mining only needs RPC plus outbound peers.
- On non-test chains qbit enforces the standard mining preconditions: the node must have at least one peer and must not be in initial block download.

### Request a block template

qbit's GBT path is standard BIP22/BIP23. The default rule set is `["segwit"]`; signet callers must request `["segwit","signet"]`.

```bash
curl -u qbitrpc:change-this \
  --data-binary '{"jsonrpc":"1.0","id":"qbit","method":"getblocktemplate","params":[{"rules":["segwit"]}]}' \
  http://127.0.0.1:8352/
```

For signet, change the payload to `{"rules":["segwit","signet"]}`.

Mining-critical fields to consume:

- `version`: canonical qbit block version, with `chain_id=0` and `auxpow=false` on permissionless candidates
- `previousblockhash`
- `transactions`
- `coinbasevalue`
- `bits`
- `target`
- `curtime`
- `height`
- `default_witness_commitment`

Operational notes:

- qbit canonicalizes BIP9-style versions into its version layout before returning them.
- `submitblock` still ignores its second BIP22 compatibility argument.
- `weightlimit` is `2000000` and `WITNESS_SCALE_FACTOR=1`, so do not reuse Bitcoin's witness-discount arithmetic.

### Build and submit the block

At minimum a valid permissionless block must:

1. Reuse the template's `version`, `previousblockhash`, `bits`, and `curtime`
2. Build a coinbase at `height`
3. Set the coinbase output value to `coinbasevalue`
4. Add the witness commitment when segwit rules are active
5. Solve the header and submit the full block hex through `submitblock`

The regtest example in [`test/test-permissionless.sh`](../test/test-permissionless.sh) does exactly this by reusing qbit's functional-test block-building helpers.

### Real miner path

To turn this into a day-1 operator setup, swap the simulator for a real Stratum miner:

1. Start `qbitd`
2. Start `ckpool`
3. Point your miner at the ckpool Stratum port
4. Use a qbit payout address as the worker username
5. Keep the `qbit` RPC side and the miner side separate so the node stays a node

The simulator in [`lab/miner-sim/miner_sim.py`](../lab/miner-sim/miner_sim.py) stays in the repo as the regression harness, but it is not the production miner path.

Operator-lab commands:

```bash
make up-permissionless-pool
make test-real-miner
```

- `make up-permissionless-pool` starts `qbitd + ckpool` and waits for a real external miner
- `make test-real-miner` uses the bundled `cpuminer-opt` client as an operator-facing smoke test
- `make test-permissionless-p2mr` runs the same permissionless stack with `-p2mronly=1` and forces
  `QBIT_MINER_ADDRESS=auto` so local non-P2MR payout overrides do not mask the restricted-output smoke

### Regtest-only difficulty helpers

The repo intentionally keeps the ckpool fractional-difficulty compatibility patch on every chain, but only applies the `1/256` difficulty floor on regtest.

- Regtest builds keep the `1/256` floor in the ckpool image and in the runtime defaults.
- Signet builds keep the generic qbit compatibility changes, but skip the regtest floor so signet does not silently inherit regtest difficulty assumptions.
- Non-regtest ckpool startup requires explicit `CKPOOL_MINDIFF` and `CKPOOL_STARTDIFF`; set them from a reviewed operator policy instead of inheriting bootstrap defaults.
- `CKPOOL_MAXDIFF` is optional and caps ckpool's vardiff-selected share difficulty when you need an operator-facing upper bound.
- The bundled `cpuminer-opt` patch only maps an exact zero share difficulty to `1/256`; signet mode does not depend on that behavior.

## Merge-mining quickstart

### Create a cached AuxPoW candidate

```bash
curl -u qbitrpc:change-this \
  --data-binary '{"jsonrpc":"1.0","id":"qbit","method":"createauxblock","params":["<your-qbit-payout-address>"]}' \
  http://127.0.0.1:8352/
```

Expected response fields:

- `hash`
- `chainid`
- `previousblockhash`
- `coinbasevalue`
- `bits`
- `height`
- `target`

Implementation notes:

- qbit rebuilds the candidate with your payout script, forces the AuxPoW version bit, recalculates time and difficulty, and caches the full block internally.
- The cache is keyed by aux block hash.
- The cache is pruned when the active qbit tip changes.
- Same-tip templates remain available until qbit's `-auxpowtemplateexpiry` age window is exceeded. qbit defaults this to `60` minutes; `0` disables age expiry.

### Build the AuxPoW payload

The AuxPoW payload itself is serialized as:

1. Parent coinbase transaction, without witness
2. Coinbase merkle branch
3. Coinbase branch index
4. Chain merkle branch
5. Chain index
6. Parent block header

Commitment rules qbit enforces:

- The merged-mining magic is `0xfabe6d6d`
- `createauxblock.commitmentorder` selects the root byte order for that candidate: `internal` uses `ser_uint256(chain_root)` for historical qbit compatibility, and `display` uses `ser_uint256(chain_root)[::-1]` for the standard display/big-endian order
- New qbit nodes also return `commitmentactivationheight` so operators can see where the response switches; older qbit nodes that omit `commitmentorder` used the historical internal order
- If the magic header is present, it must appear exactly once and be immediately followed by the chain merkle root in the selected byte order
- After the chain merkle root come two little-endian `uint32`s: `merkle_size` and `nonce`
- The slot index is deterministic from the nonce and chain ID; it is not free-form
- `coinbase_branch_index` must be `0`
- Parent PoW must satisfy qbit's target bits

The helper in `examples/python-auxpow-payload.py` reads the `createauxblock` JSON, reuses qbit's own functional-test helper, and prints a valid `auxpow_hex`. Point it at a separate qbit checkout with `--qbit-src` or `QBIT_SRC_DIR`.
When the template includes `commitmentorder`, the wrapper passes that value to the helper and refuses qbit helper checkouts that do not support the activation-aware `commitment_order` argument.

### Real merge-mining path

The production-shaped version of this flow is:

1. `qbitd` produces AuxPoW templates
2. a real bridge or pool service requests templates and builds parent-chain work
3. Bitcoin parent miners solve the parent block
4. the bridge submits the AuxPoW payload back to qbit

The current [`lab/auxpow/auxpow_coordinator.py`](../lab/auxpow/auxpow_coordinator.py) is the reference bridge for regtest and a signet-capable qbit RPC client, not the final production pool.

Operator-lab commands:

```bash
make up-auxpow
make up-auxpow-bridge
make test-auxpow
make test-auxpow-stratum-bip310
```

- `make up-auxpow` runs the one-shot coordinator path without the test harness wrapper
- `make up-auxpow-bridge` keeps a long-running bridge alive on the configured qbit chain
- `make test-auxpow` runs the deterministic one-shot positive and negative-path checks
- `make test-auxpow-stratum-bip310` checks the Python bridge's BIP310 mask negotiation

The bundled Stratum bridge refreshes miner jobs when either chain tip changes and also age-refreshes jobs before qbit's default template expiry. The default `AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS=2700` is 45 minutes, leaving a 15 minute buffer before qbit's default 60 minute `-auxpowtemplateexpiry` window. Set it lower for faster job rotation or `0` to disable age-based bridge refresh.

AuxPoW Stratum vardiff is enabled by default in compose so mixed miner hardware does not require one fixed share-difficulty setting. Each miner connection starts at `AUXPOW_STRATUM_VARDIFF_STARTUP_DIFF` (or `AUXPOW_STRATUM_SHARE_DIFF` when unset), retargets every `AUXPOW_STRATUM_VARDIFF_RETARGET_SECONDS` toward `AUXPOW_STRATUM_VARDIFF_TARGET_SHARE_SECONDS` or `AUXPOW_STRATUM_VARDIFF_TARGET_SHARES_PER_SECOND`, and clamps movement with `AUXPOW_STRATUM_VARDIFF_MIN_DIFF`, `AUXPOW_STRATUM_VARDIFF_MAX_DIFF`, and `AUXPOW_STRATUM_VARDIFF_MAX_STEP_FACTOR`. Set `AUXPOW_STRATUM_VARDIFF_ENABLED=0` to keep the fixed advertised `AUXPOW_STRATUM_SHARE_DIFF` behavior.

The bridge reconstructs one canonical parent header from each Stratum submit. Diagnostic header variants remain available with `AUXPOW_STRATUM_DIAG_VARIANTS=1`, but they are not accepted by default. Use `AUXPOW_STRATUM_HEADER_VARIANT=<variant>` or `AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT=1` only while debugging a miner compatibility issue.

BIP310 version rolling defaults to `AUXPOW_STRATUM_VERSION_MASK=1fffe000`, and the bridge grants miners the intersection of that parent-chain mask and the requested mask. Permissionless qbit, ckpool, and direct PRISM Stratum use the connected qbit node's `getblocktemplate.versionrollingmask` as the server mask, falling back to `1fffe000` only when GBT is unavailable or does not expose the field. AuxPoW Stratum mode still serves parent-chain headers and submits child work through `createauxblock` / `submitauxblock`; when the connected qbit node exposes `versionrollingmask`, the bridge logs it at startup as compatibility context.

Useful diagnostics:

- `AUXPOW_STRATUM_DIAG_JSONL=1`: emit structured JSON lines for startup, jobs, submits, variants, and submissions
- `AUXPOW_STRATUM_DIAG_EVENTS=1`: emit compact human-readable diagnostic events
- `AUXPOW_STRATUM_STATS_INTERVAL_SECONDS=60`: print per-worker counters and accepted-share rate
- `AUXPOW_STRATUM_EXPECTED_HASHRATES='{"worker": 1000000000000}'`: compare observed shares/sec against expected hashrate

Signet note:

- The coordinator now derives or validates a chain-correct qbit payout address before calling `createauxblock`.
- The bundled parent side still uses the local `bitcoind` helper by default, which is deterministic on regtest.
- If your signet qbit target is too high for the bundled brute-force parent loop, that is the remaining blocker for accepted `submitauxblock` results; use a real parent-chain miner or pool in that case.

### Submit the AuxPoW

```bash
curl -u qbitrpc:change-this \
  --data-binary '{"jsonrpc":"1.0","id":"qbit","method":"submitauxblock","params":["<hash>","<auxpow_hex>"]}' \
  http://127.0.0.1:8352/
```

Result semantics:

- Accepted block: JSON `null`
- Expired cached template or template from a previous qbit tip: `stale-prevblk`
- Invalid payload: standard qbit rejection string such as `bad-auxpow-parent-hash` or `bad-auxpow-commitment`

Important behavior:

- Rejected payloads remain retryable until one is accepted or the cached template expires
- Accepted submissions remove the cached template entry

## Payout and wallet guidance

- Coinbase maturity is `1000` blocks on every network.
- At qbit's aggregate `60s` cadence, that is about `16.7` hours.
- Many Bitcoin-oriented pool stacks hardcode `100`. Override this explicitly before enabling payouts.
- Use a dedicated payout wallet or descriptor set for pool rewards.
- Public qbit networks use P2MR-only wallet output types. Generate payout addresses with qbit itself, and keep pool payout transactions inside qbit's allowed output policy.
- Direct PRISM Stratum uses the leading username segment as the payout address. On qbit test chains, invalid username payouts fall back to `tq1zlsq9dpxz8mennhdpr9nf9s0f2tjtq6gxs9m84k6xglhkfp92q2zszzu4m3`; set `PRISM_USERNAME_FALLBACK_ADDRESS` to override that address.
- Direct PRISM Stratum does not emit miner outputs below the payout floor. The default floor is `14,720` bits from `3,680` P2MR spend bytes, `1` bit/byte, and a `4x` safety multiplier. Set `PRISM_PAYOUT_MIN_OUTPUT_BITS` for a fixed absolute floor, or tune `PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES`, `PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE`, and `PRISM_PAYOUT_SAFETY_MULTIPLIER`. The old `_SATS` payout env names are still accepted as legacy aliases.
- Until DATUM-style per-hardware templates are supported, use conservative non-DATUM settlement caps for one stock-firmware-safe Stratum template. The qbit-prism launch defaults are `12` direct recipient coinbase outputs, `16` total settlement coinbase outputs, and `1,000` recipients per CTV fanout chunk. These are hardware policy defaults, not qbit consensus limits.
- Monitor `/audit/carry-forward-integrity` for carry-forward replay mismatches and mirror the published `audit_head_sha256`. Launch policy is no compaction of carry-forward rows until signed checkpoints cover the old row range and replacement opening balances.
- Expect more frequent UTXO creation than on Bitcoin if you pay out on a block-count schedule instead of a balance threshold.

## Share difficulty guidance

qbit targets roughly ten times Bitcoin's aggregate block frequency. If your pool software seeds share difficulty from Bitcoin assumptions, it will usually start too high.

Practical starting point:

- Start your initial share difficulty at roughly one tenth of your Bitcoin default
- Use `CKPOOL_MINDIFF`, `CKPOOL_STARTDIFF`, and optionally `CKPOOL_MAXDIFF` to bound ckpool vardiff behavior
- Document a lower floor for small solo miners so shares do not become too sparse
- Keep `CKPOOL_PUBLIC_DIFF_POLICY=explicit` for signet/mainnet-style deployments so startup fails before serving miners when policy values are missing

Treat this as an operational tuning baseline, not a consensus rule.

Regtest caveat:

- The local regtest floor is intentionally `1/256`, and ckpool caps vardiff at current network difficulty.
- Because qbit regtest's true network difficulty is below that floor, vardiff is expected to stay at `0.00390625` there even at high hash rates.
- Validate production vardiff behavior against signet/mainnet-like targets, or against the controlled fake-RPC probe in [`tests/fake_qbit_rpc.py`](../tests/fake_qbit_rpc.py), instead of using regtest block-solve cadence as the signal.

## Troubleshooting

### `stale-prevblk`

The cached aux template no longer matches the active tip, or it aged past qbit's `-auxpowtemplateexpiry` window. Request a fresh `createauxblock` result and rebuild the AuxPoW payload. For Stratum bridge mode, keep `AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS` below qbit's template expiry so miners receive replacement work before the node ages out the cached candidate.

### `bad-auxpow-parent-hash`

The parent header in the AuxPoW payload does not satisfy qbit's target bits. Rebuild the payload and solve the parent header again.

### `bad-auxpow-commitment`

The parent coinbase scriptSig does not contain a valid merged-mining commitment. Verify:

- the `0xfabe6d6d` magic header
- the committed chain merkle root
- the trailing `merkle_size` and `nonce`
- the chain root placement inside the scriptSig

### `AuxPow decode failed`

Your `auxpow_hex` is not valid serialized `CAuxPow`. Re-serialize the payload exactly as documented in [`doc/merge-mining-protocol.md`](./merge-mining-protocol.md).

### GBT or `submitblock` issues

- Ensure you request GBT with `rules=["segwit"]`
- Preserve the returned `version`
- Include the witness commitment
- Use `getblockchaininfo` to confirm the node is not in IBD on non-test chains
- Confirm the node has peers before expecting mining RPCs to work on mainnet-like networks

### Fee and size mismatches

If your stack assumes Bitcoin's witness discount, it will overestimate vsize and fee density on qbit. Use serialized size or weight directly, because qbit sets `WITNESS_SCALE_FACTOR=1`.
