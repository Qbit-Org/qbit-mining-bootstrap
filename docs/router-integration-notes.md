# Router Integration Notes

These notes capture public operator guidance for placing a router or hash
aggregator in front of the qbit permissionless ckpool path.

## Compatibility Baseline

- Route permissionless qbit miners to ckpool's Stratum listener.
- Keep qbit payout usernames chain-correct; public qbit chains require P2MR
  payout addresses.
- Point ckpool at qbit RPC, not qbit P2P. Mainnet qbit RPC is `8352`; mainnet
  qbit P2P is `8355`.
- Preserve qbit's returned block version unless the miner negotiated version
  rolling inside the configured mask.

## Version Rolling

The bootstrap ckpool image starts with `CKPOOL_VERSION_MASK_MODE=dynamic`. At
startup it asks qbitd for `getblocktemplate` and uses
`versionrollingmask` when the connected node exposes it. Older qbitd builds
fall back to the configured `CKPOOL_VERSION_MASK`; the public sample env uses
`1fffe000` to match current qbitd permissionless templates.

Routers that negotiate BIP310 should only pass miner-controlled version bits
inside the mask granted by ckpool. If a miner requests a mask, the effective
mask is the intersection of the miner's requested mask and qbit's configured
mask. If qbitd returns `versionrollingmask=00000000`, routers should treat
version rolling as disabled for that upstream.

## Vardiff

ckpool caps vardiff at current network difficulty. Under qbit regtest, the
patched ckpool path floors network difficulty at `1/256`, so Stratum
difficulty is expected to stay at `0.00390625` even when a CPU miner solves
many local blocks. Treat that as a regtest artifact.

For signet or mainnet-like deployments, set `CKPOOL_MINDIFF`,
`CKPOOL_STARTDIFF`, and optionally `CKPOOL_MAXDIFF` based on expected worker
hashrate and desired share volume. Do not reuse regtest share-difficulty
defaults for production-facing routers without retuning. Bootstrap now fails
closed on non-regtest chains when the required min/start difficulty values are
missing.

## Username Handling

Use the qbit payout address as the leading username segment. If the router adds
worker identity, append it after a separator that the router can parse
consistently, for example:

```text
<qbit-payout-address>.<worker-id>
```

Validate malformed, empty, overlong, or control-character worker suffixes at
the router boundary before forwarding to ckpool.

## Operational Checks

- Confirm `getblocktemplate '{"rules":["segwit"]}'` succeeds before admitting
  miners.
- Confirm submitted blocks are accepted by qbit through `submitblock`.
- Exercise reconnect storms and ensure miners receive a fresh notify after a
  qbit tip change.
- Alert on slow clean-job propagation from qbit tip changes to miner notify.
- Keep RPC credentials deployment-specific, and keep published qbit RPC ports
  loopback-only unless the deployment has explicit firewalling and auth.
