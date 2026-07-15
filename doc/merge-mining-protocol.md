# qbit Merge-Mining Protocol Walkthrough

qbit exposes a Namecoin-style AuxPoW interface:

- `createauxblock(payout_address)`
- `submitauxblock(hash, auxpow_hex)`

The node caches the full candidate block after `createauxblock`, so the miner only has to construct and submit the serialized `CAuxPow` payload.

## 1. Request the aux template

Call `createauxblock` with a qbit payout address. qbit returns:

- `hash`: aux block hash
- `chainid`: qbit chain ID, `47` on mainnet and `31430` on testnet4
- `previousblockhash`
- `coinbasevalue`
- `bits`
- `height`
- `target`

qbit also:

- rewrites the block version into AuxPoW form
- updates the block time
- recalculates required work
- stores the full block in an internal cache

## 2. Build the chain commitment

The parent coinbase `scriptSig` must commit to the aux block hash through a chain merkle root. qbit accepts the merged-mining header:

```text
fabe6d6d || chain_merkle_root || merkle_size_le32 || nonce_le32
```

Use the byte order returned by `createauxblock.commitmentorder` for each candidate:

- `display`: the standard 32-byte chain merkle root in display/big-endian order, the same byte order used by RPC hash hex strings. Code that has `ser_uint256(chain_merkle_root)` must reverse those 32 bytes before appending the root to the parent coinbase `scriptSig`. With an empty chain merkle branch, the committed root bytes are `bytes.fromhex(createauxblock.hash)`.
- `internal`: qbit's historical compatibility mode. Commit Bitcoin Core's internal little-endian `uint256` serialization directly, equivalent to `ser_uint256(chain_merkle_root)`.

New qbit nodes also return `commitmentactivationheight` so operators can see where `commitmentorder` switches. Older qbit nodes that omit `commitmentorder` used the historical internal order.

Rules enforced by qbit:

- the merged-mining header may appear at most once
- if present, it must be immediately followed by the chain merkle root in the candidate's selected byte order
- if omitted, the chain merkle root in the candidate's selected byte order must appear within the first 20 bytes of the `scriptSig`
- the footer must contain both `merkle_size` and `nonce`

## 3. Compute the deterministic slot index

The chain index is derived from the nonce and chain ID, not chosen arbitrarily:

```text
rand = LCG(nonce)
rand = (rand + chain_id) mod 2^32
rand = LCG(rand)
index = rand & ((1 << merkle_height) - 1)
```

For qbit:

- mainnet uses `chain_id = 47`
- testnet4 uses `chain_id = 31430`
- the chain merkle branch height must be at most `30`

If the submitted `chain_index` does not match qbit's recomputation, the node rejects the block with `bad-auxpow-chain-index`.

## 4. Serialize `CAuxPow`

qbit serializes AuxPoW as:

1. Parent coinbase transaction, without witness
2. Coinbase merkle branch vector
3. Coinbase branch index as signed little-endian `int32`
4. Chain merkle branch vector
5. Chain index as signed little-endian `int32`
6. Parent block header (`CPureBlockHeader`)

Validation rules:

- the coinbase transaction must really be a coinbase
- `coinbase_branch_index` must be `0`
- merkle branches must match the declared indices
- the parent block merkle root must match the coinbase merkle branch result
- the parent block hash must satisfy qbit's `bits`

## 5. Submit the payload

Send the original `hash` from `createauxblock` plus the serialized AuxPoW hex to `submitauxblock`.

Result handling:

- JSON `null`: accepted
- `stale-prevblk`: cached candidate is gone because the qbit tip changed or the template age exceeded qbit's `-auxpowtemplateexpiry`
- `bad-auxpow-*`: payload failed a specific validation step

Rejected submissions remain retryable until qbit either accepts one payload for that cached candidate or the cache entry expires.

## 6. Cache behavior

The qbit node keeps a same-tip cache of AuxPoW candidates:

- keyed by aux block hash
- pruned whenever the active tip changes
- pruned when the candidate exceeds `-auxpowtemplateexpiry`, which defaults to `60` minutes
- accepted entries removed immediately after success

This matters operationally:

- long-lived parent work can go stale if the qbit tip changes or the candidate ages past the node expiry window
- bridge and pool software should issue replacement work before the qbit expiry window; the bundled Stratum bridge defaults to a 45 minute age refresh

## 7. Practical implementation pattern

The safest way to stay aligned with qbit consensus is:

1. call `createauxblock`
2. feed the JSON result into qbit's own test-framework helper
3. submit the returned `auxpow_hex`

`examples/python-auxpow-payload.py` implements that pattern so the repo does not need to maintain an independent AuxPoW serializer. Pass `--qbit-src` or set `QBIT_SRC_DIR` so it can import qbit's functional-test helper from a separate checkout.
When the template includes `commitmentorder`, the wrapper passes that value to the helper and refuses qbit helper checkouts that do not support the activation-aware `commitment_order` argument.
