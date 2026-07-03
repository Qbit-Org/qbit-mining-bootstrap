# qbit-pool-builder

Standalone Rust builder for deterministic qbit P2MR coinbase transactions and
signed payout manifests.

The crate is transport-independent: Stratum, share logging, and reward-window
selection live outside this crate. Callers provide a coinbase value, block
height, and weighted payout entitlements. The builder deterministically sorts
entitlements, allocates every satoshi with largest-remainder rounding, emits only
P2MR payout scripts plus the mandatory witness commitment output, and signs a
canonical manifest for audit.

For Stratum transports, `CoinbaseBuildRequest.coinbase_script_sig_suffix_hex`
can append deterministic caller-owned bytes after the BIP34 height push. Use
that for extranonce placeholders when issuing work and rebuild the final signed
manifest with the actual extranonce bytes from the accepted block share. The
payout outputs and witness commitment remain builder-owned; the transport only
varies coinbase input entropy.

Run unit tests:

```sh
make test-builder
```

Run the local qbit regtest reproduction:

```sh
QBIT_BIN_DIR=/path/to/qbit/build/bin make test-builder-regtest
```

That smoke derives real regtest P2MR addresses, builds Rust coinbases for
N=1/50/500 payouts, submits the solved blocks through `submitblock`, and verifies
the on-chain coinbase bytes match the signed payout manifest.
