#!/usr/bin/env python3

from __future__ import annotations

import unittest
from decimal import Decimal

from lab.prism.ctv_broadcaster import (
    AWAITING_MATURITY,
    BROADCAST,
    BROADCASTABLE,
    CONFIRMED,
    CPFP_CHILD_TX_VERSION,
    REORGED,
    BroadcasterError,
    BroadcastAttempt,
    CtvFanoutBroadcaster,
    FanoutArtifact,
    RpcError,
    build_unsigned_cpfp_child,
)

FANOUT_TXID = "ab" * 32
CHILD_TXID = "cc" * 32
COINBASE_BLOCK = "0b" * 32
MATURITY = 10


def artifact() -> FanoutArtifact:
    return FanoutArtifact(
        fanout_txid=FANOUT_TXID,
        fanout_tx_hex="03000000ff",  # opaque to the fake node
        anchor_vout=8,
        coinbase_txid="2c" * 32,
        coinbase_block_hash=COINBASE_BLOCK,
        coinbase_height=100,
        parent_coinbase_vout=3,
    )


def no_anchor_artifact() -> FanoutArtifact:
    return FanoutArtifact(
        fanout_txid=FANOUT_TXID,
        fanout_tx_hex="03000000ff",  # opaque to the fake node
        anchor_vout=None,
        coinbase_txid="2c" * 32,
        coinbase_block_hash=COINBASE_BLOCK,
        coinbase_height=100,
        parent_coinbase_vout=3,
    )


class FakeRpc:
    def __init__(
        self,
        *,
        tip: int = 200,
        mempool=(),
        confirmed=(),
        reorged_blocks=(),
        missing_blocks=(),
        transient_missing_blocks=(),
        parent_spent_on_chain: bool = False,
        parent_spender_txid: str = FANOUT_TXID,
    ) -> None:
        self.tip = tip
        self.mempool = set(mempool)
        self.confirmed = set(confirmed)
        self.reorged_blocks = set(reorged_blocks)
        self.missing_blocks = set(missing_blocks)
        # Block hashes that raise on the first getblock and succeed afterward,
        # modelling a transient node error mid-scan.
        self.transient_missing_blocks = set(transient_missing_blocks)
        self.parent_spent_on_chain = parent_spent_on_chain
        self.parent_spender_txid = parent_spender_txid
        self.submitted_packages: list = []
        self.submitted_raw_transactions: list[str] = []

    def __call__(self, method, params=None, *, wallet=None):
        params = params or []
        if method == "getblockchaininfo":
            return {"blocks": self.tip}
        if method == "getrawmempool":
            return list(self.mempool)
        if method == "getrawtransaction":
            txid = params[0]
            if txid in self.confirmed:
                return {"confirmations": 6}
            if txid in self.mempool:
                return {"confirmations": 0}
            raise RpcError("getrawtransaction", "No such mempool transaction")
        if method == "getblockhash":
            return f"{int(params[0]):064x}"
        if method == "getblock":
            block_hash = params[0]
            if block_hash in self.transient_missing_blocks:
                self.transient_missing_blocks.discard(block_hash)
                raise RpcError("getblock", "Block temporarily unavailable")
            if block_hash in self.missing_blocks:
                raise RpcError("getblock", "Block not found")
            if block_hash in self.reorged_blocks:
                return {"confirmations": -1}
            if len(params) > 1 and int(params[1]) == 2:
                txs = []
                if self.parent_spent_on_chain and block_hash == f"{self.tip:064x}":
                    txs.append(
                        {
                            "txid": self.parent_spender_txid,
                            "vin": [
                                {
                                    "txid": artifact().coinbase_txid,
                                    "vout": artifact().parent_coinbase_vout,
                                }
                            ],
                        }
                    )
                return {"confirmations": 1, "tx": txs}
            return {"confirmations": 10}
        if method == "gettxout":
            txid, vout, include_mempool = params
            if txid == artifact().coinbase_txid and int(vout) == artifact().parent_coinbase_vout:
                if self.parent_spent_on_chain:
                    return None
                if bool(include_mempool) and FANOUT_TXID in self.mempool:
                    return None
                return {"bestblock": COINBASE_BLOCK, "confirmations": 10, "value": Decimal("1.0")}
            raise RpcError("gettxout", "No such txout")
        if method == "listunspent":
            return [
                {"txid": "cd" * 32, "vout": 0, "amount": Decimal("1.0"), "spendable": True}
            ]
        if method == "getnewaddress":
            return "qbrt1zchange"
        if method == "getaddressinfo":
            return {"scriptPubKey": "5220" + "11" * 32}
        if method == "signrawtransactionwithwallet":
            return {"hex": params[0], "complete": True}
        if method == "submitpackage":
            self.submitted_packages.append(params[0])
            return {
                "package_msg": "success",
                "tx-results": {
                    "wA": {"txid": FANOUT_TXID},
                    "wB": {"txid": CHILD_TXID},
                },
            }
        if method == "sendrawtransaction":
            self.submitted_raw_transactions.append(str(params[0]))
            self.mempool.add(FANOUT_TXID)
            return FANOUT_TXID
        raise RpcError(method, "unexpected method in FakeRpc")


def broadcaster(fake: FakeRpc) -> CtvFanoutBroadcaster:
    return CtvFanoutBroadcaster(fake, funding_wallet="broadcaster", maturity=MATURITY)


def direct_broadcaster(fake: FakeRpc) -> CtvFanoutBroadcaster:
    return CtvFanoutBroadcaster(fake, maturity=MATURITY)


class BuildChildTests(unittest.TestCase):
    def test_structure_and_change(self) -> None:
        unsigned, change = build_unsigned_cpfp_child(
            FANOUT_TXID, 8, "cd" * 32, 1, 50_000, 12_000, "5220" + "11" * 32
        )
        self.assertEqual(change, 38_000)
        # version 3 (TRUC) + exactly two inputs.
        self.assertTrue(unsigned.startswith("03000000"))
        self.assertEqual(unsigned[8:10], "02")
        # both prevouts present (palindromic bytes => reversal is a no-op here).
        self.assertIn("ab" * 32, unsigned)
        self.assertIn("cd" * 32, unsigned)
        self.assertIn("5220" + "11" * 32, unsigned)
        self.assertEqual(CPFP_CHILD_TX_VERSION, 3)

    def test_rejects_fee_at_or_above_funding(self) -> None:
        with self.assertRaises(BroadcasterError):
            build_unsigned_cpfp_child(FANOUT_TXID, 8, "cd" * 32, 1, 50_000, 50_000, "51")

    def test_rejects_zero_fee(self) -> None:
        with self.assertRaises(BroadcasterError):
            build_unsigned_cpfp_child(FANOUT_TXID, 8, "cd" * 32, 1, 50_000, 0, "51")

    def test_rejects_funding_equal_to_anchor(self) -> None:
        with self.assertRaises(BroadcasterError):
            build_unsigned_cpfp_child(FANOUT_TXID, 8, FANOUT_TXID, 8, 50_000, 10, "51")


class SettlementStatusTests(unittest.TestCase):
    def test_awaiting_maturity(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY - 1)
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), AWAITING_MATURITY)

    def test_broadcastable_when_mature(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY)
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), BROADCASTABLE)

    def test_broadcast_when_in_mempool(self) -> None:
        fake = FakeRpc(tip=200, mempool=[FANOUT_TXID])
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), BROADCAST)

    def test_confirmed(self) -> None:
        fake = FakeRpc(tip=200, confirmed=[FANOUT_TXID])
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), CONFIRMED)

    def test_confirmed_without_txindex_when_parent_outpoint_is_spent_on_chain(self) -> None:
        fake = FakeRpc(tip=200, parent_spent_on_chain=True)
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), CONFIRMED)

    def test_unexpected_parent_spender_is_not_confirmed(self) -> None:
        fake = FakeRpc(tip=200, parent_spent_on_chain=True, parent_spender_txid="ef" * 32)
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), REORGED)

    def test_spent_parent_with_unknown_spender_is_not_broadcastable(self) -> None:
        fake = FakeRpc(
            tip=200,
            parent_spent_on_chain=True,
            missing_blocks=[f"{artifact().coinbase_height + 1:064x}"],
        )
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), REORGED)

    def test_transient_scan_failure_does_not_poison_spender_coverage(self) -> None:
        # A transient getblock error mid-scan must not mark the unread tail as
        # covered: the spender lives in the tip block, but the scan aborts at
        # height 150 first. A later call, once the node recovers, must still
        # find the spender instead of leaving it hidden until restart.
        transient_hash = f"{150:064x}"
        fake = FakeRpc(
            tip=200,
            parent_spent_on_chain=True,
            transient_missing_blocks=[transient_hash],
        )
        engine = broadcaster(fake)
        self.assertEqual(engine.settlement_status(artifact()), REORGED)
        self.assertEqual(engine.settlement_status(artifact()), CONFIRMED)

    def test_reorged_when_coinbase_block_disconnected(self) -> None:
        fake = FakeRpc(tip=200, reorged_blocks=[COINBASE_BLOCK])
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), REORGED)

    def test_disconnected_coinbase_before_maturity_keeps_waiting(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY - 1, reorged_blocks=[COINBASE_BLOCK])
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), AWAITING_MATURITY)

    def test_confirmed_beats_reorg_flag(self) -> None:
        # A confirmed fanout is final even if the coinbase block shows disconnected.
        fake = FakeRpc(tip=200, confirmed=[FANOUT_TXID], reorged_blocks=[COINBASE_BLOCK])
        self.assertEqual(broadcaster(fake).settlement_status(artifact()), CONFIRMED)


class BroadcastTests(unittest.TestCase):
    def test_broadcasts_when_broadcastable(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY)
        attempt = broadcaster(fake).broadcast(artifact(), fee_sats=50_000)
        self.assertTrue(attempt.submitted)
        self.assertEqual(attempt.status, BROADCAST)
        self.assertEqual(attempt.child_txid, CHILD_TXID)
        self.assertEqual(attempt.fee_sats, 50_000)
        self.assertEqual(len(fake.submitted_packages), 1)
        self.assertEqual(fake.submitted_raw_transactions, [])

    def test_no_anchor_direct_broadcasts_parent_when_no_cpfp_fee_requested(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY)
        attempt = direct_broadcaster(fake).broadcast(no_anchor_artifact(), fee_sats=0)
        self.assertTrue(attempt.submitted)
        self.assertEqual(attempt.status, BROADCAST)
        self.assertIsNone(attempt.child_txid)
        self.assertEqual(attempt.fee_sats, 0)
        self.assertEqual(fake.submitted_raw_transactions, [no_anchor_artifact().fanout_tx_hex])
        self.assertEqual(fake.submitted_packages, [])

    def test_no_anchor_artifact_broadcasts_parent_directly_even_with_cpfp_fee_configured(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY)
        attempt = broadcaster(fake).broadcast(no_anchor_artifact(), fee_sats=50_000)
        self.assertTrue(attempt.submitted)
        self.assertEqual(attempt.status, BROADCAST)
        self.assertIsNone(attempt.child_txid)
        self.assertEqual(fake.submitted_raw_transactions, [no_anchor_artifact().fanout_tx_hex])
        self.assertEqual(fake.submitted_packages, [])

    def test_anchored_fanout_requires_positive_cpfp_fee(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY)
        attempt = direct_broadcaster(fake).broadcast(artifact(), fee_sats=0)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, BROADCASTABLE)
        self.assertEqual(attempt.fee_sats, 0)
        self.assertEqual(attempt.package_msg, "invalid_fee")
        self.assertIn("CPFP fee", attempt.detail)
        self.assertEqual(fake.submitted_raw_transactions, [])
        self.assertEqual(fake.submitted_packages, [])

    def test_positive_cpfp_fee_without_wallet_is_rejected_not_raised(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY)
        attempt = direct_broadcaster(fake).broadcast(artifact(), fee_sats=50_000)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, BROADCASTABLE)
        self.assertEqual(attempt.fee_sats, 50_000)
        self.assertIn("funding wallet", attempt.detail)
        self.assertEqual(fake.submitted_packages, [])

    def test_idempotent_when_already_in_mempool(self) -> None:
        fake = FakeRpc(tip=200, mempool=[FANOUT_TXID])
        attempt = broadcaster(fake).broadcast(artifact(), fee_sats=50_000)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, BROADCAST)
        self.assertEqual(len(fake.submitted_packages), 0)

    def test_no_broadcast_before_maturity(self) -> None:
        fake = FakeRpc(tip=100 + MATURITY - 1)
        attempt = direct_broadcaster(fake).broadcast(artifact(), fee_sats=0)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, AWAITING_MATURITY)
        self.assertEqual(len(fake.submitted_packages), 0)
        self.assertEqual(fake.submitted_raw_transactions, [])

    def test_no_broadcast_when_confirmed(self) -> None:
        fake = FakeRpc(tip=200, confirmed=[FANOUT_TXID])
        attempt = broadcaster(fake).broadcast(artifact(), fee_sats=50_000)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, CONFIRMED)

    def test_bump_resubmits_at_higher_fee(self) -> None:
        fake = FakeRpc(tip=200, mempool=[FANOUT_TXID])
        attempt = broadcaster(fake).bump(artifact(), higher_fee_sats=90_000)
        self.assertTrue(attempt.submitted)
        self.assertEqual(attempt.fee_sats, 90_000)
        self.assertEqual(len(fake.submitted_packages), 1)

    def test_bump_refuses_when_confirmed(self) -> None:
        fake = FakeRpc(tip=200, confirmed=[FANOUT_TXID])
        attempt = broadcaster(fake).bump(artifact(), higher_fee_sats=90_000)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, CONFIRMED)
        self.assertEqual(len(fake.submitted_packages), 0)

    def test_bump_refuses_when_reorged(self) -> None:
        fake = FakeRpc(tip=200, reorged_blocks=[COINBASE_BLOCK])
        attempt = broadcaster(fake).bump(artifact(), higher_fee_sats=90_000)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, REORGED)

    def test_bump_rejects_negative_fee(self) -> None:
        fake = FakeRpc(tip=200, mempool=[FANOUT_TXID])
        attempt = broadcaster(fake).bump(artifact(), higher_fee_sats=-1)
        self.assertFalse(attempt.submitted)
        self.assertEqual(attempt.status, BROADCAST)
        self.assertEqual(attempt.fee_sats, -1)
        self.assertEqual(attempt.package_msg, "invalid_fee")
        self.assertIn("higher_fee_sats must be non-negative", attempt.detail)
        self.assertEqual(len(fake.submitted_packages), 0)


if __name__ == "__main__":
    unittest.main()
