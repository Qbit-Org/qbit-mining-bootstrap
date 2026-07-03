#!/usr/bin/env python3
"""Non-custodial CTV fanout broadcaster engine (live-node orchestration).

This is the operational loop that gets a precommitted CTV fanout confirmed. It
is deliberately decoupled from persistence: given a fanout artifact, a node RPC
callable, and optionally a funding wallet, it

- gates broadcasting on parent-coinbase maturity and active-chain state,
- submits a fee-bearing fanout parent directly when settlement reserved a
  network fee in the fanout template,
- builds and signs a child-pays-for-parent (CPFP) child that spends the
  fanout's keyless P2A anchor plus a funding input the wallet owns when extra
  fee sponsorship is requested,
- submits the ``[parent]`` transaction or ``[parent, child]`` package,
- detects confirmation, and
- can fee-bump a stuck child by re-issuing it at a higher fee.

It holds **no covenant key**: the fanout outputs are fixed by the covenant. When
the parent fee reserved at block-build time is no longer enough, the broadcaster
can only add fee from its own funding input, with change returning to it.
Because the anchor is keyless, any owed miner can run this same engine to rescue
their payout.

``broadcast`` / ``bump`` return a :class:`BroadcastAttempt` for the caller to
journal (e.g. into ``qbit_ctv_fanout_broadcast_attempts``); this module does not
touch Postgres so the node-orchestration logic is testable in isolation and
against a regtest node without a database.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Optional

# qbit coinbase maturity (consensus): a coinbase output is spendable once the
# active tip is at least this many blocks above the coinbase height.
COINBASE_MATURITY = 1000
SATOSHIS_PER_QBIT = 100_000_000
P2A_ANCHOR_SCRIPT_PUBKEY_HEX = "51024e73"
# RBF-signalling, non-final sequence so a stuck child can be re-issued higher.
CPFP_CHILD_SEQUENCE = 0xFFFF_FFFD
CPFP_CHILD_TX_VERSION = 3  # TRUC, so a 0-fee parent relays as a package
TEMPLATE_MISMATCH = "Transaction template hash does not match"

# Settlement statuses, matching the Rust `FanoutSettlementStatus` plus a
# `broadcast` (in-mempool, unconfirmed) state the live loop needs.
AWAITING_MATURITY = "awaiting_maturity"
BROADCASTABLE = "broadcastable"
BROADCAST = "broadcast"
CONFIRMED = "confirmed"
REORGED = "reorged"


class BroadcasterError(RuntimeError):
    pass


class RpcError(RuntimeError):
    def __init__(self, method: str, error: Any):
        self.method = method
        self.error = error
        super().__init__(f"qbit RPC {method} failed: {error}")


@dataclass(frozen=True)
class FanoutArtifact:
    """The recoverable data the broadcaster needs, as persisted alongside a
    fanout manifest set."""

    fanout_txid: str
    fanout_tx_hex: str
    anchor_vout: int | None
    coinbase_txid: str
    coinbase_block_hash: str
    coinbase_height: int
    parent_coinbase_vout: int


@dataclass(frozen=True)
class BroadcastAttempt:
    fanout_txid: str
    status: str
    submitted: bool
    child_txid: Optional[str] = None
    fee_sats: Optional[int] = None
    package_msg: Optional[str] = None
    detail: str = ""


def qbit_to_sats(amount: Any) -> int:
    return int((Decimal(str(amount)) * SATOSHIS_PER_QBIT).to_integral_exact())


def _compact_size(value: int) -> bytes:
    if value < 0xFD:
        return bytes([value])
    if value <= 0xFFFF:
        return b"\xfd" + value.to_bytes(2, "little")
    if value <= 0xFFFF_FFFF:
        return b"\xfe" + value.to_bytes(4, "little")
    return b"\xff" + value.to_bytes(8, "little")


def _serialize_input(display_txid_hex: str, vout: int, sequence: int) -> bytes:
    txid = bytes.fromhex(display_txid_hex)
    if len(txid) != 32:
        raise BroadcasterError(f"txid must be 32 bytes: {display_txid_hex}")
    # Transactions serialize the prevout txid in internal (reversed) byte order.
    return txid[::-1] + vout.to_bytes(4, "little") + b"\x00" + sequence.to_bytes(4, "little")


def _serialize_output(value_sats: int, script_pubkey_hex: str) -> bytes:
    spk = bytes.fromhex(script_pubkey_hex)
    return value_sats.to_bytes(8, "little") + _compact_size(len(spk)) + spk


def build_unsigned_cpfp_child(
    fanout_txid: str,
    anchor_vout: int,
    funding_txid: str,
    funding_vout: int,
    funding_value_sats: int,
    fee_sats: int,
    change_script_pubkey_hex: str,
) -> tuple[str, int]:
    """Serialize the unsigned CPFP child (legacy form, ready for the wallet to
    sign the funding input). Mirrors the Rust ``build_cpfp_child``: the anchor
    input is keyless (empty scriptSig/witness) and the funding input is left
    unsigned. Returns ``(unsigned_tx_hex, change_value_sats)``.
    """
    if fee_sats <= 0:
        raise BroadcasterError("CPFP fee must be positive")
    if fee_sats >= funding_value_sats:
        raise BroadcasterError(
            "funding value must exceed the fee so the change output is positive"
        )
    if fanout_txid == funding_txid and anchor_vout == funding_vout:
        raise BroadcasterError("funding input must differ from the anchor outpoint")
    change_value_sats = funding_value_sats - fee_sats  # anchor contributes 0
    tx = CPFP_CHILD_TX_VERSION.to_bytes(4, "little")
    tx += _compact_size(2)
    tx += _serialize_input(fanout_txid, anchor_vout, CPFP_CHILD_SEQUENCE)
    tx += _serialize_input(funding_txid, funding_vout, CPFP_CHILD_SEQUENCE)
    tx += _compact_size(1)
    tx += _serialize_output(change_value_sats, change_script_pubkey_hex)
    tx += (0).to_bytes(4, "little")  # lock_time
    return tx.hex(), change_value_sats


class CtvFanoutBroadcaster:
    def __init__(
        self,
        rpc_call: Callable[..., Any],
        *,
        funding_wallet: str | None = None,
        maturity: int = COINBASE_MATURITY,
    ) -> None:
        self._rpc = rpc_call
        self.funding_wallet = funding_wallet
        self.maturity = maturity
        self._parent_spender_by_outpoint: dict[tuple[str, int], str] = {}
        self._parent_spender_scanned_min_height: int | None = None
        self._parent_spender_scanned_max_height: int | None = None

    # -- chain queries -----------------------------------------------------

    def _tx_info(self, txid: str) -> Optional[dict[str, Any]]:
        try:
            return self._rpc("getrawtransaction", [txid, True])
        except Exception:
            return None

    def _block_info(self, block_hash: str) -> Optional[dict[str, Any]]:
        try:
            return self._rpc("getblock", [block_hash])
        except Exception:
            return None

    def _tip_height(self) -> int:
        return int(self._rpc("getblockchaininfo")["blocks"])

    def _in_mempool(self, txid: str) -> bool:
        return txid in set(self._rpc("getrawmempool"))

    def _parent_coinbase_output_spent_on_chain(self, artifact: FanoutArtifact) -> bool:
        try:
            return (
                self._rpc(
                    "gettxout",
                    [artifact.coinbase_txid, artifact.parent_coinbase_vout, False],
                )
                is None
            )
        except Exception:
            return False

    def _parent_coinbase_spender_txid_on_chain(
        self,
        artifact: FanoutArtifact,
        *,
        tip_height: int,
        parent_spent_on_chain: bool | None = None,
    ) -> str | None:
        if parent_spent_on_chain is None:
            parent_spent_on_chain = self._parent_coinbase_output_spent_on_chain(artifact)
        if not parent_spent_on_chain:
            return None
        outpoint = (artifact.coinbase_txid.lower(), artifact.parent_coinbase_vout)
        if outpoint in self._parent_spender_by_outpoint:
            return self._parent_spender_by_outpoint[outpoint]
        self._scan_parent_spenders_on_chain(artifact.coinbase_height + 1, tip_height)
        return self._parent_spender_by_outpoint.get(outpoint)

    def _scan_parent_spenders_on_chain(self, start_height: int, tip_height: int) -> None:
        """Index prevout spenders for ``[start_height, tip_height]``, reusing
        any range already scanned.

        Only heights that were actually read extend the covered window, and the
        window is kept contiguous. A transient RPC failure mid-scan therefore
        leaves the unread tail *uncovered* so a later call retries it, instead
        of permanently hiding a spender (which would misclassify a confirmed
        fanout as reorged) until the process restarts.
        """
        if start_height > tip_height:
            return
        min_height = self._parent_spender_scanned_min_height
        max_height = self._parent_spender_scanned_max_height

        if min_height is None or max_height is None:
            last_scanned = self._scan_parent_spenders_on_chain_range(start_height, tip_height)
            if last_scanned is not None:
                self._parent_spender_scanned_min_height = start_height
                self._parent_spender_scanned_max_height = last_scanned
            return

        # Extend upward. Starting at ``max_height + 1`` (rather than
        # ``start_height``) keeps the covered window contiguous even when this
        # artifact's coinbase sits above everything scanned so far.
        if tip_height > max_height:
            last_scanned = self._scan_parent_spenders_on_chain_range(max_height + 1, tip_height)
            if last_scanned is not None:
                self._parent_spender_scanned_max_height = last_scanned

        # Extend downward. Only lower the watermark if the whole gap below the
        # window was scanned, so a partial scan never claims coverage over the
        # blocks it skipped.
        if start_height < min_height:
            lower_end = min_height - 1
            last_scanned = self._scan_parent_spenders_on_chain_range(start_height, lower_end)
            if last_scanned == lower_end:
                self._parent_spender_scanned_min_height = start_height

    def _scan_parent_spenders_on_chain_range(self, start_height: int, end_height: int) -> int | None:
        """Scan ``[start_height, end_height]`` ascending, recording every prevout
        spender seen.

        Returns the highest height scanned contiguously from ``start_height``,
        or ``None`` if not even the first block could be read, so the caller can
        avoid extending its covered window past a failed scan.
        """
        last_scanned: int | None = None
        for height in range(start_height, end_height + 1):
            try:
                block_hash = str(self._rpc("getblockhash", [height]))
                block = self._rpc("getblock", [block_hash, 2])
            except Exception:
                return last_scanned
            if not isinstance(block, dict):
                return last_scanned
            for tx in block.get("tx", []):
                if not isinstance(tx, dict):
                    continue
                txid = str(tx.get("txid", ""))
                if not txid:
                    continue
                for txin in tx.get("vin", []):
                    if not isinstance(txin, dict):
                        continue
                    parent_txid = str(txin.get("txid", "")).lower()
                    if not parent_txid:
                        continue
                    try:
                        vout = int(txin.get("vout", -1))
                    except (TypeError, ValueError):
                        continue
                    self._parent_spender_by_outpoint[(parent_txid, vout)] = txid
            last_scanned = height
        return last_scanned

    def settlement_status(self, artifact: FanoutArtifact) -> str:
        """Derive settlement status purely from chain facts. A confirmed fanout
        is ``CONFIRMED`` regardless of the coinbase's reorg state."""
        info = self._tx_info(artifact.fanout_txid)
        if info is not None and int(info.get("confirmations", 0)) >= 1:
            return CONFIRMED

        tip_height = self._tip_height()
        block = self._block_info(artifact.coinbase_block_hash)
        if block is None or int(block.get("confirmations", -1)) < 0:
            if tip_height < artifact.coinbase_height + self.maturity:
                return AWAITING_MATURITY
            # The funding coinbase is height-mature but no longer on the active
            # chain and the fanout has not confirmed: the payout must be
            # recomputed.
            return REORGED

        if tip_height < artifact.coinbase_height + self.maturity:
            return AWAITING_MATURITY
        parent_spent_on_chain = self._parent_coinbase_output_spent_on_chain(artifact)
        if parent_spent_on_chain:
            spender_txid = self._parent_coinbase_spender_txid_on_chain(
                artifact,
                tip_height=tip_height,
                parent_spent_on_chain=True,
            )
            if spender_txid is not None and spender_txid.lower() == artifact.fanout_txid.lower():
                return CONFIRMED
            return REORGED
        if self._in_mempool(artifact.fanout_txid):
            return BROADCAST
        return BROADCASTABLE

    # -- broadcasting ------------------------------------------------------

    def _select_funding_utxo(self, required_sats: int) -> dict[str, Any]:
        if self.funding_wallet is None:
            raise BroadcasterError("funding wallet is required for CPFP fee sponsorship")
        utxos = self._rpc("listunspent", [1, 9999999, [], True], wallet=self.funding_wallet)
        spendable = [
            utxo
            for utxo in utxos
            if bool(utxo.get("spendable", False)) and qbit_to_sats(utxo["amount"]) > required_sats
        ]
        if not spendable:
            raise BroadcasterError(
                f"{self.funding_wallet} has no spendable UTXO above {required_sats} sats"
            )
        return max(spendable, key=lambda utxo: qbit_to_sats(utxo["amount"]))

    def _build_signed_child(self, artifact: FanoutArtifact, fee_sats: int) -> tuple[str, str]:
        if self.funding_wallet is None:
            raise BroadcasterError("funding wallet is required for CPFP fee sponsorship")
        if artifact.anchor_vout is None:
            raise BroadcasterError("fanout has no CPFP anchor")
        funding = self._select_funding_utxo(fee_sats)
        change_address = self._rpc("getnewaddress", ["", "p2mr"], wallet=self.funding_wallet)
        change_spk = self._rpc("getaddressinfo", [change_address], wallet=self.funding_wallet)[
            "scriptPubKey"
        ]
        unsigned_hex, _change = build_unsigned_cpfp_child(
            artifact.fanout_txid,
            artifact.anchor_vout,
            str(funding["txid"]),
            int(funding["vout"]),
            qbit_to_sats(funding["amount"]),
            fee_sats,
            change_spk,
        )
        # The anchor input is keyless; declaring its prevout lets the wallet sign
        # only the funding input and leave the anchor witness empty.
        anchor_prevtx = [
            {
                "txid": artifact.fanout_txid,
                "vout": artifact.anchor_vout,
                "scriptPubKey": P2A_ANCHOR_SCRIPT_PUBKEY_HEX,
                "amount": 0,
            }
        ]
        signed = self._rpc(
            "signrawtransactionwithwallet",
            [unsigned_hex, anchor_prevtx],
            wallet=self.funding_wallet,
        )
        if not bool(signed.get("complete", False)):
            raise BroadcasterError(f"funding input did not sign cleanly: {signed.get('errors')}")
        return str(signed["hex"]), unsigned_hex

    def _submit_package(self, artifact: FanoutArtifact, signed_child_hex: str) -> dict[str, Any]:
        return self._rpc(
            "submitpackage", [[artifact.fanout_tx_hex, signed_child_hex], 0]
        )

    def _submit_parent(self, artifact: FanoutArtifact, *, detail: str) -> BroadcastAttempt:
        txid = str(self._rpc("sendrawtransaction", [artifact.fanout_tx_hex]))
        submitted = txid.lower() == artifact.fanout_txid.lower()
        return BroadcastAttempt(
            fanout_txid=artifact.fanout_txid,
            status=BROADCAST if submitted else BROADCASTABLE,
            submitted=submitted,
            fee_sats=0,
            package_msg="success" if submitted else "txid_mismatch",
            detail=detail if submitted else f"{detail}: submitted txid {txid} did not match artifact",
        )

    def broadcast(self, artifact: FanoutArtifact, fee_sats: int) -> BroadcastAttempt:
        """Idempotently get the fanout into the mempool.

        No-op (with the observed status) unless the fanout is ``BROADCASTABLE``;
        already-broadcast or confirmed fanouts are not re-submitted.
        """
        status = self.settlement_status(artifact)
        if status in (CONFIRMED, BROADCAST):
            return BroadcastAttempt(
                fanout_txid=artifact.fanout_txid,
                status=status,
                submitted=False,
                detail="already in mempool or confirmed; no re-broadcast",
            )
        if status != BROADCASTABLE:
            return BroadcastAttempt(
                fanout_txid=artifact.fanout_txid,
                status=status,
                submitted=False,
                detail=f"not broadcastable ({status})",
            )
        if fee_sats < 0:
            return BroadcastAttempt(
                fanout_txid=artifact.fanout_txid,
                status=BROADCASTABLE,
                submitted=False,
                fee_sats=fee_sats,
                package_msg="invalid_fee",
                detail="initial broadcast: fee_sats must be non-negative",
            )
        return self._submit(artifact, fee_sats, detail="initial broadcast")

    def bump(self, artifact: FanoutArtifact, higher_fee_sats: int) -> BroadcastAttempt:
        """Re-issue the CPFP child at a higher fee to chase the fee market.

        The TRUC child is replaceable, so a higher-fee child supersedes the
        stuck one. Refuses to bump a fanout that has already confirmed."""
        status = self.settlement_status(artifact)
        if status == CONFIRMED:
            return BroadcastAttempt(
                fanout_txid=artifact.fanout_txid,
                status=CONFIRMED,
                submitted=False,
                detail="already confirmed; nothing to bump",
            )
        if status == REORGED:
            return BroadcastAttempt(
                fanout_txid=artifact.fanout_txid,
                status=REORGED,
                submitted=False,
                detail="coinbase reorged; recompute payout before bumping",
            )
        if higher_fee_sats < 0:
            return BroadcastAttempt(
                fanout_txid=artifact.fanout_txid,
                status=status,
                submitted=False,
                fee_sats=higher_fee_sats,
                package_msg="invalid_fee",
                detail="fee bump: higher_fee_sats must be non-negative",
            )
        return self._submit(artifact, higher_fee_sats, detail="fee bump")

    def _submit(self, artifact: FanoutArtifact, fee_sats: int, *, detail: str) -> BroadcastAttempt:
        try:
            if artifact.anchor_vout is None:
                return self._submit_parent(
                    artifact,
                    detail=f"{detail}: no CPFP anchor",
                )
            if fee_sats == 0:
                return BroadcastAttempt(
                    fanout_txid=artifact.fanout_txid,
                    status=BROADCASTABLE,
                    submitted=False,
                    fee_sats=fee_sats,
                    package_msg="invalid_fee",
                    detail=f"{detail}: CPFP fee must be positive for anchored fanout",
                )
            signed_child_hex, _unsigned = self._build_signed_child(artifact, fee_sats)
            result = self._submit_package(artifact, signed_child_hex)
        except Exception as exc:
            return BroadcastAttempt(
                fanout_txid=artifact.fanout_txid,
                status=BROADCASTABLE,
                submitted=False,
                fee_sats=fee_sats,
                package_msg="error",
                detail=f"{detail}: {exc}",
            )
        package_msg = str(result.get("package_msg", ""))
        submitted = package_msg.lower() == "success"
        child_txid = None
        for txid, tx_result in (result.get("tx-results", {}) or {}).items():
            wtxid = tx_result.get("txid") if isinstance(tx_result, dict) else None
            if wtxid and wtxid != artifact.fanout_txid:
                child_txid = wtxid
        return BroadcastAttempt(
            fanout_txid=artifact.fanout_txid,
            status=BROADCAST if submitted else BROADCASTABLE,
            submitted=submitted,
            child_txid=child_txid,
            fee_sats=fee_sats,
            package_msg=package_msg,
            detail=detail if submitted else f"{detail}: package rejected ({package_msg})",
        )
