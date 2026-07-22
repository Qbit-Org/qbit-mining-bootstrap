"""PRISM active-chain reconciliation and payout-publication ownership."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
import threading
from typing import Any, Callable, Mapping

from lab.prism.coordinator_config import TESTNET_QBIT_CHAINS
from lab.prism.payout_state import PayoutStateCandidate, TemplateRefreshSuperseded


@dataclass(frozen=True)
class ReorgPorts:
    """Dynamic infrastructure required by active-chain reconciliation."""

    rpc_call: Callable[..., object]
    ledger: Callable[[], Any]
    ensure_job_cache_state: Callable[[], None]
    source_tip: Callable[[], str | None]
    reserve_external_tip: Callable[[str], None]
    max_supersession_retries: Callable[[], int]
    prepare_lock: Callable[[], AbstractContextManager[object]]
    capture_source: Callable[[], tuple[int, int, str | None, str, float]]
    prepared_candidate: Callable[
        [tuple[int, int, str | None, str, float]],
        PayoutStateCandidate,
    ]
    publication_required: Callable[[PayoutStateCandidate | None], bool]
    block_publication: Callable[..., None]
    publication_guard: Callable[[], AbstractContextManager[object]]
    publish_candidate: Callable[[PayoutStateCandidate], int | None]
    observe_preparation: Callable[[float], None]
    chain_view_untrusted: Callable[[], bool]
    monotonic: Callable[[], float]
    reconcile_with_admission: Callable[[str], Mapping[str, object]]


@dataclass(frozen=True)
class ReorgState:
    inactive_block_count: int
    reactivated_block_count: int
    reconcile_skip_count: int
    reconcile_error_count: int
    matured_payout_count: int
    last_tip_hash: str | None
    last_trusted: bool
    last_monotonic: float | None


def qbit_chain_view_untrusted(
    rpc_call: Callable[..., object],
    chain: str,
) -> bool:
    """Return whether the node cannot provide a coherent validated tip."""

    blockchain_info = rpc_call("getblockchaininfo")
    if not isinstance(blockchain_info, dict):
        raise RuntimeError("getblockchaininfo returned non-object")
    public_chain = chain.lower() in {"main", "mainnet", *TESTNET_QBIT_CHAINS}
    if (
        blockchain_info.get("initialblockdownload") is not False
        if public_chain
        else bool(blockchain_info.get("initialblockdownload"))
    ):
        return True
    blocks_raw = blockchain_info.get("blocks")
    headers_raw = blockchain_info.get("headers")
    if public_chain and (blocks_raw is None or headers_raw is None):
        return True
    if blocks_raw is not None and headers_raw is not None:
        try:
            blocks = int(blocks_raw)
            headers = int(headers_raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "getblockchaininfo blocks/headers are not integers"
            ) from exc
        if blocks < 0 or headers < 0 or headers != blocks:
            return True
    return False


class ReorgReconcilerService:
    """Own the reconciliation state machine, cache, and bounded retry state."""

    def __init__(
        self,
        ports: ReorgPorts,
        *,
        enabled: bool = True,
        cache_seconds: float = 5.0,
        inactive_block_count: int = 0,
        reactivated_block_count: int = 0,
        reconcile_skip_count: int = 0,
        reconcile_error_count: int = 0,
        matured_payout_count: int = 0,
        last_tip_hash: str | None = None,
        last_trusted: bool = False,
        last_monotonic: float | None = None,
    ) -> None:
        self.ports = ports
        self._lock = threading.RLock()
        self.enabled = bool(enabled)
        self.cache_seconds = max(0.0, float(cache_seconds))
        self.inactive_block_count = int(inactive_block_count)
        self.reactivated_block_count = int(reactivated_block_count)
        self.reconcile_skip_count = int(reconcile_skip_count)
        self.reconcile_error_count = int(reconcile_error_count)
        self.matured_payout_count = int(matured_payout_count)
        self.last_tip_hash = last_tip_hash
        self.last_trusted = bool(last_trusted)
        self.last_monotonic = last_monotonic

    def snapshot(self) -> ReorgState:
        with self._lock:
            return ReorgState(
                inactive_block_count=self.inactive_block_count,
                reactivated_block_count=self.reactivated_block_count,
                reconcile_skip_count=self.reconcile_skip_count,
                reconcile_error_count=self.reconcile_error_count,
                matured_payout_count=self.matured_payout_count,
                last_tip_hash=self.last_tip_hash,
                last_trusted=self.last_trusted,
                last_monotonic=self.last_monotonic,
            )

    def ensure_current(self, *, expected_tip_hash: str | None = None) -> bool:
        if not self.enabled and expected_tip_hash is None:
            return True
        current_tip = str(self.ports.rpc_call("getbestblockhash"))
        if expected_tip_hash is not None and current_tip != expected_tip_hash:
            raise TemplateRefreshSuperseded(
                "qbit tip changed while prepared work was queued "
                f"expected={expected_tip_hash} current={current_tip}"
            )
        if not self.enabled:
            return True
        state = self.snapshot()
        if (
            self.cache_seconds > 0
            and state.last_trusted
            and state.last_tip_hash == current_tip
            and state.last_monotonic is not None
            and self.ports.monotonic() - state.last_monotonic <= self.cache_seconds
            and not self.ports.chain_view_untrusted()
        ):
            return True
        return self.ensure_tip(current_tip)

    def ensure_tip(self, tip_hash: str) -> bool:
        if not self.enabled:
            return True
        summary = self.ports.reconcile_with_admission(tip_hash)
        return not bool(summary.get("untrusted") or summary.get("superseded"))

    def _finish(
        self,
        summary: dict[str, object],
        *,
        tip_hash: str | None,
        trusted: bool,
        inactive_blocks: int,
        reactivated_blocks: int,
        matured_payouts: int,
    ) -> dict[str, object]:
        with self._lock:
            self.inactive_block_count += inactive_blocks
            self.reactivated_block_count += reactivated_blocks
            self.matured_payout_count += matured_payouts
            self.last_tip_hash = tip_hash
            self.last_trusted = trusted
            self.last_monotonic = self.ports.monotonic()
        summary["inactive_blocks"] = inactive_blocks
        summary["reactivated_blocks"] = reactivated_blocks
        summary["matured_payouts"] = matured_payouts
        return summary

    def reconcile(
        self,
        *,
        tip_hash: str | None = None,
        force_publish: bool = False,
        source_reserved: bool = False,
    ) -> dict[str, object]:
        summary: dict[str, object] = {
            "enabled": self.enabled,
            "untrusted": False,
            "superseded": False,
            "published_generation": None,
            "watched_blocks": 0,
            "inactive_blocks": 0,
            "reactivated_blocks": 0,
            "matured_payouts": 0,
        }
        if not self.enabled:
            return summary
        self.ports.ensure_job_cache_state()
        if (
            not source_reserved
            and tip_hash is not None
            and self.ports.source_tip() != tip_hash
        ):
            self.ports.reserve_external_tip(tip_hash)

        inactive_blocks_total = 0
        reactivated_blocks_total = 0
        matured_payouts_total = 0
        supersession_retries = 0
        skip_recorded = False
        max_supersession_retries = self.ports.max_supersession_retries()

        while True:
            candidate_to_publish: PayoutStateCandidate | None = None
            error_candidate: PayoutStateCandidate | None = None
            attempt_trusted = True
            try:
                with self.ports.prepare_lock():
                    prepared_started = self.ports.monotonic()
                    captured_source = self.ports.capture_source()
                    payout_changed = False
                    inactive_blocks = 0
                    reactivated_blocks = 0
                    matured_payouts = 0
                    summary["untrusted"] = False
                    summary["watched_blocks"] = 0
                    try:
                        if self.ports.chain_view_untrusted():
                            if not skip_recorded:
                                with self._lock:
                                    self.reconcile_skip_count += 1
                                skip_recorded = True
                            summary["untrusted"] = True
                            attempt_trusted = False
                            if force_publish:
                                candidate_to_publish = self.ports.prepared_candidate(
                                    captured_source
                                )
                        else:
                            active_tip_height = int(
                                self.ports.rpc_call("getblockcount")
                            )
                            ledger = self.ports.ledger()
                            watch_blocks = getattr(ledger, "reorg_watch_blocks", None)
                            if not callable(watch_blocks):
                                candidate = self.ports.prepared_candidate(
                                    captured_source
                                )
                                if (
                                    force_publish
                                    or self.ports.publication_required(candidate)
                                ):
                                    candidate_to_publish = candidate
                            else:
                                rows = watch_blocks(
                                    active_tip_height=active_tip_height
                                )
                                summary["watched_blocks"] = len(rows)
                                for row in rows:
                                    block_height = int(row["block_height"])
                                    block_hash = str(row["block_hash"]).lower()
                                    chain_state = str(row.get("chain_state", ""))
                                    if block_height > active_tip_height:
                                        if chain_state == "confirmed":
                                            inactive = ledger.mark_pool_block_inactive(
                                                block_hash=block_hash,
                                                active_tip_height=active_tip_height,
                                            )
                                            inactive_count = int(
                                                inactive.get("inactive_count", 0)
                                            )
                                            inactive_blocks += inactive_count
                                            payout_changed = (
                                                payout_changed or bool(inactive_count)
                                            )
                                        continue
                                    active_hash = str(
                                        self.ports.rpc_call(
                                            "getblockhash",
                                            [block_height],
                                        )
                                    ).lower()
                                    on_active_chain = active_hash == block_hash
                                    if on_active_chain and chain_state == "inactive":
                                        with self.ports.publication_guard():
                                            reactivated = ledger.reactivate_pool_block(
                                                block_hash=block_hash,
                                                active_tip_height=active_tip_height,
                                            )
                                        reactivated_count = int(
                                            reactivated.get("reactivated_count", 0)
                                        )
                                        reactivated_blocks += reactivated_count
                                        payout_changed = (
                                            payout_changed or bool(reactivated_count)
                                        )
                                    elif (
                                        not on_active_chain
                                        and chain_state == "confirmed"
                                    ):
                                        inactive = ledger.mark_pool_block_inactive(
                                            block_hash=block_hash,
                                            active_tip_height=active_tip_height,
                                        )
                                        inactive_count = int(
                                            inactive.get("inactive_count", 0)
                                        )
                                        inactive_blocks += inactive_count
                                        payout_changed = (
                                            payout_changed or bool(inactive_count)
                                        )

                                mark_mature = getattr(
                                    ledger,
                                    "mark_mature_pool_payouts",
                                    None,
                                )
                                if callable(mark_mature):
                                    matured = mark_mature(
                                        active_tip_height=active_tip_height
                                    )
                                    matured_payouts = int(
                                        matured.get("matured_count", 0)
                                    )
                                    payout_changed = (
                                        payout_changed or bool(matured_payouts)
                                    )

                                inactive_blocks_total += inactive_blocks
                                reactivated_blocks_total += reactivated_blocks
                                matured_payouts_total += matured_payouts
                                candidate = self.ports.prepared_candidate(
                                    captured_source
                                )
                                if (
                                    payout_changed
                                    or force_publish
                                    or self.ports.publication_required(candidate)
                                ):
                                    candidate_to_publish = candidate
                    except Exception:
                        inactive_blocks_total += inactive_blocks
                        reactivated_blocks_total += reactivated_blocks
                        matured_payouts_total += matured_payouts
                        if payout_changed:
                            error_candidate = self.ports.prepared_candidate(
                                captured_source
                            )
                            self.ports.block_publication(force=True)
                        with self._lock:
                            self.inactive_block_count += inactive_blocks_total
                            self.reactivated_block_count += reactivated_blocks_total
                            self.matured_payout_count += matured_payouts_total
                            self.reconcile_error_count += 1
                            self.last_tip_hash = tip_hash
                            self.last_trusted = False
                            self.last_monotonic = self.ports.monotonic()
                        raise
                    finally:
                        self.ports.observe_preparation(
                            max(0.0, self.ports.monotonic() - prepared_started)
                        )

                    if candidate_to_publish is not None:
                        self.ports.block_publication(force=True)
            except Exception:
                if error_candidate is not None:
                    if self.ports.publish_candidate(error_candidate) is None:
                        self.ports.block_publication()
                raise

            if candidate_to_publish is not None:
                published = self.ports.publish_candidate(candidate_to_publish)
                if published is None:
                    supersession_retries += 1
                    if supersession_retries > max_supersession_retries:
                        summary["superseded"] = True
                        self.ports.block_publication()
                        return self._finish(
                            summary,
                            tip_hash=tip_hash,
                            trusted=False,
                            inactive_blocks=inactive_blocks_total,
                            reactivated_blocks=reactivated_blocks_total,
                            matured_payouts=matured_payouts_total,
                        )
                    tip_hash = self.ports.source_tip() or tip_hash
                    continue
                summary["published_generation"] = published
            return self._finish(
                summary,
                tip_hash=tip_hash,
                trusted=attempt_trusted,
                inactive_blocks=inactive_blocks_total,
                reactivated_blocks=reactivated_blocks_total,
                matured_payouts=matured_payouts_total,
            )


class ReorgCompatibilityField:
    """Route retained coordinator fields to their single service owner."""

    def __init__(self, name: str, default: object) -> None:
        self.name = name
        self.default = default
        self.backing = f"_reorg_compat_{name}"

    def __get__(self, instance: Any, owner: type[Any]) -> object:
        if instance is None:
            return self
        service = instance.__dict__.get("_reorg_reconciler_service")
        if service is not None:
            return getattr(service, self.name)
        return instance.__dict__.get(self.backing, self.default)

    def __set__(self, instance: Any, value: object) -> None:
        service = instance.__dict__.get("_reorg_reconciler_service")
        if service is not None:
            setattr(service, self.name, value)
            return
        instance.__dict__[self.backing] = value
