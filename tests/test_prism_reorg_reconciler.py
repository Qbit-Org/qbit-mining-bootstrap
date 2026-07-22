from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
import unittest

from lab.prism.reorg_reconciler import (
    ReorgPorts,
    ReorgReconcilerService,
    qbit_chain_view_untrusted,
)


class FakeLedger:
    def __init__(self) -> None:
        self.inactivated: list[str] = []
        self.reactivated: list[str] = []

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        assert active_tip_height == 10
        return [
            {"block_height": 8, "block_hash": "aa", "chain_state": "confirmed"},
            {"block_height": 9, "block_hash": "bb", "chain_state": "inactive"},
        ]

    def mark_pool_block_inactive(self, **kwargs: object) -> dict[str, int]:
        self.inactivated.append(str(kwargs["block_hash"]))
        return {"inactive_count": 1}

    def reactivate_pool_block(self, **kwargs: object) -> dict[str, int]:
        self.reactivated.append(str(kwargs["block_hash"]))
        return {"reactivated_count": 1}

    def mark_mature_pool_payouts(self, **_kwargs: object) -> dict[str, int]:
        return {"matured_count": 2}


def make_ports(
    *,
    ledger: object | None = None,
    publish: object = 7,
    chain_untrusted: bool = False,
) -> tuple[ReorgPorts, list[tuple[str, object]]]:
    events: list[tuple[str, object]] = []
    active_ledger = FakeLedger() if ledger is None else ledger

    def rpc_call(method: str, params: object = None) -> object:
        if method == "getbestblockhash":
            return "tip"
        if method == "getblockcount":
            return 10
        if method == "getblockhash":
            return {8: "not-aa", 9: "bb"}[int(params[0])]  # type: ignore[index]
        raise AssertionError(method)

    return (
        ReorgPorts(
            rpc_call=rpc_call,
            ledger=lambda: active_ledger,
            ensure_job_cache_state=lambda: events.append(("ensure", None)),
            source_tip=lambda: "tip",
            reserve_external_tip=lambda tip: events.append(("reserve", tip)),
            max_supersession_retries=lambda: 1,
            prepare_lock=nullcontext,
            capture_source=lambda: (0, 0, "tip", "test", 0.0),
            prepared_candidate=lambda _source: SimpleNamespace(),  # type: ignore[arg-type]
            publication_required=lambda _candidate=None: False,
            block_publication=lambda **kwargs: events.append(("block", kwargs)),
            publication_guard=nullcontext,
            publish_candidate=lambda _candidate: publish,  # type: ignore[return-value]
            observe_preparation=lambda elapsed: events.append(("observe", elapsed)),
            chain_view_untrusted=lambda: chain_untrusted,
            monotonic=lambda: 100.0,
            reconcile_with_admission=lambda tip: {
                "tip": tip,
                "untrusted": chain_untrusted,
            },
        ),
        events,
    )


class ReorgReconcilerServiceTests(unittest.TestCase):
    def test_trusted_same_tip_cache_still_checks_live_chain_trust(self) -> None:
        ports, _events = make_ports()
        service = ReorgReconcilerService(
            ports,
            cache_seconds=5.0,
            last_tip_hash="tip",
            last_trusted=True,
            last_monotonic=98.0,
        )
        self.assertTrue(service.ensure_current())

        untrusted_ports, _events = make_ports(chain_untrusted=True)
        untrusted = ReorgReconcilerService(
            untrusted_ports,
            cache_seconds=5.0,
            last_tip_hash="tip",
            last_trusted=True,
            last_monotonic=98.0,
        )
        self.assertFalse(untrusted.ensure_current())

    def test_reconcile_owns_chain_mutations_counts_and_publication(self) -> None:
        ledger = FakeLedger()
        ports, events = make_ports(ledger=ledger)
        service = ReorgReconcilerService(ports)

        summary = service.reconcile(tip_hash="tip", force_publish=True)

        self.assertEqual(ledger.inactivated, ["aa"])
        self.assertEqual(ledger.reactivated, ["bb"])
        self.assertEqual(summary["published_generation"], 7)
        self.assertEqual(summary["inactive_blocks"], 1)
        self.assertEqual(summary["reactivated_blocks"], 1)
        self.assertEqual(summary["matured_payouts"], 2)
        self.assertIn(("block", {"force": True}), events)
        state = service.snapshot()
        self.assertEqual(state.inactive_block_count, 1)
        self.assertEqual(state.reactivated_block_count, 1)
        self.assertEqual(state.matured_payout_count, 2)
        self.assertTrue(state.last_trusted)

    def test_chain_view_validation_fails_closed(self) -> None:
        self.assertTrue(
            qbit_chain_view_untrusted(
                lambda _method: {
                    "initialblockdownload": False,
                    "blocks": 9,
                    "headers": 10,
                },
                "main",
            )
        )
        with self.assertRaisesRegex(RuntimeError, "non-object"):
            qbit_chain_view_untrusted(lambda _method: [], "regtest")


if __name__ == "__main__":
    unittest.main()
