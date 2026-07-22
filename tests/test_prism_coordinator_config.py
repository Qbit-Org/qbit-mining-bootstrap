#!/usr/bin/env python3
"""Focused PRISM coordinator config tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import unittest
from tests.prism_vardiff_test_support import *


class PrismCoordinatorVardiffTests(unittest.TestCase):
    def test_send_error_includes_canonical_reason_id_data(self) -> None:
        server = coordinator()
        sent: list[dict[str, object]] = []
        client = SimpleNamespace(send=lambda payload: sent.append(payload))

        server.send_error(client, "submit-1", 21, "stale job", reason=PRISM_REJECTION_STALE_JOB)  # type: ignore[arg-type]

        self.assertEqual(
            sent,
            [
                {
                    "id": "submit-1",
                    "result": None,
                    "error": [21, "stale job", {"reason_id": PRISM_REJECTION_STALE_JOB}],
                }
            ],
        )
    def test_scaled_target_difficulty_uses_pow_limit_units(self) -> None:
        pow_limit = target_from_compact("207fffff")

        self.assertEqual(scaled_target_difficulty(pow_limit), 1_000_000)
        self.assertEqual(scaled_target_difficulty(pow_limit // 4), 4_000_000)
    def test_qbit_gbt_rules_include_signet_rule_only_for_signet(self) -> None:
        self.assertEqual(qbit_gbt_rules("regtest"), ["segwit"])
        self.assertEqual(qbit_gbt_rules("testnet4"), ["segwit"])
        self.assertEqual(qbit_gbt_rules("signet"), ["segwit", "signet"])
    def test_qbit_template_fingerprint_ignores_clock_only_fields(self) -> None:
        base = gbt_template("00" * 32, curtime=1)
        base["longpollid"] = "10:0"
        base["mintime"] = 1
        clock_only = gbt_template("00" * 32, curtime=2)
        clock_only["longpollid"] = "10:1"
        clock_only["mintime"] = 2
        changed_value = dict(clock_only)
        changed_value["coinbasevalue"] = 49_99999999

        self.assertEqual(qbit_template_fingerprint(base), qbit_template_fingerprint(clock_only))
        self.assertNotEqual(qbit_template_fingerprint(base), qbit_template_fingerprint(changed_value))
    def test_resolve_version_mask_uses_gbt_versionrollingmask(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = TemplateRpc({"versionrollingmask": "1fffe000"})
        server.rpc = rpc
        server.qbit_chain = "signet"

        selection = server.resolve_version_rolling_mask(0x000000FF)

        self.assertEqual(selection.selected_mask, 0x1FFFE000)
        self.assertEqual(selection.source, "qbit_getblocktemplate")
        self.assertEqual(rpc.calls, [("getblocktemplate", [{"rules": ["segwit", "signet"]}])])
    def test_resolve_version_mask_falls_back_only_when_gbt_missing_or_unavailable(self) -> None:
        missing = PrismCoordinator.__new__(PrismCoordinator)
        missing.rpc = TemplateRpc({})
        missing.qbit_chain = "regtest"

        missing_selection = missing.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)

        self.assertEqual(missing_selection.selected_mask, direct_stratum.QBIT_VERSION_ROLLING_MASK)
        self.assertEqual(missing_selection.source, "fallback")
        self.assertEqual(missing_selection.detail, "missing_versionrollingmask")

        unavailable = PrismCoordinator.__new__(PrismCoordinator)
        unavailable.rpc = FakeRpc()
        unavailable.qbit_chain = "regtest"

        unavailable_selection = unavailable.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)

        self.assertEqual(unavailable_selection.selected_mask, direct_stratum.QBIT_VERSION_ROLLING_MASK)
        self.assertEqual(unavailable_selection.source, "fallback")
        self.assertTrue(unavailable_selection.detail.startswith("probe_error:"))
    def test_resolve_version_mask_disables_only_on_gbt_zero_mask(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.rpc = TemplateRpc({"versionrollingmask": "00000000"})
        server.qbit_chain = "regtest"

        selection = server.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)

        self.assertEqual(selection.selected_mask, 0)
        self.assertEqual(selection.source, "qbit_getblocktemplate")
        self.assertEqual(selection.detail, "disabled_by_zero_mask")
    def test_resolve_version_mask_rejects_invalid_gbt_mask(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.rpc = TemplateRpc({"versionrollingmask": "not-hex"})
        server.qbit_chain = "regtest"

        with self.assertRaisesRegex(SystemExit, "invalid getblocktemplate.versionrollingmask"):
            server.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)
    def test_configure_negotiates_requested_mask_with_gbt_server_mask(self) -> None:
        server = coordinator()
        server.version_mask = 0x1FFFE000
        state = client()
        captured: dict[str, object] = {}
        server.send_result = lambda _client, request_id, result: captured.update(  # type: ignore[method-assign]
            {"request_id": request_id, "result": result}
        )

        server.handle_configure(
            state,
            "configure-1",
            [
                ["version-rolling"],
                {"version-rolling.mask": "0000f000"},
            ],
        )

        self.assertEqual(captured["request_id"], "configure-1")
        self.assertEqual(
            captured["result"],
            {
                "version-rolling": True,
                "version-rolling.mask": "0000e000",
            },
        )
        self.assertEqual(state.version_mask, 0x0000E000)
    def test_configure_disables_version_rolling_when_gbt_mask_is_zero(self) -> None:
        server = coordinator()
        server.version_mask = 0
        state = client()
        captured: dict[str, object] = {}
        server.send_result = lambda _client, request_id, result: captured.update(  # type: ignore[method-assign]
            {"request_id": request_id, "result": result}
        )

        server.handle_configure(
            state,
            "configure-1",
            [
                ["version-rolling"],
                {"version-rolling.mask": "ffffffff"},
            ],
        )

        self.assertEqual(
            captured["result"],
            {
                "version-rolling": False,
                "version-rolling.mask": "00000000",
            },
        )
        self.assertEqual(state.version_mask, 0)
    def test_accepted_share_difficulty_uses_actual_target_unless_overridden(self) -> None:
        server = coordinator()
        worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        context = SimpleNamespace(
            worker=worker,
            job=SimpleNamespace(share_target=target_from_compact("207fffff") // 2),
        )
        server.share_weights_by_username = {}

        self.assertEqual(server.accepted_share_difficulty(context), 2_000_000)

        server.share_weights_by_username = {"miner-a": 7}
        self.assertEqual(server.accepted_share_difficulty(context), 7)
    def test_resolve_worker_accepts_bare_payout_address(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(script_byte="22")
        server.rpc = rpc

        worker = server.resolve_worker(PAYOUT_ADDRESS)

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])
        self.assertEqual(worker.username, PAYOUT_ADDRESS)
        self.assertEqual(worker.payout_address, PAYOUT_ADDRESS)
        self.assertIsNone(worker.worker_name)
        self.assertEqual(worker.p2mr_program_hex, "22" * 32)
    def test_resolve_worker_accepts_address_worker_username(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(script_byte="33")
        server.rpc = rpc

        worker = server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-a")

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])
        self.assertEqual(worker.username, f"{PAYOUT_ADDRESS}.rig-a")
        self.assertEqual(worker.payout_address, PAYOUT_ADDRESS)
        self.assertEqual(worker.worker_name, "rig-a")
        self.assertEqual(worker.p2mr_program_hex, "33" * 32)
    def test_resolve_worker_caches_successful_address_validation(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(script_byte="33")
        server.rpc = rpc

        first = server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-a")
        second = server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-b")

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])
        self.assertEqual(first.p2mr_program_hex, second.p2mr_program_hex)
    def test_payout_address_cache_evicts_least_recently_used_entry(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.payout_address_cache_max_entries = 2
        server.payout_address_cache_ttl_seconds = 60

        class AnyAddressRpc:
            def __init__(self) -> None:
                self.validated: list[str] = []

            def call(self, method: str, params: list[object] | None = None) -> object:
                address = str((params or [""])[0])
                self.validated.append(address)
                return {"isvalid": True, "scriptPubKey": "5220" + "33" * 32}

        rpc = AnyAddressRpc()
        server.rpc = rpc

        server.validate_p2mr_address("address-a", label="test")
        server.validate_p2mr_address("address-b", label="test")
        server.validate_p2mr_address("address-a", label="test")
        server.validate_p2mr_address("address-c", label="test")

        self.assertEqual(rpc.validated, ["address-a", "address-b", "address-c"])
        self.assertEqual(list(server._p2mr_address_cache), ["address-a", "address-c"])

        server.validate_p2mr_address("address-b", label="test")
        self.assertEqual(rpc.validated[-1], "address-b")
        self.assertEqual(len(server._p2mr_address_cache), 2)
    def test_payout_address_cache_revalidates_expired_entry(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.payout_address_cache_max_entries = 2
        server.payout_address_cache_ttl_seconds = 5
        rpc = AddressValidationRpc(script_byte="33")
        server.rpc = rpc
        now = 100.0

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: now,
        ):
            server.validate_p2mr_address(PAYOUT_ADDRESS, label="test")
            now = 104.0
            server.validate_p2mr_address(PAYOUT_ADDRESS, label="test")
            now = 106.0
            server.validate_p2mr_address(PAYOUT_ADDRESS, label="test")

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS, PAYOUT_ADDRESS])
    def test_concurrent_worker_resolution_singleflights_address_validation(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        entered = threading.Event()
        release = threading.Event()

        class BlockingAddressRpc(AddressValidationRpc):
            def call(self, method: str, params: list[object] | None = None) -> object:
                if method == "validateaddress":
                    entered.set()
                    if not release.wait(timeout=5):
                        raise TimeoutError("test did not release validateaddress")
                return super().call(method, params)

        rpc = BlockingAddressRpc(script_byte="33")
        server.rpc = rpc
        server._ensure_p2mr_address_cache_state()
        workers: list[WorkerIdentity] = []
        errors: list[BaseException] = []

        def resolve(index: int) -> None:
            try:
                workers.append(server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-{index}"))
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=resolve, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        self.assertTrue(entered.wait(timeout=5))
        release.set()
        for thread in threads:
            thread.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(len(workers), 8)
        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])
    def test_concurrent_failed_worker_resolution_shares_singleflight_error(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        entered = threading.Event()
        release = threading.Event()

        class FailingAddressRpc:
            def __init__(self) -> None:
                self.calls = 0

            def call(self, method: str, params: list[object] | None = None) -> object:
                self.calls += 1
                entered.set()
                if not release.wait(timeout=5):
                    raise TimeoutError("test did not release validateaddress")
                raise RuntimeError("qbitd unavailable")

        rpc = FailingAddressRpc()
        server.rpc = rpc
        server._ensure_p2mr_address_cache_state()
        errors: list[BaseException] = []

        def resolve() -> None:
            try:
                server.resolve_worker(PAYOUT_ADDRESS)
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=resolve) for _ in range(8)]
        for thread in threads:
            thread.start()
        self.assertTrue(entered.wait(timeout=5))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with server._p2mr_address_cache_lock:
                pending = server._p2mr_address_validation_inflight[PAYOUT_ADDRESS]
                if pending.waiters == 7:
                    break
            time.sleep(0.001)
        self.assertEqual(pending.waiters, 7)
        release.set()
        for thread in threads:
            thread.join(timeout=5)

        self.assertEqual(rpc.calls, 1)
        self.assertEqual(len(errors), 8)
        self.assertTrue(all("qbitd unavailable" in str(exc) for exc in errors))
    def test_resolve_worker_rejects_invalid_base_address_with_worker_suffix(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc()
        server.rpc = rpc
        server.username_fallback_address = None

        with self.assertRaises(StratumError) as raised:
            server.resolve_worker("not-a-qbit-address.rig-a")

        self.assertEqual(raised.exception.code, 20)
        self.assertEqual(rpc.validated, ["not-a-qbit-address"])
    def test_resolve_worker_uses_configured_fallback_for_invalid_username(self) -> None:
        fallback_address = "tq1fallback"
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(valid_address=fallback_address, script_byte="44")
        server.rpc = rpc
        server.username_fallback_address = fallback_address

        worker = server.resolve_worker("not-a-qbit-address.rig-a")

        self.assertEqual(rpc.validated, ["not-a-qbit-address", fallback_address])
        self.assertEqual(worker.username, "not-a-qbit-address.rig-a")
        self.assertEqual(worker.payout_address, fallback_address)
        self.assertEqual(worker.worker_name, "rig-a")
        self.assertEqual(worker.p2mr_program_hex, "44" * 32)
    def test_resolve_worker_uses_testnet_default_fallback_for_invalid_username(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(valid_address=DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS, script_byte="55")
        server.rpc = rpc

        with patch.dict(os.environ, {"QBIT_CHAIN": "testnet4"}, clear=True):
            worker = server.resolve_worker("not-a-qbit-address")

        self.assertEqual(rpc.validated, ["not-a-qbit-address", DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS])
        self.assertEqual(worker.username, "not-a-qbit-address")
        self.assertEqual(worker.payout_address, DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS)
        self.assertIsNone(worker.worker_name)
        self.assertEqual(worker.p2mr_program_hex, "55" * 32)
    def test_default_username_fallback_is_testnet_only_unless_configured(self) -> None:
        with patch.dict(os.environ, {"QBIT_CHAIN": "testnet4"}, clear=True):
            self.assertEqual(default_prism_username_fallback_address(), DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS)
        with patch.dict(os.environ, {"QBIT_CHAIN": "regtest"}, clear=True):
            self.assertIsNone(default_prism_username_fallback_address())
        with patch.dict(
            os.environ,
            {"QBIT_CHAIN": "regtest", "PRISM_USERNAME_FALLBACK_ADDRESS": "qbrt1fallback"},
            clear=True,
        ):
            self.assertEqual(default_prism_username_fallback_address(), "qbrt1fallback")
    def test_prism_payout_policy_defaults_to_no_pool_fee(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy,
            {
                "p2mr_spend_input_bytes": 3_680,
                "target_feerate_sats_per_byte": 1,
                "safety_multiplier": 4,
            },
        )
    def test_prism_payout_policy_allows_fixed_min_output_bits_override(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {"PRISM_PAYOUT_MIN_OUTPUT_BITS": "10000"}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy,
            {
                "p2mr_spend_input_bytes": 3_680,
                "target_feerate_sats_per_byte": 1,
                "safety_multiplier": 4,
                "min_output_sats": 10_000,
            },
        )
    def test_prism_payout_policy_falls_back_to_legacy_min_output_sats_override(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {"PRISM_PAYOUT_MIN_OUTPUT_SATS": "10000"}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["min_output_sats"], 10_000)
    def test_prism_payout_policy_min_output_bits_overrides_legacy_sats_override(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_PAYOUT_MIN_OUTPUT_BITS": "11000",
                "PRISM_PAYOUT_MIN_OUTPUT_SATS": "10000",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["min_output_sats"], 11_000)
    def test_prism_coinbase_tag_defaults_to_prism(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(default_prism_coinbase_tag_hex(), "/PRISM/".encode("ascii").hex())
    def test_prism_coinbase_tag_is_configurable_and_can_be_disabled(self) -> None:
        with patch.dict(os.environ, {"PRISM_COINBASE_TAG": "/CUSTOM/"}, clear=True):
            self.assertEqual(default_prism_coinbase_tag_hex(), "/CUSTOM/".encode("ascii").hex())
        with patch.dict(os.environ, {"PRISM_COINBASE_TAG": ""}, clear=True):
            self.assertEqual(default_prism_coinbase_tag_hex(), "")
    def test_prism_coinbase_tag_rejects_non_printable_or_long_values(self) -> None:
        for tag, message in (
            ("PRISM\n", "printable ASCII"),
            ("P" * 41, "at most 40 bytes"),
            ("PRISM-π", "ASCII"),
        ):
            with self.subTest(tag=tag), patch.dict(
                os.environ, {"PRISM_COINBASE_TAG": tag}, clear=True
            ):
                with self.assertRaisesRegex(SystemExit, message):
                    default_prism_coinbase_tag_hex()
    def test_coinbase_script_sig_suffix_places_pool_tag_before_extranonce(self) -> None:
        server = coordinator()
        server.coinbase_tag_hex = "/PRISM/".encode("ascii").hex()

        suffix = server.coinbase_script_sig_suffix_hex("aabbccdd", "00" * 8)

        self.assertEqual(suffix, "/PRISM/".encode("ascii").hex() + "aabbccdd" + "00" * 8)
        self.assertTrue(suffix.endswith("aabbccdd" + "00" * 8))
    def test_prism_payout_policy_allows_formula_overrides(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES": "2500",
                "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE": "2",
                "PRISM_PAYOUT_SAFETY_MULTIPLIER": "3",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy,
            {
                "p2mr_spend_input_bytes": 2_500,
                "target_feerate_sats_per_byte": 2,
                "safety_multiplier": 3,
            },
        )
    def test_prism_payout_policy_formula_uses_legacy_feerate_alias(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {"PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE": "2"}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["target_feerate_sats_per_byte"], 2)
    def test_prism_payout_policy_formula_bits_feerate_overrides_legacy_alias(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE": "3",
                "PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE": "2",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["target_feerate_sats_per_byte"], 3)
    def test_prism_payout_policy_rejects_invalid_floor_settings(self) -> None:
        cases = [
            ({"PRISM_PAYOUT_MIN_OUTPUT_BITS": "0"}, "PRISM_PAYOUT_MIN_OUTPUT_BITS must be positive"),
            (
                {"PRISM_PAYOUT_MIN_OUTPUT_BITS": "not-int"},
                "PRISM_PAYOUT_MIN_OUTPUT_BITS must be an integer",
            ),
            ({"PRISM_PAYOUT_MIN_OUTPUT_SATS": "0"}, "PRISM_PAYOUT_MIN_OUTPUT_SATS must be positive"),
            (
                {"PRISM_PAYOUT_MIN_OUTPUT_SATS": "not-int"},
                "PRISM_PAYOUT_MIN_OUTPUT_SATS must be an integer",
            ),
            (
                {"PRISM_PAYOUT_SAFETY_MULTIPLIER": "0"},
                "PRISM_PAYOUT_SAFETY_MULTIPLIER must be positive",
            ),
            (
                {"PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE": "0"},
                "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE must be positive",
            ),
        ]
        for env_vars, expected in cases:
            with self.subTest(env_vars=env_vars), patch.dict(os.environ, env_vars, clear=True):
                server = coordinator()
                with self.assertRaisesRegex(SystemExit, expected):
                    server.prism_payout_policy()
    def test_prism_pool_fee_address_config_validates_p2mr_address(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="88")

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(server.rpc.validated, ["tq1fee"])
        self.assertEqual(
            policy["pool_fee_policy"],
            {
                "fee_bps": 125,
                "recipient_id": "tq1fee",
                "order_key": "tq1fee",
                "p2mr_program_hex": "88" * 32,
            },
        )
    def test_prism_pool_fee_enabled_allows_zero_bps_policy(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="66")

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "0",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["pool_fee_policy"]["fee_bps"], 0)
        self.assertEqual(policy["pool_fee_policy"]["p2mr_program_hex"], "66" * 32)
    def test_prism_pool_fee_program_config_requires_recipient_identity(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32,
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "PRISM_POOL_FEE_RECIPIENT_ID"):
                server.prism_payout_policy()
    def test_prism_pool_fee_program_config_uses_explicit_order_key(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32,
                "PRISM_POOL_FEE_RECIPIENT_ID": "pool-fee",
                "PRISM_POOL_FEE_ORDER_KEY": "000-pool-fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy["pool_fee_policy"],
            {
                "fee_bps": 125,
                "recipient_id": "pool-fee",
                "order_key": "000-pool-fee",
                "p2mr_program_hex": "55" * 32,
            },
        )
    def test_prism_pool_fee_config_rejects_ambiguous_or_invalid_settings(self) -> None:
        cases = [
            (
                {"PRISM_POOL_FEE_ENABLED": "1", "PRISM_POOL_FEE_ADDRESS": "tq1fee"},
                "PRISM_POOL_FEE_BPS",
            ),
            (
                {
                    "PRISM_POOL_FEE_ENABLED": "1",
                    "PRISM_POOL_FEE_BPS": "10001",
                    "PRISM_POOL_FEE_ADDRESS": "tq1fee",
                },
                "between 0 and 10000",
            ),
            (
                {
                    "PRISM_POOL_FEE_ENABLED": "1",
                    "PRISM_POOL_FEE_BPS": "125",
                    "PRISM_POOL_FEE_ADDRESS": "tq1fee",
                    "PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32,
                },
                "exactly one",
            ),
        ]
        for env_vars, expected in cases:
            with self.subTest(env_vars=env_vars), patch.dict(os.environ, env_vars, clear=True):
                server = coordinator()
                server.rpc = AddressRpc(valid_address="tq1fee")
                with self.assertRaisesRegex(SystemExit, expected):
                    server.prism_payout_policy()
    def test_prism_pool_fee_config_rejects_disabled_fee_settings(self) -> None:
        cases = [
            {"PRISM_POOL_FEE_BPS": "125"},
            {"PRISM_POOL_FEE_ADDRESS": "tq1fee"},
            {"PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32},
            {"PRISM_POOL_FEE_RECIPIENT_ID": "pool-fee"},
            {
                "PRISM_POOL_FEE_ENABLED": "0",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
        ]
        for env_vars in cases:
            with self.subTest(env_vars=env_vars), patch.dict(os.environ, env_vars, clear=True):
                server = coordinator()
                server.rpc = AddressRpc(valid_address="tq1fee")
                with self.assertRaisesRegex(SystemExit, "PRISM_POOL_FEE_ENABLED=1"):
                    server.prism_payout_policy()
    def test_prism_pool_fee_config_rejects_non_p2mr_fee_address(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", p2mr=False)

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "P2MR"):
                server.prism_payout_policy()
    def test_prism_ctv_settlement_config_uses_legacy_unit_aliases(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_SATS": "10485760",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT": "25",
            },
            clear=True,
        ):
            config = server.prism_ctv_settlement_config()

        self.assertIsNotNone(config)
        assert config is not None
        self.assertEqual(config["direct_floor_sats"], 10_485_760)
        self.assertEqual(
            config["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 25, "premium_bps": 12_000},
        )
    def test_prism_ctv_settlement_config_uses_node_fee_estimate_by_default(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"feerate": "0.00001001"})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            config = server.prism_ctv_settlement_config()

        self.assertIsNotNone(config)
        assert config is not None
        self.assertEqual(
            config["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 1001, "premium_bps": 12_000},
        )
        self.assertIn(("estimatesmartfee", [2]), rpc.calls)
    def test_prism_ctv_settlement_config_caches_node_fee_estimate_per_block_height(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"feerate": "0.00001001"})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            first = server.prism_ctv_settlement_config(block_height=10)
            rpc.estimate = {"feerate": "0.00002000"}
            second = server.prism_ctv_settlement_config(block_height=10)
            next_height = server.prism_ctv_settlement_config(block_height=11)

        assert first is not None
        assert second is not None
        assert next_height is not None
        self.assertEqual(
            first["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 1001, "premium_bps": 12_000},
        )
        self.assertEqual(second["fanout_fee_rate_policy"], first["fanout_fee_rate_policy"])
        self.assertEqual(
            next_height["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 2000, "premium_bps": 12_000},
        )
        self.assertEqual(
            [call for call in rpc.calls if call[0] == "estimatesmartfee"],
            [("estimatesmartfee", [2]), ("estimatesmartfee", [2])],
        )
    def test_prism_ctv_settlement_config_separates_fee_cache_by_parent_hash(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"feerate": "0.00001001"})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            first = server.prism_ctv_settlement_config(block_height=10, parent_hash="aa" * 32)
            rpc.estimate = {"feerate": "0.00002000"}
            same_parent = server.prism_ctv_settlement_config(block_height=10, parent_hash="aa" * 32)
            reorg_parent = server.prism_ctv_settlement_config(block_height=10, parent_hash="bb" * 32)

        assert first is not None
        assert same_parent is not None
        assert reorg_parent is not None
        self.assertEqual(same_parent["fanout_fee_rate_policy"], first["fanout_fee_rate_policy"])
        self.assertEqual(
            reorg_parent["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 2000, "premium_bps": 12_000},
        )
        self.assertEqual(
            [call for call in rpc.calls if call[0] == "estimatesmartfee"],
            [("estimatesmartfee", [2]), ("estimatesmartfee", [2])],
        )
    def test_prism_ctv_settlement_config_fails_closed_when_fee_estimate_unavailable(self) -> None:
        server = coordinator()
        server.rpc = FeeEstimateRpc({"errors": ["insufficient data"]})

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "unable to compute PRISM CTV fanout fee rate"):
                server.prism_ctv_settlement_config()
    def test_prism_ctv_settlement_config_retries_after_fee_estimate_failure(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"errors": ["insufficient data"]})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            with self.assertRaises(RuntimeError):
                server.prism_ctv_settlement_config(block_height=10)
            rpc.estimate = {"feerate": "0.00002000"}
            recovered = server.prism_ctv_settlement_config(block_height=10)

        assert recovered is not None
        self.assertEqual(
            recovered["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 2000, "premium_bps": 12_000},
        )
        self.assertEqual(
            [call for call in rpc.calls if call[0] == "estimatesmartfee"],
            [("estimatesmartfee", [2]), ("estimatesmartfee", [2])],
        )
    def test_ctv_broadcaster_daemon_uses_coordinator_ledger_and_config(self) -> None:
        server = coordinator()
        server.ctv_broadcaster_wallet = None
        server.ctv_broadcaster_fee_sats = 0
        server.ctv_broadcaster_limit = 7
        captured: dict[str, object] = {}

        class FakeDaemon:
            def __init__(self, ledger: object, broadcaster: object, *, fee_sats: int) -> None:
                captured["ledger"] = ledger
                captured["broadcaster"] = broadcaster
                captured["fee_sats"] = fee_sats

            def run_once(
                self,
                *,
                limit: int,
                progress_callback: object,
                chunk_size: int,
                tip_refresh_pending: object,
                chunk_callback: object,
            ) -> object:
                captured["limit"] = limit
                captured["chunk_size"] = chunk_size
                captured["tip_refresh_pending"] = tip_refresh_pending
                captured["chunk_callback"] = chunk_callback
                return SimpleNamespace(
                    scanned_count=1,
                    submitted_count=0,
                    updated_count=1,
                    failed_count=0,
                    yielded_to_tip_refresh=False,
                )

        with patch("lab.prism.prism_coordinator.CtvFanoutBroadcastDaemon", FakeDaemon):
            result = server.run_ctv_fanout_broadcaster_once()

        self.assertIs(captured["ledger"], server.ledger)
        self.assertEqual(captured["fee_sats"], 0)
        self.assertEqual(captured["limit"], 7)
        self.assertEqual(captured["chunk_size"], 5)
        self.assertTrue(callable(captured["tip_refresh_pending"]))
        self.assertTrue(callable(captured["chunk_callback"]))
        self.assertEqual(result.updated_count, 1)
        self.assertIsNotNone(captured["broadcaster"])
    def test_ctv_broadcaster_daemon_requires_wallet_for_cpfp_fee(self) -> None:
        server = coordinator()
        server.ctv_broadcaster_wallet = None
        server.ctv_broadcaster_fee_sats = 1

        with self.assertRaisesRegex(ValueError, "ctv_broadcaster_wallet is required"):
            server.make_ctv_fanout_broadcast_daemon()
    def test_make_ledger_requires_explicit_memory_opt_in(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit):
                server.make_ledger()

        with patch.dict(os.environ, {"PRISM_ALLOW_MEMORY_LEDGER": "1"}, clear=True):
            ledger = server.make_ledger()

        self.assertEqual(ledger.backend_name, "memory")
    def test_trusted_ledger_key_must_be_configured_or_explicitly_test_mode(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit):
                server.load_trusted_ledger_writer_public_key()

        with patch.dict(os.environ, {"PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "aa" * 32}, clear=True):
            self.assertEqual(server.load_trusted_ledger_writer_public_key(), "aa" * 32)

        with patch.dict(os.environ, {"PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1"}, clear=True):
            self.assertIsNone(server.load_trusted_ledger_writer_public_key())
    def test_fixed_ledger_session_token_requires_explicit_opt_in(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        env = {
            "PRISM_POSTGRES_PSQL_COMMAND": "psql postgresql://example.invalid/qbit",
            "PRISM_LEDGER_WRITER_SESSION_TOKEN": "fixed-session",
        }

        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(SystemExit):
                server.make_ledger()

        with patch.dict(os.environ, {**env, "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN": "1"}, clear=True):
            with patch("lab.prism.prism_coordinator.PsqlShareLedger") as fake_ledger:
                fake_ledger.return_value = SimpleNamespace(backend_name="postgres-psql")
                ledger = server.make_ledger()

        self.assertEqual(ledger.backend_name, "postgres-psql")
        self.assertEqual(fake_ledger.call_args.kwargs["writer_session_token"], "fixed-session")
    def test_same_tip_retention_requires_connection_derived_production_bound(self) -> None:
        with self.assertRaisesRegex(
            SystemExit,
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
        ):
            validate_same_tip_job_retention_limits(
                retention_seconds=30,
                per_connection=0,
                max_connections=0,
                production=False,
            )
        with self.assertRaisesRegex(SystemExit, "PRISM_STRATUM_MAX_CONNECTIONS"):
            validate_same_tip_job_retention_limits(
                retention_seconds=30,
                per_connection=64,
                max_connections=0,
                production=True,
            )

        validate_same_tip_job_retention_limits(
            retention_seconds=30,
            per_connection=64,
            max_connections=1_900,
            production=True,
        )
        validate_same_tip_job_retention_limits(
            retention_seconds=30,
            per_connection=64,
            max_connections=0,
            production=False,
        )
        validate_same_tip_job_retention_limits(
            retention_seconds=0,
            per_connection=0,
            max_connections=0,
            production=True,
        )
    def test_production_gate_rejects_prism_test_bypasses_without_capacity_evidence(self) -> None:
        base = {
            "QBIT_PRODUCTION": "1",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "PRISM_POSTGRES_PSQL_COMMAND": "psql postgresql://example.invalid/qbit",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
            "PRISM_LEDGER_WRITER_ID": "managed-writer",
            "PRISM_LEDGER_WRITER_EPOCH": "7",
            "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            "PRISM_STRATUM_MAX_CONNECTIONS": "1900",
        }
        for name in (
            "PRISM_ALLOW_MEMORY_LEDGER",
            "PRISM_ALLOW_TEST_SIGNING_SEEDS",
            "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY",
            "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN",
        ):
            with self.subTest(name=name), patch.dict(os.environ, {**base, name: "1"}, clear=True):
                with self.assertRaisesRegex(SystemExit, name):
                    validate_prism_production_gate()

        with patch.dict(os.environ, base, clear=True):
            validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {**base, "PRISM_STRATUM_MAX_CONNECTIONS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "PRISM_STRATUM_MAX_CONNECTIONS"):
                validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {**base, "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                SystemExit,
                "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS",
            ):
                validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {**base, "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                SystemExit,
                "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
            ):
                validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {**base, "QBIT_CHAIN": "mainnet", "PRISM_STRATUM_STALE_GRACE_SECONDS": "3"},
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "mainnet requires PRISM_STRATUM_STALE_GRACE_SECONDS=0"):
                validate_prism_production_gate()

        # Off mainnet, production mode accepts a bounded grace window.
        with patch.dict(
            os.environ,
            {**base, "PRISM_STRATUM_STALE_GRACE_SECONDS": "2"},
            clear=True,
        ):
            validate_prism_production_gate()

        with patch.dict(os.environ, {**base, "PRISM_POSTGRES_PASSWORD": "change-this"}, clear=True):
            with self.assertRaisesRegex(SystemExit, "PRISM_POSTGRES_PASSWORD"):
                validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {
                **base,
                "PRISM_POSTGRES_PASSWORD": "not-default",
                "PRISM_POSTGRES_PSQL_COMMAND": "",
                "PRISM_DATABASE_URL": "postgresql://qbit:change-this@prism-postgres:5432/qbit",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "PRISM_DATABASE_URL"):
                validate_prism_production_gate()
    def test_production_gate_rejects_unsafe_difficulty_without_capacity_gate(self) -> None:
        base = {
            "QBIT_PRODUCTION": "1",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "PRISM_POSTGRES_PSQL_COMMAND": "psql postgresql://example.invalid/qbit",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
            "PRISM_LEDGER_WRITER_ID": "managed-writer",
            "PRISM_LEDGER_WRITER_EPOCH": "7",
            "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
        }
        cases = (
            ({"PRISM_STRATUM_SHARE_DIFF": ""}, "requires PRISM_STRATUM_SHARE_DIFF"),
            ({"PRISM_STRATUM_SHARE_DIFF": "not-a-decimal"}, "must be a decimal number"),
            ({"PRISM_STRATUM_SHARE_DIFF": "NaN"}, "PRISM_STRATUM_SHARE_DIFF must be positive"),
            ({"PRISM_STRATUM_SHARE_DIFF": "0"}, "PRISM_STRATUM_SHARE_DIFF must be positive"),
            ({"PRISM_STRATUM_SHARE_DIFF": "1e-9"}, "lab-only 1e-9 difficulty"),
            ({"PRISM_STRATUM_VARDIFF_MIN_DIFF": "8192"}, "minimum exceeds its start"),
            ({"PRISM_STRATUM_VARDIFF_START_DIFF": "131072"}, "start exceeds its maximum"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides), patch.dict(
                os.environ,
                {**base, **overrides},
                clear=True,
            ):
                with self.assertRaisesRegex(SystemExit, message):
                    validate_prism_production_gate()
    def test_mainnet_implies_production_gate(self) -> None:
        with patch.dict(
            os.environ,
            {
                "QBIT_CHAIN": "mainnet",
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "requires PRISM_STRATUM_SHARE_DIFF"):
                validate_prism_production_gate()
    def test_compatibility_production_flag_implies_production_gate(self) -> None:
        with patch.dict(
            os.environ,
            {
                "QBIT_CHAIN": "testnet4",
                "QBIT_TOOLS_PRODUCTION": "1",
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "requires PRISM_STRATUM_SHARE_DIFF"):
                validate_prism_production_gate()
    def test_mainnet_ctv_requires_static_fee_rate_before_runtime_startup(self) -> None:
        env = {
            "QBIT_CHAIN": "mainnet",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "PRISM_POSTGRES_PSQL_COMMAND": "psql postgresql://example.invalid/qbit",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
            "PRISM_LEDGER_WRITER_ID": "managed-writer",
            "PRISM_LEDGER_WRITER_EPOCH": "7",
            "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            "PRISM_CTV_SETTLEMENT_ENABLED": "1",
        }
        with (
            patch.dict(os.environ, env, clear=True),
            self.assertRaisesRegex(
                SystemExit,
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
            ),
        ):
            validate_prism_production_gate()
    def test_live_chain_identity_accepts_main_alias_and_pinned_genesis(self) -> None:
        genesis = "12" * 32
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.qbit_chain = "mainnet"

        class Rpc:
            def call(self, method: str, params: object = None) -> object:
                if method == "getblockchaininfo":
                    return {
                        "chain": "main",
                        "initialblockdownload": False,
                        "blocks": 100,
                        "headers": 100,
                    }
                if method == "getnetworkinfo":
                    return {"connections": 2}
                if method == "getblockhash" and params == [0]:
                    return genesis
                raise RuntimeError(method)

        server.rpc = Rpc()
        with patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": genesis}, clear=True):
            server.validate_live_chain_identity()
    def test_live_chain_identity_rejects_incomplete_public_readiness(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.qbit_chain = "mainnet"
        genesis = "12" * 32

        cases = (
            ({"chain": "main", "blocks": 10, "headers": 10}, {"connections": 1}, "initial block"),
            (
                {"chain": "main", "initialblockdownload": False, "blocks": 9, "headers": 10},
                {"connections": 1},
                "not caught up",
            ),
            (
                {"chain": "main", "initialblockdownload": False, "blocks": 10, "headers": 10},
                {"connections": 0},
                "requires at least 1",
            ),
        )
        for blockchain_info, network_info, message in cases:
            with self.subTest(message=message):
                server.rpc = SimpleNamespace(
                    call=lambda method, params=None: (
                        blockchain_info
                        if method == "getblockchaininfo"
                        else network_info
                        if method == "getnetworkinfo"
                        else genesis
                    )
                )
                with (
                    patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": genesis}, clear=True),
                    self.assertRaisesRegex(RuntimeError, message),
                ):
                    server.validate_live_chain_identity()
    def test_live_template_preflight_enforces_freshness_and_relay_fee_floor(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        previous_hash = "34" * 32
        template = {"height": 1, "curtime": int(time.time()), "previousblockhash": previous_hash}
        server.current_template_artifacts = lambda: SimpleNamespace(
            template=template,
            previousblockhash=previous_hash,
        )
        server.rpc = SimpleNamespace(
            call=lambda method, params=None: {
                "minrelaytxfee": "0.00001000",
                "mempoolminfee": "0.00001000",
            }
        )
        server._ctv_fanout_market_fee_rate_cache = {}

        enabled = {
            "PRISM_CTV_SETTLEMENT_ENABLED": "1",
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "1000",
            "PRISM_TEMPLATE_MAX_AGE_SECONDS": "120",
        }
        with patch.dict(os.environ, enabled, clear=True):
            server.validate_live_template_and_fee_policy()

        server._ctv_fanout_market_fee_rate_cache = {}
        with (
            patch.dict(
                os.environ,
                {**enabled, "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "1"},
                clear=True,
            ),
            self.assertRaisesRegex(RuntimeError, "below the connected node relay floor"),
        ):
            server.validate_live_template_and_fee_policy()

        template["curtime"] = int(time.time()) - 121
        with (
            patch.dict(os.environ, enabled, clear=True),
            self.assertRaisesRegex(RuntimeError, "block template is stale"),
        ):
            server.validate_live_template_and_fee_policy()
    def test_live_chain_identity_rejects_wrong_chain_or_genesis(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.qbit_chain = "mainnet"
        server.rpc = SimpleNamespace(
            call=lambda method, params=None: (
                {"chain": "regtest"} if method == "getblockchaininfo" else "34" * 32
            )
        )
        with patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": "12" * 32}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "does not match RPC chain"):
                server.validate_live_chain_identity()

        server.rpc = SimpleNamespace(
            call=lambda method, params=None: (
                {"chain": "main"} if method == "getblockchaininfo" else "34" * 32
            )
        )
        with patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": "12" * 32}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "does not match the connected"):
                server.validate_live_chain_identity()
    def _pending_append(self, tag: str, accepted_at_ms: int = 2) -> PendingShareAppend:
        from lab.prism.share_ledger import PendingShare

        return PendingShareAppend(
            pending_share=PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex=tag * 32,
            collection_only=False,
            credit_policy=None,
        )

class PrismCoordinatorReliabilityTests(unittest.TestCase):
    def _bare_coordinator(self) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.lock = threading.RLock()
        server.stop_event = threading.Event()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 120.0
        server.watchdog_interval_seconds = 15.0
        return server
    def test_positive_float_env_rejects_non_finite_values(self) -> None:
        for raw in ("nan", "inf", "-inf"):
            with self.subTest(raw=raw), patch.dict(
                os.environ, {"PRISM_WATCHDOG_TIMEOUT_SECONDS": raw}, clear=True
            ):
                with self.assertRaisesRegex(SystemExit, "PRISM_WATCHDOG_TIMEOUT_SECONDS must be finite"):
                    env_positive_float("PRISM_WATCHDOG_TIMEOUT_SECONDS", 120.0)

class PrismListenerProfileTests(unittest.TestCase):
    def test_highdiff_listener_disabled_without_port(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            base = load_prism_vardiff_config(Decimal("0.000000001"))
            self.assertIsNone(load_prism_highdiff_listener("0.0.0.0", base))
    def test_highdiff_listener_defaults_to_nicehash_floor(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            base = load_prism_vardiff_config(Decimal("0.000000001"))
            profile = load_prism_highdiff_listener("10.0.0.1", base)

        assert profile is not None
        self.assertEqual(profile.name, "highdiff")
        self.assertEqual(profile.bind, "10.0.0.1")
        self.assertEqual(profile.port, 4334)
        self.assertEqual(profile.heartbeat_name, "stratum_accept_highdiff")
        self.assertEqual(profile.share_difficulty, Decimal("500000"))
        self.assertEqual(profile.minimum_advertised_difficulty, Decimal("500000"))
        self.assertEqual(profile.vardiff_config.min_difficulty, Decimal("500000"))
        self.assertEqual(profile.vardiff_config.startup_difficulty, Decimal("500000"))
        self.assertEqual(profile.vardiff_config.max_difficulty, Decimal("4294967296"))
        # Everything but the difficulty bounds is inherited from the base config.
        self.assertEqual(profile.vardiff_config.enabled, base.enabled)
        self.assertEqual(
            profile.vardiff_config.target_share_interval_seconds,
            base.target_share_interval_seconds,
        )
        self.assertEqual(
            profile.vardiff_config.retarget_interval_seconds,
            base.retarget_interval_seconds,
        )
        self.assertEqual(profile.vardiff_config.max_step_factor, base.max_step_factor)
    def test_highdiff_listener_env_overrides(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4335"
            os.environ["PRISM_STRATUM_HIGHDIFF_BIND"] = "127.0.0.2"
            os.environ["PRISM_STRATUM_HIGHDIFF_MIN_DIFF"] = "600000"
            os.environ["PRISM_STRATUM_HIGHDIFF_START_DIFF"] = "1000000"
            os.environ["PRISM_STRATUM_HIGHDIFF_MAX_DIFF"] = "8000000"
            os.environ["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "700000"
            base = load_prism_vardiff_config(Decimal("0.000000001"))
            profile = load_prism_highdiff_listener("0.0.0.0", base)

        assert profile is not None
        self.assertEqual(profile.bind, "127.0.0.2")
        self.assertEqual(profile.port, 4335)
        self.assertEqual(profile.share_difficulty, Decimal("700000"))
        self.assertEqual(profile.minimum_advertised_difficulty, Decimal("600000"))
        self.assertEqual(profile.vardiff_config.min_difficulty, Decimal("600000"))
        self.assertEqual(profile.vardiff_config.startup_difficulty, Decimal("1000000"))
        self.assertEqual(profile.vardiff_config.max_difficulty, Decimal("8000000"))
    def test_highdiff_listener_rejects_inconsistent_bounds(self) -> None:
        base = load_prism_vardiff_config(Decimal("0.000000001"))
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_MIN_DIFF"] = "1000000"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_MAX_DIFF"] = "400000"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)
        for bad_port in ("not-a-port", "0", "70000"):
            with patch.dict(os.environ, {}, clear=False):
                clear_stratum_diff_env()
                os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = bad_port
                with self.assertRaises(SystemExit):
                    load_prism_highdiff_listener("0.0.0.0", base)
    def test_client_startup_difficulty_uses_listener_profile(self) -> None:
        server = coordinator()
        profile = StratumListenerProfile(
            name="highdiff",
            bind="0.0.0.0",
            port=4334,
            share_difficulty=Decimal("700000"),
            vardiff_config=highdiff_vardiff_config(),
            heartbeat_name="stratum_accept_highdiff",
        )
        self.assertEqual(server.client_startup_difficulty(profile), Decimal("500000"))
        # Without a profile the default listener behavior is unchanged.
        self.assertEqual(server.client_startup_difficulty(), Decimal("0.000000001"))
        # With vardiff disabled the listener's fixed share difficulty applies.
        fixed_profile = StratumListenerProfile(
            name="highdiff",
            bind="0.0.0.0",
            port=4334,
            share_difficulty=Decimal("700000"),
            vardiff_config=highdiff_vardiff_config(enabled=False),
            heartbeat_name="stratum_accept_highdiff",
        )
        self.assertEqual(server.client_startup_difficulty(fixed_profile), Decimal("700000"))
    def test_stratum_accept_heartbeat_names(self) -> None:
        server = coordinator()
        # Coordinators built without listener profiles (tests, legacy) keep the
        # historical single heartbeat name.
        self.assertEqual(server.stratum_accept_heartbeat_names(), ("stratum_accept",))
        server.listener_profiles = [
            StratumListenerProfile(
                name="default",
                bind="0.0.0.0",
                port=3340,
                share_difficulty=Decimal("1"),
                vardiff_config=server.vardiff_config,
                heartbeat_name="stratum_accept",
            ),
            StratumListenerProfile(
                name="highdiff",
                bind="0.0.0.0",
                port=4334,
                share_difficulty=Decimal("500000"),
                vardiff_config=highdiff_vardiff_config(),
                heartbeat_name="stratum_accept_highdiff",
            ),
        ]
        self.assertEqual(
            server.stratum_accept_heartbeat_names(),
            ("stratum_accept", "stratum_accept_highdiff"),
        )
    def test_parse_stratum_password_options(self) -> None:
        self.assertEqual(parse_stratum_password_options(""), (None, None))
        self.assertEqual(parse_stratum_password_options("x"), (None, None))
        self.assertEqual(parse_stratum_password_options("d=8192"), (Decimal("8192"), None))
        self.assertEqual(
            parse_stratum_password_options("md=4096,d=8192"),
            (Decimal("8192"), Decimal("4096")),
        )
        self.assertEqual(
            parse_stratum_password_options("D=500000, MD=500000"),
            (Decimal("500000"), Decimal("500000")),
        )
        self.assertEqual(parse_stratum_password_options("d=abc"), (None, None))
        self.assertEqual(parse_stratum_password_options("d=-5,md=0"), (None, None))
        self.assertEqual(parse_stratum_password_options("foo=1,md=2048"), (None, Decimal("2048")))
    def test_password_d_below_highdiff_floor_is_clamped(self) -> None:
        server = coordinator()
        state = client()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.share_difficulty = Decimal("500000")
        state.requested_difficulty = Decimal("1000")

        target = server.apply_client_difficulty_requests(state)

        self.assertEqual(target, Decimal("500000"))
        assert state.vardiff_config is not None
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("500000"))
        self.assertEqual(state.vardiff_config.startup_difficulty, Decimal("500000"))
    def test_password_md_raises_personal_floor_and_retarget_respects_it(self) -> None:
        server = coordinator()
        state = client()
        state.requested_min_difficulty = Decimal("256")
        state.share_difficulty = Decimal("1")

        target = server.apply_client_difficulty_requests(state)

        self.assertEqual(target, Decimal("256"))
        assert state.vardiff_config is not None
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("256"))
        self.assertEqual(state.vardiff_config.max_difficulty, Decimal("1024"))

        # A zero-share retarget window wants to step down 4x; the personal
        # floor must hold it at 256.
        state.share_difficulty = Decimal("256")
        server.retarget_client(
            state,
            current_difficulty=Decimal("256"),
            accepted_shares=0,
            submitted_shares=0,
            accepted_difficulty=Decimal("0"),
            elapsed_seconds=Decimal("2"),
        )
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("256"))
    def test_apply_requests_is_stable_across_reapplication(self) -> None:
        server = coordinator()
        state = client()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.requested_min_difficulty = Decimal("600000")
        state.requested_difficulty = Decimal("700000")

        first = server.apply_client_difficulty_requests(state)
        second = server.apply_client_difficulty_requests(state)

        self.assertEqual(first, Decimal("700000"))
        self.assertEqual(second, Decimal("700000"))
        assert state.vardiff_config is not None
        # Recomputed from the pristine listener config: the floor is the md=
        # value, not a compounded one.
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("600000"))
    def test_suggest_difficulty_before_subscribe_applies_directly(self) -> None:
        server = coordinator()
        state = ClientState(sock=object(), address=("127.0.0.1", 1), connection_id=2, extranonce1_hex="00000002")
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 7, [512])

        self.assertEqual(state.suggested_difficulty, Decimal("512"))
        self.assertEqual(state.share_difficulty, Decimal("512"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(sent, [{"id": 7, "result": True, "error": None}])
    def test_suggest_difficulty_post_authorize_advertises_with_job(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        jobs: dict[str, object] = {"count": 0}

        def fake_send_job(client: object, clean_jobs: bool) -> bool:
            jobs.update({"count": jobs["count"] + 1, "clean": clean_jobs})
            return True

        server.maybe_send_job = fake_send_job  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 8, [512])

        self.assertEqual(state.pending_share_difficulty, Decimal("512"))
        self.assertEqual(jobs["count"], 1)
        self.assertTrue(jobs["clean"])
        self.assertEqual(sent, [{"id": 8, "result": True, "error": None}])
    def test_suggest_difficulty_rolls_back_pending_on_build_failure(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        state.difficulty_generation = 7
        state.send = lambda payload: None  # type: ignore[method-assign]
        server.maybe_send_job = lambda client, *, clean_jobs: False  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 9, [512])

        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("1"))
        self.assertEqual(state.difficulty_generation, 7)
    def test_suggest_difficulty_yields_to_password_d_option(self) -> None:
        server = coordinator()
        state = client()
        state.requested_difficulty = Decimal("512")
        state.share_difficulty = Decimal("512")
        state.send = lambda payload: None  # type: ignore[method-assign]
        server.maybe_send_job = lambda client, *, clean_jobs: True  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 10, [128])

        # d= wins: the suggestion is recorded but the resolved target stays at
        # the explicit password difficulty, so nothing is re-advertised.
        self.assertEqual(state.suggested_difficulty, Decimal("128"))
        self.assertEqual(state.share_difficulty, Decimal("512"))
        self.assertIsNone(state.pending_share_difficulty)
    def test_suggest_difficulty_ignores_junk_values(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        for junk in ([], ["nan"], ["-4"], ["0"], [None]):
            server.handle_suggest_difficulty(state, 11, junk)  # type: ignore[arg-type]

        self.assertIsNone(state.vardiff_config)
        self.assertEqual(state.share_difficulty, Decimal("1"))
        self.assertEqual(len(sent), 5)
    def test_highdiff_share_diff_tracks_start_and_validates_bounds(self) -> None:
        base = load_prism_vardiff_config(Decimal("0.000000001"))
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_MIN_DIFF"] = "1000000"
            os.environ["PRISM_STRATUM_HIGHDIFF_START_DIFF"] = "1000000"
            profile = load_prism_highdiff_listener("0.0.0.0", base)
        assert profile is not None
        # Unset fixed difficulty tracks the start difficulty instead of a
        # constant that could fall below a raised floor.
        self.assertEqual(profile.share_difficulty, Decimal("1000000"))

        # An explicit fixed difficulty outside the listener bounds must fail
        # startup: advertising below the floor is exactly what the high-diff
        # listener exists to prevent.
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "1000"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "8589934592"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)
    def authorize_server_and_client(self) -> tuple[PrismCoordinator, ClientState, list[object]]:
        server = coordinator()
        server.rpc = AddressValidationRpc()
        server.username_fallback_address = None
        server.maybe_send_job = lambda client, *, clean_jobs: True  # type: ignore[method-assign]
        state = ClientState(sock=object(), address=("127.0.0.1", 1), connection_id=3, extranonce1_hex="00000003")
        state.subscribed = True
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        return server, state, sent
    def test_authorize_password_applies_before_first_job(self) -> None:
        server, state, sent = self.authorize_server_and_client()

        server.handle_request(
            state,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "d=0.5,md=0.25"]},
        )

        self.assertTrue(state.authorized)
        self.assertEqual(state.requested_difficulty, Decimal("0.5"))
        self.assertEqual(state.requested_min_difficulty, Decimal("0.25"))
        # Applied directly (no job exists yet), so the first
        # set_difficulty/notify pair advertises the requested value.
        self.assertEqual(state.share_difficulty, Decimal("0.5"))
        self.assertIsNone(state.pending_share_difficulty)
        assert state.vardiff_config is not None
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("0.25"))
        self.assertEqual(sent, [{"id": 5, "result": True, "error": None}])
    def test_reauthorize_with_plain_password_clears_stale_overrides(self) -> None:
        server, state, _ = self.authorize_server_and_client()
        server.handle_request(
            state,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "d=0.5,md=0.25"]},
        )
        assert state.vardiff_config is not None

        server.handle_request(
            state,
            {"id": 6, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
        )

        # The new password carries no options: prior overrides are dropped and
        # the client falls back to the pristine listener policy (its current
        # difficulty is left alone; vardiff drifts it under listener bounds).
        self.assertIsNone(state.requested_difficulty)
        self.assertIsNone(state.requested_min_difficulty)
        self.assertIsNone(state.vardiff_config)
        self.assertEqual(state.share_difficulty, Decimal("0.5"))
    def test_reauthorize_with_new_difficulty_sends_single_job_pair(self) -> None:
        server, state, _ = self.authorize_server_and_client()
        send_job_calls: list[bool] = []

        def counting_send_job(current: ClientState, *, clean_jobs: bool) -> bool:
            send_job_calls.append(clean_jobs)
            current.active_job = SimpleNamespace(
                template={"previousblockhash": "aa" * 32},
                payout_state_generation=0,
            )
            server.current_tip_first_seen = ("aa" * 32, None)
            return True

        server.maybe_send_job = counting_send_job  # type: ignore[method-assign]

        server.handle_request(
            state,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
        )
        self.assertEqual(len(send_job_calls), 1)

        # A re-authorize whose new d= advertises a fresh difficulty/job pair
        # must not be followed by a second back-to-back pair.
        server.handle_request(
            state,
            {"id": 6, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "d=0.5"]},
        )
        self.assertEqual(len(send_job_calls), 2)
        self.assertEqual(state.pending_share_difficulty, Decimal("0.5"))
    def test_authorize_rejects_and_disconnects_above_username_connection_limit(self) -> None:
        server, first, _ = self.authorize_server_and_client()
        server.stratum_max_connections_per_username = 1
        second = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=4,
            extranonce1_hex="00000004",
        )
        second.send = lambda payload: None  # type: ignore[method-assign]
        server.clients.update({first, second})

        server.handle_request(
            first,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
        )
        with self.assertRaises(StratumError) as raised:
            server.handle_request(
                second,
                {"id": 6, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
            )

        self.assertTrue(raised.exception.disconnect)
        self.assertEqual(raised.exception.message, "too many connections for username")
        self.assertEqual(server.connection_limit_rejection_counts["username"], 1)
        self.assertFalse(second.authorized)
    def test_reauthorize_limit_error_preserves_live_session(self) -> None:
        server, live, _ = self.authorize_server_and_client()
        server.stratum_max_connections_per_username = 1
        occupant = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=4,
            extranonce1_hex="00000004",
        )
        occupant.send = lambda payload: None  # type: ignore[method-assign]
        server.clients.update({live, occupant})

        server.handle_request(
            live,
            {
                "id": 5,
                "method": "mining.authorize",
                "params": [f"{PAYOUT_ADDRESS}.original", "x"],
            },
        )
        server.handle_request(
            occupant,
            {
                "id": 6,
                "method": "mining.authorize",
                "params": [f"{PAYOUT_ADDRESS}.full", "x"],
            },
        )
        original_worker = live.worker

        with self.assertRaises(StratumError) as raised:
            server.handle_request(
                live,
                {
                    "id": 7,
                    "method": "mining.authorize",
                    "params": [f"{PAYOUT_ADDRESS}.full", "x"],
                },
            )

        self.assertFalse(raised.exception.disconnect)
        self.assertTrue(live.authorized)
        self.assertIs(live.worker, original_worker)
        self.assertEqual(live.username, f"{PAYOUT_ADDRESS}.original")
    def test_username_connection_limit_is_disabled_by_default(self) -> None:
        server = coordinator()
        first = client()
        second = client()
        server.clients.update({first, second})
        worker = WorkerIdentity(
            username=PAYOUT_ADDRESS,
            payout_address=PAYOUT_ADDRESS,
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )

        self.assertTrue(server.reserve_client_username(first, worker))
        self.assertTrue(server.reserve_client_username(second, worker))
        self.assertEqual(server.connection_limit_rejection_counts["username"], 0)
    def test_username_limit_does_not_count_idle_clients_for_empty_username(self) -> None:
        server = coordinator()
        server.stratum_max_connections_per_username = 1
        idle = client()
        first = client()
        second = client()
        server.clients.update({idle, first, second})
        worker = WorkerIdentity(
            username="",
            payout_address=PAYOUT_ADDRESS,
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )

        self.assertTrue(server.reserve_client_username(first, worker))
        self.assertFalse(server.reserve_client_username(second, worker))
        self.assertEqual(server.connection_limit_rejection_counts["username"], 1)
    def test_accept_loop_rejects_above_global_connection_limit(self) -> None:
        server = coordinator()
        server.stratum_max_connections = 1
        server.clients.add(client())

        class AcceptedSocket:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        accepted = AcceptedSocket()

        class OneConnectionListener:
            def __init__(self) -> None:
                self.calls = 0

            def accept(self) -> tuple[object, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    return accepted, ("127.0.0.1", 1000)
                server.stop_event.set()
                raise socket.timeout()

        profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=3340,
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )

        server.accept_loop(OneConnectionListener(), profile)  # type: ignore[arg-type]

        self.assertTrue(accepted.closed)
        self.assertEqual(server.connection_limit_rejection_counts["global"], 1)
    def test_accept_loop_recovers_from_descriptor_exhaustion(self) -> None:
        server = coordinator()
        server.stratum_accept_resource_exhaustion_backoff_seconds = 0

        class ExhaustedListener:
            def __init__(self) -> None:
                self.calls = 0

            def accept(self) -> tuple[object, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    raise OSError(errno.EMFILE, "too many open files")
                server.stop_event.set()
                raise socket.timeout()

        listener = ExhaustedListener()
        profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=3340,
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )

        server.accept_loop(listener, profile)  # type: ignore[arg-type]

        self.assertEqual(listener.calls, 2)
        self.assertEqual(server.accept_resource_exhaustion_count, 1)
    def test_resource_backoff_keeps_accept_watchdog_heartbeat_fresh(self) -> None:
        server = coordinator()
        server.stratum_accept_resource_exhaustion_backoff_seconds = 0.03
        server.watchdog_timeout_seconds = 0.01

        server._wait_after_stratum_resource_failure("stratum_accept")

        self.assertFalse(server._overdue_heartbeats(time.monotonic()))
    def test_accept_loop_recovers_when_handler_thread_cannot_start(self) -> None:
        server = coordinator()
        server.connection_counter = 0
        server.stratum_send_timeout_seconds = 0
        server.stratum_accept_resource_exhaustion_backoff_seconds = 0

        class AcceptedSocket:
            def __init__(self) -> None:
                self.closed = False

            def settimeout(self, timeout: object) -> None:
                pass

            def shutdown(self, how: int) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        accepted = AcceptedSocket()

        class OneConnectionListener:
            def __init__(self) -> None:
                self.calls = 0

            def accept(self) -> tuple[object, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    return accepted, ("127.0.0.1", 1000)
                server.stop_event.set()
                raise socket.timeout()

        listener = OneConnectionListener()
        profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=3340,
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )

        with patch.object(threading.Thread, "start", side_effect=RuntimeError("can't start new thread")):
            server.accept_loop(listener, profile)  # type: ignore[arg-type]

        self.assertEqual(listener.calls, 2)
        self.assertTrue(accepted.closed)
        self.assertFalse(server.clients)
        self.assertEqual(server.connection_setup_failure_count, 1)
    def test_handle_client_cleans_up_when_makefile_hits_descriptor_limit(self) -> None:
        server = coordinator()

        class MakefileFailureSocket:
            def __init__(self) -> None:
                self.closed = False

            def makefile(self, *args: object, **kwargs: object) -> object:
                raise OSError(errno.EMFILE, "too many open files")

            def shutdown(self, how: int) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        sock = MakefileFailureSocket()
        state = ClientState(
            sock=sock,  # type: ignore[arg-type]
            address=("127.0.0.1", 1000),
            connection_id=1,
            extranonce1_hex="00000001",
        )
        server.clients.add(state)

        server.handle_client(state)

        self.assertTrue(sock.closed)
        self.assertNotIn(state, server.clients)
        self.assertEqual(server.accept_resource_exhaustion_count, 1)
    def test_accept_loop_assigns_listener_profiles_and_unique_extranonce(self) -> None:
        server = coordinator()
        server.connection_counter = 0
        server.stratum_send_timeout_seconds = 0.0

        def listening_socket() -> socket.socket:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(0.1)
            return listener

        default_listener = listening_socket()
        highdiff_listener = listening_socket()
        default_profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=default_listener.getsockname()[1],
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )
        highdiff_profile = StratumListenerProfile(
            name="highdiff",
            bind="127.0.0.1",
            port=highdiff_listener.getsockname()[1],
            share_difficulty=Decimal("500000"),
            vardiff_config=highdiff_vardiff_config(),
            heartbeat_name="stratum_accept_highdiff",
            minimum_advertised_difficulty=Decimal("500000"),
        )
        threads = [
            threading.Thread(target=server.accept_loop, args=(default_listener, default_profile), daemon=True),
            threading.Thread(target=server.accept_loop, args=(highdiff_listener, highdiff_profile), daemon=True),
        ]
        for thread in threads:
            thread.start()
        connections = []
        try:
            connections.append(socket.create_connection(("127.0.0.1", default_profile.port), timeout=5))
            connections.append(socket.create_connection(("127.0.0.1", highdiff_profile.port), timeout=5))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with server.lock:
                    if len(server.clients) == 2:
                        break
                time.sleep(0.01)
            with server.lock:
                clients_by_listener = {c.listener_name: c for c in server.clients}
            self.assertEqual(set(clients_by_listener), {"default", "highdiff"})
            self.assertEqual(
                clients_by_listener["default"].share_difficulty,
                Decimal("0.000000001"),
            )
            self.assertEqual(
                clients_by_listener["highdiff"].share_difficulty,
                Decimal("500000"),
            )
            self.assertIs(
                clients_by_listener["highdiff"].listener_vardiff_config,
                highdiff_profile.vardiff_config,
            )
            self.assertEqual(
                clients_by_listener["highdiff"].minimum_advertised_difficulty,
                Decimal("500000"),
            )
            self.assertEqual(
                clients_by_listener["default"].minimum_advertised_difficulty,
                Decimal("0"),
            )
            extranonces = {c.extranonce1_hex for c in clients_by_listener.values()}
            self.assertEqual(extranonces, {"00000001", "00000002"})
            self.assertIn("stratum_accept", server._heartbeats)
            self.assertIn("stratum_accept_highdiff", server._heartbeats)
        finally:
            server.stop_event.set()
            for connection in connections:
                connection.close()
            default_listener.close()
            highdiff_listener.close()
            for thread in threads:
                thread.join(timeout=5)
