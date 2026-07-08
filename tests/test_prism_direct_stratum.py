#!/usr/bin/env python3

from __future__ import annotations

import unittest
from decimal import Decimal

from lab.auxpow import stratum_codec
from lab.prism import direct_stratum


EXTRANONCE1 = "11111111"
EXTRANONCE2_SIZE = 8
PLACEHOLDER = EXTRANONCE1 + ("00" * EXTRANONCE2_SIZE)


def tx_output(value_sats: int, script_hex: str) -> str:
    return value_sats.to_bytes(8, "little").hex() + direct_stratum.compact_size(len(bytes.fromhex(script_hex))).hex() + script_hex


def synthetic_coinbase_template(witness_commitment: str | None = None) -> str:
    script_sig = "5100" + PLACEHOLDER
    p2mr_script = "5220" + ("aa" * 32)
    witness_commitment = witness_commitment or "6a24aa21a9ed" + ("bb" * 32)
    txin = (
        "01"
        + ("00" * 32)
        + "ffffffff"
        + direct_stratum.compact_size(len(bytes.fromhex(script_sig))).hex()
        + script_sig
        + "feffffff"
    )
    txout = "02" + tx_output(5_000_000_000, p2mr_script) + tx_output(0, witness_commitment)
    witness = "01" + "20" + ("00" * 32)
    return "02000000" + "0001" + txin + txout + witness + "00000000"


def coinbase_with(
    scriptsig_suffix_hex: str,
    *,
    payout_outputs: list[tuple[int, str]] | None = None,
    witness_commitment_hex: str | None = None,
    witness_nonce_hex: str = "00" * 32,
) -> str:
    """Build a coinbase whose scriptSig is ``OP_1 OP_0`` (height) plus an arbitrary suffix.

    The suffix is where the extranonce placeholder lives. ``payout_outputs``,
    ``witness_commitment_hex`` and ``witness_nonce_hex`` let a test deliberately
    embed the same bytes elsewhere in the coinbase to exercise collisions.
    """
    script_sig = "5100" + scriptsig_suffix_hex
    witness_commitment_hex = witness_commitment_hex or ("6a24aa21a9ed" + ("bb" * 32))
    outputs = list(payout_outputs or [(5_000_000_000, "5220" + ("aa" * 32))])
    outputs.append((0, witness_commitment_hex))
    txin = (
        "01"
        + ("00" * 32)
        + "ffffffff"
        + direct_stratum.compact_size(len(bytes.fromhex(script_sig))).hex()
        + script_sig
        + "feffffff"
    )
    txout = direct_stratum.compact_size(len(outputs)).hex() + "".join(
        tx_output(value, script) for value, script in outputs
    )
    witness = (
        "01"
        + direct_stratum.compact_size(len(bytes.fromhex(witness_nonce_hex))).hex()
        + witness_nonce_hex
    )
    return "02000000" + "0001" + txin + txout + witness + "00000000"


def manifest_for(coinbase_tx_hex: str) -> dict[str, str]:
    return {
        "coinbase_tx_hex": coinbase_tx_hex,
        "coinbase_txid": direct_stratum.transaction_txid_display(coinbase_tx_hex),
    }


def synthetic_transaction(seed: str) -> str:
    script = seed * 3
    return (
        "01000000"
        + "01"
        + (seed * 32)
        + "00000000"
        + direct_stratum.compact_size(len(bytes.fromhex(script))).hex()
        + script
        + "ffffffff"
        + "01"
        + tx_output(1, "51")
        + "00000000"
    )


def synthetic_witness_transaction(seed: str) -> str:
    script = seed * 3
    witness_item = seed * 5
    return (
        "01000000"
        + "0001"
        + "01"
        + (seed * 32)
        + "00000000"
        + direct_stratum.compact_size(len(bytes.fromhex(script))).hex()
        + script
        + "ffffffff"
        + "01"
        + tx_output(1, "51")
        + "01"
        + direct_stratum.compact_size(len(bytes.fromhex(witness_item))).hex()
        + witness_item
        + "00000000"
    )


def merkle_root(hashes: list[bytes]) -> bytes:
    while len(hashes) > 1:
        hashes = [
            stratum_codec.double_sha256(
                hashes[index] + (hashes[index + 1] if index + 1 < len(hashes) else hashes[index])
            )
            for index in range(0, len(hashes), 2)
        ]
    return hashes[0]


def witness_commitment_script(transaction_hexes: list[str]) -> str:
    root = merkle_root([bytes(32)] + [direct_stratum.transaction_wtxid_internal(tx_hex) for tx_hex in transaction_hexes])
    commitment = stratum_codec.double_sha256(root + bytes(32))
    return "6a24aa21a9ed" + commitment.hex()


def transaction_end(data: bytes, offset: int) -> int:
    offset += 4
    has_witness = data[offset] == 0 and data[offset + 1] != 0
    if has_witness:
        offset += 2
    input_count, offset = direct_stratum.read_compact_size(data, offset, field_name="transaction inputs")
    for input_index in range(input_count):
        offset = direct_stratum.skip_bytes(data, offset, 36, field_name=f"transaction input[{input_index}] prevout")
        script_len, offset = direct_stratum.read_compact_size(
            data,
            offset,
            field_name=f"transaction input[{input_index}] script",
        )
        offset = direct_stratum.skip_bytes(data, offset, script_len, field_name=f"transaction input[{input_index}] script")
        offset = direct_stratum.skip_bytes(data, offset, 4, field_name=f"transaction input[{input_index}] sequence")
    output_count, offset = direct_stratum.read_compact_size(data, offset, field_name="transaction outputs")
    for output_index in range(output_count):
        offset = direct_stratum.skip_bytes(data, offset, 8, field_name=f"transaction output[{output_index}] value")
        script_len, offset = direct_stratum.read_compact_size(
            data,
            offset,
            field_name=f"transaction output[{output_index}] script",
        )
        offset = direct_stratum.skip_bytes(data, offset, script_len, field_name=f"transaction output[{output_index}] script")
    if has_witness:
        for input_index in range(input_count):
            item_count, offset = direct_stratum.read_compact_size(data, offset, field_name=f"transaction witness[{input_index}]")
            for item_index in range(item_count):
                item_len, offset = direct_stratum.read_compact_size(
                    data,
                    offset,
                    field_name=f"transaction witness[{input_index}][{item_index}]",
                )
                offset = direct_stratum.skip_bytes(
                    data,
                    offset,
                    item_len,
                    field_name=f"transaction witness[{input_index}][{item_index}]",
                )
    return direct_stratum.skip_bytes(data, offset, 4, field_name="transaction locktime")


def block_transaction_hexes(block_hex: str) -> list[str]:
    data = bytes.fromhex(block_hex)
    offset = 80
    tx_count, offset = direct_stratum.read_compact_size(data, offset, field_name="block transaction count")
    result = []
    for _ in range(tx_count):
        start = offset
        offset = transaction_end(data, offset)
        result.append(data[start:offset].hex())
    if offset != len(data):
        raise ValueError("block has trailing bytes")
    return result


def template(transactions: list[str] | None = None) -> dict[str, object]:
    return {
        "previousblockhash": "00" * 32,
        "version": 0x20000000,
        "bits": "207fffff",
        "curtime": 1_700_000_000,
        "transactions": [{"data": tx_hex} for tx_hex in transactions or []],
    }


class PrismDirectStratumTests(unittest.TestCase):
    def test_builder_coinbase_template_splits_without_witness_and_reassembles_submitblock(self) -> None:
        coinbase_tx_hex = synthetic_coinbase_template()
        manifest = {
            "coinbase_tx_hex": coinbase_tx_hex,
            "coinbase_txid": direct_stratum.transaction_txid_display(coinbase_tx_hex),
        }
        job = direct_stratum.make_job_from_builder_manifest(
            job_id="job-1",
            template=template(),
            manifest=manifest,
            extranonce1_hex=EXTRANONCE1,
            extranonce2_size=EXTRANONCE2_SIZE,
            desired_share_difficulty=Decimal("1"),
        )

        submission = direct_stratum.assemble_submission(
            job,
            extranonce2_hex="cafebabe00000001",
            ntime_hex=job.ntime,
            nonce_hex="0000002a",
            version_bits_hex="00002000",
            version_mask=direct_stratum.QBIT_VERSION_ROLLING_MASK,
        )

        self.assertEqual(submission.applied_version_hex, "20002000")
        self.assertEqual(
            submission.coinbase_tx_hex,
            coinbase_tx_hex.replace(PLACEHOLDER, EXTRANONCE1 + "cafebabe00000001", 1),
        )
        self.assertEqual(
            direct_stratum.strip_witness_transaction(submission.coinbase_tx_hex).hex(),
            submission.coinbase_txid_preimage_hex,
        )
        self.assertEqual(submission.block_hex, submission.header_hex + "01" + submission.coinbase_tx_hex)
        self.assertEqual(len(bytes.fromhex(submission.header_hex)), 80)

    def test_merkle_branch_uses_transaction_txids_excluding_witness(self) -> None:
        extra_tx = synthetic_transaction("22")
        manifest = {
            "coinbase_tx_hex": synthetic_coinbase_template(),
            "coinbase_txid": direct_stratum.transaction_txid_display(synthetic_coinbase_template()),
        }
        job = direct_stratum.make_job_from_builder_manifest(
            job_id="job-branch",
            template=template([extra_tx]),
            manifest=manifest,
            extranonce1_hex=EXTRANONCE1,
            extranonce2_size=EXTRANONCE2_SIZE,
            desired_share_difficulty=Decimal("1"),
        )

        self.assertEqual(job.merkle_branch, (direct_stratum.transaction_txid_internal(extra_tx).hex(),))

        submission = direct_stratum.assemble_submission(
            job,
            extranonce2_hex="0102030405060708",
            ntime_hex=job.ntime,
            nonce_hex="00000000",
        )
        coinbase_txid = stratum_codec.double_sha256(bytes.fromhex(submission.coinbase_txid_preimage_hex))
        expected_merkle_root = stratum_codec.double_sha256(coinbase_txid + direct_stratum.transaction_txid_internal(extra_tx))
        expected_header = stratum_codec.serialize_header_from_stratum_fields(
            version_hex=job.version,
            prevhash_hex=job.prevhash,
            merkle_root_serialized=expected_merkle_root,
            ntime_hex=job.ntime,
            nbits_hex=job.nbits,
            nonce_hex="00000000",
        )

        self.assertEqual(submission.header_hex, expected_header.hex())
        self.assertTrue(submission.block_hex.endswith(submission.coinbase_tx_hex + extra_tx))

    def test_witness_commitment_uses_template_wtxids_for_serialized_block(self) -> None:
        witness_tx = synthetic_witness_transaction("44")
        self.assertNotEqual(
            direct_stratum.transaction_txid_internal(witness_tx),
            direct_stratum.transaction_wtxid_internal(witness_tx),
        )
        commitment_script = witness_commitment_script([witness_tx])
        coinbase_tx_hex = synthetic_coinbase_template(commitment_script)
        manifest = {
            "coinbase_tx_hex": coinbase_tx_hex,
            "coinbase_txid": direct_stratum.transaction_txid_display(coinbase_tx_hex),
        }
        job = direct_stratum.make_job_from_builder_manifest(
            job_id="job-witness",
            template=template([witness_tx]),
            manifest=manifest,
            extranonce1_hex=EXTRANONCE1,
            extranonce2_size=EXTRANONCE2_SIZE,
            desired_share_difficulty=Decimal("1"),
        )

        self.assertEqual(job.merkle_branch, (direct_stratum.transaction_txid_internal(witness_tx).hex(),))
        self.assertEqual(
            direct_stratum.witness_merkle_leaves_hex(job.transaction_hexes),
            [direct_stratum.transaction_wtxid_internal(witness_tx).hex()],
        )

        submission = direct_stratum.assemble_submission(
            job,
            extranonce2_hex="0102030405060708",
            ntime_hex=job.ntime,
            nonce_hex="00000000",
        )
        block_txs = block_transaction_hexes(submission.block_hex)
        self.assertEqual(block_txs, [submission.coinbase_tx_hex, witness_tx])
        self.assertIn(witness_commitment_script(block_txs[1:]), block_txs[0])

    def test_rejects_mismatched_txid_and_placeholder_not_in_scriptsig(self) -> None:
        coinbase_tx_hex = synthetic_coinbase_template()
        manifest = {
            "coinbase_tx_hex": coinbase_tx_hex,
            "coinbase_txid": "00" * 32,
        }
        with self.assertRaisesRegex(ValueError, "coinbase_txid"):
            direct_stratum.make_job_from_builder_manifest(
                job_id="bad-txid",
                template=template(),
                manifest=manifest,
                extranonce1_hex=EXTRANONCE1,
                extranonce2_size=EXTRANONCE2_SIZE,
                desired_share_difficulty=Decimal("1"),
            )

        # A coinbase whose scriptSig does not carry the placeholder at its tail is
        # rejected outright rather than silently mis-split.
        wrong_suffix = coinbase_with("ab" * (len(bytes.fromhex(PLACEHOLDER))))
        with self.assertRaisesRegex(ValueError, "does not end its coinbase scriptSig"):
            direct_stratum.split_coinbase_extranonce(
                wrong_suffix, PLACEHOLDER, field_name="coinbase txid preimage"
            )
        # A scriptSig shorter than the placeholder cannot contain it.
        with self.assertRaisesRegex(ValueError, "shorter than the extranonce placeholder"):
            direct_stratum.split_coinbase_extranonce(
                coinbase_with(""), PLACEHOLDER, field_name="coinbase txid preimage"
            )
        # Truncated / non-coinbase hex is reported, never an unhandled crash.
        with self.assertRaisesRegex(ValueError, "too short to be a coinbase transaction"):
            direct_stratum.split_coinbase_extranonce("aa", PLACEHOLDER, field_name="coinbase txid preimage")

    def test_split_is_robust_when_placeholder_bytes_appear_in_outputs_and_witness(self) -> None:
        # The extranonce placeholder bytes are deliberately echoed in a payout
        # output script, in the witness commitment, and in the witness nonce. The
        # old substring split raised "extranonce placeholder more than once" here
        # and tore down the miner's client thread.
        placeholder_bytes = bytes.fromhex(PLACEHOLDER)
        collision_output_script = (
            "6a" + direct_stratum.compact_size(len(placeholder_bytes)).hex() + PLACEHOLDER
        )
        witness_commitment = "6a24aa21a9ed" + PLACEHOLDER + ("bb" * (32 - len(placeholder_bytes)))
        witness_nonce = PLACEHOLDER + ("00" * (32 - len(placeholder_bytes)))
        extranonce2_hex = "cafebabe00000001"

        coinbase_tx_hex = coinbase_with(
            PLACEHOLDER,
            payout_outputs=[(5_000_000_000, "5220" + ("aa" * 32)), (7, collision_output_script)],
            witness_commitment_hex=witness_commitment,
            witness_nonce_hex=witness_nonce,
        )
        # Sanity: the placeholder really does appear several times in the coinbase.
        self.assertGreaterEqual(coinbase_tx_hex.count(PLACEHOLDER), 4)

        job = direct_stratum.make_job_from_builder_manifest(
            job_id="job-collision",
            template=template(),
            manifest=manifest_for(coinbase_tx_hex),
            extranonce1_hex=EXTRANONCE1,
            extranonce2_size=EXTRANONCE2_SIZE,
            desired_share_difficulty=Decimal("1"),
        )
        submission = direct_stratum.assemble_submission(
            job,
            extranonce2_hex=extranonce2_hex,
            ntime_hex=job.ntime,
            nonce_hex="00000000",
        )

        # Only the scriptSig occurrence is rewritten; the colliding copies in the
        # outputs, witness commitment, and witness nonce are preserved byte-for-byte.
        expected = coinbase_with(
            EXTRANONCE1 + extranonce2_hex,
            payout_outputs=[(5_000_000_000, "5220" + ("aa" * 32)), (7, collision_output_script)],
            witness_commitment_hex=witness_commitment,
            witness_nonce_hex=witness_nonce,
        )
        self.assertEqual(submission.coinbase_tx_hex, expected)
        self.assertEqual(submission.coinbase_tx_hex.count(PLACEHOLDER), 3)
        self.assertEqual(
            direct_stratum.strip_witness_transaction(submission.coinbase_tx_hex).hex(),
            submission.coinbase_txid_preimage_hex,
        )

    def test_split_anchors_on_scriptsig_when_placeholder_collides_with_null_outpoint(self) -> None:
        # A low connection id yields extranonce1="00000000", so the placeholder is
        # all zero bytes -- which is contained in the 32-byte all-zero null outpoint
        # that appears *before* the scriptSig. A naive first-occurrence search would
        # split inside the outpoint and corrupt the coinbase (and the duplicate check
        # would raise). Structural parsing anchors correctly on the scriptSig.
        extranonce1 = "00000000"
        placeholder = extranonce1 + ("00" * EXTRANONCE2_SIZE)
        extranonce2_hex = "0123456789abcdef"
        coinbase_tx_hex = coinbase_with(placeholder)

        # The naive substring search would land inside the null outpoint, well
        # before the real scriptSig insertion point.
        scriptsig_offset = coinbase_tx_hex.index("5100" + placeholder) + len("5100")
        self.assertLess(coinbase_tx_hex.find(placeholder) * 1, scriptsig_offset)
        self.assertGreater(coinbase_tx_hex.count(placeholder), 1)

        job = direct_stratum.make_job_from_builder_manifest(
            job_id="job-zero-extranonce",
            template=template(),
            manifest=manifest_for(coinbase_tx_hex),
            extranonce1_hex=extranonce1,
            extranonce2_size=EXTRANONCE2_SIZE,
            desired_share_difficulty=Decimal("1"),
        )
        submission = direct_stratum.assemble_submission(
            job,
            extranonce2_hex=extranonce2_hex,
            ntime_hex=job.ntime,
            nonce_hex="00000000",
        )

        self.assertEqual(submission.coinbase_tx_hex, coinbase_with(extranonce1 + extranonce2_hex))
        # The null outpoint ahead of the scriptSig is untouched.
        self.assertTrue(
            submission.coinbase_tx_hex.startswith("02000000" + "0001" + "01" + ("00" * 32) + "ffffffff")
        )
        self.assertEqual(
            direct_stratum.strip_witness_transaction(submission.coinbase_tx_hex).hex(),
            submission.coinbase_txid_preimage_hex,
        )

    def test_version_bits_are_limited_to_negotiated_gbt_mask(self) -> None:
        coinbase_tx_hex = synthetic_coinbase_template()
        manifest = {
            "coinbase_tx_hex": coinbase_tx_hex,
            "coinbase_txid": direct_stratum.transaction_txid_display(coinbase_tx_hex),
        }
        job = direct_stratum.make_job_from_builder_manifest(
            job_id="job-mask",
            template=template(),
            manifest=manifest,
            extranonce1_hex=EXTRANONCE1,
            extranonce2_size=EXTRANONCE2_SIZE,
            desired_share_difficulty=Decimal("1"),
        )

        submission = direct_stratum.assemble_submission(
            job,
            extranonce2_hex="0000000000000001",
            ntime_hex=job.ntime,
            nonce_hex="00000000",
            version_bits_hex="00002000",
            version_mask=direct_stratum.QBIT_VERSION_ROLLING_MASK,
        )

        self.assertEqual(submission.applied_version_hex, "20002000")

        with self.assertRaisesRegex(ValueError, "outside the negotiated mask"):
            direct_stratum.assemble_submission(
                job,
                extranonce2_hex="0000000000000001",
                ntime_hex=job.ntime,
                nonce_hex="00000000",
                version_bits_hex="000000ff",
                version_mask=direct_stratum.QBIT_VERSION_ROLLING_MASK,
            )

    def test_select_version_rolling_mask_uses_gbt_field_or_fallback(self) -> None:
        advertised = direct_stratum.select_version_rolling_mask(
            {"versionrollingmask": "1fffe000"},
            0x000000FF,
        )
        missing = direct_stratum.select_version_rolling_mask({}, direct_stratum.QBIT_VERSION_ROLLING_MASK)
        disabled = direct_stratum.select_version_rolling_mask(
            {"versionrollingmask": "00000000"},
            direct_stratum.QBIT_VERSION_ROLLING_MASK,
        )

        self.assertEqual(advertised.selected_mask, 0x1FFFE000)
        self.assertEqual(advertised.source, "qbit_getblocktemplate")
        self.assertEqual(missing.selected_mask, direct_stratum.QBIT_VERSION_ROLLING_MASK)
        self.assertEqual(missing.source, "fallback")
        self.assertEqual(disabled.selected_mask, 0)
        self.assertEqual(disabled.detail, "disabled_by_zero_mask")

        with self.assertRaisesRegex(ValueError, "invalid getblocktemplate.versionrollingmask"):
            direct_stratum.select_version_rolling_mask(
                {"versionrollingmask": "not-hex"},
                direct_stratum.QBIT_VERSION_ROLLING_MASK,
            )


class EffectiveShareTargetTests(unittest.TestCase):
    def test_network_cap_governs_without_minimum(self) -> None:
        # bits 207fffff: network difficulty ~4.7e-10, so a 500k desired
        # difficulty is capped down to the (easier) network target.
        qbit_target = direct_stratum.target_from_compact_hex("207fffff")

        share_target = direct_stratum.effective_share_target(Decimal("500000"), qbit_target)

        self.assertEqual(share_target, qbit_target)
        self.assertLess(direct_stratum.target_difficulty(share_target), Decimal("1"))

    def test_minimum_advertised_difficulty_overrides_network_cap(self) -> None:
        qbit_target = direct_stratum.target_from_compact_hex("207fffff")

        share_target = direct_stratum.effective_share_target(
            Decimal("500000"),
            qbit_target,
            minimum_advertised_difficulty=Decimal("500000"),
        )

        self.assertEqual(share_target, direct_stratum.difficulty_target(Decimal("500000")))
        # Compare at wire precision: set_difficulty sends float(difficulty),
        # and Decimal round-tripping can sit within 1e-27 of the floor.
        self.assertGreaterEqual(
            float(direct_stratum.target_difficulty(share_target)),
            500000.0,
        )

    def test_minimum_is_inert_when_desired_already_meets_it(self) -> None:
        # Mature chain: network target harder than the floor target, desired
        # difficulty above the floor. The floor changes nothing.
        qbit_target = direct_stratum.difficulty_target(Decimal("80000000"))

        share_target = direct_stratum.effective_share_target(
            Decimal("600000"),
            qbit_target,
            minimum_advertised_difficulty=Decimal("500000"),
        )

        self.assertEqual(share_target, direct_stratum.difficulty_target(Decimal("600000")))


if __name__ == "__main__":
    unittest.main()
