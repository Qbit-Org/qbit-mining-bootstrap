#!/usr/bin/env python3
"""Transport-independent direct qbit Stratum helpers for PRISM pool jobs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any

from lab.auxpow import stratum_codec

getcontext().prec = 40

DIFF1_TARGET = int("00000000ffff0000000000000000000000000000000000000000000000000000", 16)
HEX_MASK_RE = re.compile(r"^(?:0x)?[0-9a-fA-F]{1,8}$")
QBIT_VERSION_ROLLING_MASK = 0x1FFFE000
QBIT_VERSION_ROLLING_MASK_HEX = f"{QBIT_VERSION_ROLLING_MASK:08x}"


@dataclass(frozen=True)
class VersionRollingMaskSelection:
    selected_mask: int
    source: str
    detail: str
    advertised_mask: str | None = None


@dataclass(frozen=True)
class DirectQbitStratumJob:
    job_id: str
    previousblockhash_display: str
    prevhash: str
    coinb1: str
    coinb2: str
    full_coinbase_prefix: str
    full_coinbase_suffix: str
    merkle_branch: tuple[str, ...]
    transaction_hexes: tuple[str, ...]
    version: str
    nbits: str
    ntime: str
    qbit_target: int
    share_target: int
    share_difficulty: Decimal
    extranonce1_hex: str
    extranonce2_size: int
    clean_jobs: bool = True


@dataclass(frozen=True)
class DirectQbitSubmission:
    coinbase_tx_hex: str
    coinbase_txid_preimage_hex: str
    header_hex: str
    block_hex: str
    block_hash_hex: str
    block_hash_int: int
    share_pass: bool
    block_pass: bool
    applied_version_hex: str


def normalize_version_rolling_mask(value: Any, *, field_name: str) -> int:
    if isinstance(value, int):
        if value < 0 or value > 0xFFFFFFFF:
            raise ValueError(f"{field_name} must fit uint32")
        return value

    text = str(value).strip()
    if not HEX_MASK_RE.fullmatch(text):
        raise ValueError(f"{field_name} must be 1 to 8 hex chars")
    if text.lower().startswith("0x"):
        text = text[2:]
    return int(text, 16) & 0xFFFFFFFF


def select_version_rolling_mask(template: dict[str, Any], fallback_mask: int) -> VersionRollingMaskSelection:
    if fallback_mask < 0 or fallback_mask > 0xFFFFFFFF:
        raise ValueError("fallback version rolling mask must fit uint32")
    if "versionrollingmask" not in template:
        return VersionRollingMaskSelection(fallback_mask, "fallback", "missing_versionrollingmask")

    advertised = template.get("versionrollingmask")
    try:
        selected = normalize_version_rolling_mask(advertised, field_name="versionrollingmask")
    except ValueError as exc:
        raise ValueError(f"invalid getblocktemplate.versionrollingmask: {exc}") from exc

    if selected == 0:
        return VersionRollingMaskSelection(selected, "qbit_getblocktemplate", "disabled_by_zero_mask", str(advertised))
    return VersionRollingMaskSelection(selected, "qbit_getblocktemplate", "advertised", str(advertised))


def compact_size(value: int) -> bytes:
    if value < 0:
        raise ValueError("compact size cannot be negative")
    if value < 253:
        return bytes([value])
    if value <= 0xFFFF:
        return b"\xfd" + value.to_bytes(2, "little")
    if value <= 0xFFFF_FFFF:
        return b"\xfe" + value.to_bytes(4, "little")
    if value <= 0xFFFF_FFFF_FFFF_FFFF:
        return b"\xff" + value.to_bytes(8, "little")
    raise ValueError("compact size is too large")


def read_compact_size(data: bytes, offset: int, *, field_name: str) -> tuple[int, int]:
    if offset >= len(data):
        raise ValueError(f"{field_name} truncated before compact size")
    first = data[offset]
    offset += 1
    if first < 253:
        return first, offset
    if first == 253:
        width = 2
    elif first == 254:
        width = 4
    else:
        width = 8
    if offset + width > len(data):
        raise ValueError(f"{field_name} truncated inside compact size")
    return int.from_bytes(data[offset : offset + width], "little"), offset + width


def skip_bytes(data: bytes, offset: int, count: int, *, field_name: str) -> int:
    if count < 0:
        raise ValueError(f"{field_name} cannot skip a negative byte count")
    if offset + count > len(data):
        raise ValueError(f"{field_name} truncated")
    return offset + count


def strip_witness_transaction(tx_hex: str) -> bytes:
    tx = bytes.fromhex(stratum_codec.validate_hex(tx_hex, field_name="transaction"))
    if len(tx) < 10:
        raise ValueError("transaction is too short")

    version = tx[:4]
    offset = 4
    has_witness = tx[offset] == 0 and tx[offset + 1] != 0
    if has_witness:
        offset += 2
    body_start = offset

    input_count, offset = read_compact_size(tx, offset, field_name="transaction inputs")
    for input_index in range(input_count):
        offset = skip_bytes(tx, offset, 36, field_name=f"transaction input[{input_index}] prevout")
        script_len, offset = read_compact_size(tx, offset, field_name=f"transaction input[{input_index}] script")
        offset = skip_bytes(tx, offset, script_len, field_name=f"transaction input[{input_index}] script")
        offset = skip_bytes(tx, offset, 4, field_name=f"transaction input[{input_index}] sequence")

    output_count, offset = read_compact_size(tx, offset, field_name="transaction outputs")
    for output_index in range(output_count):
        offset = skip_bytes(tx, offset, 8, field_name=f"transaction output[{output_index}] value")
        script_len, offset = read_compact_size(tx, offset, field_name=f"transaction output[{output_index}] script")
        offset = skip_bytes(tx, offset, script_len, field_name=f"transaction output[{output_index}] script")

    witness_start = offset
    if has_witness:
        for input_index in range(input_count):
            item_count, offset = read_compact_size(tx, offset, field_name=f"transaction witness[{input_index}]")
            for item_index in range(item_count):
                item_len, offset = read_compact_size(
                    tx,
                    offset,
                    field_name=f"transaction witness[{input_index}][{item_index}]",
                )
                offset = skip_bytes(
                    tx,
                    offset,
                    item_len,
                    field_name=f"transaction witness[{input_index}][{item_index}]",
                )

    locktime = tx[offset : offset + 4]
    if len(locktime) != 4:
        raise ValueError("transaction truncated before locktime")
    offset += 4
    if offset != len(tx):
        raise ValueError("transaction has trailing bytes after locktime")

    if not has_witness:
        return tx
    return version + tx[body_start:witness_start] + locktime


def transaction_txid_internal(tx_hex: str) -> bytes:
    return stratum_codec.double_sha256(strip_witness_transaction(tx_hex))


def transaction_wtxid_internal(tx_hex: str) -> bytes:
    tx = bytes.fromhex(stratum_codec.validate_hex(tx_hex, field_name="transaction"))
    return stratum_codec.double_sha256(tx)


def witness_merkle_leaves_hex(transaction_hexes: tuple[str, ...]) -> list[str]:
    return [transaction_wtxid_internal(tx_hex).hex() for tx_hex in transaction_hexes]


def transaction_txid_display(tx_hex: str) -> str:
    return transaction_txid_internal(tx_hex)[::-1].hex()


def target_from_compact_hex(nbits_hex: str) -> int:
    nbits_hex = stratum_codec.validate_uint32_hex(nbits_hex, field_name="nbits")
    compact = int(nbits_hex, 16)
    size = compact >> 24
    mantissa = compact & 0x007FFFFF
    if size <= 3:
        return mantissa >> (8 * (3 - size))
    return mantissa << (8 * (size - 3))


def difficulty_target(difficulty: Decimal | int | str) -> int:
    parsed = parse_positive_decimal(difficulty, field_name="difficulty")
    target = int(Decimal(DIFF1_TARGET) / parsed)
    return max(1, min(target, (1 << 256) - 1))


def target_difficulty(target: int) -> Decimal:
    if target <= 0:
        raise ValueError("target must be positive")
    return Decimal(DIFF1_TARGET) / Decimal(target)


def effective_share_target(
    desired_share_difficulty: Decimal | int | str,
    qbit_target: int,
    *,
    minimum_advertised_difficulty: Decimal | int | str = Decimal("0"),
) -> int:
    share_target = max(difficulty_target(desired_share_difficulty), qbit_target)
    minimum = parse_nonnegative_decimal(minimum_advertised_difficulty, field_name="minimum_advertised_difficulty")
    if minimum > 0:
        share_target = min(share_target, difficulty_target(minimum))
    return share_target


def parse_positive_decimal(value: Decimal | int | str, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{field_name} must be a decimal") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def parse_nonnegative_decimal(value: Decimal | int | str, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{field_name} must be a decimal") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return parsed


def coinbase_scriptsig_span(tx: bytes, *, field_name: str) -> tuple[int, int]:
    """Return the (start, length) byte span of the coinbase input's scriptSig.

    The transaction is parsed structurally (version, optional SegWit marker/flag,
    then the coinbase input's outpoint and scriptSig length prefix) so the
    extranonce can be located by byte offset instead of by substring search. This
    is collision proof: bytes that merely *look* like the extranonce placeholder
    elsewhere in the coinbase (a payout output script, the witness commitment, the
    witness nonce, the all-zero null outpoint, or the scriptSig height push) are
    never mistaken for the extranonce insertion point.
    """
    if len(tx) < 10:
        raise ValueError(f"{field_name} is too short to be a coinbase transaction")
    offset = 4  # version
    if tx[offset] == 0 and tx[offset + 1] != 0:  # SegWit marker + flag
        offset += 2
    input_count, offset = read_compact_size(tx, offset, field_name=f"{field_name} inputs")
    if input_count < 1:
        raise ValueError(f"{field_name} has no coinbase input")
    offset = skip_bytes(tx, offset, 36, field_name=f"{field_name} coinbase outpoint")
    script_len, offset = read_compact_size(tx, offset, field_name=f"{field_name} coinbase scriptSig")
    script_start = offset
    # Validate the scriptSig is fully present without advancing past it.
    skip_bytes(tx, offset, script_len, field_name=f"{field_name} coinbase scriptSig")
    return script_start, script_len


def split_coinbase_extranonce(tx_hex: str, placeholder_hex: str, *, field_name: str) -> tuple[str, str]:
    """Split a coinbase around the extranonce placeholder at its known offset.

    The pool builder appends the extranonce placeholder (extranonce1 followed by
    ``extranonce2_size`` zero bytes) to the end of the coinbase input's scriptSig.
    We locate that scriptSig by parsing the transaction and split immediately
    before the placeholder, returning the hex prefix/suffix around it. Unlike a
    raw ``tx_hex.find(placeholder)`` this never trips over a coincidental copy of
    the placeholder bytes elsewhere in the coinbase, so a single unlucky template
    cannot corrupt the split or crash the caller.
    """
    tx_hex = stratum_codec.validate_hex(tx_hex, field_name=field_name)
    placeholder_hex = stratum_codec.validate_hex(placeholder_hex, field_name="extranonce placeholder")
    if not placeholder_hex:
        raise ValueError("extranonce placeholder cannot be empty")
    tx = bytes.fromhex(tx_hex)
    placeholder = bytes.fromhex(placeholder_hex)
    script_start, script_len = coinbase_scriptsig_span(tx, field_name=field_name)
    if len(placeholder) > script_len:
        raise ValueError(f"{field_name} coinbase scriptSig is shorter than the extranonce placeholder")
    script_end = script_start + script_len
    placeholder_start = script_end - len(placeholder)
    if tx[placeholder_start:script_end] != placeholder:
        raise ValueError(f"{field_name} does not end its coinbase scriptSig with the extranonce placeholder")
    return tx[:placeholder_start].hex(), tx[script_end:].hex()


def transaction_hexes_from_template(template: dict[str, Any]) -> tuple[str, ...]:
    txs = template.get("transactions", [])
    if not isinstance(txs, list):
        raise ValueError("template transactions must be an array")
    result = []
    for index, tx in enumerate(txs):
        if not isinstance(tx, dict) or "data" not in tx:
            raise ValueError(f"template transaction[{index}] must include data")
        result.append(stratum_codec.validate_hex(str(tx["data"]), field_name=f"template transaction[{index}] data"))
    return tuple(result)


def merkle_branch_for_coinbase(transaction_hexes: tuple[str, ...]) -> tuple[str, ...]:
    hashes = [b"\x00" * 32]
    hashes.extend(transaction_txid_internal(tx_hex) for tx_hex in transaction_hexes)
    index = 0
    branch: list[str] = []
    while len(hashes) > 1:
        sibling_index = index ^ 1
        if sibling_index >= len(hashes):
            sibling_index = index
        branch.append(hashes[sibling_index].hex())

        next_hashes = []
        for offset in range(0, len(hashes), 2):
            left = hashes[offset]
            right = hashes[offset + 1] if offset + 1 < len(hashes) else hashes[offset]
            next_hashes.append(stratum_codec.double_sha256(left + right))
        hashes = next_hashes
        index //= 2
    return tuple(branch)


def make_job_from_builder_manifest(
    *,
    job_id: str,
    template: dict[str, Any],
    manifest: dict[str, Any],
    extranonce1_hex: str,
    extranonce2_size: int,
    desired_share_difficulty: Decimal | int | str,
    minimum_advertised_difficulty: Decimal | int | str = Decimal("0"),
    clean_jobs: bool = True,
    transaction_hexes: tuple[str, ...] | None = None,
) -> DirectQbitStratumJob:
    if extranonce2_size <= 0:
        raise ValueError("extranonce2_size must be positive")
    extranonce1_hex = stratum_codec.validate_hex(extranonce1_hex, field_name="extranonce1")
    coinbase_tx_hex = stratum_codec.validate_hex(str(manifest["coinbase_tx_hex"]), field_name="coinbase_tx_hex")
    expected_txid = manifest.get("coinbase_txid")
    actual_txid = transaction_txid_display(coinbase_tx_hex)
    if expected_txid is not None and str(expected_txid).lower() != actual_txid:
        raise ValueError("manifest coinbase_txid does not match coinbase_tx_hex without witness")

    placeholder_hex = extranonce1_hex + ("00" * extranonce2_size)
    no_witness_coinbase_hex = strip_witness_transaction(coinbase_tx_hex).hex()
    coinb1, coinb2 = split_coinbase_extranonce(
        no_witness_coinbase_hex,
        placeholder_hex,
        field_name="coinbase txid preimage",
    )
    full_prefix, full_suffix = split_coinbase_extranonce(
        coinbase_tx_hex,
        placeholder_hex,
        field_name="full coinbase transaction",
    )

    if transaction_hexes is None:
        transaction_hexes = transaction_hexes_from_template(template)
    else:
        transaction_hexes = tuple(
            stratum_codec.validate_hex(tx_hex, field_name=f"transaction_hexes[{index}]")
            for index, tx_hex in enumerate(transaction_hexes)
        )

    nbits = stratum_codec.validate_uint32_hex(str(template["bits"]), field_name="template bits")
    qbit_target = target_from_compact_hex(nbits)
    share_target = effective_share_target(
        desired_share_difficulty,
        qbit_target,
        minimum_advertised_difficulty=minimum_advertised_difficulty,
    )
    version = f"{int(template['version']) & 0xFFFFFFFF:08x}"
    ntime = f"{int(template['curtime']) & 0xFFFFFFFF:08x}"
    previousblockhash = stratum_codec.validate_hash_hex(
        str(template["previousblockhash"]),
        field_name="template previousblockhash",
    )

    return DirectQbitStratumJob(
        job_id=str(job_id),
        previousblockhash_display=previousblockhash,
        prevhash=stratum_codec.stratum_prevhash_from_display_hash(previousblockhash),
        coinb1=coinb1,
        coinb2=coinb2,
        full_coinbase_prefix=full_prefix,
        full_coinbase_suffix=full_suffix,
        merkle_branch=merkle_branch_for_coinbase(transaction_hexes),
        transaction_hexes=transaction_hexes,
        version=version,
        nbits=nbits,
        ntime=ntime,
        qbit_target=qbit_target,
        share_target=share_target,
        share_difficulty=target_difficulty(share_target),
        extranonce1_hex=extranonce1_hex,
        extranonce2_size=extranonce2_size,
        clean_jobs=clean_jobs,
    )


def assemble_submission(
    job: DirectQbitStratumJob,
    *,
    extranonce2_hex: str,
    ntime_hex: str,
    nonce_hex: str,
    version_bits_hex: str | None = None,
    version_mask: int = 0,
) -> DirectQbitSubmission:
    extranonce2_hex = stratum_codec.validate_hex(extranonce2_hex, field_name="extranonce2")
    if len(extranonce2_hex) != job.extranonce2_size * 2:
        raise ValueError("unexpected extranonce2 size")
    version_hex = stratum_codec.apply_version_bits(job.version, version_bits_hex, version_mask)
    coinbase_without_witness, header = stratum_codec.assemble_header_from_notify_submit(
        coinb1_hex=job.coinb1,
        extranonce1_hex=job.extranonce1_hex,
        extranonce2_hex=extranonce2_hex,
        coinb2_hex=job.coinb2,
        merkle_branch_hex=list(job.merkle_branch),
        version_hex=version_hex,
        prevhash_hex=job.prevhash,
        ntime_hex=ntime_hex,
        nbits_hex=job.nbits,
        nonce_hex=nonce_hex,
    )
    full_coinbase_hex = job.full_coinbase_prefix + job.extranonce1_hex + extranonce2_hex + job.full_coinbase_suffix
    if strip_witness_transaction(full_coinbase_hex) != coinbase_without_witness:
        raise ValueError("full coinbase witness transaction does not match Stratum txid preimage")
    header_hash_int = stratum_codec.header_hash_int(header)
    return DirectQbitSubmission(
        coinbase_tx_hex=full_coinbase_hex,
        coinbase_txid_preimage_hex=coinbase_without_witness.hex(),
        header_hex=header.hex(),
        block_hex=serialize_block(header, full_coinbase_hex, job.transaction_hexes).hex(),
        block_hash_hex=stratum_codec.header_hash_hex(header),
        block_hash_int=header_hash_int,
        share_pass=header_hash_int <= job.share_target,
        block_pass=header_hash_int <= job.qbit_target,
        applied_version_hex=version_hex,
    )


def serialize_block(header: bytes, coinbase_tx_hex: str, transaction_hexes: tuple[str, ...]) -> bytes:
    if len(header) != 80:
        raise ValueError("header must be 80 bytes")
    txs = [bytes.fromhex(stratum_codec.validate_hex(coinbase_tx_hex, field_name="coinbase_tx_hex"))]
    txs.extend(
        bytes.fromhex(stratum_codec.validate_hex(tx_hex, field_name=f"transaction_hexes[{index}]"))
        for index, tx_hex in enumerate(transaction_hexes)
    )
    return header + compact_size(len(txs)) + b"".join(txs)
