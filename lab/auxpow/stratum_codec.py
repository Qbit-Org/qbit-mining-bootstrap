#!/usr/bin/env python3
"""Pure Stratum v1 header helpers for the AuxPoW bridge."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


UINT32_HEX_LENGTH = 8
HASH_HEX_LENGTH = 64


@dataclass(frozen=True)
class HeaderVariant:
    name: str
    header: bytes


def validate_hex(value: str, *, field_name: str, expected_chars: int | None = None) -> str:
    if expected_chars is not None and len(value) != expected_chars:
        raise ValueError(f"{field_name} must be {expected_chars // 2} bytes of hex")
    if len(value) % 2 != 0:
        raise ValueError(f"{field_name} must contain an even number of hex characters")
    try:
        bytes.fromhex(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be hexadecimal") from exc
    return value.lower()


def validate_uint32_hex(value: str, *, field_name: str) -> str:
    return validate_hex(value, field_name=field_name, expected_chars=UINT32_HEX_LENGTH)


def validate_hash_hex(value: str, *, field_name: str) -> str:
    return validate_hex(value, field_name=field_name, expected_chars=HASH_HEX_LENGTH)


def parse_mask_hex(value: object, *, field_name: str) -> int:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an 8-character hex string")
    validate_uint32_hex(value, field_name=field_name)
    return int(value, 16) & 0xFFFFFFFF


def format_mask_hex(value: int) -> str:
    return f"{value & 0xFFFFFFFF:08x}"


def apply_version_bits(job_version_hex: str, version_bits_hex: str | None, version_mask: int) -> str:
    job_version_hex = validate_uint32_hex(job_version_hex, field_name="job version")
    if version_bits_hex is None:
        return job_version_hex
    version_bits = parse_mask_hex(version_bits_hex, field_name="version_bits")
    if version_bits & ~version_mask:
        raise ValueError("version_bits include bits outside the negotiated mask")
    job_version = int(job_version_hex, 16) & 0xFFFFFFFF
    return format_mask_hex((job_version & ~version_mask) | (version_bits & version_mask))


def double_sha256(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def flip_word_bytes(data: bytes) -> bytes:
    if len(data) % 4 != 0:
        raise ValueError("data length must be divisible by 4")
    return b"".join(data[offset : offset + 4][::-1] for offset in range(0, len(data), 4))


def stratum_prevhash_from_display_hash(display_hash_hex: str) -> str:
    """Return the notify prevhash that serializes back to a Bitcoin uint256 hash."""

    display_hash_hex = validate_hash_hex(display_hash_hex, field_name="previousblockhash")
    serialized_prevhash = bytes.fromhex(display_hash_hex)[::-1]
    return flip_word_bytes(serialized_prevhash).hex()


def serialized_prevhash_from_display_hash(display_hash_hex: str) -> bytes:
    display_hash_hex = validate_hash_hex(display_hash_hex, field_name="previousblockhash")
    return bytes.fromhex(display_hash_hex)[::-1]


def assemble_coinbase(coinb1_hex: str, extranonce1_hex: str, extranonce2_hex: str, coinb2_hex: str) -> bytes:
    return bytes.fromhex(
        validate_hex(coinb1_hex, field_name="coinb1")
        + validate_hex(extranonce1_hex, field_name="extranonce1")
        + validate_hex(extranonce2_hex, field_name="extranonce2")
        + validate_hex(coinb2_hex, field_name="coinb2")
    )


def compute_merkle_root_from_branch_hex(coinbase_bytes: bytes, merkle_branch_hex: list[str]) -> bytes:
    merkle = double_sha256(coinbase_bytes)
    for offset, sibling_hex in enumerate(merkle_branch_hex):
        sibling = bytes.fromhex(validate_hash_hex(sibling_hex, field_name=f"merkle_branch[{offset}]"))
        merkle = double_sha256(merkle + sibling)
    return merkle


def serialize_header_from_stratum_fields(
    *,
    version_hex: str,
    prevhash_hex: str,
    merkle_root_serialized: bytes,
    ntime_hex: str,
    nbits_hex: str,
    nonce_hex: str,
) -> bytes:
    version_hex = validate_uint32_hex(version_hex, field_name="version")
    prevhash_hex = validate_hash_hex(prevhash_hex, field_name="prevhash")
    ntime_hex = validate_uint32_hex(ntime_hex, field_name="ntime")
    nbits_hex = validate_uint32_hex(nbits_hex, field_name="nbits")
    nonce_hex = validate_uint32_hex(nonce_hex, field_name="nonce")
    if len(merkle_root_serialized) != 32:
        raise ValueError("merkle root must be 32 bytes")

    display_header = bytes.fromhex(
        version_hex
        + prevhash_hex
        + flip_word_bytes(merkle_root_serialized).hex()
        + ntime_hex
        + nbits_hex
        + nonce_hex
    )
    return flip_word_bytes(display_header)


def assemble_header_from_notify_submit(
    *,
    coinb1_hex: str,
    extranonce1_hex: str,
    extranonce2_hex: str,
    coinb2_hex: str,
    merkle_branch_hex: list[str],
    version_hex: str,
    prevhash_hex: str,
    ntime_hex: str,
    nbits_hex: str,
    nonce_hex: str,
) -> tuple[bytes, bytes]:
    coinbase = assemble_coinbase(coinb1_hex, extranonce1_hex, extranonce2_hex, coinb2_hex)
    merkle_root = compute_merkle_root_from_branch_hex(coinbase, merkle_branch_hex)
    header = serialize_header_from_stratum_fields(
        version_hex=version_hex,
        prevhash_hex=prevhash_hex,
        merkle_root_serialized=merkle_root,
        ntime_hex=ntime_hex,
        nbits_hex=nbits_hex,
        nonce_hex=nonce_hex,
    )
    return coinbase, header


def header_hash(header_bytes: bytes) -> bytes:
    if len(header_bytes) != 80:
        raise ValueError("header must be 80 bytes")
    return double_sha256(header_bytes)


def header_hash_int(header_bytes: bytes) -> int:
    return int.from_bytes(header_hash(header_bytes), "little")


def header_hash_hex(header_bytes: bytes) -> str:
    return header_hash(header_bytes)[::-1].hex()


def diagnostic_header_variants(
    *,
    version_hex: str,
    prevhash_stratum_hex: str,
    previousblockhash_display_hex: str,
    merkle_root_serialized: bytes,
    ntime_hex: str,
    nbits_hex: str,
    nonce_hex: str,
) -> list[HeaderVariant]:
    version_raw = validate_uint32_hex(version_hex, field_name="version")
    version_rev = bytes.fromhex(version_raw)[::-1].hex()
    prevhash_stratum = bytes.fromhex(validate_hash_hex(prevhash_stratum_hex, field_name="prevhash"))
    prevhash_serialized = serialized_prevhash_from_display_hash(previousblockhash_display_hex)
    prevhash_display = bytes.fromhex(validate_hash_hex(previousblockhash_display_hex, field_name="previousblockhash"))
    ntime_raw = validate_uint32_hex(ntime_hex, field_name="ntime")
    ntime_rev = bytes.fromhex(ntime_raw)[::-1].hex()
    nbits_raw = validate_uint32_hex(nbits_hex, field_name="nbits")
    nbits_rev = bytes.fromhex(nbits_raw)[::-1].hex()
    nonce_raw = validate_uint32_hex(nonce_hex, field_name="nonce")
    nonce_rev = bytes.fromhex(nonce_raw)[::-1].hex()
    if len(merkle_root_serialized) != 32:
        raise ValueError("merkle root must be 32 bytes")

    variants = {
        "stratum-raw": bytes.fromhex(
            version_raw + prevhash_stratum.hex() + merkle_root_serialized.hex() + ntime_raw + nbits_raw + nonce_raw
        ),
        "serialized-raw": bytes.fromhex(
            version_raw + prevhash_serialized.hex() + merkle_root_serialized.hex() + ntime_raw + nbits_raw + nonce_raw
        ),
        "display-raw": bytes.fromhex(
            version_raw + prevhash_display.hex() + merkle_root_serialized.hex() + ntime_raw + nbits_raw + nonce_raw
        ),
        "stratum-revfields": bytes.fromhex(
            version_rev + prevhash_stratum.hex() + merkle_root_serialized.hex() + ntime_rev + nbits_rev + nonce_rev
        ),
        "serialized-revfields": bytes.fromhex(
            version_rev + prevhash_serialized.hex() + merkle_root_serialized.hex() + ntime_rev + nbits_rev + nonce_rev
        ),
        "display-revfields": bytes.fromhex(
            version_rev + prevhash_display.hex() + merkle_root_serialized.hex() + ntime_rev + nbits_rev + nonce_rev
        ),
    }
    return [HeaderVariant(name, header) for name, header in variants.items()]
