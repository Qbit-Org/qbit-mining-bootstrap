#!/usr/bin/env python3
"""Differential parity oracle for the PRISM payout-window pipeline.

Why this exists: the window pipeline -- accepted-share records in, folded
window plus canonical digest plus audit-builder spool fragments out -- is
slated to gain a second implementation that must be proven byte-identical to
the shipped Python one before it can take traffic (issue #131's method: run
both against the same inputs and assert byte-identical outputs). That proof
needs three things to exist *before* the second implementation does: a corpus
derived from the fold's own invariants, frozen byte-exact reference outputs
produced by the shipped pipeline at a known revision, and a runner that can
drive more than one backend over the same corpus. This module provides all
three, composing the two house precedents: the seeded-corpus discipline of
``tests.test_prism_incremental_payout_window`` (whose record factory it
reuses) and the frozen-reference discipline of the metrics render-parity
fixture.

The three byte outputs pinned per corpus case, all inside the migration's
scope:

1. ``record_jsons`` -- the folded window as the ordered
   ``AcceptedShareRecord.to_prism_json()`` sequence, one canonical JSON
   encoding per record (sorted keys, ``(",", ":")`` separators, ASCII
   escapes), exactly the encoding each window page pre-computes.
2. ``canonical_bytes`` / ``canonical_digest`` -- the canonical JSON array
   exactly as ``IncrementalShareJsonSequence.canonical_json_sha256`` streams
   it (non-empty page fragments joined with ``,`` inside ``[``/``]``) and its
   SHA-256 hex digest. The digest is *not* a hash of a monolithic re-encode;
   an implementation with wrong framing produces a different digest, so the
   framed bytes are pinned alongside the digest.
3. ``spool_tail`` -- the audit-builder spool payload tail written by
   ``_ShareWindowSerialization.acquire_spooled_tail``, byte-for-byte,
   including the ``,"compact_share_identities":`` / ``,"compact_shares":`` /
   ``}`` framing, because that framing is the wire contract.

Adapter contract (the seam a future backend implements): inputs in, the three
byte outputs out. Inputs are one :class:`WindowPipelineCase`, available as an
implementation-neutral JSON document via :func:`case_input_document` -- the
snapshot records with every durable field explicit (``credit_policy`` is null
when absent), the snapshot anchor, ``window_weight``, ``page_size``, and zero
or more append-only advance steps that the backend must fold through its
incremental path. The backend returns :class:`WindowPipelineOutputs`.
Register it with :func:`register_adapter` and select it by setting
``QBIT_WINDOW_PIPELINE_PARITY_ADAPTER``; selection defaults to the shipped
Python pipeline. There is deliberately no production feature switch here --
the per-component switch belongs to the migration slice, not to the oracle.

Regenerating the frozen reference is reproducible and a no-op on an
unchanged tree:

    python3 -m tests.window_pipeline_parity regenerate
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import random
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Protocol

from lab.prism.bundle_compiler import _ShareWindowSerialization
from lab.prism.share_ledger import (
    DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    AcceptedShareRecord,
    IncrementalShareWindow,
)
from tests.test_prism_incremental_payout_window import accepted_share


# Distinct from the sibling test's RANDOM_MASTER_SEED so the two corpora can
# never silently alias; derived per-case seeds use fixed documented offsets.
PARITY_RANDOM_MASTER_SEED = 0xB17E_5EED

REFERENCE_SCHEMA = "qbit-prism-window-pipeline-parity-reference/v1"
CASE_INPUT_SCHEMA = "qbit-prism-window-pipeline-parity-input/v1"
REGENERATE_COMMAND = "python3 -m tests.window_pipeline_parity regenerate"
REFERENCE_FIXTURE_RELPATH = "tests/fixtures/window_pipeline_parity/reference.json"
REFERENCE_FIXTURE_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "window_pipeline_parity" / "reference.json"
)

# Cases whose canonical document fits under this bound additionally pin the
# literal bytes in the fixture, so a divergence shows *what* diverged, not
# just that something did. Larger cases are pinned by length + SHA-256, which
# is still byte-exact; every framing and encoding rule those cases exercise
# is also covered by a literal-pinned case.
LITERAL_PIN_MAX_CANONICAL_BYTES = 16_384

ADAPTER_ENV_VAR = "QBIT_WINDOW_PIPELINE_PARITY_ADAPTER"
DEFAULT_ADAPTER_NAME = "python"

# The durable/public share identity, in declaration order. Deliberately
# excludes ``newly_inserted`` and ``candidate_outbox_state``: both are
# process-local append metadata (compare=False on the dataclass) and are
# outside the pipeline's byte contract.
_RECORD_INPUT_FIELDS = (
    "share_seq",
    "share_id",
    "miner_id",
    "order_key",
    "p2mr_program_hex",
    "share_difficulty",
    "network_difficulty",
    "template_height",
    "job_id",
    "job_issued_at_ms",
    "accepted_at_ms",
    "ntime",
    "credit_policy",
)


@dataclass(frozen=True)
class WindowPipelineAdvance:
    """One append-only delta step folded through the incremental path."""

    anchor_job_issued_at_ms: int
    delta_records: tuple[AcceptedShareRecord, ...]


@dataclass(frozen=True)
class WindowPipelineCase:
    """One corpus case: a full snapshot plus optional incremental advances."""

    name: str
    why: str
    anchor_job_issued_at_ms: int
    window_weight: int
    page_size: int
    snapshot_records: tuple[AcceptedShareRecord, ...]
    advances: tuple[WindowPipelineAdvance, ...] = ()


@dataclass(frozen=True)
class WindowPipelineOutputs:
    """The pipeline's three byte outputs for one corpus case."""

    record_jsons: tuple[bytes, ...]
    canonical_bytes: bytes
    canonical_digest: str
    spool_tail: bytes


class WindowPipelineAdapter(Protocol):
    """One backend driven over the corpus: inputs in, three byte outputs out."""

    name: str

    def run(self, case: WindowPipelineCase) -> WindowPipelineOutputs: ...


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json_bytes(value: object) -> bytes:
    """The pipeline's canonical encoder, byte-identical to the window pages."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def record_input_json(record: AcceptedShareRecord) -> dict[str, object]:
    """One record as implementation-neutral input data, every field explicit."""
    return {name: getattr(record, name) for name in _RECORD_INPUT_FIELDS}


def case_input_document(case: WindowPipelineCase) -> dict[str, object]:
    """The full case as the JSON document a non-Python backend consumes.

    Record order is preserved verbatim: unsorted snapshot input is part of
    what several cases exercise, so normalizing here would erase it.
    """
    return {
        "schema": CASE_INPUT_SCHEMA,
        "name": case.name,
        "window_weight": case.window_weight,
        "page_size": case.page_size,
        "snapshot": {
            "anchor_job_issued_at_ms": case.anchor_job_issued_at_ms,
            "records": [record_input_json(record) for record in case.snapshot_records],
        },
        "advances": [
            {
                "anchor_job_issued_at_ms": step.anchor_job_issued_at_ms,
                "records": [record_input_json(record) for record in step.delta_records],
            }
            for step in case.advances
        ],
    }


def case_input_sha256(case: WindowPipelineCase) -> str:
    return sha256_hex(canonical_json_bytes(case_input_document(case)))


def final_anchor_job_issued_at_ms(case: WindowPipelineCase) -> int:
    if case.advances:
        return case.advances[-1].anchor_job_issued_at_ms
    return case.anchor_job_issued_at_ms


def union_rebuild_case(case: WindowPipelineCase) -> WindowPipelineCase:
    """The snapshot-only case whose bytes an incremental fold must land on."""
    union_records = case.snapshot_records + tuple(
        record for step in case.advances for record in step.delta_records
    )
    return WindowPipelineCase(
        name=f"{case.name}-union-rebuild",
        why=(
            "from_full_snapshot over the union of the snapshot and every"
            " advance delta, at the final anchor; the incremental path must"
            " land on these exact bytes"
        ),
        anchor_job_issued_at_ms=final_anchor_job_issued_at_ms(case),
        window_weight=case.window_weight,
        page_size=case.page_size,
        snapshot_records=union_records,
    )


def fold_case(case: WindowPipelineCase) -> IncrementalShareWindow:
    """Drive the shipped fold: full snapshot, then each advance in order."""
    window = IncrementalShareWindow.from_full_snapshot(
        list(case.snapshot_records),
        anchor_job_issued_at_ms=case.anchor_job_issued_at_ms,
        window_weight=case.window_weight,
        page_size=case.page_size,
    )
    for step in case.advances:
        window, _stats = window.advance(
            list(step.delta_records),
            anchor_job_issued_at_ms=step.anchor_job_issued_at_ms,
        )
    return window


def spool_tail_bytes(shares: list[dict[str, object]]) -> bytes:
    """The exact spool payload tail the shipped serialization writes.

    Drives the real ``acquire_spooled_tail`` against an in-memory spool file
    so the pinned bytes are the shipped write path's output, framing
    included, not a reconstruction of it.
    """
    serialization = _ShareWindowSerialization(
        key=("window-pipeline-parity", len(shares), 0),
        share_count=len(shares),
        share_snapshot_sha256="window-pipeline-parity",
        _spool_factory=io.BytesIO,
    )
    leased = serialization.acquire_spooled_tail(shares)
    if leased is None:
        raise RuntimeError("in-memory spool lease unexpectedly unavailable")
    spool, size = leased
    spool.seek(0)
    tail = spool.read()
    if len(tail) != size:
        raise RuntimeError(f"spool reported {size} bytes but holds {len(tail)}")
    serialization.release_spooled_tail()
    serialization.retire_spool()
    return tail


def outputs_from_window(window: IncrementalShareWindow) -> WindowPipelineOutputs:
    """Capture the three byte outputs from one folded window."""
    json_records = window.json_records()
    record_jsons = tuple(canonical_json_bytes(record) for record in json_records)
    fragments = [
        page.canonical_json_items
        for page in json_records.pages
        if page.canonical_json_items
    ]
    canonical_bytes = b"[" + b",".join(fragments) + b"]"
    return WindowPipelineOutputs(
        record_jsons=record_jsons,
        canonical_bytes=canonical_bytes,
        canonical_digest=json_records.canonical_json_sha256(),
        spool_tail=spool_tail_bytes(list(json_records)),
    )


def record_stream_bytes(record_jsons: tuple[bytes, ...]) -> bytes:
    # Injective: canonical encodings ASCII-escape every control character, so
    # a raw newline can never occur inside one record's bytes.
    return b"\n".join(record_jsons)


@dataclass(frozen=True)
class PythonWindowPipelineAdapter:
    """The shipped Python pipeline, registered as the default backend."""

    name: str = "python"

    def run(self, case: WindowPipelineCase) -> WindowPipelineOutputs:
        return outputs_from_window(fold_case(case))


_ADAPTER_FACTORIES: dict[str, Callable[[], WindowPipelineAdapter]] = {
    DEFAULT_ADAPTER_NAME: PythonWindowPipelineAdapter,
}


def register_adapter(name: str, factory: Callable[[], WindowPipelineAdapter]) -> None:
    if name in _ADAPTER_FACTORIES:
        raise ValueError(f"window-pipeline parity adapter {name!r} is already registered")
    _ADAPTER_FACTORIES[name] = factory


def resolve_adapter(name: str | None = None) -> WindowPipelineAdapter:
    """Resolve one backend; defaults to the shipped Python pipeline."""
    resolved = name or os.environ.get(ADAPTER_ENV_VAR) or DEFAULT_ADAPTER_NAME
    factory = _ADAPTER_FACTORIES.get(resolved)
    if factory is None:
        raise ValueError(
            f"unknown window-pipeline parity adapter {resolved!r};"
            f" registered adapters: {sorted(_ADAPTER_FACTORIES)}"
        )
    return factory()


# --- corpus -----------------------------------------------------------------
#
# Every case is derived from an invariant of ``from_full_snapshot``/``advance``
# or of the byte encodings, and says which. Seeded and deterministic: the
# builders below always produce the same records, so the frozen reference is
# reproducible from an unchanged tree.


def _empty_window_case() -> WindowPipelineCase:
    return WindowPipelineCase(
        name="empty-window",
        why="zero eligible records: [] framing, digest of [], empty compact arrays",
        anchor_job_issued_at_ms=1_000,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=(),
    )


def _single_record_case() -> WindowPipelineCase:
    record = accepted_share(
        1,
        share_difficulty=5,
        job_issued_at_ms=1_999,
        accepted_at_ms=2_000,
    )
    return WindowPipelineCase(
        name="single-record",
        why=(
            "one record with credit_policy None: to_prism_json omits the key"
            " while the compact share tuple carries an explicit null -- the"
            " null-vs-absent divergence surface"
        ),
        anchor_job_issued_at_ms=2_000,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=(record,),
    )


def _crossing_row_retained_case() -> WindowPipelineCase:
    anchor_ms = 10_000
    records = (
        accepted_share(1, share_difficulty=2, job_issued_at_ms=anchor_ms - 4, accepted_at_ms=anchor_ms - 3),
        accepted_share(
            2,
            share_difficulty=3,
            job_issued_at_ms=anchor_ms - 2,
            accepted_at_ms=anchor_ms - 1,
            credit_policy="stale-grace",
        ),
        accepted_share(3, share_difficulty=8, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
    )
    return WindowPipelineCase(
        name="crossing-row-retained",
        why=(
            "difficulties (2,3,8) at weight 10: the final whole share crossing"
            " window_weight (seq 2) is deliberately retained, total 11 > 10;"
            " an implementation that drops it diverges here"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _crossing_row_exact_fit_case() -> WindowPipelineCase:
    anchor_ms = 10_000
    records = (
        accepted_share(1, share_difficulty=2, job_issued_at_ms=anchor_ms - 4, accepted_at_ms=anchor_ms - 3),
        accepted_share(2, share_difficulty=3, job_issued_at_ms=anchor_ms - 2, accepted_at_ms=anchor_ms - 1),
        accepted_share(3, share_difficulty=7, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
    )
    return WindowPipelineCase(
        name="crossing-row-exact-fit",
        why=(
            "difficulties (2,3,7) at weight 10: cumulative weight lands"
            " exactly on window_weight, so seq 1 is dropped and there is no"
            " crossing row; the pair with crossing-row-retained separates the"
            " >= cutoff from a wrong > cutoff"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _eligibility_filtering_case() -> WindowPipelineCase:
    anchor_ms = 30_000
    records = (
        accepted_share(1, share_difficulty=4, job_issued_at_ms=anchor_ms - 2, accepted_at_ms=anchor_ms - 1),
        accepted_share(2, share_difficulty=5, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms - 1),
        accepted_share(3, share_difficulty=6, job_issued_at_ms=anchor_ms - 1, accepted_at_ms=anchor_ms + 1),
        accepted_share(4, share_difficulty=7, job_issued_at_ms=anchor_ms + 2, accepted_at_ms=anchor_ms + 3),
        accepted_share(5, share_difficulty=8, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
    )
    return WindowPipelineCase(
        name="eligibility-filtering",
        why=(
            "one record excluded by job_issued_at_ms alone, one by"
            " accepted_at_ms alone, one by both; seq 5 sits exactly on the"
            " anchor, pinning the inclusive <= comparisons"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=100,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _unsorted_input_case() -> WindowPipelineCase:
    rng = random.Random(PARITY_RANDOM_MASTER_SEED + 1)
    anchor_ms = 40_000
    share_seqs = rng.sample(range(1, 100), 9)
    records = [
        accepted_share(
            share_seq,
            share_difficulty=rng.choice((1, 2, 3, 7, 19)),
            job_issued_at_ms=anchor_ms - rng.randint(0, 30),
            accepted_at_ms=anchor_ms - rng.randint(0, 30),
            credit_policy="stale-grace" if rng.randrange(4) == 0 else None,
        )
        for share_seq in share_seqs
    ]
    return WindowPipelineCase(
        name="unsorted-input",
        why=(
            "snapshot input arrives in non-ascending share_seq order with"
            " non-contiguous seqs; the fold's sorted() is load-bearing and the"
            " output ordering is pinned"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=1_000_000,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=tuple(records),
    )


def _identity_reuse_case() -> WindowPipelineCase:
    anchor_ms = 50_000
    base = [
        accepted_share(
            share_seq,
            share_difficulty=share_seq,
            job_issued_at_ms=anchor_ms - share_seq,
            accepted_at_ms=anchor_ms - share_seq,
            credit_policy="stale-grace" if share_seq in (2, 5) else None,
        )
        for share_seq in range(1, 7)
    ]
    # Force full (miner_id, order_key, p2mr_program_hex) identity collisions:
    # the factory varies p2mr with share_seq, so collisions must be explicit.
    base[3] = replace(
        base[3],
        miner_id=base[0].miner_id,
        order_key=base[0].order_key,
        p2mr_program_hex=base[0].p2mr_program_hex,
    )
    base[5] = replace(
        base[5],
        miner_id=base[1].miner_id,
        order_key=base[1].order_key,
        p2mr_program_hex=base[1].p2mr_program_hex,
    )
    return WindowPipelineCase(
        name="credit-policy-identity-reuse",
        why=(
            "duplicate (miner_id, order_key, p2mr_program_hex) triples pin"
            " identity deduplication and identity_index reuse in the compact"
            " spool fragments, alongside mixed credit_policy values"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=1_000_000,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=tuple(base),
    )


def _non_ascii_strings_case() -> WindowPipelineCase:
    # share_id embeds the raw Stratum username, which survives payout-address
    # fallback un-sanitized, and the ledger validates none of the string
    # fields, so non-ASCII (including non-BMP), quotes, backslashes and
    # control characters can all reach the durable record. json.dumps here
    # runs with ensure_ascii=True, so every such character is \\u-escaped
    # (non-BMP as surrogate pairs) -- a default a different JSON encoder is
    # likely to get wrong.
    anchor_ms = 60_000
    records = (
        replace(
            accepted_share(1, share_difficulty=3, job_issued_at_ms=anchor_ms - 3, accepted_at_ms=anchor_ms - 2),
            share_id="minér-é.workér:0a1b",
            miner_id="minér-é",
            order_key="00:minér-é",
        ),
        replace(
            accepted_share(
                2,
                share_difficulty=4,
                job_issued_at_ms=anchor_ms - 2,
                accepted_at_ms=anchor_ms - 1,
                credit_policy="stale-grace",
            ),
            share_id='\U0001f680-share-"quoted"\\back\\slash',
            job_id="job-\t\n\x01",
        ),
        accepted_share(3, share_difficulty=5, job_issued_at_ms=anchor_ms - 1, accepted_at_ms=anchor_ms),
    )
    return WindowPipelineCase(
        name="non-ascii-strings",
        why=(
            "string fields carrying non-ASCII, a non-BMP code point, quotes,"
            " backslashes and control characters: pins the ensure_ascii escape"
            " rules, surrogate-pair escaping included"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=1_000_000,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _page_boundary_case(retained_count: int, seed_offset: int) -> WindowPipelineCase:
    rng = random.Random(PARITY_RANDOM_MASTER_SEED + seed_offset)
    anchor_ms = 70_000 + retained_count
    records = tuple(
        accepted_share(
            share_seq,
            share_difficulty=rng.choice((1, 2, 3, 7, 19)),
            job_issued_at_ms=anchor_ms - rng.randint(0, 30),
            accepted_at_ms=anchor_ms - rng.randint(0, 30),
            credit_policy="stale-grace" if rng.randrange(4) == 0 else None,
        )
        for share_seq in range(1, retained_count + 1)
    )
    return WindowPipelineCase(
        name=f"page-boundary-{retained_count}",
        why=(
            f"exactly {retained_count} retained records around the default"
            f" page size {DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE}: pins"
            " page splitting and the digest's page-fragment framing at the"
            " boundary"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=10**9,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _multi_page_case() -> WindowPipelineCase:
    rng = random.Random(PARITY_RANDOM_MASTER_SEED + 11)
    anchor_ms = 90_000
    record_count = 1_800
    records = [
        accepted_share(
            share_seq,
            share_difficulty=rng.choice((1, 2, 3, 7, 19)),
            job_issued_at_ms=anchor_ms - rng.randint(0, 30),
            accepted_at_ms=anchor_ms - rng.randint(0, 30),
            credit_policy="stale-grace" if rng.randrange(4) == 0 else None,
        )
        for share_seq in range(1, record_count + 1)
    ]
    # Weight chosen so the newest 1699 shares miss the weight by one and the
    # 1700th (by seq: 101) is the retained crossing row, landing the cutoff in
    # a page interior across four pages.
    window_weight = sum(record.share_difficulty for record in records[101:]) + 1
    shuffled = list(records)
    rng.shuffle(shuffled)
    return WindowPipelineCase(
        name="multi-page-interior-cutoff",
        why=(
            "1800 unsorted input records with the retention cutoff landing"
            " inside a page interior: pins multi-page fragment concatenation"
            " and a crossing row far from page edges"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=window_weight,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=tuple(shuffled),
    )


def _incremental_small_case() -> WindowPipelineCase:
    anchor_ms = 80_000
    difficulties = {1: 5, 2: 1, 3: 1, 4: 2, 5: 1, 6: 3, 7: 1}
    snapshot = [
        accepted_share(
            share_seq,
            share_difficulty=difficulty,
            job_issued_at_ms=anchor_ms - share_seq,
            accepted_at_ms=anchor_ms - share_seq,
            credit_policy="stale-grace" if share_seq == 6 else None,
        )
        for share_seq, difficulty in difficulties.items()
    ]
    # Never eligible at any anchor this case reaches: the incremental path
    # never sees it in a delta, and the union rebuild must exclude it too.
    snapshot.append(
        accepted_share(8, share_difficulty=100, job_issued_at_ms=10**9, accepted_at_ms=10**9)
    )
    first_advance = WindowPipelineAdvance(
        anchor_job_issued_at_ms=anchor_ms + 1,
        delta_records=(
            accepted_share(9, share_difficulty=2, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms - 1),
            accepted_share(10, share_difficulty=4, job_issued_at_ms=anchor_ms - 10, accepted_at_ms=anchor_ms + 1),
        ),
    )
    second_advance = WindowPipelineAdvance(
        anchor_job_issued_at_ms=anchor_ms + 10,
        delta_records=(
            accepted_share(11, share_difficulty=1, job_issued_at_ms=anchor_ms + 5, accepted_at_ms=anchor_ms + 2),
            accepted_share(
                12,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms + 10,
                accepted_at_ms=anchor_ms - 5,
                credit_policy="stale-grace",
            ),
            accepted_share(13, share_difficulty=1, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms + 10),
            accepted_share(
                14,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms + 9,
                accepted_at_ms=anchor_ms + 9,
                credit_policy="stale-grace",
            ),
        ),
    )
    return WindowPipelineCase(
        name="incremental-two-advances",
        why=(
            "two advance() steps over a page_size-3 window: append into a"
            " partially filled page, spill into new pages, expire a whole head"
            " page and partially expire the next; the folded bytes must equal"
            " a full rebuild over the union"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=9,
        page_size=3,
        snapshot_records=tuple(snapshot),
        advances=(first_advance, second_advance),
    )


def _bulk_seeded_case() -> WindowPipelineCase:
    rng = random.Random(PARITY_RANDOM_MASTER_SEED + 12)
    anchor_ms = 1_000_000
    advance_anchor_ms = anchor_ms + 10
    snapshot: list[AcceptedShareRecord] = []
    for share_seq in range(1, 4_001):
        if rng.random() < 0.1:
            # Ineligible at the snapshot anchor and at every later anchor in
            # this case, since the incremental path never re-delivers it.
            lateness = rng.choice(("job", "accepted", "both"))
            job_ms = (
                anchor_ms + rng.randint(11, 500)
                if lateness in ("job", "both")
                else anchor_ms - rng.randint(0, 30)
            )
            accepted_ms = (
                anchor_ms + rng.randint(11, 500)
                if lateness in ("accepted", "both")
                else anchor_ms - rng.randint(0, 30)
            )
        else:
            job_ms = anchor_ms - rng.randint(0, 30)
            accepted_ms = anchor_ms - rng.randint(0, 30)
        snapshot.append(
            accepted_share(
                share_seq,
                share_difficulty=rng.choice((1, 2, 3, 7, 19, 101, 1_000)),
                job_issued_at_ms=job_ms,
                accepted_at_ms=accepted_ms,
                credit_policy="stale-grace" if rng.randrange(5) == 0 else None,
            )
        )
    rng.shuffle(snapshot)
    delta: list[AcceptedShareRecord] = []
    for share_seq in range(4_001, 4_701):
        if rng.randrange(2):
            job_ms = rng.randint(anchor_ms + 1, advance_anchor_ms)
            accepted_ms = advance_anchor_ms - rng.randint(0, 40)
        else:
            job_ms = advance_anchor_ms - rng.randint(0, 40)
            accepted_ms = rng.randint(anchor_ms + 1, advance_anchor_ms)
        delta.append(
            accepted_share(
                share_seq,
                share_difficulty=rng.choice((1, 2, 3, 7, 19, 101, 1_000)),
                job_issued_at_ms=job_ms,
                accepted_at_ms=accepted_ms,
                credit_policy="stale-grace" if rng.randrange(5) == 0 else None,
            )
        )
    return WindowPipelineCase(
        name="bulk-seeded",
        why=(
            "4000 seeded snapshot records (roughly a tenth ineligible) plus a"
            " 700-record advance: representative page-fragment concatenation,"
            " identity deduplication at natural factory collisions, and head"
            " expiry at scale"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=450_000,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=tuple(snapshot),
        advances=(
            WindowPipelineAdvance(
                anchor_job_issued_at_ms=advance_anchor_ms,
                delta_records=tuple(delta),
            ),
        ),
    )


def build_corpus() -> tuple[WindowPipelineCase, ...]:
    """Every corpus case, deterministic and in a stable order."""
    return (
        _empty_window_case(),
        _single_record_case(),
        _crossing_row_retained_case(),
        _crossing_row_exact_fit_case(),
        _eligibility_filtering_case(),
        _unsorted_input_case(),
        _identity_reuse_case(),
        _non_ascii_strings_case(),
        _page_boundary_case(DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE - 1, seed_offset=8),
        _page_boundary_case(DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE, seed_offset=9),
        _page_boundary_case(DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE + 1, seed_offset=10),
        _multi_page_case(),
        _incremental_small_case(),
        _bulk_seeded_case(),
    )


# --- frozen reference -------------------------------------------------------


def reference_case_entry(case: WindowPipelineCase, outputs: WindowPipelineOutputs) -> dict[str, object]:
    stream = record_stream_bytes(outputs.record_jsons)
    if outputs.canonical_digest != sha256_hex(outputs.canonical_bytes):
        raise RuntimeError(
            f"case {case.name}: canonical digest does not hash the framed"
            " canonical bytes; the streaming model no longer matches the"
            " shipped pipeline"
        )
    entry: dict[str, object] = {
        "why": case.why,
        "input_sha256": case_input_sha256(case),
        "record_count": len(outputs.record_jsons),
        "record_stream_len": len(stream),
        "record_stream_sha256": sha256_hex(stream),
        "canonical_bytes_len": len(outputs.canonical_bytes),
        "canonical_bytes_sha256": sha256_hex(outputs.canonical_bytes),
        "canonical_digest": outputs.canonical_digest,
        "spool_tail_len": len(outputs.spool_tail),
        "spool_tail_sha256": sha256_hex(outputs.spool_tail),
    }
    if len(outputs.canonical_bytes) <= LITERAL_PIN_MAX_CANONICAL_BYTES:
        entry["pinned_literals"] = {
            "record_jsons": [record.decode("ascii") for record in outputs.record_jsons],
            "canonical_bytes": outputs.canonical_bytes.decode("ascii"),
            "spool_tail": outputs.spool_tail.decode("ascii"),
        }
    return entry


def reference_document() -> dict[str, object]:
    """The frozen-reference document, regenerated from the shipped pipeline."""
    adapter = PythonWindowPipelineAdapter()
    return {
        "schema": REFERENCE_SCHEMA,
        "regenerate_with": REGENERATE_COMMAND,
        "master_seed": f"0x{PARITY_RANDOM_MASTER_SEED:08x}",
        "default_page_size": DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        "cases": {case.name: reference_case_entry(case, adapter.run(case)) for case in build_corpus()},
    }


def encode_reference_document(document: dict[str, object]) -> bytes:
    # sort_keys plus the default ensure_ascii keeps the fixture byte-stable,
    # so regeneration on an unchanged tree is a no-op.
    return json.dumps(document, indent=2, sort_keys=True).encode("ascii") + b"\n"


def load_reference_document() -> dict[str, object]:
    try:
        raw = REFERENCE_FIXTURE_PATH.read_bytes()
    except FileNotFoundError:
        raise AssertionError(
            f"frozen reference {REFERENCE_FIXTURE_RELPATH} is missing;"
            f" regenerate it with: {REGENERATE_COMMAND}"
        ) from None
    return json.loads(raw)


def diverging_outputs(
    reference_case: dict[str, object],
    outputs: WindowPipelineOutputs,
) -> list[str]:
    """Every way one backend's outputs diverge from one frozen case.

    An empty list is parity. Length and SHA-256 comparisons hold for every
    case; literal comparisons additionally localize the first divergence when
    the case pins literals.
    """
    problems: list[str] = []

    def check_stream(label: str, actual: bytes, literal: str | None) -> None:
        expected_len = reference_case[f"{label}_len"]
        expected_sha = reference_case[f"{label}_sha256"]
        if len(actual) != expected_len:
            problems.append(f"{label}: {len(actual)} bytes != frozen {expected_len}")
        if sha256_hex(actual) != expected_sha:
            problems.append(f"{label}: sha256 diverges from the frozen reference")
        if literal is not None and literal.encode("ascii") != actual:
            problems.append(f"{label}: bytes diverge from the frozen literal")

    literals = reference_case.get("pinned_literals") or {}
    if len(outputs.record_jsons) != reference_case["record_count"]:
        problems.append(
            f"record_count: {len(outputs.record_jsons)} != frozen {reference_case['record_count']}"
        )
    check_stream(
        "record_stream",
        record_stream_bytes(outputs.record_jsons),
        "\n".join(literals["record_jsons"]) if literals else None,
    )
    if literals:
        expected_records = [text.encode("ascii") for text in literals["record_jsons"]]
        for index, (expected, actual) in enumerate(zip(expected_records, outputs.record_jsons)):
            if expected != actual:
                problems.append(
                    f"record_jsons[{index}]: {actual!r} != frozen {expected!r}"
                )
                break
    check_stream("canonical_bytes", outputs.canonical_bytes, literals.get("canonical_bytes"))
    if outputs.canonical_digest != reference_case["canonical_digest"]:
        problems.append(
            f"canonical_digest: {outputs.canonical_digest} != frozen"
            f" {reference_case['canonical_digest']}"
        )
    check_stream("spool_tail", outputs.spool_tail, literals.get("spool_tail"))
    return problems


def _main(argv: list[str]) -> int:
    if argv != ["regenerate"]:
        print(f"usage: {REGENERATE_COMMAND}", file=sys.stderr)
        return 2
    payload = encode_reference_document(reference_document())
    REFERENCE_FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if REFERENCE_FIXTURE_PATH.exists() and REFERENCE_FIXTURE_PATH.read_bytes() == payload:
        print(f"unchanged: {REFERENCE_FIXTURE_RELPATH}")
        return 0
    REFERENCE_FIXTURE_PATH.write_bytes(payload)
    print(f"wrote {len(payload)} bytes: {REFERENCE_FIXTURE_RELPATH}")
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
