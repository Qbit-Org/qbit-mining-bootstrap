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
``tests.test_prism_incremental_payout_window`` and the frozen-reference
discipline of the metrics render-parity fixture.

What is pinned per corpus case, all inside the migration's scope:

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
4. ``advance_stats`` -- one ``IncrementalWindowAdvanceStats`` triple
   (``added_rows``, ``expired_rows``, ``touched_pages``) per advance step, in
   order. These are not wire bytes, but the coordinator exports them to
   metrics on every incremental materialization, so a backend that owns
   ``advance()`` reports them and they must agree. ``touched_pages`` has a
   definition (see :class:`WindowPipelineAdvanceStats`) that is easy to
   re-derive wrongly from the retained result instead of from the work done.
5. A rejection, when the shipped pipeline refuses the case. ``from_full_
   snapshot`` refuses five shapes and ``advance()`` six; a backend that
   accepts an input the shipped pipeline rejects, or rejects one it accepts,
   ships a different payout window. A rejected case freezes only the
   implementation-neutral reason category (``REJECTION_REASONS``), never a
   Python exception class or message, and ``diverging_outputs`` treats
   rejected-vs-outputs in either direction as a divergence and
   rejected-vs-rejected with different categories as one too.

The oracle pins the *pipeline's rejection*, not the coordinator's recovery
from it. In production ``payout_state`` catches the advance fallback, drops
its cached window and re-materializes from a full rescan; that recovery is
coordinator behaviour and is tested there (``tests.test_prism_payout_state``).
Pinning recovery bytes here would let a backend that silently recovers inside
itself -- returning the rescan's bytes where the shipped pipeline refuses --
pass as parity, which is exactly the divergence class the rejection cases
exist to catch.

Integer width. The ledger schema is wider than any fixed machine type:
``share_difficulty`` and ``network_difficulty`` are ``numeric(78,0)``,
``share_seq``, ``template_height`` and ``ntime`` are ``bigint``, and the
millisecond fields are epoch milliseconds above 2^40; Python carries all of
them exactly and renders every digit. The contract the corpus enforces is
narrower, and the decision is made here explicitly: the pipeline's declared
integer widths are the Rust widths --

    share_seq                          u64
    share_difficulty                   u128
    network_difficulty                 u128
    template_height                    u64
    ntime                              u32
    job_issued_at_ms, accepted_at_ms   i64   (and every anchor)
    window_weight                      u128
    page_size                          u64

-- with the fold's difficulty accumulators u128 as well, so a window whose
difficulty total leaves u128 is outside the domain too. Why u128 and not the
schema's 78 digits: ``share_difficulty`` is ``(pow_limit_target*1e6)//target``,
so at a Bitcoin-mainnet-scale target a share is ~2^98 and a 16x window totals
~2^102 -- a factor of roughly 6*10^7 short of 2^128, unreachable on any chain
this code will see. ``numeric(78,0)`` is a generic wide-numeric column
choice, not a considered statement that difficulties exceed 2^128; widening
the Rust fold to arbitrary precision would put bignum arithmetic on the exact
path the migration exists to make faster.

A narrower domain is modelled as exactly that, not as a divergence. A
backend declares its domain (:class:`WindowPipelineIntegerDomain`: the
shipped Python pipeline declares ``UNBOUNDED_INTEGER_DOMAIN``, the daemon
``RUST_INTEGER_DOMAIN``, and a backend that declares nothing claims all of
it) and may answer a case outside that domain with the third adapter
outcome, :class:`WindowPipelineUnsupported`, carrying a declared reason. The
frozen reference keeps Python's bytes for such cases; the harness accepts
``Unsupported`` only where the case really lies outside the backend's
declared domain (:func:`diverging_outputs` is told so by
:func:`domain_exclusion`), still fails a backend that returns wrong *bytes*
there (a saturating u128 accumulator, say), and fails a backend that
declares ``Unsupported`` for a case inside its declared domain. This is what
the system really does: the Rust daemon answers ``out_of_range`` instead of
folding, the coordinator degrades to the in-process fold for that
materialization without retiring the daemon, and Python's unbounded integers
produce the right answer.

The width cases are split accordingly. ``wide-integers`` carries every
integer field far above the 2^31/2^53/2^64 boundaries at which a wrapping or
saturating 32/53/64-bit backend breaks, yet inside every declared width, so
both backends produce bytes and full parity is enforced there: the
protection it was built for (nine narrow-integer backends passed the
original fourteen-case corpus) survives the domain restriction, and the
divergence suite re-checks it against u32-wrapping, u64-wrapping and
u64-saturating backends. The genuinely out-of-domain values live in
``difficulty-beyond-u128`` (``share_difficulty`` 2^128+1 as the retained
crossing row, ``network_difficulty`` at 2^128-1, 2^128 and numeric(78,0)'s
maximum, ``window_weight`` and the retained total above 2^128) and
``ntime-beyond-u32`` (``ntime`` at 2^32 and bigint's maximum): Python's
bytes are frozen, a u128/u32 backend answers ``Unsupported``, and one that
clamps or truncates renders different digits and fails.

Adapter contract (the seam a future backend implements): inputs in, one
result out. Inputs are one :class:`WindowPipelineCase`, available as an
implementation-neutral JSON document via :func:`case_input_document` -- the
snapshot records with every durable field explicit (``credit_policy`` is
null when absent), the snapshot anchor, ``window_weight``, ``page_size``, and
zero or more append-only advance steps that the backend must fold through
its incremental path. The backend returns :class:`WindowPipelineOutputs`,
:class:`WindowPipelineRejection`, or -- only for a case outside its declared
integer domain -- :class:`WindowPipelineUnsupported`. Register it with
:func:`register_adapter` and select it by setting
``QBIT_WINDOW_PIPELINE_PARITY_ADAPTER``; selection defaults to the shipped
Python pipeline. There is deliberately no production feature switch here --
the per-component switch belongs to the migration slice, not to the oracle.

The corpus owns its record factory (:func:`corpus_record`) rather than
importing the sibling golden test's, so an edit there cannot move this
corpus; and literal-pinned cases freeze the input document itself alongside
the outputs, so when the fixture is regenerated the diff shows whether
inputs moved, outputs moved, or both. Without that, any input-only change
fails the suite with "regenerate" and the resulting diff cannot tell the
two apart.

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
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Protocol

from lab.prism.bundle_compiler import (
    PRISM_SERVE_BUILDER_PROTOCOL_VERSION,
    _ShareWindowSerialization,
)
from lab.prism.prism_tools import prism_tool_command
from lab.prism.share_ledger import (
    DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    AcceptedShareRecord,
    IncrementalShareWindow,
    IncrementalWindowAdvanceStats,
    IncrementalWindowFallback,
)


# Distinct from the sibling test's RANDOM_MASTER_SEED so the two corpora can
# never silently alias; derived per-case seeds use fixed documented offsets.
PARITY_RANDOM_MASTER_SEED = 0xB17E_5EED

REFERENCE_SCHEMA = "qbit-prism-window-pipeline-parity-reference/v2"
CASE_INPUT_SCHEMA = "qbit-prism-window-pipeline-parity-input/v1"
REGENERATE_COMMAND = "python3 -m tests.window_pipeline_parity regenerate"
REFERENCE_FIXTURE_RELPATH = "tests/fixtures/window_pipeline_parity/reference.json"
REFERENCE_FIXTURE_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "window_pipeline_parity" / "reference.json"
)

# Cases whose canonical document fits under this bound additionally pin the
# literal bytes in the fixture, so a divergence shows *what* diverged, not
# just that something did; cases whose canonical *input* document fits under
# it pin that document too, so an input-only change is visible as such in the
# fixture diff. Larger cases are pinned by length + SHA-256, which is still
# byte-exact; every framing and encoding rule those cases exercise is also
# covered by a literal-pinned case.
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

# The implementation-neutral rejection vocabulary, one category per condition
# the shipped pipeline refuses. Categories name the *condition*, never the
# Python exception or its message: a backend in another language reports the
# same category from its own error type. The two groups mirror the two entry
# points so a reader can tell which phase refused without knowing the case.
FULL_SNAPSHOT_REJECTIONS = (
    "duplicate_share_seq",
    "duplicate_share_id",
    "non_positive_difficulty",
    "non_positive_window_weight",
    "non_positive_page_size",
)
ADVANCE_REJECTIONS = (
    "anchor_regression",
    "delta_non_positive_difficulty",
    "delta_ineligible_at_anchor",
    "delta_repeats_eligible_share",
    "delta_not_append",
    "delta_order_not_increasing",
)
REJECTION_REASONS = FULL_SNAPSHOT_REJECTIONS + ADVANCE_REJECTIONS

# How the shipped pipeline's rejections map onto the vocabulary. The shipped
# fold signals each condition only through an exception message, so the
# Python adapter classifies by exact message; a message not in this table is
# re-raised rather than guessed, so a new or reworded rejection path surfaces
# as a harness error instead of being quietly filed under a wrong category.
_REJECTION_MESSAGE_CATEGORIES = {
    "full payout window contains duplicate share_seq": "duplicate_share_seq",
    "full payout window contains duplicate share_id": "duplicate_share_id",
    "full payout window contains non-positive difficulty": "non_positive_difficulty",
    "window_weight must be positive": "non_positive_window_weight",
    "page_size must be positive": "non_positive_page_size",
    "snapshot anchor moved backwards": "anchor_regression",
    "delta contains non-positive share difficulty": "delta_non_positive_difficulty",
    "delta contains a share ineligible at the new anchor": "delta_ineligible_at_anchor",
    "delta repeats a share eligible at the previous anchor": "delta_repeats_eligible_share",
    "newly eligible share is not an append": "delta_not_append",
    "delta share_seq order is not increasing": "delta_order_not_increasing",
}
assert set(_REJECTION_MESSAGE_CATEGORIES.values()) == set(REJECTION_REASONS)


def rejection_category_for_message(message: str) -> str | None:
    """The vocabulary category for one shipped-pipeline rejection message.

    None for a message the table does not know, which callers must treat as
    a harness fault rather than guess at. Shared with the fake serve builder
    so its ``rejection`` field is classified exactly as the Python adapter
    classifies.
    """
    return _REJECTION_MESSAGE_CATEGORIES.get(message)


# --- declared integer domains ------------------------------------------------
#
# The integer fields a record carries, in declaration order; the strings are
# outside the width contract.
_RECORD_INTEGER_FIELDS = (
    "share_seq",
    "share_difficulty",
    "network_difficulty",
    "template_height",
    "job_issued_at_ms",
    "accepted_at_ms",
    "ntime",
)


@dataclass(frozen=True)
class IntegerWidth:
    """One declared integer width: an inclusive range, or unbounded."""

    name: str
    minimum: int | None
    maximum: int | None

    def admits(self, value: int) -> bool:
        return (self.minimum is None or value >= self.minimum) and (
            self.maximum is None or value <= self.maximum
        )


WIDTH_U32 = IntegerWidth("u32", 0, 2**32 - 1)
WIDTH_U64 = IntegerWidth("u64", 0, 2**64 - 1)
WIDTH_U128 = IntegerWidth("u128", 0, 2**128 - 1)
WIDTH_I64 = IntegerWidth("i64", -(2**63), 2**63 - 1)
WIDTH_UNBOUNDED = IntegerWidth("unbounded", None, None)


@dataclass(frozen=True)
class WindowPipelineIntegerDomain:
    """The integer widths one backend declares for the pipeline's inputs.

    Every record integer, the anchors, ``window_weight`` and ``page_size``
    carry a declared width; ``difficulty_total`` bounds the fold's
    accumulator -- the sum of ``share_difficulty`` over every record a case
    feeds the backend (snapshot plus every delta) must fit it, which is a
    conservative envelope of every partial sum the fold forms. A case with
    any value outside its width is outside the domain, and only then may the
    backend answer :class:`WindowPipelineUnsupported`.
    """

    share_seq: IntegerWidth
    share_difficulty: IntegerWidth
    network_difficulty: IntegerWidth
    template_height: IntegerWidth
    ntime: IntegerWidth
    timestamps_ms: IntegerWidth
    window_weight: IntegerWidth
    page_size: IntegerWidth
    difficulty_total: IntegerWidth

    def record_field_width(self, name: str) -> IntegerWidth:
        if name in ("job_issued_at_ms", "accepted_at_ms"):
            return self.timestamps_ms
        if name not in _RECORD_INTEGER_FIELDS:
            raise KeyError(f"{name!r} is not an integer record field")
        return getattr(self, name)

    def exclusion(self, case: WindowPipelineCase) -> str | None:
        """Why the case lies outside this domain, or None when it is inside.

        Walks the inputs in document order -- the top-level fields, the
        snapshot records, then each advance's anchor and records -- and names
        the first value outside its width, so the reason is stable and reads
        as the field that caused it; the accumulator envelope is checked last.
        """

        def outside(label: str, value: int, width: IntegerWidth) -> str | None:
            if width.admits(int(value)):
                return None
            return f"{label} {int(value)} is outside {width.name}"

        for label, value, width in (
            ("window_weight", case.window_weight, self.window_weight),
            ("page_size", case.page_size, self.page_size),
            ("snapshot anchor_job_issued_at_ms", case.anchor_job_issued_at_ms, self.timestamps_ms),
        ):
            problem = outside(label, value, width)
            if problem is not None:
                return problem
        groups: list[tuple[str, int | None, tuple[AcceptedShareRecord, ...]]] = [
            ("snapshot", None, case.snapshot_records)
        ]
        groups.extend(
            (f"advance[{index}]", step.anchor_job_issued_at_ms, step.delta_records)
            for index, step in enumerate(case.advances)
        )
        total = 0
        for label, anchor_ms, records in groups:
            if anchor_ms is not None:
                problem = outside(f"{label} anchor_job_issued_at_ms", anchor_ms, self.timestamps_ms)
                if problem is not None:
                    return problem
            for index, record in enumerate(records):
                for name in _RECORD_INTEGER_FIELDS:
                    problem = outside(
                        f"{label} record[{index}].{name}",
                        getattr(record, name),
                        self.record_field_width(name),
                    )
                    if problem is not None:
                        return problem
                total += int(record.share_difficulty)
        return outside("difficulty total over every record", total, self.difficulty_total)


# The shipped Python pipeline: arbitrary-precision integers throughout.
UNBOUNDED_INTEGER_DOMAIN = WindowPipelineIntegerDomain(
    share_seq=WIDTH_UNBOUNDED,
    share_difficulty=WIDTH_UNBOUNDED,
    network_difficulty=WIDTH_UNBOUNDED,
    template_height=WIDTH_UNBOUNDED,
    ntime=WIDTH_UNBOUNDED,
    timestamps_ms=WIDTH_UNBOUNDED,
    window_weight=WIDTH_UNBOUNDED,
    page_size=WIDTH_UNBOUNDED,
    difficulty_total=WIDTH_UNBOUNDED,
)

# The Rust window pipeline, mirroring the declared-width table in
# crates/qbit-prism/src/window.rs (the AcceptedShare field types plus the
# prepare_window request fields and the u128 accumulators). Changing either
# side without the other is a contract change and must show up here.
RUST_INTEGER_DOMAIN = WindowPipelineIntegerDomain(
    share_seq=WIDTH_U64,
    share_difficulty=WIDTH_U128,
    network_difficulty=WIDTH_U128,
    template_height=WIDTH_U64,
    ntime=WIDTH_U32,
    timestamps_ms=WIDTH_I64,
    window_weight=WIDTH_U128,
    page_size=WIDTH_U64,
    difficulty_total=WIDTH_U128,
)


def corpus_record(
    share_seq: int,
    *,
    share_difficulty: int,
    job_issued_at_ms: int,
    accepted_at_ms: int,
    credit_policy: str | None = None,
) -> AcceptedShareRecord:
    """The corpus's own deterministic record factory.

    Owned here rather than imported from the sibling golden test so that an
    edit to that module's factory can never move this corpus: the frozen
    reference is only as stable as the inputs that produced it, and the
    fixture stores those inputs' hash (and, for small cases, the document).
    The derivations deliberately match the sibling factory as of the first
    freeze, so the reference needed no regeneration when ownership moved.
    """
    return AcceptedShareRecord(
        share_seq=share_seq,
        share_id=f"share-{share_seq}",
        miner_id=f"miner-{share_seq % 11}",
        order_key=f"{share_seq % 11:02d}:miner-{share_seq % 11}",
        p2mr_program_hex=f"{share_seq % 256:02x}" * 32,
        share_difficulty=share_difficulty,
        network_difficulty=1_000_000 + (share_seq % 17),
        template_height=800_000 + (share_seq // 32),
        job_id=f"job-{share_seq}",
        job_issued_at_ms=job_issued_at_ms,
        accepted_at_ms=accepted_at_ms,
        ntime=1_700_000_000 + share_seq,
        credit_policy=credit_policy,
    )


@dataclass(frozen=True)
class WindowPipelineAdvance:
    """One append-only delta step folded through the incremental path."""

    anchor_job_issued_at_ms: int
    delta_records: tuple[AcceptedShareRecord, ...]


@dataclass(frozen=True)
class WindowPipelineCase:
    """One corpus case: a full snapshot plus optional incremental advances.

    ``expected_rejection`` is the category the case was *built* to trigger
    (None for a case the pipeline accepts). It is not input -- the reference
    freezes what the shipped pipeline actually reports -- but the structural
    test holds the two equal so a rejection case cannot quietly start passing
    for a different reason, or none.
    """

    name: str
    why: str
    anchor_job_issued_at_ms: int
    window_weight: int
    page_size: int
    snapshot_records: tuple[AcceptedShareRecord, ...]
    advances: tuple[WindowPipelineAdvance, ...] = ()
    expected_rejection: str | None = None


@dataclass(frozen=True)
class WindowPipelineAdvanceStats:
    """The bounded-work triple one advance step reports, seam-owned.

    Mirrors ``IncrementalWindowAdvanceStats`` without importing its type into
    the contract. ``touched_pages`` counts pre-existing pages whose records
    the advance inspected or rewrote: the last pre-existing page when the
    delta was appended into it, and the pre-existing page that was partially
    expired, deduplicated -- so a page both appended into and partially
    expired counts once, a page appended into and then expired wholesale in
    the same advance still counts (its records were rewritten), a freshly
    allocated append page never counts even when partial expiry lands in it,
    and pages expired wholesale without being appended into never count.
    That is the shipped definition; it is derived from the work performed,
    not from the retained result, which is why a re-implementation that
    computes it from survivors gets the expired-append page wrong.
    """

    added_rows: int
    expired_rows: int
    touched_pages: int

    @classmethod
    def from_shipped(cls, stats: IncrementalWindowAdvanceStats) -> WindowPipelineAdvanceStats:
        return cls(
            added_rows=int(stats.added_rows),
            expired_rows=int(stats.expired_rows),
            touched_pages=int(stats.touched_pages),
        )

    def as_document(self) -> dict[str, int]:
        return {
            "added_rows": self.added_rows,
            "expired_rows": self.expired_rows,
            "touched_pages": self.touched_pages,
        }


@dataclass(frozen=True)
class WindowPipelineOutputs:
    """The pipeline's outputs for one accepted corpus case.

    ``advance_stats`` carries one entry per advance step the case folded, in
    order; it is empty for snapshot-only cases.
    """

    record_jsons: tuple[bytes, ...]
    canonical_bytes: bytes
    canonical_digest: str
    spool_tail: bytes
    advance_stats: tuple[WindowPipelineAdvanceStats, ...] = ()


@dataclass(frozen=True)
class WindowPipelineRejection:
    """The pipeline refused the case, for one of ``REJECTION_REASONS``."""

    reason: str

    def __post_init__(self) -> None:
        if self.reason not in REJECTION_REASONS:
            raise ValueError(
                f"unknown window-pipeline rejection reason {self.reason!r};"
                f" the vocabulary is {list(REJECTION_REASONS)}"
            )


@dataclass(frozen=True)
class WindowPipelineUnsupported:
    """The backend's declared integer domain does not include this input.

    Neither a rejection (the pipeline did not refuse the case; this backend
    cannot represent it) nor a divergence (the frozen reference keeps the
    shipped pipeline's result): it is the outcome the coordinator really
    sees when the daemon answers ``out_of_range`` and the window is folded
    in-process instead. ``reason`` is the backend's declared explanation,
    carried for the report and never matched on -- whether the outcome is
    legitimate is decided from the backend's declared domain alone, see
    :func:`diverging_outputs`.
    """

    reason: str


WindowPipelineResult = (
    WindowPipelineOutputs | WindowPipelineRejection | WindowPipelineUnsupported
)


class WindowPipelineAdapter(Protocol):
    """One backend driven over the corpus: inputs in, a result out.

    ``integer_domain`` is the domain the backend declares; a backend without
    the attribute claims the unbounded domain, so any ``Unsupported`` it
    returns is a divergence.
    """

    name: str
    integer_domain: WindowPipelineIntegerDomain

    def run(self, case: WindowPipelineCase) -> WindowPipelineResult: ...


def adapter_integer_domain(adapter: object) -> WindowPipelineIntegerDomain:
    """The domain a backend declares; one that declares nothing claims all of it."""
    domain = getattr(adapter, "integer_domain", UNBOUNDED_INTEGER_DOMAIN)
    if not isinstance(domain, WindowPipelineIntegerDomain):
        raise TypeError(
            f"adapter {getattr(adapter, 'name', adapter)!r} declares an"
            " integer_domain that is not a WindowPipelineIntegerDomain"
        )
    return domain


def domain_exclusion(adapter: object, case: WindowPipelineCase) -> str | None:
    """Why the case is outside the backend's declared domain, or None."""
    return adapter_integer_domain(adapter).exclusion(case)


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


def case_input_bytes(case: WindowPipelineCase) -> bytes:
    return canonical_json_bytes(case_input_document(case))


def case_input_sha256(case: WindowPipelineCase) -> str:
    return sha256_hex(case_input_bytes(case))


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


def fold_case_with_stats(
    case: WindowPipelineCase,
) -> tuple[IncrementalShareWindow, tuple[WindowPipelineAdvanceStats, ...]]:
    """Drive the shipped fold: full snapshot, then each advance in order.

    Raises exactly what the shipped pipeline raises; :func:`run_python_pipeline`
    is the seam-facing wrapper that classifies those into rejections.
    """
    window = IncrementalShareWindow.from_full_snapshot(
        list(case.snapshot_records),
        anchor_job_issued_at_ms=case.anchor_job_issued_at_ms,
        window_weight=case.window_weight,
        page_size=case.page_size,
    )
    stats: list[WindowPipelineAdvanceStats] = []
    for step in case.advances:
        window, step_stats = window.advance(
            list(step.delta_records),
            anchor_job_issued_at_ms=step.anchor_job_issued_at_ms,
        )
        stats.append(WindowPipelineAdvanceStats.from_shipped(step_stats))
    return window, tuple(stats)


def fold_case(case: WindowPipelineCase) -> IncrementalShareWindow:
    """The folded window of a case the shipped pipeline accepts."""
    return fold_case_with_stats(case)[0]


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


def outputs_from_window(
    window: IncrementalShareWindow,
    advance_stats: tuple[WindowPipelineAdvanceStats, ...] = (),
) -> WindowPipelineOutputs:
    """Capture the byte outputs from one folded window."""
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
        advance_stats=advance_stats,
    )


def run_python_pipeline(case: WindowPipelineCase) -> WindowPipelineResult:
    """The shipped pipeline over one case: outputs, or its rejection classified.

    Only the fold is guarded: a failure while capturing outputs (spool, JSON)
    is a harness fault and propagates, never a rejection.
    """
    try:
        window, advance_stats = fold_case_with_stats(case)
    except (ValueError, IncrementalWindowFallback) as exc:
        category = rejection_category_for_message(str(exc))
        if category is None:
            raise
        return WindowPipelineRejection(category)
    return outputs_from_window(window, advance_stats)


def record_stream_bytes(record_jsons: tuple[bytes, ...]) -> bytes:
    # Injective: canonical encodings ASCII-escape every control character, so
    # a raw newline can never occur inside one record's bytes.
    return b"\n".join(record_jsons)


@dataclass(frozen=True)
class PythonWindowPipelineAdapter:
    """The shipped Python pipeline, registered as the default backend."""

    name: str = "python"
    integer_domain: WindowPipelineIntegerDomain = UNBOUNDED_INTEGER_DOMAIN

    def run(self, case: WindowPipelineCase) -> WindowPipelineResult:
        return run_python_pipeline(case)


def _split_record_fragments(items: bytes) -> tuple[bytes, ...]:
    """Split one canonical items stream into its per-record byte spans.

    The spans come straight from the backend's bytes (``raw_decode`` only
    reports where each JSON object ends), so a backend encoding divergence is
    still visible in the returned fragments rather than being papered over by
    a re-encode.
    """
    if not items:
        return ()
    text = items.decode("ascii")
    decoder = json.JSONDecoder()
    fragments: list[bytes] = []
    position = 0
    while position < len(text):
        _, end = decoder.raw_decode(text, position)
        fragments.append(text[position:end].encode("ascii"))
        if end < len(text) and text[end] != ",":
            raise ValueError(
                f"canonical items stream has {text[end]!r} where a record"
                " separator was expected"
            )
        position = end + 1
    return tuple(fragments)


@dataclass(frozen=True)
class RustDaemonWindowPipelineAdapter:
    """The Rust --serve daemon's prepare_window pipeline as a backend.

    Drives the real daemon binary over its JSONL protocol: one ``full``
    preparation for the snapshot, then one ``advance`` per delta step, holding
    the canonical items stream exactly the way the coordinator's opaque
    mirror does (drop the reported prefix, append the returned suffix) and
    taking each advance's stats triple from its envelope. The three byte
    outputs are taken from the daemon's bytes; only the spool tail runs the
    shipped Python serialization over the lazily parsed records, which is
    precisely the production fallback path under the Rust switch.

    Non-success envelopes are classified on their structure, never on the
    diagnostic ``error`` text: ``fold_invalid`` and ``fallback`` carry the
    daemon's ``rejection`` category, which is the oracle's own vocabulary and
    is filed as :class:`WindowPipelineRejection` (an unknown category is a
    harness error, not a guess); ``out_of_range`` -- an integer or difficulty
    total outside the widths this adapter declares as ``RUST_INTEGER_DOMAIN``
    -- is :class:`WindowPipelineUnsupported`. Anything else is a harness
    error, exactly as the coordinator treats it as a daemon anomaly.

    Selected with ``QBIT_WINDOW_PIPELINE_PARITY_ADAPTER=rust-daemon``; the
    daemon binary resolves through ``prism_tool_command`` (``cargo run``
    unless ``PRISM_TOOL_BIN_DIR`` provides a prebuilt binary).
    """

    name: str = "rust-daemon"
    integer_domain: WindowPipelineIntegerDomain = RUST_INTEGER_DOMAIN

    @staticmethod
    def daemon_command() -> list[str]:
        """Return the real deployment tool invocation used by this adapter."""
        return prism_tool_command("qbit-prism-build-audit-bundle") + [
            "--serve",
            "--signing-key-seed-hex",
            "42" * 32,
            "--ledger-signing-key-seed-hex",
            "43" * 32,
        ]

    @staticmethod
    def _wire_record(record: AcceptedShareRecord) -> dict[str, object]:
        return record_input_json(record)

    @staticmethod
    def _declined(envelope: dict[str, object]) -> WindowPipelineRejection | WindowPipelineUnsupported:
        """Classify one non-``ok`` prepare_window envelope by its structure."""
        if envelope.get("request") != "prepare_window":
            raise RuntimeError(f"prepare_window failed: {envelope}")
        if envelope.get("out_of_range") is True:
            return WindowPipelineUnsupported(
                str(envelope.get("error") or "value outside the daemon's declared widths")
            )
        if envelope.get("fold_invalid") is True or envelope.get("fallback") is True:
            category = envelope.get("rejection")
            if not isinstance(category, str):
                raise RuntimeError(
                    f"daemon rejection carries no machine-readable category: {envelope}"
                )
            # A category outside the vocabulary raises ValueError here: a
            # harness error, never a quiet filing under the wrong reason.
            return WindowPipelineRejection(category)
        raise RuntimeError(f"prepare_window failed: {envelope}")

    def run(self, case: WindowPipelineCase) -> WindowPipelineResult:
        process = subprocess.Popen(
            self.daemon_command(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        advance_stats: list[WindowPipelineAdvanceStats] = []
        try:
            assert process.stdin is not None and process.stdout is not None
            handshake = json.loads(process.stdout.readline())
            expected_handshake = {
                "event": "handshake",
                "tool": "qbit-prism-build-audit-bundle",
                "protocol": PRISM_SERVE_BUILDER_PROTOCOL_VERSION,
            }
            announced_handshake = {
                key: handshake.get(key) for key in expected_handshake
            }
            if announced_handshake != expected_handshake:
                raise RuntimeError(
                    f"daemon announced handshake {announced_handshake!r},"
                    f" expected {expected_handshake!r}"
                )

            def exchange(request: dict[str, object]) -> tuple[dict[str, object], bytes | None]:
                """One round trip; the payload is None when the daemon declined."""
                assert process.stdin is not None and process.stdout is not None
                process.stdin.write(
                    json.dumps(request, separators=(",", ":")).encode("ascii") + b"\n"
                )
                process.stdin.flush()
                envelope = json.loads(process.stdout.readline())
                if envelope.get("ok") is not True:
                    return envelope, None
                length_key = (
                    "window_items_len"
                    if "window_items_len" in envelope
                    else "appended_items_len"
                )
                payload = process.stdout.read(int(envelope[length_key]))
                process.stdout.read(1)  # the terminating newline
                return envelope, payload

            envelope, items = exchange(
                {
                    "request": "prepare_window",
                    "mode": "full",
                    "append_invalidation_epoch": 0,
                    "anchor_job_issued_at_ms": case.anchor_job_issued_at_ms,
                    "window_weight": case.window_weight,
                    "page_size": case.page_size,
                    "records": [
                        self._wire_record(record) for record in case.snapshot_records
                    ],
                }
            )
            if items is None:
                return self._declined(envelope)
            for step in case.advances:
                envelope, appended = exchange(
                    {
                        "request": "prepare_window",
                        "mode": "advance",
                        "append_invalidation_epoch": 0,
                        "anchor_job_issued_at_ms": step.anchor_job_issued_at_ms,
                        "base_digest": envelope["share_snapshot_sha256"],
                        "records": [
                            self._wire_record(record) for record in step.delta_records
                        ],
                    }
                )
                if appended is None:
                    return self._declined(envelope)
                items = items[int(envelope["retained_drop_bytes"]) :] + appended
                advance_stats.append(
                    WindowPipelineAdvanceStats(
                        added_rows=int(envelope["added_rows"]),
                        expired_rows=int(envelope["expired_rows"]),
                        touched_pages=int(envelope["touched_pages"]),
                    )
                )
        finally:
            if process.stdin is not None:
                process.stdin.close()
            process.wait(timeout=30.0)
            if process.stdout is not None:
                process.stdout.close()

        record_jsons = _split_record_fragments(items)
        if len(record_jsons) != int(envelope["record_count"]):
            raise RuntimeError(
                f"daemon reported {envelope['record_count']} records but the"
                f" items stream holds {len(record_jsons)}"
            )
        shares = [json.loads(fragment) for fragment in record_jsons]
        return WindowPipelineOutputs(
            record_jsons=record_jsons,
            canonical_bytes=b"[" + items + b"]",
            canonical_digest=str(envelope["share_snapshot_sha256"]),
            spool_tail=spool_tail_bytes(shares),
            advance_stats=tuple(advance_stats),
        )


_ADAPTER_FACTORIES: dict[str, Callable[[], WindowPipelineAdapter]] = {
    DEFAULT_ADAPTER_NAME: PythonWindowPipelineAdapter,
    "rust-daemon": RustDaemonWindowPipelineAdapter,
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
    record = corpus_record(
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
        corpus_record(1, share_difficulty=2, job_issued_at_ms=anchor_ms - 4, accepted_at_ms=anchor_ms - 3),
        corpus_record(
            2,
            share_difficulty=3,
            job_issued_at_ms=anchor_ms - 2,
            accepted_at_ms=anchor_ms - 1,
            credit_policy="stale-grace",
        ),
        corpus_record(3, share_difficulty=8, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
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
        corpus_record(1, share_difficulty=2, job_issued_at_ms=anchor_ms - 4, accepted_at_ms=anchor_ms - 3),
        corpus_record(2, share_difficulty=3, job_issued_at_ms=anchor_ms - 2, accepted_at_ms=anchor_ms - 1),
        corpus_record(3, share_difficulty=7, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
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
        corpus_record(1, share_difficulty=4, job_issued_at_ms=anchor_ms - 2, accepted_at_ms=anchor_ms - 1),
        corpus_record(2, share_difficulty=5, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms - 1),
        corpus_record(3, share_difficulty=6, job_issued_at_ms=anchor_ms - 1, accepted_at_ms=anchor_ms + 1),
        corpus_record(4, share_difficulty=7, job_issued_at_ms=anchor_ms + 2, accepted_at_ms=anchor_ms + 3),
        corpus_record(5, share_difficulty=8, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
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
        corpus_record(
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
        corpus_record(
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
    # likely to get wrong. The fourth record widens the alphabet to the
    # characters that separate Python's encoder from a "below 0x20 plus quote
    # and backslash" re-encoder: DEL (0x7f, escaped by Python, raw in
    # serde_json and most hand-rolled encoders), solidus (raw in Python,
    # escaped by some encoders), the short escapes \\r \\b \\f, U+001F (the
    # last short-less control), U+2028/U+2029 (escaped by Python, raw in
    # encoders that only escape JavaScript-unsafe text), and space and
    # uppercase, which must pass through untouched. They sit in the identity
    # fields too so the spool's compact identities carry the same escapes.
    anchor_ms = 60_000
    records = (
        replace(
            corpus_record(1, share_difficulty=3, job_issued_at_ms=anchor_ms - 3, accepted_at_ms=anchor_ms - 2),
            share_id="minér-é.workér:0a1b",
            miner_id="minér-é",
            order_key="00:minér-é",
        ),
        replace(
            corpus_record(
                2,
                share_difficulty=4,
                job_issued_at_ms=anchor_ms - 2,
                accepted_at_ms=anchor_ms - 1,
                credit_policy="stale-grace",
            ),
            share_id='\U0001f680-share-"quoted"\\back\\slash',
            job_id="job-\t\n\x01",
        ),
        corpus_record(3, share_difficulty=5, job_issued_at_ms=anchor_ms - 1, accepted_at_ms=anchor_ms),
        replace(
            corpus_record(4, share_difficulty=6, job_issued_at_ms=anchor_ms - 1, accepted_at_ms=anchor_ms),
            share_id="MINER-A/worker 1:\x7f\r\b\x0c\x1f\u2028\u2029",
            miner_id="MINER-A/worker 1\x7f",
            order_key="04:MINER-A/worker 1\x7f",
        ),
    )
    return WindowPipelineCase(
        name="non-ascii-strings",
        why=(
            "string fields carrying non-ASCII, a non-BMP code point, quotes,"
            " backslashes, control characters, DEL, solidus, CR/BS/FF, U+001F,"
            " U+2028/U+2029, space and uppercase: pins the ensure_ascii escape"
            " rules -- surrogate pairs, \\u007f for DEL, raw solidus, short"
            " escapes, and pass-through of printable ASCII"
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
        corpus_record(
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
        corpus_record(
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
        corpus_record(
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
        corpus_record(8, share_difficulty=100, job_issued_at_ms=10**9, accepted_at_ms=10**9)
    )
    first_advance = WindowPipelineAdvance(
        anchor_job_issued_at_ms=anchor_ms + 1,
        delta_records=(
            corpus_record(9, share_difficulty=2, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms - 1),
            corpus_record(10, share_difficulty=4, job_issued_at_ms=anchor_ms - 10, accepted_at_ms=anchor_ms + 1),
        ),
    )
    second_advance = WindowPipelineAdvance(
        anchor_job_issued_at_ms=anchor_ms + 10,
        delta_records=(
            corpus_record(11, share_difficulty=1, job_issued_at_ms=anchor_ms + 5, accepted_at_ms=anchor_ms + 2),
            corpus_record(
                12,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms + 10,
                accepted_at_ms=anchor_ms - 5,
                credit_policy="stale-grace",
            ),
            corpus_record(13, share_difficulty=1, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms + 10),
            corpus_record(
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
            corpus_record(
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
            corpus_record(
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


# A realistic production anchor: epoch milliseconds in 2025, above 2^40. The
# previous cases all sit below 2^31, which is exactly why a 32-bit backend
# could pass them; every width case below runs at this scale.
_REALISTIC_ANCHOR_MS = 1_755_000_000_000


def _wide_integers_case() -> WindowPipelineCase:
    anchor_ms = _REALISTIC_ANCHOR_MS
    # Every value sits far above the 2^31 / 2^53 / 2^64 boundaries at which a
    # wrapping or saturating 32/53/64-bit backend breaks, yet inside every
    # declared Rust width (share_seq and template_height u64, difficulties
    # and window_weight u128, ntime u32), so both backends must produce these
    # exact bytes. Retention runs newest-first until the accumulated weight
    # reaches window_weight. The three newest (2^53+1, 2^63, 2^64+1) sum to
    # 2^64+2^63+2^53+2, below window_weight only because of its 2^127 term,
    # so the oldest (2^127+3) is the retained crossing row and all four
    # survive: a backend that wraps or saturates at 64 bits (or truncates
    # window_weight to 64 bits) stops after three, and one that clamps the
    # inputs renders different digits even where it keeps the count. The
    # difficulty total over every record stays below 2^128, inside the u128
    # accumulator, so the case is inside the declared domain entirely.
    records = (
        replace(
            corpus_record(1, share_difficulty=2**127 + 3, job_issued_at_ms=anchor_ms - 3, accepted_at_ms=anchor_ms - 2),
            network_difficulty=2**128 - 1,
            template_height=2**62,
            ntime=2**32 - 1,
        ),
        replace(
            corpus_record(2**31, share_difficulty=2**64 + 1, job_issued_at_ms=anchor_ms - 2, accepted_at_ms=anchor_ms - 1),
            network_difficulty=2**64 + 1,
            template_height=2**31 + 1,
            ntime=2**31,
        ),
        replace(
            corpus_record(2**32 + 5, share_difficulty=2**63, job_issued_at_ms=anchor_ms - 1, accepted_at_ms=anchor_ms - 1),
            network_difficulty=2**64 - 1,
            template_height=0,
            ntime=0,
        ),
        # share_seq at u64's maximum (the factory's derived template_height
        # lands near 2^59); ntime must be set explicitly because the
        # factory's default would add the seq to it.
        replace(
            corpus_record(2**64 - 1, share_difficulty=2**53 + 1, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
            network_difficulty=2**53 + 1,
            ntime=2**31 + 1,
        ),
        # Excluded by a one-millisecond margin at 2^40 scale: a backend
        # comparing truncated or float-rounded timestamps includes it.
        corpus_record(7, share_difficulty=1, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms),
    )
    return WindowPipelineCase(
        name="wide-integers",
        why=(
            "every integer field above 2^32 at a realistic epoch-ms anchor,"
            " all inside the declared Rust widths: share_difficulty across"
            " 2^53+1, 2^63, 2^64+1 and 2^127+3, network_difficulty up to"
            " 2^128-1, template_height 2^31+1, 2^62 and near 2^59, ntime"
            " 2^31 to 2^32-1, share_seq up to 2^64-1, a zero"
            " template_height/ntime, and window_weight 2^127+2^64+2^63+4 so"
            " the retention cutoff depends on exact arithmetic above 2^64;"
            " pins full-digit rendering and non-wrapping, non-saturating,"
            " non-float accumulation, with full parity enforced on every"
            " backend"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=2**127 + 2**64 + 2**63 + 4,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _difficulty_beyond_u128_case() -> WindowPipelineCase:
    anchor_ms = _REALISTIC_ANCHOR_MS
    # Newest-first: 2^63, then 2^64-1, then 2^128+1 crosses window_weight and
    # is retained; the oldest (2^127) is dropped. The retained total is above
    # 2^128 and so is window_weight itself, so neither the inputs nor the
    # accumulator fit a 128-bit integer: a backend that clamps at u128::MAX
    # renders different digits for the crossing row and for the numeric(78,0)
    # maximum network_difficulty even though its retained count agrees.
    records = (
        replace(
            corpus_record(1, share_difficulty=2**127, job_issued_at_ms=anchor_ms - 3, accepted_at_ms=anchor_ms - 3),
            network_difficulty=2**128 - 1,
        ),
        replace(
            corpus_record(2, share_difficulty=2**128 + 1, job_issued_at_ms=anchor_ms - 2, accepted_at_ms=anchor_ms - 2),
            network_difficulty=10**78 - 1,
        ),
        replace(
            corpus_record(3, share_difficulty=2**64 - 1, job_issued_at_ms=anchor_ms - 1, accepted_at_ms=anchor_ms - 1),
            network_difficulty=2**128,
        ),
        replace(
            corpus_record(4, share_difficulty=2**63, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
            network_difficulty=2**128 - 1,
        ),
    )
    return WindowPipelineCase(
        name="difficulty-beyond-u128",
        why=(
            "numeric(78,0) width taken literally: share_difficulty 2^128+1 as"
            " the retained crossing row, network_difficulty at 2^128-1, 2^128"
            " and 10^78-1, window_weight and the retained total above 2^128"
            " -- outside the declared u128 widths, so a backend declaring"
            " them answers Unsupported here while the frozen bytes stay"
            " Python's; one that clamps at u128::MAX, saturating or not,"
            " renders different digits and fails -- see the module docstring"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=2**128 + 2**64,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _ntime_beyond_u32_case() -> WindowPipelineCase:
    anchor_ms = _REALISTIC_ANCHOR_MS
    # ntime is bigint in the ledger but a 32-bit header field and u32 in the
    # declared Rust domain: 2^32 (one above the width) and bigint's maximum
    # both lie outside it while every other field is ordinary, so this is
    # the one width the case exercises.
    records = (
        replace(
            corpus_record(1, share_difficulty=3, job_issued_at_ms=anchor_ms - 2, accepted_at_ms=anchor_ms - 1),
            ntime=2**32,
        ),
        replace(
            corpus_record(
                2,
                share_difficulty=4,
                job_issued_at_ms=anchor_ms - 1,
                accepted_at_ms=anchor_ms - 1,
                credit_policy="stale-grace",
            ),
            ntime=2**63 - 1,
        ),
        corpus_record(3, share_difficulty=5, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
    )
    return WindowPipelineCase(
        name="ntime-beyond-u32",
        why=(
            "ntime at 2^32 and 2^63-1 (bigint's maximum) on otherwise"
            " ordinary records at a realistic anchor: outside the declared"
            " u32 width alone, so a backend declaring it answers Unsupported"
            " here while the frozen bytes stay Python's full-digit rendering;"
            " one that truncates ntime to 32 bits renders different digits"
            " and fails"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=1_000,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=records,
    )


def _advance_exact_fit_expiry_case() -> WindowPipelineCase:
    anchor_ms = 100_000
    snapshot = tuple(
        corpus_record(share_seq, share_difficulty=4, job_issued_at_ms=anchor_ms - share_seq, accepted_at_ms=anchor_ms - share_seq)
        for share_seq in (1, 2, 3)
    )
    return WindowPipelineCase(
        name="advance-exact-fit-expiry",
        why=(
            "three records of difficulty 4 at weight 10 (all retained, total"
            " 12) plus one delta of difficulty 2: 14-4 = 10 >= 10 expires the"
            " head row exactly at the weight, retaining seqs 2..4 at total 10;"
            " an advance-side expiry using > instead of >= retains all four."
            " The crossing-row-exact-fit sibling pins the snapshot-side twin"
            " of this comparison; the advance loops are separate code in any"
            " re-implementation"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=snapshot,
        advances=(
            WindowPipelineAdvance(
                anchor_job_issued_at_ms=anchor_ms + 1,
                delta_records=(
                    corpus_record(4, share_difficulty=2, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms + 1),
                ),
            ),
        ),
    )


def _advance_delta_exceeds_window_case() -> WindowPipelineCase:
    anchor_ms = 110_000
    snapshot = tuple(
        corpus_record(share_seq, share_difficulty=3, job_issued_at_ms=anchor_ms - share_seq, accepted_at_ms=anchor_ms - share_seq)
        for share_seq in (1, 2, 3)
    )
    delta = tuple(
        corpus_record(share_seq, share_difficulty=5, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms + 1)
        for share_seq in (4, 5, 6, 7, 8)
    )
    return WindowPipelineCase(
        name="advance-delta-exceeds-window",
        why=(
            "weight 8, page_size 2, three retained records of difficulty 3 in"
            " pages [1,2],[3], then one advance of five records of difficulty"
            " 5 (25 alone outweighs the window): the delta fills page [3,4]"
            " and allocates [5,6],[7,8]; expiry must run across the appended"
            " pages too, expiring every old page and three of the five new"
            " rows to retain [7,8] at total 10. A backend that expires only"
            " among pre-existing pages -- the natural shape of a drop-prefix /"
            " append-suffix protocol -- retains four. touched_pages is 1: the"
            " page appended into counts although this same advance then"
            " expires it wholesale"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=8,
        page_size=2,
        snapshot_records=snapshot,
        advances=(WindowPipelineAdvance(anchor_job_issued_at_ms=anchor_ms + 1, delta_records=delta),),
    )


def _advance_partial_expiry_in_appended_page_case() -> WindowPipelineCase:
    anchor_ms = 120_000
    snapshot = tuple(
        corpus_record(share_seq, share_difficulty=3, job_issued_at_ms=anchor_ms - share_seq, accepted_at_ms=anchor_ms - share_seq)
        for share_seq in (1, 2, 3)
    )
    delta = tuple(
        corpus_record(share_seq, share_difficulty=difficulty, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms + 1)
        for share_seq, difficulty in zip((4, 5, 6, 7, 8), (5, 5, 5, 2, 9))
    )
    return WindowPipelineCase(
        name="advance-partial-expiry-in-appended-page",
        why=(
            "as advance-delta-exceeds-window but with delta difficulties"
            " (5,5,5,2,9): whole-page expiry stops at the freshly allocated"
            " page [7,8] (total 11) and the partial loop then expires seq 7"
            " inside it, retaining [8] at total 9; touched_pages stays 1"
            " because a freshly allocated append page never counts even when"
            " partial expiry lands in it"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=8,
        page_size=2,
        snapshot_records=snapshot,
        advances=(WindowPipelineAdvance(anchor_job_issued_at_ms=anchor_ms + 1, delta_records=delta),),
    )


def _advance_from_empty_window_case() -> WindowPipelineCase:
    anchor_ms = 130_000
    return WindowPipelineCase(
        name="advance-from-empty-window",
        why=(
            "an empty window (no pages, no last page to append into) advanced"
            " with a one-record delta: the record lands in a freshly allocated"
            " page, nothing expires, touched_pages is 0; a backend that"
            " unwraps the last page fails here"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=(),
        advances=(
            WindowPipelineAdvance(
                anchor_job_issued_at_ms=anchor_ms + 1,
                delta_records=(
                    corpus_record(1, share_difficulty=5, job_issued_at_ms=anchor_ms + 1, accepted_at_ms=anchor_ms + 1),
                ),
            ),
        ),
    )


def _advance_empty_delta_case() -> WindowPipelineCase:
    anchor_ms = 140_000
    return WindowPipelineCase(
        name="advance-empty-delta",
        why=(
            "one retained record advanced to a later anchor with an empty"
            " delta: bytes identical to the snapshot, stats all zero; a backend"
            " that allocates an empty page and frames a trailing separator, or"
            " re-runs expiry with a stale total, diverges"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=(
            corpus_record(1, share_difficulty=5, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
        ),
        advances=(WindowPipelineAdvance(anchor_job_issued_at_ms=anchor_ms + 1, delta_records=()),),
    )


def _advance_equal_anchor_case() -> WindowPipelineCase:
    anchor_ms = 150_000
    return WindowPipelineCase(
        name="advance-equal-anchor",
        why=(
            "an advance at the same anchor as the snapshot: the regression"
            " check is strictly <, so this is accepted (only an empty delta"
            " can be, since no record is both newly eligible and eligible at"
            " the same anchor); a backend rejecting on <= diverges"
        ),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=10,
        page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        snapshot_records=(
            corpus_record(1, share_difficulty=5, job_issued_at_ms=anchor_ms, accepted_at_ms=anchor_ms),
        ),
        advances=(WindowPipelineAdvance(anchor_job_issued_at_ms=anchor_ms, delta_records=()),),
    )


# --- rejection cases --------------------------------------------------------
#
# One case per condition the shipped pipeline refuses, each built to trigger
# exactly that condition and nothing earlier in the check order. The three
# from_full_snapshot duplicate/non-positive shapes are unreachable from the
# database (PK, UNIQUE, CHECK), but the in-memory ledger and the daemon
# protocol are not the database, so they are pinned too. What is pinned is
# the rejection itself, not the coordinator's recovery from it.

_REJECTION_ANCHOR_MS = 200_000


def _rejection_case(
    name: str,
    category: str,
    why: str,
    *,
    snapshot_records: tuple[AcceptedShareRecord, ...],
    window_weight: int = 100,
    page_size: int = DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    advances: tuple[WindowPipelineAdvance, ...] = (),
) -> WindowPipelineCase:
    return WindowPipelineCase(
        name=name,
        why=why,
        anchor_job_issued_at_ms=_REJECTION_ANCHOR_MS,
        window_weight=window_weight,
        page_size=page_size,
        snapshot_records=snapshot_records,
        advances=advances,
        expected_rejection=category,
    )


def _eligible(share_seq: int, *, share_difficulty: int = 1, share_id: str | None = None) -> AcceptedShareRecord:
    """A record eligible at the rejection anchor, at one ms per seq of age."""
    record = corpus_record(
        share_seq,
        share_difficulty=share_difficulty,
        job_issued_at_ms=_REJECTION_ANCHOR_MS - share_seq,
        accepted_at_ms=_REJECTION_ANCHOR_MS - share_seq,
    )
    return record if share_id is None else replace(record, share_id=share_id)


def _newly_eligible(share_seq: int, *, share_difficulty: int = 1, job_offset_ms: int = 1, accepted_offset_ms: int = 1) -> AcceptedShareRecord:
    """A delta record first eligible at anchor+1 (both stamps above the old anchor)."""
    return corpus_record(
        share_seq,
        share_difficulty=share_difficulty,
        job_issued_at_ms=_REJECTION_ANCHOR_MS + job_offset_ms,
        accepted_at_ms=_REJECTION_ANCHOR_MS + accepted_offset_ms,
    )


def _full_snapshot_rejection_cases() -> tuple[WindowPipelineCase, ...]:
    return (
        _rejection_case(
            "reject-duplicate-share-seq",
            "duplicate_share_seq",
            "two eligible records share share_seq 1 under distinct share_ids;"
            " the sorted-order duplicate check fires before the share_id one",
            snapshot_records=(_eligible(1), _eligible(1, share_difficulty=2, share_id="share-1-again")),
        ),
        _rejection_case(
            "reject-duplicate-share-id",
            "duplicate_share_id",
            "seqs 1 and 2 carry the same share_id; distinct seqs so only the"
            " share_id check can fire",
            snapshot_records=(_eligible(1), _eligible(2, share_id="share-1")),
        ),
        _rejection_case(
            "reject-non-positive-difficulty",
            "non_positive_difficulty",
            "one eligible record with share_difficulty 0 (the check runs on"
            " eligible records only, so it must be eligible to be refused)",
            snapshot_records=(_eligible(1, share_difficulty=0),),
        ),
        _rejection_case(
            "reject-non-positive-window-weight",
            "non_positive_window_weight",
            "window_weight 0 with one ordinary eligible record; refused before"
            " any record is examined",
            snapshot_records=(_eligible(1),),
            window_weight=0,
        ),
        _rejection_case(
            "reject-non-positive-page-size",
            "non_positive_page_size",
            "page_size 0 with one ordinary eligible record; refused before any"
            " record is examined",
            snapshot_records=(_eligible(1),),
            page_size=0,
        ),
    )


def _advance_rejection_cases() -> tuple[WindowPipelineCase, ...]:
    # Seqs 1 and 3 retained at the anchor; seq 2 is the gap a late-visible
    # row would fill.
    retained = (_eligible(1), _eligible(3))

    def advance(*delta: AcceptedShareRecord, anchor_offset_ms: int = 1) -> tuple[WindowPipelineAdvance, ...]:
        return (
            WindowPipelineAdvance(
                anchor_job_issued_at_ms=_REJECTION_ANCHOR_MS + anchor_offset_ms,
                delta_records=delta,
            ),
        )

    return (
        _rejection_case(
            "reject-anchor-regression",
            "anchor_regression",
            "an advance whose anchor is one ms earlier than the window's, with"
            " an empty delta: the anchor check fires before any record check",
            snapshot_records=retained,
            advances=advance(anchor_offset_ms=-1),
        ),
        _rejection_case(
            "reject-delta-non-positive-difficulty",
            "delta_non_positive_difficulty",
            "a newly eligible delta record of difficulty 0; the difficulty"
            " check is the first per-record check",
            snapshot_records=retained,
            advances=advance(_newly_eligible(4, share_difficulty=0)),
        ),
        _rejection_case(
            "reject-delta-ineligible-job-only",
            "delta_ineligible_at_anchor",
            "a delta record whose job_issued_at_ms is one ms past the new"
            " anchor while accepted_at_ms is on it: ineligible by job alone",
            snapshot_records=retained,
            advances=advance(_newly_eligible(4, job_offset_ms=2, accepted_offset_ms=1)),
        ),
        _rejection_case(
            "reject-delta-ineligible-accepted-only",
            "delta_ineligible_at_anchor",
            "a delta record whose accepted_at_ms is one ms past the new anchor"
            " while job_issued_at_ms is on it: ineligible by accepted alone",
            snapshot_records=retained,
            advances=advance(_newly_eligible(4, job_offset_ms=1, accepted_offset_ms=2)),
        ),
        _rejection_case(
            "reject-delta-repeats-retained-share",
            "delta_repeats_eligible_share",
            "the delta re-delivers seq 3, a share eligible at the previous"
            " anchor and still inside the window: refused as a repeat before"
            " the append-order check could see its seq",
            snapshot_records=retained,
            advances=advance(_eligible(3)),
        ),
        _rejection_case(
            "reject-delta-not-append",
            "delta_not_append",
            "a newly eligible record with seq 2, below the window's last seq"
            " 3: the late-visible row that cannot be folded as an append and"
            " in production forces the full rescan",
            snapshot_records=retained,
            advances=advance(_newly_eligible(2)),
        ),
        _rejection_case(
            "reject-delta-order-not-increasing",
            "delta_order_not_increasing",
            "two newly eligible records delivered as seqs 5 then 4, both"
            " above the window's last seq so only the intra-delta order check"
            " can fire",
            snapshot_records=retained,
            advances=advance(_newly_eligible(5), _newly_eligible(4)),
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
        _wide_integers_case(),
        _difficulty_beyond_u128_case(),
        _ntime_beyond_u32_case(),
        _advance_exact_fit_expiry_case(),
        _advance_delta_exceeds_window_case(),
        _advance_partial_expiry_in_appended_page_case(),
        _advance_from_empty_window_case(),
        _advance_empty_delta_case(),
        _advance_equal_anchor_case(),
        *_full_snapshot_rejection_cases(),
        *_advance_rejection_cases(),
    )


# --- frozen reference -------------------------------------------------------


def reference_case_entry(case: WindowPipelineCase, result: WindowPipelineResult) -> dict[str, object]:
    entry: dict[str, object] = {
        "why": case.why,
        "input_sha256": case_input_sha256(case),
    }
    literals: dict[str, object] = {}
    if len(case_input_bytes(case)) <= LITERAL_PIN_MAX_CANONICAL_BYTES:
        literals["input"] = case_input_document(case)
    if isinstance(result, WindowPipelineUnsupported):
        raise RuntimeError(
            f"case {case.name}: the shipped pipeline declared the case"
            f" unsupported ({result.reason}); the reference freezes only the"
            " shipped pipeline's outputs or rejections"
        )
    if isinstance(result, WindowPipelineRejection):
        entry["rejected"] = result.reason
    else:
        outputs = result
        stream = record_stream_bytes(outputs.record_jsons)
        if outputs.canonical_digest != sha256_hex(outputs.canonical_bytes):
            raise RuntimeError(
                f"case {case.name}: canonical digest does not hash the framed"
                " canonical bytes; the streaming model no longer matches the"
                " shipped pipeline"
            )
        entry.update(
            {
                "record_count": len(outputs.record_jsons),
                "record_stream_len": len(stream),
                "record_stream_sha256": sha256_hex(stream),
                "canonical_bytes_len": len(outputs.canonical_bytes),
                "canonical_bytes_sha256": sha256_hex(outputs.canonical_bytes),
                "canonical_digest": outputs.canonical_digest,
                "spool_tail_len": len(outputs.spool_tail),
                "spool_tail_sha256": sha256_hex(outputs.spool_tail),
                "advance_stats": [stats.as_document() for stats in outputs.advance_stats],
            }
        )
        if len(outputs.canonical_bytes) <= LITERAL_PIN_MAX_CANONICAL_BYTES:
            literals.update(
                {
                    "record_jsons": [record.decode("ascii") for record in outputs.record_jsons],
                    "canonical_bytes": outputs.canonical_bytes.decode("ascii"),
                    "spool_tail": outputs.spool_tail.decode("ascii"),
                }
            )
    if literals:
        entry["pinned_literals"] = literals
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
    document = json.loads(raw)
    if document.get("schema") != REFERENCE_SCHEMA:
        raise AssertionError(
            f"frozen reference {REFERENCE_FIXTURE_RELPATH} has schema"
            f" {document.get('schema')!r}, expected {REFERENCE_SCHEMA!r};"
            f" regenerate it with: {REGENERATE_COMMAND}"
        )
    return document


def diverging_outputs(
    reference_case: dict[str, object],
    result: WindowPipelineResult,
    *,
    domain_exclusion: str | None = None,
) -> list[str]:
    """Every way one backend's result diverges from one frozen case.

    An empty list is parity. A rejection matches only a frozen rejection of
    the same category; outputs match only a frozen outputs entry, where
    length and SHA-256 comparisons hold for every case, literal comparisons
    additionally localize the first divergence when the case pins literals,
    and the per-advance stats must agree exactly.

    ``domain_exclusion`` is why the case lies outside the backend's declared
    integer domain (:func:`domain_exclusion`), or None when it lies inside.
    An ``Unsupported`` result is parity exactly when the case is outside:
    the frozen reference keeps the shipped pipeline's result and the backend
    has declared, rather than mis-rendered, the input it cannot carry. A
    backend that answers ``Unsupported`` for a case inside its declared
    domain diverges, and a backend that produces bytes for a case outside
    its domain is held to the frozen bytes like any other.
    """
    expected_rejection = reference_case.get("rejected")
    if isinstance(result, WindowPipelineUnsupported):
        if domain_exclusion is None:
            return [
                f"unsupported: backend declared {result.reason!r} but the"
                " case is inside its declared integer domain"
            ]
        return []
    if isinstance(result, WindowPipelineRejection):
        if expected_rejection is None:
            return [
                f"rejected: backend rejected as {result.reason!r} but the"
                " frozen reference has outputs"
            ]
        if result.reason != expected_rejection:
            return [f"rejected: {result.reason!r} != frozen {expected_rejection!r}"]
        return []
    outputs = result
    if expected_rejection is not None:
        return [
            f"rejected: backend produced outputs but the frozen reference is"
            f" rejected as {expected_rejection!r}"
        ]

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
    literal_records = literals.get("record_jsons")
    if len(outputs.record_jsons) != reference_case["record_count"]:
        problems.append(
            f"record_count: {len(outputs.record_jsons)} != frozen {reference_case['record_count']}"
        )
    check_stream(
        "record_stream",
        record_stream_bytes(outputs.record_jsons),
        "\n".join(literal_records) if literal_records is not None else None,
    )
    if literal_records is not None:
        expected_records = [text.encode("ascii") for text in literal_records]
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
    actual_stats = [stats.as_document() for stats in outputs.advance_stats]
    if actual_stats != reference_case["advance_stats"]:
        problems.append(
            f"advance_stats: {actual_stats} != frozen {reference_case['advance_stats']}"
        )
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
