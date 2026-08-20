#!/usr/bin/env python3
"""Hold the window pipeline to its frozen byte-exact reference.

Why these tests exist: a second implementation of the payout-window pipeline
can only be adopted if it is byte-identical to the shipped Python one, and
that comparison is only trustworthy if (a) the shipped pipeline reproduces a
frozen reference committed at a known revision, (b) the corpus demonstrably
exercises the fold's edge cases rather than just its bulk behavior, and
(c) the harness has been *seen to fail* on known divergence classes. The
first two are the parity tests below; the third is the divergence-detection
suite, which drives deliberately perturbed backends -- wrong field order,
wrong digest framing, null-instead-of-absent, a dropped crossing row, spool
whitespace, narrow or saturating integers, a fold that never refuses or
misfiles its refusals, advance expiry with the wrong comparison or confined
to pre-existing pages, an equal-anchor rejection, a raw DEL or an escaped
solidus, and advance stats derived from the wrong thing -- and asserts the
harness catches each one exactly where it should and nowhere else. Green
alone is not evidence here: every class from the integer-width one onward
was green against the original fourteen-case corpus, which could not see it.
"""

from __future__ import annotations

import json
import os
import unittest
from dataclasses import replace
from unittest import mock

from lab.prism.bundle_compiler import _compact_share_payload
from lab.prism.share_ledger import (
    DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    IncrementalShareWindow,
    IncrementalWindowFallback,
)
from tests import window_pipeline_parity as parity


class WindowPipelineAdapterRegistryTests(unittest.TestCase):
    def test_selection_defaults_to_the_shipped_python_pipeline(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ.pop(parity.ADAPTER_ENV_VAR, None)
            adapter = parity.resolve_adapter()
        self.assertEqual(adapter.name, "python")
        self.assertIsInstance(adapter, parity.PythonWindowPipelineAdapter)

    def test_environment_variable_selects_a_registered_backend(self) -> None:
        class _RegisteredBackend:
            name = "registry-selection-probe"

            def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
                raise NotImplementedError

        parity.register_adapter("registry-selection-probe", _RegisteredBackend)
        try:
            with mock.patch.dict(
                os.environ,
                {parity.ADAPTER_ENV_VAR: "registry-selection-probe"},
            ):
                self.assertIsInstance(parity.resolve_adapter(), _RegisteredBackend)
        finally:
            parity._ADAPTER_FACTORIES.pop("registry-selection-probe", None)

    def test_duplicate_registration_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "already registered"):
            parity.register_adapter("python", parity.PythonWindowPipelineAdapter)

    def test_unknown_backend_selection_is_a_clear_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "unknown window-pipeline parity adapter"
        ):
            parity.resolve_adapter("no-such-backend")

    def test_rejection_vocabulary_is_closed(self) -> None:
        # A backend cannot invent a category, and the vocabulary is exactly
        # the two entry points' condition lists: a rejection the shipped
        # pipeline cannot express cannot be frozen as parity either.
        with self.assertRaisesRegex(ValueError, "unknown window-pipeline rejection reason"):
            parity.WindowPipelineRejection("ValueError")
        self.assertEqual(
            parity.REJECTION_REASONS,
            parity.FULL_SNAPSHOT_REJECTIONS + parity.ADVANCE_REJECTIONS,
        )
        self.assertEqual(len(set(parity.REJECTION_REASONS)), 11)


class WindowPipelineParityOracleTests(unittest.TestCase):
    """The selected backend must reproduce every frozen corpus case."""

    corpus: tuple[parity.WindowPipelineCase, ...]
    cases: dict[str, parity.WindowPipelineCase]
    reference_cases: dict[str, dict[str, object]]
    results: dict[str, parity.WindowPipelineResult]

    @classmethod
    def setUpClass(cls) -> None:
        cls.corpus = parity.build_corpus()
        cls.cases = {case.name: case for case in cls.corpus}
        cls.reference_cases = parity.load_reference_document()["cases"]
        adapter = parity.resolve_adapter()
        cls.results = {case.name: adapter.run(case) for case in cls.corpus}

    def _outputs(self, name: str) -> parity.WindowPipelineOutputs:
        result = self.results[name]
        self.assertIsInstance(result, parity.WindowPipelineOutputs, name)
        assert isinstance(result, parity.WindowPipelineOutputs)
        return result

    def test_backend_reproduces_every_frozen_case(self) -> None:
        self.assertEqual(set(self.cases), set(self.reference_cases))
        for name, case in self.cases.items():
            with self.subTest(case=name):
                entry = self.reference_cases[name]
                self.assertEqual(
                    parity.case_input_sha256(case),
                    entry["input_sha256"],
                    "corpus inputs drifted from the ones the reference froze",
                )
                pinned_input = (entry.get("pinned_literals") or {}).get("input")
                if pinned_input is not None:
                    # The literal makes input drift legible in the fixture
                    # diff; here it also localizes which field moved.
                    self.assertEqual(parity.case_input_document(case), pinned_input)
                self.assertEqual(parity.diverging_outputs(entry, self.results[name]), [])

    def test_outputs_are_internally_consistent(self) -> None:
        # The digest must hash exactly the framed canonical bytes, and the
        # page-fragment concatenation must equal the flat per-record encoding
        # -- the identity that makes page layout invisible on the wire.
        for name, case in self.cases.items():
            if case.expected_rejection is not None:
                continue
            with self.subTest(case=name):
                outputs = self._outputs(name)
                self.assertEqual(
                    parity.sha256_hex(outputs.canonical_bytes),
                    outputs.canonical_digest,
                )
                self.assertEqual(
                    b"[" + b",".join(outputs.record_jsons) + b"]",
                    outputs.canonical_bytes,
                )
                self.assertEqual(len(outputs.advance_stats), len(case.advances))

    def test_incremental_advances_land_on_full_rebuild_bytes(self) -> None:
        # advance() folding deltas must produce the same byte outputs as
        # from_full_snapshot over the union at the final anchor. The rebuild
        # side always runs the shipped Python pipeline, so a selected
        # non-default backend is compared against it here as well. Only
        # accepted advances have a rebuild to land on: a rejected advance's
        # rebuild is the coordinator's recovery, which is deliberately not
        # pinned (see the module docstring).
        python_adapter = parity.PythonWindowPipelineAdapter()
        advancing = [
            case for case in self.corpus if case.advances and case.expected_rejection is None
        ]
        self.assertGreaterEqual(len(advancing), 8)
        for case in advancing:
            with self.subTest(case=case.name):
                rebuilt = python_adapter.run(parity.union_rebuild_case(case))
                self.assertEqual(
                    replace(self._outputs(case.name), advance_stats=()),
                    rebuilt,
                )

    def test_rejections_are_pinned_for_every_condition(self) -> None:
        # Every category in the vocabulary is frozen by at least one case,
        # each rejection case froze exactly the category it was built for
        # (so it cannot drift into passing for a different reason), and no
        # accepted case froze a rejection.
        pinned: dict[str, list[str]] = {}
        for name, case in self.cases.items():
            entry = self.reference_cases[name]
            with self.subTest(case=name):
                if case.expected_rejection is None:
                    self.assertNotIn("rejected", entry)
                    continue
                self.assertEqual(entry.get("rejected"), case.expected_rejection)
                self.assertNotIn("record_count", entry)
                self.assertEqual(self.results[name], parity.WindowPipelineRejection(case.expected_rejection))
                pinned.setdefault(case.expected_rejection, []).append(name)
        self.assertEqual(set(pinned), set(parity.REJECTION_REASONS))
        # Both eligibility exclusions are pinned separately, and the repeat
        # case repeats a share still inside the window.
        self.assertEqual(len(pinned["delta_ineligible_at_anchor"]), 2)
        repeat = self.cases["reject-delta-repeats-retained-share"]
        retained_seqs = {
            record.share_seq
            for record in parity.fold_case(replace(repeat, advances=())).records()
        }
        self.assertTrue(
            all(record.share_seq in retained_seqs for record in repeat.advances[0].delta_records)
        )

    def test_frozen_reference_regeneration_is_a_no_op(self) -> None:
        committed = parity.REFERENCE_FIXTURE_PATH.read_bytes()
        regenerated = parity.encode_reference_document(parity.reference_document())
        if regenerated != committed:
            self.fail(
                "the frozen reference no longer matches the shipped pipeline"
                " and corpus; if the byte contract changed deliberately,"
                f" regenerate it with: {parity.REGENERATE_COMMAND}"
            )

    def test_corpus_exercises_the_declared_edge_cases(self) -> None:
        # An accidentally hollow corpus would prove nothing, so pin the
        # structural properties each case exists for.
        self.assertEqual(DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE, 512)
        reference = self.reference_cases
        self.assertEqual(reference["empty-window"]["record_count"], 0)
        self.assertEqual(reference["single-record"]["record_count"], 1)
        for count in (511, 512, 513):
            self.assertEqual(reference[f"page-boundary-{count}"]["record_count"], count)
        self.assertEqual(
            len(parity.fold_case(self.cases["page-boundary-512"]).pages), 1
        )
        self.assertEqual(
            len(parity.fold_case(self.cases["page-boundary-513"]).pages), 2
        )

        multi_page_case = self.cases["multi-page-interior-cutoff"]
        multi_page = parity.fold_case(multi_page_case)
        self.assertGreaterEqual(len(multi_page.pages), 3)
        self.assertGreater(multi_page.total_difficulty, multi_page_case.window_weight)

        bulk = parity.fold_case(self.cases["bulk-seeded"])
        self.assertGreaterEqual(reference["bulk-seeded"]["record_count"], 2_500)
        self.assertGreaterEqual(len(bulk.pages), 4)
        self.assertEqual(len(self.cases["bulk-seeded"].advances), 1)
        self.assertGreaterEqual(len(self.cases["incremental-two-advances"].advances), 2)

        crossing = parity.fold_case(self.cases["crossing-row-retained"])
        self.assertGreater(
            crossing.total_difficulty,
            self.cases["crossing-row-retained"].window_weight,
        )
        exact_fit = parity.fold_case(self.cases["crossing-row-exact-fit"])
        self.assertEqual(
            exact_fit.total_difficulty,
            self.cases["crossing-row-exact-fit"].window_weight,
        )

        self.assertEqual(reference["eligibility-filtering"]["record_count"], 2)

        unsorted_case = self.cases["unsorted-input"]
        input_seqs = [record.share_seq for record in unsorted_case.snapshot_records]
        self.assertNotEqual(input_seqs, sorted(input_seqs))
        output_seqs = [
            record.share_seq for record in parity.fold_case(unsorted_case).records()
        ]
        self.assertEqual(output_seqs, sorted(input_seqs))

        # The escape alphabet: everything Python's ensure_ascii encoder
        # treats specially, plus the printable characters that must pass
        # through raw (solidus, space, uppercase). DEL is the separating
        # case -- escaped by Python, raw in most other encoders -- so it is
        # asserted both escaped and never raw, in the spool identities too.
        non_ascii = reference["non-ascii-strings"]["pinned_literals"]["canonical_bytes"]
        for needle in (
            "\\ud83d\\ude80", "\\u00e9", "\\u0001", "\\t", "\\n", '\\"', "\\\\",
            "\\u007f", "\\r", "\\b", "\\f", "\\u001f", "\\u2028", "\\u2029",
            "MINER-A/worker 1",
        ):
            self.assertIn(needle, non_ascii)
        self.assertNotIn("\\/", non_ascii)
        self.assertNotIn("\x7f", non_ascii)
        non_ascii_spool = reference["non-ascii-strings"]["pinned_literals"]["spool_tail"]
        self.assertIn("MINER-A/worker 1\\u007f", non_ascii_spool)
        self.assertNotIn("\x7f", non_ascii_spool)

        # Null-vs-absent: prism JSON omits an absent credit_policy while the
        # compact share tuple carries an explicit null.
        single = reference["single-record"]["pinned_literals"]
        self.assertNotIn("credit_policy", single["canonical_bytes"])
        self.assertIn(",null]", single["spool_tail"])

        reuse_tail = reference["credit-policy-identity-reuse"]["pinned_literals"]["spool_tail"]
        spool = json.loads("{" + reuse_tail[1:])
        identities = spool["compact_share_identities"]
        compact_shares = spool["compact_shares"]
        self.assertLess(len(identities), len(compact_shares))
        self.assertEqual(
            sorted({row[2] for row in compact_shares}),
            list(range(len(identities))),
        )

        # Integer width: the case cannot be quietly shrunk back under any
        # fixed width, and the retention cutoff genuinely depends on bits
        # above 2^64 (the three newest rows fall short of the weight only
        # because of its 2^127 term; truncated to 64 bits they would not).
        wide_case = self.cases["wide-integers"]
        wide_inputs = wide_case.snapshot_records
        self.assertTrue(any(record.share_difficulty.bit_length() > 64 for record in wide_inputs))
        self.assertTrue(any(record.network_difficulty.bit_length() > 128 for record in wide_inputs))
        self.assertTrue(any(record.share_seq.bit_length() > 32 for record in wide_inputs))
        self.assertTrue(any(record.template_height.bit_length() > 32 for record in wide_inputs))
        self.assertTrue(any(record.ntime.bit_length() > 32 for record in wide_inputs))
        self.assertTrue(any(record.template_height == 0 and record.ntime == 0 for record in wide_inputs))
        self.assertGreater(wide_case.anchor_job_issued_at_ms.bit_length(), 40)
        self.assertGreater(wide_case.window_weight.bit_length(), 64)
        self.assertEqual(reference["wide-integers"]["record_count"], 4)
        wide = parity.fold_case(wide_case)
        self.assertGreater(wide.total_difficulty, wide_case.window_weight)
        newest_three = sum(record.share_difficulty for record in wide.records()[1:])
        self.assertLess(newest_three, wide_case.window_weight)
        self.assertGreaterEqual(newest_three % 2**64, wide_case.window_weight % 2**64)
        wide_literal = reference["wide-integers"]["pinned_literals"]["canonical_bytes"]
        for digits in (2**127 + 3, 10**77, 2**63 - 1, 2**53 + 1):
            self.assertIn(str(digits), wide_literal)

        beyond_case = self.cases["difficulty-beyond-u128"]
        self.assertTrue(
            any(record.share_difficulty.bit_length() > 128 for record in beyond_case.snapshot_records)
        )
        self.assertGreater(beyond_case.window_weight.bit_length(), 128)
        self.assertEqual(reference["difficulty-beyond-u128"]["record_count"], 3)
        beyond = parity.fold_case(beyond_case)
        self.assertGreater(beyond.total_difficulty.bit_length(), 128)
        self.assertEqual(beyond.records()[0].share_difficulty, 2**128 + 1)
        beyond_literal = reference["difficulty-beyond-u128"]["pinned_literals"]["canonical_bytes"]
        for digits in (2**128 + 1, 2**128, 2**128 - 1, 10**78 - 1):
            self.assertIn(str(digits), beyond_literal)

        # Advance-path shapes and their stats, pinned at the values the
        # shipped fold reports so touched_pages' subtle definition cannot be
        # re-frozen to a plausible wrong one without a visible diff here.
        exact_case = self.cases["advance-exact-fit-expiry"]
        self.assertEqual(reference["advance-exact-fit-expiry"]["record_count"], 3)
        self.assertEqual(parity.fold_case(exact_case).total_difficulty, exact_case.window_weight)
        self.assertEqual(
            reference["advance-exact-fit-expiry"]["advance_stats"],
            [{"added_rows": 1, "expired_rows": 1, "touched_pages": 1}],
        )
        exceeds_case = self.cases["advance-delta-exceeds-window"]
        self.assertGreater(
            sum(record.share_difficulty for record in exceeds_case.advances[0].delta_records),
            exceeds_case.window_weight,
        )
        self.assertEqual(reference["advance-delta-exceeds-window"]["record_count"], 2)
        self.assertEqual(
            reference["advance-delta-exceeds-window"]["advance_stats"],
            [{"added_rows": 5, "expired_rows": 6, "touched_pages": 1}],
        )
        self.assertEqual(reference["advance-partial-expiry-in-appended-page"]["record_count"], 1)
        self.assertEqual(
            reference["advance-partial-expiry-in-appended-page"]["advance_stats"],
            [{"added_rows": 5, "expired_rows": 7, "touched_pages": 1}],
        )
        self.assertEqual(self.cases["advance-from-empty-window"].snapshot_records, ())
        self.assertEqual(reference["advance-from-empty-window"]["record_count"], 1)
        self.assertEqual(
            reference["advance-from-empty-window"]["advance_stats"],
            [{"added_rows": 1, "expired_rows": 0, "touched_pages": 0}],
        )
        for name in ("advance-empty-delta", "advance-equal-anchor"):
            case = self.cases[name]
            self.assertEqual(case.advances[0].delta_records, ())
            self.assertEqual(
                reference[name]["advance_stats"],
                [{"added_rows": 0, "expired_rows": 0, "touched_pages": 0}],
            )
            # Bytes identical to the snapshot alone: the advance is a no-op.
            self.assertEqual(
                reference[name]["canonical_bytes_sha256"],
                parity.sha256_hex(parity.outputs_from_window(parity.fold_case(replace(case, advances=()))).canonical_bytes),
            )
        equal_anchor = self.cases["advance-equal-anchor"]
        self.assertEqual(
            equal_anchor.advances[0].anchor_job_issued_at_ms,
            equal_anchor.anchor_job_issued_at_ms,
        )
        self.assertGreater(
            self.cases["advance-empty-delta"].advances[0].anchor_job_issued_at_ms,
            self.cases["advance-empty-delta"].anchor_job_issued_at_ms,
        )


# --- perturbed backends -----------------------------------------------------


def _window_holding(
    records: tuple[parity.AcceptedShareRecord, ...],
    case: parity.WindowPipelineCase,
    anchor_ms: int,
) -> IncrementalShareWindow:
    """A window retaining exactly these records, with no cutoff applied."""
    window = IncrementalShareWindow.from_full_snapshot(
        list(records),
        anchor_job_issued_at_ms=anchor_ms,
        window_weight=sum(int(record.share_difficulty) for record in records) + 1,
        page_size=case.page_size,
    )
    return replace(window, window_weight=case.window_weight)


_INTEGER_RECORD_FIELDS = (
    "share_seq",
    "share_difficulty",
    "network_difficulty",
    "template_height",
    "job_issued_at_ms",
    "accepted_at_ms",
    "ntime",
)


def _map_case_integers(
    case: parity.WindowPipelineCase,
    transform,
    *,
    record_fields: tuple[str, ...] = _INTEGER_RECORD_FIELDS,
) -> parity.WindowPipelineCase:
    """The case as a backend carrying its integers in some narrower type sees it."""

    def mapped(record: parity.AcceptedShareRecord) -> parity.AcceptedShareRecord:
        return replace(record, **{name: transform(getattr(record, name)) for name in record_fields})

    return replace(
        case,
        anchor_job_issued_at_ms=transform(case.anchor_job_issued_at_ms),
        window_weight=transform(case.window_weight),
        snapshot_records=tuple(mapped(record) for record in case.snapshot_records),
        advances=tuple(
            replace(
                step,
                anchor_job_issued_at_ms=transform(step.anchor_job_issued_at_ms),
                delta_records=tuple(mapped(record) for record in step.delta_records),
            )
            for step in case.advances
        ),
    )


def _reencoded(result: parity.WindowPipelineResult, old: bytes, new: bytes) -> parity.WindowPipelineResult:
    """The backend's native byte outputs re-encoded with one escape rule changed."""
    if isinstance(result, parity.WindowPipelineRejection):
        return result
    record_jsons = tuple(record.replace(old, new) for record in result.record_jsons)
    canonical_bytes = result.canonical_bytes.replace(old, new)
    return replace(
        result,
        record_jsons=record_jsons,
        canonical_bytes=canonical_bytes,
        canonical_digest=parity.sha256_hex(canonical_bytes),
    )


class _FieldOrderPerturbedAdapter:
    """Encodes record keys in insertion order instead of sorted order."""

    name = "perturbed-field-order"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        window = parity.fold_case(case)
        base = parity.outputs_from_window(window)
        record_jsons = tuple(
            json.dumps(record, sort_keys=False, separators=(",", ":"), default=str).encode()
            for record in window.json_records()
        )
        canonical_bytes = b"[" + b",".join(record_jsons) + b"]"
        return replace(
            base,
            record_jsons=record_jsons,
            canonical_bytes=canonical_bytes,
            canonical_digest=parity.sha256_hex(canonical_bytes),
        )


class _MissingPageSeparatorAdapter:
    """Streams the digest without the comma between page fragments."""

    name = "perturbed-missing-page-separator"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        window = parity.fold_case(case)
        base = parity.outputs_from_window(window)
        fragments = [
            page.canonical_json_items
            for page in window.pages
            if page.canonical_json_items
        ]
        return replace(
            base,
            canonical_digest=parity.sha256_hex(b"[" + b"".join(fragments) + b"]"),
        )


class _NullCreditPolicyAdapter:
    """Emits credit_policy as an explicit null instead of omitting the key."""

    name = "perturbed-null-credit-policy"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        window = parity.fold_case(case)
        base = parity.outputs_from_window(window)
        record_jsons = tuple(
            parity.canonical_json_bytes({**record, "credit_policy": record.get("credit_policy")})
            for record in window.json_records()
        )
        canonical_bytes = b"[" + b",".join(record_jsons) + b"]"
        return replace(
            base,
            record_jsons=record_jsons,
            canonical_bytes=canonical_bytes,
            canonical_digest=parity.sha256_hex(canonical_bytes),
        )


class _CrossingRowDroppedAdapter:
    """Applies a wrong strict cutoff: the crossing row is dropped."""

    name = "perturbed-crossing-row-dropped"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        window = parity.fold_case(case)
        records = window.records()
        if records and window.total_difficulty > case.window_weight:
            records = records[1:]
        rebuilt = IncrementalShareWindow.from_full_snapshot(
            list(records),
            anchor_job_issued_at_ms=parity.final_anchor_job_issued_at_ms(case),
            window_weight=case.window_weight,
            page_size=case.page_size,
        )
        return parity.outputs_from_window(rebuilt)


class _SpoolWhitespaceAdapter:
    """Writes the spool fragments with json.dumps's default spaced separators."""

    name = "perturbed-spool-whitespace"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        window = parity.fold_case(case)
        base = parity.outputs_from_window(window)
        identities, compact_shares = _compact_share_payload(list(window.json_records()))
        spool_tail = (
            b',"compact_share_identities":'
            + json.dumps(identities).encode()
            + b',"compact_shares":'
            + json.dumps(compact_shares).encode()
            + b"}"
        )
        return replace(base, spool_tail=spool_tail)


class _WrappingU64Adapter:
    """Carries every integer in a wrapping unsigned 64-bit type."""

    name = "perturbed-u64-wrap"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        return parity.run_python_pipeline(_map_case_integers(case, lambda value: value % 2**64))


class _SaturatingU128DifficultyAdapter:
    """Clamps share_difficulty and window_weight at u128::MAX: a saturating u128 accumulator."""

    name = "perturbed-u128-saturating-difficulty"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        return parity.run_python_pipeline(
            _map_case_integers(
                case,
                lambda value: min(value, 2**128 - 1),
                record_fields=("share_difficulty",),
            )
        )


class _NeverRefusingAdapter:
    """Folds whatever it is given: where the shipped fold refuses, it recovers by itself.

    Models a backend with every check removed, or one that performs the
    coordinator's full-rescan recovery internally and returns its bytes as
    if nothing had happened -- the bytes a lenient fold or the recovery
    would produce are exactly what the oracle must not accept as parity.
    """

    name = "perturbed-never-refusing"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        result = parity.run_python_pipeline(case)
        if isinstance(result, parity.WindowPipelineOutputs):
            return result
        union = case.snapshot_records + tuple(
            record for step in case.advances for record in step.delta_records
        )
        seen_seqs: set[int] = set()
        seen_ids: set[str] = set()
        kept = []
        for record in union:
            if (
                record.share_seq in seen_seqs
                or record.share_id in seen_ids
                or record.share_difficulty <= 0
            ):
                continue
            seen_seqs.add(record.share_seq)
            seen_ids.add(record.share_id)
            kept.append(record)
        folded = IncrementalShareWindow.from_full_snapshot(
            kept,
            anchor_job_issued_at_ms=parity.final_anchor_job_issued_at_ms(case),
            window_weight=max(case.window_weight, 1),
            page_size=max(case.page_size, 1),
        )
        return parity.outputs_from_window(
            folded,
            tuple(
                parity.WindowPipelineAdvanceStats(len(step.delta_records), 0, 0)
                for step in case.advances
            ),
        )


class _MisfiledRejectionAdapter:
    """Refuses where the shipped fold does, but files every refusal under one category."""

    name = "perturbed-misfiled-rejection"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        result = parity.run_python_pipeline(case)
        if isinstance(result, parity.WindowPipelineRejection):
            return parity.WindowPipelineRejection("anchor_regression")
        return result


class _StrictAdvanceExpiryAdapter:
    """Expires on > instead of >= in the advance loops (modelled as weight+1 while advancing)."""

    name = "perturbed-strict-advance-expiry"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        try:
            window = IncrementalShareWindow.from_full_snapshot(
                list(case.snapshot_records),
                anchor_job_issued_at_ms=case.anchor_job_issued_at_ms,
                window_weight=case.window_weight,
                page_size=case.page_size,
            )
            stats = []
            for step in case.advances:
                window, step_stats = replace(window, window_weight=case.window_weight + 1).advance(
                    list(step.delta_records),
                    anchor_job_issued_at_ms=step.anchor_job_issued_at_ms,
                )
                stats.append(parity.WindowPipelineAdvanceStats.from_shipped(step_stats))
        except (ValueError, IncrementalWindowFallback):
            return parity.run_python_pipeline(case)
        return parity.outputs_from_window(replace(window, window_weight=case.window_weight), tuple(stats))


class _OldPagesOnlyExpiryAdapter:
    """Expires only among pre-existing rows; the appended delta is immune.

    The natural shape of a drop-prefix / append-suffix protocol: when the
    shipped fold exhausts every old row and goes on into the delta, this
    backend stops at the old rows and keeps the delta whole.
    """

    name = "perturbed-old-pages-only-expiry"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        try:
            window = IncrementalShareWindow.from_full_snapshot(
                list(case.snapshot_records),
                anchor_job_issued_at_ms=case.anchor_job_issued_at_ms,
                window_weight=case.window_weight,
                page_size=case.page_size,
            )
            stats = []
            for step in case.advances:
                advanced, step_stats = window.advance(
                    list(step.delta_records),
                    anchor_job_issued_at_ms=step.anchor_job_issued_at_ms,
                )
                retained_seqs = {record.share_seq for record in advanced.records()}
                if all(record.share_seq in retained_seqs for record in step.delta_records):
                    window = advanced
                else:
                    step_stats = replace(step_stats, expired_rows=len(window.records()))
                    window = _window_holding(step.delta_records, case, step.anchor_job_issued_at_ms)
                stats.append(parity.WindowPipelineAdvanceStats.from_shipped(step_stats))
        except (ValueError, IncrementalWindowFallback):
            return parity.run_python_pipeline(case)
        return parity.outputs_from_window(window, tuple(stats))


class _EqualAnchorRejectingAdapter:
    """Treats an advance at the same anchor as a regression (<= instead of <)."""

    name = "perturbed-equal-anchor-rejecting"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        anchor_ms = case.anchor_job_issued_at_ms
        for step in case.advances:
            if step.anchor_job_issued_at_ms == anchor_ms:
                return parity.WindowPipelineRejection("anchor_regression")
            anchor_ms = step.anchor_job_issued_at_ms
        return parity.run_python_pipeline(case)


class _RawDelAdapter:
    """An encoder that escapes only below 0x20 plus quote and backslash: DEL stays raw."""

    name = "perturbed-raw-del"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        return _reencoded(parity.run_python_pipeline(case), b"\\u007f", b"\x7f")


class _EscapedSolidusAdapter:
    """An encoder that emits the optional JSON escape for solidus."""

    name = "perturbed-escaped-solidus"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        return _reencoded(parity.run_python_pipeline(case), b"/", b"\\/")


class _NoAdvanceStatsAdapter:
    """Produces the bytes but reports no per-advance stats."""

    name = "perturbed-no-advance-stats"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        result = parity.run_python_pipeline(case)
        if isinstance(result, parity.WindowPipelineRejection):
            return result
        return replace(result, advance_stats=())


class _SurvivorTouchedPagesAdapter:
    """Derives touched_pages from which pre-existing pages survive, not from the work done.

    Counts a pre-existing page when it is partially retained, and the
    append-into page only if any of its rows survive -- so a page appended
    into and then expired wholesale in the same advance is not counted, where
    the shipped fold counts it because its records were rewritten.
    """

    name = "perturbed-survivor-touched-pages"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineResult:
        try:
            window = IncrementalShareWindow.from_full_snapshot(
                list(case.snapshot_records),
                anchor_job_issued_at_ms=case.anchor_job_issued_at_ms,
                window_weight=case.window_weight,
                page_size=case.page_size,
            )
            stats = []
            for step in case.advances:
                advanced, step_stats = window.advance(
                    list(step.delta_records),
                    anchor_job_issued_at_ms=step.anchor_job_issued_at_ms,
                )
                retained_seqs = {record.share_seq for record in advanced.records()}
                touched: set[int] = set()
                last = window.pages[-1] if window.pages else None
                if step.delta_records and last is not None and len(last.records) < case.page_size:
                    if any(record.share_seq in retained_seqs for record in last.records):
                        touched.add(len(window.pages) - 1)
                for index, page in enumerate(window.pages):
                    surviving = sum(1 for record in page.records if record.share_seq in retained_seqs)
                    if 0 < surviving < len(page.records):
                        touched.add(index)
                stats.append(
                    parity.WindowPipelineAdvanceStats(
                        added_rows=step_stats.added_rows,
                        expired_rows=step_stats.expired_rows,
                        touched_pages=len(touched),
                    )
                )
                window = advanced
        except (ValueError, IncrementalWindowFallback):
            return parity.run_python_pipeline(case)
        return parity.outputs_from_window(window, tuple(stats))


class WindowPipelineDivergenceDetectionTests(unittest.TestCase):
    """Prove the harness fails on known divergence classes, and only there."""

    cases: dict[str, parity.WindowPipelineCase]
    reference_cases: dict[str, dict[str, object]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.cases = {case.name: case for case in parity.build_corpus()}
        cls.reference_cases = parity.load_reference_document()["cases"]

    def _diverging(self, adapter: parity.WindowPipelineAdapter, case_name: str) -> list[str]:
        return parity.diverging_outputs(
            self.reference_cases[case_name],
            adapter.run(self.cases[case_name]),
        )

    def test_record_field_reordering_is_detected(self) -> None:
        problems = self._diverging(_FieldOrderPerturbedAdapter(), "single-record")
        self.assertTrue(problems)
        self.assertTrue(any(problem.startswith("record_jsons[0]") for problem in problems))

    def test_missing_page_separator_is_detected_only_on_multi_page_windows(self) -> None:
        adapter = _MissingPageSeparatorAdapter()
        problems = self._diverging(adapter, "page-boundary-513")
        self.assertTrue(problems)
        self.assertTrue(all(problem.startswith("canonical_digest") for problem in problems))
        # On a single page the missing separator never fires, so this framing
        # bug is invisible there -- the reason multi-page corpora must exist.
        self.assertEqual(self._diverging(adapter, "page-boundary-512"), [])

    def test_null_credit_policy_instead_of_absent_is_detected(self) -> None:
        problems = self._diverging(_NullCreditPolicyAdapter(), "single-record")
        self.assertTrue(problems)
        self.assertTrue(any(problem.startswith("record_jsons[0]") for problem in problems))

    def test_dropped_crossing_row_is_detected_only_where_one_exists(self) -> None:
        adapter = _CrossingRowDroppedAdapter()
        problems = self._diverging(adapter, "crossing-row-retained")
        self.assertTrue(any(problem.startswith("record_count") for problem in problems))
        # The exact-fit sibling has no crossing row, so the wrong cutoff
        # passes there -- the pair is what localizes the off-by-one.
        self.assertEqual(self._diverging(adapter, "crossing-row-exact-fit"), [])

    def test_spool_separator_whitespace_is_detected(self) -> None:
        problems = self._diverging(_SpoolWhitespaceAdapter(), "credit-policy-identity-reuse")
        self.assertTrue(problems)
        self.assertTrue(all(problem.startswith("spool_tail") for problem in problems))

    def test_wrapping_64_bit_integers_are_detected_only_above_64_bits(self) -> None:
        adapter = _WrappingU64Adapter()
        # Truncating window_weight and the difficulties stops retention one
        # row early, so the count itself diverges, not only the digits.
        problems = self._diverging(adapter, "wide-integers")
        self.assertTrue(any(problem.startswith("record_count") for problem in problems))
        self.assertTrue(self._diverging(adapter, "difficulty-beyond-u128"))
        # Every other case lives below 2^31, where a 64-bit backend is exact:
        # the width holes were invisible until the wide cases existed.
        for name in ("single-record", "incremental-two-advances", "bulk-seeded"):
            self.assertEqual(self._diverging(adapter, name), [])

    def test_saturating_u128_difficulty_is_refused_by_the_beyond_u128_case_alone(self) -> None:
        # The documented decision: 128-bit difficulty accumulation passes
        # wide-integers and is refused only where numeric(78,0) exceeds it,
        # by the clamped crossing row's digits (retained count still agrees).
        adapter = _SaturatingU128DifficultyAdapter()
        self.assertEqual(self._diverging(adapter, "wide-integers"), [])
        problems = self._diverging(adapter, "difficulty-beyond-u128")
        self.assertTrue(any(problem.startswith("record_jsons[0]") for problem in problems))
        self.assertFalse(any(problem.startswith("record_count") for problem in problems))

    def test_a_backend_that_never_refuses_is_detected_on_every_rejection_case(self) -> None:
        adapter = _NeverRefusingAdapter()
        rejection_cases = [case for case in self.cases.values() if case.expected_rejection]
        self.assertEqual(len(rejection_cases), 12)
        for case in rejection_cases:
            with self.subTest(case=case.name):
                problems = self._diverging(adapter, case.name)
                self.assertEqual(len(problems), 1)
                self.assertTrue(problems[0].startswith("rejected: backend produced outputs"))
        # Where the shipped fold accepts, the lenient backend is identical.
        self.assertEqual(self._diverging(adapter, "incremental-two-advances"), [])

    def test_misfiled_rejection_category_is_detected(self) -> None:
        adapter = _MisfiledRejectionAdapter()
        self.assertEqual(self._diverging(adapter, "reject-anchor-regression"), [])
        self.assertEqual(
            self._diverging(adapter, "reject-delta-not-append"),
            ["rejected: 'anchor_regression' != frozen 'delta_not_append'"],
        )

    def test_strict_advance_expiry_is_detected_only_at_exact_fit(self) -> None:
        adapter = _StrictAdvanceExpiryAdapter()
        problems = self._diverging(adapter, "advance-exact-fit-expiry")
        self.assertTrue(any(problem.startswith("record_count") for problem in problems))
        # No other advance in the corpus lands an expiry exactly on the
        # weight, and the snapshot-side twin is separate code.
        for name in ("incremental-two-advances", "bulk-seeded", "advance-delta-exceeds-window", "crossing-row-exact-fit"):
            self.assertEqual(self._diverging(adapter, name), [])

    def test_expiry_confined_to_old_pages_is_detected_only_when_the_delta_outweighs_the_window(self) -> None:
        adapter = _OldPagesOnlyExpiryAdapter()
        for name in ("advance-delta-exceeds-window", "advance-partial-expiry-in-appended-page"):
            with self.subTest(case=name):
                problems = self._diverging(adapter, name)
                self.assertTrue(any(problem.startswith("record_count") for problem in problems))
        for name in ("incremental-two-advances", "bulk-seeded", "advance-exact-fit-expiry"):
            self.assertEqual(self._diverging(adapter, name), [])

    def test_equal_anchor_rejection_is_detected(self) -> None:
        adapter = _EqualAnchorRejectingAdapter()
        self.assertEqual(
            self._diverging(adapter, "advance-equal-anchor"),
            ["rejected: backend rejected as 'anchor_regression' but the frozen reference has outputs"],
        )
        self.assertEqual(self._diverging(adapter, "reject-anchor-regression"), [])
        self.assertEqual(self._diverging(adapter, "advance-empty-delta"), [])

    def test_raw_del_and_escaped_solidus_are_detected(self) -> None:
        for adapter in (_RawDelAdapter(), _EscapedSolidusAdapter()):
            with self.subTest(adapter=adapter.name):
                problems = self._diverging(adapter, "non-ascii-strings")
                self.assertTrue(any(problem.startswith("record_jsons[3]") for problem in problems))
                self.assertEqual(self._diverging(adapter, "single-record"), [])

    def test_missing_advance_stats_are_detected_on_advancing_cases(self) -> None:
        adapter = _NoAdvanceStatsAdapter()
        problems = self._diverging(adapter, "advance-from-empty-window")
        self.assertEqual(len(problems), 1)
        self.assertTrue(problems[0].startswith("advance_stats"))
        self.assertEqual(self._diverging(adapter, "single-record"), [])

    def test_touched_pages_from_survivors_is_detected_only_where_an_appended_page_expires(self) -> None:
        adapter = _SurvivorTouchedPagesAdapter()
        for name in ("advance-delta-exceeds-window", "advance-partial-expiry-in-appended-page"):
            with self.subTest(case=name):
                problems = self._diverging(adapter, name)
                self.assertTrue(problems)
                self.assertTrue(all(problem.startswith("advance_stats") for problem in problems))
        # Where the appended page survives (partially or whole), the two
        # definitions agree -- which is why those cases alone proved nothing.
        for name in ("advance-exact-fit-expiry", "incremental-two-advances", "advance-from-empty-window"):
            self.assertEqual(self._diverging(adapter, name), [])


if __name__ == "__main__":
    unittest.main()
