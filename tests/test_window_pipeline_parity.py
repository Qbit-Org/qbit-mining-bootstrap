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
whitespace -- and asserts the harness catches each one exactly where it
should and nowhere else.
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

            def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineOutputs:
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


class WindowPipelineParityOracleTests(unittest.TestCase):
    """The selected backend must reproduce every frozen corpus case."""

    corpus: tuple[parity.WindowPipelineCase, ...]
    cases: dict[str, parity.WindowPipelineCase]
    reference_cases: dict[str, dict[str, object]]
    outputs: dict[str, parity.WindowPipelineOutputs]

    @classmethod
    def setUpClass(cls) -> None:
        cls.corpus = parity.build_corpus()
        cls.cases = {case.name: case for case in cls.corpus}
        cls.reference_cases = parity.load_reference_document()["cases"]
        adapter = parity.resolve_adapter()
        cls.outputs = {case.name: adapter.run(case) for case in cls.corpus}

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
                self.assertEqual(parity.diverging_outputs(entry, self.outputs[name]), [])

    def test_outputs_are_internally_consistent(self) -> None:
        # The digest must hash exactly the framed canonical bytes, and the
        # page-fragment concatenation must equal the flat per-record encoding
        # -- the identity that makes page layout invisible on the wire.
        for name, outputs in self.outputs.items():
            with self.subTest(case=name):
                self.assertEqual(
                    parity.sha256_hex(outputs.canonical_bytes),
                    outputs.canonical_digest,
                )
                self.assertEqual(
                    b"[" + b",".join(outputs.record_jsons) + b"]",
                    outputs.canonical_bytes,
                )

    def test_incremental_advances_land_on_full_rebuild_bytes(self) -> None:
        # advance() folding deltas must produce the same three byte outputs
        # as from_full_snapshot over the union at the final anchor. The
        # rebuild side always runs the shipped Python pipeline, so a selected
        # non-default backend is compared against it here as well.
        python_adapter = parity.PythonWindowPipelineAdapter()
        advancing = [case for case in self.corpus if case.advances]
        self.assertGreaterEqual(len(advancing), 2)
        for case in advancing:
            with self.subTest(case=case.name):
                self.assertEqual(
                    self.outputs[case.name],
                    python_adapter.run(parity.union_rebuild_case(case)),
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

        non_ascii = reference["non-ascii-strings"]["pinned_literals"]["canonical_bytes"]
        for needle in ("\\ud83d\\ude80", "\\u00e9", "\\u0001", "\\t", "\\n", '\\"', "\\\\"):
            self.assertIn(needle, non_ascii)

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


class _FieldOrderPerturbedAdapter:
    """Encodes record keys in insertion order instead of sorted order."""

    name = "perturbed-field-order"

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineOutputs:
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

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineOutputs:
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

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineOutputs:
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

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineOutputs:
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

    def run(self, case: parity.WindowPipelineCase) -> parity.WindowPipelineOutputs:
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
        self.assertTrue(any("credit_policy" in problem for problem in problems))

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


if __name__ == "__main__":
    unittest.main()
