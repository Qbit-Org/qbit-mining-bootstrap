//! The frozen payout-window corpus (#269), shared by the native replay
//! (`window_frozen_vectors`) and the serve-daemon gate (`window_daemon_gate`).
//!
//! `tests/fixtures/window_pipeline_parity/reference.json` was generated on
//! 2.x.x by the Python differential oracle and is never edited here; its
//! five inputs that the oracle derived from a seeded `random.Random` live in
//! the `inputs-unpinned.json` sidecar beside it. Every input is checked
//! against its frozen `input_sha256` before any output is compared, and every
//! output comparison is against the frozen bytes, never a re-serialization.
//!
//! `supplementary.json` holds the few cases the corpus never reaches (a whole
//! page expiring at exactly `window_weight`), exported from the same 2.x.x
//! oracle with the same entry shape and replayed under their own tally.
#![allow(dead_code)]

use qbit_prism::window::{prepare_window_out_of_range, DeclaredWidth};
use qbit_prism::AcceptedShare;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fmt;

const REFERENCE_JSON: &str =
    include_str!("../../../../tests/fixtures/window_pipeline_parity/reference.json");
const UNPINNED_INPUTS_JSON: &str =
    include_str!("../../../../tests/fixtures/window_pipeline_parity/inputs-unpinned.json");
const SUPPLEMENTARY_JSON: &str =
    include_str!("../../../../tests/fixtures/window_pipeline_parity/supplementary.json");

/// Pin of the frozen corpus file. The unpinned-inputs sidecar needs no pin of
/// its own: each of its documents is pinned by its `input_sha256` in here.
pub const REFERENCE_SHA256: &str =
    "017c787d3b894d92702d65774e47224ce5a09838bcd1118549f740a21b406142";
/// Pin of the supplementary cases, whose entries carry their own inputs.
pub const SUPPLEMENTARY_SHA256: &str =
    "4a89967643938cfbec933b7f5582118e583245986eb2adf9e533d017ea252fc6";
const REFERENCE_SCHEMA: &str = "qbit-prism-window-pipeline-parity-reference/v2";
const INPUT_SCHEMA: &str = "qbit-prism-window-pipeline-parity-input/v1";
const UNPINNED_SCHEMA: &str = "qbit-prism-window-pipeline-parity-unpinned-inputs/v1";
const SUPPLEMENTARY_SCHEMA: &str = "qbit-prism-window-pipeline-parity-supplementary/v1";
/// The 2.x.x commit whose oracle exported both the sidecar and the
/// supplementary cases.
const ORACLE_SOURCE_COMMIT: &str = "504846cc0b72e8f86ed17f896d4ccbbe196a31dc";
const SUPPLEMENTARY_CASES: [&str; 4] = [
    "advance-page-expiry-at-weight",
    "advance-page-expiry-at-weight-second-page",
    "advance-page-expiry-one-above-weight",
    "advance-page-expiry-one-below-weight",
];
const UNPINNED_CASES: [&str; 5] = [
    "bulk-seeded",
    "multi-page-interior-cutoff",
    "page-boundary-511",
    "page-boundary-512",
    "page-boundary-513",
];

pub const FULL_REJECTIONS: [&str; 5] = [
    "duplicate_share_seq",
    "duplicate_share_id",
    "non_positive_difficulty",
    "non_positive_window_weight",
    "non_positive_page_size",
];
pub const ADVANCE_REJECTIONS: [&str; 6] = [
    "anchor_regression",
    "delta_non_positive_difficulty",
    "delta_ineligible_at_anchor",
    "delta_repeats_eligible_share",
    "delta_not_append",
    "delta_order_not_increasing",
];

/// A corpus case whose input leaves 3.x.x's declared integer widths (2.x.x's
/// `RUST_INTEGER_DOMAIN`): its frozen bytes came from Python's unbounded
/// integers and are not checkable here by design, so it is declined -- and
/// the decline itself is asserted.
pub struct OutOfDomain {
    pub case: &'static str,
    pub field: &'static str,
    pub literal: &'static str,
    pub width: DeclaredWidth,
}

pub const OUT_OF_DOMAIN: [OutOfDomain; 2] = [
    OutOfDomain {
        case: "difficulty-beyond-u128",
        field: "window_weight",
        // 2^128 + 2^64
        literal: "340282366920938463481821351505477763072",
        width: DeclaredWidth::U128,
    },
    OutOfDomain {
        case: "ntime-beyond-u32",
        field: "records[0].ntime",
        // 2^32
        literal: "4294967296",
        width: DeclaredWidth::U32,
    },
];

/// The counts both replays must reach.
pub const EXPECTED_TALLY: Tally = Tally {
    cases: 35,
    rejections: 12,
    byte_compared: 21,
    declined: 2,
    with_advance_stats: 8,
};

/// The counts both replays must reach on the supplementary cases.
pub const EXPECTED_SUPPLEMENTARY_TALLY: Tally = Tally {
    cases: 4,
    rejections: 0,
    byte_compared: 4,
    declined: 0,
    with_advance_stats: 4,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Full,
    Advance(usize),
}

impl fmt::Display for Phase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Phase::Full => formatter.write_str("full"),
            Phase::Advance(index) => write!(formatter, "advances[{index}]"),
        }
    }
}

pub enum Expectation {
    Outputs,
    Rejected { category: String, phase: Phase },
    Declined(&'static OutOfDomain),
}

/// One corpus case: its frozen reference entry and its input document.
pub struct Case {
    pub name: String,
    pub entry: Value,
    pub input: Value,
    pub input_from_sidecar: bool,
}

#[derive(Deserialize)]
pub struct TypedInput {
    pub window_weight: u128,
    pub page_size: usize,
    pub snapshot: TypedStep,
    pub advances: Vec<TypedStep>,
}

#[derive(Deserialize)]
pub struct TypedStep {
    pub anchor_job_issued_at_ms: i64,
    pub records: Vec<AcceptedShare>,
}

impl Case {
    pub fn advances(&self) -> &[Value] {
        self.input["advances"]
            .as_array()
            .expect("input advances is an array")
    }

    pub fn out_of_domain(&self) -> Option<&'static OutOfDomain> {
        OUT_OF_DOMAIN.iter().find(|entry| entry.case == self.name)
    }

    pub fn expectation(&self) -> Expectation {
        if let Some(declined) = self.out_of_domain() {
            return Expectation::Declined(declined);
        }
        match self.entry.get("rejected").and_then(Value::as_str) {
            None => Expectation::Outputs,
            Some(category) => {
                let phase = if FULL_REJECTIONS.contains(&category) {
                    Phase::Full
                } else {
                    // The frozen entry records only the category: the phase
                    // is derived from the input. `assert_corpus_shape` holds
                    // every advance rejection to exactly one advance, so the
                    // refused step is that one and the full fold must succeed.
                    Phase::Advance(self.advances().len() - 1)
                };
                Expectation::Rejected {
                    category: category.to_string(),
                    phase,
                }
            }
        }
    }

    /// The anchor of the case's final step, where its frozen window stands.
    pub fn final_anchor(&self) -> &Value {
        self.advances()
            .last()
            .map_or(&self.input["snapshot"]["anchor_job_issued_at_ms"], |step| {
                &step["anchor_job_issued_at_ms"]
            })
    }

    /// The spool tail bytes the frozen entry vouches for: the pinned literal,
    /// or else `computed` when it hashes to the frozen `spool_tail_sha256`.
    pub fn frozen_spool_tail(&self, computed: &[u8]) -> Option<Vec<u8>> {
        let literal = self
            .entry
            .get("pinned_literals")
            .and_then(|literals| literals.get("spool_tail"))
            .and_then(Value::as_str);
        match literal {
            Some(literal) => Some(literal.as_bytes().to_vec()),
            None => (self.entry["spool_tail_sha256"].as_str()
                == Some(sha256_hex(computed).as_str()))
            .then(|| computed.to_vec()),
        }
    }

    /// The typed parse of the input at 3.x.x's declared widths -- the same
    /// strict serde parse the daemon applies to its request line.
    pub fn typed(&self) -> Result<TypedInput, serde_json::Error> {
        serde_json::from_str(&python_json(&self.input))
    }

    /// The `full` prepare_window request exactly as the daemon receives it.
    pub fn full_request(&self) -> Value {
        json!({
            "request": "prepare_window",
            "mode": "full",
            "append_invalidation_epoch": 0,
            "anchor_job_issued_at_ms": self.input["snapshot"]["anchor_job_issued_at_ms"].clone(),
            "window_weight": self.input["window_weight"].clone(),
            "page_size": self.input["page_size"].clone(),
            "records": self.input["snapshot"]["records"].clone(),
        })
    }

    pub fn advance_request(&self, index: usize, base_digest: &str) -> Value {
        let step = &self.advances()[index];
        json!({
            "request": "prepare_window",
            "mode": "advance",
            "append_invalidation_epoch": 0,
            "anchor_job_issued_at_ms": step["anchor_job_issued_at_ms"].clone(),
            "base_digest": base_digest,
            "records": step["records"].clone(),
        })
    }
}

/// Load both files, check the pins and every input's `input_sha256`, and
/// assert the corpus shape, before any output is compared.
pub fn load() -> Vec<Case> {
    assert_eq!(
        sha256_hex(REFERENCE_JSON.as_bytes()),
        REFERENCE_SHA256,
        "tests/fixtures/window_pipeline_parity/reference.json no longer matches its pin; \
         the frozen corpus changes only in a reviewed commit that updates the pin"
    );
    let reference: Value = serde_json::from_str(REFERENCE_JSON).expect("reference.json parses");
    assert_eq!(reference["schema"], REFERENCE_SCHEMA);
    assert_eq!(reference["default_page_size"], 512);
    assert_eq!(reference["master_seed"], "0xb17e5eed");
    let sidecar: Value =
        serde_json::from_str(UNPINNED_INPUTS_JSON).expect("inputs-unpinned.json parses");
    assert_eq!(sidecar["schema"], UNPINNED_SCHEMA);
    assert_eq!(sidecar["source_commit"], ORACLE_SOURCE_COMMIT);
    let sidecar_cases = sidecar["cases"]
        .as_object()
        .expect("sidecar cases is an object");
    let entries = reference["cases"]
        .as_object()
        .expect("reference cases is an object");

    let mut cases = Vec::with_capacity(entries.len());
    let mut sidecar_used = BTreeSet::new();
    for (name, entry) in entries {
        let pinned = entry
            .get("pinned_literals")
            .and_then(|literals| literals.get("input"));
        let (input, input_from_sidecar) = match (pinned, sidecar_cases.get(name)) {
            (Some(input), None) => (input.clone(), false),
            (None, Some(input)) => {
                sidecar_used.insert(name.as_str());
                (input.clone(), true)
            }
            (Some(_), Some(_)) => panic!("{name}: input is both pinned and in the sidecar"),
            (None, None) => panic!("{name}: input is neither pinned nor in the sidecar"),
        };
        assert_eq!(input["schema"], INPUT_SCHEMA, "{name}: input schema");
        assert_eq!(input["name"], name.as_str(), "{name}: input name");
        cases.push(Case {
            name: name.clone(),
            entry: entry.clone(),
            input,
            input_from_sidecar,
        });
    }

    // input_sha256 first: a changed input invalidates every output check.
    assert_input_hashes(&cases);
    assert_corpus_shape(&cases, &sidecar_used, sidecar_cases.len());
    assert_integer_domain(&cases);
    cases
}

/// Load the supplementary cases, check their pin and every input's
/// `input_sha256`, and assert their shape, before any output is compared.
pub fn load_supplementary() -> Vec<Case> {
    assert_eq!(
        sha256_hex(SUPPLEMENTARY_JSON.as_bytes()),
        SUPPLEMENTARY_SHA256,
        "tests/fixtures/window_pipeline_parity/supplementary.json no longer matches its pin; \
         it changes only in a reviewed commit that re-exports it and updates the pin"
    );
    let document: Value =
        serde_json::from_str(SUPPLEMENTARY_JSON).expect("supplementary.json parses");
    assert_eq!(document["schema"], SUPPLEMENTARY_SCHEMA);
    assert_eq!(document["source_commit"], ORACLE_SOURCE_COMMIT);
    let entries = document["cases"]
        .as_object()
        .expect("supplementary cases is an object");
    let mut names: Vec<&str> = entries.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, SUPPLEMENTARY_CASES, "supplementary case names");

    let cases: Vec<Case> = entries
        .iter()
        .map(|(name, entry)| {
            let input = entry["pinned_literals"]["input"].clone();
            assert_eq!(input["schema"], INPUT_SCHEMA, "{name}: input schema");
            assert_eq!(input["name"], name.as_str(), "{name}: input name");
            assert!(
                entry.get("rejected").is_none(),
                "{name}: the oracle accepted every supplementary case"
            );
            Case {
                name: name.clone(),
                entry: entry.clone(),
                input,
                input_from_sidecar: false,
            }
        })
        .collect();
    assert_input_hashes(&cases);
    for case in &cases {
        assert!(
            prepare_window_out_of_range(&case.full_request()).is_none(),
            "{}: supplementary inputs lie inside the declared widths",
            case.name
        );
        assert!(
            !case.advances().is_empty(),
            "{}: every supplementary case advances",
            case.name
        );
    }
    cases
}

fn assert_input_hashes(cases: &[Case]) {
    let input_mismatches: Vec<String> = cases
        .iter()
        .filter_map(|case| {
            let recomputed = sha256_hex(python_json(&case.input).as_bytes());
            let frozen = case.entry["input_sha256"].as_str().unwrap_or("<missing>");
            (recomputed != frozen).then(|| {
                format!(
                    "{}: input_sha256 frozen {frozen}, recomputed {recomputed}{}",
                    case.name,
                    if case.input_from_sidecar {
                        " (input from inputs-unpinned.json)"
                    } else {
                        " (pinned input)"
                    }
                )
            })
        })
        .collect();
    assert!(
        input_mismatches.is_empty(),
        "{} corpus input(s) no longer reproduce their frozen input_sha256:\n{}",
        input_mismatches.len(),
        input_mismatches.join("\n")
    );
}

fn assert_corpus_shape(cases: &[Case], sidecar_used: &BTreeSet<&str>, sidecar_len: usize) {
    assert_eq!(cases.len(), 35, "corpus case count");
    let rejected: Vec<&Case> = cases
        .iter()
        .filter(|case| case.entry.get("rejected").is_some())
        .collect();
    assert_eq!(rejected.len(), 12, "rejection case count");
    assert_eq!(cases.len() - rejected.len(), 23, "success case count");
    let pinned = cases.iter().filter(|case| !case.input_from_sidecar).count();
    assert_eq!(pinned, 30, "pinned input count");
    assert_eq!(
        sidecar_used.iter().copied().collect::<Vec<_>>(),
        UNPINNED_CASES,
        "sidecar supplies exactly the five unpinned inputs"
    );
    assert_eq!(
        sidecar_len,
        UNPINNED_CASES.len(),
        "sidecar holds no extra cases"
    );

    let categories: BTreeSet<&str> = rejected
        .iter()
        .map(|case| {
            case.entry["rejected"]
                .as_str()
                .expect("category is a string")
        })
        .collect();
    let vocabulary: BTreeSet<&str> = FULL_REJECTIONS
        .iter()
        .chain(ADVANCE_REJECTIONS.iter())
        .copied()
        .collect();
    assert_eq!(
        categories, vocabulary,
        "rejection categories cover the vocabulary"
    );
    for case in &rejected {
        let category = case.entry["rejected"].as_str().unwrap();
        if FULL_REJECTIONS.contains(&category) {
            assert!(
                case.advances().is_empty(),
                "{}: full rejection with advances",
                case.name
            );
        } else {
            // `Case::expectation` derives the refused step from this.
            assert_eq!(
                case.advances().len(),
                1,
                "{}: advance rejection with other than exactly one advance",
                case.name
            );
        }
    }

    // The frozen advance_stats is one entry per advance, so it is empty
    // exactly when the case has no advances.
    let mut with_stats = 0;
    for case in cases
        .iter()
        .filter(|case| case.entry.get("rejected").is_none())
    {
        for field in [
            "canonical_bytes_len",
            "canonical_bytes_sha256",
            "canonical_digest",
            "record_count",
            "record_stream_len",
            "record_stream_sha256",
            "spool_tail_len",
            "spool_tail_sha256",
        ] {
            assert!(
                !case.entry[field].is_null(),
                "{}: frozen {field} missing",
                case.name
            );
        }
        let stats = case.entry["advance_stats"]
            .as_array()
            .unwrap_or_else(|| panic!("{}: advance_stats is not a list", case.name));
        assert_eq!(
            stats.len(),
            case.advances().len(),
            "{}: advance_stats length",
            case.name
        );
        if !stats.is_empty() {
            with_stats += 1;
        }
    }
    assert_eq!(with_stats, 8, "cases with advance_stats");
}

/// 3.x.x's width classifier agrees with the two-entry table on all 35 inputs.
fn assert_integer_domain(cases: &[Case]) {
    let mut declined = 0;
    for case in cases {
        let found = prepare_window_out_of_range(&case.full_request());
        match (case.out_of_domain(), found) {
            (None, None) => {}
            (Some(expected), Some(found)) => {
                declined += 1;
                assert_eq!(
                    found.field, expected.field,
                    "{}: out-of-range field",
                    case.name
                );
                assert_eq!(
                    found.literal, expected.literal,
                    "{}: out-of-range literal",
                    case.name
                );
                assert_eq!(
                    found.width, expected.width,
                    "{}: out-of-range width",
                    case.name
                );
            }
            (expected, found) => panic!(
                "{}: integer-domain classification differs from the table: table {:?}, \
                 prepare_window_out_of_range {found:?}",
                case.name,
                expected.map(|entry| (entry.field, entry.width.name()))
            ),
        }
    }
    assert_eq!(declined, OUT_OF_DOMAIN.len(), "out-of-domain case count");
}

/// Byte outputs of one folded window, from either replay.
pub struct Outputs {
    pub record_count: usize,
    /// The canonical items stream, without the enclosing brackets.
    pub canonical_items: Vec<u8>,
    pub canonical_digest: String,
    /// Per-record canonical fragments, in window order.
    pub fragments: Vec<Vec<u8>>,
    pub spool_tail: Vec<u8>,
    /// `[added_rows, expired_rows, touched_pages]` per advance.
    pub advance_stats: Vec<[usize; 3]>,
}

pub enum Outcome {
    Outputs(Box<Outputs>),
    Rejected {
        phase: Phase,
        category: Option<String>,
        message: String,
    },
    Declined {
        field: String,
        width: String,
        detail: String,
    },
}

impl Outcome {
    fn describe(&self) -> String {
        match self {
            Outcome::Outputs(outputs) => format!("a window of {} records", outputs.record_count),
            Outcome::Rejected {
                phase,
                category,
                message,
            } => format!("rejection {category:?} at {phase} ({message})"),
            Outcome::Declined {
                field,
                width,
                detail,
            } => format!("decline of {field} at {width} ({detail})"),
        }
    }
}

impl Expectation {
    fn describe(&self) -> String {
        match self {
            Expectation::Outputs => "a window".to_string(),
            Expectation::Rejected { category, phase } => format!("rejection {category} at {phase}"),
            Expectation::Declined(entry) => {
                format!("decline of {} at {}", entry.field, entry.width.name())
            }
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tally {
    pub cases: usize,
    pub rejections: usize,
    pub byte_compared: usize,
    pub declined: usize,
    pub with_advance_stats: usize,
}

impl fmt::Display for Tally {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} cases, {} rejections, {} byte-compared, {} declined, {} with advance stats",
            self.cases, self.rejections, self.byte_compared, self.declined, self.with_advance_stats
        )
    }
}

/// Every mismatch across the corpus, named by case and field, reported once.
#[derive(Default)]
pub struct Mismatches(Vec<String>);

impl Mismatches {
    pub fn push(&mut self, case: &str, field: &str, detail: impl fmt::Display) {
        self.0.push(format!("{case}: {field}: {detail}"));
    }

    pub fn check<T: PartialEq + fmt::Debug>(
        &mut self,
        case: &str,
        field: &str,
        expected: T,
        actual: T,
    ) {
        if expected != actual {
            self.push(case, field, format!("frozen {expected:?}, got {actual:?}"));
        }
    }

    /// Compare exact bytes, reporting the first differing offset with context.
    pub fn check_bytes(&mut self, case: &str, field: &str, expected: &[u8], actual: &[u8]) {
        if expected == actual {
            return;
        }
        let offset = expected
            .iter()
            .zip(actual)
            .position(|(left, right)| left != right)
            .unwrap_or(expected.len().min(actual.len()));
        let context = |bytes: &[u8]| {
            let start = offset.saturating_sub(24);
            let end = (offset + 40).min(bytes.len());
            String::from_utf8_lossy(&bytes[start.min(end)..end]).into_owned()
        };
        self.push(
            case,
            field,
            format!(
                "frozen {} bytes, got {} bytes; first difference at byte {offset}: \
                 frozen {:?}, got {:?}",
                expected.len(),
                actual.len(),
                context(expected),
                context(actual)
            ),
        );
    }

    /// Settle one case: its outcome against the corpus expectation.
    pub fn settle(&mut self, case: &Case, outcome: Outcome, tally: &mut Tally) {
        tally.cases += 1;
        let name = case.name.as_str();
        match (case.expectation(), outcome) {
            (Expectation::Outputs, Outcome::Outputs(outputs)) => {
                tally.byte_compared += 1;
                if !outputs.advance_stats.is_empty() {
                    tally.with_advance_stats += 1;
                }
                self.compare_outputs(case, &outputs);
            }
            (
                Expectation::Rejected {
                    category: expected,
                    phase: expected_phase,
                },
                Outcome::Rejected {
                    phase, category, ..
                },
            ) => {
                tally.rejections += 1;
                self.check(name, "rejection phase", expected_phase, phase);
                self.check(name, "rejection category", Some(expected), category);
            }
            (Expectation::Declined(entry), Outcome::Declined { field, width, .. }) => {
                tally.declined += 1;
                self.check(name, "declined field", entry.field, field.as_str());
                self.check(name, "declined width", entry.width.name(), width.as_str());
            }
            (expected, outcome) => self.push(
                name,
                "outcome",
                format!(
                    "expected {}, got {}",
                    expected.describe(),
                    outcome.describe()
                ),
            ),
        }
    }

    fn compare_outputs(&mut self, case: &Case, outputs: &Outputs) {
        let name = case.name.as_str();
        let entry = &case.entry;
        let frozen_usize = |field: &str| entry[field].as_u64().map(|value| value as usize);
        let frozen_str = |field: &str| entry[field].as_str().unwrap_or("<missing>");
        let pinned = |field: &str| entry.get("pinned_literals").and_then(|l| l.get(field));

        self.check(
            name,
            "record_count",
            frozen_usize("record_count"),
            Some(outputs.record_count),
        );

        let mut canonical = Vec::with_capacity(outputs.canonical_items.len() + 2);
        canonical.push(b'[');
        canonical.extend_from_slice(&outputs.canonical_items);
        canonical.push(b']');
        self.check(
            name,
            "canonical_bytes_len",
            frozen_usize("canonical_bytes_len"),
            Some(canonical.len()),
        );
        self.check(
            name,
            "canonical_bytes_sha256",
            frozen_str("canonical_bytes_sha256"),
            sha256_hex(&canonical).as_str(),
        );
        self.check(
            name,
            "canonical_digest",
            frozen_str("canonical_digest"),
            outputs.canonical_digest.as_str(),
        );
        if let Some(literal) = pinned("canonical_bytes").and_then(Value::as_str) {
            self.check_bytes(
                name,
                "pinned canonical_bytes",
                literal.as_bytes(),
                &canonical,
            );
        }
        let stream = outputs.fragments.join(&b"\n"[..]);
        self.check(
            name,
            "record_stream_len",
            frozen_usize("record_stream_len"),
            Some(stream.len()),
        );
        self.check(
            name,
            "record_stream_sha256",
            frozen_str("record_stream_sha256"),
            sha256_hex(&stream).as_str(),
        );
        if let Some(literals) = pinned("record_jsons").and_then(Value::as_array) {
            self.check(
                name,
                "pinned record_jsons count",
                literals.len(),
                outputs.fragments.len(),
            );
            for (index, (literal, fragment)) in literals.iter().zip(&outputs.fragments).enumerate()
            {
                self.check_bytes(
                    name,
                    &format!("pinned record_jsons[{index}]"),
                    literal.as_str().unwrap_or_default().as_bytes(),
                    fragment,
                );
            }
        }

        self.check(
            name,
            "spool_tail_len",
            frozen_usize("spool_tail_len"),
            Some(outputs.spool_tail.len()),
        );
        self.check(
            name,
            "spool_tail_sha256",
            frozen_str("spool_tail_sha256"),
            sha256_hex(&outputs.spool_tail).as_str(),
        );
        if let Some(literal) = pinned("spool_tail").and_then(Value::as_str) {
            self.check_bytes(
                name,
                "pinned spool_tail",
                literal.as_bytes(),
                &outputs.spool_tail,
            );
        }

        let frozen_stats: Option<Vec<[usize; 3]>> =
            entry["advance_stats"].as_array().map(|steps| {
                steps
                    .iter()
                    .map(|step| {
                        ["added_rows", "expired_rows", "touched_pages"]
                            .map(|field| step[field].as_u64().unwrap_or(u64::MAX) as usize)
                    })
                    .collect()
            });
        self.check(
            name,
            "advance_stats",
            frozen_stats,
            Some(outputs.advance_stats.clone()),
        );
    }

    pub fn finish(self, what: &str, tally: Tally, expected: Tally) {
        println!("{what}: {tally}");
        let mut mismatches = self.0;
        if tally != expected {
            mismatches.push(format!("corpus: tally: expected {expected}, got {tally}"));
        }
        assert!(
            mismatches.is_empty(),
            "{what}: {} mismatch(es) against the frozen corpus:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// CPython's `json.dumps(value, sort_keys=True, separators=(",", ":"))`,
/// `ensure_ascii` included. Integer literals pass through verbatim
/// (`arbitrary_precision`), and keys are sorted here rather than relying on
/// serde_json's map order, which a `preserve_order` feature would change.
pub fn python_json(value: &Value) -> String {
    let mut out = String::new();
    push_python_json(&mut out, value);
    out
}

fn push_python_json(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
        Value::Number(number) => out.push_str(number.as_str()),
        Value::String(text) => push_python_string(out, text),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_python_json(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_python_string(out, key);
                out.push(':');
                push_python_json(out, &map[key]);
            }
            out.push('}');
        }
    }
}

/// A test-only copy of `window.rs`'s private `push_python_json_escaped`:
/// everything outside 0x20..=0x7e escapes as lowercase `\uXXXX`, non-BMP as
/// a UTF-16 surrogate pair, exactly like CPython's `ensure_ascii` encoder.
fn push_python_string(out: &mut String, value: &str) {
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{0c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            ' '..='~' => out.push(character),
            _ => {
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out.push('"');
}

/// The per-record canonical fragments joined by `\n`.
pub fn record_stream(fragments: &[Vec<u8>]) -> Vec<u8> {
    fragments.join(&b"\n"[..])
}

/// The frozen compact build-request tail the 2.x.x coordinator spooled --
/// `,"compact_share_identities":<IDS>,"compact_shares":<ROWS>}` with compact
/// separators and `ensure_ascii` -- a shape the daemon's build request still
/// accepts. Identities deduplicate in first-seen order; one row per record.
pub fn spool_tail(shares: &[AcceptedShare]) -> Vec<u8> {
    let mut identity_indexes: HashMap<(&str, &str, &str), usize> = HashMap::new();
    let mut identities = String::new();
    let mut rows = String::new();
    for share in shares {
        let identity = (
            share.miner_id.as_str(),
            share.order_key.as_str(),
            share.p2mr_program_hex.as_str(),
        );
        let next_index = identity_indexes.len();
        let identity_index = *identity_indexes.entry(identity).or_insert_with(|| {
            if next_index > 0 {
                identities.push(',');
            }
            identities.push('[');
            push_python_string(&mut identities, identity.0);
            identities.push(',');
            push_python_string(&mut identities, identity.1);
            identities.push(',');
            push_python_string(&mut identities, identity.2);
            identities.push(']');
            next_index
        });
        if !rows.is_empty() {
            rows.push(',');
        }
        rows.push_str(&format!("[{},", share.share_seq));
        push_python_string(&mut rows, &share.share_id);
        rows.push_str(&format!(
            ",{identity_index},{},{},{},",
            share.share_difficulty, share.job_issued_at_ms, share.accepted_at_ms
        ));
        match &share.credit_policy {
            Some(policy) => push_python_string(&mut rows, policy),
            None => rows.push_str("null"),
        }
        rows.push(']');
    }
    format!(",\"compact_share_identities\":[{identities}],\"compact_shares\":[{rows}]}}")
        .into_bytes()
}
