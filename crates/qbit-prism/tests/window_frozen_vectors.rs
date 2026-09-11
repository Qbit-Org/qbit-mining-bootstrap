//! Native replay of the frozen payout-window corpus (#269).
//!
//! Every case of `tests/fixtures/window_pipeline_parity/reference.json` --
//! generated on 2.x.x by the Python oracle -- is folded and advanced with
//! `qbit_prism::window::PayoutWindow` and compared against the frozen bytes.
//! The two cases outside the declared integer widths must fail the typed
//! parse; see `support/window_corpus.rs`. The supplementary cases replay the
//! same way under their own tally.

#[path = "support/window_corpus.rs"]
mod window_corpus;

use qbit_prism::window::{canonical_share_fragment, prepare_window_out_of_range, PayoutWindow};
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use window_corpus::{spool_tail, Case, Mismatches, Outcome, Outputs, Phase, Tally};

fn replay(case: &Case, mismatches: &mut Mismatches) -> Outcome {
    let typed = match case.typed() {
        Ok(typed) => typed,
        Err(error) => {
            // Only an out-of-width integer may fail the typed parse; the
            // classifier names it, and `settle` holds it to the table.
            let found = prepare_window_out_of_range(&case.full_request());
            return match found {
                Some(found) => Outcome::Declined {
                    field: found.field,
                    width: found.width.name().to_string(),
                    detail: error.to_string(),
                },
                None => Outcome::Rejected {
                    phase: Phase::Full,
                    category: None,
                    message: format!("typed parse failed in domain: {error}"),
                },
            };
        }
    };

    let mut window = match PayoutWindow::from_full_snapshot(
        typed.snapshot.records,
        typed.snapshot.anchor_job_issued_at_ms,
        typed.window_weight,
        typed.page_size,
    ) {
        Ok(window) => window,
        Err(error) => {
            return Outcome::Rejected {
                phase: Phase::Full,
                category: error.rejection_category().map(str::to_string),
                message: error.to_string(),
            }
        }
    };
    let mut advance_stats = Vec::with_capacity(typed.advances.len());
    for (index, step) in typed.advances.into_iter().enumerate() {
        let previous_items = window.canonical_items_bytes();
        match window.advance(step.records, step.anchor_job_issued_at_ms) {
            Ok((advanced, stats, byte_delta)) => {
                // The coordinator's mirror surgery lands on the new stream.
                let mut mirror = previous_items
                    .get(byte_delta.retained_drop_bytes..)
                    .unwrap_or_default()
                    .to_vec();
                if byte_delta.retained_drop_bytes > previous_items.len() {
                    mismatches.push(
                        &case.name,
                        &format!("advances[{index}] retained_drop_bytes"),
                        format!(
                            "{} exceeds the previous {} bytes",
                            byte_delta.retained_drop_bytes,
                            previous_items.len()
                        ),
                    );
                }
                mirror.extend_from_slice(&byte_delta.appended_items);
                mismatches.check_bytes(
                    &case.name,
                    &format!("advances[{index}] byte delta"),
                    &advanced.canonical_items_bytes(),
                    &mirror,
                );
                advance_stats.push([stats.added_rows, stats.expired_rows, stats.touched_pages]);
                window = advanced;
            }
            Err(error) => {
                return Outcome::Rejected {
                    phase: Phase::Advance(index),
                    category: error.rejection_category().map(str::to_string),
                    message: error.to_string(),
                }
            }
        }
    }

    let shares = window.shares_for_build();
    let canonical_items = window.canonical_items_bytes();
    let fragments: Vec<Vec<u8>> = shares.iter().map(canonical_share_fragment).collect();
    // Fragments encoded record by record must be the window's own items
    // stream, exactly.
    mismatches.check_bytes(
        &case.name,
        "fragments joined by ','",
        &canonical_items,
        &fragments.join(&b","[..]),
    );
    Outcome::Outputs(Box::new(Outputs {
        record_count: window.record_count(),
        canonical_items,
        canonical_digest: window.canonical_digest_hex(),
        fragments,
        spool_tail: spool_tail(&shares),
        advance_stats,
    }))
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| message.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// Replay every case in isolation: a panic inside the engine (a
/// `debug_assert!`, say) becomes a mismatch naming its case, and the
/// remaining cases still run.
fn replay_all(what: &str, cases: &[Case], expected: Tally) {
    let mut mismatches = Mismatches::default();
    let mut tally = Tally::default();
    for case in cases {
        match catch_unwind(AssertUnwindSafe(|| replay(case, &mut mismatches))) {
            Ok(outcome) => mismatches.settle(case, outcome, &mut tally),
            Err(payload) => {
                tally.cases += 1;
                mismatches.push(&case.name, "panic", panic_message(payload.as_ref()));
            }
        }
    }
    mismatches.finish(what, tally, expected);
}

#[test]
fn frozen_corpus_replays_through_the_native_window() {
    replay_all(
        "window_frozen_vectors",
        &window_corpus::load(),
        window_corpus::EXPECTED_TALLY,
    );
}

#[test]
fn supplementary_cases_replay_through_the_native_window() {
    replay_all(
        "window_frozen_vectors supplementary",
        &window_corpus::load_supplementary(),
        window_corpus::EXPECTED_SUPPLEMENTARY_TALLY,
    );
}
