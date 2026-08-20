//! Payout-window fold, canonical digest, and incremental advance.
//!
//! This is the Rust side of the #131 window-pipeline migration: a
//! byte-identical reimplementation of the coordinator's
//! `IncrementalShareWindow` (`lab/prism/share_ledger.py`) proven against the
//! differential parity oracle in `tests/window_pipeline_parity.py`. Every
//! rule here mirrors a named invariant of the Python fold: eligibility on
//! both timestamps, ascending `share_seq` with duplicate detection, duplicate
//! `share_id` detection, non-positive difficulty rejection, the exact
//! crossing-row retention rule (the final whole share crossing
//! `window_weight` is deliberately retained), 512-record paging, and the
//! digest framing (`[`, non-empty page fragments joined by `,`, `]`).
//!
//! The canonical per-record encoding reproduces CPython's
//! `json.dumps(record, sort_keys=True, separators=(",", ":"))` byte-for-byte
//! (`ensure_ascii` escapes included, non-BMP as surrogate pairs) because the
//! coordinator's cache keys, audit digests, and spool payloads are all
//! derived from those exact bytes.

use crate::AcceptedShare;
use sha2::{Digest, Sha256};
use std::fmt;
use std::rc::Rc;

/// Mirrors `DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE`.
pub const DEFAULT_WINDOW_PAGE_SIZE: usize = 512;

/// Full-snapshot rejections; mirrors the `ValueError`s raised by
/// `IncrementalShareWindow.from_full_snapshot`.
#[derive(Debug, PartialEq, Eq)]
pub enum WindowFoldError {
    NonPositiveWindowWeight,
    NonPositivePageSize,
    DuplicateShareSeq,
    DuplicateShareId,
    NonPositiveDifficulty,
}

impl fmt::Display for WindowFoldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            WindowFoldError::NonPositiveWindowWeight => "window_weight must be positive",
            WindowFoldError::NonPositivePageSize => "page_size must be positive",
            WindowFoldError::DuplicateShareSeq => {
                "full payout window contains duplicate share_seq"
            }
            WindowFoldError::DuplicateShareId => "full payout window contains duplicate share_id",
            WindowFoldError::NonPositiveDifficulty => {
                "full payout window contains non-positive difficulty"
            }
        };
        formatter.write_str(message)
    }
}

/// Advance rejections; mirrors `IncrementalWindowFallback`, whose message
/// strings the coordinator logs when it falls back to a full rescan.
#[derive(Debug, PartialEq, Eq)]
pub enum WindowAdvanceError {
    AnchorMovedBackwards,
    NonPositiveDifficulty,
    IneligibleAtNewAnchor,
    RepeatsPreviouslyEligible,
    NotAnAppend,
    DeltaOrderNotIncreasing,
}

impl fmt::Display for WindowAdvanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            WindowAdvanceError::AnchorMovedBackwards => "snapshot anchor moved backwards",
            WindowAdvanceError::NonPositiveDifficulty => {
                "delta contains non-positive share difficulty"
            }
            WindowAdvanceError::IneligibleAtNewAnchor => {
                "delta contains a share ineligible at the new anchor"
            }
            WindowAdvanceError::RepeatsPreviouslyEligible => {
                "delta repeats a share eligible at the previous anchor"
            }
            WindowAdvanceError::NotAnAppend => "newly eligible share is not an append",
            WindowAdvanceError::DeltaOrderNotIncreasing => {
                "delta share_seq order is not increasing"
            }
        };
        formatter.write_str(message)
    }
}

/// Mirrors `IncrementalWindowAdvanceStats`, `touched_pages` definition
/// included: pre-existing retained boundary pages whose records were
/// inspected or rewritten; newly allocated append pages and pages expired
/// wholesale do not count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowAdvanceStats {
    pub added_rows: usize,
    pub expired_rows: usize,
    pub touched_pages: usize,
}

/// Byte-level surgery for one advance, letting the coordinator maintain its
/// opaque mirror of the canonical items stream without re-encoding anything:
/// drop `retained_drop_bytes` from the front of the previous stream, then
/// append `appended_items` (leading `,` included exactly when the retained
/// part is non-empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowAdvanceByteDelta {
    pub retained_drop_bytes: usize,
    pub appended_items: Vec<u8>,
}

fn push_python_json_escaped(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    for character in value.chars() {
        let code_point = character as u32;
        match character {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{08}' => out.extend_from_slice(b"\\b"),
            '\t' => out.extend_from_slice(b"\\t"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\u{0c}' => out.extend_from_slice(b"\\f"),
            '\r' => out.extend_from_slice(b"\\r"),
            _ if (0x20..0x7f).contains(&code_point) => out.push(code_point as u8),
            _ if code_point < 0x10000 => {
                push_u16_escape(out, code_point as u16);
            }
            _ => {
                // Non-BMP code points escape as a UTF-16 surrogate pair,
                // exactly like CPython's ensure_ascii encoder.
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    push_u16_escape(out, *unit);
                }
            }
        }
    }
    out.push(b'"');
}

fn push_u16_escape(out: &mut Vec<u8>, unit: u16) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.extend_from_slice(b"\\u");
    out.push(HEX[usize::from((unit >> 12) & 0xf)]);
    out.push(HEX[usize::from((unit >> 8) & 0xf)]);
    out.push(HEX[usize::from((unit >> 4) & 0xf)]);
    out.push(HEX[usize::from(unit & 0xf)]);
}

/// One record's canonical JSON encoding: byte-identical to
/// `json.dumps(record.to_prism_json(), sort_keys=True, separators=(",", ":"))`.
/// Keys are emitted in sorted order with `credit_policy` omitted when absent,
/// matching `AcceptedShareRecord.to_prism_json`.
pub fn canonical_share_fragment(share: &AcceptedShare) -> Vec<u8> {
    let mut out = Vec::with_capacity(256 + share.share_id.len() + share.miner_id.len());
    out.extend_from_slice(b"{\"accepted_at_ms\":");
    out.extend_from_slice(share.accepted_at_ms.to_string().as_bytes());
    if let Some(credit_policy) = &share.credit_policy {
        out.extend_from_slice(b",\"credit_policy\":");
        push_python_json_escaped(&mut out, credit_policy);
    }
    out.extend_from_slice(b",\"job_id\":");
    push_python_json_escaped(&mut out, &share.job_id);
    out.extend_from_slice(b",\"job_issued_at_ms\":");
    out.extend_from_slice(share.job_issued_at_ms.to_string().as_bytes());
    out.extend_from_slice(b",\"miner_id\":");
    push_python_json_escaped(&mut out, &share.miner_id);
    out.extend_from_slice(b",\"network_difficulty\":");
    out.extend_from_slice(share.network_difficulty.to_string().as_bytes());
    out.extend_from_slice(b",\"ntime\":");
    out.extend_from_slice(share.ntime.to_string().as_bytes());
    out.extend_from_slice(b",\"order_key\":");
    push_python_json_escaped(&mut out, &share.order_key);
    out.extend_from_slice(b",\"p2mr_program_hex\":");
    push_python_json_escaped(&mut out, &share.p2mr_program_hex);
    out.extend_from_slice(b",\"share_difficulty\":");
    out.extend_from_slice(share.share_difficulty.to_string().as_bytes());
    out.extend_from_slice(b",\"share_id\":");
    push_python_json_escaped(&mut out, &share.share_id);
    out.extend_from_slice(b",\"share_seq\":");
    out.extend_from_slice(share.share_seq.to_string().as_bytes());
    out.extend_from_slice(b",\"template_height\":");
    out.extend_from_slice(share.template_height.to_string().as_bytes());
    out.push(b'}');
    out
}

/// Mirrors `_IncrementalShareWindowPage`: records plus their pre-encoded
/// canonical fragments and the page's total difficulty.
#[derive(Debug)]
pub struct WindowPage {
    pub records: Vec<AcceptedShare>,
    pub fragments: Vec<Vec<u8>>,
    pub total_difficulty: u128,
}

impl WindowPage {
    fn from_records(records: Vec<AcceptedShare>) -> Self {
        let fragments: Vec<Vec<u8>> = records.iter().map(canonical_share_fragment).collect();
        let total_difficulty = records
            .iter()
            .map(|record| record.share_difficulty)
            .fold(0u128, u128::saturating_add);
        WindowPage {
            records,
            fragments,
            total_difficulty,
        }
    }

    fn from_retained(records: Vec<AcceptedShare>, fragments: Vec<Vec<u8>>) -> Self {
        let total_difficulty = records
            .iter()
            .map(|record| record.share_difficulty)
            .fold(0u128, u128::saturating_add);
        WindowPage {
            records,
            fragments,
            total_difficulty,
        }
    }
}

/// Mirrors `IncrementalShareWindow`: the immutable paged cache of the exact
/// whole-share payout-window superset. Pages are reference-counted so an
/// advanced window shares its retained interior with the previous generation
/// still serving in-flight builds.
#[derive(Debug)]
pub struct PayoutWindow {
    pub anchor_job_issued_at_ms: i64,
    pub window_weight: u128,
    pub page_size: usize,
    pub pages: Vec<Rc<WindowPage>>,
    pub total_difficulty: u128,
}

impl PayoutWindow {
    pub fn from_full_snapshot(
        records: Vec<AcceptedShare>,
        anchor_job_issued_at_ms: i64,
        window_weight: u128,
        page_size: usize,
    ) -> Result<PayoutWindow, WindowFoldError> {
        if window_weight == 0 {
            return Err(WindowFoldError::NonPositiveWindowWeight);
        }
        if page_size == 0 {
            return Err(WindowFoldError::NonPositivePageSize);
        }

        let mut eligible: Vec<AcceptedShare> = records
            .into_iter()
            .filter(|record| {
                record.job_issued_at_ms <= anchor_job_issued_at_ms
                    && record.accepted_at_ms <= anchor_job_issued_at_ms
            })
            .collect();
        eligible.sort_by_key(|record| record.share_seq);

        let mut prior_seq: Option<u64> = None;
        let mut share_ids: std::collections::HashSet<&str> =
            std::collections::HashSet::with_capacity(eligible.len());
        for record in &eligible {
            if let Some(prior) = prior_seq {
                if record.share_seq <= prior {
                    return Err(WindowFoldError::DuplicateShareSeq);
                }
            }
            if !share_ids.insert(record.share_id.as_str()) {
                return Err(WindowFoldError::DuplicateShareId);
            }
            if record.share_difficulty == 0 {
                return Err(WindowFoldError::NonPositiveDifficulty);
            }
            prior_seq = Some(record.share_seq);
        }
        drop(share_ids);

        // Walk newest-to-oldest accumulating weight; the final whole share
        // crossing window_weight is deliberately retained.
        let mut start = eligible.len();
        let mut retained_weight: u128 = 0;
        for index in (0..eligible.len()).rev() {
            if retained_weight >= window_weight {
                break;
            }
            retained_weight = retained_weight.saturating_add(eligible[index].share_difficulty);
            start = index;
        }
        let retained: Vec<AcceptedShare> = eligible.split_off(start);
        let mut pages: Vec<Rc<WindowPage>> =
            Vec::with_capacity(retained.len().div_ceil(page_size).max(1));
        let mut remaining = retained;
        while !remaining.is_empty() {
            let tail = if remaining.len() > page_size {
                remaining.split_off(page_size)
            } else {
                Vec::new()
            };
            pages.push(Rc::new(WindowPage::from_records(remaining)));
            remaining = tail;
        }
        Ok(PayoutWindow {
            anchor_job_issued_at_ms,
            window_weight,
            page_size,
            pages,
            total_difficulty: retained_weight,
        })
    }

    pub fn record_count(&self) -> usize {
        self.pages.iter().map(|page| page.records.len()).sum()
    }

    /// Every retained record's canonical fragment lengths, oldest first,
    /// flattened across pages.
    fn fragment_lengths(&self) -> Vec<usize> {
        self.pages
            .iter()
            .flat_map(|page| page.fragments.iter().map(Vec::len))
            .collect()
    }

    /// The canonical items stream: every fragment joined with `,`, no
    /// enclosing brackets. Identical to the concatenation the digest hashes,
    /// because the page framing skips empty pages and joins with the same
    /// separator, making page layout invisible on the wire.
    pub fn canonical_items_bytes(&self) -> Vec<u8> {
        let total: usize = self
            .pages
            .iter()
            .flat_map(|page| page.fragments.iter().map(Vec::len))
            .sum();
        let count = self.record_count();
        let mut out = Vec::with_capacity(total + count.saturating_sub(1));
        let mut needs_separator = false;
        for page in &self.pages {
            for fragment in &page.fragments {
                if needs_separator {
                    out.push(b',');
                }
                out.extend_from_slice(fragment);
                needs_separator = true;
            }
        }
        out
    }

    /// Mirrors `IncrementalShareJsonSequence.canonical_json_sha256`: `[`,
    /// non-empty page fragments joined with `,`, `]`.
    pub fn canonical_digest_hex(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"[");
        let mut needs_separator = false;
        for page in &self.pages {
            if page.fragments.is_empty() {
                continue;
            }
            let mut first_in_page = true;
            if needs_separator {
                digest.update(b",");
            }
            for fragment in &page.fragments {
                if !first_in_page {
                    digest.update(b",");
                }
                digest.update(fragment);
                first_in_page = false;
            }
            needs_separator = true;
        }
        digest.update(b"]");
        let bytes = digest.finalize();
        let mut out = String::with_capacity(64);
        for byte in bytes {
            use fmt::Write;
            let _ = write!(out, "{byte:02x}");
        }
        out
    }

    /// Flattened clone of the retained records for one audit build. Prepared
    /// windows are never loaned out: builds get their own copy so the window
    /// state (and its advance lineage) survives every build outcome.
    pub fn shares_for_build(&self) -> Vec<AcceptedShare> {
        let mut out = Vec::with_capacity(self.record_count());
        for page in &self.pages {
            out.extend(page.records.iter().cloned());
        }
        out
    }

    /// Mirrors `IncrementalShareWindow.advance` exactly, including the
    /// validation order, the partial-page expiry at the old edge, and the
    /// `touched_pages` accounting. Also returns the byte-level surgery the
    /// coordinator applies to its opaque canonical-items mirror.
    pub fn advance(
        &self,
        delta: Vec<AcceptedShare>,
        anchor_job_issued_at_ms: i64,
    ) -> Result<(PayoutWindow, WindowAdvanceStats, WindowAdvanceByteDelta), WindowAdvanceError>
    {
        if anchor_job_issued_at_ms < self.anchor_job_issued_at_ms {
            return Err(WindowAdvanceError::AnchorMovedBackwards);
        }
        let prior_seq: Option<u64> = self
            .pages
            .last()
            .and_then(|page| page.records.last())
            .map(|record| record.share_seq);
        let mut prior_delta_seq: Option<u64> = None;
        for record in &delta {
            if record.share_difficulty == 0 {
                return Err(WindowAdvanceError::NonPositiveDifficulty);
            }
            if record.job_issued_at_ms > anchor_job_issued_at_ms
                || record.accepted_at_ms > anchor_job_issued_at_ms
            {
                return Err(WindowAdvanceError::IneligibleAtNewAnchor);
            }
            if record.job_issued_at_ms <= self.anchor_job_issued_at_ms
                && record.accepted_at_ms <= self.anchor_job_issued_at_ms
            {
                return Err(WindowAdvanceError::RepeatsPreviouslyEligible);
            }
            if let Some(prior) = prior_seq {
                if record.share_seq <= prior {
                    return Err(WindowAdvanceError::NotAnAppend);
                }
            }
            if let Some(prior) = prior_delta_seq {
                if record.share_seq <= prior {
                    return Err(WindowAdvanceError::DeltaOrderNotIncreasing);
                }
            }
            prior_delta_seq = Some(record.share_seq);
        }

        let old_fragment_lengths = self.fragment_lengths();
        let old_count = old_fragment_lengths.len();
        let added_rows = delta.len();
        let delta_fragments: Vec<Vec<u8>> = delta.iter().map(canonical_share_fragment).collect();
        let delta_difficulty = delta
            .iter()
            .map(|record| record.share_difficulty)
            .fold(0u128, u128::saturating_add);

        // Copy page references, never their retained contents. `touched` is
        // pre-existing retained boundary pages only, exactly as in Python.
        let mut pages: Vec<Rc<WindowPage>> = self.pages.clone();
        let mut touched_existing_pages: std::collections::BTreeSet<usize> =
            std::collections::BTreeSet::new();
        let mut delta_records = delta;
        let mut delta_encoded = delta_fragments;
        if !delta_records.is_empty() {
            if let Some(last) = pages.last() {
                if last.records.len() < self.page_size {
                    let available = self.page_size - last.records.len();
                    let take = available.min(delta_records.len());
                    if take > 0 {
                        let mut merged_records = last.records.clone();
                        let mut merged_fragments = last.fragments.clone();
                        let tail_records = delta_records.split_off(take);
                        let tail_fragments = delta_encoded.split_off(take);
                        merged_records.extend(delta_records);
                        merged_fragments.extend(delta_encoded);
                        delta_records = tail_records;
                        delta_encoded = tail_fragments;
                        let last_index = pages.len() - 1;
                        pages[last_index] =
                            Rc::new(WindowPage::from_retained(merged_records, merged_fragments));
                        touched_existing_pages.insert(self.pages.len() - 1);
                    }
                }
            }
        }
        while !delta_records.is_empty() {
            let take = self.page_size.min(delta_records.len());
            let tail_records = delta_records.split_off(take);
            let tail_fragments = delta_encoded.split_off(take);
            pages.push(Rc::new(WindowPage::from_retained(
                delta_records,
                delta_encoded,
            )));
            delta_records = tail_records;
            delta_encoded = tail_fragments;
        }

        let mut total_difficulty = self.total_difficulty.saturating_add(delta_difficulty);
        let mut expired_rows: usize = 0;
        let mut first_retained_page: usize = 0;
        while first_retained_page < pages.len()
            && total_difficulty - pages[first_retained_page].total_difficulty
                >= self.window_weight
        {
            let page = &pages[first_retained_page];
            total_difficulty -= page.total_difficulty;
            expired_rows += page.records.len();
            first_retained_page += 1;
        }
        let original_page_offset = first_retained_page;
        if first_retained_page > 0 {
            pages.drain(..first_retained_page);
        }

        if let Some(first_page) = pages.first().cloned() {
            let mut partial_expired: usize = 0;
            while partial_expired < first_page.records.len()
                && total_difficulty - first_page.records[partial_expired].share_difficulty
                    >= self.window_weight
            {
                total_difficulty -= first_page.records[partial_expired].share_difficulty;
                partial_expired += 1;
            }
            if partial_expired > 0 {
                pages[0] = Rc::new(WindowPage::from_retained(
                    first_page.records[partial_expired..].to_vec(),
                    first_page.fragments[partial_expired..].to_vec(),
                ));
                expired_rows += partial_expired;
                if original_page_offset < self.pages.len() {
                    touched_existing_pages.insert(original_page_offset);
                }
            }
        }

        let advanced = PayoutWindow {
            anchor_job_issued_at_ms,
            window_weight: self.window_weight,
            page_size: self.page_size,
            pages,
            total_difficulty,
        };
        let stats = WindowAdvanceStats {
            added_rows,
            expired_rows,
            touched_pages: touched_existing_pages.len(),
        };
        let byte_delta = advance_byte_delta(
            &old_fragment_lengths,
            &advanced,
            old_count,
            expired_rows,
            added_rows,
        );
        Ok((advanced, stats, byte_delta))
    }
}

/// Byte surgery from the old canonical items stream to the advanced one.
/// Expiry always drops a prefix of the combined old-then-appended record
/// order, so the mirror update is a prefix drop plus a suffix append.
fn advance_byte_delta(
    old_fragment_lengths: &[usize],
    advanced: &PayoutWindow,
    old_count: usize,
    expired_rows: usize,
    added_rows: usize,
) -> WindowAdvanceByteDelta {
    let dropped_old = expired_rows.min(old_count);
    let retained_old = old_count - dropped_old;
    let retained_drop_bytes = if dropped_old == 0 {
        0
    } else {
        let dropped_bytes: usize = old_fragment_lengths[..dropped_old].iter().sum();
        if retained_old > 0 {
            // Each dropped fragment takes the comma that followed it.
            dropped_bytes + dropped_old
        } else {
            // The whole old stream goes, separators included.
            dropped_bytes + old_count.saturating_sub(1)
        }
    };
    // Fragments past the retained old records are the appended survivors:
    // the delta records that were folded in and not immediately expired.
    let surviving_new = advanced.record_count() - retained_old;
    debug_assert!(surviving_new <= added_rows);
    let mut appended_items = Vec::new();
    if surviving_new > 0 {
        let mut skip = retained_old;
        let mut first = true;
        for page in &advanced.pages {
            for fragment in &page.fragments {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                if !first || retained_old > 0 {
                    appended_items.push(b',');
                }
                appended_items.extend_from_slice(fragment);
                first = false;
            }
        }
    }
    WindowAdvanceByteDelta {
        retained_drop_bytes,
        appended_items,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn share(seq: u64, difficulty: u128, job_ms: i64, accepted_ms: i64) -> AcceptedShare {
        AcceptedShare {
            share_seq: seq,
            share_id: format!("share-{seq}"),
            miner_id: format!("miner-{}", seq % 3),
            order_key: format!("{:02}:miner", seq % 3),
            p2mr_program_hex: format!("aa{seq:02x}"),
            share_difficulty: difficulty,
            network_difficulty: 1000,
            template_height: 100,
            job_id: format!("job-{seq}"),
            job_issued_at_ms: job_ms,
            accepted_at_ms: accepted_ms,
            ntime: 1_700_000_000,
            credit_policy: if seq % 4 == 0 {
                Some("stale-grace".to_string())
            } else {
                None
            },
        }
    }

    #[test]
    fn fragment_escapes_match_python_json_dumps() {
        let mut record = share(1, 5, 10, 11);
        record.share_id = "min\u{00e9}r \"q\" \\b \u{0001}\t\u{1f680}\u{007f}".to_string();
        record.credit_policy = None;
        let fragment = canonical_share_fragment(&record);
        let text = String::from_utf8(fragment).expect("fragment must be ASCII");
        assert!(text.contains(
            "\"share_id\":\"min\\u00e9r \\\"q\\\" \\\\b \\u0001\\t\\ud83d\\ude80\\u007f\""
        ));
        // Sorted key order, credit_policy omitted when absent.
        assert!(text.starts_with("{\"accepted_at_ms\":11,\"job_id\":"));
        assert!(text.ends_with(",\"template_height\":100}"));
    }

    #[test]
    fn crossing_row_is_retained() {
        // Difficulties (2, 3, 8) at weight 10: the final whole share crossing
        // window_weight (seq 2) is retained, total 11 > 10.
        let records = vec![
            share(1, 2, 6, 7),
            share(2, 3, 8, 9),
            share(3, 8, 10, 10),
        ];
        let window = PayoutWindow::from_full_snapshot(records, 10, 10, 512).expect("fold");
        assert_eq!(window.record_count(), 2);
        assert_eq!(window.total_difficulty, 11);
        let exact = PayoutWindow::from_full_snapshot(
            vec![share(1, 2, 6, 7), share(2, 3, 8, 9), share(3, 7, 10, 10)],
            10,
            10,
            512,
        )
        .expect("fold");
        assert_eq!(exact.record_count(), 2);
        assert_eq!(exact.total_difficulty, 10);
    }

    #[test]
    fn eligibility_uses_both_timestamps() {
        let records = vec![
            share(1, 4, 9, 9),
            share(2, 5, 11, 9),
            share(3, 6, 9, 11),
            share(4, 8, 10, 10),
        ];
        let window = PayoutWindow::from_full_snapshot(records, 10, 100, 512).expect("fold");
        assert_eq!(window.record_count(), 2);
    }

    #[test]
    fn duplicate_and_zero_difficulty_are_rejected() {
        let duplicate_seq = vec![share(1, 2, 5, 5), share(1, 3, 6, 6)];
        assert_eq!(
            PayoutWindow::from_full_snapshot(duplicate_seq, 10, 10, 512).unwrap_err(),
            WindowFoldError::DuplicateShareSeq
        );
        let mut duplicate_id = vec![share(1, 2, 5, 5), share(2, 3, 6, 6)];
        duplicate_id[1].share_id = "share-1".to_string();
        assert_eq!(
            PayoutWindow::from_full_snapshot(duplicate_id, 10, 10, 512).unwrap_err(),
            WindowFoldError::DuplicateShareId
        );
        let zero = vec![share(1, 0, 5, 5)];
        assert_eq!(
            PayoutWindow::from_full_snapshot(zero, 10, 10, 512).unwrap_err(),
            WindowFoldError::NonPositiveDifficulty
        );
    }

    #[test]
    fn advance_matches_full_rebuild_and_reports_byte_surgery() {
        let snapshot: Vec<AcceptedShare> = (1..=7)
            .map(|seq| share(seq, [5, 1, 1, 2, 1, 3, 1][usize::try_from(seq).unwrap() - 1], 100 - i64::try_from(seq).unwrap(), 100 - i64::try_from(seq).unwrap()))
            .collect();
        let window =
            PayoutWindow::from_full_snapshot(snapshot.clone(), 100, 9, 3).expect("fold");
        let old_items = window.canonical_items_bytes();
        let delta = vec![share(9, 2, 101, 99), share(10, 4, 90, 101)];
        let (advanced, stats, byte_delta) =
            window.advance(delta.clone(), 101).expect("advance");

        let mut union = snapshot;
        union.extend(delta);
        let rebuilt = PayoutWindow::from_full_snapshot(union, 101, 9, 3).expect("rebuild");
        assert_eq!(
            advanced.canonical_digest_hex(),
            rebuilt.canonical_digest_hex()
        );
        assert_eq!(stats.added_rows, 2);
        assert!(stats.expired_rows > 0);

        // The reported surgery must transform the old items stream into the
        // advanced one.
        let mut mirrored = old_items[byte_delta.retained_drop_bytes..].to_vec();
        mirrored.extend_from_slice(&byte_delta.appended_items);
        assert_eq!(mirrored, advanced.canonical_items_bytes());
    }

    #[test]
    fn advance_rejections_mirror_python() {
        let window = PayoutWindow::from_full_snapshot(
            vec![share(1, 2, 5, 5), share(2, 3, 6, 6)],
            10,
            100,
            512,
        )
        .expect("fold");
        assert_eq!(
            window.advance(vec![], 9).unwrap_err(),
            WindowAdvanceError::AnchorMovedBackwards
        );
        assert_eq!(
            window.advance(vec![share(3, 0, 11, 11)], 11).unwrap_err(),
            WindowAdvanceError::NonPositiveDifficulty
        );
        assert_eq!(
            window.advance(vec![share(3, 1, 12, 11)], 11).unwrap_err(),
            WindowAdvanceError::IneligibleAtNewAnchor
        );
        assert_eq!(
            window.advance(vec![share(3, 1, 9, 9)], 11).unwrap_err(),
            WindowAdvanceError::RepeatsPreviouslyEligible
        );
        assert_eq!(
            window.advance(vec![share(2, 1, 11, 11)], 11).unwrap_err(),
            WindowAdvanceError::NotAnAppend
        );
        assert_eq!(
            window
                .advance(vec![share(4, 1, 11, 11), share(3, 1, 11, 10)], 11)
                .unwrap_err(),
            WindowAdvanceError::DeltaOrderNotIncreasing
        );
    }

    #[test]
    fn empty_delta_advances_anchor_only() {
        let window = PayoutWindow::from_full_snapshot(
            vec![share(1, 2, 5, 5), share(2, 3, 6, 6)],
            10,
            100,
            512,
        )
        .expect("fold");
        let digest = window.canonical_digest_hex();
        let (advanced, stats, byte_delta) = window.advance(vec![], 20).expect("advance");
        assert_eq!(advanced.anchor_job_issued_at_ms, 20);
        assert_eq!(advanced.canonical_digest_hex(), digest);
        assert_eq!(
            stats,
            WindowAdvanceStats {
                added_rows: 0,
                expired_rows: 0,
                touched_pages: 0
            }
        );
        assert_eq!(byte_delta.retained_drop_bytes, 0);
        assert!(byte_delta.appended_items.is_empty());
    }

    #[test]
    fn expiry_that_consumes_the_whole_old_window_still_reports_exact_surgery() {
        let window = PayoutWindow::from_full_snapshot(
            vec![share(1, 2, 5, 5), share(2, 3, 6, 6)],
            10,
            6,
            512,
        )
        .expect("fold");
        let old_items = window.canonical_items_bytes();
        // A heavy delta expires every old record.
        let (advanced, stats, byte_delta) = window
            .advance(vec![share(3, 6, 11, 11), share(4, 1, 11, 11)], 11)
            .expect("advance");
        assert_eq!(stats.expired_rows, 2);
        let mut mirrored = old_items[byte_delta.retained_drop_bytes..].to_vec();
        mirrored.extend_from_slice(&byte_delta.appended_items);
        assert_eq!(mirrored, advanced.canonical_items_bytes());
    }
}
