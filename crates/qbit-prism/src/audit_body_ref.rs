use crate::{
    build_prism_reward_manifest_with_cut, canonical_audit_bundle_bytes,
    window::push_python_json_escaped_contents, AcceptedShare, AuditBundle,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub const AUDIT_BODY_REF_SCHEMA: &str = "qbit.prism.audit-body-ref.v1";
pub const AUDIT_BUNDLE_V2_SCHEMA: &str = "qbit.prism.audit-bundle.v2";
pub const AUDIT_SHARE_SEGMENT_SCHEMA: &str = "qbit.prism.audit-share-segment.v1";
pub const AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA: &str = "qbit.prism.window-completeness-proof.v1";

#[derive(Debug, thiserror::Error)]
pub enum AuditBodyRefError {
    #[error("failed to read audit artifact: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid audit artifact JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid audit body ref: {0}")]
    Invalid(String),
    #[error("audit body ref hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
}

#[derive(Debug, Deserialize)]
struct AuditArtifactSchema {
    #[serde(default)]
    schema: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuditBodyRef {
    schema: String,
    audit_bundle_sha256: String,
    share_count: usize,
    #[serde(default)]
    shares_key_index: Option<usize>,
    bundle_without_shares: Value,
    share_parts: Vec<SharePart>,
}

#[derive(Debug, Deserialize)]
struct AuditBundleV2 {
    schema: String,
    audit_bundle_sha256: String,
    share_count: usize,
    #[serde(default)]
    shares_key_index: Option<usize>,
    bundle_without_shares: Value,
    share_window_proof: AuditWindowCompletenessProof,
}

#[derive(Debug, Deserialize)]
struct AuditWindowCompletenessProof {
    schema: String,
    first_share_seq: u64,
    last_share_seq: u64,
    share_count: usize,
    #[serde(default)]
    share_slice_digest_hex: Option<String>,
    #[serde(default)]
    share_parts_digest_hex: Option<String>,
    share_parts: Vec<SharePart>,
}

// Each variant's fields are in 2.x's insertion order, which the window proof's
// parts digest covers (`share_parts_digest_hex`): do not reorder them.
#[derive(Debug, Serialize)]
#[serde(tag = "kind")]
enum SharePart {
    #[serde(rename = "segment")]
    Segment {
        first_share_seq: u64,
        last_share_seq: u64,
        share_count: usize,
        sha256: String,
        body_uri: String,
    },
    #[serde(rename = "segment_range")]
    SegmentRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        segment_first_share_seq: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        segment_last_share_seq: Option<u64>,
        first_share_seq: u64,
        last_share_seq: u64,
        share_count: usize,
        range_sha256: String,
        body_uri: String,
    },
    #[serde(rename = "segment_prefix")]
    SegmentPrefix {
        first_share_seq: u64,
        last_share_seq: u64,
        share_count: usize,
        prefix_sha256: String,
        body_uri: String,
    },
    #[serde(rename = "inline")]
    Inline {
        first_share_seq: u64,
        last_share_seq: u64,
        share_count: usize,
        shares: Vec<AcceptedShare>,
    },
}

// Serde's internally tagged enum buffer cannot represent u128, including
// difficulties larger than u64::MAX. Dispatch on the JSON tag explicitly so
// inline shares retain serde_json's arbitrary-precision integer decoding.
impl<'de> Deserialize<'de> for SharePart {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            kind: String,
            first_share_seq: u64,
            last_share_seq: u64,
            share_count: usize,
            segment_first_share_seq: Option<u64>,
            segment_last_share_seq: Option<u64>,
            sha256: Option<String>,
            body_uri: Option<String>,
            range_sha256: Option<String>,
            prefix_sha256: Option<String>,
            shares: Option<Vec<AcceptedShare>>,
        }
        let value = Value::deserialize(deserializer)?;
        let fields: Fields = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        let missing = |field: &'static str| <D::Error as serde::de::Error>::missing_field(field);
        let first_share_seq = fields.first_share_seq;
        let last_share_seq = fields.last_share_seq;
        let share_count = fields.share_count;
        match fields.kind.as_str() {
            "segment" => Ok(Self::Segment {
                first_share_seq,
                last_share_seq,
                share_count,
                sha256: fields.sha256.ok_or_else(|| missing("sha256"))?,
                body_uri: fields.body_uri.ok_or_else(|| missing("body_uri"))?,
            }),
            "segment_range" => Ok(Self::SegmentRange {
                first_share_seq,
                last_share_seq,
                share_count,
                segment_first_share_seq: fields.segment_first_share_seq,
                segment_last_share_seq: fields.segment_last_share_seq,
                range_sha256: fields.range_sha256.ok_or_else(|| missing("range_sha256"))?,
                body_uri: fields.body_uri.ok_or_else(|| missing("body_uri"))?,
            }),
            "segment_prefix" => Ok(Self::SegmentPrefix {
                first_share_seq,
                last_share_seq,
                share_count,
                prefix_sha256: fields
                    .prefix_sha256
                    .ok_or_else(|| missing("prefix_sha256"))?,
                body_uri: fields.body_uri.ok_or_else(|| missing("body_uri"))?,
            }),
            "inline" => Ok(Self::Inline {
                first_share_seq,
                last_share_seq,
                share_count,
                shares: fields.shares.ok_or_else(|| missing("shares"))?,
            }),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &["segment", "segment_range", "segment_prefix", "inline"],
            )),
        }
    }
}

#[cfg(test)]
mod inline_share_tests {
    use super::*;

    #[test]
    fn internally_tagged_inline_shares_preserve_u128_difficulty() {
        let difficulty = u128::from(u64::MAX) + 100;
        let part = serde_json::json!({
            "kind":"inline", "first_share_seq":1, "last_share_seq":1,
            "share_count":1, "shares":[{
                "share_seq":1,"share_id":"test","miner_id":"miner",
                "order_key":"miner","p2mr_program_hex":"11".repeat(32),
                "share_difficulty":difficulty,"network_difficulty":difficulty,
                "template_height":1,"job_id":"job","job_issued_at_ms":1,
                "accepted_at_ms":1,"ntime":1
            }]
        });
        let SharePart::Inline { shares, .. } = serde_json::from_value(part).unwrap() else {
            panic!("expected inline share part")
        };
        assert_eq!(shares[0].share_difficulty, difficulty);
        assert_eq!(shares[0].network_difficulty, difficulty);
    }
}

#[cfg(test)]
mod body_uri_tests {
    use super::*;

    #[test]
    fn a_file_uri_segment_resolves_to_its_local_path() {
        let base = Path::new("/audits/block");
        for (uri, path) in [
            ("file:///segments/a.json", PathBuf::from("/segments/a.json")),
            (
                "file://localhost/segments/a.json",
                PathBuf::from("/segments/a.json"),
            ),
            ("/segments/a.json", PathBuf::from("/segments/a.json")),
            ("file://segments/a.json", base.join("segments/a.json")),
            ("segments/a.json", base.join("segments/a.json")),
        ] {
            assert_eq!(resolve_body_uri(uri, Some(base)).unwrap(), path, "{uri}");
        }
        assert_eq!(
            resolve_body_uri("file:///segments/a.json", None).unwrap(),
            PathBuf::from("/segments/a.json")
        );
    }

    /// A segment in a store this build does not read is refused, never read
    /// as a missing local path.
    #[test]
    fn a_segment_uri_with_another_scheme_is_refused() {
        for uri in ["s3://audits/a.json", "https://audits.invalid/a.json"] {
            assert!(
                matches!(
                    resolve_body_uri(uri, Some(Path::new("/audits/block"))),
                    Err(AuditBodyRefError::Invalid(_))
                ),
                "{uri}"
            );
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AuditShareSegment {
    schema: String,
    first_share_seq: u64,
    last_share_seq: u64,
    share_count: usize,
    shares: Vec<AcceptedShare>,
}

pub fn load_audit_bundle_from_path(
    path: impl AsRef<Path>,
) -> Result<AuditBundle, AuditBodyRefError> {
    let path = path.as_ref();
    let input_json = fs::read_to_string(path)?;
    let base_dir = path.parent();
    parse_audit_bundle_json(&input_json, base_dir)
}

pub fn parse_audit_bundle_json(
    input_json: &str,
    base_dir: Option<&Path>,
) -> Result<AuditBundle, AuditBodyRefError> {
    let header: AuditArtifactSchema = serde_json::from_str(input_json)?;
    match header.schema.as_deref() {
        Some(AUDIT_BODY_REF_SCHEMA | AUDIT_BUNDLE_V2_SCHEMA) => {
            let value: Value = serde_json::from_str(input_json)?;
            parse_audit_bundle_value(value, base_dir)
        }
        _ => Ok(serde_json::from_str(input_json)?),
    }
}

pub fn parse_audit_bundle_value(
    value: Value,
    base_dir: Option<&Path>,
) -> Result<AuditBundle, AuditBodyRefError> {
    if value.get("schema").and_then(Value::as_str) == Some(AUDIT_BODY_REF_SCHEMA) {
        let body_ref: AuditBodyRef = serde_json::from_value(value)?;
        resolve_audit_body_ref(body_ref, base_dir)
    } else if value.get("schema").and_then(Value::as_str) == Some(AUDIT_BUNDLE_V2_SCHEMA) {
        let bundle_v2: AuditBundleV2 = serde_json::from_value(value)?;
        resolve_audit_bundle_v2(bundle_v2, base_dir)
    } else {
        Ok(serde_json::from_value(value)?)
    }
}

fn resolve_audit_body_ref(
    body_ref: AuditBodyRef,
    base_dir: Option<&Path>,
) -> Result<AuditBundle, AuditBodyRefError> {
    if body_ref.schema != AUDIT_BODY_REF_SCHEMA {
        return Err(AuditBodyRefError::Invalid(format!(
            "unsupported body-ref schema {}",
            body_ref.schema
        )));
    }
    refuse_window_cut(&body_ref.bundle_without_shares, "audit-body-ref.v1")?;
    let expected_sha256 = body_ref.audit_bundle_sha256.to_lowercase();
    if expected_sha256.len() != 64 || !expected_sha256.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(AuditBodyRefError::Invalid(
            "audit_bundle_sha256 must be 64 hex characters".to_string(),
        ));
    }
    let mut shares = Vec::with_capacity(body_ref.share_count);
    for part in body_ref.share_parts {
        append_share_part(&mut shares, part, base_dir)?;
    }
    if shares.len() != body_ref.share_count {
        return Err(AuditBodyRefError::Invalid(format!(
            "expected {} shares, reconstructed {}",
            body_ref.share_count,
            shares.len()
        )));
    }
    validate_contiguous_shares(&shares, "audit body ref")?;

    let bundle_value = bundle_value_with_shares(
        body_ref.bundle_without_shares,
        body_ref.shares_key_index,
        shares,
    )?;
    let bundle: AuditBundle = serde_json::from_value(bundle_value)?;
    let canonical = canonical_audit_bundle_bytes(&bundle)?;
    let actual_sha256 = hex::encode(Sha256::digest(&canonical));
    if actual_sha256 != expected_sha256 {
        return Err(AuditBodyRefError::HashMismatch {
            expected: expected_sha256,
            actual: actual_sha256,
        });
    }
    Ok(bundle)
}

fn resolve_audit_bundle_v2(
    bundle_v2: AuditBundleV2,
    base_dir: Option<&Path>,
) -> Result<AuditBundle, AuditBodyRefError> {
    if bundle_v2.schema != AUDIT_BUNDLE_V2_SCHEMA {
        return Err(AuditBodyRefError::Invalid(format!(
            "unsupported audit-bundle.v2 schema {}",
            bundle_v2.schema
        )));
    }
    refuse_window_cut(&bundle_v2.bundle_without_shares, "audit-bundle.v2")?;
    let proof = bundle_v2.share_window_proof;
    if proof.schema != AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA {
        return Err(AuditBodyRefError::Invalid(format!(
            "unsupported window proof schema {}",
            proof.schema
        )));
    }
    let expected_sha256 = bundle_v2.audit_bundle_sha256.to_lowercase();
    if expected_sha256.len() != 64 || !expected_sha256.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(AuditBodyRefError::Invalid(
            "audit_bundle_sha256 must be 64 hex characters".to_string(),
        ));
    }

    let share_parts = proof.share_parts;
    if let Some(expected_digest) = proof.share_parts_digest_hex.as_ref() {
        if !expected_digest.eq_ignore_ascii_case(&share_parts_digest_hex(&share_parts)?) {
            return Err(AuditBodyRefError::Invalid(
                "window proof share_parts_digest_hex mismatch".to_string(),
            ));
        }
    }
    let mut shares = Vec::with_capacity(bundle_v2.share_count);
    for part in share_parts {
        append_share_part(&mut shares, part, base_dir)?;
    }
    if shares.len() != bundle_v2.share_count || shares.len() != proof.share_count {
        return Err(AuditBodyRefError::Invalid(format!(
            "expected {} shares, reconstructed {}",
            bundle_v2.share_count,
            shares.len()
        )));
    }
    validate_contiguous_shares(&shares, "audit-bundle.v2")?;
    if shares.first().map(|share| share.share_seq) != Some(proof.first_share_seq)
        || shares.last().map(|share| share.share_seq) != Some(proof.last_share_seq)
    {
        return Err(AuditBodyRefError::Invalid(
            "window proof share_seq range does not match reconstructed shares".to_string(),
        ));
    }

    let bundle_value = bundle_value_with_shares(
        bundle_v2.bundle_without_shares,
        bundle_v2.shares_key_index,
        shares,
    )?;
    let bundle: AuditBundle = serde_json::from_value(bundle_value)?;
    // Always `None`: a cut is refused above, as these envelopes predate
    // dual-writer windows.
    let expected_reward_manifest = build_prism_reward_manifest_with_cut(
        &bundle.shares,
        &bundle.found_block,
        bundle.reward_manifest.cut,
    )
    .map_err(|err| {
        AuditBodyRefError::Invalid(format!(
            "audit-bundle.v2 reward manifest reconstruction failed: {err}"
        ))
    })?;
    if let Some(share_slice_digest_hex) = proof.share_slice_digest_hex {
        if !share_slice_digest_hex
            .eq_ignore_ascii_case(&expected_reward_manifest.share_slice_digest_hex)
        {
            return Err(AuditBodyRefError::Invalid(
                "window proof share_slice_digest_hex mismatch".to_string(),
            ));
        }
    }
    if expected_reward_manifest != bundle.reward_manifest {
        return Err(AuditBodyRefError::Invalid(
            "audit-bundle.v2 reward_manifest mismatch".to_string(),
        ));
    }

    let canonical = canonical_audit_bundle_bytes(&bundle)?;
    let actual_sha256 = hex::encode(Sha256::digest(&canonical));
    if actual_sha256 != expected_sha256 {
        return Err(AuditBodyRefError::HashMismatch {
            expected: expected_sha256,
            actual: actual_sha256,
        });
    }
    Ok(bundle)
}

/// A window proof's `share_parts_digest_hex` as 2.x wrote it (#731): sha256 of
/// Python's `json.dumps({"share_parts": parts}, separators=(",", ":"))`, so each
/// part's keys are in insertion order, `kind` first, and non-ASCII text is
/// escaped. Each [`SharePart`] serializes its tag and then its fields in
/// declaration order, which is that order, and an inline share in
/// [`AcceptedShare`]'s order, as 2.x's share records hold it. Through a
/// `serde_json::Value` the keys would come out sorted instead, because the
/// workspace builds serde_json without `preserve_order`.
fn share_parts_digest_hex(share_parts: &[SharePart]) -> Result<String, AuditBodyRefError> {
    #[derive(Serialize)]
    struct SharePartsDigest<'a> {
        share_parts: &'a [SharePart],
    }
    python_json_sha256(&SharePartsDigest { share_parts })
}

fn bundle_value_with_shares(
    mut bundle_without_shares: Value,
    _shares_key_index: Option<usize>,
    shares: Vec<AcceptedShare>,
) -> Result<Value, AuditBodyRefError> {
    let object = bundle_without_shares.as_object_mut().ok_or_else(|| {
        AuditBodyRefError::Invalid("bundle_without_shares must be an object".to_string())
    })?;
    object.insert("shares".to_string(), serde_json::to_value(shares)?);
    Ok(bundle_without_shares)
}

fn append_share_part(
    shares: &mut Vec<AcceptedShare>,
    part: SharePart,
    base_dir: Option<&Path>,
) -> Result<(), AuditBodyRefError> {
    match part {
        SharePart::Segment {
            first_share_seq,
            last_share_seq,
            share_count,
            sha256,
            body_uri,
        } => {
            let segment_path = resolve_body_uri(&body_uri, base_dir)?;
            let segment_bytes = fs::read(&segment_path)?;
            let actual_sha256 = hex::encode(Sha256::digest(&segment_bytes));
            let expected_sha256 = sha256.to_lowercase();
            if actual_sha256 != expected_sha256 {
                return Err(AuditBodyRefError::HashMismatch {
                    expected: expected_sha256,
                    actual: actual_sha256,
                });
            }
            let segment: AuditShareSegment = serde_json::from_slice(&segment_bytes)?;
            validate_share_segment_metadata(
                &segment,
                first_share_seq,
                last_share_seq,
                share_count,
                &segment_path,
            )?;
            append_contiguous(shares, segment.shares)?;
        }
        SharePart::SegmentRange {
            segment_first_share_seq: _,
            segment_last_share_seq: _,
            first_share_seq,
            last_share_seq,
            share_count,
            range_sha256,
            body_uri,
        } => {
            let segment_path = resolve_body_uri(&body_uri, base_dir)?;
            let segment_bytes = fs::read(&segment_path)?;
            let segment: AuditShareSegment = serde_json::from_slice(&segment_bytes)?;
            validate_share_segment_schema(&segment, &segment_path)?;
            let selected = select_share_segment_range(
                segment.shares,
                first_share_seq,
                last_share_seq,
                share_count,
                "share segment range",
            )?;
            verify_share_segment_sha256(first_share_seq, last_share_seq, &selected, &range_sha256)?;
            append_contiguous(shares, selected)?;
        }
        SharePart::SegmentPrefix {
            first_share_seq,
            last_share_seq,
            share_count,
            prefix_sha256,
            body_uri,
        } => {
            let segment_path = resolve_body_uri(&body_uri, base_dir)?;
            let segment_bytes = fs::read(&segment_path)?;
            let segment: AuditShareSegment = serde_json::from_slice(&segment_bytes)?;
            validate_share_segment_schema(&segment, &segment_path)?;
            if segment.first_share_seq != first_share_seq {
                return Err(AuditBodyRefError::Invalid(format!(
                    "share segment prefix first_share_seq mismatch at {}",
                    segment_path.display()
                )));
            }
            let selected = select_share_segment_range(
                segment.shares,
                first_share_seq,
                last_share_seq,
                share_count,
                "share segment prefix",
            )?;
            verify_share_segment_sha256(
                first_share_seq,
                last_share_seq,
                &selected,
                &prefix_sha256,
            )?;
            append_contiguous(shares, selected)?;
        }
        SharePart::Inline {
            first_share_seq,
            last_share_seq,
            share_count,
            shares: inline_shares,
        } => {
            validate_part_shares(
                &inline_shares,
                first_share_seq,
                last_share_seq,
                share_count,
                "inline share part",
            )?;
            append_contiguous(shares, inline_shares)?;
        }
    }
    Ok(())
}

fn validate_share_segment_schema(
    segment: &AuditShareSegment,
    segment_path: &Path,
) -> Result<(), AuditBodyRefError> {
    if segment.schema != AUDIT_SHARE_SEGMENT_SCHEMA {
        return Err(AuditBodyRefError::Invalid(format!(
            "unsupported share segment schema {} at {}",
            segment.schema,
            segment_path.display()
        )));
    }
    Ok(())
}

fn validate_share_segment_metadata(
    segment: &AuditShareSegment,
    first_share_seq: u64,
    last_share_seq: u64,
    share_count: usize,
    segment_path: &Path,
) -> Result<(), AuditBodyRefError> {
    validate_share_segment_schema(segment, segment_path)?;
    if segment.first_share_seq != first_share_seq
        || segment.last_share_seq != last_share_seq
        || segment.share_count != share_count
    {
        return Err(AuditBodyRefError::Invalid(format!(
            "share segment metadata mismatch at {}",
            segment_path.display()
        )));
    }
    validate_part_shares(
        &segment.shares,
        first_share_seq,
        last_share_seq,
        share_count,
        "share segment",
    )
}

fn select_share_segment_range(
    segment_shares: Vec<AcceptedShare>,
    first_share_seq: u64,
    last_share_seq: u64,
    share_count: usize,
    label: &str,
) -> Result<Vec<AcceptedShare>, AuditBodyRefError> {
    // Only the referenced range must be contiguous: a slot may hold a ledger
    // gap outside it (#733), and only the selected shares enter the bundle.
    let selected = segment_shares
        .into_iter()
        .filter(|share| share.share_seq >= first_share_seq && share.share_seq <= last_share_seq)
        .collect::<Vec<_>>();
    validate_part_shares(
        &selected,
        first_share_seq,
        last_share_seq,
        share_count,
        label,
    )?;
    Ok(selected)
}

/// Check a range or prefix part's digest over its selected shares. 3.x hashes
/// the range as serde_json writes it. 1.x and 2.x hashed it with Python's
/// `json.dumps`, which escapes non-ASCII text as `\uXXXX` (#709). The two
/// forms differ only when a share carries non-ASCII text, such as a worker
/// name, and each encodes the same shares, so either form is accepted. A
/// digest of neither form is a mismatch, reported against the serde_json form.
fn verify_share_segment_sha256(
    first_share_seq: u64,
    last_share_seq: u64,
    shares: &[AcceptedShare],
    expected_sha256: &str,
) -> Result<(), AuditBodyRefError> {
    let segment = AuditShareSegment {
        schema: AUDIT_SHARE_SEGMENT_SCHEMA.to_string(),
        first_share_seq,
        last_share_seq,
        share_count: shares.len(),
        shares: shares.to_vec(),
    };
    let expected_sha256 = expected_sha256.to_lowercase();
    let actual_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&segment)?));
    if actual_sha256 == expected_sha256 || python_json_sha256(&segment)? == expected_sha256 {
        return Ok(());
    }
    Err(AuditBodyRefError::HashMismatch {
        expected: expected_sha256,
        actual: actual_sha256,
    })
}

/// sha256 of `value` as CPython's `json.dumps(value, separators=(",", ":"))`
/// writes it, which is how 1.x and 2.x hashed their storage artifacts: compact
/// serde_json in field order, with non-ASCII text escaped.
fn python_json_sha256<T: Serialize + ?Sized>(value: &T) -> Result<String, AuditBodyRefError> {
    let mut bytes = Vec::new();
    value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut bytes,
        PythonJsonFormatter,
    ))?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

/// serde_json's compact formatter with `json.dumps`'s default
/// `ensure_ascii=True`: every character outside printable ASCII is written as
/// `\uXXXX`, and one above the BMP as a UTF-16 surrogate pair. serde_json
/// passes `"`, `\` and control characters to `write_char_escape`, whose
/// escapes are CPython's too, and everything else in a string to
/// `write_string_fragment`.
struct PythonJsonFormatter;

impl serde_json::ser::Formatter for PythonJsonFormatter {
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let mut escaped = Vec::with_capacity(fragment.len());
        push_python_json_escaped_contents(&mut escaped, fragment);
        writer.write_all(&escaped)
    }
}

fn resolve_body_uri(body_uri: &str, base_dir: Option<&Path>) -> Result<PathBuf, AuditBodyRefError> {
    let path = match body_uri.strip_prefix("file://") {
        // RFC 8089 names this host `localhost`, before an absolute path.
        Some(path) if path.starts_with("localhost/") => &path["localhost".len()..],
        // Otherwise a `file://` URI names the same local path, as it does for
        // a row's own `body_uri`, which the import, the API and the sidecar
        // tooling strip the same way.
        Some(path) => path,
        // A segment in a store this build does not read is not absent: no
        // wait makes it readable here, so it is refused, not read as a path.
        None if body_uri.contains("://") => {
            return Err(AuditBodyRefError::Invalid(format!(
                "unsupported share segment URI scheme: {body_uri}"
            )))
        }
        None => body_uri,
    };
    let path = PathBuf::from(path);
    Ok(if path.is_absolute() {
        path
    } else if let Some(base_dir) = base_dir {
        base_dir.join(path)
    } else {
        path
    })
}

fn validate_part_shares(
    shares: &[AcceptedShare],
    first_share_seq: u64,
    last_share_seq: u64,
    share_count: usize,
    label: &str,
) -> Result<(), AuditBodyRefError> {
    if shares.len() != share_count {
        return Err(AuditBodyRefError::Invalid(format!(
            "{label} expected {share_count} shares, found {}",
            shares.len()
        )));
    }
    if shares.first().map(|share| share.share_seq) != Some(first_share_seq)
        || shares.last().map(|share| share.share_seq) != Some(last_share_seq)
    {
        return Err(AuditBodyRefError::Invalid(format!(
            "{label} share_seq range does not match metadata"
        )));
    }
    validate_contiguous_shares(shares, label)
}

fn append_contiguous(
    target: &mut Vec<AcceptedShare>,
    mut incoming: Vec<AcceptedShare>,
) -> Result<(), AuditBodyRefError> {
    if let (Some(previous), Some(next)) = (target.last(), incoming.first()) {
        if previous.share_seq + 1 != next.share_seq {
            return Err(AuditBodyRefError::Invalid(format!(
                "share parts are not contiguous: {} then {}",
                previous.share_seq, next.share_seq
            )));
        }
    }
    target.append(&mut incoming);
    Ok(())
}

/// The storage envelopes (`audit-body-ref.v1`, `audit-bundle.v2`) are 2.x
/// artifacts: the cutover import, the read-model API's legacy bodies and the
/// verify and canonicalize CLIs read them, and no 3.x writer produces them. Their completeness checks require a contiguous `share_seq` run, which
/// a dual-writer window with a cut need not be (a late peer row outside the
/// cut leaves a gap). So a window with a cut is refused by name here, never
/// half-checked: such a bundle only ever travels as a flat logical bundle.
fn refuse_window_cut(bundle_without_shares: &Value, label: &str) -> Result<(), AuditBodyRefError> {
    let cut = bundle_without_shares
        .get("reward_manifest")
        .and_then(|manifest| manifest.get("cut"));
    if cut.is_some_and(|cut| !cut.is_null()) {
        return Err(AuditBodyRefError::Invalid(format!(
            "{label} predates dual-writer window cuts: a bundle whose reward manifest has a cut is only valid as a flat audit bundle"
        )));
    }
    Ok(())
}

fn validate_contiguous_shares(
    shares: &[AcceptedShare],
    label: &str,
) -> Result<(), AuditBodyRefError> {
    for pair in shares.windows(2) {
        if pair[0].share_seq + 1 != pair[1].share_seq {
            return Err(AuditBodyRefError::Invalid(format!(
                "{label} has non-contiguous share_seq values: {} then {}",
                pair[0].share_seq, pair[1].share_seq
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod python_json_tests {
    use super::*;

    #[test]
    fn python_json_formatter_writes_cpython_json_dumps_bytes() {
        let value = serde_json::json!({"text": "\"\\\n\u{1}\u{7f}\u{1f}ø€\u{1f980}/"});
        let mut bytes = Vec::new();
        value
            .serialize(&mut serde_json::Serializer::with_formatter(
                &mut bytes,
                PythonJsonFormatter,
            ))
            .unwrap();
        // CPython's json.dumps(value, separators=(",", ":")).
        assert_eq!(
            bytes,
            br#"{"text":"\"\\\n\u0001\u007f\u001f\u00f8\u20ac\ud83e\udd80/"}"#
        );
    }
}

#[cfg(test)]
mod share_parts_digest_tests {
    use super::*;

    /// 2.x.x's v2 body with two segment_range parts; its slots are named by
    /// their production paths (tests/fixtures/prism-legacy-v2-audit-body).
    const SEGMENT_RANGE_BODY: &str = include_str!(
        "../../../tests/fixtures/prism-legacy-v2-audit-body/prism-audit-bundle-body-0000000000000000000000000000000000000000000000000000c8c8c8c8c8c8-65b11e1b7e2025472fad2e4cd6b555eaba5eab2a4903e17179ba792d58780a4b.json"
    );
    /// A v2 body written by 2.x.x's AuditArtifactStore with one inline part
    /// whose workers are non-ASCII (tests/fixtures/prism-legacy-v2-inline-audit-body).
    const INLINE_BODY: &str = include_str!(
        "../../../tests/fixtures/prism-legacy-v2-inline-audit-body/prism-audit-bundle-body-0000000000000000000000000000000000000000000000000000c9c9c9c9c9c9-c328186054819ca745a8dfbd3bc2ab6e9e1a73d88adc0417356781c28b06cbd5.json"
    );

    fn is_parts_digest_mismatch(result: &Result<AuditBundle, AuditBodyRefError>) -> bool {
        matches!(
            result,
            Err(AuditBodyRefError::Invalid(message))
                if message == "window proof share_parts_digest_hex mismatch"
        )
    }

    #[test]
    fn parts_digest_reproduces_the_digests_2x_wrote() {
        for body in [SEGMENT_RANGE_BODY, INLINE_BODY] {
            let body: Value = serde_json::from_str(body).unwrap();
            let proof = &body["share_window_proof"];
            let parts: Vec<SharePart> =
                serde_json::from_value(proof["share_parts"].clone()).unwrap();
            assert_eq!(
                share_parts_digest_hex(&parts).unwrap(),
                proof["share_parts_digest_hex"].as_str().unwrap()
            );
        }
    }

    #[test]
    fn a_2x_inline_body_with_non_ascii_workers_canonicalizes() {
        let bundle = parse_audit_bundle_json(INLINE_BODY, None).unwrap();
        let body: Value = serde_json::from_str(INLINE_BODY).unwrap();
        let canonical = canonical_audit_bundle_bytes(&bundle).unwrap();
        assert_eq!(
            hex::encode(Sha256::digest(&canonical)),
            body["audit_bundle_sha256"].as_str().unwrap()
        );
        assert!(INLINE_BODY.contains("Bj\\u00f8rn-rig") && INLINE_BODY.contains("\\ud83e\\udd80"));
        assert!(bundle.shares[1].miner_id.ends_with(".Bj\u{f8}rn-rig"));
        assert!(bundle.shares[4].miner_id.ends_with(".rig-\u{1f980}"));
    }

    #[test]
    fn a_changed_part_descriptor_or_inline_share_is_refused() {
        let segment_range_body: Value = serde_json::from_str(SEGMENT_RANGE_BODY).unwrap();
        // As 2.x wrote it, the descriptor passes the parts digest and the
        // check goes on to the slots, which are not on this host.
        let unchanged = parse_audit_bundle_value(segment_range_body.clone(), None);
        assert!(!is_parts_digest_mismatch(&unchanged), "{unchanged:?}");

        let parts_path = "/share_window_proof/share_parts";
        type Change = (&'static str, fn(&mut Value));
        let changes: [Change; 5] = [
            ("range digest", |parts| {
                parts[0]["range_sha256"] = Value::from("00".repeat(32));
            }),
            ("slot", |parts| {
                parts[1]["body_uri"] = parts[0]["body_uri"].clone();
            }),
            ("bounds", |parts| {
                parts[0]["last_share_seq"] = Value::from(3);
                parts[0]["share_count"] = Value::from(3);
                parts[1]["first_share_seq"] = Value::from(4);
                parts[1]["share_count"] = Value::from(3);
            }),
            ("segment bounds dropped", |parts| {
                let part = parts[0].as_object_mut().unwrap();
                part.remove("segment_first_share_seq");
                part.remove("segment_last_share_seq");
            }),
            ("part order", |parts| {
                parts.as_array_mut().unwrap().reverse();
            }),
        ];
        for (label, change) in changes {
            let mut body = segment_range_body.clone();
            change(body.pointer_mut(parts_path).unwrap());
            let result = parse_audit_bundle_value(body, None);
            assert!(is_parts_digest_mismatch(&result), "{label}: {result:?}");
        }

        // The form 3.x recomputed before #731, over sorted keys, matches no
        // writer's digest.
        let mut body = segment_range_body;
        let sorted = hex::encode(Sha256::digest(
            serde_json::to_vec(
                &serde_json::json!({ "share_parts": body.pointer(parts_path).unwrap() }),
            )
            .unwrap(),
        ));
        body["share_window_proof"]["share_parts_digest_hex"] = Value::from(sorted);
        let result = parse_audit_bundle_value(body, None);
        assert!(is_parts_digest_mismatch(&result), "sorted keys: {result:?}");

        // A changed inline share is refused by the parts digest.
        let mut body: Value = serde_json::from_str(INLINE_BODY).unwrap();
        let share = body
            .pointer_mut("/share_window_proof/share_parts/0/shares/1")
            .unwrap();
        share["miner_id"] = Value::from("miner-mid-a.Bjorn-rig");
        let result = parse_audit_bundle_value(body, None);
        assert!(
            is_parts_digest_mismatch(&result),
            "inline share: {result:?}"
        );
    }
}
