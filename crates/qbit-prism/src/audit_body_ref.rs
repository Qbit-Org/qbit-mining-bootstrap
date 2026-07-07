use crate::{
    build_prism_reward_manifest, canonical_audit_bundle_bytes, AcceptedShare, AuditBundle,
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

#[derive(Debug, Deserialize, Serialize)]
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
        let actual_digest = hex::encode(Sha256::digest(serde_json::to_vec(
            &serde_json::json!({ "share_parts": &share_parts }),
        )?));
        if !expected_digest.eq_ignore_ascii_case(&actual_digest) {
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
    let expected_reward_manifest = build_prism_reward_manifest(&bundle.shares, &bundle.found_block)
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
            let segment_path = resolve_body_uri(&body_uri, base_dir);
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
            let segment_path = resolve_body_uri(&body_uri, base_dir);
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
            let actual_sha256 =
                canonical_share_segment_sha256(first_share_seq, last_share_seq, &selected)?;
            let expected_sha256 = range_sha256.to_lowercase();
            if actual_sha256 != expected_sha256 {
                return Err(AuditBodyRefError::HashMismatch {
                    expected: expected_sha256,
                    actual: actual_sha256,
                });
            }
            append_contiguous(shares, selected)?;
        }
        SharePart::SegmentPrefix {
            first_share_seq,
            last_share_seq,
            share_count,
            prefix_sha256,
            body_uri,
        } => {
            let segment_path = resolve_body_uri(&body_uri, base_dir);
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
            let actual_sha256 =
                canonical_share_segment_sha256(first_share_seq, last_share_seq, &selected)?;
            let expected_sha256 = prefix_sha256.to_lowercase();
            if actual_sha256 != expected_sha256 {
                return Err(AuditBodyRefError::HashMismatch {
                    expected: expected_sha256,
                    actual: actual_sha256,
                });
            }
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
    validate_contiguous_shares(&segment_shares, label)?;
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

fn canonical_share_segment_sha256(
    first_share_seq: u64,
    last_share_seq: u64,
    shares: &[AcceptedShare],
) -> Result<String, AuditBodyRefError> {
    let segment = AuditShareSegment {
        schema: AUDIT_SHARE_SEGMENT_SCHEMA.to_string(),
        first_share_seq,
        last_share_seq,
        share_count: shares.len(),
        shares: shares.to_vec(),
    };
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&segment)?)))
}

fn resolve_body_uri(body_uri: &str, base_dir: Option<&Path>) -> PathBuf {
    let path = PathBuf::from(body_uri);
    if path.is_absolute() {
        path
    } else if let Some(base_dir) = base_dir {
        base_dir.join(path)
    } else {
        path
    }
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
