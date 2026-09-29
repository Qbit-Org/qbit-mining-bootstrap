//! An independent proof-of-work check for credited shares. It rebuilds the
//! header from the job the backend issued and the submit's raw wire fields,
//! sharing nothing with `Job::assemble_submission` except SHA-256.
use num_bigint::BigUint;
use qbit_prism_server::codec::{double_sha256, Job};

fn hex4(field: &str) -> Result<u32, String> {
    if field.len() != 8 || !field.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!("{field:?} is not a 4-byte hex field"));
    }
    u32::from_str_radix(field, 16).map_err(|e| e.to_string())
}

/// The raw `mining.submit` fields after the username and job ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawProof {
    pub extranonce2: String,
    pub ntime: String,
    pub nonce: String,
    pub version_bits: Option<String>,
}

/// The header hash these fields prove on `job`, or why they are not a
/// well-formed proof for it under the negotiated `mask`.
pub fn header_hash(job: &Job, mask: u32, proof: &RawProof) -> Result<BigUint, String> {
    let extranonce2 = &proof.extranonce2;
    if extranonce2.len() != job.extranonce2_size * 2 {
        return Err(format!("extranonce2 {extranonce2:?} has the wrong size"));
    }
    let ntime = hex4(&proof.ntime)?;
    let nonce = hex4(&proof.nonce)?;
    if ntime < job.mintime {
        return Err(format!("ntime {ntime} precedes mintime {}", job.mintime));
    }
    let version = match &proof.version_bits {
        None => job.version,
        Some(bits) => {
            let bits = hex4(bits)?;
            if bits & !mask != 0 {
                return Err(format!("version bits {bits:08x} outside mask {mask:08x}"));
            }
            (job.version & !mask) | bits
        }
    };
    let preimage = hex::decode(format!(
        "{}{}{}{}",
        job.coinb1, job.extranonce1, extranonce2, job.coinb2
    ))
    .map_err(|e| format!("coinbase preimage: {e}"))?;
    let mut root = double_sha256(&preimage);
    for sibling in &job.merkle_branch {
        let mut pair = root.to_vec();
        pair.extend_from_slice(sibling);
        root = double_sha256(&pair);
    }
    let mut previous = hex::decode(&job.previousblockhash).map_err(|e| e.to_string())?;
    previous.reverse();
    let mut header = Vec::with_capacity(80);
    header.extend_from_slice(&version.to_le_bytes());
    header.extend_from_slice(&previous);
    header.extend_from_slice(&root);
    header.extend_from_slice(&ntime.to_le_bytes());
    header.extend_from_slice(&job.nbits.to_le_bytes());
    header.extend_from_slice(&nonce.to_le_bytes());
    if header.len() != 80 {
        return Err("header is not 80 bytes".into());
    }
    Ok(BigUint::from_bytes_le(&double_sha256(&header)))
}

/// A nonce, searched from `start`, whose header meets `job`'s share target.
pub fn solve(job: &Job, mask: u32, proof: &RawProof, start: u32) -> Option<String> {
    (0..256u32).find_map(|step| {
        let nonce = format!("{:08x}", start.wrapping_add(step));
        let candidate = RawProof {
            nonce: nonce.clone(),
            ..proof.clone()
        };
        header_hash(job, mask, &candidate)
            .ok()
            .filter(|hash| *hash <= job.share_target)
            .map(|_| nonce)
    })
}
