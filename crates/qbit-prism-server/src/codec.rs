//! Transport-independent Stratum v1 transaction and proof-of-work codec.
use anyhow::{bail, ensure, Context, Result};
use num_bigint::BigUint;
use num_traits::{One, ToPrimitive, Zero};
use qbit_pool_builder::PayoutManifest;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Instant};

pub const VERSION_ROLLING_MASK: u32 = 0x1fff_e000;

pub fn double_sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(Sha256::digest(bytes)).into()
}

pub fn hash_display(hash: &[u8; 32]) -> String {
    hex::encode(hash.iter().rev().copied().collect::<Vec<_>>())
}

pub fn compact_size(value: u64) -> Vec<u8> {
    match value {
        0..=252 => vec![value as u8],
        253..=0xffff => [vec![253], (value as u16).to_le_bytes().to_vec()].concat(),
        0x10000..=0xffff_ffff => [vec![254], (value as u32).to_le_bytes().to_vec()].concat(),
        _ => [vec![255], value.to_le_bytes().to_vec()].concat(),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .context("transaction length overflow")?;
        let part = self
            .bytes
            .get(self.offset..end)
            .context("truncated transaction")?;
        self.offset = end;
        Ok(part)
    }

    fn count(&mut self) -> Result<usize> {
        let first = self.take(1)?[0];
        let value = match first {
            253 => u16::from_le_bytes(self.take(2)?.try_into()?) as u64,
            254 => u32::from_le_bytes(self.take(4)?.try_into()?) as u64,
            255 => u64::from_le_bytes(self.take(8)?.try_into()?),
            other => other as u64,
        };
        ensure!(
            match first {
                253 => value >= 253,
                254 => value > 0xffff,
                255 => value > 0xffff_ffff,
                _ => true,
            },
            "noncanonical compact size"
        );
        let value: usize = value.try_into().context("transaction count overflow")?;
        ensure!(
            value <= self.bytes.len(),
            "transaction count exceeds input size"
        );
        Ok(value)
    }
}

/// Where one serialized transaction's parts sit inside the slice it starts at.
/// A block walker needs `len`; the txid preimage needs the version, the
/// witness-free body and the locktime.
struct TransactionLayout {
    body: std::ops::Range<usize>,
    locktime: std::ops::Range<usize>,
    len: usize,
}

/// Walk one segwit-aware transaction from the start of `bytes`, which may hold
/// more than that transaction. Returns where it ends, so a caller reading a
/// block's transactions back to back can advance past each one.
fn walk_transaction(bytes: &[u8]) -> Result<TransactionLayout> {
    ensure!(bytes.len() >= 10, "transaction is too short");
    let mut c = Cursor { bytes, offset: 4 };
    let witness = bytes[4] == 0 && bytes[5] != 0;
    if witness {
        ensure!(bytes[5] == 1, "unsupported witness transaction flags");
        c.take(2)?;
    }
    let body_start = c.offset;
    let inputs = c.count()?;
    ensure!(inputs > 0, "transaction has no inputs");
    for _ in 0..inputs {
        c.take(36)?;
        let n = c.count()?;
        c.take(n)?;
        c.take(4)?;
    }
    let outputs = c.count()?;
    for _ in 0..outputs {
        c.take(8)?;
        let n = c.count()?;
        c.take(n)?;
    }
    let body_end = c.offset;
    if witness {
        for _ in 0..inputs {
            let items = c.count()?;
            for _ in 0..items {
                let n = c.count()?;
                c.take(n)?;
            }
        }
    }
    let locktime_start = c.offset;
    c.take(4)?;
    Ok(TransactionLayout {
        body: body_start..body_end,
        locktime: locktime_start..c.offset,
        len: c.offset,
    })
}

/// Strip witness structurally; txids never hash witness marker, flag or stacks.
pub fn strip_witness_transaction(tx: &[u8]) -> Result<Vec<u8>> {
    let layout = walk_transaction(tx)?;
    ensure!(layout.len == tx.len(), "transaction has trailing bytes");
    Ok([&tx[..4], &tx[layout.body], &tx[layout.locktime]].concat())
}

/// The witness merkle leaves of an assembled block: `double_sha256` of every
/// non-coinbase transaction's full serialized bytes, lowercase hex, in block
/// order. Identical by construction to
/// [`witness_merkle_leaves_hex`]`(&`[`transactions_from_template`]`(template))`
/// for a block [`Job::assemble_submission`] built from that template, which
/// appends the template's transactions verbatim after the coinbase.
///
/// A claim re-derives the leaves this way instead of storing one 64-hex string
/// per transaction beside the candidate, which would scale the stored document
/// with the block. The bytes are authenticated before this runs: the claim
/// checks them against `block_sha256` and their 80-byte header against
/// `block_hash`.
pub fn witness_merkle_leaves_from_block(block: &[u8]) -> Result<Vec<String>> {
    ensure!(block.len() >= 80, "truncated block header");
    let mut c = Cursor {
        bytes: block,
        offset: 80,
    };
    let count = c.count().context("block transaction count")?;
    // A block always has its coinbase. Zero would also make `count - 1`
    // underflow below.
    ensure!(count > 0, "block declares no transactions");
    let mut offset = c.offset;
    let mut leaves = Vec::with_capacity(count - 1);
    for index in 0..count {
        let rest = block
            .get(offset..)
            .context("truncated block transaction list")?;
        let layout = walk_transaction(rest)
            .with_context(|| format!("block transaction {index} of {count}"))?;
        // The coinbase is not a witness merkle leaf; the commitment uses the
        // all-zero placeholder for it, exactly as the template path does.
        if index > 0 {
            leaves.push(hex::encode(double_sha256(&rest[..layout.len])));
        }
        offset += layout.len;
    }
    ensure!(
        offset == block.len(),
        "block has trailing bytes after {count} transactions"
    );
    Ok(leaves)
}

pub fn split_coinbase_extranonce(tx: &[u8], placeholder: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    ensure!(
        tx.len() >= 10 && !placeholder.is_empty(),
        "invalid coinbase or extranonce placeholder"
    );
    let mut c = Cursor {
        bytes: tx,
        offset: 4,
    };
    if tx[4] == 0 && tx[5] != 0 {
        c.take(2)?;
    }
    ensure!(c.count()? == 1, "coinbase must have exactly one input");
    let prevout = c.take(36)?;
    ensure!(
        prevout[..32] == [0; 32] && prevout[32..] == [255; 4],
        "coinbase has non-null prevout"
    );
    let n = c.count()?;
    let script = c.take(n)?;
    ensure!(
        script.ends_with(placeholder),
        "coinbase scriptSig does not end with extranonce placeholder"
    );
    let start = c.offset - placeholder.len();
    Ok((tx[..start].to_vec(), tx[c.offset..].to_vec()))
}

pub fn target_from_compact(bits: u32) -> Result<BigUint> {
    let size = bits >> 24;
    let mantissa = bits & 0x007f_ffff;
    ensure!(
        bits & 0x0080_0000 == 0 && mantissa != 0,
        "negative or zero compact target"
    );
    ensure!(size <= 34, "compact target overflow");
    let value = if size <= 3 {
        BigUint::from(mantissa >> (8 * (3 - size)))
    } else {
        BigUint::from(mantissa) << (8 * (size - 3)) as usize
    };
    ensure!(
        !value.is_zero() && value.bits() <= 256,
        "compact target out of range"
    );
    Ok(value)
}

pub fn difficulty_target(difficulty: f64) -> Result<BigUint> {
    ensure!(
        difficulty.is_finite() && difficulty > 0.0,
        "difficulty must be positive and finite"
    );
    // Divide by the exact binary rational advertised in JSON, avoiding an f64
    // conversion of a 256-bit target near a proof-of-work boundary.
    let bits = difficulty.to_bits();
    let raw_exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = (bits & ((1_u64 << 52) - 1)) | if raw_exponent == 0 { 0 } else { 1_u64 << 52 };
    let exponent = if raw_exponent == 0 {
        -1074
    } else {
        raw_exponent - 1023 - 52
    };
    let mut numerator = target_from_compact(0x1d00ffff)?;
    let mut denominator = BigUint::from(mantissa);
    if exponent < 0 {
        numerator <<= (-exponent) as usize;
    } else {
        denominator <<= exponent as usize;
    }
    Ok((numerator / denominator)
        .max(BigUint::one())
        .min((BigUint::one() << 256usize) - BigUint::one()))
}

pub fn target_difficulty(target: &BigUint) -> Result<f64> {
    ensure!(!target.is_zero(), "target must be positive");
    Ok(target_from_compact(0x1d00ffff)?
        .to_f64()
        .context("difficulty conversion")?
        / target.to_f64().context("target conversion")?)
}

pub fn scaled_target_difficulty(target: &BigUint) -> Result<u128> {
    ensure!(!target.is_zero(), "target must be positive");
    ((target_from_compact(0x207fffff)? * BigUint::from(1_000_000_u64)) / target)
        .max(BigUint::one())
        .to_u128()
        .context("scaled difficulty exceeds u128")
}

pub fn parse_u32_hex(value: &str) -> Result<u32> {
    ensure!(
        value.len() == 8 && value.bytes().all(|c| c.is_ascii_hexdigit()),
        "expected a 4-byte hex string"
    );
    Ok(u32::from_str_radix(value, 16)?)
}

pub fn version_mask_from_template(template: &Value, fallback: u32) -> Result<u32> {
    match template.get("versionrollingmask") {
        None => Ok(fallback),
        Some(Value::Number(n)) => Ok(n
            .as_u64()
            .context("invalid versionrollingmask")?
            .try_into()?),
        Some(Value::String(s)) => {
            let s = s.strip_prefix("0x").unwrap_or(s);
            ensure!(
                !s.is_empty() && s.len() <= 8 && s.bytes().all(|c| c.is_ascii_hexdigit()),
                "invalid versionrollingmask"
            );
            Ok(u32::from_str_radix(s, 16)?)
        }
        _ => bail!("invalid versionrollingmask"),
    }
}

pub fn merkle_branch_for_coinbase(transactions: &[Vec<u8>]) -> Result<Vec<[u8; 32]>> {
    let mut level = vec![[0; 32]];
    for tx in transactions {
        level.push(double_sha256(&strip_witness_transaction(tx)?));
    }
    let mut branch = Vec::new();
    while level.len() > 1 {
        branch.push(level[1]);
        if level.len() % 2 != 0 {
            level.push(*level.last().unwrap());
        }
        level = level
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| double_sha256(&[p[0].as_slice(), p[1].as_slice()].concat()))
            .collect();
    }
    Ok(branch)
}

pub fn transactions_from_template(template: &Value) -> Result<Vec<Vec<u8>>> {
    let empty = Vec::new();
    let transactions = match template.get("transactions") {
        None => &empty,
        Some(value) => value
            .as_array()
            .context("template transactions must be an array")?,
    };
    transactions
        .iter()
        .map(|t| {
            Ok(hex::decode(
                t["data"].as_str().context("transaction data missing")?,
            )?)
        })
        .collect()
}

pub fn witness_merkle_leaves_hex(transactions: &[Vec<u8>]) -> Vec<String> {
    transactions
        .iter()
        .map(|tx| hex::encode(double_sha256(tx)))
        .collect()
}

/// What issued work may still earn (#478).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum JobKind {
    /// Ordinary work: its share may be credited under the usual fences.
    #[default]
    Credit,
    /// Work retired by a same-parent payout replacement. It may still carry a
    /// block to the node while its parent is the active tip, but it reaches no
    /// credit path at all: no stale grace, no parent grace, and no deferred
    /// share on a captured block.
    BlockOnly,
}

#[derive(Clone, Debug)]
pub struct Job {
    pub job_id: String,
    pub previousblockhash: String,
    pub prevhash: String,
    pub coinb1: Arc<str>,
    pub coinb2: Arc<str>,
    pub full_coinbase_prefix: Arc<str>,
    pub full_coinbase_suffix: Arc<str>,
    pub merkle_branch: Vec<[u8; 32]>,
    pub transactions: Arc<Vec<Vec<u8>>>,
    pub version: u32,
    pub version_mask: u32,
    pub nbits: u32,
    pub ntime: u32,
    pub mintime: u32,
    pub network_target: BigUint,
    pub share_target: BigUint,
    pub share_difficulty: f64,
    pub extranonce1: String,
    pub extranonce2_size: usize,
    pub clean_jobs: bool,
    /// Absolute expiry of remotely restored work; reconnects never slide it.
    pub resume_expires_at: Option<Instant>,
    pub refresh_generation: u64,
    /// Payout state is invalidated independently of the parent block hash.
    pub payout_revision: i64,
    /// Set only by the Stratum session that retires this work; never on the wire.
    pub kind: JobKind,
}

impl Job {
    #[allow(clippy::too_many_arguments)]
    pub fn from_manifest(
        job_id: String,
        template: &Value,
        manifest: &PayoutManifest,
        extranonce1: &str,
        extranonce2_size: usize,
        desired_difficulty: f64,
        minimum_difficulty: f64,
        clean_jobs: bool,
    ) -> Result<Self> {
        ensure!(
            extranonce2_size > 0 && extranonce2_size <= 32,
            "invalid extranonce2 size"
        );
        ensure!(
            minimum_difficulty.is_finite() && minimum_difficulty >= 0.0,
            "invalid difficulty floor"
        );
        let coinbase = hex::decode(&manifest.coinbase_tx_hex)?;
        let stripped = strip_witness_transaction(&coinbase)?;
        ensure!(
            hash_display(&double_sha256(&stripped)).eq_ignore_ascii_case(&manifest.coinbase_txid),
            "manifest coinbase txid mismatch"
        );
        let mut placeholder = hex::decode(extranonce1)?;
        placeholder.resize(placeholder.len() + extranonce2_size, 0);
        let (coinb1, coinb2) = split_coinbase_extranonce(&stripped, &placeholder)?;
        let (full_prefix, full_suffix) = split_coinbase_extranonce(&coinbase, &placeholder)?;
        let transactions = transactions_from_template(template)?;
        let merkle_branch = merkle_branch_for_coinbase(&transactions)?;
        let previousblockhash = template["previousblockhash"]
            .as_str()
            .context("template previousblockhash missing")?
            .to_ascii_lowercase();
        let mut prevhash_bytes = hex::decode(&previousblockhash)?;
        ensure!(
            prevhash_bytes.len() == 32,
            "previousblockhash must be 32 bytes"
        );
        prevhash_bytes.reverse();
        for word in prevhash_bytes.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let nbits = parse_u32_hex(template["bits"].as_str().context("template bits missing")?)?;
        let network_target = target_from_compact(nbits)?;
        let mut share_target = difficulty_target(desired_difficulty)?.max(network_target.clone());
        if minimum_difficulty > 0.0 {
            share_target = share_target.min(difficulty_target(minimum_difficulty)?);
        }
        let version = template["version"]
            .as_i64()
            .context("template version missing")? as u32;
        let ntime: u32 = template["curtime"]
            .as_u64()
            .context("template curtime missing")?
            .try_into()?;
        let mintime = template
            .get("mintime")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .try_into()?;
        Ok(Self {
            job_id,
            previousblockhash,
            prevhash: hex::encode(prevhash_bytes),
            coinb1: hex::encode(coinb1).into(),
            coinb2: hex::encode(coinb2).into(),
            full_coinbase_prefix: hex::encode(full_prefix).into(),
            full_coinbase_suffix: hex::encode(full_suffix).into(),
            merkle_branch,
            transactions: Arc::new(transactions),
            version,
            version_mask: version_mask_from_template(template, VERSION_ROLLING_MASK)?,
            nbits,
            ntime,
            mintime,
            share_difficulty: target_difficulty(&share_target)?,
            network_target,
            share_target,
            extranonce1: extranonce1.to_owned(),
            extranonce2_size,
            clean_jobs,
            resume_expires_at: None,
            refresh_generation: 0,
            payout_revision: 0,
            kind: JobKind::Credit,
        })
    }

    pub fn notify(&self) -> Value {
        json!({"id":null,"method":"mining.notify","params":[self.job_id,self.prevhash,self.coinb1,self.coinb2,
            self.merkle_branch.iter().map(hex::encode).collect::<Vec<_>>(),format!("{:08x}",self.version),
            format!("{:08x}",self.nbits),format!("{:08x}",self.ntime),self.clean_jobs]})
    }

    /// Stamp shared template work with connection entropy and its assigned
    /// target. Transaction hashes and the large coinbase buffers stay shared.
    pub fn reassign(
        &self,
        job_id: String,
        extranonce1: &str,
        difficulty: f64,
        minimum_difficulty: f64,
    ) -> Result<Self> {
        ensure!(
            extranonce1.len() == self.extranonce1.len(),
            "unexpected extranonce1 size"
        );
        hex::decode(extranonce1).context("invalid extranonce1")?;
        ensure!(
            minimum_difficulty.is_finite() && minimum_difficulty >= 0.0,
            "invalid difficulty floor"
        );
        let mut job = self.clone();
        job.job_id = job_id;
        job.extranonce1 = extranonce1.into();
        job.share_target = difficulty_target(difficulty)?.max(job.network_target.clone());
        if minimum_difficulty > 0.0 {
            job.share_target = job.share_target.min(difficulty_target(minimum_difficulty)?);
        }
        job.share_difficulty = target_difficulty(&job.share_target)?;
        Ok(job)
    }

    pub fn assemble_submission(
        &self,
        extranonce2: &str,
        ntime: &str,
        nonce: &str,
        version_bits: Option<&str>,
        version_mask: u32,
    ) -> Result<Submission> {
        ensure!(
            extranonce2.len() == self.extranonce2_size * 2,
            "unexpected extranonce2 size"
        );
        hex::decode(extranonce2).context("invalid extranonce2")?;
        let ntime = parse_u32_hex(ntime)?;
        let nonce = parse_u32_hex(nonce)?;
        ensure!(ntime >= self.mintime, "ntime precedes template mintime");
        let applied_version = match version_bits {
            None => self.version,
            Some(value) => {
                let bits = parse_u32_hex(value)?;
                ensure!(
                    bits & !version_mask == 0,
                    "version_bits include bits outside negotiated mask"
                );
                (self.version & !version_mask) | bits
            }
        };
        let coinbase = hex::decode(format!(
            "{}{}{}{}",
            self.full_coinbase_prefix, self.extranonce1, extranonce2, self.full_coinbase_suffix
        ))?;
        let preimage = hex::decode(format!(
            "{}{}{}{}",
            self.coinb1, self.extranonce1, extranonce2, self.coinb2
        ))?;
        ensure!(
            strip_witness_transaction(&coinbase)? == preimage,
            "coinbase witness and txid preimage mismatch"
        );
        let mut merkle = double_sha256(&preimage);
        for sibling in &self.merkle_branch {
            merkle = double_sha256(&[merkle.as_slice(), sibling.as_slice()].concat());
        }
        let mut previous = hex::decode(&self.previousblockhash)?;
        previous.reverse();
        let header = [
            applied_version.to_le_bytes().as_slice(),
            previous.as_slice(),
            merkle.as_slice(),
            ntime.to_le_bytes().as_slice(),
            self.nbits.to_le_bytes().as_slice(),
            nonce.to_le_bytes().as_slice(),
        ]
        .concat();
        ensure!(header.len() == 80, "invalid header size");
        let hash = double_sha256(&header);
        let hash_int = BigUint::from_bytes_le(&hash);
        let block_pass = hash_int <= self.network_target;
        let mut block = Vec::new();
        if block_pass {
            block.extend(&header);
            block.extend(compact_size(1 + self.transactions.len() as u64));
            block.extend(&coinbase);
            for transaction in self.transactions.iter() {
                block.extend(transaction);
            }
        }
        Ok(Submission {
            coinbase_tx_hex: hex::encode(coinbase),
            header_hex: hex::encode(header),
            block_hex: hex::encode(block),
            block_hash_hex: hash_display(&hash),
            share_pass: hash_int <= self.share_target,
            block_pass,
            extranonce2_hex: extranonce2.to_ascii_lowercase(),
            ntime,
            nonce,
            applied_version,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Submission {
    pub coinbase_tx_hex: String,
    pub header_hex: String,
    pub block_hex: String,
    pub block_hash_hex: String,
    pub share_pass: bool,
    pub block_pass: bool,
    pub extranonce2_hex: String,
    pub ntime: u32,
    pub nonce: u32,
    pub applied_version: u32,
}

#[cfg(test)]
mod witness_leaf_tests {
    use super::*;
    use qbit_pool_builder::{build_manifest, CoinbaseBuildRequest, WeightedEntitlement};

    /// A pre-segwit transaction: one input, one output, no marker or witness.
    fn legacy_transaction(tag: u8) -> Vec<u8> {
        let mut tx = vec![1, 0, 0, 0, 1];
        tx.extend([tag; 32]);
        tx.extend(0u32.to_le_bytes());
        tx.extend([1, 0x51]);
        tx.extend(u32::MAX.to_le_bytes());
        tx.push(1);
        tx.extend(1_000u64.to_le_bytes());
        tx.extend([1, 0x51]);
        tx.extend(0u32.to_le_bytes());
        tx
    }

    /// The same shape with the segwit marker, flag and a one-item witness
    /// stack, so its `double_sha256` covers bytes a txid never would.
    fn segwit_transaction(tag: u8) -> Vec<u8> {
        let mut tx = vec![2, 0, 0, 0, 0, 1, 1];
        tx.extend([tag; 32]);
        tx.extend(1u32.to_le_bytes());
        tx.push(0);
        tx.extend(u32::MAX.to_le_bytes());
        tx.push(1);
        tx.extend(2_000u64.to_le_bytes());
        tx.extend([1, 0x51]);
        tx.extend([1, 2, 0xaa, 0xbb]);
        tx.extend(0u32.to_le_bytes());
        tx
    }

    /// A block `assemble_submission` produced from a template holding exactly
    /// these transactions, with the template beside it.
    fn assembled_block(transactions: &[Vec<u8>]) -> (Vec<u8>, Value) {
        let manifest = build_manifest(CoinbaseBuildRequest {
            block_height: 101,
            coinbase_value_sats: 5_000_000_000,
            entitlements: vec![WeightedEntitlement {
                recipient_id: "miner".into(),
                order_key: "miner".into(),
                p2mr_program_hex: "ab".repeat(32),
                weight: 1,
            }],
            witness_nonce_hex: Some("00".repeat(32)),
            witness_merkle_leaves_hex: witness_merkle_leaves_hex(transactions),
            coinbase_script_sig_suffix_hex: Some(format!("505249534d12345678{}", "00".repeat(8))),
            pinned_first_output: None,
        })
        .unwrap();
        // The largest target this codec represents, so all but one hash in
        // 65,536 is a block: this test is about the block's bytes, not proof
        // of work, and the nonce search below makes the fixture deterministic.
        let template = json!({"height":101,"coinbasevalue":5_000_000_000u64,
            "previousblockhash":"0123456789abcdef".repeat(4),"version":0x20000000u32,
            "bits":"2100ffff","curtime":1_700_000_000u32,"mintime":1_699_999_999u32,
            "transactions": transactions.iter().map(|tx| json!({"data": hex::encode(tx)}))
                .collect::<Vec<_>>()});
        let job = Job::from_manifest(
            "job".into(),
            &template,
            &manifest,
            "12345678",
            8,
            1e-9,
            0.0,
            true,
        )
        .unwrap();
        let submission = (0u32..64)
            .map(|nonce| {
                job.assemble_submission(
                    "0000000000000000",
                    "6553f100",
                    &format!("{nonce:08x}"),
                    None,
                    VERSION_ROLLING_MASK,
                )
                .unwrap()
            })
            .find(|submission| submission.block_pass)
            .expect("fixture block must carry its bytes");
        (hex::decode(&submission.block_hex).unwrap(), template)
    }

    #[test]
    fn block_derived_leaves_equal_the_template_derived_leaves() {
        for transactions in [
            vec![],
            vec![legacy_transaction(1)],
            vec![segwit_transaction(2)],
            vec![
                legacy_transaction(3),
                segwit_transaction(4),
                segwit_transaction(5),
                legacy_transaction(6),
            ],
        ] {
            let (block, template) = assembled_block(&transactions);
            let expected =
                witness_merkle_leaves_hex(&transactions_from_template(&template).unwrap());
            assert_eq!(
                witness_merkle_leaves_from_block(&block).unwrap(),
                expected,
                "{} transactions",
                transactions.len()
            );
            // The coinbase is excluded, and every leaf is lowercase 64-hex.
            assert_eq!(expected.len(), transactions.len());
            assert!(expected
                .iter()
                .all(|leaf| leaf.len() == 64 && leaf.bytes().all(|b| b.is_ascii_hexdigit())));
        }
    }

    #[test]
    fn malformed_blocks_are_rejected_rather_than_yielding_partial_leaves() {
        let transactions = vec![legacy_transaction(7), segwit_transaction(8)];
        let (block, _) = assembled_block(&transactions);
        let header = &block[..80];
        // Three transactions, so the count is one compact-size byte.
        let body = &block[81..];
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("truncated block header", block[..79].to_vec()),
            (
                "truncated block transaction list",
                block[..block.len() - 3].to_vec(),
            ),
            (
                "block has trailing bytes",
                [block.as_slice(), &[0u8]].concat(),
            ),
            (
                "block declares no transactions",
                [header, &compact_size(0)].concat(),
            ),
            (
                "count above the transactions present",
                [header, &compact_size(4), body].concat(),
            ),
            (
                "count below the transactions present",
                [header, &compact_size(2), body].concat(),
            ),
        ];
        for (case, bytes) in cases {
            assert!(
                witness_merkle_leaves_from_block(&bytes).is_err(),
                "accepted {case}"
            );
        }
        assert!(witness_merkle_leaves_from_block(&block).is_ok());
    }
}
