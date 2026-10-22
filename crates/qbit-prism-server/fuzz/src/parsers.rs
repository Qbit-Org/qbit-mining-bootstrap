//! The codec's byte and number parsers on arbitrary input: transactions,
//! blocks, coinbase splits, compact targets, difficulties, hex fields and
//! template JSON. None may panic or allocate beyond the input's size, and
//! each accepted value must round-trip.
use crate::{alloc, violation};
use num_bigint::BigUint;
use qbit_prism_server::codec::{self, Job};
use serde_json::Value;
use std::sync::OnceLock;

/// Bitcoin's `GetCompact`: the canonical compact encoding of a target.
pub fn compact(target: &BigUint) -> u32 {
    let bytes = target.to_bytes_be();
    let mut size = bytes.len() as u32;
    let mut mantissa = if size <= 3 {
        (target.clone() << (8 * (3 - size) as usize))
            .to_u32_digits()
            .first()
            .copied()
            .unwrap_or(0)
    } else {
        u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]])
    };
    if mantissa & 0x0080_0000 != 0 {
        mantissa >>= 8;
        size += 1;
    }
    (size << 24) | mantissa
}

fn manifest() -> &'static qbit_pool_builder::PayoutManifest {
    static MANIFEST: OnceLock<qbit_pool_builder::PayoutManifest> = OnceLock::new();
    MANIFEST.get_or_init(|| {
        qbit_pool_builder::build_manifest(qbit_pool_builder::CoinbaseBuildRequest {
            block_height: 101,
            coinbase_value_sats: 5_000_000_000,
            entitlements: vec![qbit_pool_builder::WeightedEntitlement {
                recipient_id: "miner".into(),
                order_key: "miner".into(),
                p2mr_program_hex: "ab".repeat(32),
                weight: 1,
            }],
            witness_nonce_hex: Some("00".repeat(32)),
            witness_merkle_leaves_hex: vec![],
            coinbase_script_sig_suffix_hex: Some(format!("12345678{}", "00".repeat(8))),
            pinned_first_output: None,
        })
        .expect("fixed manifest")
    })
}

fn transactions(data: &[u8]) {
    if let Ok(stripped) = codec::strip_witness_transaction(data) {
        if stripped.len() > data.len() {
            violation("stripping witness grew a transaction");
        }
        match codec::strip_witness_transaction(&stripped) {
            Ok(again) if again == stripped => {}
            _ => violation("a stripped transaction does not strip to itself"),
        }
    }
    if let Ok(leaves) = codec::witness_merkle_leaves_from_block(data) {
        if leaves.len() > data.len() / 10
            || leaves
                .iter()
                .any(|l| l.len() != 64 || !l.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            violation("block leaves out of shape");
        }
    }
    if let Some((&cut, rest)) = data.split_first() {
        let cut = usize::from(cut) % 33;
        if rest.len() > cut {
            let (tx, placeholder) = (rest, &rest[rest.len() - cut..]);
            if let Ok((prefix, suffix)) = codec::split_coinbase_extranonce(tx, placeholder) {
                if [prefix.as_slice(), placeholder, &suffix].concat() != tx {
                    violation("a coinbase split does not reassemble");
                }
            }
        }
    }
}

fn numbers(data: &[u8]) {
    let word = |at: usize| {
        data.get(at..at + 4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
    };
    let float = |at: usize| {
        data.get(at..at + 8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
    };
    let max = (BigUint::from(1u8) << 256usize) - 1u8;
    if let Some(bits) = word(0) {
        if let Ok(target) = codec::target_from_compact(bits) {
            if target == BigUint::default() || target > max {
                violation(format!("compact {bits:08x} decoded out of range"));
            }
            if codec::target_from_compact(compact(&target)).ok() != Some(target.clone()) {
                violation(format!("compact {bits:08x} does not round-trip"));
            }
        }
    }
    let (Some(a), Some(b)) = (float(0), float(8)) else {
        return;
    };
    for d in [a, b] {
        let valid = d.is_finite() && d > 0.0;
        match codec::difficulty_target(d) {
            Ok(target) if valid => {
                if target == BigUint::default() || target > max {
                    violation(format!("difficulty {d:e} gave a target out of range"));
                }
                match codec::target_difficulty(&target) {
                    Ok(back) if back.is_finite() && back > 0.0 => {}
                    other => violation(format!("target of {d:e} has difficulty {other:?}")),
                }
                let _ = codec::scaled_target_difficulty(&target);
            }
            Err(_) if !valid => {}
            other => violation(format!("difficulty {d:e} answered {other:?}")),
        }
    }
    if a.is_finite()
        && b.is_finite()
        && 0.0 < a
        && a <= b
        && codec::difficulty_target(a).unwrap() < codec::difficulty_target(b).unwrap()
    {
        violation(format!(
            "a higher difficulty {b:e} gave a larger target than {a:e}"
        ));
    }
}

fn text(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(value) = codec::parse_u32_hex(text) {
        if !format!("{value:08x}").eq_ignore_ascii_case(text) {
            violation(format!("hex field {text:?} does not round-trip"));
        }
    }
    let Ok(template) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let _ = codec::version_mask_from_template(&template, codec::VERSION_ROLLING_MASK);
    if let Ok(transactions) = codec::transactions_from_template(&template) {
        let _ = codec::merkle_branch_for_coinbase(&transactions);
    }
    let _ = Job::from_manifest(
        "job".into(),
        &template,
        manifest(),
        "12345678",
        8,
        1e-9,
        0.0,
        true,
    );
}

pub fn run(data: &[u8]) {
    manifest();
    alloc::bounded((1 << 20) + 16 * data.len(), "a parser iteration", || {
        transactions(data);
        numbers(data);
        text(data);
    });
}
