//! One canonical field definition for whole and split audit serialization.
use super::*;
use serde::ser::SerializeStruct;
use std::io::Write;

impl Serialize for AuditBundleRef<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("AuditBundle", self.field_count() + 2)?;
        state.serialize_field("schema", self.schema)?;
        state.serialize_field("shares", self.shares)?;
        self.serialize_suffix(state)
    }
}

impl AuditBundleRef<'_> {
    fn field_count(&self) -> usize {
        7 + usize::from(self.coinbase_script_sig_suffix_hex.is_some())
            + usize::from(!self.witness_merkle_leaves_hex.is_empty())
            + usize::from(!self.audit_commitment_leaves_hex.is_empty())
            + usize::from(self.audit_commitment_root_hex.is_some())
            + usize::from(self.settlement_mode_decision.is_some())
            + usize::from(self.ctv_fanout_fee_policy.is_some())
            + usize::from(self.ctv_fanout_manifest_set.is_some())
    }

    fn serialize_suffix<S: SerializeStruct>(&self, mut state: S) -> Result<S::Ok, S::Error> {
        state.serialize_field("found_block", self.found_block)?;
        state.serialize_field("prior_balances", self.prior_balances)?;
        state.serialize_field("payout_policy", self.payout_policy)?;
        if self.coinbase_script_sig_suffix_hex.is_some() {
            state.serialize_field(
                "coinbase_script_sig_suffix_hex",
                self.coinbase_script_sig_suffix_hex,
            )?;
        } else {
            state.skip_field("coinbase_script_sig_suffix_hex")?;
        }
        if !self.witness_merkle_leaves_hex.is_empty() {
            state.serialize_field("witness_merkle_leaves_hex", self.witness_merkle_leaves_hex)?;
        } else {
            state.skip_field("witness_merkle_leaves_hex")?;
        }
        if !self.audit_commitment_leaves_hex.is_empty() {
            state.serialize_field(
                "audit_commitment_leaves_hex",
                self.audit_commitment_leaves_hex,
            )?;
        } else {
            state.skip_field("audit_commitment_leaves_hex")?;
        }
        if self.audit_commitment_root_hex.is_some() {
            state.serialize_field("audit_commitment_root_hex", self.audit_commitment_root_hex)?;
        } else {
            state.skip_field("audit_commitment_root_hex")?;
        }
        state.serialize_field("ledger_window_attestation", self.ledger_window_attestation)?;
        state.serialize_field("reward_manifest", self.reward_manifest)?;
        state.serialize_field("payout_policy_manifest", self.payout_policy_manifest)?;
        if self.settlement_mode_decision.is_some() {
            state.serialize_field("settlement_mode_decision", self.settlement_mode_decision)?;
        } else {
            state.skip_field("settlement_mode_decision")?;
        }
        if self.ctv_fanout_fee_policy.is_some() {
            state.serialize_field("ctv_fanout_fee_policy", self.ctv_fanout_fee_policy)?;
        } else {
            state.skip_field("ctv_fanout_fee_policy")?;
        }
        if self.ctv_fanout_manifest_set.is_some() {
            state.serialize_field("ctv_fanout_manifest_set", self.ctv_fanout_manifest_set)?;
        } else {
            state.skip_field("ctv_fanout_manifest_set")?;
        }
        state.serialize_field("signed_coinbase_manifest", self.signed_coinbase_manifest)?;
        state.end()
    }
}

struct Suffix<'a>(AuditBundleRef<'a>);

impl Serialize for Suffix<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0
            .serialize_suffix(serializer.serialize_struct("AuditBundle", self.0.field_count())?)
    }
}

pub(super) struct DigestWriter(pub Sha256);

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Replace only the serializer's opening object brace; all fields and escaping
/// still go through the same serializer as the complete canonical bundle.
struct ContinueObject<W> {
    writer: W,
    started: bool,
}

impl<W: Write> Write for ContinueObject<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if !self.started && !bytes.is_empty() {
            if bytes[0] != b'{' {
                return Err(std::io::Error::other("audit suffix must be an object"));
            }
            self.writer.write_all(b",")?;
            self.started = true;
            self.writer.write_all(&bytes[1..])?;
            Ok(bytes.len())
        } else {
            self.writer.write(bytes)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

fn write_prefix(
    mut writer: impl Write,
    schema: &str,
    shares: &[AcceptedShare],
) -> Result<(), serde_json::Error> {
    writer
        .write_all(b"{\"schema\":")
        .map_err(serde_json::Error::io)?;
    serde_json::to_writer(&mut writer, schema)?;
    writer
        .write_all(b",\"shares\":")
        .map_err(serde_json::Error::io)?;
    serde_json::to_writer(writer, shares)
}

fn write_suffix(writer: impl Write, body: &AuditBundleBody) -> Result<(), serde_json::Error> {
    serde_json::to_writer(
        ContinueObject {
            writer,
            started: false,
        },
        &Suffix(AuditBundleRef::from_parts(body, &[])),
    )
}

/// A single build's canonical SHA256 state after `schema` and accepted `shares`.
/// It owns no share array and is deliberately consumed on completion. This is
/// not a native window digest or a proof authorizing reuse across refreshes.
pub struct CanonicalAuditHashPrefix {
    writer: DigestWriter,
    schema: &'static str,
}

impl CanonicalAuditHashPrefix {
    /// Hash the original ordered window while its audit body is being built.
    pub fn new(shares: &[AcceptedShare]) -> Result<Self, serde_json::Error> {
        let schema = audit_bundle_schema_for_shares(shares);
        let mut writer = DigestWriter(Sha256::new());
        write_prefix(&mut writer, schema, shares)?;
        Ok(Self { writer, schema })
    }

    /// Finish with the body built from the exact same immutable share window.
    /// As with `AuditBundleBody::into_bundle`, the caller owns that association;
    /// this performs no membership verification and grants no publication right.
    pub fn finish(mut self, body: &AuditBundleBody) -> Result<String, PrismError> {
        if body.schema != self.schema {
            return Err(PrismError::AuditMismatch { artifact: "schema" });
        }
        write_suffix(&mut self.writer, body)?;
        Ok(hex::encode(self.writer.0.finalize()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (AuditBundleBody, Vec<AcceptedShare>) {
        let input: serde_json::Value = serde_json::from_str(include_str!(
            "../fixtures/bootstrap-small-log.prism-fixture.json"
        ))
        .unwrap();
        let shares: Vec<AcceptedShare> = serde_json::from_value(input["shares"].clone()).unwrap();
        let key = ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap();
        let ledger_key = ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap();
        let body = build_audit_bundle_body(
            &shares,
            serde_json::from_value(input["found_block"].clone()).unwrap(),
            vec![],
            PayoutPolicy::day_one_default(),
            &key,
            &ledger_key,
        )
        .unwrap();
        (body, shares)
    }

    #[test]
    fn split_framing_emits_exact_bytes_and_rejects_a_different_schema() {
        let (mut body, shares) = fixture();
        for shares in [&shares[..], &[]] {
            let mut bytes = Vec::new();
            write_prefix(&mut bytes, &body.schema, shares).unwrap();
            write_suffix(&mut bytes, &body).unwrap();
            assert_eq!(
                bytes,
                canonical_audit_bundle_bytes_from_parts(&body, shares).unwrap()
            );
        }
        let prefix = CanonicalAuditHashPrefix::new(&shares).unwrap();
        body.schema.push_str("-different");
        assert!(matches!(
            prefix.finish(&body),
            Err(PrismError::AuditMismatch { artifact: "schema" })
        ));
    }

    #[test]
    fn prefix_and_suffix_propagate_writer_failures() {
        struct Failed;
        impl Write for Failed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected writer failure"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (body, shares) = fixture();
        assert!(write_prefix(Failed, &body.schema, &shares)
            .unwrap_err()
            .is_io());
        assert!(write_suffix(Failed, &body).unwrap_err().is_io());
    }
}
