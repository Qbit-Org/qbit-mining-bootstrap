//! The claim's quarantine of a row that fails validation (#387), against a
//! real PostgreSQL: the fence on the claim's own hash, token and state, the
//! parking outcome reported over the original diagnosis, and the failures
//! that must never park. Faults are keyed by block hash, so the other claims
//! this test binary runs never meet them.
use super::faults::{self, Fault};
use super::*;
use qbit_prism_test_gate as gate;

struct Database {
    admin: PgPool,
    schema: String,
    ledger: Ledger,
}

impl Database {
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let admin = PgPool::connect(&raw).await?;
        let schema = format!("prism_candidate_park_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let mut url = url::Url::parse(&raw)?;
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let ledger = Ledger::connect(url.as_str(), "park".into(), 4, true).await?;
        Ok(Some(Self {
            admin,
            schema,
            ledger,
        }))
    }

    async fn close(self) -> Result<()> {
        self.ledger.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

/// Run `body` on a fresh schema and drop it afterwards, reporting both.
async fn run(
    body: impl for<'a> FnOnce(&'a Database) -> futures_util::future::LocalBoxFuture<'a, Result<()>>,
) -> Result<()> {
    let Some(db) = Database::open().await? else {
        return Ok(());
    };
    let result = body(&db).await;
    let cleanup = db.close().await;
    match (result, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("schema cleanup also failed: {cleanup}")))
        }
    }
}

/// A due pending row whose document is not a `Candidate`. Every CHECK holds,
/// the offer-state ones included, so a test can advance it; its decode fails
/// with the `document` validation kind. The hash is random, so the faults of
/// concurrent tests never collide.
async fn insert_invalid(pool: &PgPool) -> Result<String> {
    let hash = hex::encode(Sha256::digest(Uuid::new_v4().as_bytes()));
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,block_bytes,window_anchor_ms,window_prior_balances_sha256) VALUES($1,'{}'::jsonb,repeat('0',64),'\\x00'::bytea,1,repeat('0',64))")
        .bind(&hash).execute(pool).await?;
    Ok(hash)
}

/// The claim and scheduling columns a parking would change.
#[derive(Debug, PartialEq)]
struct Claim {
    token: Option<String>,
    instance: Option<String>,
    expires: bool,
    last_error: Option<String>,
    next_attempt_at: String,
    state: String,
    attempts: i32,
}

async fn claim_columns(pool: &PgPool, hash: &str) -> Result<Claim> {
    let row = sqlx::query("SELECT claim_token,claim_instance_id,claim_expires_at IS NOT NULL AS expires,last_error,next_attempt_at::text AS next_attempt_at,state,attempt_count FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(hash).fetch_one(pool).await?;
    Ok(Claim {
        token: row.try_get("claim_token")?,
        instance: row.try_get("claim_instance_id")?,
        expires: row.try_get("expires")?,
        last_error: row.try_get("last_error")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        state: row.try_get("state")?,
        attempts: row.try_get("attempt_count")?,
    })
}

/// The claim's error, which must not be a claim.
async fn claim_failure(ledger: &Ledger) -> Result<anyhow::Error> {
    match ledger.claim_candidate(60).await {
        Ok(Some(claim)) => bail!("claimed {}", claim.candidate.block_hash),
        Ok(None) => bail!("no row was claimable"),
        Err(error) => Ok(error),
    }
}

/// The original diagnosis and its typed kind survive whatever parking did.
fn assert_diagnosis_kept(error: &anyhow::Error, kind: ValidationKind) {
    let text = format!("{error:#}");
    assert_eq!(
        error.downcast_ref::<InvalidCandidate>(),
        Some(&InvalidCandidate(kind)),
        "{text}"
    );
    assert!(text.contains("invalid persisted candidate"), "{text}");
}

/// The claim this attempt took is still exactly as the claim left it: not
/// parked, no reason, the schedule untouched.
fn assert_not_parked(claim: &Claim, due: &str) {
    assert!(claim.token.is_some(), "{claim:?}");
    assert_eq!(claim.instance.as_deref(), Some("park"), "{claim:?}");
    assert!(claim.expires, "{claim:?}");
    assert_eq!(claim.last_error, None, "{claim:?}");
    assert_eq!(claim.next_attempt_at, due, "{claim:?}");
}

/// Control: the invalid row is parked with its database hash and kind, and
/// the returned error keeps the diagnosis under the parked outcome.
#[tokio::test]
async fn invalid_row_is_parked_with_its_hash_kind_and_diagnosis() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let error = claim_failure(&db.ledger).await?;
            assert_diagnosis_kept(&error, ValidationKind::Document);
            assert_eq!(
                error.to_string(),
                format!(
                    "candidate {hash} failed validation and was parked; operator action required"
                )
            );
            let claim = claim_columns(&db.ledger.pool, &hash).await?;
            let reason = claim.last_error.as_deref().unwrap_or_default();
            assert!(
                reason.starts_with(&format!(
                    "candidate {hash}: validation document: invalid persisted candidate: "
                )),
                "{reason}"
            );
            assert_eq!(
                (
                    claim.token,
                    claim.instance,
                    claim.expires,
                    claim.next_attempt_at.as_str(),
                    claim.attempts
                ),
                (None, None, false, "infinity", 1)
            );
            assert!(db.ledger.claim_candidate(60).await?.is_none());
            Ok(())
        })
    })
    .await
}

/// A replacement owner that took the row while the stale decode ran keeps
/// its claim: the stale token parks nothing and reports so.
#[tokio::test]
async fn replacement_owner_is_not_parked_by_a_stale_decode() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            faults::inject(
                &hash,
                Fault::AfterDecodeSql(format!("UPDATE qbit_block_candidate_outbox SET claim_token='replacement',claim_instance_id='successor',claim_expires_at=clock_timestamp()+interval '60 seconds' WHERE block_hash='{hash}'")),
            );
            let error = claim_failure(&db.ledger).await?;
            assert_diagnosis_kept(&error, ValidationKind::Document);
            let text = format!("{error:#}");
            assert!(text.contains(&format!("candidate {hash} failed validation and was not parked")), "{text}");
            assert!(text.contains("candidate to park was not held by this claim"), "{text}");
            let claim = claim_columns(&db.ledger.pool, &hash).await?;
            assert_eq!(
                (claim.token.as_deref(), claim.instance.as_deref(), claim.expires, claim.last_error.as_deref()),
                (Some("replacement"), Some("successor"), true, None)
            );
            assert_eq!((claim.next_attempt_at.as_str(), claim.state.as_str()), (due.as_str(), "pending"));
            Ok(())
        })
    })
    .await
}

/// A row the same token advanced to another unfinished state while the
/// decode ran is not parked: the fence includes the state the claim selected.
#[tokio::test]
async fn advanced_state_is_not_parked_by_a_stale_decode() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            faults::inject(
                &hash,
                Fault::AfterDecodeSql(format!("UPDATE qbit_block_candidate_outbox SET state='offer_reserved',offer_reserved_at=clock_timestamp(),offer_reserved_by='park' WHERE block_hash='{hash}'")),
            );
            let error = claim_failure(&db.ledger).await?;
            assert_diagnosis_kept(&error, ValidationKind::Document);
            let text = format!("{error:#}");
            assert!(text.contains("was not parked"), "{text}");
            let claim = claim_columns(&db.ledger.pool, &hash).await?;
            assert_not_parked(&claim, &due);
            assert_eq!(claim.state, "offer_reserved");
            let reserved_by: Option<String> = sqlx::query_scalar("SELECT offer_reserved_by FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(&hash).fetch_one(&db.ledger.pool).await?;
            assert_eq!(reserved_by.as_deref(), Some("park"));
            Ok(())
        })
    })
    .await
}

/// A parking transaction that updated the row and then failed before its
/// commit rolled back: the claim reports no parking, keeps the diagnosis,
/// and the row keeps its claim and schedule.
#[tokio::test]
async fn failure_before_the_parking_commit_reports_no_parking() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            faults::inject(&hash, Fault::FailBeforeParkCommit);
            let error = claim_failure(&db.ledger).await?;
            assert_diagnosis_kept(&error, ValidationKind::Document);
            let text = format!("{error:#}");
            assert!(
                text.contains("was not parked: injected failure before the parking commit"),
                "{text}"
            );
            assert!(!text.contains("was parked;"), "{text}");
            assert_not_parked(&claim_columns(&db.ledger.pool, &hash).await?, &due);
            Ok(())
        })
    })
    .await
}

/// A database refusal of the parking transaction itself (the writer fence
/// on a halted cluster) parks nothing and is reported beside the diagnosis.
#[tokio::test]
async fn refused_parking_transaction_reports_no_parking() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            faults::inject(
                &hash,
                Fault::AfterDecodeSql(
                    "UPDATE qbit_prism_cluster SET fatal_error='injected halt' WHERE singleton"
                        .into(),
                ),
            );
            let error = claim_failure(&db.ledger).await?;
            assert_diagnosis_kept(&error, ValidationKind::Document);
            let text = format!("{error:#}");
            assert!(
                text.contains("was not parked: cluster halted: injected halt"),
                "{text}"
            );
            assert_not_parked(&claim_columns(&db.ledger.pool, &hash).await?, &due);
            Ok(())
        })
    })
    .await
}

/// A commit whose reply is lost is an unknown outcome, never reported as
/// parked, even though here the row was in fact durably parked.
#[tokio::test]
async fn lost_parking_commit_reply_is_reported_unknown() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            faults::inject(&hash, Fault::LoseParkCommitReply);
            let error = claim_failure(&db.ledger).await?;
            assert_diagnosis_kept(&error, ValidationKind::Document);
            let text = format!("{error:#}");
            assert!(
                text.contains("whether it was parked is unknown: the parking commit failed"),
                "{text}"
            );
            assert!(
                !text.contains("was parked;") && !text.contains("was not parked"),
                "{text}"
            );
            let claim = claim_columns(&db.ledger.pool, &hash).await?;
            assert_eq!(claim.next_attempt_at, "infinity", "{claim:?}");
            Ok(())
        })
    })
    .await
}

/// A decode that never completes (the blocking task panicked) proves nothing
/// about the row: the join error is returned as it is and nothing is parked.
#[tokio::test]
async fn decode_join_failure_is_not_parked() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            faults::inject(&hash, Fault::PanicInDecode);
            let error = claim_failure(&db.ledger).await?;
            assert!(
                error
                    .downcast_ref::<tokio::task::JoinError>()
                    .is_some_and(|join| join.is_panic()),
                "{error:#}"
            );
            assert!(
                error.downcast_ref::<InvalidCandidate>().is_none(),
                "{error:#}"
            );
            assert_not_parked(&claim_columns(&db.ledger.pool, &hash).await?, &due);
            Ok(())
        })
    })
    .await
}

/// A transient database failure during the decode (a pool timeout) is not
/// evidence about the row and never parks it, even though the row would fail
/// validation. Kept apart from the column-error case so a quarantine of every
/// decode error is caught by this transient case alone.
#[tokio::test]
async fn transient_decode_database_error_is_not_parked() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            let fault = Fault::TransientDecodeDatabaseError;
            faults::inject(&hash, fault.clone());
            let error = claim_failure(&db.ledger).await?;
            assert!(
                matches!(
                    error.downcast_ref::<sqlx::Error>(),
                    Some(sqlx::Error::PoolTimedOut)
                ),
                "{fault:?}: {error:#}"
            );
            assert!(
                error.downcast_ref::<InvalidCandidate>().is_none(),
                "{fault:?}: {error:#}"
            );
            let claim = claim_columns(&db.ledger.pool, &hash).await?;
            assert_ne!(claim.next_attempt_at, "infinity", "{fault:?}: {claim:?}");
            assert_not_parked(&claim, &due);
            Ok(())
        })
    })
    .await
}

/// An unclassified column error from the decode is not a validation failure
/// and never parks, even on a row that would fail validation.
#[tokio::test]
async fn unclassified_decode_error_is_not_parked() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            faults::inject(&hash, Fault::UnclassifiedDecodeError);
            let error = claim_failure(&db.ledger).await?;
            assert!(error.downcast_ref::<sqlx::Error>().is_some(), "{error:#}");
            assert!(
                error.downcast_ref::<InvalidCandidate>().is_none(),
                "{error:#}"
            );
            assert_not_parked(&claim_columns(&db.ledger.pool, &hash).await?, &due);
            Ok(())
        })
    })
    .await
}

/// A persisted offer outcome no variant names is a lifecycle validation
/// failure. The disposable schema drops 011's offer CHECK to store one.
#[tokio::test]
async fn unknown_persisted_offer_outcome_parks_as_lifecycle() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            sqlx::raw_sql(&format!("ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT qbit_block_candidate_outbox_offer_check; UPDATE qbit_block_candidate_outbox SET offer_outcome='bogus' WHERE block_hash='{hash}'"))
                .execute(&db.ledger.pool).await?;
            let error = claim_failure(&db.ledger).await?;
            assert_eq!(
                error.downcast_ref::<InvalidCandidate>(),
                Some(&InvalidCandidate(ValidationKind::Lifecycle)),
                "{error:#}"
            );
            let claim = claim_columns(&db.ledger.pool, &hash).await?;
            let reason = claim.last_error.as_deref().unwrap_or_default();
            assert!(
                reason.starts_with(&format!("candidate {hash}: validation lifecycle: claimed candidate row records offer outcome \"bogus\"")),
                "{reason}"
            );
            assert_eq!(claim.next_attempt_at, "infinity");
            Ok(())
        })
    })
    .await
}

/// The parking `UPDATE` itself failing in the database (a disposable-schema
/// trigger refuses only that write) parks nothing, keeps the diagnosis, and
/// reports the failed durable update.
#[tokio::test]
async fn failing_parking_update_reports_no_parking() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let hash = insert_invalid(&db.ledger.pool).await?;
            let due = claim_columns(&db.ledger.pool, &hash).await?.next_attempt_at;
            sqlx::raw_sql("CREATE FUNCTION refuse_parking() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.next_attempt_at = 'infinity' THEN RAISE EXCEPTION 'injected parking update failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER refuse_parking BEFORE UPDATE ON qbit_block_candidate_outbox FOR EACH ROW EXECUTE FUNCTION refuse_parking()")
                .execute(&db.ledger.pool).await?;
            let error = claim_failure(&db.ledger).await?;
            assert_diagnosis_kept(&error, ValidationKind::Document);
            let text = format!("{error:#}");
            assert!(
                text.contains("was not parked") && text.contains("injected parking update failure"),
                "{text}"
            );
            assert!(!text.contains("was parked;"), "{text}");
            assert_not_parked(&claim_columns(&db.ledger.pool, &hash).await?, &due);
            Ok(())
        })
    })
    .await
}

/// A malformed supported row is parked from every unfinished state #391
/// claims, and parking changes only the claim, the reason, the schedule and
/// `updated_at`: the state and the offer evidence are exactly as they were.
#[tokio::test]
async fn malformed_rows_park_in_every_unfinished_state_with_their_offer_evidence() -> Result<()> {
    run(|db| {
        Box::pin(async move {
            let pool = &db.ledger.pool;
            let states = [
                ("pending", ""),
                ("offer_reserved", ",offer_reserved_at=clock_timestamp(),offer_reserved_by='frontend-a'"),
                ("offered", ",offer_reserved_at=clock_timestamp(),offer_reserved_by='frontend-a',offered_at_ms=1700000000000,offer_outcome='rejected',offer_reply='bad-cb-height'"),
                ("reconciliation", ",offer_reserved_at=clock_timestamp(),offer_reserved_by='frontend-a',offered_at_ms=1700000000000,offer_outcome='unknown',last_error='an earlier reconciliation reason'"),
            ];
            let mut rows = Vec::new();
            for (state, offer) in states {
                let hash = insert_invalid(pool).await?;
                sqlx::raw_sql(&format!("UPDATE qbit_block_candidate_outbox SET state='{state}'{offer} WHERE block_hash='{hash}'"))
                    .execute(pool).await?;
                let before: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE block_hash=$1")
                    .bind(&hash).fetch_one(pool).await?;
                rows.push((state, hash, before));
            }
            for _ in &rows {
                let error = claim_failure(&db.ledger).await?;
                assert_diagnosis_kept(&error, ValidationKind::Document);
                assert!(error.to_string().contains("was parked;"), "{error:#}");
            }
            assert!(db.ledger.claim_candidate(60).await?.is_none());
            let evidence = |mut row: Value| {
                let columns = row.as_object_mut().expect("an outbox row is an object");
                for column in [
                    "claim_token",
                    "claim_instance_id",
                    "claim_expires_at",
                    "last_error",
                    "next_attempt_at",
                    "updated_at",
                    "attempt_count",
                ] {
                    columns.remove(column);
                }
                row
            };
            for (state, hash, before) in rows {
                let after: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM qbit_block_candidate_outbox o WHERE block_hash=$1")
                    .bind(&hash).fetch_one(pool).await?;
                let claim = claim_columns(pool, &hash).await?;
                assert_eq!(
                    (claim.token, claim.instance, claim.expires, claim.next_attempt_at.as_str(), claim.state.as_str(), claim.attempts),
                    (None, None, false, "infinity", state, 1),
                    "{state}"
                );
                let reason = claim.last_error.unwrap_or_default();
                assert!(
                    reason.starts_with(&format!("candidate {hash}: validation document: ")),
                    "{state}: {reason}"
                );
                assert_eq!(evidence(after), evidence(before), "{state}");
            }
            Ok(())
        })
    })
    .await
}

/// The reason keeps the identity and the kind whole and bounds the whole
/// string, cutting a long diagnosis on a character boundary.
#[test]
fn parking_reason_keeps_identity_and_kind_within_the_bound() {
    let hash = "ab".repeat(32);
    let error = anyhow::anyhow!("é".repeat(2000)).context(InvalidCandidate(ValidationKind::Block));
    let reason = parking_reason(&hash, ValidationKind::Block, &error);
    assert!(reason.len() <= PARKING_REASON_MAX_BYTES, "{}", reason.len());
    assert!(
        reason.starts_with(&format!("candidate {hash}: validation block: éé")),
        "{reason}"
    );
    assert!(reason.ends_with('…'), "{reason}");

    let error = anyhow::anyhow!("inner")
        .context("outer")
        .context(InvalidCandidate(ValidationKind::Document));
    assert_eq!(
        parking_reason(&hash, ValidationKind::Document, &error),
        format!("candidate {hash}: validation document: outer: inner")
    );
}
