//! Exercise the real refresh/issue/persist/resume path over serialized job rows.
//! Only ledger and node I/O are replaced by the shared in-memory fixture.
use super::*;

type Edit<T> = fn(&mut T);

#[test]
fn issued_inputs_preserve_stored_identity_and_config_errors() {
    let mut config = config::test_config();
    let inputs = BundleInputs::capture(&config, None).unwrap();
    let mut stored = StoredPrepared {
        template: Value::Null,
        snapshot: Arc::new(Snapshot {
            anchor_ms: 0,
            share_seq: 0,
            payout_revision: 0,
            shares: vec![],
            prior_balances: vec![],
        }),
        bundle: None,
        inputs: Some(inputs),
        fee: None,
        fingerprint: String::new(),
        generation: 0,
        parent_of_tip: String::new(),
        coinbase_suffix: String::new(),
    };
    assert!(std::ptr::eq(
        stored.issued_inputs(&config).unwrap().unwrap(),
        stored.inputs.as_ref().unwrap(),
    ));
    stored.inputs.as_mut().unwrap().audit_builder_version += 1;
    assert!(stored.issued_inputs(&config).unwrap().is_none());
    config.manifest_seed = "invalid".into();
    assert!(stored.issued_inputs(&config).is_err());
    stored.inputs = None;
    assert!(stored.issued_inputs(&config).unwrap().is_none());
}

async fn issue(ctv: bool, bootstrap: bool) -> (Fixture, MiningJob<JobContext>) {
    let f = Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.ctv_enabled = ctv;
            config.ctv_fee = ctv.then_some(FanoutFeeRatePolicy::new(1000, 12000));
        },
        None,
    )
    .await;
    if bootstrap {
        f.store
            .snapshot
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .shares
            .clear();
    }
    f.coordinator.refresh_once().await.unwrap();
    let worker = f.job(1, 0, "original.worker").context.worker.clone();
    let job = f
        .coordinator
        .build_job(&worker, "00000001", 1e-12, 0.0)
        .await
        .unwrap();
    f.coordinator
        .persist_issued_job(&worker, &job, 0x1fffe000, Duration::from_secs(30))
        .await
        .unwrap();
    (f, job)
}

fn edit_prepared(f: &Fixture, job: &MiningJob<JobContext>, edit: impl FnOnce(&mut Value)) {
    edit(
        &mut f
            .store
            .jobs
            .lock()
            .unwrap()
            .get_mut(&job.context.prepared.storage_key)
            .unwrap()
            .payload,
    );
}

async fn assert_miss(f: &Fixture, job: &MiningJob<JobContext>) {
    // A miss must not rebuild, including for a bootstrap job. Holding the
    // build slot makes accidental rebuilds fail this bounded assertion.
    let _permit = f.coordinator.build_slots.acquire().await.unwrap();
    let resumed = tokio::time::timeout(
        Duration::from_secs(3),
        f.coordinator
            .resume_job(&job.context.worker, &job.wire.job_id),
    )
    .await
    .expect("cache miss must not enter the builder")
    .expect("incompatible inputs are a cache miss, not a backend error");
    assert!(resumed.is_none());
}

#[tokio::test]
async fn stored_inputs_resume_unchanged_with_original_work_and_expiry() {
    for ctv in [false, true] {
        for bootstrap in [false, true] {
            let (f, issued) = issue(ctv, bootstrap).await;
            let key = &issued.context.prepared.storage_key;
            let original = f.store.jobs.lock().unwrap()[key].payload.clone();
            assert_eq!(
                original["inputs"],
                serde_json::to_value(&issued.context.prepared.inputs).unwrap()
            );
            assert_eq!(original["bundle"].is_null(), bootstrap);
            let deadline = f.store.jobs.lock().unwrap()[&issued.wire.job_id].expires_at_ms;
            f.store.clock_offset_ms.store(5_000, Ordering::SeqCst);
            let resumed = f
                .coordinator
                .resume_job(&issued.context.worker, &issued.wire.job_id)
                .await
                .unwrap()
                .expect("unchanged stored inputs resume");
            assert_eq!(
                resumed.context.prepared.inputs,
                issued.context.prepared.inputs
            );
            assert_eq!(
                resumed.context.prepared.window,
                issued.context.prepared.window
            );
            assert_eq!(
                serde_json::to_vec(&resumed.context.bundle).unwrap(),
                serde_json::to_vec(&issued.context.bundle).unwrap()
            );
            assert_eq!(resumed.wire.coinb1, issued.wire.coinb1);
            assert_eq!(resumed.wire.coinb2, issued.wire.coinb2);
            assert_eq!(resumed.wire.extranonce1, issued.wire.extranonce1);
            assert_eq!(resumed.wire.share_target, issued.wire.share_target);
            assert_eq!(resumed.wire.share_difficulty, issued.wire.share_difficulty);
            assert_eq!(resumed.wire.version_mask, 0x1fffe000);
            assert_eq!(resumed.wire.payout_revision, issued.wire.payout_revision);
            assert!(
                resumed
                    .wire
                    .resume_expires_at
                    .unwrap()
                    .saturating_duration_since(Instant::now())
                    <= Duration::from_secs(25)
            );
            assert_eq!(f.store.jobs.lock().unwrap()[key].payload, original);
            assert_eq!(
                f.store.jobs.lock().unwrap()[&issued.wire.job_id].expires_at_ms,
                deadline
            );
            f.store.clock_offset_ms.store(30_000, Ordering::SeqCst);
            assert_miss(&f, &issued).await;
        }
    }
}

#[tokio::test]
async fn stored_builder_version_mismatch_is_a_resume_miss() {
    for bootstrap in [false, true] {
        let (f, issued) = issue(false, bootstrap).await;
        // Model an issued row from a different binary without changing its
        // original bundle or hashes. The bundle itself cannot prove this.
        edit_prepared(&f, &issued, |payload| {
            payload["inputs"]["audit_builder_version"] =
                json!(qbit_prism::AUDIT_BUILDER_VERSION + 1);
        });
        assert_miss(&f, &issued).await;
    }
}

#[tokio::test]
async fn changed_ctv_configuration_is_a_stored_job_resume_miss() {
    let changes: [(bool, Edit<Config>); 4] = [
        (false, |config| config.ctv_enabled = true),
        (true, |config| config.ctv_enabled = false),
        (true, |config| config.ctv_direct_floor += 1),
        (true, |config| {
            config.ctv_config.reserved_coinbase_outputs += 1
        }),
    ];
    for bootstrap in [false, true] {
        for (ctv, change) in changes {
            let (mut f, issued) = issue(ctv, bootstrap).await;
            let config =
                Arc::get_mut(&mut Arc::get_mut(&mut f.coordinator).unwrap().config).unwrap();
            change(config);
            assert_miss(&f, &issued).await;
        }
    }
}

#[tokio::test]
async fn stored_ctv_fee_inputs_must_match_the_issued_fee() {
    let (f, issued) = issue(true, false).await;
    edit_prepared(&f, &issued, |payload| {
        payload["inputs"]["ctv"]["fanout_fee_policy"] = Value::Null;
    });
    assert_miss(&f, &issued).await;
}

#[tokio::test]
async fn legacy_prepared_rows_without_inputs_are_resume_misses() {
    for bootstrap in [false, true] {
        let (f, issued) = issue(false, bootstrap).await;
        edit_prepared(&f, &issued, |payload| {
            payload.as_object_mut().unwrap().remove("inputs");
        });
        assert_miss(&f, &issued).await;
    }
}

#[tokio::test]
async fn malformed_stored_inputs_and_legacy_records_remain_resume_errors() {
    let corruptions: [(&str, Edit<Value>); 5] = [
        ("null", |payload| payload["inputs"] = Value::Null),
        ("partial", |payload| {
            payload["inputs"]
                .as_object_mut()
                .unwrap()
                .remove("audit_builder_version");
        }),
        ("missing ctv", |payload| {
            payload["inputs"].as_object_mut().unwrap().remove("ctv");
        }),
        ("bad ctv", |payload| {
            payload["inputs"]["ctv"] = json!({"direct_floor_sats":"invalid"});
        }),
        ("legacy snapshot", |payload| {
            payload.as_object_mut().unwrap().remove("inputs");
            payload["snapshot"] = Value::Null;
        }),
    ];
    for (corruption, edit) in corruptions {
        let (f, issued) = issue(false, false).await;
        edit_prepared(&f, &issued, edit);
        let error = match f
            .coordinator
            .resume_job(&issued.context.worker, &issued.wire.job_id)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("{corruption} must remain an error"),
        };
        assert_error(error, "backend-rpc-unavailable", "job resume unavailable");
    }
}

#[tokio::test]
async fn stored_job_resume_preserves_current_policy_and_signer_checks() {
    let changes: [Edit<Config>; 3] = [
        |config| config.payout_policy.safety_multiplier += 1,
        |config| config.manifest_seed = hash(0x33),
        |config| config.ledger_seed = hash(0x44),
    ];
    for change in changes {
        let (mut f, issued) = issue(false, false).await;
        let config = Arc::get_mut(&mut Arc::get_mut(&mut f.coordinator).unwrap().config).unwrap();
        change(config);
        assert_miss(&f, &issued).await;
    }
}

#[tokio::test]
async fn matching_stored_inputs_still_require_the_original_bundle_policy_and_signers() {
    let changes: [Edit<Value>; 3] = [
        |payload| payload["bundle"]["payout_policy"]["safety_multiplier"] = json!(999),
        |payload| {
            payload["bundle"]["signed_coinbase_manifest"]["signature"]["public_key_hex"] =
                json!(hash(0x33));
        },
        |payload| {
            payload["bundle"]["ledger_window_attestation"]["signature"]["public_key_hex"] =
                json!(hash(0x44));
        },
    ];
    for change in changes {
        let (f, issued) = issue(false, false).await;
        edit_prepared(&f, &issued, change);
        assert_miss(&f, &issued).await;
    }
}
