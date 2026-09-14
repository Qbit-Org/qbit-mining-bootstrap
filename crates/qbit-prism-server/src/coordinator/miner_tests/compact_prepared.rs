use super::*;
use crate::coordinator::prepared_storage::compact::{
    CanonicalCompactBalances, CapturedCompactPrepared, CompactOwner, IncompatibleCompactBuild,
    OriginalPreparedBuild,
};
use crate::ledger::{PayoutState, StoredCompactPrepared};
use work_ledger::WorkLedger;

// Reuse the fixture's original build parts under an unreserved key. This is
// test setup, not a production conversion from a legacy/resumed Prepared.
fn original_build(original: &Prepared) -> CompactOwner<OriginalPreparedBuild> {
    OriginalPreparedBuild::from_original_build(
        format!("prepared:fresh:{}", uuid::Uuid::new_v4().simple()),
        original.stored.clone(),
        original.window,
        original.inputs.clone(),
    )
}

async fn captured(f: &Fixture) -> CompactOwner<CapturedCompactPrepared> {
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    f.coordinator
        .capture_compact_prepared(original_build(&original), 130_000)
        .await
        .unwrap()
}

fn observation(captured: &CapturedCompactPrepared) -> StoredCompactPrepared {
    StoredCompactPrepared {
        record: captured.record.clone(),
        template: captured.original.stored.template.clone(),
        prior_balances: captured.original.snapshot.prior_balances.clone(),
        original_expires_at_ms: captured.original_expires_at_ms,
        expires_at_ms: 190_000,
    }
}

fn issued(f: &Fixture, captured: &CapturedCompactPrepared) -> StoredJob {
    StoredJob {
        prepared_key: captured.original.storage_key.clone(),
        worker: f.job(1, 0, "original.worker").context.worker.clone(),
        extranonce1: "00000001".into(),
        extranonce2_size: 8,
        share_target_hex: "01".into(),
        share_difficulty: 1.0,
        version_mask: 0,
        expires_at_ms: 180_000,
    }
}

fn script_window(f: &Fixture, captured: &CapturedCompactPrepared) {
    f.store
        .compact
        .windows
        .lock()
        .unwrap()
        .push_back(Ok(Window {
            shares: captured.original.snapshot.shares.clone(),
            prior_balances: captured.original.snapshot.prior_balances.clone(),
            payout_revision: 37,
        }));
}

async fn cleanup_finished(f: &Fixture) {
    let permit = tokio::time::timeout(
        Duration::from_secs(5),
        f.coordinator.build_slots.clone().acquire_owned(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(permit);
    assert_eq!(f.coordinator.build_slots.available_permits(), 1);
}

fn build_balance_order_bundle(
    snapshot: &Snapshot,
    found: FoundBlock,
    inputs: &BundleInputs,
    suffix: String,
) -> AuditBundle {
    let manifest = ManifestSigningKey::from_seed_hex(&hash(0x11)).unwrap();
    let ledger = ManifestSigningKey::from_seed_hex(&hash(0x22)).unwrap();
    if let Some(ctv) = &inputs.ctv {
        qbit_prism::build_audit_bundle_with_ctv_settlement_options(
            snapshot.shares.clone(),
            found,
            snapshot.prior_balances.clone(),
            inputs.payout_policy.clone(),
            ctv.direct_floor_sats,
            ctv.settlement_config,
            ctv.fanout_fee_policy,
            Some(suffix),
            vec![],
            &manifest,
            &ledger,
        )
        .unwrap()
    } else {
        qbit_prism::build_audit_bundle_with_coinbase_options(
            snapshot.shares.clone(),
            found,
            snapshot.prior_balances.clone(),
            inputs.payout_policy.clone(),
            Some(suffix),
            vec![],
            &manifest,
            &ledger,
        )
        .unwrap()
    }
}

#[tokio::test]
async fn canonical_inputs_precede_original_hash_and_roundtrip_without_rewriting_legacy() {
    use crate::coordinator::prepared_storage::compact::CompactDropProbe;
    let runtime_thread = std::thread::current().id();
    for ctv in [false, true] {
        let f = Fixture::build(
            Duration::from_secs(10),
            |config| config.ctv_enabled = ctv,
            None,
        )
        .await;
        let original = f.coordinator.prepared.read().await.clone().unwrap();
        let inputs = BundleInputs::capture(&f.coordinator.config, None).unwrap();
        let mut raw = (*original.snapshot).clone();
        // SQL under a natural-language collation can yield a before B, while
        // the immutable balance blob's bytewise comparator yields B before a.
        raw.prior_balances = vec![
            qbit_prism::CarryForwardBalance {
                recipient_id: "miner-a".into(),
                order_key: "a-order".into(),
                p2mr_program_hex: hash(0x11),
                balance_sats: 1000,
            },
            qbit_prism::CarryForwardBalance {
                recipient_id: "miner-b".into(),
                order_key: "B-order".into(),
                p2mr_program_hex: hash(0x22),
                balance_sats: 500,
            },
        ];
        let found = original.bundle.as_ref().unwrap().found_block.clone();
        let suffix = original.stored.coinbase_suffix.clone();
        let permit = f
            .coordinator
            .build_slots
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let (raw, snapshot, legacy, bundle) = tokio::task::spawn_blocking({
            let inputs = inputs.clone();
            let found = found.clone();
            let suffix = suffix.clone();
            move || {
                let _permit = permit;
                let legacy =
                    build_balance_order_bundle(&raw, found.clone(), &inputs, suffix.clone());
                let mut canonical = raw.clone();
                canonical.prior_balances = CanonicalCompactBalances::prepare(
                    std::mem::take(&mut canonical.prior_balances),
                    &_permit,
                )
                .into_original_build();
                // The first compact original bundle is built only AFTER its
                // typed inputs have their final order, never normalized later.
                let bundle = build_balance_order_bundle(&canonical, found, &inputs, suffix);
                (raw, Arc::new(canonical), legacy, bundle)
            }
        })
        .await
        .unwrap();
        assert_eq!(raw.prior_balances[0].order_key, "a-order");
        assert_eq!(snapshot.prior_balances[0].order_key, "B-order");
        assert_eq!(
            qbit_prism::prior_balances_digest(&raw.prior_balances),
            qbit_prism::prior_balances_digest(&snapshot.prior_balances)
        );
        // Both ordinary and CTV payout values, signed outputs and attestations
        // are unchanged. Only the original bundle's input array order differs.
        assert_eq!(bundle.reward_manifest, legacy.reward_manifest);
        assert_eq!(bundle.payout_policy_manifest, legacy.payout_policy_manifest);
        assert_eq!(
            bundle.signed_coinbase_manifest,
            legacy.signed_coinbase_manifest
        );
        assert_eq!(
            bundle.ledger_window_attestation,
            legacy.ledger_window_attestation
        );
        assert_eq!(
            bundle.ctv_fanout_manifest_set,
            legacy.ctv_fanout_manifest_set
        );
        let report =
            qbit_prism::verify_audit_bundle(&bundle, &inputs.signer_keys.ledger_key_hex).unwrap();
        let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
        assert_ne!(
            report.audit_bundle_sha256_hex,
            hex::encode(Sha256::digest(&legacy_bytes))
        );
        let window = WindowRef::from_snapshot(&snapshot).unwrap();
        let stored = Arc::new(StoredPrepared {
            template: original.template.clone(),
            snapshot: snapshot.clone(),
            bundle: Some(Arc::new(bundle.clone())),
            fee: None,
            fingerprint: original.fingerprint.clone(),
            generation: original.generation,
            parent_of_tip: original.parent_of_tip.clone(),
            coinbase_suffix: suffix.clone(),
        });
        let source = OriginalPreparedBuild::from_original_build(
            format!("prepared:canonical:{}", uuid::Uuid::new_v4().simple()),
            stored,
            window,
            inputs.clone(),
        );
        let captured = f
            .coordinator
            .capture_compact_prepared(source, 130_000)
            .await
            .unwrap();
        assert_eq!(
            captured
                .record
                .audit_hashes
                .as_ref()
                .unwrap()
                .audit_bundle_sha256,
            report.audit_bundle_sha256_hex
        );
        let issued = issued(&f, &captured);
        f.store
            .compact
            .reads
            .lock()
            .unwrap()
            .push_back(Ok(Some(observation(&captured))));
        script_window(&f, &captured);
        let hydrated = f
            .coordinator
            .hydrate_compact_inputs(&issued)
            .await
            .unwrap()
            .unwrap();
        let rebuilt = hydrated
            .spawn_blocking(move |owned| {
                build_balance_order_bundle(&owned.snapshot, found, &owned.inputs, suffix)
            })
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&rebuilt).unwrap(),
            serde_json::to_vec(&bundle).unwrap()
        );
        let rebuilt_report =
            qbit_prism::verify_audit_bundle(&rebuilt, &inputs.signer_keys.ledger_key_hex).unwrap();
        assert_eq!(
            rebuilt_report.audit_bundle_sha256_hex,
            report.audit_bundle_sha256_hex
        );
        assert_eq!(
            rebuilt_report.coinbase_manifest_sha256_hex,
            report.coinbase_manifest_sha256_hex
        );

        for keep_raw_snapshot in [true, false] {
            let incompatible = Arc::new(StoredPrepared {
                template: original.template.clone(),
                snapshot: if keep_raw_snapshot {
                    Arc::new(raw.clone())
                } else {
                    snapshot.clone()
                },
                bundle: Some(Arc::new(legacy.clone())),
                fee: None,
                fingerprint: original.fingerprint.clone(),
                generation: original.generation,
                parent_of_tip: original.parent_of_tip.clone(),
                coinbase_suffix: original.stored.coinbase_suffix.clone(),
            });
            let source = OriginalPreparedBuild::from_original_build(
                format!("prepared:incompatible:{}", uuid::Uuid::new_v4().simple()),
                incompatible,
                window,
                inputs.clone(),
            );
            let (dropped, receive) = tokio::sync::oneshot::channel();
            let release = ReleaseProbe(Arc::new(prepared_storage::RepairProbe::default()));
            let source = OriginalPreparedBuild::with_drop_probe(
                source,
                CompactDropProbe {
                    dropped: Some(dropped),
                    release: release.0.clone(),
                    runtime_thread,
                },
            );
            let capture = tokio::spawn({
                let c = f.coordinator.clone();
                async move { c.capture_compact_prepared(source, 130_000).await }
            });
            let dropped_on = tokio::time::timeout(Duration::from_secs(5), receive)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(dropped_on, runtime_thread);
            assert_eq!(
                f.coordinator.build_slots.available_permits(),
                0,
                "rejected original inputs retain admission through their cleanup"
            );
            release.0.release();
            let Err(error) = capture.await.unwrap() else {
                panic!("a completed noncanonical bundle cannot acquire a new audit identity")
            };
            assert_eq!(f.coordinator.build_slots.available_permits(), 1);
            assert!(matches!(
                (
                    keep_raw_snapshot,
                    error.downcast::<IncompatibleCompactBuild>().unwrap()
                ),
                (true, IncompatibleCompactBuild::NonCanonicalBalances)
                    | (false, IncompatibleCompactBuild::BundleBalancesMismatch)
            ));
            assert_eq!(serde_json::to_vec(&legacy).unwrap(), legacy_bytes);
        }
        assert!(f.store.compact.save_calls.lock().unwrap().is_empty());
        assert!(Arc::ptr_eq(
            f.coordinator.prepared.read().await.as_ref().unwrap(),
            &original
        ));
    }
}

#[tokio::test]
async fn capture_rejects_mismatched_original_window_without_changing_identity() {
    use crate::coordinator::prepared_storage::compact::CompactDropProbe;
    let runtime_thread = std::thread::current().id();
    let f = Fixture::new(Duration::from_secs(10)).await;
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    let original_bytes = serde_json::to_vec(original.bundle.as_ref().unwrap()).unwrap();
    for mismatch in [
        "anchor",
        "missing range",
        "unexpected range",
        "first",
        "last",
        "count",
    ] {
        let mut window = original.window;
        let mut snapshot = (*original.snapshot).clone();
        match mismatch {
            "anchor" => window.anchor_ms += 1,
            "missing range" => window.shares = None,
            "unexpected range" => snapshot.shares.clear(),
            "first" => window.shares.as_mut().unwrap().first_share_seq += 1,
            "last" => window.shares.as_mut().unwrap().last_share_seq += 1,
            _ => window.shares.as_mut().unwrap().share_count += 1,
        }
        let source = OriginalPreparedBuild::from_original_build(
            format!("prepared:mismatched:{}", uuid::Uuid::new_v4().simple()),
            Arc::new(StoredPrepared {
                snapshot: Arc::new(snapshot),
                template: original.template.clone(),
                bundle: original.bundle.clone(),
                fee: original.fee,
                fingerprint: original.fingerprint.clone(),
                generation: original.generation,
                parent_of_tip: original.parent_of_tip.clone(),
                coinbase_suffix: original.stored.coinbase_suffix.clone(),
            }),
            window,
            original.inputs.clone(),
        );
        let (dropped, receive) = tokio::sync::oneshot::channel();
        let release = ReleaseProbe(Arc::new(prepared_storage::RepairProbe::default()));
        let source = OriginalPreparedBuild::with_drop_probe(
            source,
            CompactDropProbe {
                dropped: Some(dropped),
                release: release.0.clone(),
                runtime_thread,
            },
        );
        let capture = tokio::spawn({
            let c = f.coordinator.clone();
            async move { c.capture_compact_prepared(source, 130_000).await }
        });
        let dropped_on = tokio::time::timeout(Duration::from_secs(5), receive)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(dropped_on, runtime_thread);
        assert_eq!(f.coordinator.build_slots.available_permits(), 0);
        release.0.release();
        let Err(error) = capture.await.unwrap() else {
            panic!("{mismatch}: mismatched original window was accepted")
        };
        assert!(matches!(
            error.downcast::<IncompatibleCompactBuild>().unwrap(),
            IncompatibleCompactBuild::WindowSnapshotMismatch
        ));
        assert_eq!(f.coordinator.build_slots.available_permits(), 1);
        assert_eq!(
            serde_json::to_vec(original.bundle.as_ref().unwrap()).unwrap(),
            original_bytes
        );
    }
    assert!(f.store.compact.save_calls.lock().unwrap().is_empty());
    assert!(Arc::ptr_eq(
        f.coordinator.prepared.read().await.as_ref().unwrap(),
        &original
    ));
}

#[tokio::test]
async fn fresh_capture_saves_without_publication_and_preserves_inline_conflicts() {
    for empty in [false, true] {
        let mut f = Fixture::new(Duration::from_secs(10)).await;
        // The fixture supplies original builder outputs without database I/O.
        // Remove its access view: fresh capture must not read a publication.
        let original = f.coordinator.prepared.write().await.take().unwrap();
        let mut snapshot = (*original.snapshot).clone();
        if empty {
            snapshot.shares.clear();
        }
        let window = WindowRef::from_snapshot(&snapshot).unwrap();
        let stored = Arc::new(StoredPrepared {
            snapshot: Arc::new(snapshot),
            template: original.template.clone(),
            bundle: (!empty).then(|| original.stored.bundle.clone().unwrap()),
            fee: original.fee,
            fingerprint: original.fingerprint.clone(),
            generation: original.generation,
            parent_of_tip: original.parent_of_tip.clone(),
            coinbase_suffix: original.stored.coinbase_suffix.clone(),
        });
        let key = format!("prepared:fresh:{}", uuid::Uuid::new_v4().simple());
        let source = OriginalPreparedBuild::from_original_build(
            key.clone(),
            stored.clone(),
            window,
            original.inputs.clone(),
        );
        f.detect(1).await;
        let config = Arc::get_mut(&mut Arc::get_mut(&mut f.coordinator).unwrap().config).unwrap();
        config.payout_policy.safety_multiplier += 1;
        config.ctv_enabled = true;
        config.ctv_direct_floor += 1;
        config.manifest_seed = hash(0x33);
        config.ledger_seed = hash(0x44);
        let captured = f
            .coordinator
            .capture_compact_prepared(source, 130_000)
            .await
            .unwrap();
        assert!(f.coordinator.prepared.read().await.is_none());
        assert!(f.store.jobs.lock().unwrap().is_empty());
        assert_eq!(captured.record.payout_policy, original.inputs.payout_policy);
        assert_eq!(captured.record.ctv, original.inputs.ctv);
        assert_eq!(captured.record.signer_keys, original.inputs.signer_keys);
        assert_eq!(captured.record.window, window);
        assert_eq!(captured.record.audit_hashes.is_none(), empty);
        f.store
            .compact
            .saves
            .lock()
            .unwrap()
            .extend([Ok(true), Ok(false)]);
        assert!(f
            .coordinator
            .save_captured_compact(&captured)
            .await
            .unwrap());
        assert!(!f
            .coordinator
            .save_captured_compact(&captured)
            .await
            .unwrap());
        assert!(f.coordinator.prepared.read().await.is_none());
        for saved in f.store.compact.save_calls.lock().unwrap().iter() {
            assert_eq!(saved.key, key);
            assert_eq!(saved.original_expires_at_ms, 130_000);
            assert_eq!(saved.record, captured.record);
        }

        // Negative control for the old published-only path: an inline save
        // already owns the selected key. Compact save must keep that conflict
        // an error and leave the incompatible row and its expiry untouched.
        let occupied = format!("prepared:inline:{}", uuid::Uuid::new_v4().simple());
        let inline = serde_json::to_value(&stored).unwrap();
        f.store
            .save_job(&occupied, &inline, 0, &hash(1), 30)
            .await
            .unwrap();
        let conflict_source = OriginalPreparedBuild::from_original_build(
            occupied.clone(),
            stored,
            window,
            original.inputs.clone(),
        );
        let conflict = f
            .coordinator
            .capture_compact_prepared(conflict_source, 130_000)
            .await
            .unwrap();
        let error = f
            .coordinator
            .save_captured_compact(&conflict)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "immutable compact prepared conflict");
        let jobs = f.store.jobs.lock().unwrap();
        assert_eq!(jobs[&occupied].payload, inline);
        assert_eq!(jobs[&occupied].expires_at_ms, 130_000);
    }
}

#[tokio::test]
async fn capture_preserves_original_identity_and_exact_save_arguments() {
    let mut f = Fixture::build(
        Duration::from_secs(10),
        |config| {
            config.ctv_enabled = true;
            config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
        },
        None,
    )
    .await;
    f.coordinator.refresh_once().await.unwrap();
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    let source = original_build(&original);
    let config = Arc::get_mut(&mut Arc::get_mut(&mut f.coordinator).unwrap().config).unwrap();
    config.ctv_enabled = false;
    config.ctv_direct_floor += 1;
    config.ctv_fee = None;
    config.coinbase_tag = "/changed/".into();
    config.manifest_seed = hash(0x33);
    config.ledger_seed = hash(0x44);
    let captured = f
        .coordinator
        .capture_compact_prepared(source, 130_000)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&captured.original.stored, &original.stored));
    assert_ne!(captured.original.storage_key, original.storage_key);
    assert_eq!(captured.record.window, original.window);
    assert_eq!(captured.record.share_seq, original.snapshot.share_seq);
    assert_eq!(
        captured.record.payout_revision,
        original.snapshot.payout_revision
    );
    assert_eq!(captured.record.payout_policy, original.inputs.payout_policy);
    assert_eq!(captured.record.ctv, original.inputs.ctv);
    assert!(captured.record.ctv.is_some());
    assert_eq!(captured.record.fee, original.fee);
    assert_eq!(captured.record.signer_keys, original.inputs.signer_keys);
    assert_eq!(
        captured.record.audit_builder_version,
        original.inputs.audit_builder_version
    );
    assert_eq!(
        captured.record.coinbase_suffix_hex,
        original.stored.coinbase_suffix
    );
    assert_eq!(captured.record.fingerprint, original.fingerprint);
    assert_eq!(captured.record.generation, original.generation);
    assert_eq!(
        captured.record.parent_hash,
        original.template["previousblockhash"]
    );
    assert_eq!(captured.record.parent_of_tip, original.parent_of_tip);
    let report = qbit_prism::verify_audit_bundle(
        original.bundle.as_ref().unwrap(),
        &original.inputs.signer_keys.ledger_key_hex,
    )
    .unwrap();
    let hashes = captured.record.audit_hashes.as_ref().unwrap();
    assert_eq!(hashes.audit_bundle_sha256, report.audit_bundle_sha256_hex);
    assert_eq!(
        hashes.coinbase_manifest_sha256,
        report.coinbase_manifest_sha256_hex
    );

    // The original payout is still leased while the DB's transaction fence advances.
    f.node.lock().unwrap().tip = hash(2);
    f.node.lock().unwrap().fail = Some("getblocktemplate".into());
    assert!(f.coordinator.refresh_once().await.is_err());
    f.store.revision.store(7, Ordering::SeqCst);
    f.store
        .compact
        .saves
        .lock()
        .unwrap()
        .extend([Ok(true), Ok(false)]);
    assert!(f
        .coordinator
        .save_captured_compact(&captured)
        .await
        .unwrap());
    f.store.clock_offset_ms.store(1000, Ordering::SeqCst);
    assert!(!f
        .coordinator
        .save_captured_compact(&captured)
        .await
        .unwrap());
    let saves = f.store.compact.save_calls.lock().unwrap();
    assert_eq!(saves.len(), 2);
    for saved in saves.iter() {
        assert_eq!(saved.key, captured.original.storage_key);
        assert_eq!(saved.record, captured.record);
        assert_eq!(saved.template_sha256, captured.template.sha256());
        assert_eq!(saved.balances, original.snapshot.prior_balances);
        assert_eq!(saved.current_revision, 7);
        assert_eq!(saved.record.payout_revision, 0);
        assert_eq!(saved.original_expires_at_ms, 130_000);
    }
}

#[tokio::test]
async fn bootstrap_capture_uses_original_empty_window_even_after_worker_build() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.store
        .snapshot
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .shares
        .clear();
    f.coordinator.refresh_once().await.unwrap();
    let source = original_build(f.coordinator.prepared.read().await.as_ref().unwrap());
    let worker = f.job(1, 0, "original.worker").context.worker.clone();
    let issued = f
        .coordinator
        .build_job(&worker, "00000001", 1e-12, 0.0)
        .await
        .unwrap();
    f.coordinator
        .persist_issued_job(&worker, &issued, 0, Duration::from_secs(30))
        .await
        .unwrap();
    let resumed = f
        .coordinator
        .resume_job(&worker, &issued.wire.job_id)
        .await
        .unwrap()
        .unwrap();
    let original = resumed.context.prepared.clone();
    assert!(original.bundle.is_some());
    assert!(original.stored.bundle.is_none());
    let captured = f
        .coordinator
        .capture_compact_prepared(source, 130_000)
        .await
        .unwrap();
    assert_eq!(captured.record.window, original.window);
    assert!(!Arc::ptr_eq(&captured.original, &original));
    assert!(captured.record.window.shares.is_none());
    assert!(captured.record.audit_hashes.is_none());
    assert_eq!(
        captured.record.coinbase_suffix_hex,
        original.stored.coinbase_suffix
    );
}

#[tokio::test]
async fn original_build_capture_does_not_recapture_legacy_resumed_inputs() {
    let mut f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    let source = original_build(&original);
    let worker = f.job(1, 0, "original.worker").context.worker.clone();
    let job = f
        .coordinator
        .build_job(&worker, "00000001", 1e-12, 0.0)
        .await
        .unwrap();
    f.coordinator
        .persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
        .await
        .unwrap();
    let config = Arc::get_mut(&mut Arc::get_mut(&mut f.coordinator).unwrap().config).unwrap();
    config.payout_policy.safety_multiplier += 1;
    let resumed = f
        .coordinator
        .resume_job(&worker, &job.wire.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        resumed.context.prepared.inputs.payout_policy,
        original.inputs.payout_policy
    );
    let captured = f
        .coordinator
        .capture_compact_prepared(source, 130_000)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&captured.original.stored, &original.stored));
    assert!(!Arc::ptr_eq(&captured.original, &resumed.context.prepared));
    assert_eq!(captured.record.payout_policy, original.inputs.payout_policy);
    let report = qbit_prism::verify_audit_bundle(
        original.stored.bundle.as_ref().unwrap(),
        &original.inputs.signer_keys.ledger_key_hex,
    )
    .unwrap();
    assert_eq!(
        captured
            .record
            .audit_hashes
            .as_ref()
            .unwrap()
            .audit_bundle_sha256,
        report.audit_bundle_sha256_hex
    );
}

struct ReleaseProbe(Arc<prepared_storage::RepairProbe>);
impl Drop for ReleaseProbe {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn with_probe() -> (Fixture, CompactOwner<OriginalPreparedBuild>, ReleaseProbe) {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let original = f.coordinator.prepared.read().await.clone().unwrap();
    let probe = Arc::new(prepared_storage::RepairProbe::default());
    let source =
        OriginalPreparedBuild::with_capture_probe(original_build(&original), probe.clone());
    (f, source, ReleaseProbe(probe))
}

#[tokio::test]
async fn cancelled_capture_retains_build_permit_until_actual_completion() {
    let (f, source, probe) = with_probe().await;
    let capture = tokio::spawn({
        let c = f.coordinator.clone();
        async move { c.capture_compact_prepared(source, 130_000).await }
    });
    probe.0.entered.notified().await;
    capture.abort();
    assert!(matches!(capture.await, Err(error) if error.is_cancelled()));
    assert_eq!(f.coordinator.build_slots.available_permits(), 0);
    assert!(f
        .coordinator
        .build_slots
        .clone()
        .try_acquire_owned()
        .is_err());
    probe.0.release();
    let permit = tokio::time::timeout(
        Duration::from_secs(5),
        f.coordinator.build_slots.clone().acquire_owned(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(probe.0.calls.load(Ordering::SeqCst), 1);
    assert!(f.store.compact.save_calls.lock().unwrap().is_empty());
    drop(permit);
    assert_eq!(f.coordinator.build_slots.available_permits(), 1);
}

#[tokio::test]
async fn save_revalidates_authority_and_fixed_deadline_after_capture_wait() {
    for expire in [false, true] {
        let (f, source, probe) = with_probe().await;
        let save = tokio::spawn({
            let c = f.coordinator.clone();
            async move {
                let captured = c.capture_compact_prepared(source, 130_000).await?;
                c.save_captured_compact(&captured).await
            }
        });
        probe.0.entered.notified().await;
        if expire {
            f.store.clock_offset_ms.store(30_000, Ordering::SeqCst);
        } else {
            f.store.revision.store(1, Ordering::SeqCst);
        }
        probe.0.release();
        assert!(save.await.unwrap().is_err());
        assert!(f.store.compact.save_calls.lock().unwrap().is_empty());
        assert_eq!(f.coordinator.build_slots.available_permits(), 1);
    }
}

#[tokio::test]
async fn typed_adapters_preserve_coherent_state_and_error_variants() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    let state = PayoutState {
        payout_revision: 17,
        prior_balances_digest: [9; 32],
    };
    f.store.compact.states.lock().unwrap().push_back(Ok(state));
    assert_eq!(f.store.payout_state().await.unwrap(), state);
    f.store
        .compact
        .states
        .lock()
        .unwrap()
        .push_back(Err(WindowError::Database(sqlx::Error::PoolClosed)));
    assert!(matches!(
        f.store.payout_state().await,
        Err(WindowError::Database(sqlx::Error::PoolClosed))
    ));
}

#[tokio::test]
async fn hydration_preserves_original_inputs_and_three_distinct_deadlines() {
    for empty in [false, true] {
        let mut f = Fixture::build(
            Duration::from_secs(10),
            |config| {
                config.ctv_enabled = true;
                config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
            },
            None,
        )
        .await;
        if empty {
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
        let publication = f.coordinator.prepared.read().await.clone().unwrap();
        let captured = captured(&f).await;
        let issued = issued(&f, &captured);
        let config = Arc::get_mut(&mut Arc::get_mut(&mut f.coordinator).unwrap().config).unwrap();
        config.payout_policy.safety_multiplier += 1;
        config.ctv_enabled = false;
        config.ctv_direct_floor += 1;
        config.ctv_fee = None;
        config.coinbase_tag = "/new config/".into();
        config.extranonce2_size += 1;
        // Reservation expired, but this original issued job and its retained
        // dependency are still live. Hydration must not invent another expiry.
        f.store.clock_offset_ms.store(40_000, Ordering::SeqCst);
        f.store
            .compact
            .reads
            .lock()
            .unwrap()
            .push_back(Ok(Some(observation(&captured))));
        script_window(&f, &captured);
        let hydrated = f
            .coordinator
            .hydrate_compact_inputs(&issued)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hydrated.record, captured.record);
        assert_eq!(hydrated.template, captured.original.stored.template);
        assert_eq!(
            serde_json::to_value(&hydrated.snapshot).unwrap(),
            serde_json::to_value(&captured.original.snapshot).unwrap()
        );
        assert_eq!(hydrated.inputs.payout_policy, captured.record.payout_policy);
        assert_ne!(
            hydrated.inputs.payout_policy,
            f.coordinator.config.payout_policy
        );
        assert_eq!(hydrated.inputs.ctv, captured.record.ctv);
        assert!(hydrated.inputs.ctv.is_some());
        assert_eq!(hydrated.inputs.signer_keys, captured.record.signer_keys);
        assert_eq!(
            hydrated.inputs.audit_builder_version,
            captured.record.audit_builder_version
        );
        assert_eq!(hydrated.original_expires_at_ms, 130_000);
        assert_eq!(hydrated.issued_expires_at_ms, 180_000);
        assert_eq!(hydrated.retained_until_ms, 190_000);
        assert_eq!(hydrated.observed_payout_revision, 37);
        assert_eq!(hydrated.snapshot.payout_revision, 0);
        assert_eq!(hydrated.record.audit_hashes.is_none(), empty);
        assert_eq!(
            WindowRef::from_snapshot(&hydrated.snapshot).unwrap(),
            captured.record.window
        );
        assert_eq!(
            *f.store.compact.window_calls.lock().unwrap(),
            vec![(captured.record.window, BalanceSource::AsIssued)]
        );
        assert_eq!(f.coordinator.window_reads.available_permits(), 1);
        assert_eq!(f.coordinator.build_slots.available_permits(), 0);
        assert_eq!(hydrated.build_permit.num_permits(), 1);
        assert!(Arc::ptr_eq(
            f.coordinator.prepared.read().await.as_ref().unwrap(),
            &publication
        ));
        drop(hydrated);
        cleanup_finished(&f).await;
    }
}

#[tokio::test]
async fn builder_and_key_incompatibility_miss_before_window_or_build_admission() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let captured = captured(&f).await;
    let issued = issued(&f, &captured);
    let _occupied = f
        .coordinator
        .build_slots
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    for kind in 0..4 {
        let mut stored = observation(&captured);
        match kind {
            0 => stored.record.audit_builder_version += 1,
            1 => stored.record.signer_keys.manifest_key_hex = hash(0x55),
            2 => stored.record.signer_keys.ledger_key_hex = hash(0x66),
            _ => stored
                .record
                .signer_keys
                .manifest_key_hex
                .make_ascii_uppercase(),
        }
        f.store
            .compact
            .reads
            .lock()
            .unwrap()
            .push_back(Ok(Some(stored)));
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            f.coordinator.hydrate_compact_inputs(&issued)
        )
        .await
        .expect("incompatibility must not wait for build admission")
        .unwrap()
        .is_none());
    }
    assert!(f.store.compact.window_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_and_cross_kind_links_error_before_lookup_but_foreign_instances_work() {
    use crate::coordinator::prepared_storage::compact::InvalidPreparedDependency;
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let captured = captured(&f).await;
    let mut issued = issued(&f, &captured);
    for key in [
        String::new(),
        "issued-job".into(),
        "frontend:0123456789abcdef0123456789abcdef".into(),
        "prepared:frontend".into(),
        "prepared:frontend:invalid".into(),
        "prepared:prepared:frontend:0123456789abcdef0123456789abcdef".into(),
        "prepared:bad\ninstance:0123456789abcdef0123456789abcdef".into(),
        format!(
            "prepared:{}:0123456789abcdef0123456789abcdef",
            "x".repeat(129)
        ),
    ] {
        issued.prepared_key = key;
        let result = f.coordinator.hydrate_compact_inputs(&issued).await;
        assert!(matches!(result, Err(error) if error.is::<InvalidPreparedDependency>()));
    }
    assert!(f.store.compact.read_keys.lock().unwrap().is_empty());
    for instance in ["", "other-frontend", "other:zone:frontend"] {
        issued.prepared_key = format!("prepared:{instance}:0123456789abcdef0123456789abcdef");
        f.store.compact.reads.lock().unwrap().push_back(Ok(None));
        assert!(f
            .coordinator
            .hydrate_compact_inputs(&issued)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            f.store.compact.read_keys.lock().unwrap().last(),
            Some(&issued.prepared_key)
        );
    }
}

#[tokio::test]
async fn hydration_keeps_lookup_errors_distinct_from_absence_and_expiry() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let captured = captured(&f).await;
    let mut issued = issued(&f, &captured);
    issued.expires_at_ms = 100_000;
    assert!(f
        .coordinator
        .hydrate_compact_inputs(&issued)
        .await
        .unwrap()
        .is_none());
    assert!(f.store.compact.read_keys.lock().unwrap().is_empty());
    issued.expires_at_ms = 180_000;
    let task_error = tokio::spawn(async { panic!("controlled compact lookup task failure") })
        .await
        .unwrap_err();
    f.store.compact.reads.lock().unwrap().extend([
        Ok(None),
        Err(sqlx::Error::PoolClosed.into()),
        Err(serde_json::from_str::<Value>("{").unwrap_err().into()),
        Err(WindowError::SnapshotDigestMismatch {
            expected: [1; 32],
            actual: [2; 32],
        }
        .into()),
        Err(task_error.into()),
    ]);
    assert!(f
        .coordinator
        .hydrate_compact_inputs(&issued)
        .await
        .unwrap()
        .is_none());
    for kind in 0..4 {
        let result = f.coordinator.hydrate_compact_inputs(&issued).await;
        let Err(error) = result else {
            panic!("lookup failure must remain an error")
        };
        assert!(match kind {
            0 => error.is::<sqlx::Error>(),
            1 => error.is::<serde_json::Error>(),
            2 => matches!(
                error.downcast_ref::<WindowError>(),
                Some(WindowError::SnapshotDigestMismatch { .. })
            ),
            _ => error
                .downcast_ref::<tokio::task::JoinError>()
                .is_some_and(|error| error.is_panic()),
        });
    }
    assert!(f.store.compact.window_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn hydration_rechecks_issued_and_retention_deadlines_after_window_wait() {
    for retained in [false, true] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let captured = captured(&f).await;
        let issued = issued(&f, &captured);
        let mut stored = observation(&captured);
        if retained {
            stored.expires_at_ms = 170_000;
        }
        f.store
            .compact
            .reads
            .lock()
            .unwrap()
            .push_back(Ok(Some(stored)));
        script_window(&f, &captured);
        let gate = Arc::new(Gate::default());
        *f.store.compact.window_gate.lock().unwrap() = Some(gate.clone());
        let hydrate = tokio::spawn({
            let c = f.coordinator.clone();
            async move { c.hydrate_compact_inputs(&issued).await }
        });
        gate.entered.notified().await;
        assert_eq!(f.coordinator.build_slots.available_permits(), 0);
        assert_eq!(f.coordinator.window_reads.available_permits(), 0);
        f.store
            .clock_offset_ms
            .store(if retained { 70_000 } else { 80_000 }, Ordering::SeqCst);
        gate.release.notify_one();
        assert!(hydrate.await.unwrap().unwrap().is_none());
        cleanup_finished(&f).await;
        assert_eq!(f.coordinator.window_reads.available_permits(), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn hydrated_inputs_and_permit_survive_until_blocking_cleanup_finishes() {
    use crate::coordinator::prepared_storage::compact::CompactDropProbe;
    let runtime_thread = std::thread::current().id();
    for outcome in ["cancel", "clock error", "expired", "returned"] {
        let f = Fixture::new(Duration::from_secs(10)).await;
        f.coordinator.refresh_once().await.unwrap();
        let captured = captured(&f).await;
        let issued = issued(&f, &captured);
        f.store
            .compact
            .reads
            .lock()
            .unwrap()
            .push_back(Ok(Some(observation(&captured))));
        script_window(&f, &captured);
        let (dropped, receive) = tokio::sync::oneshot::channel();
        let release = ReleaseProbe(Arc::new(prepared_storage::RepairProbe::default()));
        *f.store.compact.drop_probe.lock().unwrap() = Some(CompactDropProbe {
            dropped: Some(dropped),
            release: release.0.clone(),
            runtime_thread,
        });
        let window = Arc::new(Gate::default());
        *f.store.compact.window_gate.lock().unwrap() = Some(window.clone());
        let hydrate = tokio::spawn({
            let c = f.coordinator.clone();
            async move { c.hydrate_compact_inputs(&issued).await }
        });
        window.entered.notified().await;
        // Install the clock gate only after the initial clock read, so this
        // wait owns the reader's successfully reconstructed window.
        let clock = Arc::new(Gate::default());
        *f.store.compact.clock_gate.lock().unwrap() = Some(clock.clone());
        window.release.notify_one();
        clock.entered.notified().await;
        let mut external_drop = None;
        match outcome {
            "cancel" => {
                hydrate.abort();
                assert!(matches!(hydrate.await, Err(error) if error.is_cancelled()));
            }
            "clock error" => {
                f.store.compact.fail_clock.store(true, Ordering::SeqCst);
                clock.release.notify_one();
                assert!(matches!(hydrate.await.unwrap(), Err(error) if error.is::<sqlx::Error>()));
            }
            "expired" => {
                f.store.clock_offset_ms.store(80_000, Ordering::SeqCst);
                clock.release.notify_one();
                assert!(hydrate.await.unwrap().unwrap().is_none());
            }
            _ => {
                clock.release.notify_one();
                let inputs = hydrate.await.unwrap().unwrap().unwrap();
                // Dropping after handoff, outside a Tokio context, still uses
                // the captured runtime for owned blocking cleanup.
                let (finished, completion) = tokio::sync::oneshot::channel();
                let thread = std::thread::spawn(move || {
                    drop(inputs);
                    let _ = finished.send(());
                });
                external_drop = Some((thread, completion));
            }
        }
        let dropped_on = tokio::time::timeout(Duration::from_secs(5), receive)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(dropped_on, runtime_thread);
        if let Some((thread, _)) = &external_drop {
            // An inline drop on this external thread must fail rather than
            // deadlock the runtime in join() while the probe awaits release.
            assert_ne!(dropped_on, thread.thread().id());
        }
        assert_eq!(
            f.coordinator.build_slots.available_permits(),
            0,
            "{outcome}"
        );
        release.0.release();
        cleanup_finished(&f).await;
        if let Some((thread, completion)) = external_drop {
            tokio::time::timeout(Duration::from_secs(5), completion)
                .await
                .unwrap()
                .unwrap();
            thread.join().unwrap();
        }
    }
}

#[tokio::test]
async fn typed_lookup_and_save_keep_misses_errors_and_duplicate_results_distinct() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let captured = captured(&f).await;
    f.store.compact.reads.lock().unwrap().extend([
        Ok(Some(observation(&captured))),
        Ok(None),
        Err(sqlx::Error::PoolClosed.into()),
        Err(serde_json::from_str::<Value>("{").unwrap_err().into()),
    ]);
    let key = &captured.original.storage_key;
    let stored = f.store.compact_prepared(key).await.unwrap().unwrap();
    assert_eq!(stored.record, captured.record);
    assert_eq!(stored.original_expires_at_ms, 130_000);
    assert_eq!(stored.expires_at_ms, 190_000);
    assert!(f.store.compact_prepared(key).await.unwrap().is_none());
    let error = f.store.compact_prepared(key).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::PoolClosed)
    ));
    assert!(f
        .store
        .compact_prepared(key)
        .await
        .unwrap_err()
        .is::<serde_json::Error>());
    assert_eq!(
        *f.store.compact.read_keys.lock().unwrap(),
        vec![key.clone(); 4]
    );
    f.store
        .compact
        .saves
        .lock()
        .unwrap()
        .extend([Ok(false), Err(sqlx::Error::PoolClosed.into())]);
    assert!(!f
        .coordinator
        .save_captured_compact(&captured)
        .await
        .unwrap());
    assert!(f
        .coordinator
        .save_captured_compact(&captured)
        .await
        .unwrap_err()
        .is::<sqlx::Error>());
}

#[tokio::test]
async fn window_adapter_forwards_as_issued_reference_and_typed_failures() {
    let f = Fixture::new(Duration::from_secs(10)).await;
    f.coordinator.refresh_once().await.unwrap();
    let captured = captured(&f).await;
    let issued = issued(&f, &captured);
    let reference = captured.record.window;
    let task_error = tokio::spawn(async { panic!("controlled window task failure") })
        .await
        .unwrap_err();
    f.store.compact.windows.lock().unwrap().extend([
        Err(WindowError::Incomplete {
            expected: 2,
            got: 1,
        }),
        Err(WindowError::BalanceSnapshotMissing { digest: [8; 32] }),
        Err(WindowError::SnapshotDigestMismatch {
            expected: [1; 32],
            actual: [2; 32],
        }),
        Err(WindowError::Database(sqlx::Error::PoolClosed)),
        Err(WindowError::Decode(anyhow::anyhow!(
            "controlled decode failure"
        ))),
        Err(WindowError::TaskFailed(task_error)),
    ]);
    for kind in 0..6 {
        f.store
            .compact
            .reads
            .lock()
            .unwrap()
            .push_back(Ok(Some(observation(&captured))));
        let Err(error) = f.coordinator.hydrate_compact_inputs(&issued).await else {
            panic!("window failure must remain an error")
        };
        assert!(match (kind, error.downcast::<WindowError>().unwrap()) {
            (
                0,
                WindowError::Incomplete {
                    expected: 2,
                    got: 1,
                },
            ) => true,
            (1, WindowError::BalanceSnapshotMissing { digest }) => digest == [8; 32],
            (2, WindowError::SnapshotDigestMismatch { expected, actual }) =>
                expected == [1; 32] && actual == [2; 32],
            (3, WindowError::Database(sqlx::Error::PoolClosed)) => true,
            (4, WindowError::Decode(error)) => error.to_string() == "controlled decode failure",
            (5, WindowError::TaskFailed(error)) => error.is_panic(),
            _ => false,
        });
        assert_eq!(f.coordinator.window_reads.available_permits(), 1);
        cleanup_finished(&f).await;
    }
    assert_eq!(
        *f.store.compact.window_calls.lock().unwrap(),
        vec![(reference, BalanceSource::AsIssued); 6]
    );
}
