use super::*;

#[tokio::test]
async fn real_refresh_build_persist_publish_and_client_job_delivery_have_constant_rpc_count() {
    let mut expected = None;
    for clients in [1, 32, 128] {
        let fixture = Fixture::new(Duration::from_secs(30)).await;
        fixture.coordinator.refresh_once().await.unwrap();
        let before = fixture.node.lock().unwrap().calls.clone();
        if let Some(expected) = &expected {
            assert_eq!(&before, expected);
        } else {
            expected = Some(before.clone());
        }
        assert!(before.contains(&"getblocktemplate".into()));
        assert_eq!(
            before
                .iter()
                .filter(|method| method.as_str() == "getblockheader")
                .count(),
            1
        );
        for client in 0..clients {
            let worker = fixture
                .job(1, 0, &format!("original.worker{client}"))
                .context
                .worker
                .clone();
            let job = fixture
                .coordinator
                .build_job(&worker, &format!("{client:08x}"), 1e-12, 0.0)
                .await
                .unwrap();
            fixture
                .coordinator
                .persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
                .await
                .unwrap();
            assert_eq!(job.wire.previousblockhash, hash(1));
            assert_eq!(
                job.wire.refresh_generation,
                *fixture.coordinator.refresh.borrow()
            );
        }
        assert_eq!(
            fixture.node.lock().unwrap().calls,
            before,
            "per-client work must not issue node RPC"
        );
        assert_eq!(
            fixture.store.jobs.lock().unwrap().len(),
            clients + 1,
            "prepared bundle stored once, issued records once each"
        );
    }
}

#[tokio::test]
async fn failed_real_refresh_retains_published_credit_until_lease_expires() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let old = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    fixture.node.lock().unwrap().tip = hash(2);
    fixture.store.fail_save.store(true, Ordering::SeqCst);
    assert!(fixture.coordinator.refresh_once().await.is_err());
    let started = fixture
        .coordinator
        .observed_tip
        .read()
        .await
        .divergence_for_test();
    assert!(fixture.coordinator.refresh_once().await.is_err());
    fixture.detect(3).await;
    assert!(fixture.coordinator.refresh_once().await.is_err());
    assert_eq!(
        fixture
            .coordinator
            .observed_tip
            .read()
            .await
            .divergence_for_test(),
        started,
        "repeated failures and newer detected tips must not renew the lease"
    );
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .age_for_test(Duration::from_secs(11));
    fixture.submit(&old, false).await.unwrap();
    assert_eq!(
        fixture.store.records.lock().unwrap()[0].0.credit_policy,
        None
    );
    fixture
        .coordinator
        .observed_tip
        .write()
        .await
        .expire_lease_for_test(Duration::from_secs(121));
    assert_error(
        fixture.submit(&old, false).await.unwrap_err(),
        "stale-job",
        "stale job",
    );
}

#[tokio::test]
async fn real_refresh_persistence_gate_preserves_lease_then_publishes_delivery_grace() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let old = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    fixture.node.lock().unwrap().tip = hash(2);
    let gate = Arc::new(Gate::default());
    *fixture.store.save_gate.lock().unwrap() = Some(gate.clone());
    let coordinator = fixture.coordinator.clone();
    let refresh = tokio::spawn(async move { coordinator.refresh_once().await });
    gate.entered.notified().await;
    fixture.submit(&old, false).await.unwrap();
    assert_eq!(
        fixture.store.records.lock().unwrap()[0].0.credit_policy,
        None
    );
    assert!(fixture.store.records.lock().unwrap()[0].1.is_none());
    let issued = fixture
        .coordinator
        .build_job(&worker, "00000002", 1e-12, 0.0)
        .await
        .unwrap();
    fixture
        .coordinator
        .persist_issued_job(&worker, &issued, 0, Duration::from_secs(30))
        .await
        .unwrap();
    let resumed = fixture
        .coordinator
        .resume_job(&worker, &issued.wire.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.wire.previousblockhash, old.wire.previousblockhash);
    assert_eq!(
        resumed.context.prepared.snapshot.payout_revision,
        old.context.prepared.snapshot.payout_revision
    );
    gate.release.notify_one();
    refresh.await.unwrap().unwrap();
    assert_error(
        fixture.submit(&old, false).await.unwrap_err(),
        "stale-job",
        "stale job",
    );
    // New publication precedes per-connection delivery; grace is now eligible.
    let proof = fixture.proof(&old, 100);
    fixture
        .coordinator
        .submit(&worker, &old, proof, true.into())
        .await
        .unwrap();
    assert_eq!(
        fixture.store.records.lock().unwrap()[1]
            .0
            .credit_policy
            .as_deref(),
        Some("stale-grace")
    );
}

#[tokio::test]
async fn resume_stale_payout_returns_unknown_while_revision_outage_remains_unavailable() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let issued = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    fixture
        .coordinator
        .persist_issued_job(&worker, &issued, 0, Duration::from_secs(30))
        .await
        .unwrap();
    fixture.store.revision.store(1, Ordering::SeqCst);
    assert!(fixture
        .coordinator
        .resume_job(&worker, &issued.wire.job_id)
        .await
        .unwrap()
        .is_none());
    fixture.store.fail_revision.store(true, Ordering::SeqCst);
    assert_error(
        fixture
            .coordinator
            .resume_job(&worker, &issued.wire.job_id)
            .await
            .err()
            .unwrap(),
        "backend-rpc-unavailable",
        "job resume unavailable",
    );
}

#[tokio::test]
async fn replacement_lease_cannot_persist_a_superseded_same_parent_payout() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    fixture.coordinator.refresh_once().await.unwrap();
    let worker = fixture.job(1, 0, "original.worker").context.worker.clone();
    let old = fixture
        .coordinator
        .build_job(&worker, "00000000", 1e-12, 0.0)
        .await
        .unwrap();
    fixture.store.revision.store(1, Ordering::SeqCst);
    fixture.coordinator.refresh_once().await.unwrap();
    assert_eq!(
        fixture
            .coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .unwrap()
            .snapshot
            .payout_revision,
        1
    );
    fixture.detect(2).await;
    let result = fixture
        .coordinator
        .persist_issued_job(&worker, &old, 0, Duration::from_secs(30))
        .await;
    assert!(
        result.is_err(),
        "lease must not resurrect an already replaced payout snapshot"
    );
    assert!(!fixture
        .store
        .jobs
        .lock()
        .unwrap()
        .contains_key(&old.wire.job_id));
}

/// Observations of `qbit_prism_refresh_seconds` with `trigger`, summed over
/// every acquisition, beside the rendered refresh samples for the message.
fn refresh_observations(coordinator: &Coordinator, trigger: &str) -> (f64, String) {
    let wanted = format!("trigger=\"{trigger}\"");
    let rendered = coordinator.metrics.render();
    let samples: Vec<&str> = rendered
        .lines()
        .filter(|line| line.starts_with("qbit_prism_refresh_seconds_count{"))
        .collect();
    let count = samples
        .iter()
        .filter(|line| line.contains(&wanted))
        .map(|line| line.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
        .fold(0.0, |sum, value| sum + value);
    (count, samples.join("\n"))
}

fn assert_refresh_observations(coordinator: &Coordinator, trigger: &str, expected: f64) {
    let (count, samples) = refresh_observations(coordinator, trigger);
    assert_eq!(count, expected, "trigger={trigger} samples:\n{samples}");
}

/// The refresh trigger label keeps its precedence on a new template for the
/// same tip, where no ledger probe runs at entry: a payout revision that
/// changed as well is named `revision`, not `template`, and a template change
/// alone stays `template`.
#[tokio::test]
async fn same_tip_template_change_names_a_revision_change_ahead_of_template() {
    let fixture = Fixture::new(Duration::from_secs(10)).await;
    // The fixture installs published work without a cached window, so the
    // first refresh rebuilds on the same template and is labelled `reanchor`,
    // as documented; it leaves the cached window the next refreshes read.
    fixture.coordinator.refresh_once().await.unwrap();
    assert_refresh_observations(&fixture.coordinator, "reanchor", 1.0);
    let retemplate = |coinbasevalue: u64| {
        let mut node = fixture.node.lock().unwrap();
        let tip = node.tip.clone();
        node.template = Some(json!({"version":0x20000000u32,"bits":"207fffff",
            "curtime":chrono::Utc::now().timestamp(),"previousblockhash":tip,
            "transactions":[],"height":101,"coinbasevalue":coinbasevalue}));
    };
    // A new template on the same tip and a new payout revision on one poll.
    retemplate(500_000_001);
    fixture.store.revision.store(1, Ordering::SeqCst);
    fixture.coordinator.refresh_once().await.unwrap();
    assert_eq!(
        fixture
            .coordinator
            .prepared
            .read()
            .await
            .as_ref()
            .unwrap()
            .snapshot
            .payout_revision,
        1
    );
    assert_refresh_observations(&fixture.coordinator, "revision", 1.0);
    assert_refresh_observations(&fixture.coordinator, "template", 0.0);
    // A new template alone.
    retemplate(500_000_002);
    fixture.coordinator.refresh_once().await.unwrap();
    assert_refresh_observations(&fixture.coordinator, "template", 1.0);
    assert_refresh_observations(&fixture.coordinator, "revision", 1.0);
}

/// The label's precedence over what changed, `template` last.
#[test]
fn refresh_trigger_precedence_is_tip_revision_balances_reanchor_shares_fee_template() {
    let within = RefreshChanges {
        window_within_reanchor: true,
        ..RefreshChanges::default()
    };
    assert_eq!(classify_refresh(within), RefreshTrigger::Template);
    assert_eq!(
        classify_refresh(RefreshChanges {
            fee: true,
            ..within
        }),
        RefreshTrigger::Fee
    );
    assert_eq!(
        classify_refresh(RefreshChanges {
            shares: true,
            fee: true,
            ..within
        }),
        RefreshTrigger::Shares
    );
    assert_eq!(
        classify_refresh(RefreshChanges {
            window_within_reanchor: false,
            shares: true,
            fee: true,
            ..within
        }),
        RefreshTrigger::Reanchor
    );
    assert_eq!(
        classify_refresh(RefreshChanges {
            balances: true,
            window_within_reanchor: false,
            shares: true,
            fee: true,
            ..within
        }),
        RefreshTrigger::Balances
    );
    assert_eq!(
        classify_refresh(RefreshChanges {
            revision: true,
            balances: true,
            window_within_reanchor: false,
            shares: true,
            fee: true,
            ..within
        }),
        RefreshTrigger::Revision
    );
    assert_eq!(
        classify_refresh(RefreshChanges {
            tip: true,
            revision: true,
            balances: true,
            window_within_reanchor: false,
            shares: true,
            fee: true,
        }),
        RefreshTrigger::Tip
    );
}
