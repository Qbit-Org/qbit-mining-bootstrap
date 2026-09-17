//! Exercise the production Stratum codec, listener and submit observation path.
use super::*;
use qbit_prism_server::metrics::Metrics;

#[path = "contract.rs"]
mod contract;

#[tokio::test]
async fn stratum_hashes_heights_jobs_and_miners_never_enter_metrics_exposition() {
    let metrics = Arc::new(Metrics::default());
    let config = StratumConfig::default();
    let stats = config.stats.clone();
    let (address, backend, _refresh, shutdown, task) =
        start_with_metrics(config, metrics.clone()).await;
    let mut previous: Option<(String, u64)> = None;
    let mut identities = Vec::new();
    let mut heights = Vec::new();
    for (index, generation) in [0x5eed_cafe_u64, 0x1bad_b002, 0x6a09_e667]
        .into_iter()
        .enumerate()
    {
        backend.generation.store(generation, Ordering::SeqCst);
        backend
            .jobs
            .store(861_729_453 + index as u64 * 103, Ordering::Relaxed);
        let height = generation + 1;
        let parent = format!("{generation:064x}");
        let username = format!("miner.privacy-sentinel-{index}-d3a76f90");
        let mut client = Client::connect(address).await;
        client.login(&username).await;
        let job_id = client.notify["params"][0].as_str().unwrap().to_owned();
        // Positive evidence that the synthetic identity/height reached actual
        // issued work, rather than merely existing in this test's local data.
        {
            let jobs = backend.stored.lock().unwrap();
            let (job, worker, _, _) = &jobs[&job_id];
            assert_eq!(worker.username, username);
            assert_eq!(job.wire.previousblockhash, parent);
            assert!(job
                .wire
                .coinb1
                .contains(&hex::encode((height as u32).to_le_bytes())));
        }
        client.send(client.solved_submit(20, &username, 0)).await;
        assert_eq!(client.response(20).await["result"], true);
        assert!(backend.credited_workers.lock().unwrap().contains(&username));
        assert_eq!(backend.shares.lock().unwrap().len(), index + 1);
        identities.extend([parent, username.clone(), job_id]);
        identities.extend(backend.shares.lock().unwrap().iter().cloned());
        heights.push(height);

        // An identifier-bearing backend error traverses the real normalizer;
        // its unchanged protocol metadata must not become a metric label.
        let reason = format!("future-reason-{}-{height}", identities[0]);
        let message = format!("synthetic refusal for {username} at height {height}");
        *backend.submit_error.lock().unwrap() = Some(StratumError {
            code: 20,
            message: message.clone(),
            reason_id: Some(reason.clone()),
        });
        client.send(client.solved_submit(21, &username, 1000)).await;
        assert_eq!(
            client.response(21).await["error"],
            json!([20, message, {"reason_id":reason}])
        );
        identities.extend([reason, message]);
        metrics.publish_stratum(&stats.snapshot(generation), true, 2, 0);
        let body = observability::http_metrics(metrics.clone()).await;
        contract::validate(&body, false).unwrap();
        contract::private_identifiers(&body, &identities, &heights).unwrap();
        let values = contract::samples(&body).unwrap();
        assert_eq!(
            values["qbit_prism_accepted_shares_total"],
            (index + 1) as f64
        );
        assert_eq!(
            values["qbit_prism_rejections_total{reason_id=\"unrecognised\"}"],
            (index + 1) as f64
        );
        assert_eq!(
            values["qbit_prism_share_ack_seconds_count{result=\"accepted\"}"],
            (index + 1) as f64
        );
        assert_eq!(
            values["qbit_prism_share_ack_seconds_count{result=\"rejected\"}"],
            (index + 1) as f64
        );
        if let Some((before, before_height)) = previous {
            contract::height_independent(&before, before_height, &body, height).unwrap();
        }
        previous = Some((body, height));
        drop(client);
    }
    shutdown.send(true).unwrap();
    task.await.unwrap();
}
