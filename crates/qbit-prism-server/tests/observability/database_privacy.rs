use super::*;

#[path = "contract.rs"]
mod contract;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; required CI runs --test observability_database -- --ignored"]
async fn candidate_identifiers_and_heights_never_enter_collector_http_metrics() -> Result<()> {
    let raw = gate::required_database_url(gate::site!())?;
    let admin = PgPool::connect(&raw).await?;
    let schema = format!("prism_privacy_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let mut url = url::Url::parse(&raw)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let result = async {
        let ledger = Ledger::connect(url.as_str(), "metrics-privacy-test".into(), 4, true).await?;
        let result = check_candidates(&ledger).await;
        ledger.pool.close().await;
        result
    }
    .await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    result
}

async fn check_candidates(ledger: &Ledger) -> Result<()> {
    let metrics = Arc::new(Metrics::default());
    let state = ApiState::new(ledger.pool.clone(), ApiConfig::default(), metrics.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/metrics", listener.local_addr()?);
    let app = router(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let result = async {
        let mut identifiers = Vec::new();
        let mut heights = Vec::new();
        let mut previous: Option<(String, u64)> = None;
        for (index, height) in [1_392_746_851_u64, 826_491_573, 1_725_938_461].into_iter().enumerate() {
            let hash = format!("{:064x}", 0xdecafbad_u64 + index as u64);
            let digest = format!("{:064x}", 0xcafed00d_u64 + index as u64);
            let miner = format!("miner.candidate-privacy-{index}-e79dc421");
            let job = format!("job-candidate-privacy-{index}-a843f690");
            let candidate = serde_json::json!({"block_hash":hash,"block_height":height,"height":height,"job_id":job,"miner":miner});
            // The collector intentionally reads only metadata; arbitrary JSON
            // must neither be decoded as a candidate nor exposed as labels.
            sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,created_at) VALUES($1,$2,$3,clock_timestamp()-interval '5 seconds')")
                .bind(&hash).bind(&candidate).bind(&digest).execute(&ledger.pool).await?;
            let stored: serde_json::Value = sqlx::query_scalar("SELECT candidate FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(&hash).fetch_one(&ledger.pool).await?;
            ensure!(stored == candidate, "privacy fixture must reach the collector's real source table");
            identifiers.extend([hash, digest, miner, job]);
            heights.push(height);
            let snapshot = collectors::database(&ledger.pool, &metrics).await?;
            ensure!(snapshot.candidates == (index + 1) as u64);
            ensure!(snapshot.candidate_oldest >= Duration::from_secs(5));
            metrics.publish_database(Some(snapshot));
            state.publish_metrics(metrics.render())?;
            let response = client.get(&endpoint).send().await?.error_for_status()?;
            ensure!(response.headers()["x-prism-metrics-state"] == "fresh");
            ensure!(response.headers()["cache-control"] == "no-store");
            let body = response.text().await?;
            contract::validate(&body, false).map_err(anyhow::Error::msg)?;
            contract::private_identifiers(&body, &identifiers, &heights).map_err(anyhow::Error::msg)?;
            ensure!(sample(&body, "qbit_prism_block_candidates_pending") == (index + 1) as f64);
            if let Some((before, before_height)) = previous {
                contract::height_independent(&before, before_height, &body, height).map_err(anyhow::Error::msg)?;
            }
            previous = Some((body, height));
        }
        // An unsuccessful observation remains unknown through the same HTTP
        // path; the last cached success must not retain either data or identity.
        metrics.publish_database(None);
        let body = client.get(&endpoint).send().await?.error_for_status()?.text().await?;
        contract::validate(&body, false).map_err(anyhow::Error::msg)?;
        contract::private_identifiers(&body, &identifiers, &heights).map_err(anyhow::Error::msg)?;
        ensure!(sample(&body, "qbit_prism_block_candidates_pending") == -1.);
        ensure!(sample(&body, "qbit_prism_collector_success{collector=\"database\"}") == 0.);
        Ok(())
    }.await;
    server.abort();
    let _ = server.await;
    result
}
