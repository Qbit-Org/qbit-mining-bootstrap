//! #529 on a real PostgreSQL: the found-block offer's standby wait never holds
//! the block. The fixture's database has no replication, so a configured
//! standby is not streaming there: the block must be offered at once, landed
//! once and counted `absent`. With the wait off nothing is read or counted.
//! A lagging standby, one that stopped, and a role that cannot read the
//! positions are covered against a real primary/standby pair in
//! `tests/support/offer_standby_flush.rs`.
use super::*;
use std::collections::BTreeMap;

impl Fixture {
    /// A third frontend on the fixture's database with its own metrics and
    /// the standby wait `offer_standby`.
    async fn standby_frontend(
        &self,
        instance_id: &str,
        offer_standby: Option<crate::ledger::OfferStandbyWait>,
    ) -> Result<Arc<Coordinator>> {
        let frontend = Coordinator::new(
            Config {
                instance_id: instance_id.into(),
                offer_standby,
                ..(*self.coordinator.config).clone()
            },
            Arc::new(crate::metrics::Metrics::default()),
        )
        .await?;
        *frontend.observed_tip.write().await = TipState::baseline("aa".repeat(32));
        Ok(frontend)
    }
}

/// `qbit_prism_block_offer_standby_wait_total` by outcome.
fn standby_waits(metrics: &crate::metrics::Metrics) -> BTreeMap<String, u64> {
    let rendered = metrics.render();
    ["confirmed", "absent", "lagging", "failed"]
        .into_iter()
        .map(|outcome| {
            let prefix =
                format!("qbit_prism_block_offer_standby_wait_total{{outcome=\"{outcome}\"}} ");
            let count = rendered
                .lines()
                .find_map(|line| line.strip_prefix(prefix.as_str()))
                .and_then(|value| value.trim().parse::<f64>().ok())
                .map_or(u64::MAX, |value| value as u64);
            (outcome.to_owned(), count)
        })
        .collect()
}

fn counted(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs
        .iter()
        .map(|(outcome, count)| ((*outcome).to_owned(), *count))
        .collect()
}

async fn offered_once(fixture: &Fixture, instance_id: &str) -> Result<()> {
    let row = fixture.row().await?;
    ensure!(
        row.state == "submitted"
            && row.outcome.as_deref() == Some("accepted")
            && row.reserved_by.as_deref() == Some(instance_id),
        "the found block did not land from its one offer: state {}, outcome {:?}, reserved by {:?}",
        row.state,
        row.outcome,
        row.reserved_by
    );
    ensure!(fixture.landed().await?, "the offered block did not land");
    ensure!(
        fixture.submissions().await == 1,
        "the block was not offered exactly once"
    );
    Ok(())
}

/// A standby that is not streaming has nothing to wait for: the block goes
/// to the node at once, far inside the bound, and the offer is counted
/// `absent`. A wait that held the block for its bound would take ten seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_found_block_is_offered_at_once_when_the_configured_standby_is_not_streaming(
) -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let bound = Duration::from_secs(10);
    let frontend = fixture
        .standby_frontend(
            "candidate-standby-absent",
            Some(crate::ledger::OfferStandbyWait {
                application_name: "prism_standby_not_streaming".into(),
                bound,
            }),
        )
        .await?;
    let result = async {
        // `absent`, not `failed`: the test role reads replication positions.
        let readable: bool =
            sqlx::query_scalar("SELECT pg_has_role(current_user,'pg_read_all_stats','USAGE')")
                .fetch_one(&fixture.admin)
                .await?;
        ensure!(
            readable,
            "the test database role must be a superuser or hold pg_monitor"
        );
        let started = std::time::Instant::now();
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        let took = started.elapsed();
        ensure!(
            took < bound / 2,
            "the offer took {took:?}: an absent standby held the found block"
        );
        offered_once(&fixture, "candidate-standby-absent").await?;
        let waits = standby_waits(&frontend.metrics);
        ensure!(
            waits
                == counted(&[
                    ("confirmed", 0),
                    ("absent", 1),
                    ("lagging", 0),
                    ("failed", 0)
                ]),
            "the offer was not counted absent: {waits:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}

/// With the wait off (the default) the offer reads no replication state and
/// counts nothing; the block lands from its one offer as before #529.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_the_standby_wait_off_a_found_block_is_offered_without_a_wait() -> Result<()> {
    let _serial = TEST_LOCK.lock().await;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let frontend = fixture
        .standby_frontend("candidate-standby-off", None)
        .await?;
    let result = async {
        frontend
            .process_candidate_with_lease(&fixture.claim, CANDIDATE_LEASE)
            .await?;
        offered_once(&fixture, "candidate-standby-off").await?;
        let waits = standby_waits(&frontend.metrics);
        ensure!(
            waits
                == counted(&[
                    ("confirmed", 0),
                    ("absent", 0),
                    ("lagging", 0),
                    ("failed", 0)
                ]),
            "an offer with the wait off was counted: {waits:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    frontend.ledger.pool.close().await;
    fixture.close().await?;
    result
}
