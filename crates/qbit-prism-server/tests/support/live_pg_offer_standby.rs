//! #529: a found block's offer waits, within a bound, for the dedicated
//! asynchronous standby to flush its candidate row and reservation
//! (`PRISM_OFFER_STANDBY_APPLICATION_NAME`, `PRISM_OFFER_STANDBY_FLUSH_WAIT_MS`),
//! while every share append stays asynchronous under D3.
//!
//! The drill cuts replication the moment the found block's `submitblock`
//! reaches the node, over a replication link that lags by a fixed delay, so
//! without the wait the standby cannot hold the block's row at the cut. The
//! old primary is then fenced and lost, the standby promoted, and the held
//! attempt records the node's answer on the new primary: the block survives
//! the failover and lands once. Without the wait, or with one that polled
//! `sent_lsn` instead of `flush_lsn`, the standby has no row for the offered
//! block at the cut.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The drill's wait bound: far above the link delay, so only a broken wait
/// lets the offer out before the standby has the row.
const DRILL_BOUND_MS: u64 = 5_000;
/// The replication link's delay in the drill: far above the test's reaction
/// from the node's first arrival to the cut.
const DRILL_LAG: Duration = Duration::from_millis(400);

/// Relay `client` and `upstream`, holding every upstream-to-client chunk for
/// `delay` before it is written, in order; client-to-upstream is immediate.
pub(super) async fn delayed_pump(
    client: tokio::net::TcpStream,
    upstream: tokio::net::TcpStream,
    delay: Duration,
) {
    let (mut client_read, mut client_write) = client.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let (chunks, mut due) =
        tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    let inbound = async move {
        let mut buffer = vec![0u8; 64 * 1024];
        while let Ok(read) = upstream_read.read(&mut buffer).await {
            if read == 0
                || chunks
                    .send((tokio::time::Instant::now() + delay, buffer[..read].to_vec()))
                    .is_err()
            {
                break;
            }
        }
    };
    let delivered = async move {
        while let Some((at, bytes)) = due.recv().await {
            tokio::time::sleep_until(at).await;
            if client_write.write_all(&bytes).await.is_err() {
                break;
            }
        }
    };
    let outbound = async move {
        let _ = tokio::io::copy(&mut client_read, &mut upstream_write).await;
    };
    tokio::select! {
        _ = outbound => {}
        _ = async { tokio::join!(inbound, delivered); } => {}
    }
}

impl Relay {
    /// Delay the target-to-client direction of connections accepted from
    /// here on.
    fn set_delay(&self, delay: Duration) {
        self.delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }
}

impl Pair {
    /// Reconnect the standby through a replication link that lags by `delay`
    /// and wait until it streams again.
    async fn lag_replication(&self, delay: Duration) -> Result<()> {
        self.replication.set_delay(delay);
        self.replication.fence();
        self.replication.route_to(self.primary.port);
        let admin = PgPool::connect(&self.url(self.primary.port)).await?;
        until("standby streaming through the lagging link", 60, || async {
            Ok(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_replication WHERE application_name=$1 AND state='streaming')",
            )
            .bind(STANDBY)
            .fetch_one(&admin)
            .await?)
        })
        .await?;
        admin.close().await;
        Ok(())
    }
}

fn wait_settings(bound_ms: u64) -> Vec<(&'static str, String)> {
    vec![
        (
            "RUST_LOG",
            "warn,qbit_prism_server::coordinator=info".to_owned(),
        ),
        ("PRISM_OFFER_STANDBY_APPLICATION_NAME", STANDBY.to_owned()),
        ("PRISM_OFFER_STANDBY_FLUSH_WAIT_MS", bound_ms.to_string()),
    ]
}

async fn start_servers(
    f: &mut Fixture,
    rpc_port: u16,
    extra: &[(&'static str, String)],
) -> Result<()> {
    for index in 0..2 {
        let mut overrides = vec![
            ("QBIT_RPC_PORT", rpc_port.to_string()),
            ("PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS", "120".to_owned()),
        ];
        overrides.extend(extra.iter().cloned());
        let process = f.start_server_with(index, None, &overrides)?;
        f.servers.push(process);
    }
    for index in 0..2 {
        until(&format!("server {index} readiness"), 30, || {
            healthy(f, index)
        })
        .await?;
    }
    Ok(())
}

/// `line` without its terminal colour sequences.
fn strip_ansi(line: &str) -> String {
    let mut plain = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(c);
        }
    }
    plain
}

/// The wait each server logged per block: `(block, outcome, waited_us)`.
fn logged_waits(f: &Fixture) -> Result<Vec<(String, String, u64)>> {
    let field = |line: &str, name: &str| {
        line.split_whitespace()
            .find_map(|word| word.strip_prefix(&format!("{name}=")))
            .map(|value| value.trim_matches('"').to_owned())
    };
    let mut waits = Vec::new();
    for index in 0..f.servers.len() {
        let log = std::fs::read_to_string(f.directory.path().join(format!("server-{index}.log")))?;
        for line in log
            .lines()
            .map(strip_ansi)
            .filter(|line| line.contains("block offer standby wait"))
        {
            let line = line.as_str();
            if let (Some(block), Some(outcome), Some(waited)) = (
                field(line, "block"),
                field(line, "outcome"),
                field(line, "waited_us").and_then(|value| value.parse().ok()),
            ) {
                waits.push((block, outcome, waited));
            }
        }
    }
    Ok(waits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_found_block_offered_after_the_standby_flush_survives_a_replication_cut_at_submitblock(
) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let _drill = DRILLS.lock().await;
    drop(SERIAL.lock().await);
    let mut pair = Pair::start(bin.into()).await?;
    pair.lag_replication(DRILL_LAG).await?;
    let Some(mut fixture) =
        Fixture::open_on_database(false, false, Some(&pair.url(pair.writer.port))).await?
    else {
        return Ok(());
    };
    let mut timeline = Timeline {
        started: Instant::now(),
        events: Vec::new(),
    };
    let result = async {
        let mut node = NodeGate::open(fixture.rpc_port).await?;
        let drilled = cut_after_found(&mut fixture, &mut pair, &mut node, &mut timeline).await;
        node.stop();
        drilled
    }
    .await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
        eprintln!("{}", pair.diagnostics());
        eprintln!("failover timeline: {}", timeline.render());
        pair.writer.route_to(pair.writable_port());
    }
    let cleanup = fixture.cleanup().await;
    drop(pair);
    result.and(cleanup)
}

async fn cut_after_found(
    f: &mut Fixture,
    pair: &mut Pair,
    node: &mut NodeGate,
    timeline: &mut Timeline,
) -> Result<()> {
    let primary = PgPool::connect(&pair.url(pair.primary.port)).await?;
    let standby = PgPool::connect(&pair.url(pair.standby.port)).await?;
    let schema = f.schema.clone();
    let names: String = sqlx::query_scalar("SHOW synchronous_standby_names")
        .fetch_one(&primary)
        .await?;
    ensure!(
        names.is_empty(),
        "D3 requires an asynchronous standby, found {names:?}"
    );
    start_servers(f, node.port, &wait_settings(DRILL_BOUND_MS)).await?;
    node.hold()?;
    f.start_miner(0)?;
    f.start_miner(1)?;

    // The found block reaches the node: cut replication at once, inside the
    // link's delay, as a primary lost right after its block was offered.
    let block = node.first_arrival(Duration::from_secs(60)).await?;
    pair.replication.fence();
    timeline.mark("held and replication cut");
    let row_state =
        format!("SELECT state FROM {schema}.qbit_block_candidate_outbox WHERE block_hash=$1");
    let state_on = |pool: &PgPool| {
        let (pool, query, block) = (pool.clone(), row_state.clone(), block.clone());
        async move {
            Ok::<_, anyhow::Error>(
                sqlx::query_scalar::<_, String>(&query)
                    .bind(&block)
                    .fetch_optional(&pool)
                    .await?,
            )
        }
    };
    ensure!(
        state_on(&primary).await?.as_deref() == Some("offer_reserved"),
        "the held submitblock for {block} was not preceded by its durable reservation"
    );
    until("standby replay settled after the cut", 15, || async {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT pg_last_wal_replay_lsn()=pg_last_wal_receive_lsn()",
        )
        .fetch_one(&standby)
        .await?)
    })
    .await?;
    let on_standby = state_on(&standby).await?;
    ensure!(
        on_standby.as_deref() == Some("offer_reserved"),
        "the found block {block} was offered before the standby held its reservation (standby row: {on_standby:?}); a failover now loses it"
    );
    let waits = logged_waits(f)?;
    let confirmed = waits
        .iter()
        .find(|(hash, _, _)| *hash == block)
        .with_context(|| format!("no standby wait was logged for {block}"))?;
    ensure!(
        confirmed.1 == "confirmed" && confirmed.2 >= DRILL_LAG.as_micros() as u64 / 2,
        "the offer of {block} did not wait for the lagging standby: {confirmed:?}"
    );

    // Lose the old primary: fence every writer, stop it, promote the standby
    // and move the endpoint, then let the held attempt's answer through.
    pair.writer.fence();
    timeline.mark("writers fenced");
    for miner in &mut f.miners {
        miner.stop();
    }
    let old_candidates = candidates(&primary, &schema).await?;
    primary.close().await;
    pair.primary.stop()?;
    timeline.mark("old primary stopped");
    let promoted_at = Instant::now();
    let promoted: bool = sqlx::query_scalar("SELECT pg_promote(true,60)")
        .fetch_one(&standby)
        .await?;
    ensure!(promoted, "pg_promote did not complete within 60 seconds");
    pair.promoted = true;
    timeline.mark("promoted");
    ensure!(
        state_on(&standby).await?.as_deref() == Some("offer_reserved"),
        "the promoted primary must hold the found block's reservation"
    );
    let new_candidates = candidates(&standby, &schema).await?;
    let lost_candidates: BTreeSet<_> = old_candidates
        .difference(&new_candidates)
        .cloned()
        .collect();
    pair.writer.route_to(pair.standby.port);
    timeline.mark("endpoint moved");
    node.release();
    node.forwarded(&block, Duration::from_secs(15)).await?;
    timeline.mark("node answered");

    until(
        "the found block's landing on the new primary",
        180,
        || async { Ok(state_on(&f.pool).await?.as_deref() == Some("submitted")) },
    )
    .await?;
    let landed_after = promoted_at.elapsed();
    timeline.mark("landed");
    let audits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM qbit_pool_audit_bundles WHERE block_hash=$1")
            .bind(&block)
            .fetch_one(&f.pool)
            .await?;
    ensure!(audits == 1, "the landed block has {audits} audit bundles");
    until(
        "the landed block confirmed on the new primary",
        30,
        || async {
            Ok(sqlx::query_scalar::<_, String>(
                "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1",
            )
            .bind(&block)
            .fetch_optional(&f.pool)
            .await?
            .as_deref()
                == Some("confirmed"))
        },
    )
    .await?;
    f.quiesce().await?;
    f.integrity().await?;
    let arrivals = node.arrivals();
    let offers = arrivals.iter().filter(|hash| **hash == block).count();
    ensure!(
        offers == 1,
        "the found block was offered {offers} times: {arrivals:?}"
    );
    let offered: BTreeSet<_> = arrivals.iter().collect();
    ensure!(
        offered.len() == arrivals.len(),
        "a block was offered to the node more than once: {arrivals:?}"
    );
    let unconfirmed = waits
        .iter()
        .filter(|(_, outcome, _)| outcome != "confirmed")
        .count();
    eprintln!(
        "b529 drill: block {block} offered after a {} us standby wait over a {} ms lagging link, \
         replication cut at its arrival, landed once {:.1} s after promotion; {} submitblock calls; \
         {} candidate rows lost with the gap ({} of them offered unconfirmed); logged waits: {waits:?}; \
         timeline: {}",
        confirmed.2,
        DRILL_LAG.as_millis(),
        landed_after.as_secs_f64(),
        arrivals.len(),
        lost_candidates.len(),
        unconfirmed,
        timeline.render(),
    );
    standby.close().await;
    Ok(())
}
