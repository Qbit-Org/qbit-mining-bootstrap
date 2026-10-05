//! #521 scenario 5: dense-cadence landings and a soak, with a real node.
//!
//! Two CPU miners, one on each server, mine the regtest node's easy target at
//! a throttled rate, so the pool's own blocks land seconds apart and every
//! landing bumps the payout revision the other server's work was built on:
//! the #478 path, for as long as `PRISM_DENSE_SOAK_SECONDS` (default 600).
//! Nightly only (`--ignored`); the nightly load-harness workflow runs it.
//!
//! It asserts that:
//! - every block the pool found on live work (an accepted share outside
//!   stale grace whose hash meets the network target) is a candidate, unless
//!   the block that won its height was captured before that share was
//!   accepted: a race already lost when it was found;
//! - every candidate settles: `submitted` on the node's active chain,
//!   `orphaned` off it, or abandoned before its offer only because its parent
//!   was superseded by a landed pool block at its height; and every block on
//!   the node's chain since the soak began is a confirmed pool block;
//! - each server's accepted-block-to-revision-work time (#458) stays within
//!   `PRISM_DENSE_SOAK_REBUILD_P99_SECONDS` (default 5) at p99, with no
//!   degraded landing;
//! - each server's RSS is flat: the median of the last fifth of the soak is
//!   within a quarter (plus 32 MiB) of the median of its second fifth.
//!
//! After the miners stop, seven blocks that are not the pool's bury the last
//! landings past the orphan depth, so a race lost at the very end is proven
//! before the candidates are read.
use super::*;
use num_bigint::BigUint;
use qbit_prism_server::codec::{parse_u32_hex, target_from_compact};
use std::collections::{BTreeMap, HashMap};

const DEFAULT_SOAK_SECONDS: u64 = 600;
const DEFAULT_REBUILD_P99_SECONDS: f64 = 5.0;
const SAMPLE_SECONDS: u64 = 5;
/// Blocks mined after the soak, past `PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS`
/// (6), so a lost race at the end is proven.
const ORPHAN_BURIAL_BLOCKS: u64 = 7;
/// Regtest's constant target.
const REGTEST_BITS: &str = "207fffff";

pub(super) fn setting<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse()
            .map_err(|error| anyhow::anyhow!("{name}={value:?}: {error}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("{name}: {error}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "nightly #521 scenario 5: a dense-cadence soak of PRISM_DENSE_SOAK_SECONDS (default 600)"]
async fn dense_cadence_soak_lands_every_block_rebuilds_in_budget_and_holds_rss_flat() -> Result<()>
{
    // Selected explicitly, so a missing input fails rather than skipping.
    gate::required_inputs(
        gate::site!(),
        &[gate::Input::QbitdBin, gate::Input::DatabaseUrl],
    )?;
    let seconds: u64 = setting("PRISM_DENSE_SOAK_SECONDS", DEFAULT_SOAK_SECONDS)?;
    ensure!(
        seconds >= 60,
        "PRISM_DENSE_SOAK_SECONDS must be at least 60, not {seconds}"
    );
    let budget: f64 = setting(
        "PRISM_DENSE_SOAK_REBUILD_P99_SECONDS",
        DEFAULT_REBUILD_P99_SECONDS,
    )?;
    ensure!(
        budget.is_finite() && budget > 0.0,
        "PRISM_DENSE_SOAK_REBUILD_P99_SECONDS must be finite and positive"
    );
    let Some(mut fixture) = Fixture::open(false).await? else {
        bail!("the live fixture's inputs were required above");
    };
    let result = soak(&mut fixture, seconds, budget).await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}

/// A throttled miner on server `index` for `seconds`: one hash a second and a
/// pause after each accepted share, so each miner finds a block every few
/// seconds on regtest's easy target and the two together land blocks seconds
/// apart.
fn start_soak_miner(fixture: &mut Fixture, index: usize, seconds: u64) -> Result<()> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qbit-prism-miner"));
    command.args([
        "--address",
        &format!("127.0.0.1:{}", fixture.stratum[index]),
        "--username",
        &format!("{}.soak-{index}", fixture.address),
        "--threads",
        "1",
        "--hashes-per-second",
        "1",
        "--duration-seconds",
        &seconds.to_string(),
        "--pause-after-share-ms",
        "4000",
    ]);
    fixture.miners.push(Process::spawn(
        &mut command,
        fixture
            .directory
            .path()
            .join(format!("soak-miner-{index}.json")),
    )?);
    Ok(())
}

/// Resident set size of a process, in KiB, from `/proc`.
fn rss_kib(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse()
            .ok()
    })
}

fn median(values: &mut [u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(values[values.len() / 2])
}

/// The block solutions a miner found and what the pool answered.
struct MinerBlocks {
    found: usize,
    accepted: usize,
    /// The pool's refusal messages.
    refused: Vec<String>,
    /// The parent each block was found on, by block hash (display order).
    parents: HashMap<String, String>,
}

/// [`MinerBlocks`] from a miner's JSON-lines log.
fn miner_blocks(log: &std::path::Path) -> Result<MinerBlocks> {
    let text = std::fs::read_to_string(log)?;
    let mut block_requests = std::collections::HashSet::new();
    let mut accepted = 0;
    let mut refused = Vec::new();
    let mut parents = HashMap::new();
    for line in text.lines() {
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        match event["event"].as_str() {
            Some("submit") if event["block_target_met"] == true => {
                if let Some(id) = event["request_id"].as_u64() {
                    block_requests.insert(id);
                }
                if let (Some(hash), Some(parent)) = (
                    event["block_hash"].as_str(),
                    event["previousblockhash"].as_str(),
                ) {
                    parents.insert(hash.to_owned(), parent.to_owned());
                }
            }
            Some("share")
                if event["response"]["id"]
                    .as_u64()
                    .is_some_and(|id| block_requests.contains(&id)) =>
            {
                if event["accepted"] == true {
                    accepted += 1;
                } else {
                    refused.push(event["response"]["error"].to_string());
                }
            }
            _ => {}
        }
    }
    Ok(MinerBlocks {
        found: block_requests.len(),
        accepted,
        refused,
        parents,
    })
}

/// `(le, cumulative count)` buckets of one labelled histogram series, summed
/// over every value of its other labels.
fn histogram(body: &str, name: &str) -> BTreeMap<String, f64> {
    let mut buckets = BTreeMap::new();
    let prefix = format!("{name}_bucket{{");
    for line in body.lines().filter(|line| line.starts_with(&prefix)) {
        let Some((labels, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Some(le) = labels
            .split(['{', ',', '}'])
            .find_map(|label| label.strip_prefix("le=\""))
            .map(|le| le.trim_end_matches('"').to_owned())
        else {
            continue;
        };
        *buckets.entry(le).or_insert(0.0) += value.parse::<f64>().unwrap_or(0.0);
    }
    buckets
}

/// The smallest bucket bound that holds at least `quantile` of the samples:
/// a p99 bound, never an interpolation. `None` without samples.
fn quantile_bound(buckets: &BTreeMap<String, f64>, quantile: f64) -> Option<f64> {
    let total = *buckets.get("+Inf")?;
    if total == 0.0 {
        return None;
    }
    let mut bounds: Vec<(f64, f64)> = buckets
        .iter()
        .map(|(le, count)| {
            (
                if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().unwrap_or(f64::INFINITY)
                },
                *count,
            )
        })
        .collect();
    bounds.sort_by(|a, b| a.0.total_cmp(&b.0));
    bounds
        .into_iter()
        .find(|(_, count)| *count >= quantile * total)
        .map(|(bound, _)| bound)
}

fn labelled_count(body: &str, name: &str, label: &str) -> f64 {
    let prefix = format!("{name}_count{{");
    body.lines()
        .filter(|line| line.starts_with(&prefix) && line.contains(label))
        .filter_map(|line| line.rsplit_once(' ')?.1.parse::<f64>().ok())
        .sum()
}

async fn metrics_body(fixture: &Fixture, index: usize) -> Result<String> {
    Ok(fixture
        .client
        .get(format!("http://127.0.0.1:{}/metrics", fixture.api[index]))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

pub(super) async fn soak(fixture: &mut Fixture, seconds: u64, budget: f64) -> Result<()> {
    let start_height = fixture
        .rpc("getblockcount", json!([]))
        .await?
        .as_u64()
        .context("height")?;
    let pids: Vec<u32> = fixture
        .servers
        .iter()
        .map(|server| server.child.lock().id())
        .collect();
    for index in 0..2 {
        start_soak_miner(fixture, index, seconds + 30)?;
    }
    let started = std::time::Instant::now();
    let mut rss: Vec<Vec<(u64, u64)>> = vec![Vec::new(); pids.len()];
    let mut heights = Vec::new();
    while started.elapsed() < Duration::from_secs(seconds) {
        tokio::time::sleep(Duration::from_secs(SAMPLE_SECONDS)).await;
        let at = started.elapsed().as_secs();
        for (index, pid) in pids.iter().enumerate() {
            if let Some(kib) = rss_kib(*pid) {
                rss[index].push((at, kib));
            }
        }
        if let Ok(height) = fixture.rpc("getblockcount", json!([])).await {
            heights.push((at, height.as_u64().unwrap_or_default()));
        }
        for (index, server) in fixture.servers.iter().enumerate() {
            ensure!(
                server.child.try_wait()?.is_none(),
                "server {index} exited during the soak"
            );
        }
    }
    fixture.quiesce().await?;
    let end_height = fixture
        .rpc("getblockcount", json!([]))
        .await?
        .as_u64()
        .context("height")?;
    let landed = end_height - start_height;
    ensure!(
        landed as f64 >= seconds as f64 / 30.0,
        "only {landed} blocks landed in a {seconds} s soak: the cadence was not dense"
    );
    let cadence = seconds as f64 / landed as f64;
    // A lost same-height race is proven orphaned only once the winning branch
    // is buried past the orphan depth, and the chain stops with the miners:
    // bury the last landings under blocks that are not the pool's, then let
    // every candidate settle.
    let burier = fixture.rpc("getnewaddress", json!(["", "p2mr"])).await?;
    fixture
        .rpc("generatetoaddress", json!([ORPHAN_BURIAL_BLOCKS, burier]))
        .await?;
    until("every candidate settled", 120, || async {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM qbit_block_candidate_outbox \
             WHERE state NOT IN ('submitted','orphaned','abandoned')",
        )
        .fetch_one(&fixture.pool)
        .await?
            == 0)
    })
    .await?;

    // Every found block the pool accepted is a candidate.
    let mut found = 0;
    let mut accepted = 0;
    let mut refused: BTreeMap<String, usize> = BTreeMap::new();
    let mut parents = HashMap::new();
    for miner in &fixture.miners {
        let blocks = miner_blocks(&miner.log)?;
        found += blocks.found;
        accepted += blocks.accepted;
        for reason in blocks.refused {
            *refused.entry(reason).or_default() += 1;
        }
        parents.extend(blocks.parents);
    }
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT block_hash,state,offer_outcome FROM qbit_block_candidate_outbox ORDER BY created_at",
    )
    .fetch_all(&fixture.pool)
    .await?;
    // Every block the pool found on live work is a candidate: each accepted
    // share not credited under stale grace whose hash meets the network
    // target. A solution on a tip already replaced is accepted under stale
    // grace and is no block to land; one on superseded payout work is
    // captured although its share is refused (#478), so candidates may
    // outnumber these, but never the solutions the miners found.
    let network_target = target_from_compact(parse_u32_hex(REGTEST_BITS)?)?;
    let live: Vec<String> = sqlx::query_scalar::<_, String>(
        "SELECT share_id FROM qbit_share_ledger WHERE accepted AND credit_policy IS NULL \
         AND writer_id LIKE 'live-%'",
    )
    .fetch_all(&fixture.pool)
    .await?
    .into_iter()
    .filter_map(|share_id| share_id.rsplit_once(':').map(|(_, hash)| hash.to_owned()))
    .filter(|hash| {
        BigUint::parse_bytes(hash.as_bytes(), 16).is_some_and(|value| value <= network_target)
    })
    .collect();
    let candidates: std::collections::HashSet<&str> =
        rows.iter().map(|(hash, _, _)| hash.as_str()).collect();
    let missing: Vec<&String> = live
        .iter()
        .filter(|hash| !candidates.contains(hash.as_str()))
        .collect();
    // A block not captured is a race already lost only when the block that
    // won its height was captured before this share was accepted: the other
    // server's landing had replaced the parent, and this server had not yet
    // observed it. Any other uncaptured block is a lost block.
    let mut lost_races = 0usize;
    let mut uncaptured = Vec::new();
    for hash in &missing {
        let row: Option<(i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT template_height::bigint, accepted_at FROM qbit_share_ledger \
             WHERE share_id LIKE '%:' || $1",
        )
        .bind(hash.as_str())
        .fetch_optional(&fixture.pool)
        .await?;
        let Some((height, accepted_at)) = row else {
            uncaptured.push(format!("{hash}: no share row"));
            continue;
        };
        let winner: String =
            serde_json::from_value(fixture.rpc("getblockhash", json!([height + 1])).await?)?;
        let captured: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT created_at FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(&winner)
        .fetch_optional(&fixture.pool)
        .await?;
        match captured {
            Some(at) if winner != **hash && at <= accepted_at => lost_races += 1,
            _ => uncaptured.push(format!(
                "{hash} at height {}: accepted {accepted_at}, the height's winner {winner} \
                 captured {captured:?}",
                height + 1
            )),
        }
    }
    ensure!(
        uncaptured.is_empty(),
        "{} block(s) found on live work were never captured:\n{}",
        uncaptured.len(),
        uncaptured.join("\n")
    );
    ensure!(
        rows.len() <= found,
        "{} candidates but the miners found only {found} block solutions",
        rows.len()
    );
    let mut states: HashMap<&str, usize> = HashMap::new();
    for (_, state, _) in &rows {
        *states.entry(state.as_str()).or_default() += 1;
    }
    // A candidate ends submitted (landed), orphaned (proven lost after its
    // offer), or abandoned before its offer because its parent was no
    // longer the tip: a same-height race the other server's block already
    // won, which must then be what the chain holds at that height. Abandoned
    // for any other reason -- a superseded payout revision with capture off
    // (#478) -- is a found block the pool threw away.
    let abandoned: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT block_hash,last_error FROM qbit_block_candidate_outbox WHERE state='abandoned'",
    )
    .fetch_all(&fixture.pool)
    .await?;
    for (hash, reason) in &abandoned {
        ensure!(
            reason.as_deref() == Some("parent superseded"),
            "candidate {hash} was abandoned for {reason:?}, not a superseded parent"
        );
        // Its height from the parent its miner built it on, not from its
        // share: a block captured on superseded payout work (#478) has no
        // credited share, its share deferred until the block confirms (never,
        // once abandoned) or, from block-only work, never credited.
        let parent = parents
            .get(hash)
            .with_context(|| format!("abandoned candidate {hash} is no block a miner found"))?;
        let height = fixture.rpc("getblockheader", json!([parent])).await?["height"]
            .as_i64()
            .with_context(|| {
                format!("abandoned candidate {hash}: parent {parent} has no height")
            })?
            + 1;
        let winner: String =
            serde_json::from_value(fixture.rpc("getblockhash", json!([height])).await?)?;
        ensure!(
            winner != *hash
                && rows
                    .iter()
                    .any(|(row, state, _)| *row == winner && state == "submitted"),
            "abandoned candidate {hash}: height {height} is held by {winner}, not a landed pool block"
        );
    }
    ensure!(
        rows.iter().all(|(_, state, _)| {
            state == "submitted" || state == "orphaned" || state == "abandoned"
        }),
        "a candidate did not settle: {states:?}"
    );
    // Every pool block on the active chain is credited, and every block on
    // it since the soak began is the pool's.
    let chain: HashMap<String, String> = sqlx::query_as::<_, (String, String)>(
        "SELECT block_hash,chain_state FROM qbit_pool_blocks",
    )
    .fetch_all(&fixture.pool)
    .await?
    .into_iter()
    .collect();
    let mut active = std::collections::HashSet::new();
    for height in start_height + 1..=end_height {
        let hash: String =
            serde_json::from_value(fixture.rpc("getblockhash", json!([height])).await?)?;
        ensure!(
            chain.get(&hash).map(String::as_str) == Some("confirmed"),
            "block {hash} at height {height} is on the node's chain but not a confirmed pool block"
        );
        active.insert(hash);
    }
    for (hash, state, _) in &rows {
        match state.as_str() {
            "submitted" => ensure!(
                active.contains(hash),
                "submitted candidate {hash} is not on the node's active chain"
            ),
            // A lost same-height race: the pool's own sibling won the height.
            _ => ensure!(
                !active.contains(hash),
                "orphaned candidate {hash} is on the node's active chain"
            ),
        }
    }

    // Rebuilds within budget, on each server.
    let family = "qbit_prism_accepted_block_to_revision_work_seconds";
    let mut rebuild_p99 = Vec::new();
    for index in 0..fixture.servers.len() {
        let body = metrics_body(fixture, index).await?;
        let buckets = histogram(&body, family);
        let bound = quantile_bound(&buckets, 0.99)
            .with_context(|| format!("server {index} recorded no {family} sample"))?;
        let samples = buckets.get("+Inf").copied().unwrap_or(0.0);
        ensure!(
            bound <= budget,
            "server {index}: {family} p99 is above {bound} s over {samples} landings, over the \
             {budget} s budget"
        );
        let degraded = labelled_count(&body, family, "result=\"degraded\"");
        ensure!(
            degraded == 0.0,
            "server {index}: {degraded} degraded landing(s)"
        );
        rebuild_p99.push((bound, samples));
    }

    // RSS flat, per server.
    let mut rss_summary = Vec::new();
    for (index, series) in rss.iter().enumerate() {
        let fifth = seconds / 5;
        let window = |from: u64, to: u64| -> Vec<u64> {
            series
                .iter()
                .filter(|(at, _)| *at >= from && *at < to)
                .map(|(_, kib)| *kib)
                .collect()
        };
        let early = median(&mut window(fifth, 2 * fifth))
            .with_context(|| format!("server {index}: no RSS sample in the second fifth"))?;
        let late = median(&mut window(4 * fifth, seconds + 1))
            .with_context(|| format!("server {index}: no RSS sample in the last fifth"))?;
        let ceiling = early + early / 4 + 32 * 1024;
        ensure!(
            late <= ceiling,
            "server {index}: RSS grew from a median {early} KiB to {late} KiB, over {ceiling} KiB"
        );
        rss_summary.push((early, late));
    }
    eprintln!(
        "live dense soak: {seconds} s, {landed} blocks landed (one every {cadence:.1} s), {found} \
         block solutions found, {accepted} accepted ({} on live work, {lost_races} of them races \
         already lost when found), refusals {refused:?}, candidates {states:?}; \
         revision work p99 \
         bound (s, landings) per server {rebuild_p99:?}; RSS median KiB (second fifth, last \
         fifth) per server {rss_summary:?}; heights {heights:?}",
        live.len()
    );
    Ok(())
}
