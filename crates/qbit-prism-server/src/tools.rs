use crate::{
    config::{self, Config},
    coordinator::{Coordinator, RecoveryStop},
    ledger::{
        audit_completeness, live_instances, unavailable_live_instances, AuditCompleteness,
        LiveInstancesReport, RecoveryClaim, RecoveryReader, RecoveryRow,
    },
    rpc::{Rpc, RpcReplyError},
};
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(
    version,
    about = "Native multi-instance PRISM mining and operator tools"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Serve Stratum, audit and dashboard APIs, and settlement workers.
    Run,
    /// Serve the public read API using a separate database pool or replica.
    PublicApi,
    /// Validate local configuration and key pairing without starting listeners.
    CheckConfig,
    /// Validate public reader database options without connecting or listening.
    CheckPublicDatabaseConfig,
    /// Probe HTTP or Stratum readiness without signing keys or database access.
    Healthcheck {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        public_api: bool,
    },
    /// Check node identity, database integrity, API readiness and cluster settings.
    SelfCheck,
    /// Change pool fees or CTV fee rates after stopping every frontend.
    PolicyTransition {
        /// Env-file overrides for the target policy; the process env is the current policy.
        #[arg(long)]
        to: PathBuf,
    },
    /// Inspect a cluster halt or reconcile and record an operator recovery.
    FatalState {
        #[command(subcommand)]
        command: FatalStateCommand,
    },
    /// Inspect unfinished block candidates, abandon a pending one, or recover accepted ones.
    Candidates {
        #[command(subcommand)]
        command: CandidatesCommand,
    },
    /// Validate compact target bits and print Prism's exact scaled difficulty.
    HeaderDifficulty {
        #[arg(long)]
        bits: String,
    },
    /// Apply the additive PostgreSQL migration after stopping Python writers.
    Migrate,
    /// Import legacy filesystem audit bodies into shared PostgreSQL storage.
    ImportAudits {
        /// Audit root, defaulting to PRISM_AUDIT_DIR when nonempty.
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Reconstruct missing CTV artifacts from verified stored audit bundles.
    BackfillCtv,
    /// Process one batch of durable, mature CTV fanout claims.
    BroadcastCtv,
    /// Validate a complete Stratum-to-PostgreSQL capacity qualification artifact.
    CapacityEvidence(crate::capacity::Args),
    /// Measure the actual native payout/audit builder on a synthetic share window.
    Benchmark {
        #[arg(long, default_value_t = 1000)]
        shares: usize,
        #[arg(long, default_value_t = 10)]
        miners: usize,
        #[arg(long, default_value_t = 10)]
        iterations: usize,
        #[arg(long)]
        output_json: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum FatalStateCommand {
    /// Print the stored halt; exits nonzero when the cluster is halted.
    Show,
    /// Reconcile a stopped/drained cluster and durably record why it was cleared.
    Clear {
        #[arg(long)]
        reason: String,
    },
}

#[derive(Subcommand)]
enum CandidatesCommand {
    /// Print unfinished candidates up to --limit, oldest due first; warn if truncated.
    List {
        /// Print the versioned JSON document instead of the operator table.
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(i64).range(1..=10_000))]
        limit: i64,
    },
    /// Abandon one pending, unclaimed candidate the node was never offered.
    Abandon {
        #[arg(long)]
        block_hash: String,
        #[arg(long)]
        reason: String,
    },
    /// Land already-accepted blocks for an explicit allowlist of candidates, never offering one.
    Recover {
        /// A candidate to recover; repeat for up to 32. A plan only, unless --apply is given.
        #[arg(long = "block-hash", value_name = "HASH", required = true, action = clap::ArgAction::Append)]
        block_hash: Vec<String>,
        /// Claim, verify against the node and land every listed candidate, in height order.
        #[arg(long)]
        apply: bool,
        /// One deadline, in seconds, for the whole operation: the plan, every node call and each landing.
        #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout_seconds: u64,
    },
}

/// The bound on one recovery allowlist: #259's, carried over. There is no
/// "recover everything".
const MAX_RECOVERY_BLOCKS: usize = 32;

/// The format the outbox row itself uses: `candidate_sha256 ~ '^[0-9a-f]{64}$'`.
fn require_block_hash(hash: &str) -> Result<()> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "--block-hash must be exactly 64 lowercase hexadecimal characters"
    );
    Ok(())
}

/// Parse both configurations on the single-threaded entry path, before Tokio starts.
pub fn prepare() -> Result<impl std::future::Future<Output = Result<()>>> {
    let command = Cli::parse().command.unwrap_or(Command::Run);
    let transition = match &command {
        Command::PolicyTransition { to } => Some(config::transition_configs(to)?),
        _ => None,
    };
    Ok(run(command, transition))
}

async fn run(command: Command, transition: Option<(Config, Config)>) -> Result<()> {
    match command {
        Command::Run => crate::server::run(Config::from_env()?).await,
        Command::PublicApi => {
            let (shutdown, receiver) = tokio::sync::watch::channel(false);
            let service = crate::api::public_service::run_from_env(receiver);
            tokio::pin!(service);
            tokio::select! {
                result = &mut service => result,
                result = crate::server::signal() => {
                    result?;
                    shutdown.send_replace(true);
                    tokio::time::timeout(Duration::from_secs(30), service)
                        .await.context("public API shutdown timed out")?
                }
            }
        }
        Command::CheckConfig => {
            config::check_environment()?;
            let config = Config::from_env()?;
            crate::rollups::settings_from_env()?;
            crate::stratum::StratumConfig::from_env()?.highdiff_config()?;
            crate::api::ApiConfig::from_env()?;
            crate::api::public_service::ServiceConfig::from_env()?;
            println!(
                "PRISM configuration valid; {} runtime workers",
                config.runtime_workers
            );
            Ok(())
        }
        Command::CheckPublicDatabaseConfig => {
            config::public_database_options_from_env()?;
            println!(
                "PRISM public database configuration valid; authentication is checked by readiness"
            );
            Ok(())
        }
        Command::Healthcheck { url, public_api } => healthcheck(url, public_api).await,
        Command::SelfCheck => self_check().await,
        Command::PolicyTransition { .. } => {
            let (current, target) =
                transition.context("policy transition configuration missing")?;
            let ledger =
                crate::ledger::Ledger::connect_operator(&current.database_url, false).await?;
            let result = ledger.transition_policy(&current, &target).await;
            ledger.pool.close().await;
            println!("{}", serde_json::to_string_pretty(&result?)?);
            Ok(())
        }
        Command::FatalState { command } => fatal_state(command).await,
        Command::Candidates { command } => candidates(command).await,
        Command::HeaderDifficulty { bits } => {
            let compact = crate::codec::parse_u32_hex(&bits)?;
            let target = crate::codec::target_from_compact(compact)?;
            println!("{}", crate::codec::scaled_target_difficulty(&target)?);
            Ok(())
        }
        Command::Migrate => {
            let config = config::DatabaseConfig::from_env()?;
            let ledger =
                crate::ledger::Ledger::connect_operator(&config.database_url, true).await?;
            let source = ledger
                .migration_source()
                .await?
                .map(|source| {
                    format!(
                        "{} (2.x.x release {})",
                        source.source_state,
                        source.source_release.as_deref().unwrap_or("none")
                    )
                })
                .unwrap_or_else(|| "unrecorded".to_owned());
            println!(
                "PRISM PostgreSQL schema migrations {} ready; database source: {source}",
                crate::ledger::schema_version_list(crate::ledger::REQUIRED_SCHEMA_VERSIONS)
            );
            ledger.pool.close().await;
            Ok(())
        }
        Command::ImportAudits { root } => {
            let root = audit_root(root);
            let config = config::DatabaseConfig::from_env()?;
            let ledger_public_key = config::DatabaseConfig::ledger_public_key()?;
            let ledger = crate::ledger::Ledger::connect_tool(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                false,
                None,
            )
            .await?;
            let count = ledger
                .import_legacy_audits(root.as_deref(), &ledger_public_key)
                .await;
            // Preserve the remaining-work counts even when import stopped on
            // an unverifiable or missing historical artifact.
            let completeness = audit_completeness(&ledger.pool).await;
            ledger.pool.close().await;
            let completeness = completeness?;
            println!(
                "Audit completeness: {}",
                serde_json::to_string(&completeness)?
            );
            let count = count?;
            println!("Imported {count} audit bodies");
            completeness.require_complete()
        }
        Command::BackfillCtv => {
            let config = config::DatabaseConfig::from_env()?;
            let ledger_public_key = config::DatabaseConfig::ledger_public_key()?;
            let ledger = crate::ledger::Ledger::connect_tool(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                false,
                None,
            )
            .await?;
            let count = ledger.backfill_ctv(&ledger_public_key).await?;
            println!("Backfilled {count} CTV manifest sets");
            Ok(())
        }
        Command::BroadcastCtv => {
            let coordinator = Coordinator::new_tool(
                Config::from_env()?,
                std::sync::Arc::new(crate::metrics::Metrics::default()),
            )
            .await?;
            coordinator.refresh_once().await?;
            let count = crate::broadcaster::run_once(&coordinator).await?;
            println!("Processed {count} CTV fanouts");
            Ok(())
        }
        Command::CapacityEvidence(args) => crate::capacity::run(args),
        Command::Benchmark {
            shares,
            miners,
            iterations,
            output_json,
        } => {
            let result = tokio::task::spawn_blocking(move || benchmark(shares, miners, iterations))
                .await??;
            let text = serde_json::to_string_pretty(&result)?;
            if let Some(path) = output_json {
                tokio::fs::write(path, &text).await?;
            }
            println!("{text}");
            Ok(())
        }
    }
}

fn audit_root(root: Option<PathBuf>) -> Option<PathBuf> {
    root.or_else(|| config::optional("PRISM_AUDIT_DIR").map(PathBuf::from))
}

async fn fatal_state(command: FatalStateCommand) -> Result<()> {
    match command {
        FatalStateCommand::Show => {
            let url =
                config::optional("PRISM_DATABASE_URL").context("PRISM_DATABASE_URL is required")?;
            let state = crate::ledger::Ledger::inspect_fatal_state(&url).await?;
            println!("{}", serde_json::to_string_pretty(&state)?);
            ensure!(
                state["halted"] == false,
                "cluster halted: {}",
                state["fatal_error"].as_str().unwrap_or("unknown")
            );
            Ok(())
        }
        FatalStateCommand::Clear { reason } => {
            ensure!(
                !reason.trim().is_empty() && reason.len() <= 4096,
                "--reason must contain 1 to 4096 bytes of nonblank text"
            );
            let config = Config::from_env()?;
            let ledger =
                crate::ledger::Ledger::connect_operator(&config.database_url, false).await?;
            let result = ledger.clear_fatal_state(&config, &reason).await;
            ledger.pool.close().await;
            println!("{}", serde_json::to_string_pretty(&result?)?);
            Ok(())
        }
    }
}

/// The operator candidate commands (#268, #418). `list` and `abandon` build
/// no node client, read no signing key, load no `Config` and start no
/// listener, so for them "never calls `submitblock`" is a property of the
/// code's shape rather than of its discipline. `list` needs only the
/// database URL, the way `fatal-state show` reads it; `abandon` writes an
/// ordinary ledger row, so it takes the one-shot tool connection and the
/// database configuration behind it. `recover` is the one command that reads
/// the node: its plan needs the database URL and the node RPC settings, and
/// `--apply` the frontend's whole configuration, because it rebuilds and
/// signs the audit; it drives the coordinator's own landing and has no
/// offer path to reach.
async fn candidates(command: CandidatesCommand) -> Result<()> {
    match command {
        CandidatesCommand::List { json, limit } => {
            let url =
                config::optional("PRISM_DATABASE_URL").context("PRISM_DATABASE_URL is required")?;
            let (rows, truncated) = crate::ledger::Ledger::list_candidates(&url, limit).await?;
            if json {
                let document = json!({
                    "schema": "qbit.prism.candidates.list.v1",
                    "candidates": rows,
                    "limit": limit,
                    "truncated": truncated,
                });
                println!("{}", serde_json::to_string_pretty(&document)?);
            } else if rows.is_empty() {
                // Nothing pending is this command's success case: it is what
                // the cutover runbook waits for, so it exits zero.
                println!("no unfinished candidates");
            } else {
                print!("{}", candidate_table(&rows));
                if truncated {
                    eprintln!("candidate inventory truncated at {limit} rows; more unfinished candidates exist (parked rows sort last). Increase --limit up to 10000; larger inventories require a read-only database query.");
                }
            }
            Ok(())
        }
        CandidatesCommand::Abandon { block_hash, reason } => {
            // Both inputs are checked before any connection is opened, in the
            // formats the row itself uses: `candidate_sha256 ~
            // '^[0-9a-f]{64}$'` for the hash, and `fatal-state clear`'s rule
            // for the reason, which lands in `last_error`.
            require_block_hash(&block_hash)?;
            ensure!(
                !reason.trim().is_empty() && reason.len() <= 4096,
                "--reason must contain 1 to 4096 bytes of nonblank text"
            );
            // A one-shot writer of ordinary ledger rows, not a recovery
            // command: `connect_tool` refuses a halted cluster at connect
            // exactly as a frontend would, and writes no heartbeat, so a
            // live frontend sharing this instance ID keeps its row.
            let config = config::DatabaseConfig::from_env()?;
            let ledger = crate::ledger::Ledger::connect_tool(
                &config.database_url,
                config.instance_id,
                config.database_connections,
                false,
                None,
            )
            .await?;
            let outcome = ledger.abandon_candidate(&block_hash, &reason).await;
            // Closed before the outcome is inspected, so a refusal releases
            // the pool exactly as a success does.
            ledger.pool.close().await;
            let (code, message) = abandon_report(&outcome?, &block_hash, &reason)?;
            if code == 0 {
                println!("{message}");
                return Ok(());
            }
            eprintln!("{message}");
            std::process::exit(code)
        }
        CandidatesCommand::Recover {
            block_hash,
            apply,
            timeout_seconds,
        } => recover(block_hash, apply, timeout_seconds).await,
    }
}

/// Print the message and exit with the status: a refusal, never a failure of
/// the command's machinery, which returns its error as every command does.
fn refuse(code: i32, message: String) -> ! {
    eprintln!("{message}");
    std::process::exit(code)
}

/// The operator recovery (#418): plan by default, land with `--apply`.
///
/// The allowlist is checked at the entry boundary, before any connection:
/// at most [`MAX_RECOVERY_BLOCKS`] distinct well-formed hashes. One deadline,
/// `--timeout-seconds`, is carried through everything after it: the plan's
/// reads and node calls, the coordinator's connection, and each candidate's
/// claim, rebuild, landing and finish. The plan runs on the read-only pool
/// and calls the node read-only; a refused plan applies nothing. `--apply`
/// connects as a one-shot tool, exactly as `self-check` and `broadcast-ctv`
/// do, and drives the coordinator's own landing for each planned block in
/// height order, stopping at the first that cannot be finished.
async fn recover(hashes: Vec<String>, apply: bool, timeout_seconds: u64) -> Result<()> {
    ensure!(
        hashes.len() <= MAX_RECOVERY_BLOCKS,
        "--block-hash may be given at most {MAX_RECOVERY_BLOCKS} times; recover takes an explicit allowlist, never everything"
    );
    let mut seen = HashSet::new();
    for hash in &hashes {
        require_block_hash(hash)?;
        ensure!(
            seen.insert(hash.as_str()),
            "--block-hash {hash} is listed more than once"
        );
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    let url = config::optional("PRISM_DATABASE_URL").context("PRISM_DATABASE_URL is required")?;
    let (rpc_url, rpc_user, rpc_password) = config::rpc_connection_from_env();
    let rpc = Rpc::new(
        rpc_url,
        rpc_user,
        rpc_password,
        config::seconds("PRISM_RPC_TIMEOUT_SECONDS", 15.0)?,
    )?;
    let planned = tokio::time::timeout_at(deadline, async {
        let reader = RecoveryReader::open(&url).await?;
        let plan = plan_recovery(&reader, &rpc, &hashes).await;
        // Released on the failure path too.
        reader.close().await;
        plan
    })
    .await;
    let plan = match planned {
        Ok(plan) => plan?,
        Err(_) => refuse(
            11,
            format!(
                "recovery deadline of {timeout_seconds} seconds exceeded (planning); nothing was claimed"
            ),
        ),
    };
    if !plan.blocks.is_empty() {
        print!("{}", recovery_plan_table(&plan.blocks));
    }
    if let Some((code, _)) = plan.problems.first() {
        for (_, message) in &plan.problems {
            eprintln!("{message}");
        }
        std::process::exit(*code)
    }
    let to_recover = plan.blocks.iter().filter(|block| !block.complete).count();
    let complete = plan.blocks.len() - to_recover;
    if !apply {
        if to_recover == 0 {
            println!("plan: nothing to recover; every listed block is already complete");
        } else {
            println!(
                "plan: {to_recover} to recover, {complete} already complete; rerun with --apply to land them"
            );
        }
        return Ok(());
    }
    // A one-shot tool, as `self-check` and `broadcast-ctv` are: every gate of
    // a frontend's startup (node genesis and chain, schema, halt guard,
    // cluster fingerprint), no heartbeat, nothing left behind on exit.
    let config = Config::from_env()?;
    let connected = tokio::time::timeout_at(
        deadline,
        Coordinator::new_tool(
            config,
            std::sync::Arc::new(crate::metrics::Metrics::default()),
        ),
    )
    .await;
    let coordinator = match connected {
        Ok(coordinator) => coordinator?,
        Err(_) => refuse(
            11,
            format!(
                "recovery deadline of {timeout_seconds} seconds exceeded (connecting); nothing was claimed"
            ),
        ),
    };
    let outcome = apply_recovery(&coordinator, &plan.blocks, deadline, timeout_seconds).await;
    // A landing the deadline cut short may hold its connection until the
    // server abandons it, so the close is bounded as well.
    let _ = tokio::time::timeout(Duration::from_secs(5), coordinator.ledger.pool.close()).await;
    match outcome {
        Ok((recovered, verified)) => {
            println!("recovered {recovered}, verified {verified} already complete");
            Ok(())
        }
        Err(Stop::Failure(error)) => Err(error),
        Err(Stop::Exit(code, message)) => refuse(code, message),
    }
}

/// One listed hash as the plan decided it. Height and parent are the node's:
/// the ordering and the parent rule read the chain, which is authoritative,
/// and compare it with what the row stores.
struct PlannedBlock {
    hash: String,
    height: u64,
    parent: String,
    state: String,
    claim: String,
    /// A `submitted` row whose accounting is proven, verified and skipped.
    complete: bool,
}

#[derive(Default)]
struct RecoveryPlan {
    /// Height ascending, parent before child; ties by hash.
    blocks: Vec<PlannedBlock>,
    /// Every refusal, in allowlist order. The first one's status is the exit
    /// status; all of them are printed, so one run names every problem.
    problems: Vec<(i32, String)>,
}

/// How an apply stopped: a refusal with its exit status, or a failure of the
/// machinery, returned as every command returns one.
enum Stop {
    Exit(i32, String),
    Failure(anyhow::Error),
}

impl From<anyhow::Error> for Stop {
    fn from(error: anyhow::Error) -> Self {
        Self::Failure(error)
    }
}

/// The node's view of one block: its header height and parent when the
/// active chain holds it, or why it does not. A transport or protocol
/// failure is not an answer and propagates.
async fn active_block(rpc: &Rpc, hash: &str) -> Result<Result<(u64, String), String>> {
    let header = match rpc.call("getblockheader", json!([hash])).await {
        Ok(header) => header,
        Err(error) => {
            // Only RPC_INVALID_ADDRESS_OR_KEY means this header is missing.
            // Warm-up and internal errors say nothing about chain membership.
            if let Some(reply) = error
                .downcast_ref::<RpcReplyError>()
                .filter(|reply| reply.error["code"].as_i64() == Some(-5))
            {
                return Ok(Err(format!(
                    "the node has no such block: {}",
                    reply.error["message"].as_str().unwrap_or("no reason given")
                )));
            }
            return Err(error);
        }
    };
    let height = header["height"]
        .as_u64()
        .context("qbit block header has no height")?;
    let parent = header["previousblockhash"]
        .as_str()
        .context("qbit block header has no previousblockhash")?
        .to_ascii_lowercase();
    // A header the node knows can be on a side chain; only the active
    // chain's hash at that height proves the block active.
    let active = match rpc.call("getblockhash", json!([height])).await {
        Ok(active) => active,
        Err(error) => {
            // A reorg can shorten the active chain below this known header.
            // Only RPC_INVALID_PARAMETER means the height is out of range.
            if error
                .downcast_ref::<RpcReplyError>()
                .is_some_and(|reply| reply.error["code"].as_i64() == Some(-8))
            {
                return Ok(Err(format!(
                    "height {height} is beyond the node's active chain tip"
                )));
            }
            return Err(error);
        }
    };
    if active.as_str() != Some(hash) {
        return Ok(Err(format!(
            "the node's active chain holds {} at height {height}",
            active.as_str().unwrap_or("an invalid hash")
        )));
    }
    Ok(Ok((height, parent)))
}

fn not_active_message(hash: &str, detail: &str) -> String {
    format!(
        "candidate {hash} is not on the active chain ({detail}); nothing to recover. recover never \
         offers a block: a block the node never accepted stays with the coordinator (or, while \
         pending, may be abandoned); a block a reorg removed stays in reconciliation"
    )
}

fn unsupported_storage_version_message(hash: &str, version: &Value) -> String {
    format!(
        "candidate {hash} has unsupported storage_version {version}; evidence preserved. Only \
         version 1 is supported; drain legacy rows with the pinned 2.x.x image, and use a \
         compatible release for newer formats"
    )
}

fn legacy_candidate_message(hash: &str) -> String {
    format!(
        "candidate {hash} holds a pre-migration 2.x.x document at storage_version 1; evidence \
         preserved. This release cannot replay it: drain it with the pinned 2.x.x image"
    )
}

fn orphaned_candidate_message(hash: &str) -> String {
    format!(
        "candidate {hash} is already orphaned; its candidate payload was released and its \
         accounting remains in the ledger. Leave chain changes to reconciliation"
    )
}

/// Decide the whole allowlist, fail-closed: every hash must have a row that
/// is either unfinished and provably on the active chain, or `submitted`
/// with proven accounting; an unfinished parent that is not listed refuses
/// the child, so a parent always lands first. Nothing here writes.
async fn plan_recovery(
    reader: &RecoveryReader,
    rpc: &Rpc,
    hashes: &[String],
) -> Result<RecoveryPlan> {
    let rows = reader.rows(hashes).await?;
    let rows: HashMap<&str, &RecoveryRow> = rows
        .iter()
        .map(|row| (row.block_hash.as_str(), row))
        .collect();
    let listed: HashSet<&str> = hashes.iter().map(String::as_str).collect();
    let mut plan = RecoveryPlan::default();
    let mut unlisted_parents: Vec<(usize, String, String)> = Vec::new();
    for hash in hashes {
        let Some(row) = rows.get(hash.as_str()) else {
            plan.problems
                .push((2, format!("no candidate row for {hash}")));
            continue;
        };
        let claim = claim_text(
            row.claim_instance_id.as_deref(),
            row.claim_live,
            &row.claim_expires_at
                .map_or_else(|| "-".to_owned(), |at| at.to_rfc3339()),
        );
        let planned = |height, parent, complete| PlannedBlock {
            hash: hash.clone(),
            height,
            parent,
            state: row.state.clone(),
            claim: claim.clone(),
            complete,
        };
        match row.state.as_str() {
            "orphaned" => plan.problems.push((4, orphaned_candidate_message(hash))),
            "abandoned" => plan.problems.push((
                4,
                format!("candidate {hash} is already abandoned; its evidence was released and it cannot be recovered"),
            )),
            "submitted" => {
                let mut missing = Vec::new();
                match row.landed_chain_state.as_deref() {
                    Some("confirmed") => {}
                    Some(state) => missing.push(format!("its pool block is {state}, not confirmed")),
                    None => missing.push("no qbit_pool_blocks row".to_owned()),
                }
                if !row.has_audit {
                    missing.push("no qbit_pool_audit_bundles row".to_owned());
                }
                let landed_height = row.landed_height.and_then(|height| u64::try_from(height).ok());
                if landed_height.is_none() && missing.is_empty() {
                    missing.push("its pool block has no valid height".to_owned());
                }
                if !missing.is_empty() {
                    plan.problems.push((
                        4,
                        format!(
                            "candidate {hash} is submitted but its accounting is not proven complete ({}); inspect qbit_pool_blocks and qbit_pool_audit_bundles before retrying",
                            missing.join(", ")
                        ),
                    ));
                    continue;
                }
                let (height, parent) = match active_block(rpc, hash).await? {
                    Ok(header) => header,
                    Err(detail) => {
                        plan.problems.push((9, not_active_message(hash, &detail)));
                        continue;
                    }
                };
                if Some(height) != landed_height {
                    plan.problems.push((
                        10,
                        format!(
                            "candidate {hash} landed at height {} but the node holds it at height {height}",
                            landed_height.unwrap_or_default()
                        ),
                    ));
                    continue;
                }
                if let Some(landed) = row
                    .landed_parent
                    .as_deref()
                    .filter(|landed| !landed.eq_ignore_ascii_case(&parent))
                {
                    plan.problems.push((
                        10,
                        format!("candidate {hash} landed with parent {landed} but the node's header names {parent}"),
                    ));
                    continue;
                }
                plan.blocks.push(planned(height, parent, true));
            }
            _ => {
                if row.storage_version != 1 {
                    plan.problems.push((
                        7,
                        unsupported_storage_version_message(hash, &json!(row.storage_version)),
                    ));
                    continue;
                }
                if !row.native {
                    plan.problems.push((8, legacy_candidate_message(hash)));
                    continue;
                }
                let (height, parent) = match active_block(rpc, hash).await? {
                    Ok(header) => header,
                    Err(detail) => {
                        plan.problems.push((9, not_active_message(hash, &detail)));
                        continue;
                    }
                };
                match row.stored_height {
                    None => {
                        plan.problems.push((
                            10,
                            format!("candidate {hash} has no readable stored height; this release cannot replay its document"),
                        ));
                        continue;
                    }
                    Some(stored) if u64::try_from(stored).ok() != Some(height) => {
                        plan.problems.push((
                            10,
                            format!("candidate {hash} is stored at height {stored} but the node holds it at height {height}"),
                        ));
                        continue;
                    }
                    Some(_) => {}
                }
                if !listed.contains(parent.as_str()) {
                    unlisted_parents.push((plan.problems.len(), hash.clone(), parent.clone()));
                }
                plan.blocks.push(planned(height, parent, false));
            }
        }
    }
    if !unlisted_parents.is_empty() {
        let parents: Vec<String> = unlisted_parents
            .iter()
            .map(|(_, _, parent)| parent.clone())
            .collect();
        let unfinished = reader.unfinished_states(&parents).await?;
        let mut inserted = 0;
        for (position, child, parent) in &unlisted_parents {
            if let Some((_, state)) = unfinished.iter().find(|(hash, _)| hash == parent) {
                // The parent lookup is batched, but its refusal belongs at
                // the child's allowlist position, before later problems.
                plan.problems.insert(
                    position + inserted,
                    (
                        10,
                        format!(
                            "candidate {child} has an unfinished parent {parent} ({state}) that is not in the allowlist; add --block-hash {parent} so it lands first"
                        ),
                    ),
                );
                inserted += 1;
            }
        }
    }
    plan.blocks
        .sort_by(|a, b| a.height.cmp(&b.height).then_with(|| a.hash.cmp(&b.hash)));
    Ok(plan)
}

/// Land every planned block in order, stopping at the first that cannot be
/// finished. A complete block is verified and skipped, which is what makes
/// rerunning the same allowlist a resume.
async fn apply_recovery(
    coordinator: &Coordinator,
    blocks: &[PlannedBlock],
    deadline: tokio::time::Instant,
    timeout_seconds: u64,
) -> Result<(usize, usize), Stop> {
    let (mut recovered, mut verified) = (0, 0);
    for block in blocks {
        if block.complete {
            println!(
                "verified {} at height {}: already complete",
                block.hash, block.height
            );
            verified += 1;
            continue;
        }
        println!(
            "recovering {} at height {} from {}",
            block.hash, block.height, block.state
        );
        let claimed = coordinator
            .claim_candidate_for_recovery(&block.hash, deadline)
            .await;
        let claim = match claimed {
            Err(error) if matches!(error.downcast_ref::<RecoveryStop>(), Some(RecoveryStop::Deadline)) => {
                return Err(Stop::Exit(
                    11,
                    format!(
                        "recovery deadline of {timeout_seconds} seconds exceeded (claiming); candidate {} is no longer claimed by this attempt",
                        block.hash
                    ),
                ))
            }
            Err(error) => return Err(Stop::Failure(error)),
            Ok(RecoveryClaim::Refused(outcome)) => {
                let (code, message) = recover_refusal(&outcome, &block.hash)?;
                return Err(Stop::Exit(code, message));
            }
            Ok(RecoveryClaim::Claimed(claim)) => *claim,
        };
        if let Err(error) = coordinator.recover_candidate(&claim, deadline).await {
            return Err(recovery_stop(error, &block.hash, timeout_seconds));
        }
        println!("recovered {} at height {}", block.hash, block.height);
        recovered += 1;
    }
    Ok((recovered, verified))
}

/// The exit status of a recovery that stopped after its claim. The row was
/// left recoverable by `Coordinator::recover_candidate` whichever way it
/// stopped; a node or database error is returned as the failure it is.
fn recovery_stop(error: anyhow::Error, hash: &str, timeout_seconds: u64) -> Stop {
    match error.downcast_ref::<RecoveryStop>() {
        Some(RecoveryStop::NotActive(detail)) => Stop::Exit(9, not_active_message(hash, detail)),
        Some(RecoveryStop::Refused(reason)) => Stop::Exit(
            12,
            format!("recovery of {hash} stopped: {reason}; the candidate was left recoverable"),
        ),
        Some(RecoveryStop::Deadline) => Stop::Exit(
            11,
            format!(
                "recovery deadline of {timeout_seconds} seconds exceeded (landing); candidate {hash} was left recoverable and its claim released"
            ),
        ),
        None => Stop::Failure(error.context(format!(
            "recovery of {hash} stopped; the candidate was left recoverable"
        ))),
    }
}

/// One exit status per refused recovery claim, in the numbering `abandon`
/// uses for the outcomes the two share (2, 4, 5, 7, 8), plus 12 for a row
/// this binary cannot authenticate. An outcome this function does not
/// recognise is a failure, never a "nothing to do".
fn recover_refusal(outcome: &Value, hash: &str) -> Result<(i32, String)> {
    let field = |name: &str| outcome[name].as_str().unwrap_or("unknown").to_owned();
    Ok(match outcome["outcome"].as_str().unwrap_or_default() {
        "missing" => (2, format!("no candidate row for {hash}")),
        "terminal" => (
            4,
            match field("state").as_str() {
                "orphaned" => orphaned_candidate_message(hash),
                "abandoned" => format!(
                    "candidate {hash} is already abandoned; its evidence was released and it cannot be recovered"
                ),
                state => format!(
                    "candidate {hash} is already {state}; it completed since the plan was made, rerun recover to verify it"
                ),
            },
        ),
        "claimed" => (
            5,
            format!(
                "candidate {hash} is held by {} until {}; retry after the claim expires",
                field("claim_instance_id"),
                field("claim_expires_at")
            ),
        ),
        "unsupported_storage_version" => (
            7,
            unsupported_storage_version_message(hash, &outcome["storage_version"]),
        ),
        "legacy_candidate" => (8, legacy_candidate_message(hash)),
        "invalid" => (
            12,
            format!(
                "recovery of {hash} stopped: {}; the candidate was left recoverable",
                field("reason")
            ),
        ),
        other => bail!("unrecognized recovery claim outcome {other:?} for candidate {hash}"),
    })
}

/// The plan an operator reads before `--apply`: the whole hash, so it can be
/// pasted back, the node's height and parent, the row's state and claim as
/// `list` renders it, and what `--apply` would do with it.
fn recovery_plan_table(blocks: &[PlannedBlock]) -> String {
    let mut table =
        vec![["block_hash", "height", "state", "parent", "claim", "action"].map(str::to_owned)];
    table.extend(blocks.iter().map(|block| {
        [
            block.hash.clone(),
            block.height.to_string(),
            block.state.clone(),
            block.parent.clone(),
            block.claim.clone(),
            if block.complete {
                "complete"
            } else {
                "recover"
            }
            .to_owned(),
        ]
    }));
    render_table(&table)
}

/// One exit status per abandon outcome, so a runbook can tell a row that was
/// never there (2) from one already abandoned (4), and both from the two
/// lifecycle refusals: a block that may already have been
/// offered to the node (3) and a block whose accounting has landed (6).
/// Unsupported storage versions (7) retain their evidence for a compatible reader.
/// A pre-migration 2.x.x document parked at the supported storage version (8)
/// retains its evidence for the legacy drain, which is the only thing that may
/// finish it. An outcome this function does not recognise is a failure, never a
/// "nothing to do".
fn abandon_report(outcome: &Value, block_hash: &str, reason: &str) -> Result<(i32, String)> {
    let field = |name: &str| outcome[name].as_str().unwrap_or("unknown").to_owned();
    Ok(match outcome["outcome"].as_str().unwrap_or_default() {
        "abandoned" => (0, format!("abandoned {block_hash}: {reason}")),
        "missing" => (2, format!("no candidate row for {block_hash}")),
        "offered" => (
            3,
            format!(
                "candidate {block_hash} is in state {}; it was offered to the node and is never \
                 abandoned. Its block may already have been submitted. Leave it to reconciliation",
                field("state")
            ),
        ),
        "terminal" => (
            4,
            format!(
                "candidate {block_hash} is already {}; nothing to do",
                field("state")
            ),
        ),
        "claimed" => (
            5,
            format!(
                "candidate {block_hash} is held by {} until {}; retry after the claim expires",
                field("claim_instance_id"),
                field("claim_expires_at")
            ),
        ),
        "landed" => (
            6,
            format!(
                "candidate {block_hash} is pending but its block is already in qbit_pool_blocks; \
                 reconcile it before abandoning — abandoning would discard landed accounting"
            ),
        ),
        "unsupported_storage_version" => (
            7,
            format!(
                "candidate {block_hash} has unsupported storage_version {}; evidence preserved. \
                 Only version 1 is supported; drain legacy rows with the pinned 2.x.x image, \
                 and use a compatible release for newer formats",
                outcome["storage_version"]
            ),
        ),
        "legacy_candidate" => (
            8,
            format!(
                "candidate {block_hash} holds a pre-migration 2.x.x document at storage_version \
                 1; evidence preserved. This release cannot replay it, and abandoning it would \
                 discard the block the legacy drain still owes: drain it with the pinned 2.x.x \
                 image, never an operator abandon"
            ),
        ),
        other => bail!("unrecognized abandon outcome {other:?} for candidate {block_hash}"),
    })
}

/// The text inventory an operator reads at 3 a.m. The block hash is printed
/// whole so it can be pasted straight into `candidates abandon`; a parked row
/// says `parked` where a retrying row shows its due time; and `last_error`,
/// the only unbounded field, is the only one truncated, and says so when it is.
fn candidate_table(rows: &[Value]) -> String {
    const HEADERS: [&str; 8] = [
        "block_hash",
        "state",
        "height",
        "attempts",
        "next_attempt",
        "claim",
        "sv",
        "last_error",
    ];
    let mut table = vec![HEADERS.map(str::to_owned)];
    table.extend(rows.iter().map(|row| {
        [
            cell(&row["block_hash"]),
            cell(&row["state"]),
            cell(&row["block_height"]),
            cell(&row["attempt_count"]),
            if row["parked"] == Value::Bool(true) {
                "parked".to_owned()
            } else {
                cell(&row["next_attempt_at"])
            },
            claim_cell(row),
            cell(&row["storage_version"]),
            last_error_cell(&row["last_error"]),
        ]
    }));
    render_table(&table)
}

/// Left-aligned columns two spaces apart, each as wide as its widest cell,
/// with no trailing spaces on a line.
fn render_table<const N: usize>(table: &[[String; N]]) -> String {
    let widths: Vec<usize> = (0..N)
        .map(|column| {
            table
                .iter()
                .map(|row| row[column].chars().count())
                .max()
                .unwrap_or_default()
        })
        .collect();
    table
        .iter()
        .map(|row| {
            let line = row
                .iter()
                .zip(&widths)
                .map(|(value, width)| format!("{value:<width$}"))
                .collect::<Vec<_>>()
                .join("  ");
            format!("{}\n", line.trim_end())
        })
        .collect()
}

/// An unknown value is `-`, never `0` and never blank: a candidate whose
/// document holds no readable height has an unknown height, which is
/// information, not a zero.
fn cell(value: &Value) -> String {
    match value {
        Value::Null => "-".to_owned(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// A live claim names its holder and expiry, a claim past its expiry is
/// `expired` (the row is workable again, and abandonable), and no claim at
/// all is `-`. `--json` keeps the stale holder and expiry.
fn claim_cell(row: &Value) -> String {
    claim_text(
        row["claim_instance_id"].as_str(),
        row["claim_live"].as_bool().unwrap_or(false),
        &cell(&row["claim_expires_at"]),
    )
}

fn claim_text(instance: Option<&str>, live: bool, expires_at: &str) -> String {
    match (instance, live) {
        (None, _) => "-".to_owned(),
        (Some(_), false) => "expired".to_owned(),
        (Some(instance), true) => format!("{instance} until {expires_at}"),
    }
}

fn last_error_cell(value: &Value) -> String {
    const WIDTH: usize = 72;
    let Some(text) = value.as_str() else {
        return "-".to_owned();
    };
    let flat: String = text
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if flat.chars().count() <= WIDTH {
        return flat;
    }
    format!(
        "{}…(truncated)",
        flat.chars().take(WIDTH).collect::<String>()
    )
}

fn diagnostic_host(bind: &str) -> String {
    let host = match bind.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(address)) if address.is_unspecified() => "127.0.0.1",
        Ok(std::net::IpAddr::V6(address)) if address.is_unspecified() => "::1",
        _ => bind,
    };
    config::authority_host(host)
}

async fn healthcheck(url: Option<String>, public_api: bool) -> Result<()> {
    let (bind_name, port_name, default_port) = if public_api {
        ("PRISM_PUBLIC_API_BIND", "PRISM_PUBLIC_API_PORT", 3342u16)
    } else {
        ("PRISM_AUDIT_BIND", "PRISM_AUDIT_PORT", 3341u16)
    };
    let port = config::number(port_name, default_port)?;
    if url.is_none() && port == 0 && !public_api {
        return stratum_healthcheck().await;
    }
    let host = diagnostic_host(&config::value(bind_name, "127.0.0.1"));
    let url = url.unwrap_or_else(|| format!("http://{host}:{port}/healthz"));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        // A readiness endpoint must not redirect a bearer credential elsewhere.
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut request = client.get(url);
    if !public_api {
        if let Some(token) = config::secret("PRISM_OPERATOR_BEARER_TOKEN")? {
            request = request.bearer_auth(token);
        }
    }
    let response = request.send().await?;
    ensure!(
        response.status().is_success(),
        "PRISM is unhealthy (HTTP {})",
        response.status()
    );
    let value: Value = response.json().await?;
    ensure!(value["ok"] == true, "PRISM health is not ready");
    Ok(())
}

async fn stratum_healthcheck() -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let mut port = config::number("PRISM_STRATUM_PORT", 3340u16)?;
    let mut bind = config::value("PRISM_STRATUM_BIND", "127.0.0.1");
    if port == 0 {
        port = config::number("PRISM_STRATUM_HIGHDIFF_PORT", 0u16)?;
        bind = config::optional("PRISM_STRATUM_HIGHDIFF_BIND").unwrap_or(bind);
    }
    ensure!(port > 0, "no configured listener for PRISM health probe");
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut socket =
            tokio::net::TcpStream::connect(format!("{}:{port}", diagnostic_host(&bind)))
                .await
                .context("connect Stratum health probe")?;
        socket
            .write_all(b"{\"id\":1,\"method\":\"mining.get_health\",\"params\":[]}\n")
            .await?;
        let mut reader = BufReader::new(socket).take(4097);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await?;
        ensure!(
            line.len() <= 4096 && line.last() == Some(&b'\n'),
            "invalid Stratum health response frame"
        );
        let response: Value = serde_json::from_slice(&line)?;
        ensure!(
            response["id"] == 1
                && response["error"].is_null()
                && response["result"]["ready"] == true,
            "PRISM Stratum health is not ready"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("Stratum health probe timed out")?
}

#[derive(Serialize)]
struct SelfCheckReport {
    schema: &'static str,
    ok: bool,
    instance_id: Option<String>,
    health: Option<Value>,
    carry_forward_integrity: Option<Value>,
    durability: Option<Vec<(String, String)>>,
    audit_completeness: Option<AuditCompleteness>,
    live_instances: LiveInstancesReport,
}

async fn self_check() -> Result<()> {
    let mut report = SelfCheckReport {
        schema: "qbit.prism.self-check.v2",
        ok: false,
        instance_id: None,
        health: None,
        carry_forward_integrity: None,
        durability: None,
        audit_completeness: None,
        live_instances: unavailable_live_instances(
            "unknown",
            "Heartbeat not sampled because configuration is unavailable; HA is unknown",
            crate::api::ApiConfig::default().health_stale_after(),
        ),
    };
    let result = async {
        let config = Config::from_env()?;
        let freshness =
            crate::api::health_stale_after(crate::api::health_refresh_interval_from_env()?);
        report.instance_id = Some(config.instance_id.clone());
        // Both samples are read-only and independent of the local startup
        // below: a node startup or refresh failure must hide neither the
        // cluster's heartbeats nor an unfinished historical import.
        let (instances, completeness) = tokio::join!(
            live_instances(&config.database_url, freshness),
            sample_audit_completeness(&config.database_url),
        );
        report.live_instances = instances;
        report.audit_completeness = completeness.as_ref().ok().copied();
        if let Some(completeness) = &report.audit_completeness {
            if config::production_mode()? {
                completeness.require_complete()?;
            }
        }
        // A failed heartbeat sample must not suppress the remaining local checks.
        self_check_local(config, &mut report).await?;
        completeness?;
        ensure!(
            report.live_instances.status != "failed",
            "could not read cluster heartbeats"
        );
        Ok(())
    }
    .await;
    report.ok = result.is_ok();
    println!("{}", serde_json::to_string_pretty(&report)?);
    result
}

async fn sample_audit_completeness(database_url: &str) -> Result<AuditCompleteness> {
    use sqlx::Connection;

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut connection = sqlx::PgConnection::connect(database_url).await?;
        sqlx::query("SET default_transaction_read_only = on")
            .execute(&mut connection)
            .await?;
        audit_completeness(&mut connection).await
    })
    .await;
    // Connection failures can include a credentialed DSN. Keep those out of
    // operator output, and never turn an unavailable count into zero.
    match result {
        Ok(Ok(report)) => Ok(report),
        _ => anyhow::bail!("audit completeness read failed or exceeded 5 seconds"),
    }
}

async fn self_check_local(config: Config, report: &mut SelfCheckReport) -> Result<()> {
    // A diagnostic is not a frontend: it registers no heartbeat, so its exit
    // leaves nothing for fatal-state recovery to refuse and a live frontend
    // sharing PRISM_INSTANCE_ID keeps its own status.
    let coordinator = Coordinator::new_tool(
        config,
        std::sync::Arc::new(crate::metrics::Metrics::default()),
    )
    .await?;
    coordinator.refresh_once().await?;
    let integrity: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
        .fetch_one(&coordinator.ledger.pool)
        .await?;
    report.health = Some(coordinator.health().await);
    report.carry_forward_integrity = Some(integrity.clone());
    for field in ["mismatch_count", "current_drift_count"] {
        ensure!(
            integrity[field].as_u64() == Some(0),
            "carry-forward integrity failure in {field}: {integrity}"
        );
    }
    let durability:Vec<(String,String)>=sqlx::query_as("SELECT name,setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit') ORDER BY name").fetch_all(&coordinator.ledger.pool).await?;
    report.durability = Some(durability.clone());
    for (name, value) in &durability {
        ensure!(value != "off", "PostgreSQL {name} is disabled");
    }
    healthcheck(None, false).await?;
    let stratum = crate::stratum::StratumConfig::from_env()?;
    if let Some(highdiff) = stratum.highdiff_config()? {
        let recent: Option<String> = sqlx::query_scalar(
            "SELECT miner_id FROM qbit_share_ledger ORDER BY share_seq DESC LIMIT 1",
        )
        .fetch_optional(&coordinator.ledger.pool)
        .await?;
        let username=config::optional("PRISM_SELF_CHECK_ADDRESS").or_else(||coordinator.config.username_fallback.clone()).or_else(||coordinator.config.fee_address.clone()).or(recent).context("set PRISM_SELF_CHECK_ADDRESS to a valid P2MR address to probe highdiff on an empty pool")?;
        let bind = config::optional("PRISM_STRATUM_HIGHDIFF_BIND")
            .unwrap_or_else(|| config::value("PRISM_STRATUM_BIND", "127.0.0.1"));
        let host = diagnostic_host(&bind);
        let port = config::number("PRISM_STRATUM_HIGHDIFF_PORT", 4334u16)?;
        let actual = crate::stratum::probe_first_difficulty(
            &format!("{host}:{port}"),
            &username,
            Duration::from_secs(15),
        )
        .await?;
        ensure!(
            actual >= highdiff.minimum_difficulty,
            "highdiff listener advertised a difficulty below its floor"
        );
    }
    Ok(())
}

fn benchmark(count: usize, miners: usize, iterations: usize) -> Result<Value> {
    ensure!(
        count > 0
            && count <= 10_000_000
            && miners > 0
            && miners <= count
            && iterations > 0
            && iterations <= 10_000,
        "invalid benchmark dimensions"
    );
    let shares: Vec<_> = (0..count)
        .map(|i| qbit_prism::AcceptedShare {
            share_seq: i as u64 + 1,
            share_id: format!("share-{i}"),
            miner_id: format!("miner-{}", i % miners),
            order_key: format!("miner-{}", i % miners),
            p2mr_program_hex: format!("{:064x}", i % miners + 1),
            share_difficulty: 1,
            network_difficulty: count as u128,
            template_height: 1,
            job_id: "benchmark".into(),
            job_issued_at_ms: 0,
            accepted_at_ms: 0,
            ntime: 1,
            credit_policy: None,
        })
        .collect();
    let key = qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"11".repeat(32))?;
    let ledger_key = qbit_pool_builder::ManifestSigningKey::from_seed_hex(&"22".repeat(32))?;
    let found = qbit_prism::FoundBlock {
        block_height: 2,
        coinbase_value_sats: 5_000_000_000,
        network_difficulty: count as u128,
        anchor_job_issued_at_ms: 1,
    };
    let mut milliseconds = Vec::new();
    let mut last_bytes = 0;
    for _ in 0..iterations {
        let started = Instant::now();
        let bundle = qbit_prism::build_audit_bundle(
            shares.clone(),
            found.clone(),
            vec![],
            qbit_prism::PayoutPolicy::day_one_default(),
            &key,
            &ledger_key,
        )?;
        qbit_prism::verify_audit_bundle_with_ledger_public_key(
            &bundle,
            &ledger_key.public_key_hex(),
        )?;
        milliseconds.push(started.elapsed().as_secs_f64() * 1000.0);
        last_bytes = qbit_prism::canonical_audit_bundle_bytes(&bundle)?.len();
    }
    milliseconds.sort_by(f64::total_cmp);
    Ok(
        json!({"schema":"qbit.prism.native-builder-benchmark.v1","shares":count,"miners":miners,"iterations":iterations,"build_and_verify_p50_ms":milliseconds[iterations/2],"build_and_verify_p99_ms":milliseconds[(iterations*99/100).min(iterations-1)],"canonical_audit_bytes":last_bytes,"engine":"in-process-rust"}),
    )
}

#[cfg(test)]
mod configuration_tests {
    use super::*;

    /// The recover allowlist and deadline are checked where clap parses them
    /// and, for the bounds clap cannot express, at the top of `recover`.
    #[test]
    fn recover_arguments_are_bounded_at_the_entry() {
        let hash = "ab".repeat(32);
        let parsed = Cli::try_parse_from([
            "prism",
            "candidates",
            "recover",
            "--block-hash",
            &hash,
            "--block-hash",
            &"cd".repeat(32),
            "--apply",
            "--timeout-seconds",
            "3600",
        ])
        .unwrap();
        let Some(Command::Candidates {
            command:
                CandidatesCommand::Recover {
                    block_hash,
                    apply,
                    timeout_seconds,
                },
        }) = parsed.command
        else {
            panic!("wrong command");
        };
        assert_eq!(block_hash, vec![hash.clone(), "cd".repeat(32)]);
        assert!(apply);
        assert_eq!(timeout_seconds, 3600);
        let Some(Command::Candidates {
            command:
                CandidatesCommand::Recover {
                    apply,
                    timeout_seconds,
                    ..
                },
        }) = Cli::try_parse_from(["prism", "candidates", "recover", "--block-hash", &hash])
            .unwrap()
            .command
        else {
            panic!("wrong command");
        };
        assert!(!apply, "plan-only by default");
        assert_eq!(timeout_seconds, 600, "the 2.x.x runner's default");
        for (args, expected) in [
            (vec!["prism", "candidates", "recover"], "--block-hash"),
            (
                vec![
                    "prism",
                    "candidates",
                    "recover",
                    "--block-hash",
                    &hash,
                    "--timeout-seconds",
                    "0",
                ],
                "timeout-seconds",
            ),
            (
                vec![
                    "prism",
                    "candidates",
                    "recover",
                    "--block-hash",
                    &hash,
                    "--timeout-seconds",
                    "3601",
                ],
                "timeout-seconds",
            ),
        ] {
            let error = match Cli::try_parse_from(&args) {
                Ok(_) => panic!("{args:?} was accepted"),
                Err(error) => error.to_string(),
            };
            assert!(error.contains(expected), "{args:?}: {error}");
        }
        // Malformed, duplicate and out-of-range allowlists are refused before
        // any connection, by the same rule `abandon` applies to a hash.
        assert!(require_block_hash(&hash).is_ok());
        for bad in ["abc", &"AB".repeat(32), &"zz".repeat(32), &"ab".repeat(33)] {
            assert!(require_block_hash(bad).is_err(), "{bad}");
        }
        assert_eq!(MAX_RECOVERY_BLOCKS, 32);
    }

    #[test]
    fn audit_root_env_and_cli_precedence() {
        if let Ok(case) = std::env::var("AUDIT_ROOT_TEST_CASE") {
            let args = if case == "cli" {
                vec!["prism", "import-audits", "--root", "/explicit/audits"]
            } else {
                vec!["prism", "import-audits"]
            };
            let Some(Command::ImportAudits { root }) = Cli::try_parse_from(args).unwrap().command
            else {
                panic!("wrong command");
            };
            let expected = match case.as_str() {
                "unset" | "empty" | "blank" => None,
                "env" => Some(PathBuf::from("/mounted/audits")),
                "cli" => Some(PathBuf::from("/explicit/audits")),
                _ => panic!("unknown case"),
            };
            assert_eq!(audit_root(root), expected);
            return;
        }
        for case in ["unset", "empty", "blank", "env", "cli"] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "tools::configuration_tests::audit_root_env_and_cli_precedence",
                    "--nocapture",
                ])
                .env_clear()
                .env("AUDIT_ROOT_TEST_CASE", case);
            if case != "unset" {
                let value = match case {
                    "empty" => "",
                    "blank" => "  ",
                    _ => "/mounted/audits",
                };
                command.env("PRISM_AUDIT_DIR", value);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{case}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
