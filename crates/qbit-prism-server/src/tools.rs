use crate::{
    config::{self, Config},
    coordinator::{Coordinator, RecoveryStop},
    ledger::{
        audit_completeness, live_instances, unavailable_live_instances, AuditCompleteness,
        LiveInstancesReport, RecoveryClaim, RecoveryReader, RecoveryRow, RecoveryTakeover,
    },
    rpc::{Rpc, RpcReplyError},
};
use anyhow::{bail, ensure, Context, Result};
use clap::{Args, Parser, Subcommand};
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
    /// Reset the cluster fingerprint for a signing-key rotation after stopping every frontend.
    SigningTransition {
        /// Perform the reset. Without it, print the checks and effects, change nothing and fail.
        #[arg(long)]
        confirm: bool,
    },
    /// Inspect a cluster halt or reconcile and record an operator recovery.
    FatalState {
        #[command(subcommand)]
        command: FatalStateCommand,
    },
    /// Hold block submission for every frontend of the cluster, clear the hold, or show it.
    SubmissionHold {
        #[command(subcommand)]
        command: SubmissionHoldCommand,
    },
    /// Show, set or re-personalise which 3.1 dual-writer node this database is.
    NodeIdentity {
        #[command(subcommand)]
        command: NodeIdentityCommand,
    },
    /// Seal, archive, verify, detach, drop and restore share ledger partitions.
    ShareArchive {
        #[command(subcommand)]
        command: ShareArchiveCommand,
    },
    /// Inspect unfinished block candidates, abandon a pending one, or recover accepted ones.
    Candidates {
        #[command(subcommand)]
        command: CandidatesCommand,
    },
    /// Show the dual-writer carry owner, release it on this node, or transfer it here.
    CarryOwner {
        #[command(subcommand)]
        command: CarryOwnerCommand,
    },
    /// Validate compact target bits and print Prism's exact scaled difficulty.
    HeaderDifficulty {
        #[arg(long)]
        bits: String,
    },
    /// Apply the additive PostgreSQL migration after stopping Python writers.
    Migrate {
        /// Map only migration 2's legacy share headers within 1000 template
        /// heights of the highest, then let frontends serve with the rest
        /// pending; `backfill-share-hashes` (or a later `migrate` without this
        /// flag) maps it while they serve and records 2.
        /// A flag, never an environment setting: frontends started with
        /// PRISM_POSTGRES_INIT_SCHEMA=1 migrate too.
        #[arg(long)]
        defer_share_hashes: bool,
        /// Build the indexes of migrations 13, 24 and 31 with a plain, parallel
        /// CREATE INDEX in one transaction instead of CONCURRENTLY: quicker,
        /// but appends and reads wait for it, so stop every frontend and tool
        /// first. An instance that has not reported drained or stopped is
        /// refused. A flag, never an environment setting: frontends started
        /// with PRISM_POSTGRES_INIT_SCHEMA=1 migrate too.
        #[arg(long)]
        offline_indexes: bool,
        /// Parallel workers each offline index build may use, as the server's
        /// worker slots and maintenance_work_mem allow.
        #[arg(
            long,
            default_value_t = 4,
            requires = "offline_indexes",
            value_parser = clap::value_parser!(u16).range(..=1024)
        )]
        index_build_workers: u16,
        /// maintenance_work_mem for each offline index build, as PostgreSQL
        /// writes it (512MB, 2GB). A build's sort uses about this much in all,
        /// divided among its leader and workers, at least 32MB each or fewer
        /// workers. Without it the run takes 2GB, or the server's setting when
        /// that is higher.
        #[arg(
            long,
            requires = "offline_indexes",
            value_parser = parse_index_build_memory
        )]
        index_build_memory: Option<u32>,
    },
    /// Map the rest of migration 2's share-hash backfill while frontends serve, after
    /// `migrate --defer-share-hashes`, in throttled batches, and record migration 2.
    BackfillShareHashes {
        /// The most share_seq values one batch maps. Each batch is one statement in a
        /// transaction of its own.
        #[arg(long, default_value_t = crate::ledger::ShareHashThrottle::DEFAULT_MAX_BATCH,
              value_parser = throttle_flag(crate::ledger::ShareHashThrottle::check_max_batch))]
        max_batch: i64,
        /// Each batch statement's timeout, at most 5000. A batch that outlasts it is retried at half
        /// the size, so no statement holds a snapshot on the primary longer than this.
        #[arg(long, default_value_t = crate::ledger::ShareHashThrottle::DEFAULT_STATEMENT_TIMEOUT_MS,
              value_parser = throttle_flag(crate::ledger::ShareHashThrottle::check_statement_timeout_ms))]
        statement_timeout_ms: u64,
        /// The share of the time batches may take: after a batch that took t, the backfill rests
        /// t * (1 - duty cycle) / duty cycle. From 0.01 to 1.
        #[arg(long, default_value_t = crate::ledger::ShareHashThrottle::DEFAULT_DUTY_CYCLE,
              value_parser = throttle_flag(crate::ledger::ShareHashThrottle::check_duty_cycle))]
        duty_cycle: f64,
    },
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

/// What `--index-build-memory` takes: a size as PostgreSQL writes one.
const INDEX_BUILD_MEMORY_SYNTAX: &str = "not a size PostgreSQL accepts: give a number and optionally one of the units B, kB, MB, GB or TB, which are case-sensitive, such as 512MB or 2GB";

/// `--index-build-memory` as maintenance_work_mem takes it, in kilobytes:
/// a number, whole or not, then optionally a unit (kB without one), rounded
/// to whole kilobytes and bounded to 1MB..2147483647kB, as PostgreSQL
/// parses and bounds that setting, so `migrate` refuses a size before it
/// connects instead of after the migrations ahead of the build.
fn parse_index_build_memory(value: &str) -> std::result::Result<u32, String> {
    let value = value.trim();
    let split = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let multiplier = match unit.trim_start() {
        "" | "kB" => 1.0,
        "B" => 1.0 / 1024.0,
        "MB" => 1024.0,
        "GB" => 1024.0 * 1024.0,
        "TB" => 1024.0 * 1024.0 * 1024.0,
        _ => return Err(INDEX_BUILD_MEMORY_SYNTAX.to_owned()),
    };
    let number: f64 = number
        .parse()
        .map_err(|_| INDEX_BUILD_MEMORY_SYNTAX.to_owned())?;
    let kilobytes = (number * multiplier).round_ties_even();
    if !(1024.0..=f64::from(i32::MAX)).contains(&kilobytes) {
        return Err("maintenance_work_mem must be between 1MB and 2147483647kB".to_owned());
    }
    Ok(kilobytes as u32)
}

/// The retention rules shared by every command that evaluates eligibility.
/// The defaults are the design record's: the payout window floor is taken at four
/// times the requested weight, so a difficulty rise of up to 4x between two
/// retention runs cannot reach into archived history, and a share stays online
/// for thirty days, which covers every dashboard read of raw rows with margin.
#[derive(Args)]
struct RetentionArgs {
    /// Network difficulty the payout window floor is taken at, as a whole number.
    #[arg(long)]
    network_difficulty: String,
    /// Days a share stays online after it was accepted.
    #[arg(long, default_value_t = 30)]
    retention_days: i64,
    /// Multiple of the requested window weight the floor allows for.
    #[arg(long, default_value_t = 4)]
    window_multiple: i64,
    /// Also scan the attached leaves for a share_id held by more than one of
    /// them. One pass over every attached partition, so it is not the default.
    #[arg(long)]
    check_duplicates: bool,
}

impl RetentionArgs {
    fn options(self) -> crate::ledger::archive::PlanOptions {
        crate::ledger::archive::PlanOptions {
            network_difficulty: self.network_difficulty,
            retention_days: self.retention_days,
            window_multiple: self.window_multiple,
            check_duplicates: self.check_duplicates,
        }
    }
}

#[derive(Subcommand)]
enum ShareArchiveCommand {
    /// Print every partition with its bounds, rows and the five retention
    /// conditions, each with its blocker named. Nothing is changed.
    Plan {
        #[command(flatten)]
        retention: RetentionArgs,
    },
    /// Store the canonical bytes of every audit whose share window intersects
    /// the partition, so its blocks keep serving once its shares are gone.
    Seal {
        /// Partition name, as printed by share-archive plan.
        partition: String,
    },
    /// Write the partition's rows and manifest under
    /// <root>/qbit_share_ledger/<partition>/<manifest-sha256>/ and record
    /// them in the catalog.
    Archive {
        partition: String,
        /// Archive root the layout is written under.
        #[arg(long)]
        dir: PathBuf,
        /// Write an archive again for a partition that already has one, into
        /// a new version directory that replaces the recorded one, clearing
        /// its verification and that of every later archive, which must then
        /// be written and verified again in order, each over its verified
        /// predecessor.
        #[arg(long)]
        force: bool,
    },
    /// Re-read the archive, recompute both digests, check the manifest against
    /// the catalog and the chain, including that the archive it links to is
    /// verified, and compare the live rows while they are there.
    Verify {
        partition: String,
        #[arg(long)]
        dir: PathBuf,
    },
    /// Detach a sealed, archived and verified partition once every retention
    /// condition is clear. The table stays as a standalone relation.
    Detach {
        partition: String,
        #[command(flatten)]
        retention: RetentionArgs,
    },
    /// Drop a detached, verified partition once its archive has been read back
    /// from disk and checked. The archive is the copy of record.
    Drop {
        partition: String,
        /// Archive root the recorded archive is read back from.
        #[arg(long)]
        dir: PathBuf,
    },
    /// Recreate a partition table from its archive and verify it row for row.
    Restore {
        /// Path to the archive's manifest.json, absolute or under --dir.
        manifest: PathBuf,
        /// Archive root a relative manifest path is resolved against.
        #[arg(long)]
        dir: PathBuf,
        /// Attach the restored table back under its recorded bounds.
        #[arg(long)]
        attach: bool,
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
        /// Seconds the whole clear may take, its node calls and integrity report included (10 to 3600).
        #[arg(long, default_value_t = crate::ledger::FATAL_STATE_CLEAR_BOUND.as_secs(), value_parser = clap::value_parser!(u64).range(10..=3600))]
        timeout_seconds: u64,
    },
}

/// #664: the cluster-wide block submission hold, stored in the ledger.
#[derive(Subcommand)]
enum SubmissionHoldCommand {
    /// Print the hold as JSON. Needs only PRISM_DATABASE_URL and opens it read-only.
    Show,
    /// Hold submission: no frontend claims a candidate, offers a block or sends a fanout.
    Set {
        /// Why submission is held, kept with the hold and journaled (1 to 4096 bytes).
        #[arg(long)]
        reason: String,
    },
    /// Clear the hold. Refused while a candidate is pending, unless told to offer them.
    Clear {
        /// Why the hold is cleared, journaled (1 to 4096 bytes).
        #[arg(long)]
        reason: String,
        /// Let frontends with submission enabled offer the pending candidates the hold kept back.
        #[arg(long)]
        offer_pending_candidates: bool,
    },
}

/// 3.1 dual writer: which node a database is (CONTRACT D-9). Only the
/// bootstrap and cutover steps set it, and the rebuild of a node from its
/// peer's database re-personalises it (D-16); a frontend never writes it.
#[derive(Subcommand)]
enum NodeIdentityCommand {
    /// Print the database's node identity and lineage as JSON. Needs only PRISM_DATABASE_URL.
    Show,
    /// Make this database node A's (0) or node B's (1), once, before its first dual-writer start.
    Set {
        /// 0 for node A, 1 for node B.
        #[arg(long, value_parser = clap::value_parser!(u8).range(0..=1))]
        index: u8,
    },
    /// Make a promoted physical copy of the peer's database this node's, rebuilding a node whose
    /// disk was replaced, before its first start; every PRISM process using it must be stopped.
    Repersonalise {
        /// The node being rebuilt: 0 for node A, 1 for node B. The copy must say it is the other.
        #[arg(long, value_parser = clap::value_parser!(u8).range(0..=1))]
        index: u8,
    },
}

#[derive(Subcommand)]
enum CarryOwnerCommand {
    /// Print what the carry owner guard reads (setting, node identity, both journals) and what
    /// it would decide, as JSON.
    Status,
    /// Give up carry ownership on this node: journal a release and supersede its work.
    Release {
        /// Why ownership is released, journaled (1 to 4096 bytes).
        #[arg(long)]
        reason: String,
        /// Write the release. Without it, print the checks, change nothing and fail.
        #[arg(long)]
        confirm: bool,
    },
    /// Take carry ownership on this node once the peer has released it, the release is buried
    /// and every pool block on the active chain is landed here.
    Transfer {
        /// Why ownership moves, journaled (1 to 4096 bytes).
        #[arg(long)]
        reason: String,
        /// The first height the chain scan reads. The default, 0, reads the whole chain.
        #[arg(long, default_value_t = 0)]
        from_height: u64,
        /// Write the transfer. Without it, print the checks, change nothing and fail.
        #[arg(long)]
        confirm: bool,
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
        /// Abandon a claimed row once the database clock passes its claim_expires_at, instead of
        /// after watching the claim go unrenewed for its whole lease. UNSAFE during or after a
        /// database clock step: a forward step abandons a row its live holder is landing (#581).
        #[arg(long)]
        unsafe_database_clock_expiry: bool,
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
        /// Take over another holder's claim once the database clock passes its claim_expires_at,
        /// instead of after watching it go unrenewed for its whole lease. UNSAFE during or after a
        /// database clock step: a forward step takes a live holder's row (#581).
        #[arg(long)]
        unsafe_database_clock_expiry: bool,
    },
}

/// The bound on one recovery allowlist: #259's, carried over. There is no
/// "recover everything".
const MAX_RECOVERY_BLOCKS: usize = 32;

/// A `backfill-share-hashes` throttle flag, parsed and then held to the
/// throttle's own `check`, before anything connects: each bound lives
/// there, once.
fn throttle_flag<T>(
    check: fn(T) -> Result<T>,
) -> impl Fn(&str) -> std::result::Result<T, String> + Clone + Send + Sync + 'static
where
    T: std::str::FromStr + 'static,
{
    move |value: &str| {
        let parsed = value
            .parse()
            .map_err(|_| format!("{value:?} is not a number"))?;
        check(parsed).map_err(|error| error.to_string())
    }
}

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
        Command::Run => {
            config::check_environment()?;
            crate::server::run(Config::from_env()?).await
        }
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
            config.ensure_pool_fee_settles_dust()?;
            crate::rollups::settings_from_env()?;
            crate::partitions::settings_from_env()?;
            crate::memory::landing_trim_from_env()?;
            crate::stratum::StratumConfig::from_env()?.highdiff_config()?;
            crate::api::ApiConfig::from_env()?;
            crate::api::public_service::ServiceConfig::from_env()?;
            // #291: the kill switch leads the report, where a rehearsal
            // cannot miss it.
            let submission = config.block_submission();
            if let Some(warning) = submission.warning {
                println!("WARNING: {warning}");
            }
            if submission.ctv_broadcaster == config::CtvBroadcaster::Held {
                println!("WARNING: {}", config::CTV_BROADCASTER_HELD);
            }
            println!(
                "PRISM configuration valid; {} runtime workers",
                config.runtime_workers
            );
            // A held frontend makes no found-block offer, so nothing waits.
            if submission.enabled {
                println!(
                    "PRISM_BLOCK_SUBMIT_ENABLED is on: found blocks are offered to the node's \
                     submitblock"
                );
                match &config.offer_standby {
                    Some(wait) => println!(
                        "found-block offers wait up to {} ms for standby {}; self-check verifies \
                         the role can read its position (pg_monitor)",
                        wait.bound.as_millis(),
                        wait.application_name
                    ),
                    None => println!("found-block offers do not wait for a failover standby"),
                }
            }
            // #664: the hold lives in the database, which this check never reads.
            println!(
                "a cluster-wide block submission hold in the database overrides \
                 PRISM_BLOCK_SUBMIT_ENABLED; `qbit-prism-server submission-hold show` and \
                 self-check report it"
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
        Command::Healthcheck { url, public_api } => {
            healthcheck(url, public_api, HealthRule::Container).await
        }
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
        Command::SigningTransition { confirm } => signing_transition(confirm).await,
        Command::FatalState { command } => fatal_state(command).await,
        Command::SubmissionHold { command } => submission_hold(command).await,
        Command::NodeIdentity { command } => node_identity(command).await,
        Command::ShareArchive { command } => share_archive(command).await,
        Command::Candidates { command } => candidates(command).await,
        Command::CarryOwner { command } => carry_owner(command).await,
        Command::HeaderDifficulty { bits } => {
            let compact = crate::codec::parse_u32_hex(&bits)?;
            let target = crate::codec::target_from_compact(compact)?;
            println!("{}", crate::codec::scaled_target_difficulty(&target)?);
            Ok(())
        }
        Command::Migrate {
            defer_share_hashes,
            offline_indexes,
            index_build_workers,
            index_build_memory,
        } => {
            let config = config::DatabaseConfig::from_env()?;
            let options = crate::ledger::MigrateOptions {
                share_hashes: if defer_share_hashes {
                    crate::ledger::ShareHashBackfill::Defer
                } else {
                    crate::ledger::ShareHashBackfill::Finish
                },
                index_build: if offline_indexes {
                    crate::ledger::IndexBuildMode::Offline {
                        workers: index_build_workers,
                        memory_kb: index_build_memory,
                    }
                } else {
                    crate::ledger::IndexBuildMode::Concurrent
                },
            };
            let ledger =
                crate::ledger::Ledger::connect_migrate(&config.database_url, options).await?;
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
            let pending = ledger.pending_share_hash_backfill().await;
            ledger.pool.close().await;
            let pending = pending?;
            // What the start gate required: only a backfill that permits
            // serving passes it pending, without 2.
            let ready = crate::ledger::schema_version_list(
                &crate::ledger::required_schema_versions(pending.is_some()),
            );
            match pending {
                None => println!(
                    "PRISM PostgreSQL schema migrations {ready} ready; database source: {source}"
                ),
                Some((next_seq, end_seq)) => println!(
                    "PRISM PostgreSQL schema migrations {ready} ready, and frontends may serve: migration 2's share-hash backfill is deferred with its recent range mapped, and the legacy shares from share_seq {next_seq} up to {end_seq} are not all mapped yet. Once frontends serve, run `qbit-prism-server backfill-share-hashes`, which maps them in throttled batches and records 2; database source: {source}"
                ),
            }
            Ok(())
        }
        Command::BackfillShareHashes {
            max_batch,
            statement_timeout_ms,
            duty_cycle,
        } => {
            let throttle = crate::ledger::ShareHashThrottle::new(
                max_batch,
                Duration::from_millis(statement_timeout_ms),
                duty_cycle,
            )?;
            let database_url = config::DatabaseConfig::url_from_env()?;
            // The start gate, as `migrate` passes it: a backfill that does not
            // permit serving is refused there, naming what to run instead.
            let ledger = crate::ledger::Ledger::connect_operator(&database_url, false).await?;
            let finished = ledger.backfill_share_hashes(&throttle).await;
            ledger.pool.close().await;
            let finished = finished?;
            // A database whose backfill finished already is a success too: a
            // retry after a lost reply finds 2 recorded.
            let (next_seq, end_seq) = finished.range.unzip();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "mapped": finished.mapped,
                    "next_seq": next_seq,
                    "end_seq": end_seq,
                    "seqs": finished.range.map_or(0, |(next, end)| end - next),
                    "elapsed_ms": u64::try_from(finished.elapsed.as_millis()).unwrap_or(u64::MAX),
                    "recorded": true,
                    "already_complete": finished.already_complete(),
                }))?
            );
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
            let config = Config::from_env()?;
            // #291: refused before the node or the database is reached.
            config.require_block_submission("broadcast-ctv refuses to run")?;
            let coordinator = Coordinator::new_tool(
                config,
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

/// What `signing-transition` prints, and all it does, without `--confirm`.
const SIGNING_TRANSITION_PLAN: &str = "\
signing-transition resets the pinned cluster fingerprint so that frontends with
new signing keys can pin theirs. Run it with the OLD key environment.

In one transaction, under the cluster row lock every configure takes, it checks:
  1. the cluster is not halted;
  2. the pinned fingerprint is set and is this environment's;
  3. every registered frontend is stopped, or its heartbeat is older than
     max(3 x PRISM_HEALTH_REFRESH_SECONDS, 15 seconds) by the database clock;
  4. no block candidate is pending, offer_reserved, offered or in reconciliation.
and then, in the same transaction:
  - records the old fingerprint, the old policy document with both old public
    keys, the instance rows and the payout revision in the immutable
    qbit_prism_signing_transitions journal;
  - sets qbit_prism_cluster.config_fingerprint to NULL.
The first frontend configured afterwards pins the new fingerprint.";

async fn signing_transition(confirm: bool) -> Result<()> {
    if !confirm {
        println!("{SIGNING_TRANSITION_PLAN}");
        bail!("nothing was checked or changed; rerun signing-transition with --confirm to perform the reset");
    }
    let config = Config::from_env()?;
    // The frontends' heartbeat cadence, through the reader they and
    // self-check use: run this with their environment.
    let refresh = crate::api::health_refresh_interval_from_env()?;
    let ledger = crate::ledger::Ledger::connect_operator(&config.database_url, false).await?;
    let result = ledger.transition_signing(&config, refresh).await;
    ledger.pool.close().await;
    println!("{}", serde_json::to_string_pretty(&result?)?);
    Ok(())
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
        FatalStateCommand::Clear {
            reason,
            timeout_seconds,
        } => {
            crate::ledger::require_operator_reason(&reason)?;
            let config = Config::from_env()?;
            let ledger =
                crate::ledger::Ledger::connect_operator(&config.database_url, false).await?;
            // The clear reconciles blocks: in dual-writer mode it must treat
            // peer and carry-free blocks as the frontends do.
            if let Some(dual) = &config.dual_writer {
                ledger.set_dual_writer_identity(dual.identity)?;
                ledger.set_extranonce2_size(config.extranonce2_size)?;
            }
            let result = ledger
                .clear_fatal_state_within(&config, &reason, Duration::from_secs(timeout_seconds))
                .await;
            ledger.pool.close().await;
            println!("{}", serde_json::to_string_pretty(&result?)?);
            Ok(())
        }
    }
}

/// 3.1 dual writer (CONTRACT D-9, D-16): reads only the database URL, so it
/// runs before any dual-writer setting exists. `set` personalises the
/// database as the given node (`Ledger::set_node_identity`) on the operator
/// connection, which writes no heartbeat; run again on the same node it
/// restores only what the database lost, and it refuses a database that is
/// the other node's. `repersonalise` makes a promoted physical copy of the
/// other node's database the given node's
/// (`Ledger::repersonalise_node_identity`) and adds what it did under
/// `repersonalised`. Each prints the identity and lineage that result as JSON.
async fn node_identity(command: NodeIdentityCommand) -> Result<()> {
    let url = config::DatabaseConfig::url_from_env()?;
    let ledger = crate::ledger::Ledger::connect_operator(&url, false).await?;
    let result = async {
        let node = |index: u8| {
            crate::node_identity::NodeIndex::from_index(index.into())
                .context("--index must be 0 (node A) or 1 (node B)")
        };
        let repersonalised = match command {
            NodeIdentityCommand::Show => None,
            NodeIdentityCommand::Set { index } => {
                ledger
                    .set_node_identity(node(index)?, "qbit-prism-server node-identity set")
                    .await?;
                None
            }
            NodeIdentityCommand::Repersonalise { index } => Some(
                ledger
                    .repersonalise_node_identity(
                        node(index)?,
                        "qbit-prism-server node-identity repersonalise",
                    )
                    .await?,
            ),
        };
        let mut report = json!({
            "schema": "qbit.prism.node-identity.v1",
            "identity": ledger.recorded_node_identity().await?,
            "lineage": ledger.node_lineage().await?,
        });
        if let Some(repersonalised) = repersonalised {
            report["repersonalised"] = serde_json::to_value(repersonalised)?;
        }
        Ok::<_, anyhow::Error>(report)
    }
    .await;
    ledger.pool.close().await;
    println!("{}", serde_json::to_string_pretty(&result?)?);
    Ok(())
}

/// `carry-owner`: every subcommand prints its report as JSON. `release` and
/// `transfer` fail, after printing, when a check refuses or without
/// `--confirm`.
async fn carry_owner(command: CarryOwnerCommand) -> Result<()> {
    use crate::carry_owner::{transfer, CarryOwnerSettings};
    let config = Config::from_env()?;
    let settings = CarryOwnerSettings::from_config(&config)?;
    let rpc = Rpc::new(
        config.rpc_url.clone(),
        config.rpc_user.clone(),
        config.rpc_password.clone(),
        config.rpc_timeout,
    )?;
    let ledger = crate::ledger::Ledger::connect_operator(&config.database_url, false).await?;
    if let Some(dual) = &config.dual_writer {
        ledger.set_dual_writer_identity(dual.identity)?;
        ledger.set_extranonce2_size(config.extranonce2_size)?;
    }
    let (result, writes) = match &command {
        CarryOwnerCommand::Status => (transfer::status(&ledger, &settings).await, false),
        CarryOwnerCommand::Release { reason, confirm } => {
            crate::ledger::require_operator_reason(reason)?;
            (
                transfer::release(
                    &ledger,
                    &settings,
                    &transfer::RpcChain(&rpc),
                    reason,
                    *confirm,
                )
                .await,
                true,
            )
        }
        CarryOwnerCommand::Transfer {
            reason,
            from_height,
            confirm,
        } => {
            crate::ledger::require_operator_reason(reason)?;
            let recognizer = crate::carry_owner::transfer::PoolRecognizer::new(
                &config.coinbase_tag,
                pool_fee_program(&config, &rpc).await?.as_deref(),
            )?;
            (
                transfer::transfer(
                    &ledger,
                    &settings,
                    &transfer::RpcChain(&rpc),
                    &recognizer,
                    *from_height,
                    config.candidate_orphan_confirmations,
                    reason,
                    *confirm,
                )
                .await,
                true,
            )
        }
    };
    ledger.pool.close().await;
    let report = result?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if writes {
        ensure!(
            report.passed(),
            "refused: a check failed; nothing was written"
        );
        ensure!(
            report.written.is_some(),
            "nothing was written; rerun with --confirm to write it"
        );
    }
    Ok(())
}

/// The pool-fee P2MR program, as `Coordinator::connect` resolves it: the
/// program of `PRISM_POOL_FEE_ADDRESS`, or the configured one.
async fn pool_fee_program(config: &Config, rpc: &Rpc) -> Result<Option<String>> {
    if let Some(address) = &config.fee_address {
        return Ok(Some(
            crate::coordinator::pool_fee_address_program(rpc, address).await?,
        ));
    }
    Ok(config
        .payout_policy
        .pool_fee_policy
        .as_ref()
        .map(|fee| fee.p2mr_program_hex.clone())
        .filter(|program| !program.is_empty()))
}

/// #664: every subcommand reads only the database URL, so a frontend-only
/// setting left invalid cannot stop an operator holding or releasing the
/// cluster. `show` opens it read-only and also reads a ledger from before
/// migration 023. `set` and `clear` check their reason before any connection
/// is opened, as `fatal-state clear` does, then take the operator connection,
/// which works on a halted cluster and writes no heartbeat; both print the
/// hold that results as JSON.
async fn submission_hold(command: SubmissionHoldCommand) -> Result<()> {
    let printed = |held: bool, fields: Value| -> Result<()> {
        let mut document = json!({"schema": "qbit.prism.submission-hold.v1", "held": held});
        if let (Some(document), Value::Object(fields)) = (document.as_object_mut(), fields) {
            document.extend(fields);
        }
        println!("{}", serde_json::to_string_pretty(&document)?);
        Ok(())
    };
    match command {
        SubmissionHoldCommand::Show => {
            let url =
                config::optional("PRISM_DATABASE_URL").context("PRISM_DATABASE_URL is required")?;
            let state = crate::ledger::Ledger::inspect_submission_hold(&url).await?;
            println!("{}", serde_json::to_string_pretty(&state)?);
            Ok(())
        }
        SubmissionHoldCommand::Set { reason } => {
            crate::ledger::require_operator_reason(&reason)?;
            let url = config::DatabaseConfig::url_from_env()?;
            let ledger = crate::ledger::Ledger::connect_operator(&url, false).await?;
            let result = ledger.set_submission_hold(&reason).await;
            ledger.pool.close().await;
            let (hold, newly_set) = result?;
            if !newly_set {
                eprintln!("the cluster already held block submission; its hold is unchanged");
            }
            printed(
                true,
                json!({"newly_set": newly_set, "reason": hold.reason, "set_at": hold.set_at, "set_by": hold.set_by}),
            )
        }
        SubmissionHoldCommand::Clear {
            reason,
            offer_pending_candidates,
        } => {
            crate::ledger::require_operator_reason(&reason)?;
            let url = config::DatabaseConfig::url_from_env()?;
            let ledger = crate::ledger::Ledger::connect_operator(&url, false).await?;
            let result = ledger
                .clear_submission_hold(&reason, offer_pending_candidates)
                .await;
            ledger.pool.close().await;
            let cleared = result?;
            if cleared.cleared.is_none() {
                eprintln!("the cluster held no block submission; nothing was changed");
            }
            printed(
                false,
                json!({"cleared": cleared.cleared, "pending_candidates": cleared.pending_candidates}),
            )
        }
    }
}

/// Every share-archive command runs as the operator against the primary, with
/// the frontends running, and closes its pool whichever way it ends. The
/// result is printed as JSON so a retention run is scriptable.
async fn share_archive(command: ShareArchiveCommand) -> Result<()> {
    use crate::ledger::archive;

    let config = config::DatabaseConfig::from_env()?;
    let ledger = crate::ledger::Ledger::connect_operator(&config.database_url, false).await?;
    let result = async {
        match command {
            ShareArchiveCommand::Plan { retention } => Ok(serde_json::to_value(
                archive::plan(&ledger, &retention.options()).await?,
            )?),
            ShareArchiveCommand::Seal { partition } => archive::seal(&ledger, &partition).await,
            ShareArchiveCommand::Archive {
                partition,
                dir,
                force,
            } => archive::archive(&ledger, &partition, &dir, force, &config.instance_id).await,
            ShareArchiveCommand::Verify { partition, dir } => {
                archive::verify(&ledger, &partition, &dir).await
            }
            ShareArchiveCommand::Detach {
                partition,
                retention,
            } => archive::detach(&ledger, &partition, &retention.options()).await,
            ShareArchiveCommand::Drop { partition, dir } => {
                archive::drop_partition(&ledger, &partition, &dir).await
            }
            ShareArchiveCommand::Restore {
                manifest,
                dir,
                attach,
            } => archive::restore(&ledger, &manifest, &dir, attach).await,
        }
    }
    .await;
    ledger.pool.close().await;
    println!("{}", serde_json::to_string_pretty(&result?)?);
    Ok(())
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
        CandidatesCommand::Abandon {
            block_hash,
            reason,
            unsafe_database_clock_expiry,
        } => {
            // Both inputs are checked before any connection is opened, in the
            // formats the row itself uses: `candidate_sha256 ~
            // '^[0-9a-f]{64}$'` for the hash, and `fatal-state clear`'s rule
            // for the reason, which lands in `last_error`.
            require_block_hash(&block_hash)?;
            crate::ledger::require_operator_reason(&reason)?;
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
            let takeover = takeover_rule(unsafe_database_clock_expiry);
            // #581: a claim is over once this command has watched it go
            // unrenewed for its whole lease. It waits that out, once per
            // version; a claim that changes meanwhile is live, and its
            // refusal is the answer.
            let mut waited_for: Option<String> = None;
            let outcome = loop {
                let outcome = ledger
                    .abandon_candidate(&block_hash, &reason, takeover)
                    .await;
                let Ok(refused) = &outcome else {
                    break outcome;
                };
                let Some(left) = observed_claim_wait(refused, &block_hash, &mut waited_for, None)
                else {
                    break outcome;
                };
                tokio::time::sleep(left).await;
            };
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
            unsafe_database_clock_expiry,
        } => {
            let takeover = takeover_rule(unsafe_database_clock_expiry);
            recover(block_hash, apply, timeout_seconds, takeover).await
        }
    }
}

/// The takeover rule the two candidate commands that take a claimed row
/// share (#581): an observed lease unless the operator names the unsafe one.
fn takeover_rule(unsafe_database_clock_expiry: bool) -> RecoveryTakeover {
    if unsafe_database_clock_expiry {
        RecoveryTakeover::DatabaseClock
    } else {
        RecoveryTakeover::Observed
    }
}

/// How long a candidate command waits before it retries a refusal of a claim
/// it times on its own clock (#581), or `None` when the refusal is the
/// answer: no timed claim, a claim whose version changed while the command
/// waited (the holder renewed, or a frontend took the row over, so it is
/// live), or a wait `deadline` leaves no room for. Announces the first wait.
fn observed_claim_wait(
    outcome: &Value,
    hash: &str,
    waited_for: &mut Option<String>,
    deadline: Option<tokio::time::Instant>,
) -> Option<Duration> {
    let (Some(left), Some(version)) = (
        outcome["lease_remaining_ms"].as_u64(),
        outcome["claim_version"].as_str(),
    ) else {
        return None;
    };
    let left = Duration::from_millis(left);
    let renewed = waited_for.as_deref().is_some_and(|seen| seen != version);
    let no_room = deadline.is_some_and(|deadline| tokio::time::Instant::now() + left >= deadline);
    if renewed || no_room {
        return None;
    }
    if waited_for.is_none() {
        println!(
            "candidate {hash} is claimed by {}; waiting {:.0} s for the claim to go unrenewed for its whole lease",
            outcome["claim_instance_id"].as_str().unwrap_or("unknown"),
            left.as_secs_f64().ceil()
        );
    }
    *waited_for = Some(version.to_owned());
    Some(left)
}

/// Print a candidate command's diagnostic and exit with its status.
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
async fn recover(
    hashes: Vec<String>,
    apply: bool,
    timeout_seconds: u64,
    takeover: RecoveryTakeover,
) -> Result<()> {
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
    // It lands blocks as `run` does, so it trims after them as `run` does.
    let landing_trim = crate::memory::landing_trim_from_env()?;
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
    coordinator.landing_trim.set_enabled(landing_trim);
    let outcome = apply_recovery(
        &coordinator,
        &plan.blocks,
        deadline,
        timeout_seconds,
        takeover,
    )
    .await;
    // A landing the deadline cut short may hold its connection until the
    // server abandons it, so the close is bounded as well.
    let _ = tokio::time::timeout(Duration::from_secs(5), coordinator.ledger.pool.close()).await;
    finish_recovery(outcome)
}

/// Report only after bounded claim and pool cleanup. A canceled rebuild
/// may still be running in spawn_blocking, so every stop must exit directly:
/// returning an error to main would wait for it during runtime shutdown.
fn finish_recovery(outcome: Result<(usize, usize), Stop>) -> Result<()> {
    match outcome {
        Ok((recovered, verified)) => {
            println!("recovered {recovered}, verified {verified} already complete");
            Ok(())
        }
        Err(Stop::Failure(error)) => refuse(1, format!("Error: {error:?}")),
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
/// machinery, reported with exit 1 after bounded cleanup.
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
    takeover: RecoveryTakeover,
) -> Result<(usize, usize), Stop> {
    let (mut recovered, mut verified) = (0, 0);
    for block in blocks {
        if block.complete {
            // Connection and earlier landings may outlive the plan's view
            // of the chain and accounting. Recheck at the point of use,
            // with the same read-only rules and whole-operation deadline.
            let checked = tokio::time::timeout_at(deadline, async {
                let reader = RecoveryReader::open(&coordinator.config.database_url).await?;
                let plan =
                    plan_recovery(&reader, &coordinator.rpc, std::slice::from_ref(&block.hash))
                        .await;
                reader.close().await;
                plan
            })
            .await;
            let checked = match checked {
                Ok(plan) => plan?,
                Err(_) => {
                    return Err(Stop::Exit(
                        11,
                        format!(
                            "recovery deadline of {timeout_seconds} seconds exceeded (verifying); candidate {} was not verified",
                            block.hash
                        ),
                    ))
                }
            };
            if let Some((code, message)) = checked.problems.into_iter().next() {
                return Err(Stop::Exit(code, message));
            }
            let Some(current) = checked.blocks.first().filter(|current| current.complete) else {
                return Err(Stop::Exit(
                    4,
                    format!(
                        "candidate {} is no longer complete; its state changed since planning, rerun recover to inspect it",
                        block.hash
                    ),
                ));
            };
            println!(
                "verified {} at height {}: already complete",
                current.hash, current.height
            );
            verified += 1;
            continue;
        }
        println!(
            "recovering {} at height {} from {}",
            block.hash, block.height, block.state
        );
        let claimed = claim_for_recovery(coordinator, &block.hash, deadline, takeover).await;
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

/// Claim one row for recovery. Another holder's claim is taken over, by
/// default, only after this process has watched it go unrenewed for its
/// whole lease (#581): a `claimed` refusal that names the time left is
/// waited out, once, when the deadline leaves room for it, and a holder that
/// renews meanwhile is live, so its refusal is the answer.
async fn claim_for_recovery(
    coordinator: &Coordinator,
    hash: &str,
    deadline: tokio::time::Instant,
    takeover: RecoveryTakeover,
) -> Result<RecoveryClaim> {
    let mut waited_for: Option<String> = None;
    loop {
        let claimed = coordinator
            .claim_candidate_for_recovery(hash, deadline, takeover)
            .await?;
        let RecoveryClaim::Refused(outcome) = &claimed else {
            return Ok(claimed);
        };
        let Some(left) = observed_claim_wait(outcome, hash, &mut waited_for, Some(deadline)) else {
            return Ok(claimed);
        };
        tokio::time::sleep(left).await;
    }
}

/// The exit status of a recovery that stopped after its claim. A typed stop
/// confirms cleanup left the row recoverable; other errors may include
/// unconfirmed cleanup and cannot assert the row's current disposition.
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
        None => Stop::Failure(error.context(format!("recovery of {hash} stopped"))),
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
            match outcome["lease_remaining_ms"].as_u64() {
                // #581: timed by this command, not the database clock.
                Some(left) => format!(
                    "candidate {hash} is held by {}, whose claim this command must watch go unrenewed for {} more seconds of its lease (database clock estimate: until {}); the deadline leaves no room for that, or the holder renewed while this command waited and is live. Retry with a longer --timeout-seconds, or once the holder has released it",
                    field("claim_instance_id"),
                    left.div_ceil(1000),
                    field("claim_expires_at")
                ),
                None => format!(
                    "candidate {hash} is held by {} until {}; retry after the claim expires",
                    field("claim_instance_id"),
                    field("claim_expires_at")
                ),
            },
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
            if outcome["lease_remaining_ms"].is_u64() {
                // #581: timed by this command, which waited and saw the
                // claim change, not by the database clock.
                format!(
                    "candidate {block_hash} is held by {}, whose claim changed while this command watched it (the holder renewed it, or a frontend took the row over), so it is live (database clock estimate: until {}); retry once the holder has released it",
                    field("claim_instance_id"),
                    field("claim_expires_at")
                )
            } else {
                format!(
                    "candidate {block_hash} is held by {} until {}; retry after the claim expires",
                    field("claim_instance_id"),
                    field("claim_expires_at")
                )
            },
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

/// What a healthcheck requires of the frontend it probes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HealthRule {
    /// The healthcheck subcommand, which container healthchecks run:
    /// readiness for a single writer as in 3.0, liveness for a dual-writer
    /// frontend (`readiness::liveness`).
    Container,
    /// `self-check`: readiness in either mode.
    Readiness,
}

async fn healthcheck(url: Option<String>, public_api: bool, rule: HealthRule) -> Result<()> {
    let (bind_name, port_name, default_port) = if public_api {
        ("PRISM_PUBLIC_API_BIND", "PRISM_PUBLIC_API_PORT", 3342u16)
    } else {
        ("PRISM_AUDIT_BIND", "PRISM_AUDIT_PORT", 3341u16)
    };
    let port = config::number(port_name, default_port)?;
    if url.is_none() && port == 0 && !public_api {
        // A dual-writer frontend's Stratum listeners refuse every connection
        // while it does not admit miners, catching up included, so they
        // cannot tell a live frontend from a dead one.
        ensure!(
            rule == HealthRule::Readiness || !config::flag("PRISM_DUAL_WRITER", false)?,
            "a dual-writer frontend's healthcheck reads /healthz on the operator listener: set \
             PRISM_AUDIT_PORT (its Stratum listeners refuse connections while it does not admit \
             miners)"
        );
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
    let status = response.status();
    // Read whole, so a dual-writer body can be told from a single writer's.
    let body = match response.bytes().await {
        Ok(body) => body,
        // As in 3.0, an unsuccessful status is the error, not the unread body.
        Err(error) => {
            ensure!(status.is_success(), "PRISM is unhealthy (HTTP {status})");
            return Err(error.into());
        }
    };
    match rule {
        HealthRule::Container => crate::readiness::liveness::container_health(status, &body),
        HealthRule::Readiness => {
            crate::readiness::liveness::readiness(status, serde_json::from_slice(&body))
        }
    }
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
    /// #291: `PRISM_BLOCK_SUBMIT_ENABLED` as this environment sets it, or
    /// `null` when the configuration could not be read. Each live frontend's
    /// own value is `block_submission_enabled` in its heartbeat below.
    block_submission: Option<config::BlockSubmission>,
    /// #664: the cluster's block submission hold, as `submission-hold show`
    /// prints it, or `null` when the database could not be read. A set hold
    /// holds every frontend, whatever its own setting above says; each live
    /// frontend's heartbeat carries the hold as it last read it in
    /// `block_submission_hold`.
    submission_hold: Option<Value>,
    health: Option<Value>,
    carry_forward_integrity: Option<Value>,
    durability: Option<Vec<(String, String)>>,
    /// #529: the found-block offer's failover standby, when its wait is on.
    #[serde(skip_serializing_if = "Option::is_none")]
    offer_standby: Option<crate::ledger::OfferStandbyReport>,
    audit_completeness: Option<AuditCompleteness>,
    /// Migration 2's share-hash backfill while `migrate --defer-share-hashes`
    /// has left it pending, and `backfill-share-hashes` has still to map the
    /// rest: its fence and cursor, or `unknown` when the database could not
    /// be read. Reported, never a failure; absent only when the read found
    /// nothing pending.
    #[serde(skip_serializing_if = "Option::is_none")]
    share_hash_backfill: Option<ShareHashBackfillReport>,
    live_instances: LiveInstancesReport,
}

async fn self_check() -> Result<()> {
    let mut report = SelfCheckReport {
        schema: "qbit.prism.self-check.v2",
        ok: false,
        instance_id: None,
        block_submission: None,
        submission_hold: None,
        health: None,
        carry_forward_integrity: None,
        durability: None,
        offer_standby: None,
        audit_completeness: None,
        share_hash_backfill: None,
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
        let submission = config.block_submission();
        if let Some(warning) = submission.warning {
            eprintln!("WARNING: {warning}");
        }
        report.block_submission = Some(submission);
        // Both samples are read-only and independent of the local startup
        // below: a node startup or refresh failure must hide neither the
        // cluster's heartbeats nor an unfinished historical import.
        let (instances, completeness, hold, backfill) = tokio::join!(
            live_instances(&config.database_url, freshness),
            sample_audit_completeness(&config.database_url),
            sample_submission_hold(&config.database_url),
            sample_share_hash_backfill(&config.database_url),
        );
        report.live_instances = instances;
        if let Some(ShareHashBackfillReport::Unknown { error }) = &backfill {
            eprintln!(
                "WARNING: migration 2's share-hash backfill could not be read ({error}), so \
                 self-check cannot tell whether it is pending. Read the cursor with `SELECT \
                 next_seq, end_seq FROM qbit_prism_share_hash_backfill`: the table exists only \
                 while the backfill is pending"
            );
        }
        if let Some(ShareHashBackfillReport::Pending(pending)) = &backfill {
            // Only a backfill that permits serving passes the start gate; any
            // other is refused below, and its refusal names the remedy.
            let remedy = if pending.permits_serving() {
                "Run `qbit-prism-server backfill-share-hashes` while frontends serve to map them \
                 and record migration 2; share-archive restore, detach and drop refuse until then"
            } else {
                "Every start refuses the database meanwhile, naming what to run"
            };
            eprintln!(
                "WARNING: migration 2's share-hash backfill is pending (share_hash_backfill_pending \
                 = {}): the legacy shares from share_seq {} up to {} ({} share_seq values) are \
                 not all mapped, and its cursor last moved at {}. {remedy}",
                pending
                    .fence
                    .map_or_else(|| "none".to_owned(), |fence| fence.to_string()),
                pending.next_seq,
                pending.end_seq,
                pending.remaining_seqs,
                pending.updated_at.to_rfc3339()
            );
        }
        report.share_hash_backfill = backfill;
        if let Some(hold) = hold.as_ref().filter(|hold| hold["held"] == true) {
            eprintln!(
                "WARNING: the cluster holds block submission, set by {} at {}: {}; no frontend \
                 offers a block or sends a fanout until `qbit-prism-server submission-hold clear`",
                hold["set_by"].as_str().unwrap_or("unknown"),
                hold["set_at"].as_str().unwrap_or("unknown"),
                hold["reason"].as_str().unwrap_or("unknown")
            );
        }
        report.submission_hold = hold;
        report.audit_completeness = completeness.as_ref().ok().copied();
        if let Some(completeness) = &report.audit_completeness {
            if config::production_mode()? {
                completeness.require_complete()?;
            }
        }
        // #525: a refused payout policy fails the check, but only after the
        // local checks have filled in the report.
        let pool_fee = config.ensure_pool_fee_settles_dust();
        // A failed heartbeat sample must not suppress the remaining local checks.
        let local = self_check_local(config, &mut report).await;
        pool_fee?;
        local?;
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

/// #664: the cluster's block submission hold, read-only and bounded like the
/// other samples; `None` when it could not be read.
async fn sample_submission_hold(database_url: &str) -> Option<Value> {
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::ledger::Ledger::inspect_submission_hold(database_url),
    )
    .await
    .ok()?
    .ok()
}

/// self-check's view of migration 2's share-hash backfill: pending, with
/// its fence and cursor, or unknown, with why it could not be read.
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ShareHashBackfillReport {
    Pending(crate::ledger::ShareHashBackfillPending),
    Unknown { error: String },
}

/// Migration 2's share-hash backfill while it is pending, read-only and
/// bounded by its read's own deadline; `None` only when the read found
/// nothing pending. A failed read is reported as unknown, never as done.
async fn sample_share_hash_backfill(database_url: &str) -> Option<ShareHashBackfillReport> {
    match crate::ledger::Ledger::inspect_share_hash_backfill(database_url).await {
        Ok(pending) => pending.map(ShareHashBackfillReport::Pending),
        Err(error) => Some(ShareHashBackfillReport::Unknown {
            error: reportable_error(&error),
        }),
    }
}

/// An error's text for an operator report, unless it is a connection or a
/// protocol failure, whose text can carry the database URL's parts.
fn reportable_error(error: &anyhow::Error) -> String {
    let unreported = error.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .is_some_and(|error| !matches!(error, sqlx::Error::Database(_)))
    });
    if unreported {
        "the database could not be read".to_owned()
    } else {
        format!("{error:#}")
    }
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

/// The statement timeout `self-check` gives the carry-forward integrity report
/// (#737), whatever PRISM_DATABASE_STATEMENT_TIMEOUT_MS gives its other
/// statements: about five times the minute the report takes at production
/// size. It holds no lock but its snapshot.
const SELF_CHECK_REPORT_TIMEOUT: Duration = Duration::from_secs(300);

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
    // #737: the report runs in a read-only transaction of its own, so its
    // statement timeout is its own too and ends with it.
    let mut tx = coordinator.ledger.pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await?;
    let mut integrity =
        crate::ledger::integrity_report_bounded(&mut tx, SELF_CHECK_REPORT_TIMEOUT).await?;
    tx.rollback().await?;
    // #478: the divergence line is reported, never a failure: its debt is an
    // accepted, bounded cost that exact accounting carries.
    integrity["payout_divergence"] =
        crate::ledger::payout_divergence_line(&coordinator.ledger.pool).await?;
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
    // #529: a configured wait needs a role that can read the standby's
    // position and exactly one streaming standby by that name, or it never
    // protects a found block.
    if let Some(wait) = &coordinator.config.offer_standby {
        let standby = coordinator.ledger.offer_standby_report(wait).await?;
        let usable = standby.ensure_usable();
        report.offer_standby = Some(standby);
        usable?;
    }
    healthcheck(None, false, HealthRule::Readiness).await?;
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

    #[test]
    fn recovery_stops_exit_with_an_active_blocking_rebuild() {
        const CHILD: &str = "QBIT_RECOVERY_EXIT_TEST";
        const TEST: &str =
            "tools::configuration_tests::recovery_stops_exit_with_an_active_blocking_rebuild";
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        if let Ok(mode) = std::env::var(CHILD) {
            let result = runtime.block_on(async {
                let (started, ready) = tokio::sync::oneshot::channel();
                let build = tokio_util::task::AbortOnDropHandle::new(
                    tokio::task::spawn_blocking(move || {
                        started.send(()).unwrap();
                        // A started blocking task cannot be aborted. Only
                        // process exit can end this deliberately stalled build.
                        loop {
                            std::thread::park();
                        }
                    }),
                );
                ready.await.unwrap();
                drop(build);
                let stop = if mode == "deadline" {
                    recovery_stop(RecoveryStop::Deadline.into(), "test-block", 1)
                } else {
                    recovery_stop(
                        anyhow::anyhow!("cleanup unavailable").context(
                            "releasing the recovery claim for test-block failed after the recovery deadline expired; the claim may remain until its lease expires",
                        ),
                        "test-block",
                        1,
                    )
                };
                finish_recovery(Err(stop))
            });
            // Reproduce main's runtime drop if recovery returns an error.
            drop(runtime);
            result.unwrap();
            panic!("recovery must exit the subprocess");
        }
        for (mode, code, message) in [
            ("deadline", 11, "its claim released"),
            (
                "cleanup-failure",
                1,
                "the claim may remain until its lease expires",
            ),
        ] {
            let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
            child
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD, mode)
                .kill_on_drop(true);
            let output = runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), child.output()).await
                })
                .unwrap_or_else(|_| panic!("{mode} waited for the blocking rebuild"))
                .unwrap();
            let error = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.code(), Some(code), "{error}");
            assert!(error.contains(message), "{error}");
            if mode == "cleanup-failure" {
                assert!(error.starts_with("Error: "), "{error}");
                assert!(error.contains("cleanup unavailable"), "{error}");
                assert!(!error.contains("its claim released"), "{error}");
            }
        }
    }

    /// The recover allowlist and deadline are checked where clap parses them
    /// and, for the bounds clap cannot express, at the top of `recover`.
    #[test]
    fn abandon_waits_out_an_observed_lease_unless_named_unsafe() {
        let hash = "ab".repeat(32);
        let parse = |extra: &[&str]| {
            let mut args = vec![
                "prism",
                "candidates",
                "abandon",
                "--block-hash",
                &hash,
                "--reason",
                "superseded",
            ];
            args.extend_from_slice(extra);
            let Some(Command::Candidates {
                command:
                    CandidatesCommand::Abandon {
                        unsafe_database_clock_expiry,
                        ..
                    },
            }) = Cli::try_parse_from(args).unwrap().command
            else {
                panic!("wrong command");
            };
            takeover_rule(unsafe_database_clock_expiry)
        };
        assert_eq!(parse(&[]), RecoveryTakeover::Observed, "#581's default");
        assert_eq!(
            parse(&["--unsafe-database-clock-expiry"]),
            RecoveryTakeover::DatabaseClock
        );
    }

    #[test]
    fn a_timed_claim_is_waited_out_once_per_version_and_never_past_a_deadline() {
        let refusal = |version: &str| json!({"outcome": "claimed", "claim_instance_id": "a", "lease_remaining_ms": 1500, "claim_version": version});
        let hash = "ab".repeat(32);
        let mut waited_for = None;
        assert_eq!(
            observed_claim_wait(&refusal("t#0"), &hash, &mut waited_for, None),
            Some(Duration::from_millis(1500))
        );
        // The same version again: time left is waited out again.
        assert_eq!(
            observed_claim_wait(&refusal("t#0"), &hash, &mut waited_for, None),
            Some(Duration::from_millis(1500))
        );
        // A renewal since is a live holder: the refusal is the answer.
        assert_eq!(
            observed_claim_wait(&refusal("t#1"), &hash, &mut waited_for, None),
            None
        );
        let soon = tokio::time::Instant::now() + Duration::from_millis(1000);
        assert_eq!(
            observed_claim_wait(&refusal("t#0"), &hash, &mut None, Some(soon)),
            None,
            "a wait the deadline cannot fit"
        );
        let untimed = json!({"outcome": "claimed", "claim_instance_id": "a", "claim_expires_at": "2026-01-01T00:00:00+00:00"});
        assert_eq!(
            observed_claim_wait(&untimed, &hash, &mut None, None),
            None,
            "the database clock's refusal is final"
        );
        let (code, message) = abandon_report(&refusal("t#1"), &hash, "superseded").unwrap();
        assert_eq!(code, 5);
        assert!(
            message.contains("whose claim changed while this command watched it"),
            "{message}"
        );
        let (code, message) = abandon_report(&untimed, &hash, "superseded").unwrap();
        assert_eq!(code, 5);
        assert!(
            message.contains("retry after the claim expires"),
            "{message}"
        );
    }

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
            "--unsafe-database-clock-expiry",
        ])
        .unwrap();
        let Some(Command::Candidates {
            command:
                CandidatesCommand::Recover {
                    block_hash,
                    apply,
                    timeout_seconds,
                    unsafe_database_clock_expiry,
                },
        }) = parsed.command
        else {
            panic!("wrong command");
        };
        assert_eq!(block_hash, vec![hash.clone(), "cd".repeat(32)]);
        assert!(apply);
        assert_eq!(timeout_seconds, 3600);
        assert!(unsafe_database_clock_expiry);
        let Some(Command::Candidates {
            command:
                CandidatesCommand::Recover {
                    apply,
                    timeout_seconds,
                    unsafe_database_clock_expiry,
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
        assert!(
            !unsafe_database_clock_expiry,
            "takeover waits out an observed lease by default (#581)"
        );
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

    /// `--defer-share-hashes` is `migrate`'s alone, and off unless given:
    /// plain `migrate` finishes the backfill, as before.
    #[test]
    fn migrate_defers_share_hashes_only_when_asked() {
        let parse = |args: &[&str]| {
            let Some(Command::Migrate {
                defer_share_hashes, ..
            }) = Cli::try_parse_from(args).unwrap().command
            else {
                panic!("wrong command");
            };
            defer_share_hashes
        };
        assert!(!parse(&["prism", "migrate"]));
        assert!(parse(&["prism", "migrate", "--defer-share-hashes"]));
        for args in [
            vec!["prism", "migrate", "--defer-share-hashes=false"],
            vec!["prism", "self-check", "--defer-share-hashes"],
            vec!["prism", "run", "--defer-share-hashes"],
        ] {
            assert!(Cli::try_parse_from(&args).is_err(), "{args:?} was accepted");
        }
    }

    /// The W1 cutover's migrate step, `migrate --defer-share-hashes
    /// --offline-indexes`, takes both modes in one run.
    #[test]
    fn migrate_takes_the_deferred_backfill_and_the_offline_indexes_together() {
        let Some(Command::Migrate {
            defer_share_hashes,
            offline_indexes,
            index_build_workers,
            index_build_memory,
        }) = Cli::try_parse_from([
            "prism",
            "migrate",
            "--defer-share-hashes",
            "--offline-indexes",
            "--index-build-workers",
            "6",
            "--index-build-memory",
            "1GB",
        ])
        .unwrap()
        .command
        else {
            panic!("wrong command");
        };
        assert!(defer_share_hashes);
        assert!(offline_indexes);
        assert_eq!(index_build_workers, 6);
        assert_eq!(index_build_memory, Some(1024 * 1024));
    }

    /// `backfill-share-hashes` throttles at 5,000 `share_seq`, 2 s and half
    /// the time unless told otherwise, and refuses a throttle the runner
    /// would not accept before anything connects.
    #[test]
    fn backfill_share_hashes_parses_its_throttle() {
        let parse = |args: &[&str]| {
            let Some(Command::BackfillShareHashes {
                max_batch,
                statement_timeout_ms,
                duty_cycle,
            }) = Cli::try_parse_from(args).unwrap().command
            else {
                panic!("wrong command");
            };
            (max_batch, statement_timeout_ms, duty_cycle)
        };
        assert_eq!(
            parse(&["prism", "backfill-share-hashes"]),
            (5_000, 2_000, 0.5)
        );
        assert_eq!(
            parse(&[
                "prism",
                "backfill-share-hashes",
                "--max-batch",
                "50000",
                "--statement-timeout-ms",
                "250",
                "--duty-cycle",
                "1",
            ]),
            (50_000, 250, 1.0)
        );
        assert_eq!(
            parse(&["prism", "backfill-share-hashes", "--duty-cycle", "0.01"]).2,
            0.01
        );
        assert_eq!(
            parse(&[
                "prism",
                "backfill-share-hashes",
                "--statement-timeout-ms",
                "5000"
            ])
            .1,
            5_000
        );
        for args in [
            vec!["prism", "backfill-share-hashes", "--max-batch", "0"],
            vec!["prism", "backfill-share-hashes", "--max-batch", "50001"],
            vec![
                "prism",
                "backfill-share-hashes",
                "--statement-timeout-ms",
                "0",
            ],
            vec![
                "prism",
                "backfill-share-hashes",
                "--statement-timeout-ms",
                "5001",
            ],
            vec!["prism", "backfill-share-hashes", "--duty-cycle", "0"],
            vec!["prism", "backfill-share-hashes", "--duty-cycle", "0.009"],
            vec!["prism", "backfill-share-hashes", "--duty-cycle", "1.5"],
            vec!["prism", "backfill-share-hashes", "--duty-cycle", "NaN"],
            vec!["prism", "backfill-share-hashes", "--duty-cycle", "half"],
            vec!["prism", "migrate", "--max-batch", "10"],
        ] {
            assert!(Cli::try_parse_from(&args).is_err(), "{args:?} was accepted");
        }
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

    /// `--index-build-memory` takes maintenance_work_mem as PostgreSQL
    /// writes it, in kilobytes, and refuses what PostgreSQL would refuse.
    #[test]
    fn index_build_memory_is_parsed_and_bounded_as_postgres_does() {
        for (value, kilobytes) in [
            ("2GB", 2 * 1024 * 1024),
            ("512MB", 512 * 1024),
            (" 64 MB ", 64 * 1024),
            ("1.5GB", 1536 * 1024),
            ("1048576", 1024 * 1024),
            ("1048576kB", 1024 * 1024),
            ("1048576B", 1024),
            ("1TB", 1024 * 1024 * 1024),
            ("2147483647kB", 2_147_483_647),
        ] {
            assert_eq!(parse_index_build_memory(value), Ok(kilobytes), "{value}");
        }
        for value in ["2gb", "2 G", "2GiB", "", "MB", "-1GB", "2.5.1GB"] {
            assert_eq!(
                parse_index_build_memory(value),
                Err(INDEX_BUILD_MEMORY_SYNTAX.to_owned()),
                "{value}"
            );
        }
        for value in ["1023kB", "512kB", "2TB", "3000000000"] {
            assert!(
                parse_index_build_memory(value)
                    .unwrap_err()
                    .contains("between 1MB and 2147483647kB"),
                "{value}"
            );
        }
    }
}
