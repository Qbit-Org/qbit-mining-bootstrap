//! `import-audits` and SETTLEMENT_LOCK (#765).
//!
//! On the live pair, `import-audits` held SETTLEMENT_LOCK for 90 s on a
//! legacy audit with a ~112 MB canonical body. PostgreSQL showed the session
//! `idle in transaction`, waiting on `ClientRead`, for the whole hold, then
//! "incomplete message from client" at the kill: the lock was held while the
//! client was still sending the statement. These tests put the importer's
//! connections behind a relay that slows or stalls what the client sends,
//! which leaves the server in that same state, and check what a
//! SETTLEMENT_LOCK waiter sees, how each stall ends, and that a re-run
//! resumes.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test import_audits_settlement_lock
//! ```
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, AuditBundle, FoundBlock, PayoutPolicy};
use qbit_prism_server::ledger::{
    audit_canonical_bytes, ImportAuditRow, ImportAuditsOptions, ImportStopped, Ledger,
};
use qbit_prism_test_gate as gate;
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, PgPool};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// The ledger's settlement key (`ledger.rs`).
const SETTLEMENT_LOCK: i64 = 0x505249534d000003;
/// The longest a SETTLEMENT_LOCK waiter may wait while a legacy audit is
/// imported beside it. The relay makes sending the large row take several
/// times longer, so a hold that covers the send cannot pass.
const MAX_SETTLEMENT_WAIT: Duration = Duration::from_secs(3);
/// What the relay lets through per second while it throttles: the large row
/// then takes several seconds to send, as on the pair, however fast the
/// runner is.
const THROTTLED_BYTES_PER_SECOND: u64 = 16 * 1024 * 1024;
/// The short bounds the stall tests run with.
const TEST_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const TEST_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
/// Slack for a bound to take effect on a loaded runner: the server checks
/// its timeouts promptly, but the client sees the result through the relay.
const SLACK: Duration = Duration::from_secs(8);

fn keys() -> (ManifestSigningKey, ManifestSigningKey) {
    (
        ManifestSigningKey::from_seed_hex(&"42".repeat(32)).unwrap(),
        ManifestSigningKey::from_seed_hex(&"43".repeat(32)).unwrap(),
    )
}

fn ledger_public_key() -> String {
    keys().1.public_key_hex()
}

fn share(id: u64) -> AcceptedShare {
    AcceptedShare {
        share_seq: id + 1,
        share_id: format!("worker:{id:064x}"),
        miner_id: format!("miner-{:02}", id % 16),
        order_key: format!("miner-{:02}", id % 16),
        p2mr_program_hex: format!("{:064x}", id % 16 + 1),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 1,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

/// A verifiable legacy audit of `shares` shares and `witness_leaves`
/// witness Merkle leaves, every share counted.
fn legacy_bundle(shares: u64, witness_leaves: u64) -> Result<AuditBundle> {
    let (manifest_key, ledger_key) = keys();
    let leaves = (0..witness_leaves)
        .map(|leaf| {
            format!(
                "{:064x}",
                u128::from(leaf).wrapping_mul(0x9e37_79b9_7f4a_7c15) + 1
            )
        })
        .collect();
    Ok(qbit_prism::build_audit_bundle_with_coinbase_options(
        (0..shares).map(share).collect(),
        FoundBlock {
            block_height: 100,
            coinbase_value_sats: 5_000_000_000,
            network_difficulty: u128::from(shares.div_ceil(8)),
            anchor_job_issued_at_ms: 1,
        },
        Vec::new(),
        PayoutPolicy::day_one_default(),
        None,
        leaves,
        &manifest_key,
        &ledger_key,
    )?)
}

/// A legacy row as 2.x left it: the body in a file named by `body_uri`, no
/// canonical bytes and no share snapshot. Returns the block hash and the
/// canonical body's SHA-256.
async fn seed_legacy_row(
    pool: &PgPool,
    directory: &Path,
    bundle: &AuditBundle,
    nonce: u8,
) -> Result<(String, String)> {
    let report =
        qbit_prism::verify_audit_bundle_with_ledger_public_key(bundle, &ledger_public_key())?;
    let block_hash = format!("{nonce:02x}").repeat(32);
    let path = directory.join(format!("legacy-audit-{block_hash}.json"));
    let mut file = std::io::BufWriter::new(std::fs::File::create(&path)?);
    serde_json::to_writer(&mut file, bundle)?;
    file.flush()?;
    drop(file);
    sqlx::query(
        "INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,\
         payout_manifest_sha256) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(&block_hash)
    .bind(i64::try_from(report.block_height)?)
    .bind("22".repeat(32))
    .bind(&report.coinbase_txid)
    .bind(&report.coinbase_manifest_sha256_hex)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,\
         coinbase_tx_hex,body_uri) VALUES($1,NULL,$2,$3,$4)",
    )
    .bind(&block_hash)
    .bind(&report.audit_bundle_sha256_hex)
    .bind(&report.coinbase_tx_hex)
    .bind(path.to_str().context("non-UTF-8 temporary path")?)
    .execute(pool)
    .await?;
    Ok((block_hash, report.audit_bundle_sha256_hex))
}

/// When the relay stops forwarding what a client sends.
enum Trigger {
    Never,
    /// Once this many client bytes in total have been forwarded.
    AtTotal(u64),
    /// Before the first client bytes that contain this.
    Before(&'static [u8]),
}

/// What the relay does to the bytes a client sends; the server's replies
/// always pass straight through.
struct RelayControl {
    /// Bytes per second let through, or 0 for no limit.
    rate: AtomicU64,
    /// Client bytes forwarded so far, every connection together.
    forwarded: AtomicU64,
    trigger: Mutex<Trigger>,
    /// `true` once the trigger has fired, until `release`. The connection it
    /// fired on holds back what its client sends from then on, as a stalled
    /// client would; the relay keeps reading those bytes, so a client that
    /// closes its socket still reaches the server as a closed connection.
    /// Other connections pass.
    stall: watch::Sender<bool>,
}

impl RelayControl {
    /// How much of `chunk` may be forwarded, and whether this chunk fired
    /// the trigger, which stalls its connection after that much.
    fn admit(&self, chunk: &[u8]) -> (usize, bool) {
        let mut trigger = self.trigger.lock().unwrap();
        let admitted = match *trigger {
            Trigger::Never => return (chunk.len(), false),
            Trigger::AtTotal(total) => {
                let room = total.saturating_sub(self.forwarded.load(Ordering::SeqCst));
                if (chunk.len() as u64) < room {
                    return (chunk.len(), false);
                }
                usize::try_from(room).unwrap()
            }
            Trigger::Before(pattern) => {
                if !chunk.windows(pattern.len()).any(|window| window == pattern) {
                    return (chunk.len(), false);
                }
                0
            }
        };
        *trigger = Trigger::Never;
        self.stall.send_replace(true);
        (admitted, true)
    }

    fn stall_after(&self, bytes: u64) {
        let total = self.forwarded.load(Ordering::SeqCst) + bytes;
        *self.trigger.lock().unwrap() = Trigger::AtTotal(total);
    }

    fn stall_before(&self, pattern: &'static [u8]) {
        *self.trigger.lock().unwrap() = Trigger::Before(pattern);
    }

    fn stalled(&self) -> bool {
        *self.stall.borrow()
    }

    fn release(&self) {
        *self.trigger.lock().unwrap() = Trigger::Never;
        self.stall.send_replace(false);
    }
}

/// A TCP relay in front of the test database, for the importer's
/// connections only. When the server closes a connection, the relay closes
/// the client's, and the other way round, as a direct connection would.
struct Relay {
    url: String,
    control: Arc<RelayControl>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Relay {
    async fn start(database_url: &str) -> Result<Self> {
        let mut url = url::Url::parse(database_url)?;
        let upstream = format!(
            "{}:{}",
            url.host_str()
                .context("the test database URL has no host")?,
            url.port().unwrap_or(5432)
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        url.set_port(Some(listener.local_addr()?.port()))
            .map_err(|()| anyhow::anyhow!("cannot set the relay port"))?;
        let control = Arc::new(RelayControl {
            rate: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
            trigger: Mutex::new(Trigger::Never),
            stall: watch::channel(false).0,
        });
        let shared = control.clone();
        let task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = tokio::net::TcpStream::connect(&upstream).await else {
                    continue;
                };
                let _ = (client.set_nodelay(true), server.set_nodelay(true));
                let (client_read, mut client_write) = client.into_split();
                let (mut server_read, server_write) = server.into_split();
                let (closed, closing) = watch::channel(false);
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut server_read, &mut client_write).await;
                    let _ = client_write.shutdown().await;
                    closed.send_replace(true);
                });
                tokio::spawn(pump(shared.clone(), client_read, server_write, closing));
            }
        });
        Ok(Self {
            url: url.into(),
            control,
            task,
        })
    }
}

/// Client to server: forward, throttle, or hold back once this connection
/// has stalled.
async fn pump(
    control: Arc<RelayControl>,
    mut client: tokio::net::tcp::OwnedReadHalf,
    mut server: tokio::net::tcp::OwnedWriteHalf,
    mut closed: watch::Receiver<bool>,
) {
    let mut stall = control.stall.subscribe();
    let mut stalled = false;
    let mut buffer = vec![0u8; 64 * 1024];
    let mut held = Vec::new();
    loop {
        if stalled && !control.stalled() {
            stalled = false;
            if server.write_all(&held).await.is_err() {
                return;
            }
            control
                .forwarded
                .fetch_add(held.len() as u64, Ordering::SeqCst);
            held.clear();
        }
        let read = tokio::select! {
            read = client.read(&mut buffer) => match read {
                Ok(0) | Err(_) => return,
                Ok(read) => read,
            },
            _ = closed.wait_for(|closed| *closed) => return,
            _ = stall.wait_for(|stalled| !*stalled), if stalled => continue,
        };
        let chunk = &buffer[..read];
        let admitted = if stalled {
            0
        } else {
            let (admitted, fired) = control.admit(chunk);
            stalled = fired;
            admitted
        };
        if admitted > 0 {
            let started = Instant::now();
            if server.write_all(&chunk[..admitted]).await.is_err() {
                return;
            }
            control
                .forwarded
                .fetch_add(admitted as u64, Ordering::SeqCst);
            let rate = control.rate.load(Ordering::SeqCst);
            if rate > 0 {
                let budget = Duration::from_secs_f64(admitted as f64 / rate as f64);
                tokio::time::sleep(budget.saturating_sub(started.elapsed())).await;
            }
        }
        held.extend_from_slice(&chunk[admitted..]);
    }
}

/// Takes and releases SETTLEMENT_LOCK over and over, on its own direct
/// connection, until stopped; returns the longest wait.
struct SettlementWaiter {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Result<Vec<Duration>>>,
}

impl SettlementWaiter {
    async fn start(database_url: &str) -> Result<Self> {
        let mut connection = PgConnection::connect(database_url).await?;
        // A hold this test fails on must show as a long wait, not an error.
        sqlx::query("SELECT set_config('lock_timeout','600s',false)")
            .execute(&mut connection)
            .await?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            let mut waits = Vec::new();
            while !stopped.load(Ordering::SeqCst) {
                let mut tx = connection.begin().await?;
                let asked = Instant::now();
                sqlx::query("SELECT pg_advisory_xact_lock($1)")
                    .bind(SETTLEMENT_LOCK)
                    .execute(&mut *tx)
                    .await?;
                waits.push(asked.elapsed());
                tx.commit().await?;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            connection.close().await?;
            Ok(waits)
        });
        Ok(Self { stop, task })
    }

    async fn finish(self) -> Result<Duration> {
        self.stop.store(true, Ordering::SeqCst);
        let waits = tokio::time::timeout(Duration::from_secs(60), self.task)
            .await
            .context("the SETTLEMENT_LOCK waiter never finished")???;
        ensure!(
            !waits.is_empty(),
            "the SETTLEMENT_LOCK waiter never took the lock"
        );
        Ok(waits.into_iter().max().unwrap_or_default())
    }
}

struct Fixture {
    database: FixtureDatabase,
    /// Migrated the fixture database and seeds it; the importer under test
    /// is another ledger, behind the relay.
    ledger: Ledger,
    relay: Relay,
    importer: Option<Ledger>,
}

impl Fixture {
    /// Must run on the test thread: the gate names the test from it.
    async fn open() -> Result<Option<Self>> {
        let Some(raw) = gate::database_url(gate::site!())? else {
            return Ok(None);
        };
        let database = FixtureDatabase::open(&raw, "prism_import765_").await?;
        let opened = async {
            let ledger = Ledger::connect(&database.url, "import-765".into(), 4, true).await?;
            let relay = Relay::start(&database.url).await?;
            let importer =
                Ledger::connect_tool(&relay.url, "import-765-tool".into(), 4, false, None).await?;
            anyhow::Ok((ledger, relay, importer))
        }
        .await;
        match opened {
            Ok((ledger, relay, importer)) => Ok(Some(Self {
                database,
                ledger,
                relay,
                importer: Some(importer),
            })),
            Err(error) => Err(database.abandon(error).await),
        }
    }

    fn importer(&self) -> &Ledger {
        self.importer.as_ref().unwrap()
    }

    /// An importer whose connections to the relay are plaintext even where
    /// the gate's URL asks for TLS, for the relay's pattern triggers.
    async fn plaintext_importer(&self) -> Result<Ledger> {
        let mut url = url::Url::parse(&self.relay.url)?;
        let query: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(key, _)| !key.starts_with("ssl"))
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(query)
            .append_pair("sslmode", "disable");
        Ledger::connect_tool(url.as_str(), "import-765-plain".into(), 4, false, None).await
    }

    async fn close(mut self, result: Result<()>) -> Result<()> {
        self.relay.control.release();
        if let Some(importer) = self.importer.take() {
            importer.pool.close().await;
        }
        self.ledger.pool.close().await;
        self.database.close(result).await
    }

    async fn stored_digest(&self, block_hash: &str) -> Result<Option<String>> {
        Ok(audit_canonical_bytes(&self.ledger.pool, block_hash)
            .await?
            .map(|bytes| hex::encode(Sha256::digest(bytes))))
    }

    /// Sessions of this fixture's database still inside a transaction,
    /// other than this check's own.
    async fn open_transactions(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() \
             AND pid<>pg_backend_pid() AND xact_start IS NOT NULL",
        )
        .fetch_one(&self.ledger.pool)
        .await?)
    }

    /// Wait until no session but this check's own holds a transaction open.
    async fn transactions_end(&self, within: Duration) -> Result<()> {
        let deadline = Instant::now() + within;
        loop {
            let open = self.open_transactions().await?;
            if open == 0 {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "{open} session(s) still hold a transaction open after {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn options(stop: Option<watch::Receiver<bool>>) -> ImportAuditsOptions {
    ImportAuditsOptions {
        write_timeout: TEST_WRITE_TIMEOUT,
        lock_timeout: TEST_LOCK_TIMEOUT,
        stop,
    }
}

/// The relay has stalled and the importer has sent it more than it let
/// through, so the server is waiting on the client mid-statement.
async fn stalled_mid_write(relay: &Relay) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        while !relay.control.stalled() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the relay never stalled")
}

/// The pair's row: a canonical body over 100 MB, with a witness-leaves
/// array of hundreds of thousands of entries bound beside it. Sending it
/// through the throttled relay takes several seconds; a SETTLEMENT_LOCK
/// waiter must not wait for that, and the lock is held only for the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_legacy_audit_is_sent_without_holding_settlement_lock() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = large_row(&fixture).await;
    fixture.close(result).await
}

async fn large_row(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    // About 107 MB canonical, 37 MB of it the witness leaves.
    let bundle = tokio::task::spawn_blocking(|| legacy_bundle(100_000, 560_000)).await??;
    let canonical = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
    ensure!(
        canonical.len() >= 100 * 1024 * 1024,
        "the synthetic body is {} bytes, under 100 MiB",
        canonical.len()
    );
    drop(canonical);
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x7a).await?;
    drop(bundle);
    let waiter = SettlementWaiter::start(&fixture.database.url).await?;
    fixture
        .relay
        .control
        .rate
        .store(THROTTLED_BYTES_PER_SECOND, Ordering::SeqCst);
    let mut rows = Vec::new();
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &ImportAuditsOptions::default(),
            |row| rows.push(row.clone()),
        )
        .await;
    fixture.relay.control.rate.store(0, Ordering::SeqCst);
    let longest_wait = waiter.finish().await?;
    println!(
        "longest SETTLEMENT_LOCK wait {:.3} s; {}",
        longest_wait.as_secs_f64(),
        rows.iter()
            .map(ImportAuditRow::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    );
    ensure!(imported? == 1, "the large legacy audit was not imported");
    let [row] = &rows[..] else {
        anyhow::bail!("expected one row report, got {}", rows.len());
    };
    ensure!(
        fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()),
        "the stored canonical bytes are not the legacy body"
    );
    // The relay made the write slow, so a lock held across it would show.
    ensure!(
        row.write > MAX_SETTLEMENT_WAIT,
        "the throttled write took only {:?}; the test no longer proves anything",
        row.write
    );
    ensure!(
        row.parameter_bytes[5] > 30 * 1024 * 1024 && row.parameter_bytes[8] >= 100 * 1024 * 1024,
        "the leaves or the body bound were smaller than intended: {row}"
    );
    ensure!(
        row.lock_held < MAX_SETTLEMENT_WAIT,
        "the importer held SETTLEMENT_LOCK for {:?}",
        row.lock_held
    );
    ensure!(
        longest_wait < MAX_SETTLEMENT_WAIT,
        "a SETTLEMENT_LOCK waiter waited {:.1} s beside the import (bound {MAX_SETTLEMENT_WAIT:?})",
        longest_wait.as_secs_f64()
    );
    Ok(())
}

/// A statement that stops arriving mid-send (the pair's state) is ended by
/// the server within the write timeout. SETTLEMENT_LOCK was never taken,
/// the row is rolled back, the error names it, and a re-run imports it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_that_stalls_mid_send_is_rolled_back_within_the_write_timeout() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stalled_write(&fixture).await;
    fixture.close(result).await
}

async fn stalled_write(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(2_000, 20_000)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x51).await?;
    let waiter = SettlementWaiter::start(&fixture.database.url).await?;
    fixture.relay.control.stall_after(256 * 1024);
    let started = Instant::now();
    let failed = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await;
    let elapsed = started.elapsed();
    let longest_wait = waiter.finish().await?;
    let error = format!("{:#}", failed.err().context("a stalled write imported")?);
    println!("stalled write failed after {elapsed:?}: {error}");
    ensure!(
        fixture.relay.control.stalled(),
        "the relay never stalled the write"
    );
    ensure!(
        error.contains(&block_hash),
        "the error does not name the row: {error}"
    );
    ensure!(
        elapsed < TEST_WRITE_TIMEOUT + SLACK,
        "the stalled write took {elapsed:?} to fail (write timeout {TEST_WRITE_TIMEOUT:?})"
    );
    ensure!(
        longest_wait < MAX_SETTLEMENT_WAIT,
        "a SETTLEMENT_LOCK waiter waited {longest_wait:?} during a write that never took the lock"
    );
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the stalled row was stored"
    );
    fixture.transactions_end(SLACK).await?;
    fixture.relay.control.release();
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// A client that stops between its statements while it holds
/// SETTLEMENT_LOCK loses its session, and the lock, within the lock timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_stalls_holding_settlement_lock_loses_it_within_the_lock_timeout(
) -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stalled_under_lock(&fixture).await;
    fixture.close(result).await
}

async fn stalled_under_lock(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x52).await?;
    let importer = fixture.plaintext_importer().await?;
    let waiter = SettlementWaiter::start(&fixture.database.url).await?;
    // SQLx sends COMMIT as text, whatever the connection has prepared, after
    // the lock is granted and the write guard has run.
    fixture.relay.control.stall_before(b"COMMIT");
    let started = Instant::now();
    let failed = importer
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await;
    let elapsed = started.elapsed();
    let longest_wait = waiter.finish().await?;
    importer.pool.close().await;
    let error = format!("{:#}", failed.err().context("a stalled commit imported")?);
    println!(
        "stall under the lock failed after {elapsed:?}, longest wait {longest_wait:?}: {error}"
    );
    ensure!(
        fixture.relay.control.stalled(),
        "the relay never stalled the commit"
    );
    ensure!(
        error.contains(&block_hash),
        "the error does not name the row: {error}"
    );
    ensure!(
        longest_wait >= TEST_LOCK_TIMEOUT / 2,
        "the waiter waited only {longest_wait:?}: the stall did not happen under the lock"
    );
    ensure!(
        longest_wait < TEST_LOCK_TIMEOUT + MAX_SETTLEMENT_WAIT,
        "a SETTLEMENT_LOCK waiter waited {longest_wait:?} (lock timeout {TEST_LOCK_TIMEOUT:?})"
    );
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the stalled row was stored"
    );
    fixture.transactions_end(SLACK).await?;
    fixture.relay.control.release();
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// SETTLEMENT_LOCK held elsewhere for longer than the lock timeout fails the
/// row fast and rolls it back; the holder is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_settlement_lock_fails_the_row_within_the_lock_timeout() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = busy_lock(&fixture).await;
    fixture.close(result).await
}

async fn busy_lock(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x53).await?;
    let mut holder = PgConnection::connect(&fixture.database.url).await?;
    let mut held = holder.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SETTLEMENT_LOCK)
        .execute(&mut *held)
        .await?;
    let started = Instant::now();
    let failed = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await;
    let elapsed = started.elapsed();
    let error = format!(
        "{:#}",
        failed
            .err()
            .context("imported past a held SETTLEMENT_LOCK")?
    );
    println!("busy lock failed after {elapsed:?}: {error}");
    ensure!(
        error.contains(&block_hash),
        "the error does not name the row: {error}"
    );
    ensure!(error.contains("SETTLEMENT_LOCK stayed busy"), "{error}");
    ensure!(
        elapsed < TEST_LOCK_TIMEOUT + SLACK,
        "the busy lock took {elapsed:?} to fail (lock timeout {TEST_LOCK_TIMEOUT:?})"
    );
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the row was stored"
    );
    // The holder still has its transaction and its lock.
    let holds: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted)",
    )
    .fetch_one(&mut *held)
    .await?;
    ensure!(holds, "the holder lost SETTLEMENT_LOCK");
    held.commit().await?;
    holder.close().await?;
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// A SETTLEMENT_LOCK holder that needs the row being imported (a landing
/// filling its `found_block_bits`) is let through at once: the importer's
/// next poll sees it waiting and rolls the row back, well inside the lock
/// timeout, and a re-run imports the row afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settlement_lock_holder_waiting_for_the_row_goes_first() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = holder_waits_for_row(&fixture).await;
    fixture.close(result).await
}

async fn holder_waits_for_row(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x55).await?;
    let mut holder = PgConnection::connect(&fixture.database.url).await?;
    let mut held = holder.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SETTLEMENT_LOCK)
        .execute(&mut *held)
        .await?;
    let mut patient = options(None);
    // Only letting the holder through can end this row before the bound.
    patient.lock_timeout = Duration::from_secs(30);
    let importer = fixture.importer().clone();
    let root = directory.path().to_path_buf();
    let started = Instant::now();
    let import = tokio::spawn(async move {
        importer
            .import_legacy_audits_with(Some(root.as_path()), &ledger_public_key(), &patient, |_| {})
            .await
    });
    // The row is written and the importer is polling for the lock: its
    // poll is the only statement that starts this way.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let polling: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
             AND query LIKE 'SELECT granted,CASE WHEN granted%')",
        )
        .fetch_one(&fixture.ledger.pool)
        .await?;
        if polling {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "the importer never polled for the lock"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let asked = Instant::now();
    sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=$2 WHERE block_hash=$1")
        .bind(&block_hash)
        .bind("207fffff")
        .execute(&mut *held)
        .await?;
    let waited = asked.elapsed();
    held.commit().await?;
    holder.close().await?;
    let failed = tokio::time::timeout(SLACK, import)
        .await
        .context("the import did not end")??;
    let elapsed = started.elapsed();
    let error = format!("{:#}", failed.err().context("the import kept the row")?);
    println!("holder waited {waited:?}; import failed after {elapsed:?}: {error}");
    ensure!(
        waited < Duration::from_secs(2),
        "the SETTLEMENT_LOCK holder waited {waited:?} for the row"
    );
    ensure!(
        error.contains(&block_hash),
        "the error does not name the row: {error}"
    );
    ensure!(
        error.contains(
            "the session holding SETTLEMENT_LOCK is waiting for a lock this row's write holds"
        ),
        "{error}"
    );
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the row was stored"
    );
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    let bits: Option<String> = sqlx::query_scalar(
        "SELECT found_block_bits FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(&block_hash)
    .fetch_one(&fixture.ledger.pool)
    .await?;
    ensure!(
        bits.as_deref() == Some("207fffff"),
        "the holder's write was lost: {bits:?}"
    );
    Ok(())
}

/// A stop (SIGTERM in the CLI) during a row's write abandons the row at
/// once: its connection closes, the server rolls it back, nothing waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_during_a_write_rolls_the_row_back_at_once() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stop_mid_write(&fixture).await;
    fixture.close(result).await
}

async fn stop_mid_write(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(2_000, 20_000)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x54).await?;
    fixture.relay.control.stall_after(256 * 1024);
    let (stop, stopping) = watch::channel(false);
    let mut patient = options(Some(stopping));
    // Only the stop can end this write.
    patient.write_timeout = Duration::from_secs(600);
    let importer = fixture.importer().clone();
    let root = directory.path().to_path_buf();
    let import = tokio::spawn(async move {
        importer
            .import_legacy_audits_with(Some(root.as_path()), &ledger_public_key(), &patient, |_| {})
            .await
    });
    stalled_mid_write(&fixture.relay).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stopped = Instant::now();
    stop.send_replace(true);
    let failed = tokio::time::timeout(SLACK, import)
        .await
        .context("the import did not stop")??;
    let elapsed = stopped.elapsed();
    let error = failed.err().context("a stopped import succeeded")?;
    let stop = error
        .downcast_ref::<ImportStopped>()
        .with_context(|| format!("not a stop: {error:#}"))?;
    ensure!(stop.imported == 0, "{stop}");
    ensure!(
        elapsed < Duration::from_secs(2),
        "the stop took {elapsed:?}"
    );
    fixture.transactions_end(Duration::from_secs(5)).await?;
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the abandoned row was stored"
    );
    fixture.relay.control.release();
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// The same when the holder waits for the row only through another session
/// that waits for the importer: the importer walks the holder's blockers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settlement_lock_holder_queued_behind_another_waiter_still_goes_first() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = holder_waits_through_another(&fixture).await;
    fixture.close(result).await
}

async fn holder_waits_through_another(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x57).await?;
    let mut holder = PgConnection::connect(&fixture.database.url).await?;
    let mut held = holder.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SETTLEMENT_LOCK)
        .execute(&mut *held)
        .await?;
    let mut patient = options(None);
    patient.lock_timeout = Duration::from_secs(30);
    let importer = fixture.importer().clone();
    let root = directory.path().to_path_buf();
    let import = tokio::spawn(async move {
        importer
            .import_legacy_audits_with(Some(root.as_path()), &ledger_public_key(), &patient, |_| {})
            .await
    });
    let waiting = |prefix: &'static str| {
        let pool = fixture.ledger.pool.clone();
        async move {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let found: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
                     AND query LIKE $1)",
                )
                .bind(prefix)
                .fetch_one(&pool)
                .await?;
                if found {
                    return anyhow::Ok(());
                }
                ensure!(Instant::now() < deadline, "no session ran {prefix}");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    };
    waiting("SELECT granted,CASE WHEN granted%").await?;
    // A plain session queues on the row first, holding its tuple lock...
    let mut middle = PgConnection::connect(&fixture.database.url).await?;
    let hash = block_hash.clone();
    let between = tokio::spawn(async move {
        sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=$2 WHERE block_hash=$1 /* middle */")
            .bind(&hash)
            .bind("1d00ffff")
            .execute(&mut middle)
            .await?;
        middle.close().await?;
        anyhow::Ok(())
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let queued: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
             AND wait_event_type='Lock' AND query LIKE '%/* middle */%')",
        )
        .fetch_one(&fixture.ledger.pool)
        .await?;
        if queued {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "the middle session never queued on the row"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // ...so the holder waits for that session, not for the importer.
    let asked = Instant::now();
    sqlx::query("UPDATE qbit_pool_audit_bundles SET found_block_bits=$2 WHERE block_hash=$1")
        .bind(&block_hash)
        .bind("207fffff")
        .execute(&mut *held)
        .await?;
    let waited = asked.elapsed();
    held.commit().await?;
    holder.close().await?;
    tokio::time::timeout(SLACK, between)
        .await
        .context("the middle session never finished")???;
    let failed = tokio::time::timeout(SLACK, import)
        .await
        .context("the import did not end")??;
    let error = format!("{:#}", failed.err().context("the import kept the row")?);
    println!("holder waited {waited:?} behind another waiter: {error}");
    ensure!(
        waited < Duration::from_secs(2),
        "the SETTLEMENT_LOCK holder waited {waited:?}"
    );
    ensure!(
        error.contains(
            "the session holding SETTLEMENT_LOCK is waiting for a lock this row's write holds"
        ),
        "{error}"
    );
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the row was stored"
    );
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// A stop while the server is still running the row's UPDATE (here slowed
/// by a test trigger) rolls it back within about a second: the importer's
/// transaction asks the server to check that the client is still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_while_the_server_runs_the_write_rolls_it_back_within_a_second() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stop_mid_execution(&fixture).await;
    fixture.close(result).await
}

async fn stop_mid_execution(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x56).await?;
    sqlx::raw_sql(
        "CREATE FUNCTION import765_slow() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN IF NEW.canonical_audit_bytes IS NOT NULL THEN PERFORM pg_sleep(30); END IF; \
         RETURN NEW; END $$; \
         CREATE TRIGGER import765_slow BEFORE UPDATE ON qbit_pool_audit_bundles \
         FOR EACH ROW EXECUTE FUNCTION import765_slow();",
    )
    .execute(&fixture.ledger.pool)
    .await?;
    let (stop, stopping) = watch::channel(false);
    let mut patient = options(Some(stopping));
    // Only the stop can end this write.
    patient.write_timeout = Duration::from_secs(600);
    let importer = fixture.importer().clone();
    let root = directory.path().to_path_buf();
    let import = tokio::spawn(async move {
        importer
            .import_legacy_audits_with(Some(root.as_path()), &ledger_public_key(), &patient, |_| {})
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let running: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
             AND state='active' AND wait_event='PgSleep' \
             AND query LIKE 'UPDATE qbit_pool_audit_bundles SET schema_version%')",
        )
        .fetch_one(&fixture.ledger.pool)
        .await?;
        if running {
            break;
        }
        ensure!(Instant::now() < deadline, "the server never ran the write");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let stopped = Instant::now();
    stop.send_replace(true);
    let failed = tokio::time::timeout(SLACK, import)
        .await
        .context("the import did not stop")??;
    let error = failed.err().context("a stopped import succeeded")?;
    ensure!(
        error.downcast_ref::<ImportStopped>().is_some(),
        "not a stop: {error:#}"
    );
    // Without the server's client check the trigger's 30 s would run out.
    fixture.transactions_end(Duration::from_secs(5)).await?;
    let ended = stopped.elapsed();
    println!("stopped mid-execution; the server's transaction ended {ended:?} after the stop");
    // The server checks the client every second.
    ensure!(
        ended < Duration::from_secs(3),
        "the abandoned write ran on for {ended:?} after the stop"
    );
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the abandoned row was stored"
    );
    sqlx::raw_sql("DROP TRIGGER import765_slow ON qbit_pool_audit_bundles")
        .execute(&fixture.ledger.pool)
        .await?;
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// A stop that lands after the row is written but before SETTLEMENT_LOCK is
/// granted rolls the row back, even while the first lock poll is in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_before_the_lock_is_granted_rolls_the_row_back() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stop_before_grant(&fixture).await;
    fixture.close(result).await
}

async fn stop_before_grant(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x58).await?;
    let importer = fixture.plaintext_importer().await?;
    // The first lock poll reaches the relay and goes no further.
    fixture
        .relay
        .control
        .stall_before(b"pg_try_advisory_xact_lock");
    let (stop, stopping) = watch::channel(false);
    let patient = options(Some(stopping));
    let running = importer.clone();
    let root = directory.path().to_path_buf();
    let import = tokio::spawn(async move {
        running
            .import_legacy_audits_with(Some(root.as_path()), &ledger_public_key(), &patient, |_| {})
            .await
    });
    stalled_mid_write(&fixture.relay).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stopped = Instant::now();
    stop.send_replace(true);
    let failed = tokio::time::timeout(SLACK, import)
        .await
        .context("the import did not stop")??;
    let elapsed = stopped.elapsed();
    // Let the held poll through now: the stopped importer must not use it.
    fixture.relay.control.release();
    let error = failed.err().context("a stopped import succeeded")?;
    ensure!(
        error.downcast_ref::<ImportStopped>().is_some(),
        "not a stop: {error:#}"
    );
    ensure!(
        elapsed < Duration::from_secs(2),
        "the stop took {elapsed:?}"
    );
    fixture.transactions_end(Duration::from_secs(5)).await?;
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the stopped row was stored"
    );
    importer.pool.close().await;
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// A stop between rows ends the import before the next row; a re-run
/// imports the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_between_rows_ends_the_import_before_the_next_row() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stop_between_rows(&fixture).await;
    fixture.close(result).await
}

async fn stop_between_rows(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut rows = Vec::new();
    for nonce in [0x61, 0x62] {
        let bundle = legacy_bundle(100, 10)?;
        rows.push(seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, nonce).await?);
    }
    let (stop, stopping) = watch::channel(false);
    let mut reported = Vec::new();
    let failed = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(Some(stopping)),
            |row| {
                reported.push(row.block_hash.clone());
                stop.send_replace(true);
            },
        )
        .await;
    let error = failed.err().context("a stopped import succeeded")?;
    let stopped = error
        .downcast_ref::<ImportStopped>()
        .with_context(|| format!("not a stop: {error:#}"))?;
    ensure!(stopped.imported == 1, "{stopped}");
    ensure!(
        reported == [rows[0].0.clone()],
        "rows reported: {reported:?}"
    );
    ensure!(fixture.stored_digest(&rows[0].0).await?.as_deref() == Some(rows[0].1.as_str()));
    ensure!(
        fixture.stored_digest(&rows[1].0).await?.is_none(),
        "the next row was imported"
    );
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(None),
            |_| {},
        )
        .await?;
    ensure!(imported == 1, "the re-run imported {imported} rows");
    ensure!(fixture.stored_digest(&rows[1].0).await?.as_deref() == Some(rows[1].1.as_str()));
    Ok(())
}

/// A stop that lands once the last row is in ends as a finished import,
/// not as a stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_after_the_last_row_ends_as_a_finished_import() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = stop_after_last_row(&fixture).await;
    fixture.close(result).await
}

async fn stop_after_last_row(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x63).await?;
    let (stop, stopping) = watch::channel(false);
    let imported = fixture
        .importer()
        .import_legacy_audits_with(
            Some(directory.path()),
            &ledger_public_key(),
            &options(Some(stopping)),
            |_| {
                stop.send_replace(true);
            },
        )
        .await?;
    ensure!(imported == 1, "imported {imported} rows");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

/// `docker stop` sends SIGTERM to the command, which a container runs as
/// PID 1: the command must handle it. Mid-row, it rolls the row back and
/// exits non-zero promptly with a message saying it stopped; it prints a
/// line per imported row to stderr.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_import_audits_command_stops_on_sigterm() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = sigterm(&fixture).await;
    fixture.close(result).await
}

/// What an operator's retry sees when SETTLEMENT_LOCK stays busy: exit 1,
/// the remaining-work counts on stdout, and on stderr an error naming the
/// row and the lock. Nothing is stored, so a plain re-run is the retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_import_audits_command_names_the_row_when_settlement_lock_stays_busy() -> Result<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = busy_lock_command(&fixture).await;
    fixture.close(result).await
}

async fn busy_lock_command(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = legacy_bundle(100, 10)?;
    let (block_hash, digest) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x73).await?;
    let mut holder = PgConnection::connect(&fixture.database.url).await?;
    let mut held = holder.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SETTLEMENT_LOCK)
        .execute(&mut *held)
        .await?;
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        import_audits_command(fixture, directory.path()).output(),
    )
    .await??;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!(
        "exit {:?}\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
    ensure!(
        output.status.code() == Some(1),
        "exit status {:?}",
        output.status
    );
    ensure!(
        stdout.contains(
            r#"Audit completeness: {"missing_stored_bodies":1,"missing_canonical_bytes":1}"#
        ),
        "{stdout}"
    );
    ensure!(
        stderr.contains(&format!("importing legacy audit {block_hash}"))
            && stderr.contains("SETTLEMENT_LOCK stayed busy for 2000 ms"),
        "{stderr}"
    );
    ensure!(
        fixture.stored_digest(&block_hash).await?.is_none(),
        "the row was stored"
    );
    held.commit().await?;
    holder.close().await?;
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        import_audits_command(fixture, directory.path()).output(),
    )
    .await??;
    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(output.status.success(), "the re-run failed: {stdout}");
    ensure!(stdout.contains("Imported 1 audit bodies"), "{stdout}");
    ensure!(fixture.stored_digest(&block_hash).await?.as_deref() == Some(digest.as_str()));
    Ok(())
}

fn import_audits_command(fixture: &Fixture, root: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_qbit-prism-server"));
    command
        .arg("import-audits")
        .arg("--root")
        .arg(root)
        .env_clear()
        .env("PRISM_RUNTIME_WORKERS", "2")
        .env("PRISM_INSTANCE_ID", "import-765-cli")
        .env("PRISM_DATABASE_URL", &fixture.relay.url)
        .env("PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX", ledger_public_key())
        // Only the signal can end the stalled write below.
        .env("PRISM_IMPORT_AUDITS_WRITE_TIMEOUT_MS", "600000")
        .env("PRISM_IMPORT_AUDITS_LOCK_TIMEOUT_MS", "2000")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    command
}

async fn sigterm(fixture: &Fixture) -> Result<()> {
    let directory = tempfile::tempdir()?;
    // A run to completion prints one line per row on stderr.
    let bundle = legacy_bundle(100, 10)?;
    let (first, _) = seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x71).await?;
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        import_audits_command(fixture, directory.path()).output(),
    )
    .await??;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure!(output.status.success(), "{stdout}{stderr}");
    ensure!(stdout.contains("Imported 1 audit bodies"), "{stdout}");
    let line = format!("import-audits: block_hash={first} imported parameter_bytes=block_hash:64,");
    ensure!(
        stderr.contains(&line),
        "no per-row line on stderr: {stderr}"
    );
    // A row stalled mid-write when SIGTERM arrives.
    let bundle = legacy_bundle(2_000, 20_000)?;
    let (second, _) =
        seed_legacy_row(&fixture.ledger.pool, directory.path(), &bundle, 0x72).await?;
    fixture.relay.control.stall_after(256 * 1024);
    let command = import_audits_command(fixture, directory.path()).spawn()?;
    let pid = command.id().context("the command has no PID")?;
    stalled_mid_write(&fixture.relay).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let signalled = Instant::now();
    let status = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()?;
    ensure!(status.success(), "kill -TERM failed");
    let output = tokio::time::timeout(SLACK, command.wait_with_output())
        .await
        .context("the command did not exit after SIGTERM")??;
    let elapsed = signalled.elapsed();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!(
        "exited {:?} {elapsed:?} after SIGTERM\nstdout: {stdout}\nstderr: {stderr}",
        output.status
    );
    ensure!(
        output.status.code() == Some(1),
        "exit status {:?}",
        output.status
    );
    ensure!(
        elapsed < Duration::from_secs(5),
        "the command took {elapsed:?} to stop"
    );
    ensure!(
        stderr.contains("stopped on request after importing 0 audit bodies"),
        "{stderr}"
    );
    ensure!(stdout.contains("Audit completeness:"), "{stdout}");
    fixture.transactions_end(Duration::from_secs(5)).await?;
    ensure!(
        fixture.stored_digest(&second).await?.is_none(),
        "the interrupted row was stored"
    );
    Ok(())
}
