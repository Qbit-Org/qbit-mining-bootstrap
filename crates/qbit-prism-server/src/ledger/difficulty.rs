//! Shared operational vardiff hints. These rows never determine share credit.
use super::connect::CustomBegin;
use super::migration::{timed, timed_out};
use super::*;
use sqlx::{postgres::PgTypeInfo, Executor, Type};
use std::sync::LazyLock;
use std::time::Duration;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct WorkerDifficulty {
    pub difficulty: f64,
    pub evidence_at_ms: i64,
    pub age_ms: u64,
}

fn valid_difficulty(value: f64) -> Result<()> {
    ensure!(
        value.is_finite() && value > 0.0,
        "worker difficulty must be positive and finite"
    );
    Ok(())
}

/// A worker's hint, if its evidence is younger than `$3` seconds: what
/// [`Ledger::worker_difficulty`] and a hint write's read-back return.
const READ_SQL: &str = "SELECT difficulty::text AS difficulty,floor(extract(epoch FROM evidence_at)*1000)::bigint AS evidence_at_ms,greatest(0,floor(extract(epoch FROM clock_timestamp()-evidence_at)*1000))::bigint AS age_ms FROM qbit_worker_difficulty WHERE listener=$1 AND worker_username=$2 AND evidence_at>=clock_timestamp()-$3::double precision*interval '1 second'";

/// Record evidence, keeping a row whose evidence is newer.
const RECORD_SQL: &str = "INSERT INTO qbit_worker_difficulty(listener,worker_username,difficulty,evidence_at) VALUES($1,$2,$3::text::numeric,to_timestamp($4::bigint::double precision/1000)) ON CONFLICT(listener,worker_username) DO UPDATE SET difficulty=EXCLUDED.difficulty,evidence_at=EXCLUDED.evidence_at,updated_at=clock_timestamp() WHERE qbit_worker_difficulty.evidence_at<=EXCLUDED.evidence_at";

/// Lower a higher row.
const LOWER_SQL: &str = "UPDATE qbit_worker_difficulty SET difficulty=$3::text::numeric,updated_at=clock_timestamp() WHERE listener=$1 AND worker_username=$2 AND difficulty>$3::text::numeric";

/// When the ledger accepted a share, looked up from the bounded probe floor
/// (see [`Ledger::share_accepted_at_ms`]), then unbounded.
const ACCEPTED_AT_RECENT_SQL: &str = "SELECT floor(extract(epoch FROM accepted_at)*1000)::bigint FROM qbit_share_ledger WHERE share_id=$1 AND accepted AND share_seq>=qbit_prism_share_probe_floor()";
const ACCEPTED_AT_SQL: &str =
    "SELECT floor(extract(epoch FROM accepted_at)*1000)::bigint FROM qbit_share_ledger WHERE share_id=$1 AND accepted";

/// The cluster's write guard, as [`writable`] reads it.
static GUARD_SQL: LazyLock<String> = LazyLock::new(|| {
    format!("SELECT {WRITABLE_COLUMNS} FROM qbit_prism_cluster c WHERE c.singleton")
});

/// Setting the transaction's `statement_timeout` again.
const REARM_SQL: &str = "SELECT set_config('statement_timeout',$1,true)";

/// A hint statement's parameter types, as SQLx prepares a statement from the
/// binds it carries: `text` for `&str` and `String`.
fn text() -> PgTypeInfo {
    <&str as Type<Postgres>>::type_info()
}

fn retained(row: Option<PgRow>) -> Result<Option<WorkerDifficulty>> {
    let Some(row) = row else { return Ok(None) };
    let difficulty: f64 = row.try_get::<String, _>("difficulty")?.parse()?;
    // A legacy numeric outside the wire format is unusable as a hint.
    if valid_difficulty(difficulty).is_err() {
        return Ok(None);
    }
    Ok(Some(WorkerDifficulty {
        difficulty,
        evidence_at_ms: row.try_get("evidence_at_ms")?,
        age_ms: u64::try_from(row.try_get::<i64, _>("age_ms")?)?,
    }))
}

/// How much of a hint write's bound its statements' server-side timeouts
/// leave unused, as a divisor of the bound: a tenth. Half of it covers the
/// round trips between the client's clock and the server's timer (a
/// statement's timer starts once it reaches the server, and the server's
/// cancel takes as long to come back), so the client reads the cancel
/// before its own bound passes; the other half is how old the timeout set
/// on the transaction may be when a statement is sent under it
/// ([`HintDeadline::arm`]).
const HINT_MARGIN_DIVISOR: u32 = 10;

/// PostgreSQL's largest `statement_timeout`, in milliseconds (an `int`).
const MAX_STATEMENT_TIMEOUT_MS: u64 = i32::MAX as u64;

/// One difficulty-hint write's deadline (#872): the writer bounds the write
/// by it (`timeout_at`), and every statement the write sends runs under a
/// server-side `statement_timeout` taken from what is left of it, so
/// PostgreSQL ends a statement before the writer gives up on the write. A
/// statement dropped by the writer's bound would otherwise run on until it
/// ended or reached the connection's own timeout (15 s by default), holding
/// its connection and its locks. On the writer's clock (`tokio::time`), the
/// clock its own bound runs on.
#[derive(Clone, Copy, Debug)]
pub struct HintDeadline {
    at: tokio::time::Instant,
    /// A tenth of the bound ([`HINT_MARGIN_DIVISOR`]).
    margin: Duration,
}

/// How a statement sent now in a hint's transaction is bounded.
#[derive(Debug, PartialEq, Eq)]
enum Arm {
    /// By the timeout already set, recent enough.
    Armed,
    /// By this one, set first.
    Set(Duration),
    /// Not at all: too little is left to send it.
    Spent,
}

impl HintDeadline {
    /// `bound` from now. A bound too long to add to the clock never passes
    /// (as `tokio::time::timeout` treats one).
    pub fn after(bound: Duration) -> Self {
        Self::starting(tokio::time::Instant::now(), bound)
    }

    fn starting(now: tokio::time::Instant, bound: Duration) -> Self {
        let at = now
            .checked_add(bound)
            .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365 * 30));
        Self {
            at,
            margin: (at - now) / HINT_MARGIN_DIVISOR,
        }
    }

    /// When the write's bound passes.
    pub fn at(&self) -> tokio::time::Instant {
        self.at
    }

    /// The `statement_timeout` for a statement sent at `now`: what is left of
    /// the bound, less the margin, in whole milliseconds rounded down (so it
    /// never exceeds what is left), at most PostgreSQL's largest. `None` when
    /// that is under one millisecond: a `statement_timeout` of 0 disables the
    /// timeout, so the statement is not sent at all.
    fn statement_timeout(&self, now: tokio::time::Instant) -> Option<Duration> {
        let left = self
            .at
            .saturating_duration_since(now)
            .saturating_sub(self.margin);
        let millis = u64::try_from(left.as_millis())
            .unwrap_or(u64::MAX)
            .min(MAX_STATEMENT_TIMEOUT_MS);
        (millis > 0).then(|| Duration::from_millis(millis))
    }

    /// How a statement sent at `now` is bounded when the transaction's
    /// timeout was last set at `armed_at`. That timeout was what was left
    /// then, so a statement sent under it runs at most `now - armed_at` past
    /// what is left now; up to half the margin it may, and it is sent
    /// without another round trip. Older, the timeout is set again from what
    /// is left, under the same never-zero rule as the first.
    fn arm(&self, armed_at: tokio::time::Instant, now: tokio::time::Instant) -> Arm {
        if now.saturating_duration_since(armed_at) <= self.margin / 2 {
            return Arm::Armed;
        }
        match self.statement_timeout(now) {
            Some(timeout) => Arm::Set(timeout),
            None => Arm::Spent,
        }
    }
}

/// A hint write's bound left too little time to send its next statement,
/// which was not sent (#872). It counts as `timed_out`, like the bound
/// passing on the writer's side.
#[derive(Debug)]
pub struct HintBoundSpent;

impl std::fmt::Display for HintBoundSpent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the difficulty hint's bound left too little time for its next statement")
    }
}

impl std::error::Error for HintBoundSpent {}

/// Whether `error`, from [`Ledger::write_worker_difficulty_hint`], is its
/// bound passing: a statement PostgreSQL cancelled at the timeout the write
/// set for it, told by SQLSTATE 57014 and how long it ran, never by the
/// server's message, which it translates under `lc_messages` (`timed`,
/// #852), or a statement not sent for want of time. Anything else, an
/// operator's cancel included, is the write failing.
pub fn hint_timed_out(error: &anyhow::Error) -> bool {
    timed_out(error) || error.downcast_ref::<HintBoundSpent>().is_some()
}

/// The statement that opens a hint's transaction with its first statement
/// timeout, in the one round trip a `BEGIN` takes. `timeout` comes from
/// [`HintDeadline::statement_timeout`], so it is at least 1 ms and never
/// reads 0.
fn begin_statement(timeout: Duration) -> String {
    format!(
        "BEGIN; SET LOCAL statement_timeout = {}",
        timeout.as_millis()
    )
}

type HintQuery<'q> = sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>;

/// A hint write's transaction and the statement timeout last set on it. Every
/// setting is `LOCAL`, so the transaction's end, commit or rollback, puts the
/// connection back at the pool's own `statement_timeout`.
struct HintTransaction<'c> {
    tx: Transaction<'c, Postgres>,
    deadline: HintDeadline,
    /// The `statement_timeout` in force, and when it was sent.
    armed: Duration,
    armed_at: tokio::time::Instant,
}

impl<'c> HintTransaction<'c> {
    async fn open_hint(connection: &'c mut CustomBegin, deadline: HintDeadline) -> Result<Self> {
        let armed_at = tokio::time::Instant::now();
        let armed = deadline.statement_timeout(armed_at).ok_or(HintBoundSpent)?;
        let tx = connection.custom_begin(begin_statement(armed)).await?;
        Ok(Self {
            tx,
            deadline,
            armed,
            armed_at,
        })
    }

    /// The `statement_timeout` the next statement runs under, set first if
    /// the one in force is too old ([`HintDeadline::arm`]).
    async fn arm(&mut self) -> Result<Duration> {
        let now = tokio::time::Instant::now();
        match self.deadline.arm(self.armed_at, now) {
            Arm::Armed => {}
            Arm::Spent => return Err(HintBoundSpent.into()),
            Arm::Set(timeout) => {
                // Trivial work, under the timeout it replaces.
                timed(
                    self.armed,
                    sqlx::query(REARM_SQL)
                        .bind(timeout.as_millis().to_string())
                        .execute(&mut *self.tx),
                )
                .await?;
                self.armed = timeout;
                self.armed_at = now;
            }
        }
        Ok(self.armed)
    }

    /// The `statement_timeout` that `sql`, with parameters of `types`, runs
    /// under. A statement SQLx has not prepared on this connection takes a
    /// round trip of its own first (`Parse`, `Describe`, `Sync`), whose lock
    /// waits run under a timer of their own, since `Sync` ends a statement's
    /// timer: prepared here under the timeout in force, the statement's run is
    /// then armed from what is left after it, rather than given a second
    /// full timeout. SQLx caches the prepared statement under its text, so
    /// one already prepared sends nothing here.
    async fn armed_for(&mut self, sql: &str, types: &[PgTypeInfo]) -> Result<Duration> {
        let timeout = self.arm().await?;
        timed(timeout, Executor::prepare_with(&mut *self.tx, sql, types)).await?;
        self.arm().await
    }

    /// The cluster's write guard, [`writable`]'s statement and check.
    async fn hint_guard(&mut self) -> Result<()> {
        let timeout = self.armed_for(&GUARD_SQL, &[]).await?;
        let row = timed(timeout, sqlx::query(&GUARD_SQL).fetch_one(&mut *self.tx)).await?;
        check_writable(&row)
    }

    /// [`Ledger::share_accepted_at_ms`], in this transaction.
    async fn hint_evidence_at_ms(&mut self, share_id: &str) -> Result<Option<i64>> {
        for sql in [ACCEPTED_AT_RECENT_SQL, ACCEPTED_AT_SQL] {
            let timeout = self.armed_for(sql, &[text()]).await?;
            let accepted = timed(
                timeout,
                sqlx::query_scalar::<_, i64>(sql)
                    .bind(share_id)
                    .fetch_optional(&mut *self.tx),
            )
            .await?;
            if accepted.is_some() {
                return Ok(accepted);
            }
        }
        Ok(None)
    }

    /// `sql`, with parameters of `types` bound by `bind`, after
    /// [`Self::armed_for`].
    async fn run_hint_statement<'q>(
        &mut self,
        sql: &'q str,
        types: &[PgTypeInfo],
        bind: impl FnOnce(HintQuery<'q>) -> HintQuery<'q>,
    ) -> Result<()> {
        let timeout = self.armed_for(sql, types).await?;
        timed(timeout, bind(sqlx::query(sql)).execute(&mut *self.tx)).await?;
        Ok(())
    }

    /// [`Self::run_hint_statement`], reading the row it returns, if any.
    async fn read_hint_row<'q>(
        &mut self,
        sql: &'q str,
        types: &[PgTypeInfo],
        bind: impl FnOnce(HintQuery<'q>) -> HintQuery<'q>,
    ) -> Result<Option<PgRow>> {
        let timeout = self.armed_for(sql, types).await?;
        timed(
            timeout,
            bind(sqlx::query(sql)).fetch_optional(&mut *self.tx),
        )
        .await
    }

    async fn commit_hint(mut self) -> Result<()> {
        let timeout = self.arm().await?;
        timed(timeout, self.tx.commit()).await
    }
}

impl Ledger {
    pub async fn worker_difficulty(
        &self,
        listener: &str,
        username: &str,
        ttl_seconds: u64,
    ) -> Result<Option<WorkerDifficulty>> {
        let row = sqlx::query(READ_SQL)
            .bind(listener)
            .bind(username)
            .bind(ttl_seconds as f64)
            .fetch_optional(&mut *self.acquire().await?)
            .await?;
        retained(row)
    }

    /// Write one difficulty hint and read its row back (#856), in one
    /// transaction on one connection, every statement of which PostgreSQL
    /// ends by `deadline` (#872). `share_id` is the accepted share that
    /// proves `difficulty` (its acceptance time is the evidence, and a share
    /// the ledger does not hold records nothing); `None` lowers a higher
    /// row. The read-back is the retained hint younger than `ttl_seconds`,
    /// as [`Self::worker_difficulty`] reads it, returned only once the
    /// transaction has committed.
    ///
    /// Each statement runs under a `statement_timeout` taken from what is
    /// left of the deadline less a tenth of the bound (see [`HintDeadline`]):
    /// the first is set in the `BEGIN`'s own round trip, and another only
    /// when a statement is sent more than half that margin after the last
    /// was set. A statement with less than a millisecond left is not sent
    /// ([`HintBoundSpent`]); one the server cancelled at its timeout is
    /// marked so ([`hint_timed_out`]). Either way the transaction rolls
    /// back, and every setting ends with it. One round trip per statement:
    /// for a decrease `BEGIN` with the timeout, the cluster's write guard,
    /// the write, the read-back and `COMMIT` (five); for evidence the
    /// share's probe comes first (a second, unbounded one only when it
    /// misses), and the guard and the write only when the share is found
    /// (six; seven with the second probe; five when the share is not found).
    /// A timeout set again adds one, and so does each statement the
    /// connection has not prepared yet (its first use there).
    pub async fn write_worker_difficulty_hint(
        &self,
        listener: &str,
        username: &str,
        difficulty: f64,
        share_id: Option<&str>,
        ttl_seconds: u64,
        deadline: HintDeadline,
    ) -> Result<Option<WorkerDifficulty>> {
        if share_id.is_none() {
            // As `lower_worker_difficulty` checks it, before anything is sent.
            valid_difficulty(difficulty)?;
        }
        // Spent before the checkout: no connection is taken.
        deadline
            .statement_timeout(tokio::time::Instant::now())
            .ok_or(HintBoundSpent)?;
        let mut connection = CustomBegin::checked_out(self.acquire().await?);
        let mut tx = HintTransaction::open_hint(&mut connection, deadline).await?;
        match share_id {
            None => {
                tx.hint_guard().await?;
                tx.run_hint_statement(LOWER_SQL, &[text(), text(), text()], |query| {
                    query
                        .bind(listener)
                        .bind(username)
                        .bind(difficulty.to_string())
                })
                .await?;
            }
            Some(share_id) => {
                if let Some(evidence_at_ms) = tx.hint_evidence_at_ms(share_id).await? {
                    // As `record_worker_difficulty` checks them, once the
                    // share is found.
                    valid_difficulty(difficulty)?;
                    ensure!(
                        !listener.is_empty() && !username.is_empty(),
                        "worker difficulty identity must not be empty"
                    );
                    tx.hint_guard().await?;
                    let types = [text(), text(), text(), <i64 as Type<Postgres>>::type_info()];
                    tx.run_hint_statement(RECORD_SQL, &types, |query| {
                        query
                            .bind(listener)
                            .bind(username)
                            .bind(difficulty.to_string())
                            .bind(evidence_at_ms)
                    })
                    .await?;
                }
            }
        }
        let types = [text(), text(), <f64 as Type<Postgres>>::type_info()];
        let row = tx
            .read_hint_row(READ_SQL, &types, |query| {
                query.bind(listener).bind(username).bind(ttl_seconds as f64)
            })
            .await?;
        tx.commit_hint().await?;
        retained(row)
    }

    pub async fn record_worker_difficulty(
        &self,
        listener: &str,
        username: &str,
        difficulty: f64,
        evidence_at_ms: i64,
    ) -> Result<bool> {
        valid_difficulty(difficulty)?;
        ensure!(
            !listener.is_empty() && !username.is_empty(),
            "worker difficulty identity must not be empty"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let changed = sqlx::query(RECORD_SQL)
            .bind(listener)
            .bind(username)
            .bind(difficulty.to_string())
            .bind(evidence_at_ms)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(changed != 0)
    }

    pub async fn lower_worker_difficulty(
        &self,
        listener: &str,
        username: &str,
        difficulty: f64,
    ) -> Result<bool> {
        valid_difficulty(difficulty)?;
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let changed = sqlx::query(LOWER_SQL)
            .bind(listener)
            .bind(username)
            .bind(difficulty.to_string())
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(changed != 0)
    }

    pub async fn prune_worker_difficulties(&self, ttl_seconds: u64, limit: u32) -> Result<u64> {
        ensure!(
            (1..=10000).contains(&limit),
            "worker difficulty prune limit must be 1..10000"
        );
        let mut tx = self.begin().await?;
        writable(&mut tx).await?;
        let changed = sqlx::query("WITH expired AS (SELECT listener,worker_username FROM qbit_worker_difficulty WHERE evidence_at<clock_timestamp()-$1::double precision*interval '1 second' ORDER BY evidence_at,listener,worker_username FOR UPDATE SKIP LOCKED LIMIT $2) DELETE FROM qbit_worker_difficulty d USING expired e WHERE d.listener=e.listener AND d.worker_username=e.worker_username")
            .bind(ttl_seconds as f64).bind(i64::from(limit)).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(changed)
    }

    /// When the ledger accepted `share_id`, or `None` if it holds no such
    /// accepted row.
    ///
    /// The ledger is partitioned by `share_seq` (migration 017) and has no
    /// global `share_id` index, so an unbounded probe descends one per-leaf
    /// index per attached partition. Vardiff evidence is a share the session
    /// submitted moments ago, so the bounded probe
    /// (`qbit_prism_share_probe_floor()`, the lower bound of the partition
    /// two below the one the next `share_seq` lands in) answers it from at
    /// most three leaves holding rows. A miss is not an
    /// answer: the caller uses this timestamp to decide whether a retained
    /// difficulty may be resumed, so an older but still online row must be
    /// found rather than silently reported as absent. The second probe runs
    /// unbounded, and is reached only when the cheap one found nothing.
    pub async fn share_accepted_at_ms(&self, share_id: &str) -> Result<Option<i64>> {
        let mut connection = self.acquire().await?;
        let accepted_at: Option<i64> = sqlx::query_scalar(ACCEPTED_AT_RECENT_SQL)
            .bind(share_id)
            .fetch_optional(&mut *connection)
            .await?;
        if accepted_at.is_some() {
            return Ok(accepted_at);
        }
        Ok(sqlx::query_scalar(ACCEPTED_AT_SQL)
            .bind(share_id)
            .fetch_optional(&mut *connection)
            .await?)
    }
}
