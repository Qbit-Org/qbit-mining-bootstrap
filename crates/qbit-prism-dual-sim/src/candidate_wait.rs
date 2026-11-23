//! What a wait for "no unfinished block candidate" is waiting on, and how
//! long it may take (#841).
//!
//! A frontend killed in the middle of an attempt leaves its candidate claim
//! in the table, and its row unfinished. Another frontend takes the row over
//! only once it has itself watched the claim's version, (`claim_token`,
//! `claim_renewals`), go unrenewed for the row's whole `claim_lease_seconds`
//! on its own monotonic clock: migration 021 (#581), and
//! docs/prism-ledger-ops.md, "Candidate claim leases and database clock
//! steps". The submit loop's lease is 120 s. In the 3.0 pair both frontends
//! claim from A's one outbox, so once A's frontend has died B can finish a
//! row A held only after that lease; a restarted frontend times a dead
//! predecessor's claim from its own first read. Either way, no bound a wait
//! gives live work covers it.
//!
//! So the wait watches every claim the way a frontend does. A live holder
//! renews its claim every 30 s, which writes a new version, and retries a
//! renewal lost to contention within another 30 s; a version unrenewed for
//! longer than [`RENEWAL_BOUND`] (timed as below) is one no one renews
//! (its holder died, was frozen, lost its database, or was restarted under
//! the same instance id). Such a claim first gets its lease, timed from this
//! wait's own first sight of the version, and then the wait's bound for live
//! work, `limit`, for the taker's own work. Any frontend that will take it
//! over was polling before this wait began, or started before it (a wait
//! cannot overlap a restart: both need the whole sim), so it first saw the
//! version no later than this wait did. A taker busy with an attempt of its
//! own surveys the claims only between attempts and may see the version
//! later; `limit` absorbs that unless the attempt outlasts it.
//!
//! A claim already in the table at the wait's first read of its ledger may
//! be one no one renews, and a frontend may have watched it for longer than
//! this wait has. So the wait times how long a version has gone unrenewed
//! from when it was written, as the database records it (every claim and
//! renewal sets `claim_expires_at` to its lease from `clock_timestamp()`),
//! and never from later than its own first sight. No frontend saw the
//! version before it was written, so the wait judges a claim no one renews
//! before anyone may take it over, and until a read past [`RENEWAL_BOUND`]
//! has judged a claim from its first read, that claim holds the wait. The
//! database's clock only ever moves that judgement earlier: stepped forward,
//! it makes a live claim look like one no one renews, and the wait only
//! extends; stepped back, the judgement comes later, but never later than
//! from the wait's own first sight, as for a claim that recorded no lease.
//! Claims written later are live: a dead holder writes nothing.
//!
//! Not extended: a parked row's claim (`next_attempt_at` infinite), which no
//! frontend takes over (`TAKEOVER_SQL` in qbit-prism-server's
//! `candidates.rs`), and a claim on a row the wait does not count as
//! unfinished. A claim no frontend can take over at all (a dual-writer
//! node's own outbox with its frontend down) is extended like any other,
//! so such a wait only fails later; the timeout says the holder's frontend
//! is not running. A live holder whose renewals are lost to contention
//! twice in a row is taken for one no one renews: its failure is reported
//! later, never invented.
//!
//! The deadline is one instant per wait. It only ever moves later, and at
//! most once per row: for the first version of the row's claim this wait
//! watches unrenewed past [`RENEWAL_BOUND`]. Every later version of that
//! row, its holder's renewal or a taker's claim, was written by a live
//! holder, and faults are injected between waits, never during one
//! (`Sim::inject` takes the whole sim), so that holder stays live: its work
//! has the `limit` the extension already gave. So a holder that renews too
//! slowly ever to be taken over, and never finishes, fails the wait one
//! lease plus `limit` after the wait first saw the first of its versions to
//! go unrenewed that long, and the timeout counts them. And the wait never
//! lasts past `limit + UNRECORDED_LEASE + limit` from its start: a row's
//! holder may do live work for `limit` before it stops renewing, no lease
//! is longer than [`UNRECORDED_LEASE`], and the taker then has `limit`.

use crate::frontend::Node;
use anyhow::Result;
use qbit_prism_server::coordinator::{
    CANDIDATE_LEASE_SECONDS, CANDIDATE_RENEWAL_INTERVAL, CANDIDATE_RENEWAL_TIMEOUT,
};
use sqlx::PgPool;
use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::{Duration, Instant};

/// The rows a settle waits for: in flight, or a reconciliation attempt
/// claimed or due (the live fixtures' quiesce rule). A released attempt
/// scheduled for later is bookkeeping, and waiting for it can take minutes.
/// Parenthesised, so it composes with other conditions.
pub const UNFINISHED_WHERE: &str = "(state IN ('pending', 'offer_reserved', 'offered') \
     OR (state = 'reconciliation' AND (claim_expires_at > clock_timestamp() \
         OR next_attempt_at <= clock_timestamp())))";

/// A row's next attempt in seconds from now by the database's clock: NULL
/// when unset, infinite when parked ('infinity', which PostgreSQL 16 cannot
/// subtract from).
pub const NEXT_ATTEMPT_IN_SQL: &str = "CASE WHEN next_attempt_at IS NULL THEN NULL \
     WHEN isfinite(next_attempt_at) \
     THEN EXTRACT(EPOCH FROM next_attempt_at - clock_timestamp())::float8 \
     ELSE 'Infinity'::float8 END";

/// How long a claim that recorded no lease (taken before migration 021) is
/// timed: the most any writer may take (every claim accepts 1 to 600 s), as
/// the frontends' claim observer times it. The server's own constant.
pub use qbit_prism_server::ledger::UNRECORDED_LEASE;

/// The longest a live holder leaves its claim's version unchanged, with one
/// renewal lost to contention, from the server's own renewal cadence
/// (`CANDIDATE_LEASE` and `with_tracked_heartbeat` in qbit-prism-server's
/// `coordinator.rs`; 80 s today). A renewal is bounded by `timeout` (5 s),
/// and the next starts `interval` (30 s) after it returned. One lost to
/// contention fails within `timeout`, a read that the token still holds the
/// row takes another `timeout` at most, and the retry starts at most
/// `interval` later and commits within `timeout`. A version this wait sees
/// was committed after its renewal started, so it is replaced within
/// `timeout + interval + timeout + timeout + interval + timeout`. That is
/// commit to commit, with nothing for a read's own latency: a live holder
/// at its very worst cadence may be judged a moment early, which only
/// spends its row's one extension early.
pub const RENEWAL_BOUND: Duration = CANDIDATE_RENEWAL_INTERVAL
    .saturating_mul(2)
    .saturating_add(CANDIDATE_RENEWAL_TIMEOUT.saturating_mul(4));

// The wait judges a version from when it was written, and no frontend
// takes it over before its lease from then: the difference (40 s today) is
// the time the wait has to read it again before a takeover would replace
// the claim unjudged, and leave the taker's work only the live bound from
// the wait's start.
const _: () = assert!(
    RENEWAL_BOUND.as_nanos() < Duration::from_secs(CANDIDATE_LEASE_SECONDS as u64).as_nanos()
);

/// One row a settle wait reads: unfinished, or claimed.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxRow {
    pub block_hash: String,
    pub state: String,
    /// Whether the wait waits for it ([`UNFINISHED_WHERE`]). A claimed row
    /// it does not count is read for its claim.
    pub unfinished: bool,
    pub attempt_count: i32,
    pub last_error: Option<String>,
    /// Seconds from now by the database's clock; infinite when parked.
    pub next_attempt_in_s: Option<f64>,
    pub claim: Option<Claim>,
}

impl OutboxRow {
    fn parked(&self) -> bool {
        self.next_attempt_in_s.is_some_and(f64::is_infinite)
    }
}

/// A row's claim: its version and its lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub token: String,
    pub renewals: i64,
    /// `claim_lease_seconds`: `None` for a claim taken before 021.
    pub lease_seconds: Option<i32>,
    pub instance_id: Option<String>,
    /// How long ago the version was written, by the database's clock at the
    /// read: `None` for a claim that recorded no lease.
    pub age: Option<Duration>,
}

impl Claim {
    /// How long a frontend must watch this version unrenewed before it may
    /// take the row over.
    pub fn lease(&self) -> Duration {
        self.lease_seconds.map_or(UNRECORDED_LEASE, |seconds| {
            Duration::from_secs(u64::try_from(seconds).unwrap_or(0))
        })
    }
}

/// What a claim's holder's frontend is doing, as far as this run knows: for
/// descriptions only. The wait decides by the claim itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Holder {
    /// It runs and is not frozen.
    Serving,
    /// It is not running (killed or stopped).
    Stopped,
    /// It is frozen (SIGSTOP): it renews nothing until thawed.
    Frozen,
    /// Not a frontend of this run (an operator command, say).
    Unknown,
}

impl Holder {
    fn describe(self) -> &'static str {
        match self {
            Holder::Serving => "its frontend runs",
            Holder::Stopped => "its frontend is not running",
            Holder::Frozen => "its frontend is frozen",
            Holder::Unknown => "not a frontend of this run",
        }
    }
}

/// A claim version's age in seconds by the database's clock: its lease less
/// what is left of it. NULL for a claim that recorded no lease.
const CLAIM_AGE_SQL: &str = "CASE WHEN claim_lease_seconds IS NOT NULL \
     AND isfinite(claim_expires_at) THEN GREATEST(0, claim_lease_seconds \
     - EXTRACT(EPOCH FROM claim_expires_at - clock_timestamp()))::float8 END";

/// Read every unfinished or claimed candidate of one ledger.
pub async fn read(pool: &PgPool) -> Result<Vec<OutboxRow>> {
    type Row = (
        String,
        String,
        bool,
        i32,
        Option<String>,
        Option<f64>,
        Option<String>,
        i64,
        Option<i32>,
        Option<String>,
        Option<f64>,
    );
    // A reconciliation row with no claim expiry and a later attempt makes
    // the condition NULL: not unfinished.
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT block_hash, state, COALESCE({UNFINISHED_WHERE}, false), attempt_count, \
                last_error, {NEXT_ATTEMPT_IN_SQL}, \
                claim_token, claim_renewals, claim_lease_seconds, claim_instance_id, \
                {CLAIM_AGE_SQL} \
         FROM qbit_block_candidate_outbox \
         WHERE {UNFINISHED_WHERE} OR claim_token IS NOT NULL ORDER BY block_hash"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                block_hash,
                state,
                unfinished,
                attempt_count,
                last_error,
                next,
                token,
                renewals,
                lease,
                holder,
                age,
            )| OutboxRow {
                block_hash,
                state,
                unfinished,
                attempt_count,
                last_error,
                next_attempt_in_s: next,
                claim: token.map(|token| Claim {
                    token,
                    renewals,
                    lease_seconds: lease,
                    instance_id: holder,
                    age: age.and_then(|age| Duration::try_from_secs_f64(age).ok()),
                }),
            },
        )
        .collect())
}

/// The rows of a read the wait waits for.
pub fn unfinished(rows: &[OutboxRow]) -> Vec<&OutboxRow> {
    rows.iter().filter(|row| row.unfinished).collect()
}

/// A claim a wait waited out, once its token was no longer in the table.
#[derive(Clone, Debug)]
pub struct ClaimEnd {
    pub ledger: Node,
    pub block_hash: String,
    pub instance_id: Option<String>,
    pub lease: Duration,
    /// The wait's first sight of the version.
    pub first_seen: Instant,
    /// The wait's last read that still showed it: the claim ended after.
    pub last_seen: Instant,
    /// The first read that no longer showed it: the claim ended before.
    pub ended: Instant,
}

/// What a wait's read changed, for the timeline.
#[derive(Clone, Debug)]
pub enum WaitEvent {
    /// The deadline now waits out a claim no one renews.
    WaitingOut {
        block_hash: String,
        state: String,
        instance_id: Option<String>,
        holder: Holder,
        unrenewed: Duration,
        lease: Duration,
        /// The whole time the wait now allows.
        allowed: Duration,
    },
    /// A version of a row's claim went unrenewed past [`RENEWAL_BOUND`]
    /// after the row had its one extension (a holder that renews too slowly
    /// ever to be taken over, say): the wait allows no more.
    LateAgain {
        block_hash: String,
        state: String,
        instance_id: Option<String>,
        holder: Holder,
        unrenewed: Duration,
        /// How many of the row's claim versions have gone unrenewed that
        /// long in this wait.
        versions: u32,
        /// The whole time the wait allows, unchanged.
        allowed: Duration,
    },
    /// A claim the wait waited out ended: taken over, released, or its row
    /// gone. A renewal by its own holder is not an end.
    Ended(ClaimEnd),
}

fn holder_name(instance_id: Option<&str>) -> &str {
    instance_id.unwrap_or("an unknown holder")
}

impl std::fmt::Display for WaitEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WaitEvent::WaitingOut {
                block_hash,
                state,
                instance_id,
                holder,
                unrenewed,
                lease,
                allowed,
            } => write!(
                f,
                "candidate {block_hash} ({state}) is claimed by {} ({}), unrenewed for {:.1} s, \
                 longer than a live holder leaves a claim: a frontend takes it over only once \
                 it has watched it unrenewed for its whole {} s lease, so this wait allows \
                 {:.0} s",
                holder_name(instance_id.as_deref()),
                holder.describe(),
                unrenewed.as_secs_f64(),
                lease.as_secs(),
                allowed.as_secs_f64()
            ),
            WaitEvent::LateAgain {
                block_hash,
                state,
                instance_id,
                holder,
                unrenewed,
                versions,
                allowed,
            } => write!(
                f,
                "candidate {block_hash} ({state}) is claimed by {} ({}), unrenewed for {:.1} s, \
                 longer than a live holder leaves a claim ({versions} of the row's claim \
                 versions so far), but the row has had its one extension, so this wait still \
                 allows {:.0} s",
                holder_name(instance_id.as_deref()),
                holder.describe(),
                unrenewed.as_secs_f64(),
                allowed.as_secs_f64()
            ),
            WaitEvent::Ended(end) => write!(
                f,
                "candidate {}: the claim of {} ended {:.1} to {:.1} s after this wait first saw \
                 it (lease {} s)",
                end.block_hash,
                holder_name(end.instance_id.as_deref()),
                end.last_seen
                    .saturating_duration_since(end.first_seen)
                    .as_secs_f64(),
                end.ended
                    .saturating_duration_since(end.first_seen)
                    .as_secs_f64(),
                end.lease.as_secs()
            ),
        }
    }
}

#[derive(Debug)]
struct Watched {
    token: String,
    renewals: i64,
    instance_id: Option<String>,
    lease: Duration,
    /// This wait's first and latest sight of the version.
    since: Instant,
    last_seen: Instant,
    /// When the version was written, as far as this wait can tell: the
    /// database's record, never later than `since`. Its unrenewed time is
    /// timed from here.
    written: Instant,
    /// First seen in the wait's first read of its ledger: possibly a claim
    /// no one renews, so the wait holds on until it can tell.
    from_first_read: bool,
    /// This wait watched the version unrenewed past [`RENEWAL_BOUND`]: the
    /// row has had its one extension, and the version's end is recorded.
    waited_out: bool,
    /// Unfinished and not parked at the latest read: a claim the deadline
    /// would wait out.
    eligible: bool,
    /// The waited-out claim this one renews, by the same token: its end
    /// is the token's end.
    renews: Option<(Instant, Instant)>,
}

/// A row that has had its one extension, for the first of its claim
/// versions this wait watched unrenewed past [`RENEWAL_BOUND`]; nothing
/// after it moves the deadline for the row.
#[derive(Debug)]
struct Extension {
    /// This wait's first sight of that version.
    first: Instant,
    /// How many of the row's claim versions went unrenewed that long.
    late: u32,
}

/// One wait's view of the unfinished candidates and its one deadline.
#[derive(Debug)]
pub struct CandidateWait {
    started: Instant,
    limit: Duration,
    deadline: Instant,
    /// The latest the deadline may ever move to.
    ceiling: Instant,
    extended: bool,
    /// The ledgers this wait has read.
    read: BTreeSet<Node>,
    /// By ledger and block hash: the claim version this wait last saw.
    watched: BTreeMap<(Node, String), Watched>,
    /// By ledger and block hash: every row that has had its extension.
    extensions: BTreeMap<(Node, String), Extension>,
}

impl CandidateWait {
    /// A wait that gives live work `limit` from `now`.
    pub fn new(limit: Duration, now: Instant) -> Self {
        CandidateWait {
            started: now,
            limit,
            deadline: now + limit,
            ceiling: now + limit + UNRECORDED_LEASE + limit,
            extended: false,
            read: BTreeSet::new(),
            watched: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }

    /// Note one read of `ledger`'s rows, whose reply arrived at `now`.
    /// `holder` says what a claim's holder (by instance id) is doing, for
    /// the events. Returns what changed: each claim the deadline now waits
    /// out, each later one on a row that already had its extension, and
    /// each such claim that ended (taken over, released, or its row
    /// finished).
    pub fn observe(
        &mut self,
        ledger: Node,
        rows: &[OutboxRow],
        now: Instant,
        holder: impl Fn(&str) -> Holder,
    ) -> Vec<WaitEvent> {
        let mut events = Vec::new();
        let first_read = self.read.insert(ledger);
        let current = |hash: &str| {
            rows.iter()
                .find(|row| row.block_hash == hash)
                .and_then(|row| row.claim.as_ref())
        };
        // A version this read no longer shows has ended, unless its own
        // holder renewed it.
        let mut renewed = Vec::new();
        self.watched.retain(|key, seen| {
            if key.0 != ledger {
                return true;
            }
            let claim = current(&key.1);
            if claim
                .is_some_and(|claim| claim.token == seen.token && claim.renewals == seen.renewals)
            {
                seen.last_seen = now;
                return true;
            }
            let span = seen.renews.unwrap_or((seen.since, seen.last_seen));
            if claim.is_some_and(|claim| claim.token == seen.token) {
                if seen.waited_out || seen.renews.is_some() {
                    renewed.push((key.1.clone(), span));
                }
            } else if seen.waited_out || seen.renews.is_some() {
                events.push(WaitEvent::Ended(ClaimEnd {
                    ledger,
                    block_hash: key.1.clone(),
                    instance_id: seen.instance_id.clone(),
                    lease: seen.lease,
                    first_seen: span.0,
                    last_seen: seen.last_seen,
                    ended: now,
                }));
            }
            false
        });
        for row in rows {
            let Some(claim) = &row.claim else {
                continue;
            };
            let renews = renewed
                .iter()
                .find(|(hash, _)| *hash == row.block_hash)
                .map(|(_, span)| *span);
            let seen = self
                .watched
                .entry((ledger, row.block_hash.clone()))
                .or_insert_with(|| Watched {
                    token: claim.token.clone(),
                    renewals: claim.renewals,
                    instance_id: claim.instance_id.clone(),
                    lease: claim.lease(),
                    since: now,
                    last_seen: now,
                    written: claim
                        .age
                        .and_then(|age| now.checked_sub(age))
                        .map_or(now, |written| written.min(now)),
                    from_first_read: first_read,
                    waited_out: false,
                    eligible: false,
                    renews,
                });
            seen.eligible = row.unfinished && !row.parked();
            let unrenewed = now.saturating_duration_since(seen.written);
            if seen.waited_out || unrenewed <= RENEWAL_BOUND || !seen.eligible {
                continue;
            }
            seen.waited_out = true;
            let (since, lease) = (seen.since, seen.lease);
            let (first, versions) = match self.extensions.entry((ledger, row.block_hash.clone())) {
                Entry::Vacant(entry) => {
                    entry.insert(Extension {
                        first: since,
                        late: 1,
                    });
                    (true, 1)
                }
                Entry::Occupied(mut entry) => {
                    entry.get_mut().late += 1;
                    (false, entry.get().late)
                }
            };
            if first {
                self.extend_to(since + lease + self.limit);
            }
            let doing = claim
                .instance_id
                .as_deref()
                .map_or(Holder::Unknown, &holder);
            let block_hash = row.block_hash.clone();
            let state = row.state.clone();
            let instance_id = claim.instance_id.clone();
            let allowed = self.allowed();
            events.push(if first {
                WaitEvent::WaitingOut {
                    block_hash,
                    state,
                    instance_id,
                    holder: doing,
                    unrenewed,
                    lease,
                    allowed,
                }
            } else {
                WaitEvent::LateAgain {
                    block_hash,
                    state,
                    instance_id,
                    holder: doing,
                    unrenewed,
                    versions,
                    allowed,
                }
            });
        }
        events
    }

    /// Move the deadline to `until`, if that is later, and never past the
    /// ceiling.
    fn extend_to(&mut self, until: Instant) {
        let until = until.min(self.ceiling);
        if until > self.deadline {
            self.deadline = until;
            self.extended = true;
        }
    }

    /// Whether the wait is over at `now`: past its deadline, and holding no
    /// claim from its first read that no read has judged yet; or past its
    /// ceiling. A read past [`RENEWAL_BOUND`] judges such a claim, or shows
    /// it renewed, gone or no longer counted, so the hold lasts until the
    /// wait's first read after the bound, however long its round takes.
    pub fn expired(&self, now: Instant) -> bool {
        now > self.ceiling
            || (now > self.deadline
                && !self
                    .watched
                    .values()
                    .any(|seen| seen.from_first_read && seen.eligible && !seen.waited_out))
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// The whole time this wait allows, so far.
    pub fn allowed(&self) -> Duration {
        self.deadline.saturating_duration_since(self.started)
    }

    /// The bound a timeout reports: the live bound, and why it was extended
    /// or that it reached the most any wait allows.
    pub fn bound(&self) -> String {
        if self.deadline == self.ceiling {
            format!(
                "{:.0} s (the most any wait allows: {} s for live work, the longest lease, then \
                 {} s again)",
                self.allowed().as_secs_f64(),
                self.limit.as_secs(),
                self.limit.as_secs()
            )
        } else if self.extended {
            format!(
                "{:.0} s ({} s for live work after the lease of a claim no one renews)",
                self.allowed().as_secs_f64(),
                self.limit.as_secs()
            )
        } else {
            format!("{:?}", self.limit)
        }
    }

    /// Every unfinished row of `ledger` a timeout leaves, as the read at
    /// `read_at` showed it, and why it is not being worked on: its state,
    /// its claim and its holder, how long this wait watched that claim
    /// unrenewed, and its next attempt.
    pub fn describe(
        &self,
        ledger: Node,
        rows: &[&OutboxRow],
        read_at: Instant,
        holder: impl Fn(&str) -> Holder,
    ) -> String {
        let mut text = String::new();
        for row in rows {
            if !text.is_empty() {
                text.push_str("; ");
            }
            let _ = write!(
                text,
                "{} {} ({} attempts",
                row.block_hash, row.state, row.attempt_count
            );
            match &row.claim {
                Some(claim) => {
                    let _ = write!(
                        text,
                        ", claimed by {} ({}), renewals {}, lease {} s",
                        holder_name(claim.instance_id.as_deref()),
                        claim
                            .instance_id
                            .as_deref()
                            .map_or(Holder::Unknown, &holder)
                            .describe(),
                        claim.renewals,
                        claim.lease().as_secs(),
                    );
                    if let Some(seen) = self
                        .watched
                        .get(&(ledger, row.block_hash.clone()))
                        .filter(|seen| seen.token == claim.token && seen.renewals == claim.renewals)
                    {
                        let _ = write!(
                            text,
                            ", unrenewed for {:.1} s",
                            read_at
                                .saturating_duration_since(seen.written)
                                .as_secs_f64()
                        );
                    }
                }
                None => text.push_str(", unclaimed"),
            }
            if let Some(extension) = self.extensions.get(&(ledger, row.block_hash.clone())) {
                let _ =
                    write!(
                    text,
                    ", given the lease of a claim no one renewed, first seen {:.1} s before this \
                     read",
                    read_at.saturating_duration_since(extension.first).as_secs_f64()
                );
                let later = extension.late - 1;
                if later > 0 {
                    let _ = write!(
                        text,
                        ", then {later} more of its claim's versions unrenewed for over {} s, \
                         and the row not finished",
                        RENEWAL_BOUND.as_secs()
                    );
                }
            }
            match row.next_attempt_in_s {
                Some(next) if next.is_infinite() => text.push_str(", parked"),
                Some(next) if next <= 0.0 => text.push_str(", due"),
                Some(next) => {
                    let _ = write!(text, ", next attempt in {next:.1} s");
                }
                None => {}
            }
            if let Some(error) = &row.last_error {
                let _ = write!(text, ", last error {error:?}");
            }
            text.push(')');
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Duration = Duration::from_secs(60);
    const LEASE: Duration = Duration::from_secs(120);

    fn row(hash: &str, claim: Option<(&str, i64, Option<i32>, &str)>) -> OutboxRow {
        OutboxRow {
            block_hash: hash.into(),
            state: "offered".into(),
            unfinished: true,
            attempt_count: 1,
            last_error: None,
            next_attempt_in_s: Some(-1.0),
            claim: claim.map(|(token, renewals, lease, holder)| Claim {
                token: token.into(),
                renewals,
                lease_seconds: lease,
                instance_id: Some(holder.into()),
                age: None,
            }),
        }
    }

    fn holders(holder: &str) -> Holder {
        match holder {
            "dual-sim-a" => Holder::Stopped,
            "dual-sim-b" => Holder::Serving,
            _ => Holder::Unknown,
        }
    }

    fn after(start: Instant, seconds: u64) -> Instant {
        start + Duration::from_secs(seconds)
    }

    #[test]
    fn a_renewing_holder_keeps_the_live_bound() {
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        // Renewed every 30 s, one renewal lost to contention: a new version
        // before any is watched for 80 s.
        for (at, renewals) in [(0, 1), (30, 2), (59, 3)] {
            let rows = [row("h", Some(("t", renewals, Some(120), "dual-sim-b")))];
            assert!(wait
                .observe(Node::A, &rows, after(start, at), holders)
                .is_empty());
        }
        assert_eq!(wait.allowed(), LIMIT);
        assert!(!wait.expired(start + LIMIT));
        assert!(wait.expired(start + LIMIT + Duration::from_millis(1)));
        // A version written after the first read cannot hold the wait.
        let rows = [row("h", Some(("t", 4, Some(120), "dual-sim-b")))];
        wait.observe(Node::A, &rows, after(start, 60), holders);
        assert!(wait.expired(after(start, 61)));
        assert_eq!(wait.bound(), "60s");
    }

    #[test]
    fn a_claim_no_one_renews_gets_its_lease_from_first_sight_then_the_live_bound() {
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let rows = [row("h", Some(("t", 1, Some(120), "dual-sim-a")))];
        assert!(wait.observe(Node::A, &rows, start, holders).is_empty());
        // Past the live bound, but not yet judged: the wait holds on.
        assert!(wait
            .observe(Node::A, &rows, after(start, 61), holders)
            .is_empty());
        assert!(!wait.expired(after(start, 61)));
        assert!(wait
            .observe(Node::A, &rows, start + RENEWAL_BOUND, holders)
            .is_empty());
        let judged = start + RENEWAL_BOUND + Duration::from_millis(100);
        let events = wait.observe(Node::A, &rows, judged, holders);
        assert_eq!(events.len(), 1, "{events:?}");
        let text = events[0].to_string();
        assert!(
            text.contains(
                "claimed by dual-sim-a (its frontend is not running), unrenewed for 80.1 s"
            ),
            "{text}"
        );
        assert!(
            text.contains("120 s lease, so this wait allows 180 s"),
            "{text}"
        );
        assert_eq!(wait.deadline(), start + LEASE + LIMIT);
        assert!(!wait.expired(judged));
        // Seen again, unchanged: one extension, no new event.
        let later = after(start, 100);
        assert!(wait.observe(Node::A, &rows, later, holders).is_empty());
        assert_eq!(wait.deadline(), start + LEASE + LIMIT);
        assert!(
            wait.bound().contains("180 s (60 s for live work"),
            "{}",
            wait.bound()
        );
        let text = wait.describe(Node::A, &unfinished(&rows), later, holders);
        assert!(
            text.contains("claimed by dual-sim-a (its frontend is not running), renewals 1"),
            "{text}"
        );
        assert!(text.contains("unrenewed for 100.0 s"), "{text}");
        assert!(text.contains(", due)"), "{text}");
    }

    #[test]
    fn a_serving_holder_that_stopped_renewing_is_waited_out_too() {
        // A predecessor's claim after a restart under the same instance id,
        // a holder cut off from its database, an operator's claim: what the
        // holder's process does is not the test, its renewals are.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let rows = [
            row("r", Some(("t", 2, Some(120), "dual-sim-b"))),
            row("o", Some(("t", 0, Some(30), "operator"))),
        ];
        wait.observe(Node::A, &rows, start, holders);
        let events = wait.observe(Node::A, &rows, after(start, 81), holders);
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(wait.deadline(), start + LEASE + LIMIT);
    }

    #[test]
    fn the_deadline_never_moves_back_when_the_claim_is_taken_over() {
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let dead = [row("h", Some(("t", 1, Some(120), "dual-sim-a")))];
        wait.observe(Node::A, &dead, start, holders);
        assert_eq!(
            wait.observe(Node::A, &dead, after(start, 81), holders)
                .len(),
            1
        );
        let deadline = wait.deadline();
        let held = after(start, 120);
        assert!(wait.observe(Node::A, &dead, held, holders).is_empty());
        let taken = after(start, 121);
        let taker = [row("h", Some(("t2", 0, Some(120), "dual-sim-b")))];
        let events = wait.observe(Node::A, &taker, taken, holders);
        assert_eq!(events.len(), 1, "{events:?}");
        let WaitEvent::Ended(end) = &events[0] else {
            panic!("{events:?}");
        };
        assert_eq!(
            (end.first_seen, end.last_seen, end.ended),
            (start, held, taken)
        );
        assert!(
            events[0].to_string().contains("ended 120.0 to 121.0 s"),
            "{}",
            events[0]
        );
        assert_eq!(wait.deadline(), deadline);
        // The taker's own claim, and then its row's finish, change nothing.
        assert!(wait.observe(Node::A, &[], taken, holders).is_empty());
        assert_eq!(wait.deadline(), deadline);
        assert!(wait.bound().contains("for live work"), "{}", wait.bound());
    }

    #[test]
    fn a_renewal_by_its_own_holder_is_not_an_end() {
        // A frozen holder thawed within its lease renews under its token.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let frozen = [row("h", Some(("t", 1, Some(120), "dual-sim-a")))];
        wait.observe(Node::A, &frozen, start, holders);
        assert_eq!(
            wait.observe(Node::A, &frozen, after(start, 81), holders)
                .len(),
            1
        );
        let thawed = [row("h", Some(("t", 2, Some(120), "dual-sim-a")))];
        assert!(wait
            .observe(Node::A, &thawed, after(start, 90), holders)
            .is_empty());
        // Its end is the token's: once the row is gone, timed from the
        // first sight of the claim.
        let events = wait.observe(Node::A, &[], after(start, 95), holders);
        let [WaitEvent::Ended(end)] = events.as_slice() else {
            panic!("{events:?}");
        };
        assert_eq!(
            (end.first_seen, end.last_seen, end.ended),
            (start, after(start, 90), after(start, 95))
        );
    }

    #[test]
    fn a_claim_on_a_row_that_stops_counting_has_not_ended() {
        // A reconciliation row in backoff leaves the unfinished set once
        // its database expiry passes, while its claim is still there.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let mut rows = [row("h", Some(("t", 1, Some(120), "dual-sim-a")))];
        wait.observe(Node::A, &rows, start, holders);
        assert_eq!(
            wait.observe(Node::A, &rows, after(start, 81), holders)
                .len(),
            1
        );
        rows[0].unfinished = false;
        assert!(wait
            .observe(Node::A, &rows, after(start, 90), holders)
            .is_empty());
        assert!(unfinished(&rows).is_empty());
    }

    #[test]
    fn parked_and_uncounted_rows_are_not_waited_out() {
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let mut parked = row("p", Some(("t", 1, Some(120), "dual-sim-a")));
        parked.next_attempt_in_s = Some(f64::INFINITY);
        let mut backoff = row("b", Some(("t", 1, Some(120), "dual-sim-a")));
        backoff.unfinished = false;
        let rows = [parked, backoff];
        wait.observe(Node::A, &rows, start, holders);
        // Nor do they hold the wait past its live bound.
        assert!(wait.expired(after(start, 61)));
        assert!(wait
            .observe(Node::A, &rows, after(start, 90), holders)
            .is_empty());
        assert_eq!(wait.allowed(), LIMIT);
        let text = wait.describe(Node::A, &unfinished(&rows), after(start, 90), holders);
        assert!(text.contains(", parked)"), "{text}");
    }

    #[test]
    fn an_unrecorded_lease_is_timed_as_the_longest() {
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let rows = [row("p", Some(("t", 0, None, "dual-sim-a")))];
        wait.observe(Node::A, &rows, start, holders);
        wait.observe(Node::A, &rows, after(start, 81), holders);
        assert_eq!(wait.deadline(), start + UNRECORDED_LEASE + LIMIT);
    }

    #[test]
    fn ledgers_are_watched_apart() {
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let rows = [row("h", Some(("t", 1, Some(120), "dual-sim-a")))];
        wait.observe(Node::A, &rows, start, holders);
        // B's read does not show A's row, and does not end its claim.
        assert!(wait
            .observe(Node::B, &[], after(start, 20), holders)
            .is_empty());
        assert_eq!(
            wait.observe(Node::A, &rows, after(start, 81), holders)
                .len(),
            1
        );
        // B's own sight of the same hash is timed apart, and was not in
        // B's first read.
        assert!(wait
            .observe(Node::B, &rows, after(start, 81), holders)
            .is_empty());
    }

    #[test]
    fn a_holder_that_renews_too_slowly_gets_one_lease_and_then_fails() {
        // A live holder that renews every 85 s: never long enough for a
        // takeover, never finishing. Each version is unrenewed past the
        // bound, but only the first moves the deadline.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let rows_at = |at: u64| {
            let renewals = i64::try_from(at / 85).unwrap() + 1;
            [row("h", Some(("t", renewals, Some(120), "dual-sim-b")))]
        };
        let mut events = Vec::new();
        let mut expired_at = None;
        // Read once a second, as a settle does (more often, in fact), for
        // an hour.
        for at in 0..=3600 {
            let now = after(start, at);
            let rows = rows_at(at);
            events.extend(wait.observe(Node::A, &rows, now, holders));
            if expired_at.is_none() && wait.expired(now) {
                expired_at = Some(at);
                let text = wait.describe(Node::A, &unfinished(&rows), now, holders);
                for expected in [
                    "h offered (1 attempts, claimed by dual-sim-b (its frontend runs), renewals 3, \
                     lease 120 s, unrenewed for 11.0 s",
                    "given the lease of a claim no one renewed, first seen 181.0 s before this \
                     read, then 1 more of its claim's versions unrenewed for over 80 s, and the \
                     row not finished",
                ] {
                    assert!(text.contains(expected), "{text}");
                }
            }
        }
        // The first version's lease from its first sight, then the live
        // bound: the hour never moved it.
        assert_eq!(expired_at, Some(181));
        assert_eq!(wait.deadline(), start + LEASE + LIMIT);
        assert_eq!(wait.allowed(), LEASE + LIMIT);
        assert!(
            wait.bound().contains("180 s (60 s for live work"),
            "{}",
            wait.bound()
        );
        // On the timeline: the extension once, then every late version.
        let [WaitEvent::WaitingOut { allowed, .. }, rest @ ..] = events.as_slice() else {
            panic!("{events:?}");
        };
        assert_eq!(*allowed, LEASE + LIMIT);
        assert_eq!(rest.len(), 3600 / 85 - 1, "{rest:?}");
        for (index, event) in rest.iter().enumerate() {
            let WaitEvent::LateAgain {
                versions, allowed, ..
            } = event
            else {
                panic!("{event:?}");
            };
            assert_eq!(
                (*versions, *allowed),
                (u32::try_from(index).unwrap() + 2, LEASE + LIMIT)
            );
        }
        assert!(
            rest[0].to_string().contains(
                "claimed by dual-sim-b (its frontend runs), unrenewed for 81.0 s, longer than a \
                 live holder leaves a claim (2 of the row's claim versions so far), but the row \
                 has had its one extension, so this wait still allows 180 s"
            ),
            "{}",
            rest[0]
        );
    }

    #[test]
    fn a_claim_is_judged_from_when_it_was_written() {
        // The dead holder last renewed 90 s before the wait's first read. A
        // frontend that saw that renewal takes the claim over at its lease,
        // 30 s into the wait, so the wait must judge it at once.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let mut dead = row("h", Some(("t", 1, Some(120), "dual-sim-a")));
        dead.claim.as_mut().unwrap().age = Some(Duration::from_secs(90));
        let events = wait.observe(Node::A, std::slice::from_ref(&dead), start, holders);
        let [WaitEvent::WaitingOut {
            unrenewed, allowed, ..
        }] = events.as_slice()
        else {
            panic!("{events:?}");
        };
        // The lease is still timed from the wait's own first sight: a
        // frontend that started after the holder died first saw the claim
        // no earlier than the wait did.
        assert_eq!(
            (*unrenewed, *allowed),
            (Duration::from_secs(90), LEASE + LIMIT)
        );
        let taken = after(start, 30);
        let taker = [row("h", Some(("t2", 0, Some(120), "dual-sim-b")))];
        let events = wait.observe(Node::A, &taker, taken, holders);
        let [WaitEvent::Ended(end)] = events.as_slice() else {
            panic!("{events:?}");
        };
        assert_eq!(
            (end.first_seen, end.last_seen, end.ended),
            (start, start, taken)
        );
        assert_eq!(wait.deadline(), start + LEASE + LIMIT);

        // A version written 30 s before the first read is judged 50 s
        // into the wait, not 80 s.
        let mut wait = CandidateWait::new(LIMIT, start);
        dead.claim.as_mut().unwrap().age = Some(Duration::from_secs(30));
        let rows = [dead];
        assert!(wait.observe(Node::A, &rows, start, holders).is_empty());
        assert!(wait
            .observe(Node::A, &rows, after(start, 50), holders)
            .is_empty());
        let events = wait.observe(Node::A, &rows, after(start, 51), holders);
        assert!(
            matches!(
                events.as_slice(),
                [WaitEvent::WaitingOut { unrenewed, .. }] if *unrenewed == Duration::from_secs(81)
            ),
            "{events:?}"
        );

        // A live holder: its version 70 s old at the first read, renewed
        // 5 s later. The renewal is timed from its own write.
        let mut wait = CandidateWait::new(LIMIT, start);
        let mut live = row("h", Some(("t", 1, Some(120), "dual-sim-b")));
        live.claim.as_mut().unwrap().age = Some(Duration::from_secs(70));
        assert!(wait
            .observe(Node::A, std::slice::from_ref(&live), start, holders)
            .is_empty());
        let mut renewed = row("h", Some(("t", 2, Some(120), "dual-sim-b")));
        renewed.claim.as_mut().unwrap().age = Some(Duration::ZERO);
        for at in [5, 60, 85] {
            assert!(wait
                .observe(
                    Node::A,
                    std::slice::from_ref(&renewed),
                    after(start, at),
                    holders
                )
                .is_empty());
        }
        assert_eq!(wait.allowed(), LIMIT);
        assert!(wait.expired(after(start, 61)));
    }

    #[test]
    fn the_first_read_hold_lasts_until_a_read_judges_the_claim() {
        // A round slower than the read period around the bound: the claim
        // holds the wait until a read can judge it.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let rows = [row("h", Some(("t", 1, Some(120), "dual-sim-a")))];
        wait.observe(Node::A, &rows, start, holders);
        wait.observe(
            Node::A,
            &rows,
            start + Duration::from_millis(79_900),
            holders,
        );
        assert!(!wait.expired(after(start, 85)));
        assert_eq!(
            wait.observe(Node::A, &rows, after(start, 85), holders)
                .len(),
            1
        );
        assert_eq!(wait.deadline(), start + LEASE + LIMIT);
    }

    #[test]
    fn a_claim_written_after_the_first_read_and_replaced_extends_nothing() {
        // A live holder's next attempt: no extension, no end.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        wait.observe(Node::A, &[], start, holders);
        let live = [row("h", Some(("t", 0, Some(120), "dual-sim-b")))];
        wait.observe(Node::A, &live, after(start, 10), holders);
        let next = [row("h", Some(("t2", 0, Some(120), "dual-sim-b")))];
        assert!(wait
            .observe(Node::A, &next, after(start, 50), holders)
            .is_empty());
        assert_eq!(wait.allowed(), LIMIT);
    }

    #[test]
    fn the_deadline_never_passes_the_ceiling() {
        // Two claims that recorded no lease (600 s), the second first seen
        // just before the first one's extension runs out: the second's
        // lease is cut at live work, the longest lease, and live work again.
        let start = Instant::now();
        let mut wait = CandidateWait::new(LIMIT, start);
        let first = row("p", Some(("t", 0, None, "dual-sim-a")));
        let second = row("q", Some(("u", 0, None, "dual-sim-a")));
        wait.observe(Node::A, std::slice::from_ref(&first), start, holders);
        wait.observe(
            Node::A,
            std::slice::from_ref(&first),
            after(start, 81),
            holders,
        );
        assert_eq!(wait.deadline(), start + UNRECORDED_LEASE + LIMIT);
        let both = [first, second];
        assert!(wait
            .observe(Node::A, &both, after(start, 600), holders)
            .is_empty());
        let events = wait.observe(Node::A, &both, after(start, 681), holders);
        let ceiling = start + LIMIT + UNRECORDED_LEASE + LIMIT;
        assert!(
            matches!(
                events.as_slice(),
                [WaitEvent::WaitingOut { block_hash, allowed, .. }]
                    if block_hash == "q" && start + *allowed == ceiling
            ),
            "{events:?}"
        );
        assert_eq!(wait.deadline(), ceiling);
        assert!(!wait.expired(ceiling));
        assert!(wait.expired(ceiling + Duration::from_millis(1)));
        // Not even a claim from a first read the wait could not judge yet
        // holds it past the ceiling.
        let mut late = CandidateWait::new(LIMIT, start);
        let read = ceiling - Duration::from_secs(10);
        late.observe(Node::A, std::slice::from_ref(&both[0]), read, holders);
        assert!(!late.expired(ceiling));
        assert!(late.expired(ceiling + Duration::from_millis(1)));
        assert_eq!(
            wait.bound(),
            "720 s (the most any wait allows: 60 s for live work, the longest lease, then 60 s \
             again)"
        );
    }

    #[test]
    fn the_unfinished_condition_composes() {
        assert!(UNFINISHED_WHERE.starts_with('(') && UNFINISHED_WHERE.ends_with(')'));
    }
}
