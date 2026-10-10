//! A dual-writer window's per-node cut in SQL (PRISM 3.1).
//!
//! With `PRISM_DUAL_WRITER` on, each node appends to its own database and
//! pulls the rows the peer originated. A window records a [`WindowCut`]: for
//! each node, the highest `share_seq` of that node's rows it may include. A
//! row is eligible when it was eligible in 3.0 (accepted, and accepted and
//! issued at or before the anchor) **and** its `share_seq` is at most its
//! node's entry. A peer row that reaches this database after the cut was
//! read is above the peer's entry, so it stays outside every window built
//! before it, and every proof of a window reads the same rows on either node.
//!
//! **How the entries are chosen** ([`OWN_CUT_SQL`], [`read_peer_cut`]): each
//! is a row of its node that the window's predicate admits, the newest one.
//! - This node's entry is its newest own row eligible at the anchor, read
//!   under `ORDER_LOCK` once the anchor is taken: every own row is appended
//!   under that lock, so every later one is above it.
//! - The peer's entry is its newest synced row at or below the peer sync's
//!   high-water mark for the share stream that is stamped at or before the
//!   anchor. The sync writes the mark in the transaction that inserts the
//!   rows, so every peer row below it is already committed here but one the
//!   sync refused as a conflict, and none arrives later.
//!
//! **Quarantined peer rows.** A peer row the sync refused as a conflict is
//! quarantined for good: never inserted, while the mark moves past it. A
//! window taken here leaves it out and stays whole. The peer holds it as its
//! own row, so a window whose range spans one reproduces only on the node
//! that took it, and the other node's proofs of it refuse.
//!
//! **The anchor rule and peer rows.** It applies to every row, as it always
//! has and as the fold in `qbit_prism` applies it to every bundle share.
//! Each node's stamps rise with its `share_seq` (its appends take the
//! `GREATEST` ledger clock under its own `ORDER_LOCK`), so with the entries
//! above, every row the cut admits also passes the anchor rule: the cut alone
//! decides membership, and the rule is the consistency check the verifier
//! binaries already make. A peer clock running ahead only delays that peer's
//! newest shares by the skew before they join a window. Selection and every
//! later proof apply one deterministic predicate to immutable rows, whose
//! timestamps are copied verbatim, so clock skew can never make a window
//! irreproducible or a landing fail. Exempting peer rows from the rule would
//! instead put rows in the coinbase that the verifiers drop.
//!
//! **Without a cut** (single-writer mode, every pre-3.1 window) each helper
//! here renders exactly the SQL it rendered before 3.1 and binds nothing more.
use super::*;
pub use qbit_prism::WindowCut;
use sqlx::postgres::PgArguments;

/// The anchored predicate, then, for a window with a cut, the cut's clause
/// with node 0's entry at `$cut_parameter` and node 1's at
/// `$cut_parameter + 1`. Bind them with [`BindCut::bind_cut`] after every
/// other parameter. A NULL entry admits none of that node's rows:
/// `share_seq <= NULL` is never true.
pub(crate) fn window_eligibility_sql(
    anchor_parameter: usize,
    cut_parameter: Option<usize>,
) -> String {
    format!(
        "{}{}",
        super::super::audit::anchored_eligibility_sql(anchor_parameter),
        cut_clause(cut_parameter, None)
    )
}

/// The cut's clause alone, for a statement that spells the anchored
/// predicate its own way: empty without a cut, otherwise
/// ` AND ((origin_node=0 AND share_seq<=$n) OR (origin_node=1 AND share_seq<=$n+1))`,
/// with both columns qualified by `alias` when the statement reads the ledger
/// under several names.
pub(crate) fn cut_clause(cut_parameter: Option<usize>, alias: Option<&str>) -> String {
    let Some(node_0) = cut_parameter else {
        return String::new();
    };
    let prefix = alias.map(|alias| format!("{alias}.")).unwrap_or_default();
    format!(
        " AND (({prefix}origin_node=0 AND {prefix}share_seq<=${node_0}) OR ({prefix}origin_node=1 AND {prefix}share_seq<=${}))",
        node_0 + 1
    )
}

/// The SQL values of a cut's entries: node 0's then node 1's, NULL for none.
pub(crate) fn cut_values(cut: &WindowCut) -> Result<(Option<i64>, Option<i64>), WindowError> {
    let value = |entry: Option<u64>| {
        entry
            .map(i64::try_from)
            .transpose()
            .map_err(|error| WindowError::Decode(anyhow::anyhow!("window cut entry: {error}")))
    };
    Ok((value(cut.node_0())?, value(cut.node_1())?))
}

/// Appends a cut's two entries to a query's binds, in the order
/// [`window_eligibility_sql`] numbers them; without a cut it binds nothing.
pub(crate) trait BindCut: Sized {
    fn bind_cut(self, cut: Option<&WindowCut>) -> Result<Self, WindowError>;
}

/// One body for every query type a window statement is issued as.
macro_rules! bind_cut_for {
    ($([$($generics:tt)*] $query:ty),+ $(,)?) => {$(
        impl<$($generics)*> BindCut for $query {
            fn bind_cut(self, cut: Option<&WindowCut>) -> Result<Self, WindowError> {
                Ok(match cut {
                    Some(cut) => {
                        let (node_0, node_1) = cut_values(cut)?;
                        self.bind(node_0).bind(node_1)
                    }
                    None => self,
                })
            }
        }
    )+};
}

bind_cut_for!(
    ['q] sqlx::query::Query<'q, Postgres, PgArguments>,
    ['q, O] sqlx::query::QueryScalar<'q, Postgres, O, PgArguments>,
    ['q, O] sqlx::query::QueryAs<'q, Postgres, O, PgArguments>,
);

/// `qbit_prism_audit_snapshots`' cut columns (migration 028) for a cut:
/// both NULL without one; with one, each entry, `0` for none. `share_seq`
/// starts at 1, so `0` admits no row and keeps "no cut" and "a cut that
/// admits nothing of a node" apart.
pub(crate) fn cut_columns(
    cut: Option<&WindowCut>,
) -> Result<(Option<i64>, Option<i64>), WindowError> {
    match cut {
        None => Ok((None, None)),
        Some(cut) => {
            let (node_0, node_1) = cut_values(cut)?;
            Ok((Some(node_0.unwrap_or(0)), Some(node_1.unwrap_or(0))))
        }
    }
}

/// The cut [`cut_columns`] stored; any other shape is corruption.
pub(crate) fn cut_from_columns(
    node_0: Option<i64>,
    node_1: Option<i64>,
) -> Result<Option<WindowCut>, WindowError> {
    match (node_0, node_1) {
        (None, None) => Ok(None),
        (Some(node_0), Some(node_1)) => {
            WindowCut::new(positive_entry(node_0)?, positive_entry(node_1)?)
                .map(Some)
                .map_err(|error| WindowError::Decode(error.into()))
        }
        _ => Err(WindowError::Decode(anyhow::anyhow!(
            "window cut columns are half set"
        ))),
    }
}

/// This node's own entry: its newest own row eligible at the anchor, read
/// where `ORDER_LOCK` is held, after the anchor is taken. Every own row is
/// appended under that lock (the share append and the settlement's
/// deferred-share credit; own-log recovery runs before the node serves and
/// only adds rows above the restored ones), so every own row appended later
/// is above it. A native append is accepted and stamped with the ledger
/// clock, so the newest own row is the entry; only legacy rows, rejected or
/// stamped ahead, can lie above it, and they are outside the window either
/// way. Holding to eligibility keeps both entries rows the window's predicate
/// admits: the higher one is the window's newest row, as the accepted cutoff
/// is without a cut. NULL when this node has no eligible row. `$1` is this
/// node's index, `$2` the anchor, and `$3` the retained window's entry for
/// this node, or NULL.
///
/// **One index probe per partition, whatever the planner believes.** A node's
/// own rows can all lie under a long run of the peer's, as on a node that was
/// idle while its peer served. A backward walk of the primary key with
/// `origin_node` as a filter would then read the whole run inside
/// `ORDER_LOCK`, and the planner chooses that walk whenever the node's rows
/// are a fair share of the table, because it cannot see that they are all
/// at the bottom. Bounding `origin_node` from both sides, instead of by
/// equality, leaves it out of the planner's equivalence classes, so the order
/// `(origin_node, share_seq)` is served only by the `(origin_node,
/// share_seq)` index (migration 031): one backward probe per partition, to
/// this node's newest row, which a native append makes the entry.
///
/// **Bounded below by the retained window's entry.** One probe per
/// partition would still grow with the partitions retained, all inside
/// `ORDER_LOCK`, where 3.0's cutoff reads only the newest. The entry never
/// moves back while the rows stay, so the first probe reads only the
/// partitions at or above the retained window's entry, usually the newest
/// alone. `COALESCE` evaluates the unbounded probe only when that one finds
/// nothing: on the first snapshot (no retained entry, NULL), and after
/// retention or a restore removed the retained entry's row.
pub(crate) const OWN_CUT_SQL: &str = "SELECT COALESCE(\
     (SELECT share_seq FROM qbit_share_ledger WHERE origin_node BETWEEN $1 AND $1 AND share_seq>=$3 AND accepted AND accepted_at<=to_timestamp($2::double precision/1000) AND job_issued_at<=to_timestamp($2::double precision/1000) ORDER BY origin_node DESC, share_seq DESC LIMIT 1),\
     (SELECT share_seq FROM qbit_share_ledger WHERE origin_node BETWEEN $1 AND $1 AND accepted AND accepted_at<=to_timestamp($2::double precision/1000) AND job_issued_at<=to_timestamp($2::double precision/1000) ORDER BY origin_node DESC, share_seq DESC LIMIT 1))";

/// Whether the share ledger has a valid `(origin_node, share_seq)` index
/// (migration 031), on its parent and so on every partition: the index
/// [`OWN_CUT_SQL`] and [`read_peer_cut`] read. Without it they would scan the
/// ledger, the first inside `ORDER_LOCK`, so a dual-writer snapshot checks it
/// first and refuses. A btree, both columns in one direction with default
/// null ordering, so a scan of it in either direction serves their
/// `ORDER BY`. One catalog lookup per snapshot, about 0.04 ms, re-read each
/// time so an index dropped or left invalid later is still caught.
pub(crate) const ORIGIN_INDEX_SQL: &str = "SELECT EXISTS(SELECT 1 FROM pg_index i \
     JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am m ON m.oid=c.relam \
     JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] \
     JOIN pg_attribute b ON b.attrelid=i.indrelid AND b.attnum=i.indkey[1] \
     WHERE i.indrelid='qbit_share_ledger'::regclass AND i.indnkeyatts>=2 AND i.indisvalid \
       AND i.indpred IS NULL AND m.amname='btree' AND i.indoption[0]=i.indoption[1] \
       AND i.indoption[0] IN (0,3) AND a.attname='origin_node' AND b.attname='share_seq')";

/// A dual-writer snapshot's refusal on a ledger without that index
/// ([`ORIGIN_INDEX_SQL`]). Every refresh repeats it until the index is
/// valid, so it is typed: the refresh loop alerts on it once a minute, and
/// the `dual_writer_origin_index_missing` gauge holds 1 meanwhile.
#[derive(Debug, thiserror::Error)]
#[error("a dual-writer window cut needs a valid (origin_node, share_seq) index on qbit_share_ledger (migration 031); refusing to take one without it")]
pub struct OriginIndexMissing;

/// The peer's share-stream high-water mark (CONTRACT D-14, migration 027):
/// every row the peer originated with `share_seq` at or below it is committed
/// in this database but one the sync quarantined, and none can arrive later,
/// because the peer sync pulls the share stream in `share_seq` order and
/// writes the mark in the transaction that inserts the rows it covers. NULL
/// before the first pull.
pub(crate) const PEER_HIGH_WATER_SQL: &str = "SELECT qbit_prism_peer_share_mark()";

/// The peer's newest row at or below the high-water mark that is stamped at
/// or before the anchor. The walk runs over the peer's own rows only, on the
/// `(origin_node, share_seq)` index (shaped as [`OWN_CUT_SQL`] is), from the
/// mark down: the rows it passes are the peer's rows stamped after the
/// anchor, the few within the clock skew, however many of this node's rows
/// lie above the peer's newest. `$1` is the peer's index, `$2` the mark the
/// cutoff's statement read, `$3` the anchor.
///
/// The mark is read again in this statement, and both bound the walk: the
/// cutoff's statement ran in another transaction, so a mark rewound since
/// (a restore, or a repersonalise) never admits a peer row whose lower rows
/// this database no longer holds. The second column is the mark the entry
/// used, the lower of the two; a NULL mark either time admits no row and
/// uses none.
const PEER_CUT_SQL: &str = "SELECT (SELECT share_seq FROM qbit_share_ledger WHERE origin_node BETWEEN $1 AND $1 AND share_seq<=$2 AND share_seq<=qbit_prism_peer_share_mark() AND accepted AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000) ORDER BY origin_node DESC, share_seq DESC LIMIT 1),\
     CASE WHEN qbit_prism_peer_share_mark() IS NOT NULL THEN LEAST($2, qbit_prism_peer_share_mark()) END";

/// The peer's entry for a window anchored at `anchor_ms`, and the peer mark
/// it used ([`PEER_CUT_SQL`]): its newest synced row at or below both the
/// mark the cutoff's statement read, `high_water`, and the mark now, that
/// is stamped at or before the anchor. Every peer row at or below it is in
/// this database, but one the sync quarantined, and is stamped at or before
/// the anchor too (a node's stamps rise with its `share_seq`), so the anchor
/// rule never removes a row the cut admits, and the peer's rows above it
/// join the next windows.
pub(crate) async fn read_peer_cut(
    connection: &mut sqlx::PgConnection,
    peer: i16,
    high_water: Option<i64>,
    anchor_ms: i64,
) -> Result<(Option<u64>, Option<i64>), WindowError> {
    let Some(high_water) = high_water else {
        return Ok((None, None));
    };
    let (entry, used): (Option<i64>, Option<i64>) = sqlx::query_as(PEER_CUT_SQL)
        // Planned with its values, as every bounded ledger read here is.
        .persistent(false)
        .bind(peer)
        .bind(high_water)
        .bind(anchor_ms)
        .fetch_one(&mut *connection)
        .await?;
    Ok((entry.map_or(Ok(None), positive_entry)?, used))
}

/// Two more columns for [the holding probe](super::probe_window_holding) of
/// a window with a cut:
/// - whether each node's entry is this database's row of that node under the
///   window's predicate: own rows are appended, and recovered, in
///   `share_seq` order, so holding this node's entry holds every own row
///   below it;
/// - whether the peer sync's safe mark (D-14) is at or above the peer's
///   entry, so every peer row of the window is here: one the sync
///   quarantined is in no window taken here. The cursor row names the node
///   whose stream the mark covers, so a window read on either node is judged
///   from this database's side; before the first pull there is no cursor and
///   no peer row either.
///
/// An entry absent from the cut, or below the window's first row, has no row
/// in the window and holds trivially. `$first` is the window's first
/// `share_seq`, `$anchor` its anchor, and `$cut` and `$cut + 1` its entries.
pub(crate) fn entries_held_sql(first: usize, anchor: usize, cut: usize) -> String {
    let entries = format!(
        "(VALUES (0::smallint,${cut}::bigint),(1::smallint,${}::bigint)) e(node,share_seq)",
        cut + 1
    );
    let outside = format!("e.share_seq IS NULL OR e.share_seq<${first}");
    format!(
        "(SELECT COALESCE(bool_and({outside} OR EXISTS(SELECT 1 FROM qbit_share_ledger \
           WHERE share_seq=e.share_seq AND origin_node=e.node AND {})),false) FROM {entries}),\
         (SELECT COALESCE(bool_and({outside} OR e.node IS DISTINCT FROM \
           (SELECT peer_node FROM qbit_prism_peer_sync_cursors WHERE stream='shares') \
           OR COALESCE(qbit_prism_peer_share_mark()>=e.share_seq,false)),false) FROM {entries})",
        window_eligibility_sql(anchor, Some(cut))
    )
}

/// A cut entry from a `share_seq` or a cut column the database returned:
/// `0` is none (no row has `share_seq` 0), and a negative value is corruption.
pub(crate) fn positive_entry(value: i64) -> Result<Option<u64>, WindowError> {
    match u64::try_from(value) {
        Ok(0) => Ok(None),
        Ok(entry) => Ok(Some(entry)),
        Err(_) => Err(WindowError::Decode(anyhow::anyhow!(
            "negative share_seq {value} where a window cut entry belongs"
        ))),
    }
}

#[cfg(test)]
mod dual_writer_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_a_cut_the_predicate_is_the_anchored_one_verbatim() {
        for anchor in [1, 2, 3, 9] {
            assert_eq!(
                window_eligibility_sql(anchor, None),
                super::super::super::audit::anchored_eligibility_sql(anchor)
            );
        }
        // The text 3.0 issues, pinned: single-writer SQL must not move.
        assert_eq!(
            window_eligibility_sql(3, None),
            "accepted AND accepted_at<=to_timestamp($3::double precision/1000) AND job_issued_at<=to_timestamp($3::double precision/1000)"
        );
    }

    #[test]
    fn with_a_cut_each_node_is_bounded_by_its_own_entry() {
        assert_eq!(
            window_eligibility_sql(2, Some(5)),
            "accepted AND accepted_at<=to_timestamp($2::double precision/1000) AND job_issued_at<=to_timestamp($2::double precision/1000) \
             AND ((origin_node=0 AND share_seq<=$5) OR (origin_node=1 AND share_seq<=$6))"
        );
    }

    #[test]
    fn cut_columns_round_trip_and_keep_none_apart_from_empty() {
        for cut in [
            None,
            Some(WindowCut::default()),
            Some(WindowCut::new(Some(10), None).unwrap()),
            Some(WindowCut::new(None, Some(7)).unwrap()),
            Some(WindowCut::new(Some(10), Some(11)).unwrap()),
        ] {
            let (node_0, node_1) = cut_columns(cut.as_ref()).unwrap();
            assert_eq!(cut_from_columns(node_0, node_1).unwrap(), cut);
        }
        assert_eq!(
            cut_columns(Some(&WindowCut::default())).unwrap(),
            (Some(0), Some(0))
        );
        assert_eq!(cut_columns(None).unwrap(), (None, None));
        assert!(cut_from_columns(Some(1), None).is_err());
        assert!(cut_from_columns(None, Some(1)).is_err());
        assert!(cut_from_columns(Some(-1), Some(1)).is_err());
        let huge = WindowCut::new(Some(u64::MAX), None).unwrap();
        assert!(cut_columns(Some(&huge)).is_err());
        assert!(cut_values(&huge).is_err());
    }

    #[test]
    fn sequence_positions_become_entries() {
        assert_eq!(positive_entry(0).unwrap(), None);
        assert_eq!(positive_entry(5).unwrap(), Some(5));
        assert!(positive_entry(-1).is_err());
    }
}
