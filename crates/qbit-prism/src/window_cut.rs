//! The per-node cut of a dual-writer payout window (PRISM 3.1).

use serde::de::{Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// A dual-writer payout window's per-node cut: for each node, the highest
/// `share_seq` of that node's ledger rows the window may include, or `None`
/// when it includes none of them.
///
/// In dual-writer mode node 0 (A) and node 1 (B) each append to their own
/// database and pull the rows the other originated. Their `share_seq` values
/// never collide, but a peer's row can reach a node after that node's own
/// higher-numbered rows: a late insert. A window bounded only by its anchor
/// timestamp would then change when the late row arrived. A row of node `n`
/// belongs to a window with a cut only if its `share_seq` is at most `cut[n]`,
/// so a row that arrives after the cut was taken is outside that window by
/// definition and joins later ones.
///
/// The anchor rule (`accepted_at` and `job_issued_at` at or before the
/// anchor) still applies to every row, as the fold in this crate applies it
/// to every bundle share. The builder chooses the cut so that the rule never
/// removes a row the cut admits: its own node's entry is its newest row when
/// the anchor was taken, and the peer's is the peer's newest synced row
/// stamped at or before the anchor. Selection and every later proof apply the
/// same predicate to immutable rows, so clock skew between the nodes can
/// delay a peer's shares but never make a window irreproducible.
///
/// A window without a cut (single-writer mode, and every artifact built
/// before 3.1) means exactly what it always has; every structure that carries
/// one omits the field when it is `None`, so those bytes are unchanged.
///
/// Serialized as `{"0":<share_seq or null>,"1":<share_seq or null>}`. Only
/// that object is accepted: both keys, once each, nothing else, and not the
/// array form serde would otherwise take for a struct. An entry is never `0`:
/// `share_seq` starts at 1, so "no rows of this node" has the one spelling
/// `null`, and every cut has one encoding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize)]
pub struct WindowCut {
    #[serde(rename = "0")]
    node_0: Option<u64>,
    #[serde(rename = "1")]
    node_1: Option<u64>,
}

/// Why a [`WindowCut`] could not be made.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WindowCutError {
    #[error("a window cut entry must be a share_seq of at least 1, or null")]
    ZeroEntry,
    #[error("node index {0} is not 0 or 1")]
    UnknownNode(u8),
}

impl<'de> Deserialize<'de> for WindowCut {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CutVisitor;

        impl<'de> Visitor<'de> for CutVisitor {
            type Value = WindowCut;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .write_str(r#"a window cut {"0": share_seq or null, "1": share_seq or null}"#)
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<WindowCut, A::Error> {
                let (mut node_0, mut node_1) = (None, None);
                while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                    let (slot, name) = match key.as_ref() {
                        "0" => (&mut node_0, "0"),
                        "1" => (&mut node_1, "1"),
                        other => return Err(A::Error::unknown_field(other, &["0", "1"])),
                    };
                    if slot.is_some() {
                        return Err(A::Error::duplicate_field(name));
                    }
                    let entry: Option<u64> = map.next_value()?;
                    if entry == Some(0) {
                        return Err(A::Error::custom(WindowCutError::ZeroEntry));
                    }
                    *slot = Some(entry);
                }
                Ok(WindowCut {
                    node_0: node_0.ok_or_else(|| A::Error::missing_field("0"))?,
                    node_1: node_1.ok_or_else(|| A::Error::missing_field("1"))?,
                })
            }
        }

        deserializer.deserialize_map(CutVisitor)
    }
}

impl WindowCut {
    /// The number of nodes a cut has an entry for.
    pub const NODES: u8 = 2;

    /// A cut with these entries; `Some(0)` is refused, because no row has
    /// `share_seq` 0 and "none" is spelled `None`.
    pub fn new(node_0: Option<u64>, node_1: Option<u64>) -> Result<Self, WindowCutError> {
        if node_0 == Some(0) || node_1 == Some(0) {
            return Err(WindowCutError::ZeroEntry);
        }
        Ok(Self { node_0, node_1 })
    }

    /// The entry for `node`.
    pub fn get(&self, node: u8) -> Result<Option<u64>, WindowCutError> {
        match node {
            0 => Ok(self.node_0),
            1 => Ok(self.node_1),
            other => Err(WindowCutError::UnknownNode(other)),
        }
    }

    /// This cut with `node`'s entry replaced.
    pub fn with(self, node: u8, entry: Option<u64>) -> Result<Self, WindowCutError> {
        match node {
            0 => Self::new(entry, self.node_1),
            1 => Self::new(self.node_0, entry),
            other => Err(WindowCutError::UnknownNode(other)),
        }
    }

    /// Node 0's entry.
    pub fn node_0(&self) -> Option<u64> {
        self.node_0
    }

    /// Node 1's entry.
    pub fn node_1(&self) -> Option<u64> {
        self.node_1
    }

    /// The highest entry: no row the cut admits has a larger `share_seq`.
    /// `None` when the cut admits no row at all.
    pub fn top(&self) -> Option<u64> {
        self.node_0.max(self.node_1)
    }

    /// Whether a row of `origin_node` with `share_seq` is inside the cut.
    /// The cut alone: the caller applies the anchor rule and `accepted`.
    pub fn admits(&self, origin_node: u8, share_seq: u64) -> bool {
        self.get(origin_node)
            .ok()
            .flatten()
            .is_some_and(|entry| share_seq <= entry)
    }

    /// Whether every row this cut admits is also admitted by `self`: each of
    /// `self`'s entries is at least `earlier`'s. Cuts taken later on one node
    /// always cover the cuts taken before them.
    pub fn covers(&self, earlier: &WindowCut) -> bool {
        self.node_0 >= earlier.node_0 && self.node_1 >= earlier.node_1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_both_keys_in_node_order_with_null_for_none() {
        let cut = WindowCut::new(Some(1000), None).unwrap();
        assert_eq!(
            serde_json::to_string(&cut).unwrap(),
            r#"{"0":1000,"1":null}"#
        );
        let both = WindowCut::new(Some(4), Some(7)).unwrap();
        assert_eq!(serde_json::to_string(&both).unwrap(), r#"{"0":4,"1":7}"#);
        assert_eq!(
            serde_json::to_string(&WindowCut::default()).unwrap(),
            r#"{"0":null,"1":null}"#
        );
    }

    #[test]
    fn round_trips_and_accepts_keys_in_any_order() {
        for cut in [
            WindowCut::default(),
            WindowCut::new(Some(1), None).unwrap(),
            WindowCut::new(None, Some(u64::MAX)).unwrap(),
            WindowCut::new(Some(10), Some(11)).unwrap(),
        ] {
            let json = serde_json::to_string(&cut).unwrap();
            assert_eq!(serde_json::from_str::<WindowCut>(&json).unwrap(), cut);
        }
        let reordered: WindowCut = serde_json::from_str(r#"{"1":3,"0":2}"#).unwrap();
        assert_eq!(reordered, WindowCut::new(Some(2), Some(3)).unwrap());
        let value = serde_json::json!({"0": null, "1": 9});
        assert_eq!(
            serde_json::from_value::<WindowCut>(value).unwrap(),
            WindowCut::new(None, Some(9)).unwrap()
        );
    }

    #[test]
    fn refuses_missing_extra_zero_and_malformed_entries() {
        for bad in [
            r#"{"0":1}"#,
            r#"{"1":1}"#,
            r#"{}"#,
            r#"{"0":1,"1":2,"2":3}"#,
            r#"{"0":0,"1":2}"#,
            r#"{"0":1,"1":0}"#,
            r#"{"0":-1,"1":2}"#,
            r#"{"0":"1","1":2}"#,
            r#"{"0":1.5,"1":2}"#,
            r#"[1,2]"#,
            r#"[1,null]"#,
            r#"null"#,
            r#"7"#,
            r#"{"0":1,"0":2,"1":3}"#,
        ] {
            assert!(
                serde_json::from_str::<WindowCut>(bad).is_err(),
                "{bad} must be refused"
            );
            // Documents are also decoded from an already parsed value, which
            // cannot hold a duplicate key: parsing it keeps the last one.
            let duplicate_key = bad.matches(r#""0":"#).count() > 1;
            if let (false, Ok(value)) = (
                duplicate_key,
                serde_json::from_str::<serde_json::Value>(bad),
            ) {
                assert!(
                    serde_json::from_value::<WindowCut>(value).is_err(),
                    "{bad} must be refused as a value too"
                );
            }
        }
        assert_eq!(
            WindowCut::new(Some(0), None),
            Err(WindowCutError::ZeroEntry)
        );
        assert_eq!(
            WindowCut::new(None, Some(0)),
            Err(WindowCutError::ZeroEntry)
        );
    }

    #[test]
    fn admits_only_its_own_nodes_rows_at_or_below_their_entry() {
        let cut = WindowCut::new(Some(10), Some(5)).unwrap();
        assert!(cut.admits(0, 1));
        assert!(cut.admits(0, 10));
        assert!(!cut.admits(0, 11));
        assert!(cut.admits(1, 5));
        assert!(!cut.admits(1, 6));
        // A row's node decides which entry bounds it, not its size.
        assert!(!cut.admits(1, 7));
        assert!(!cut.admits(2, 1));
        let only_a = WindowCut::new(Some(10), None).unwrap();
        assert!(!only_a.admits(1, 1));
        assert!(!WindowCut::default().admits(0, 1));
    }

    #[test]
    fn top_get_with_and_covers() {
        let cut = WindowCut::new(Some(10), Some(15)).unwrap();
        assert_eq!(cut.top(), Some(15));
        assert_eq!(WindowCut::new(Some(10), None).unwrap().top(), Some(10));
        assert_eq!(WindowCut::default().top(), None);
        assert_eq!(cut.get(0), Ok(Some(10)));
        assert_eq!(cut.get(1), Ok(Some(15)));
        assert_eq!(cut.get(2), Err(WindowCutError::UnknownNode(2)));
        assert_eq!(
            cut.with(1, None).unwrap(),
            WindowCut::new(Some(10), None).unwrap()
        );
        assert_eq!(cut.with(0, Some(0)), Err(WindowCutError::ZeroEntry));
        let later = WindowCut::new(Some(12), Some(15)).unwrap();
        assert!(later.covers(&cut));
        assert!(!cut.covers(&later));
        assert!(cut.covers(&cut));
        assert!(cut.covers(&WindowCut::default()));
        let peer_regressed = WindowCut::new(Some(20), Some(14)).unwrap();
        assert!(!peer_regressed.covers(&cut));
    }
}
