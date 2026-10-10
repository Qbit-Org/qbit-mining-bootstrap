//! Which of the two 3.1 dual-writer nodes a frontend's database is, and
//! what that identity fixes. Every row a node originates in a copied table
//! carries its index in `origin_node` (pre-3.1 rows are node 0), its
//! `share_seq` values have its parity, and its extranonce1 values come from
//! its half of the four-byte space, so the keys two nodes allocate never
//! collide. Migration 027 adds the columns; personalisation
//! (`Ledger::personalise_node`) fixes them for one database.
use serde::{Deserialize, Serialize};
use std::ops::RangeInclusive;

/// Node A (`0`) or node B (`1`). Serialized as its index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum NodeIndex {
    A,
    B,
}

impl NodeIndex {
    pub const ALL: [Self; 2] = [Self::A, Self::B];

    /// `0` or `1`, any other value `None`.
    pub fn from_index(index: i64) -> Option<Self> {
        match index {
            0 => Some(Self::A),
            1 => Some(Self::B),
            _ => None,
        }
    }

    /// The value stored in `origin_node`.
    pub fn index(self) -> i16 {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    pub fn peer(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    /// The node's name in operator text: `A` or `B`.
    pub fn name(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
        }
    }

    /// The residue modulo 2 of every `share_seq` this node allocates once
    /// personalised: node A allocates even values, node B odd. Rows from
    /// before the database was personalised keep the values a single writer
    /// gave them, so parity identifies a row's node only after that point;
    /// `origin_node` always does.
    pub fn share_seq_residue(self) -> i64 {
        i64::from(self.index())
    }

    /// The smallest value at or above `floor` with this node's parity.
    pub fn share_seq_at_or_above(self, floor: i64) -> i64 {
        if floor.rem_euclid(2) == self.share_seq_residue() {
            floor
        } else {
            floor + 1
        }
    }

    /// The extranonce1 values this node's session sequence hands out: node A
    /// `[1, 2^31-1]`, node B `[2^31, 2^32-1]`. Migration 009's reservations
    /// keep a wrapped value from being reused within each half.
    pub fn extranonce1_range(self) -> RangeInclusive<u32> {
        match self {
            Self::A => 1..=0x7fff_ffff,
            Self::B => 0x8000_0000..=0xffff_ffff,
        }
    }
}

impl std::fmt::Display for NodeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl Serialize for NodeIndex {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i16(self.index())
    }
}

impl<'de> Deserialize<'de> for NodeIndex {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let index = i64::deserialize(deserializer)?;
        Self::from_index(index)
            .ok_or_else(|| serde::de::Error::custom(format!("node index {index} is not 0 or 1")))
    }
}

/// This node's identity as configured: `PRISM_NODE_INDEX` and
/// `PRISM_CARRY_OWNER`. Exactly one of the two nodes is the carry owner,
/// the only one that pays down carried balances.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentity {
    #[serde(rename = "node_index")]
    pub node: NodeIndex,
    pub carry_owner: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_nodes_are_each_others_peer_and_serialize_as_their_index() {
        assert_eq!(NodeIndex::A.peer(), NodeIndex::B);
        assert_eq!(NodeIndex::B.peer(), NodeIndex::A);
        for node in NodeIndex::ALL {
            assert_eq!(NodeIndex::from_index(node.index().into()), Some(node));
            assert_eq!(
                serde_json::to_value(node).unwrap(),
                serde_json::json!(node.index())
            );
            assert_eq!(
                serde_json::from_value::<NodeIndex>(serde_json::json!(node.index())).unwrap(),
                node
            );
        }
        assert_eq!(NodeIndex::from_index(2), None);
        assert_eq!(NodeIndex::from_index(-1), None);
        assert!(serde_json::from_value::<NodeIndex>(serde_json::json!(2)).is_err());
        assert_eq!(
            serde_json::to_value(NodeIdentity {
                node: NodeIndex::B,
                carry_owner: false
            })
            .unwrap(),
            serde_json::json!({"node_index": 1, "carry_owner": false})
        );
    }

    #[test]
    fn share_seq_parity_splits_the_sequence_between_the_nodes() {
        for floor in [-3i64, 0, 1, 2, 41, 1 << 40] {
            for node in NodeIndex::ALL {
                let value = node.share_seq_at_or_above(floor);
                assert!(value == floor || value == floor + 1);
                assert_eq!(value.rem_euclid(2), node.share_seq_residue());
            }
            assert_ne!(
                NodeIndex::A.share_seq_at_or_above(floor),
                NodeIndex::B.share_seq_at_or_above(floor)
            );
        }
    }

    #[test]
    fn extranonce1_ranges_split_the_four_byte_space_without_zero() {
        let a = NodeIndex::A.extranonce1_range();
        let b = NodeIndex::B.extranonce1_range();
        assert_eq!(*a.start(), 1, "0 is never a session id");
        assert_eq!(*a.end() + 1, *b.start(), "the halves are adjacent");
        assert_eq!(*b.end(), u32::MAX);
        assert_eq!(*b.start(), 1 << 31);
    }
}
