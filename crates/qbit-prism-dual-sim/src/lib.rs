//! A two-node PRISM system simulation for the 3.1 dual writer
//! (CONTRACT.md §4 and §5 of the 3.1 effort; README.md in this crate).
//!
//! The simulation runs, on one host, everything a pair runs: a PostgreSQL 16
//! cluster per node, a regtest `qbitd` per node plus one for the rest of the
//! network, a `qbit-prism-server` frontend per node, a balancer stand-in
//! that routes Stratum sessions by each node's HTTP health, and real
//! Stratum load through it. A fault injector breaks any part of it, and an
//! invariant checker then reads both databases, the chain and every share
//! the miners saw acknowledged, and holds them to the payout invariants.
//!
//! Nothing here is production code; the servers it drives are the real
//! binaries, launched with the settings a scenario names.

pub mod balancer;
pub mod chain;
pub mod faults;
pub mod frontend;
pub mod invariants;
pub mod load;
pub mod measure;
pub mod postgres;
pub mod process;
pub mod relay;
pub mod report;
pub mod rpc_gate;
pub mod scenarios;
pub mod sim;
