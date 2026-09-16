//! Restart mid-drain and a held lease at storm cardinality (#270, workstream A).
//!
//! Incident 3: a frontend that dies between its claim and its terminalization.
//! `crates/qbit-prism-server/src/coordinator/candidate_lease_tests.rs` already
//! covers the in-process form of this — a cancelled task with its lease still
//! live — but an aborted future proves nothing about recovery across a real
//! process death, and #266's one-`submitblock`-per-block-hash guarantee is
//! exactly what an in-process fake cannot test.
//!
//! So this target kills a process. The fake node runs here, in the parent, and
//! counts `submitblock` keyed by block hash, so the counter outlives every
//! child. The child is the real server binary
//! (`env!("CARGO_BIN_EXE_qbit-prism-server")`), configured entirely through
//! `PRISM_*` variables. The kill point belongs to the node, not to a timer:
//! the node receives a `submitblock`, records the hash and withholds the
//! reply, and the parent signals the child then — provably past the durable
//! `offer_reserved` reservation that `Ledger::reserve_offer` commits before
//! the one `submitblock` call, and provably short of terminalization, because
//! the child never learned the outcome.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test candidate_storm_restart -- --nocapture
//! ```

#[allow(dead_code)]
#[path = "support/storm_scale.rs"]
mod storm_scale;
