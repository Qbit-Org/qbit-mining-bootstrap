//! Drain cost and admission at storm cardinality (#270, workstream A).
//!
//! The 2026-08-20 testnet4 incident left 3,120 durable block candidates behind
//! one decided height. 2.x.x answered with a Python instrument that measured
//! the per-row drain cost of that sibling set; #244 deleted it with no native
//! peer. This target ports the *properties*, not the harness: the native
//! outbox claims one row per query (`Ledger::claim_lane_sql`, `LIMIT 1` with
//! `FOR UPDATE SKIP LOCKED`), so the aggregate-page storm cannot recur in its
//! original form, but N siblings still cost a claim, a lease, a chain probe
//! and a durable write each.
//!
//! Nothing here asserts a duration. Every asserted quantity is an integer from
//! a counter the test owns — node calls per block hash, claims issued, the
//! exact terminal set, statements observed on the wire — plus the plan shape
//! of the statements the server actually issues. Durations are recorded under
//! [`storm_scale::REPORT_PREFIX`] and never asserted. The per-row cost is
//! asserted *equal* at two cardinalities measured in the same process, so the
//! reduced-N CI path proves the same property as a local run at 3,120.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test candidate_storm -- --nocapture
//! ```

#[allow(dead_code)]
#[path = "support/storm_scale.rs"]
mod storm_scale;
