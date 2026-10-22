//! One serial lock for every coordinator test module that opens a ledger.
//!
//! The ledger's advisory locks are cluster-wide constants, not schema-scoped,
//! so two test modules running in parallel contend for `ORDER_LOCK` even though
//! each works in its own throwaway schema. Each module used to hold a lock of
//! its own, which serialised it against itself and left every cross-module wait
//! to `lock_timeout` (5 s by default). #324's reconciliation tests hold the lock
//! for up to about 1.5 s, so once enough modules ran at once those waits stacked
//! past the timeout and the suite failed intermittently with "canceling
//! statement due to lock timeout" in whichever module lost the race.
//!
//! Every module that opens a ledger takes this instead. It costs wall clock,
//! since those modules no longer overlap, and buys a suite that does not fail
//! for a reason unrelated to what it is testing.
use super::*;

pub(super) static TEST_LOCK: Mutex<()> = Mutex::const_new(());
