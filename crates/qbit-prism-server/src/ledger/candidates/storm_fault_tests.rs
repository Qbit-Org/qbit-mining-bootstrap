//! Recovery at each cleanup dependency of a drained candidate (#270).
//!
//! #270 names four dependencies a drain leans on — the chain probe, the
//! landing transaction, the terminal update and the lease renewal — and asks
//! for one recovery case each, leaving the row in a consistent state. Those
//! are native seams: 2.x.x's fault vocabulary was eight cleanup steps of the
//! set-oriented collapse selector (`lab/prism/block_candidates.py`), and #270
//! fences that selector out of scope, so the mapping is derived here rather
//! than ported.
//!
//! The faults in [`super::faults`] are #387's quarantine vocabulary and sit on
//! the decode and park path; they cover none of the four, and they stay where
//! they are. They are reused only where a fault interacts with quarantine.
//!
//! Each case asserts the row is left claimable with its offer record intact,
//! its `attempt_count` incremented exactly once, and no second `submitblock`
//! reachable for that hash. Faults are keyed by block hash, so a case claiming
//! one row never meets another case's fault.
