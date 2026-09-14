//! Stratum-to-PostgreSQL load harness for PRISM (issue #271).
//!
//! The harness drives real `qbit-prism-server run` child processes over real
//! Stratum sockets against a real PostgreSQL primary (optionally with one
//! streaming standby), and produces the `qbit-prism-capacity-evidence/v2`
//! artifact plus a side report that carries everything the artifact's schema
//! cannot express.
//!
//! Nothing here changes production code. Helpers copied from the server's own
//! test support name their source file inline.

// The side report is built as one `serde_json::json!` document, and the macro
// expands recursively once per key. The default limit of 128 is not enough.
#![recursion_limit = "512"]

pub mod artifact;
pub mod cadence;
pub mod classify;
pub mod cli;
pub mod client;
pub mod cluster;
pub mod digest;
pub mod frontend;
pub mod measure;
pub mod node;
pub mod profile;
pub mod provenance;
pub mod proxy;
pub mod report;
pub mod restart;
pub mod run;
pub mod window;
