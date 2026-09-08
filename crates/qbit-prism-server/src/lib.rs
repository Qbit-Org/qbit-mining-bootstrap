//! Native PRISM runtime. PostgreSQL is the shared source of truth; each
//! frontend owns only its connections and immutable, bounded work cache.
pub mod api;
pub mod broadcaster;
pub mod codec;
pub mod config;
pub mod coordinator;
pub mod ledger;
pub mod readiness;
pub mod rollups;
pub mod rpc;
pub mod server;
pub mod stratum;
pub mod tools;
pub mod vardiff;

pub mod capacity;
