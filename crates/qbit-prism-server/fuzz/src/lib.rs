//! In-process Stratum fuzz harness (#575).
//!
//! The targets drive the production per-connection loop,
//! `qbit_prism_server::stratum::serve_connection`, over an in-memory pipe
//! with [`FuzzBackend`], a `MiningBackend` that keeps every job it built and
//! credits a share the way the coordinator's credit gate does. No socket, no
//! database, no node. Every exchange is checked against the invariants in
//! [`checks`]: every frame gets exactly one answer, in order, with the
//! request's own id; errors use only the documented codes and reason IDs;
//! notifications describe work the backend really issued to that connection;
//! the connection closes only for a documented cause; and every credited
//! share is re-verified from the issued job and the raw submit fields by
//! code that shares nothing with the codec's submission path.

pub mod alloc;
pub mod backend;
pub mod checks;
pub mod driver;
pub mod lines;
pub mod parsers;
pub mod pow;
pub mod script;

pub use backend::FuzzBackend;

/// One current-thread runtime per thread, so an iteration costs a pipe and a
/// task, not a runtime, and the seed tests' threads never share a scheduler.
pub fn runtime() -> &'static tokio::runtime::Runtime {
    thread_local! {
        static RUNTIME: &'static tokio::runtime::Runtime = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("fuzz runtime"),
        ));
    }
    RUNTIME.with(|runtime| *runtime)
}

/// Stop the fuzzer on an invariant violation, naming it.
#[track_caller]
pub fn violation(message: impl std::fmt::Display) -> ! {
    panic!("stratum fuzz invariant violated: {message}")
}
