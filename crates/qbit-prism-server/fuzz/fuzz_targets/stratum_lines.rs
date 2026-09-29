//! Stratum line codec: arbitrary bytes in arbitrary fragments (#575).
#![no_main]
use libfuzzer_sys::fuzz_target;

#[global_allocator]
static ALLOCATOR: qbit_prism_server_fuzz::alloc::Counting = qbit_prism_server_fuzz::alloc::Counting;

fuzz_target!(|data: &[u8]| qbit_prism_server_fuzz::lines::run(data));
