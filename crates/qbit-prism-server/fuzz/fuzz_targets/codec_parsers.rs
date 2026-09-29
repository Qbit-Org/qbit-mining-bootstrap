//! The codec's transaction, block and target parsers on arbitrary bytes (#575).
#![no_main]
use libfuzzer_sys::fuzz_target;

#[global_allocator]
static ALLOCATOR: qbit_prism_server_fuzz::alloc::Counting = qbit_prism_server_fuzz::alloc::Counting;

fuzz_target!(|data: &[u8]| qbit_prism_server_fuzz::parsers::run(data));
