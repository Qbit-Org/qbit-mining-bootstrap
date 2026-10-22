//! The line-codec target: arbitrary bytes, delivered in arbitrary fragments,
//! into one connection. The frame model predicts every answer from the bytes
//! alone: one per complete frame, in order, with the frame's own id; a null-id
//! `malformed-submit` for anything that is not a JSON object; one size-limit
//! refusal and a close for a frame over the bound; silence for a trailing
//! partial frame. Fragmentation must change none of it.
//!
//! Input: byte 0 picks the message bound, byte 1 the malformed-frame budget
//! (0 disables it), byte 2 seeds the fragment sizes (0 sends one write), and
//! the rest is the stream.
use crate::{alloc, backend::FuzzBackend, checks, driver::Harness, runtime};
use qbit_prism_server::stratum::StratumConfig;

const BOUNDS: [usize; 8] = [256, 512, 1024, 2048, 4096, 8192, 16384, 16384];

pub fn config(max_message_bytes: usize, malformed_budget: u32) -> StratumConfig {
    StratumConfig {
        max_message_bytes,
        max_malformed_frames_per_interval: malformed_budget,
        ..Default::default()
    }
}

pub fn run(data: &[u8]) {
    let [bound, budget, seed, stream @ ..] = data else {
        return;
    };
    let max = BOUNDS[*bound as usize % BOUNDS.len()];
    let config = config(max, u32::from(budget % 4));
    let (frames, oversize) = checks::frames(stream, max);
    // Nothing the server holds may grow with the stream beyond one frame's
    // worth of parsing and answers per frame.
    let limit = (8 << 20) + 64 * stream.len();
    alloc::bounded(limit, "a line-codec iteration", || {
        runtime().block_on(async {
            let backend = FuzzBackend::new(config.extranonce2_size, 0x207fffff, 1);
            let mut harness = Harness::new(config, backend.clone());
            let mut conn = harness.connect();
            conn.send(&[], frames.iter().map(|f| checks::Frame::classify(f)))
                .await;
            let mut rng = u32::from(*seed) | 0x9e37_0000;
            let mut rest = stream;
            while !rest.is_empty() {
                let take = if *seed == 0 {
                    rest.len()
                } else {
                    rng ^= rng << 13;
                    rng ^= rng >> 17;
                    rng ^= rng << 5;
                    1 + (rng % 64) as usize
                }
                .min(rest.len());
                conn.send(&rest[..take], []).await;
                rest = &rest[take..];
                // Let the server consume the fragment before the next one.
                tokio::task::yield_now().await;
            }
            let state = conn.close().await;
            match (oversize, state.closing) {
                (true, None) => crate::violation("an oversize frame was not refused"),
                (false, Some("oversize")) => {
                    crate::violation("a frame within the bound was refused")
                }
                _ => {}
            }
            checks::credits(&backend, &harness.accepted);
        })
    });
}
