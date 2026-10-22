use super::*;

#[test]
fn candidate_construction_yields_before_copying_issued_balances() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let fixture = Fixture::new(Duration::from_secs(10)).await;
            let mut job = fixture.job(1, 0, "original.worker");
            let share = fixture.original(&job.context.prepared).snapshot.shares[0].clone();
            let prepared =
                Arc::get_mut(&mut Arc::get_mut(&mut job.context).unwrap().prepared).unwrap();
            *Arc::make_mut(&mut Arc::get_mut(&mut prepared.reservation).unwrap().balances) = (0
                ..300_000)
                .map(|n| qbit_prism::CarryForwardBalance {
                    order_key: format!("recipient-{n:06}"),
                    recipient_id: format!("miner-{n:06}"),
                    p2mr_program_hex: hash(0xab),
                    balance_sats: n + 1,
                })
                .collect();
            for share_pass in [true, false] {
                let mut proof = fixture.proof(&job, 0);
                proof.share_pass = share_pass;
                let block_bytes = hex::decode(&proof.block_hex).unwrap();
                // Occupy the sole blocking thread. Candidate construction must
                // yield without walking the balance set on this runtime.
                let (release, wait) = std::sync::mpsc::channel::<()>();
                let (entered, ready) = tokio::sync::oneshot::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    entered.send(()).unwrap();
                    let _ = wait.recv();
                });
                ready.await.unwrap();
                let candidate = miner_submit::submission_candidate(
                    &job,
                    proof,
                    (!share_pass).then(|| share.clone()),
                );
                tokio::pin!(candidate);
                let started = Instant::now();
                assert!(
                    futures_util::poll!(&mut candidate).is_pending(),
                    "candidate construction copied the issued set on its caller"
                );
                eprintln!(
                    "300,000-balance candidate first poll: {:?}",
                    started.elapsed()
                );
                tokio::task::yield_now().await;
                drop(release);
                blocker.await.unwrap();
                let candidate = candidate.await.unwrap();
                assert_eq!(
                    candidate.as_issued_balances,
                    *job.context.prepared.reservation.balances
                );
                assert_eq!(candidate.block_bytes, block_bytes);
                assert_eq!(
                    candidate.block_sha256,
                    Candidate::block_digest_hex(&block_bytes)
                );
                assert_eq!(candidate.window, job.context.prepared.window);
                assert_eq!(
                    candidate.payout_revision,
                    job.context.prepared.snapshot.payout_revision
                );
                assert_eq!(
                    candidate.deferred_share,
                    (!share_pass).then(|| share.clone())
                );
            }
        });
}
