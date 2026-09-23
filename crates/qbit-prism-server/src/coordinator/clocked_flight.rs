//! One in-flight database clock+revision read shared by every issuance
//! boundary whose request was made before the read started.
//!
//! Each delivered job used to spend up to seven pool checkouts reading two
//! scalars of one snapshot: the database clock, which fixes its absolute
//! deadline, and the payout revision, which every revalidation compares with
//! the identity it admitted. During a work fan-out thousands of sessions ask
//! for the same two values within milliseconds, and the pool, not the
//! database, is what they wait for. A single read now serves every caller
//! whose request preceded the read.
//!
//! The join-after-request rule: a caller may share only a read that STARTED
//! at or after its own request instant, never an older in-flight or finished
//! one. `AbsoluteDeadline::from_database` subtracts a clock read from an
//! expiry and adds the remainder to the caller's own request instant, so a
//! clock read taken after the request can only shorten the deadline, never
//! extend it; a revision read taken after the request sees every bump the
//! caller could have raced with.
use super::work_ledger::ClockedRevision;
use anyhow::Result;
use futures_util::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::time::Instant as MonotonicInstant;

#[derive(Clone, Debug)]
struct SharedFailure(Arc<anyhow::Error>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

type Outcome = Result<ClockedRevision, SharedFailure>;

struct Flight {
    started: MonotonicInstant,
    outcome: Shared<BoxFuture<'static, Outcome>>,
}

#[derive(Default)]
pub(super) struct ClockedFlights {
    current: StdMutex<Option<Flight>>,
}

impl ClockedFlights {
    /// The clock and revision as of a read that started no earlier than
    /// `requested_at`. `read` performs one fresh database read; it runs at
    /// most once per flight, always to completion, and a failure is shared
    /// by every joiner, each of which keeps its own deadline and retries
    /// under its own admission.
    pub(super) async fn read<F>(
        &self,
        requested_at: MonotonicInstant,
        read: impl FnOnce() -> F,
    ) -> Result<ClockedRevision>
    where
        F: std::future::Future<Output = Result<ClockedRevision>> + Send + 'static,
    {
        let outcome = {
            let mut current = self.current.lock().unwrap();
            match current.as_ref() {
                Some(flight) if flight.started >= requested_at => flight.outcome.clone(),
                _ => {
                    // The start instant is taken under the lock, before the
                    // future exists, so no caller can observe a flight whose
                    // read began before the instant it advertises.
                    let started = MonotonicInstant::now();
                    let outcome = read()
                        .map(|result| result.map_err(|error| SharedFailure(Arc::new(error))))
                        .boxed()
                        .shared();
                    // A driver task runs the read to completion whether or
                    // not any caller is still awaiting it: a `Shared` future
                    // is polled only through a clone being awaited, so a
                    // leader cancelled with no joiner (session gone, initial
                    // job timeout) would otherwise leave the read parked on
                    // its pool checkout until a later request replaced it.
                    tokio::spawn(outcome.clone().map(|_| ()));
                    *current = Some(Flight {
                        started,
                        outcome: outcome.clone(),
                    });
                    outcome
                }
            }
        };
        outcome.await.map_err(anyhow::Error::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn reader(
        calls: &Arc<AtomicUsize>,
        value: i64,
        delay: Duration,
    ) -> impl FnOnce() -> BoxFuture<'static, Result<ClockedRevision>> {
        let calls = calls.clone();
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(delay).await;
                Ok(ClockedRevision {
                    now_ms: value,
                    payout_revision: value,
                })
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn callers_requesting_before_a_read_starts_share_it_and_later_callers_do_not() {
        let flights = Arc::new(ClockedFlights::default());
        let calls = Arc::new(AtomicUsize::new(0));
        // Two requests made before any read starts share one read.
        let first_request = MonotonicInstant::now();
        let second_request = MonotonicInstant::now();
        let a = {
            let flights = flights.clone();
            let read = reader(&calls, 1, Duration::from_millis(50));
            tokio::spawn(async move { flights.read(first_request, read).await })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        let b = flights
            .read(second_request, reader(&calls, 2, Duration::ZERO))
            .await
            .unwrap();
        let a = a.await.unwrap().unwrap();
        assert_eq!(
            (a.now_ms, b.now_ms),
            (1, 1),
            "the second request joined the first read"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // A request made after that read started must not join it, even
        // though the read has finished: it starts its own.
        let later_request = MonotonicInstant::now();
        let c = flights
            .read(later_request, reader(&calls, 3, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(c.now_ms, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_failed_read_is_shared_and_the_next_request_reads_again() {
        let flights = Arc::new(ClockedFlights::default());
        let request = MonotonicInstant::now();
        let failed = flights
            .read(request, || async { anyhow::bail!("pool closed") }.boxed())
            .await;
        assert!(failed.unwrap_err().to_string().contains("pool closed"));
        // A joiner of the failed flight sees the same failure...
        let joiner = flights
            .read(request, || {
                async { unreachable!("joined, not read") }.boxed()
            })
            .await;
        assert!(joiner.unwrap_err().to_string().contains("pool closed"));
        // ...and a request made afterwards performs a fresh read.
        let calls = Arc::new(AtomicUsize::new(0));
        let fresh = flights
            .read(MonotonicInstant::now(), reader(&calls, 9, Duration::ZERO))
            .await
            .unwrap();
        assert_eq!((fresh.now_ms, calls.load(Ordering::SeqCst)), (9, 1));
    }

    /// A reader that blocks on an explicit gate and holds a "pool slot"
    /// permit while blocked; ordering is driven by the test, no timers.
    fn gated_reader(
        calls: &Arc<AtomicUsize>,
        value: i64,
        gate: tokio::sync::oneshot::Receiver<()>,
        slots: Arc<tokio::sync::Semaphore>,
    ) -> impl FnOnce() -> BoxFuture<'static, Result<ClockedRevision>> {
        let calls = calls.clone();
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                let _slot = slots.acquire_owned().await.unwrap();
                gate.await.ok();
                Ok(ClockedRevision {
                    now_ms: value,
                    payout_revision: value,
                })
            }
            .boxed()
        }
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn a_request_made_after_an_in_flight_read_started_does_not_join_it() -> Result<()> {
        let flights = Arc::new(ClockedFlights::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let slots = Arc::new(tokio::sync::Semaphore::new(4));
        let (release, gate) = tokio::sync::oneshot::channel();
        let leader_request = MonotonicInstant::now();
        let leader = {
            let flights = flights.clone();
            let read = gated_reader(&calls, 1, gate, slots.clone());
            tokio::spawn(async move { flights.read(leader_request, read).await })
        };
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "leader's read started");
        let later = flights
            .read(MonotonicInstant::now(), reader(&calls, 2, Duration::ZERO))
            .await?;
        assert_eq!(
            later.now_ms, 2,
            "a request after the start took its own read"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        release.send(()).unwrap();
        assert_eq!(leader.await.unwrap()?.now_ms, 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_joiner_completes_the_shared_read_after_the_leader_is_cancelled() -> Result<()> {
        let flights = Arc::new(ClockedFlights::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let slots = Arc::new(tokio::sync::Semaphore::new(4));
        let (release, gate) = tokio::sync::oneshot::channel();
        let joiner_request = MonotonicInstant::now();
        let leader_request = MonotonicInstant::now();
        let leader = {
            let flights = flights.clone();
            let read = gated_reader(&calls, 1, gate, slots.clone());
            tokio::spawn(async move { flights.read(leader_request, read).await })
        };
        settle().await;
        let joiner = {
            let flights = flights.clone();
            let read = reader(&calls, 99, Duration::ZERO);
            tokio::spawn(async move { flights.read(joiner_request, read).await })
        };
        settle().await;
        leader.abort();
        let _ = leader.await;
        settle().await;
        assert!(
            !joiner.is_finished(),
            "joiner still waits on the shared read"
        );
        release.send(()).unwrap();
        assert_eq!(
            joiner.await.unwrap()?.now_ms,
            1,
            "the joiner got the leader's read"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no second read was issued");
        Ok(())
    }

    #[tokio::test]
    async fn a_cancelled_leader_with_no_joiner_still_runs_its_read_to_completion() -> Result<()> {
        let flights = Arc::new(ClockedFlights::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let leader_request = MonotonicInstant::now();
        let leader = {
            let flights = flights.clone();
            let read = gated_reader(&calls, 1, gate, slots.clone());
            tokio::spawn(async move { flights.read(leader_request, read).await })
        };
        settle().await;
        assert_eq!(slots.available_permits(), 0, "the read holds the only slot");
        leader.abort();
        let _ = leader.await;
        // With no caller left, the driver task still finishes the read and
        // releases its slot as soon as the database answers; no later
        // request is needed to unpark it.
        release.send(()).unwrap();
        settle().await;
        assert_eq!(
            slots.available_permits(),
            1,
            "the abandoned read completed and released its slot"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
