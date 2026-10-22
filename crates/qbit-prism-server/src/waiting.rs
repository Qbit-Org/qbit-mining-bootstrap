//! #655: which dependency work that has its own deadline was waiting on when
//! that deadline passed.
//!
//! A deadline drops the work it times, so the work cannot say why it was
//! late. Instead the work names each step that waits on the ledger database
//! as it runs, into a [`Waiting`] its timer keeps and reads when the deadline
//! passes, while the work is still pending ([`timeout`]; dropping the work
//! ends its marks). The Stratum session's own timeouts (`session allocation
//! timed out`, `job persistence timed out`, `job resume timed out`, ...) and
//! a template refresh that outlives its deadline label their refusals from
//! it: `backend-database-unavailable` while the work waited on the database,
//! otherwise `backend-rpc-unavailable`, the label every such refusal had
//! before #655.
//!
//! The tracker reaches the work through a task-local, as a public read's
//! remaining budget does (`api::public_service::READ_DEADLINE`), so no
//! backend signature carries it, and outside a tracked task a mark does
//! nothing. A mark lands in the tracker of the task that polls the step, so
//! only a task's own async code marks. Work shared between tasks (a
//! `Shared` future, a coalesced flight) is polled by whichever waiter polls
//! it: a waiter marks its own await of shared database work, and shared work
//! with steps of its own runs [`Waiting::track`]ed by its own tracker, which
//! each waiter [`follow`]s while it waits.
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

tokio::task_local! {
    static WAITING: Waiting;
}

/// The dependency tracked work is waiting on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dependency {
    /// Nothing the work named: the node, or a step that is neither, such as
    /// CPU work or admission behind other work.
    Unattributed,
    /// The ledger database.
    Database,
}

/// What one piece of timed work is waiting on, shared between the work and
/// whoever times it. Cloning shares the tracker.
#[derive(Clone, Debug, Default)]
pub struct Waiting(Arc<Mutex<State>>);

#[derive(Debug, Default)]
struct State {
    /// Database steps begun and not yet finished. A count, not a flag, so
    /// steps that overlap within one task still read as the database's until
    /// the last of them ends.
    database: usize,
    /// The trackers of the shared work this work is awaiting.
    following: Vec<Waiting>,
}

/// How many trackers a reading passes through. Shared work follows nothing,
/// so a reading takes at most two; the bound only stops a mistaken cycle.
const MAX_FOLLOWED: usize = 8;

impl Waiting {
    fn state(&self) -> MutexGuard<'_, State> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether `other` is this tracker, not merely one in the same state.
    pub fn same_as(&self, other: &Waiting) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Run `work` tracked here: the steps it marks, in this task, land in
    /// this tracker.
    pub async fn track<F: Future>(&self, work: F) -> F::Output {
        WAITING.scope(self.clone(), work).await
    }

    /// What the work is waiting on now: the database while a step it marked
    /// has not finished, or while shared work it follows is waiting on it.
    pub fn on(&self) -> Dependency {
        let mut reading = vec![self.clone()];
        for _ in 0..MAX_FOLLOWED {
            let mut next = Vec::new();
            for tracker in &reading {
                let state = tracker.state();
                if state.database > 0 {
                    return Dependency::Database;
                }
                next.extend(state.following.iter().cloned());
            }
            if next.is_empty() {
                break;
            }
            reading = next;
        }
        Dependency::Unattributed
    }
}

/// Await `work` for at most `limit`, tracked by a tracker of its own: its
/// output, or, once `limit` has passed, what it was waiting on then. The
/// reading is taken while the work is still pending, before it is dropped,
/// because dropping it ends its marks.
pub async fn timeout<F: Future>(limit: Duration, work: F) -> Result<F::Output, Dependency> {
    let waiting = Waiting::default();
    let work = waiting.track(work);
    tokio::pin!(work);
    tokio::time::timeout(limit, &mut work)
        .await
        .map_err(|_| waiting.on())
}

/// Await `step`, which waits on the ledger database: a deadline that passes
/// before it finishes is the database's. Outside tracked work it only awaits.
pub async fn on_database<F: Future>(step: F) -> F::Output {
    let _mark = WAITING
        .try_with(|tracker| {
            tracker.state().database += 1;
            DatabaseMark(tracker.clone())
        })
        .ok();
    step.await
}

/// Await `step`, shared work tracked by `shared`, reading through to that
/// tracker meanwhile: a deadline that passes while the shared work waits on
/// the database is the database's.
pub async fn follow<F: Future>(shared: &Waiting, step: F) -> F::Output {
    let _following = WAITING
        .try_with(|tracker| {
            tracker.state().following.push(shared.clone());
            Following {
                tracker: tracker.clone(),
                shared: shared.clone(),
            }
        })
        .ok();
    step.await
}

/// One database step's mark, ended when the step finishes or is dropped, on
/// the tracker it was made on.
struct DatabaseMark(Waiting);

impl Drop for DatabaseMark {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.database = state.database.saturating_sub(1);
    }
}

/// One await of shared work, ended the same way.
struct Following {
    tracker: Waiting,
    shared: Waiting,
}

impl Drop for Following {
    fn drop(&mut self) {
        let mut state = self.tracker.state();
        if let Some(index) = state
            .following
            .iter()
            .position(|followed| followed.same_as(&self.shared))
        {
            state.following.swap_remove(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::time::Duration;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn a_database_step_is_the_database_until_it_finishes() {
        let waiting = Waiting::default();
        let entered = Notify::new();
        let release = Notify::new();
        let work = waiting.track(async {
            assert_eq!(WAITING.with(Waiting::on), Dependency::Unattributed);
            on_database(async {
                entered.notify_one();
                release.notified().await;
            })
            .await;
            // A later step that is not the database's.
            std::future::pending::<()>().await;
        });
        tokio::pin!(work);
        tokio::select! {
            () = &mut work => unreachable!(),
            () = entered.notified() => {}
        }
        assert_eq!(waiting.on(), Dependency::Database);
        release.notify_one();
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut work)
            .await
            .is_err());
        assert_eq!(waiting.on(), Dependency::Unattributed);
    }

    #[tokio::test]
    async fn a_dropped_step_ends_its_mark() {
        let waiting = Waiting::default();
        let mut step = Box::pin(waiting.track(on_database(std::future::pending::<()>())));
        assert!((&mut step).now_or_never().is_none());
        assert_eq!(waiting.on(), Dependency::Database);
        drop(step);
        assert_eq!(waiting.on(), Dependency::Unattributed);
    }

    #[tokio::test]
    async fn nested_and_overlapping_steps_end_with_the_last() {
        let waiting = Waiting::default();
        let (first_done, first) = tokio::sync::oneshot::channel::<()>();
        let (second_done, second) = tokio::sync::oneshot::channel::<()>();
        let both = waiting
            .track(async { tokio::join!(on_database(first), on_database(on_database(second))) });
        tokio::pin!(both);
        assert!((&mut both).now_or_never().is_none());
        assert_eq!(waiting.on(), Dependency::Database);
        first_done.send(()).unwrap();
        assert!((&mut both).now_or_never().is_none());
        assert_eq!(
            waiting.on(),
            Dependency::Database,
            "the other step still waits"
        );
        second_done.send(()).unwrap();
        assert!((&mut both).now_or_never().is_some());
        assert_eq!(waiting.on(), Dependency::Unattributed);
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_reads_the_work_before_dropping_it() {
        let database = timeout(
            Duration::from_secs(1),
            on_database(std::future::pending::<()>()),
        )
        .await;
        assert_eq!(database, Err(Dependency::Database));
        let elsewhere = timeout(Duration::from_secs(1), async {
            on_database(async {}).await;
            std::future::pending::<()>().await
        })
        .await;
        assert_eq!(elsewhere, Err(Dependency::Unattributed));
        assert_eq!(
            timeout(Duration::from_secs(1), on_database(async { 7 })).await,
            Ok(7)
        );
    }

    #[tokio::test]
    async fn untracked_work_marks_nothing() {
        let waiting = Waiting::default();
        // Not inside `track`: the step only awaits.
        on_database(async {}).await;
        follow(&waiting, async {}).await;
        assert_eq!(waiting.on(), Dependency::Unattributed);
    }

    #[tokio::test]
    async fn a_follower_reads_through_to_the_shared_work_it_awaits() {
        let shared = Waiting::default();
        let (release_database, database) = tokio::sync::oneshot::channel::<()>();
        let (release_cpu, cpu) = tokio::sync::oneshot::channel::<()>();
        // Shared work: a database step, then a step that is not.
        let work = shared
            .track(async move {
                on_database(database).await.unwrap();
                cpu.await.unwrap();
            })
            .boxed()
            .shared();
        let waiter = Waiting::default();
        let other = Waiting::default();
        let first = waiter.track(follow(&shared, work.clone()));
        let second = other.track(follow(&shared, work));
        tokio::pin!(first, second);
        assert!((&mut first).now_or_never().is_none());
        assert!((&mut second).now_or_never().is_none());
        // Both waiters see the shared step, whichever of them polled it.
        assert_eq!(waiter.on(), Dependency::Database);
        assert_eq!(other.on(), Dependency::Database);
        release_database.send(()).unwrap();
        assert!((&mut first).now_or_never().is_none());
        assert_eq!(waiter.on(), Dependency::Unattributed);
        assert_eq!(other.on(), Dependency::Unattributed);
        release_cpu.send(()).unwrap();
        assert!((&mut first).now_or_never().is_some());
        assert!((&mut second).now_or_never().is_some());
        // Done waiting: nothing is followed any more.
        assert!(waiter.state().following.is_empty());
        assert!(other.state().following.is_empty());
    }

    #[tokio::test]
    async fn marks_inside_shared_work_never_land_on_a_waiter() {
        let shared = Waiting::default();
        let work = shared
            .track(on_database(std::future::pending::<()>()))
            .boxed()
            .shared();
        // A waiter that does not follow polls the shared work: its mark goes
        // to the shared tracker, never to the waiter's.
        let waiter = Waiting::default();
        let waiting = waiter.track(work);
        tokio::pin!(waiting);
        assert!((&mut waiting).now_or_never().is_none());
        assert_eq!(shared.on(), Dependency::Database);
        assert_eq!(waiter.on(), Dependency::Unattributed);
    }
}
