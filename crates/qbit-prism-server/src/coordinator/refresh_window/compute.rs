//! Two CPU lanes and an async continuation under one original build admission.
use super::*;

type Admission = Arc<tokio::sync::OwnedSemaphorePermit>;
type Pair<T, L, R> = CompactOwner<(Arc<T>, Result<L>, Result<R>, Admission)>;

/// `prefix` runs first on the left lane and hands `X`, a by-product of its
/// pass over the source, to `left` on the same thread; a failed prefix hands
/// `left` its error instead. `P` goes to `finish` without waiting for `left`.
pub(super) async fn pipeline<T, P, X, L, R, O>(
    source: CompactOwner<(Arc<T>, Admission)>,
    prefix: impl FnOnce(&T) -> Result<(P, X)> + Send + 'static,
    left: impl FnOnce(&T, Result<X>) -> Result<L> + Send + 'static,
    right: impl FnOnce(&T) -> Result<R> + Send + 'static,
    finish: impl FnOnce(P, R) -> Result<O> + Send + 'static,
) -> Result<Pair<T, L, O>>
where
    T: Send + Sync + 'static,
    P: Send + 'static,
    X: Send + 'static,
    L: Send + 'static,
    R: Send + 'static,
    O: Send + 'static,
{
    let (send_prefix, receive_prefix) = tokio::sync::oneshot::channel();
    let left_source = CompactOwner::new((source.0.clone(), source.1.clone()));
    let right_source = CompactOwner::new((source.0.clone(), source.1.clone()));
    let left = left_source.spawn_blocking(move |owned| {
        let (prefix, handoff) = match prefix(&owned.0) {
            Ok((prefix, handoff)) => (Ok(prefix), Ok(handoff)),
            Err(error) => {
                let message = format!("{error:#}");
                (
                    Err(error),
                    Err(anyhow::anyhow!("audit prefix failed: {message}")),
                )
            }
        };
        // Sending never waits for the body or its continuation. On cancellation
        // the rejected output still owns admission through off-runtime cleanup.
        let _ = send_prefix.send(CompactOwner::new((prefix, owned.1.clone())));
        let result = left(&owned.0, handoff);
        let (source, admission) = owned;
        drop(source);
        CompactOwner::new((result, admission))
    });
    let right = right_source.spawn_blocking(move |owned| {
        let result = right(&owned.0);
        let (source, admission) = owned;
        drop(source);
        CompactOwner::new((result, admission))
    });
    let continuation = async move {
        // This wait is async. Even a saturated single-thread blocking executor
        // can run prefix+left, then right, then finish without a dependency wait.
        let (prefix, right) = tokio::join!(receive_prefix, right);
        let prefix = prefix?;
        let right = right?;
        let (prefix, admission) = prefix.into_inner();
        let (right, right_admission) = right.into_inner();
        let input = CompactOwner::new((prefix, right, admission));
        drop(right_admission);
        // The right worker is finished before its successor is queued: at most
        // two computations per admitted build, including queued blocking work.
        Ok::<_, anyhow::Error>(
            input
                .spawn_blocking(move |(prefix, right, admission)| {
                    let result = (|| finish(prefix?, right?))();
                    CompactOwner::new((result, admission))
                })
                .await?,
        )
    };
    // Join both even on failure. Cancellation may detach running work; each
    // input AND completed output owns admission until its off-runtime cleanup.
    let (left, right) = tokio::join!(left, continuation);
    let left = left?;
    let right = right?;
    // Synchronous owner-to-owner transfer: no await or fallible operation may
    // intervene while these potentially large values are unguarded.
    let (source, admission) = source.into_inner();
    let (left, left_admission) = left.into_inner();
    let (right, right_admission) = right.into_inner();
    let result = CompactOwner::new((source, left, right, admission));
    drop((left_admission, right_admission));
    Ok(result)
}

#[cfg(test)]
async fn pair<T, L, R>(
    source: CompactOwner<(Arc<T>, Admission)>,
    left: impl FnOnce(&T) -> Result<L> + Send + 'static,
    right: impl FnOnce(&T) -> Result<R> + Send + 'static,
) -> Result<Pair<T, L, R>>
where
    T: Send + Sync + 'static,
    L: Send + 'static,
    R: Send + 'static,
{
    pipeline(
        source,
        |_| Ok(((), ())),
        move |source, _| left(source),
        right,
        |(), right| Ok(right),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{oneshot, Semaphore};

    struct Cleanup {
        slots: Arc<Semaphore>,
        dropped: Option<oneshot::Sender<(usize, std::thread::ThreadId)>>,
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Some(sender) = self.dropped.take() {
                let _ = sender.send((self.slots.available_permits(), std::thread::current().id()));
            }
        }
    }

    async fn source(
        slots: &Arc<Semaphore>,
    ) -> (
        CompactOwner<(Arc<Cleanup>, Admission)>,
        oneshot::Receiver<(usize, std::thread::ThreadId)>,
    ) {
        let permit = Arc::new(slots.clone().acquire_owned().await.unwrap());
        let (dropped, receive) = oneshot::channel();
        (
            CompactOwner::new((
                Arc::new(Cleanup {
                    slots: slots.clone(),
                    dropped: Some(dropped),
                }),
                permit,
            )),
            receive,
        )
    }

    async fn released(slots: &Arc<Semaphore>) {
        drop(
            tokio::time::timeout(Duration::from_secs(5), slots.clone().acquire_owned())
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn suffix_finishes_while_native_is_running() {
        let slots = Arc::new(Semaphore::new(1));
        let (source, dropped) = source(&slots).await;
        let (entered, receive) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (finished, suffix) = oneshot::channel();
        let task = tokio::spawn(pipeline(
            source,
            |_| Ok((7, ())),
            move |_, _| {
                entered.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(11)
            },
            |_| Ok(13),
            move |prefix, body| {
                finished.send(()).unwrap();
                Ok(prefix + body)
            },
        ));
        receive.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), suffix)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(slots.available_permits(), 0);
        release.send(()).unwrap();
        let result = task.await.unwrap().unwrap();
        assert_eq!(*result.1.as_ref().unwrap(), 11);
        assert_eq!(*result.2.as_ref().unwrap(), 20);
        drop(result);
        assert_eq!(dropped.await.unwrap().0, 0);
        released(&slots).await;
    }

    #[tokio::test]
    async fn prefix_and_suffix_errors_and_panics_keep_admission_until_cleanup() {
        for outcome in [
            "prefix_error",
            "prefix_panic",
            "suffix_error",
            "suffix_panic",
        ] {
            let slots = Arc::new(Semaphore::new(1));
            let (source, dropped) = source(&slots).await;
            let result = pipeline(
                source,
                move |_| {
                    if outcome == "prefix_panic" {
                        panic!("injected prefix panic");
                    }
                    anyhow::ensure!(outcome != "prefix_error", "injected prefix error");
                    Ok(((), ()))
                },
                |_, handoff| handoff,
                |_| Ok(()),
                move |(), ()| -> Result<()> {
                    if outcome == "suffix_panic" {
                        panic!("injected suffix panic");
                    }
                    anyhow::bail!("injected suffix error")
                },
            )
            .await;
            if outcome.ends_with("panic") {
                assert!(result.is_err());
            } else {
                let result = result.unwrap();
                assert!(result.2.as_ref().unwrap_err().to_string().contains(
                    if outcome == "prefix_error" {
                        "prefix error"
                    } else {
                        "suffix error"
                    }
                ));
                // The left lane receives the prefix outcome: its handoff fails
                // with the prefix error and succeeds otherwise.
                assert_eq!(
                    result.1.as_ref().err().map(|error| error.to_string()),
                    (outcome == "prefix_error")
                        .then(|| "audit prefix failed: injected prefix error".to_string())
                );
                assert_eq!(slots.available_permits(), 0);
                drop(result);
            }
            assert_eq!(dropped.await.unwrap().0, 0);
            released(&slots).await;
        }
    }

    #[tokio::test]
    async fn body_completed_before_prefix_keeps_its_output_admitted_on_cancellation() {
        let slots = Arc::new(Semaphore::new(1));
        let (source, dropped) = source(&slots).await;
        let (entered, receive) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (body_done, body_ready) = oneshot::channel();
        let (cleaned, cleanup) = oneshot::channel();
        let output_slots = slots.clone();
        let task = tokio::spawn(pipeline(
            source,
            move |_| {
                entered.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(((), ()))
            },
            |_, _| Ok(()),
            move |_| {
                body_done.send(()).unwrap();
                Ok(Cleanup {
                    slots: output_slots,
                    dropped: Some(cleaned),
                })
            },
            |(), body| Ok(body),
        ));
        receive.await.unwrap();
        body_ready.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert_eq!(slots.available_permits(), 0);
        release.send(()).unwrap();
        assert_eq!(cleanup.await.unwrap().0, 0);
        assert_eq!(dropped.await.unwrap().0, 0);
        released(&slots).await;
    }

    #[tokio::test]
    async fn cancelled_running_suffix_retains_admission_through_its_output_cleanup() {
        let slots = Arc::new(Semaphore::new(1));
        let (source, dropped) = source(&slots).await;
        let (entered, receive) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (cleaned, cleanup) = oneshot::channel();
        let output_slots = slots.clone();
        let task = tokio::spawn(pipeline(
            source,
            |_| Ok(((), ())),
            |_, _| Ok(()),
            |_| Ok(()),
            move |(), ()| {
                entered.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(Cleanup {
                    slots: output_slots,
                    dropped: Some(cleaned),
                })
            },
        ));
        receive.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert_eq!(slots.available_permits(), 0);
        release.send(()).unwrap();
        assert_eq!(cleanup.await.unwrap().0, 0);
        assert_eq!(dropped.await.unwrap().0, 0);
        released(&slots).await;
    }

    #[test]
    fn saturated_single_blocking_thread_cancellation_keeps_admission_through_cleanup() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let slots = Arc::new(Semaphore::new(1));
            let (source, dropped) = source(&slots).await;
            let runtime_thread = std::thread::current().id();
            let (entered, receive) = oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
            });
            receive.await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let left = calls.clone();
            let right = calls.clone();
            let (queued, receive) = oneshot::channel();
            let task = tokio::spawn(async move {
                queued.send(()).unwrap();
                pair(
                    source,
                    move |_| {
                        left.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                    move |_| {
                        right.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
            });
            receive.await.unwrap();
            task.abort();
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
            assert_eq!(slots.available_permits(), 0);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
            blocker.await.unwrap();
            let (available, thread) = dropped.await.unwrap();
            assert_eq!(available, 0, "snapshot cleanup must still own admission");
            assert_ne!(thread, runtime_thread);
            released(&slots).await;
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        });
    }

    #[tokio::test]
    async fn running_sibling_keeps_original_admission_on_deadline_error_and_panic() {
        for outcome in ["deadline", "error", "panic"] {
            let slots = Arc::new(Semaphore::new(1));
            let (source, dropped) = source(&slots).await;
            let (entered, receive) = oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let mut task = tokio::spawn(async move {
                pair(
                    source,
                    move |_| -> Result<()> {
                        if outcome == "panic" {
                            panic!("injected native worker failure");
                        }
                        anyhow::bail!("injected native error")
                    },
                    move |_| {
                        entered.send(()).unwrap();
                        wait.recv_timeout(Duration::from_secs(5)).unwrap();
                        Ok(())
                    },
                )
                .await
            });
            receive.await.unwrap();
            assert!(tokio::time::timeout(Duration::from_millis(20), &mut task)
                .await
                .is_err());
            assert_eq!(slots.available_permits(), 0);
            if outcome == "deadline" {
                // Model the caller cancelling the original operation after its
                // deadline; the running worker cannot be aborted by Tokio.
                task.abort();
                assert!(matches!(task.await, Err(error) if error.is_cancelled()));
                assert_eq!(slots.available_permits(), 0);
                release.send(()).unwrap();
            } else {
                release.send(()).unwrap();
                let result = task.await.unwrap();
                if outcome == "panic" {
                    assert!(result.is_err());
                } else {
                    let result = result.unwrap();
                    assert!(result
                        .1
                        .as_ref()
                        .unwrap_err()
                        .to_string()
                        .contains("native error"));
                    drop(result);
                }
            }
            assert_eq!(dropped.await.unwrap().0, 0);
            released(&slots).await;
        }
    }

    #[test]
    fn simultaneous_pairs_at_capacity_one_need_no_nested_worker_or_permit() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let slots = Arc::new(Semaphore::new(1));
            let run = || async {
                let (source, dropped) = source(&slots).await;
                let result = pair(source, |_| Ok(7), |_| Ok(11)).await.unwrap();
                assert_eq!(result.1.as_ref().unwrap(), &7);
                assert_eq!(result.2.as_ref().unwrap(), &11);
                drop(result);
                assert_eq!(dropped.await.unwrap().0, 0);
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(run(), run());
            })
            .await
            .unwrap();
            released(&slots).await;
        });
    }

    #[tokio::test]
    async fn original_deadline_expires_while_running_sibling_keeps_admission() {
        let slots = Arc::new(Semaphore::new(1));
        let (source, dropped) = source(&slots).await;
        let (entered, receive) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                pair(
                    source,
                    |_| Ok(()),
                    move |_| {
                        entered.send(()).unwrap();
                        wait.recv_timeout(Duration::from_secs(5)).unwrap();
                        Ok(())
                    },
                ),
            )
            .await
        });
        receive.await.unwrap();
        assert!(task.await.unwrap().is_err());
        assert_eq!(slots.available_permits(), 0);
        release.send(()).unwrap();
        assert_eq!(dropped.await.unwrap().0, 0);
        released(&slots).await;
    }
}
