use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;
use tokio::sync::{oneshot, Semaphore};

// Every blocking gate has a watchdog so a failed assertion cannot hang
// runtime shutdown. Ordering assertions use explicit signals, never sleeps.
const WATCHDOG: Duration = Duration::from_secs(10);

struct DropGate {
    entered: Option<oneshot::Sender<std::thread::ThreadId>>,
    release: mpsc::Receiver<()>,
    drops: Arc<AtomicUsize>,
}

impl Drop for DropGate {
    fn drop(&mut self) {
        let _ = self
            .entered
            .take()
            .unwrap()
            .send(std::thread::current().id());
        let _ = self.release.recv_timeout(WATCHDOG);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn drop_gate() -> (
    DropGate,
    oneshot::Receiver<std::thread::ThreadId>,
    mpsc::Sender<()>,
    Arc<AtomicUsize>,
) {
    let (entered, ready) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    (
        DropGate {
            entered: Some(entered),
            release: wait,
            drops: drops.clone(),
        },
        ready,
        release,
        drops,
    )
}

async fn receive<T>(signal: oneshot::Receiver<T>) -> T {
    tokio::time::timeout(WATCHDOG, signal)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_mapping_retains_admission_through_mapping_and_output_cleanup() {
    let runtime_thread = std::thread::current().id();
    let admission = Arc::new(Semaphore::new(1));
    let completion = ReadAdmission::new(admission.clone().acquire_owned().await.unwrap());
    let (state, dropping, release_drop, drops) = drop_gate();
    let state = completion.own(state);
    let (mapping, mapped) = oneshot::channel();
    let (release_map, wait_map) = mpsc::channel();
    let reader = tokio::spawn(async move {
        let _completion = completion;
        state
            .map(move |state| {
                mapping.send(()).unwrap();
                wait_map.recv_timeout(WATCHDOG).unwrap();
                Ok(state)
            })
            .await
    });
    receive(mapped).await;
    let next = admission.clone().acquire_owned();
    tokio::pin!(next);
    assert!(futures_util::poll!(&mut next).is_pending());
    reader.abort();
    assert!(matches!(reader.await, Err(join) if join.is_cancelled()));

    // A spare blocking worker can run after cancellation while the first
    // mapping is held. Queuing the permit's drop separately would fail here.
    tokio::task::spawn_blocking(|| ()).await.unwrap();
    assert!(futures_util::poll!(&mut next).is_pending());
    release_map.send(()).unwrap();
    assert_ne!(receive(dropping).await, runtime_thread);
    assert!(futures_util::poll!(&mut next).is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    // The current-thread executor can still drive timers while Drop blocks.
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_drop.send(()).unwrap();
    let second = tokio::time::timeout(WATCHDOG, next).await.unwrap().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(second);
    assert_eq!(admission.available_permits(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_waits_for_all_cleanups_regardless_of_completion_order() {
    let runtime_thread = std::thread::current().id();
    let admission = Arc::new(Semaphore::new(1));
    let completion = ReadAdmission::new(admission.clone().acquire_owned().await.unwrap());
    let (page, page_dropping, release_page, page_drops) = drop_gate();
    let (balances, balances_dropping, release_balances, balance_drops) = drop_gate();
    let (entered, ready) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let _page = completion.own(page);
        let _balances = completion.own(balances);
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    receive(ready).await;
    reader.abort();
    assert!(reader.await.unwrap_err().is_cancelled());
    assert_ne!(receive(page_dropping).await, runtime_thread);
    assert_ne!(receive(balances_dropping).await, runtime_thread);
    let next = admission.clone().acquire_owned();
    tokio::pin!(next);
    assert!(futures_util::poll!(&mut next).is_pending());

    // Both destructors have started on separate workers. Let only one end.
    release_balances.send(()).unwrap();
    while balance_drops.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::task::spawn_blocking(|| ()).await.unwrap();
    assert_eq!(page_drops.load(Ordering::SeqCst), 0);
    assert!(futures_util::poll!(&mut next).is_pending());
    release_page.send(()).unwrap();
    let second = tokio::time::timeout(WATCHDOG, next).await.unwrap().unwrap();
    assert_eq!(page_drops.load(Ordering::SeqCst), 1);
    assert_eq!(balance_drops.load(Ordering::SeqCst), 1);
    drop(second);
    assert_eq!(admission.available_permits(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn mapping_failure_retains_admission_until_input_cleanup_finishes() {
    for panic in [false, true] {
        let admission = Arc::new(Semaphore::new(1));
        let completion = ReadAdmission::new(admission.clone().acquire_owned().await.unwrap());
        let (state, dropping, release_drop, drops) = drop_gate();
        let state = completion.own(state);
        drop(completion);
        let mapper = tokio::spawn(state.map(move |_state| -> Result<(), WindowError> {
            if panic {
                panic!("controlled mapping panic");
            }
            Err(WindowError::Decode(anyhow::anyhow!(
                "controlled corruption"
            )))
        }));
        receive(dropping).await;
        assert!(admission.try_acquire().is_err());
        release_drop.send(()).unwrap();
        let result = mapper.await.unwrap();
        assert!(match result {
            Err(WindowError::TaskFailed(join)) => panic && join.is_panic(),
            Err(WindowError::Decode(_)) => !panic,
            _ => false,
        });
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(admission.available_permits(), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn successful_handoff_releases_admission_without_dropping_the_result() {
    let admission = Arc::new(Semaphore::new(1));
    let completion = ReadAdmission::new(admission.clone().acquire_owned().await.unwrap());
    let (state, dropping, release_drop, drops) = drop_gate();
    let result = completion.own(state).map(Ok).await.unwrap();
    let value = result.into_inner();
    drop(completion);
    assert_eq!(admission.available_permits(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let cleanup = tokio::task::spawn_blocking(move || drop(value));
    receive(dropping).await;
    release_drop.send(()).unwrap();
    cleanup.await.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

struct Probe(Option<oneshot::Sender<std::thread::ThreadId>>);

impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(std::thread::current().id());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_future_drops_its_payload_on_a_blocking_thread() {
    let runtime_thread = std::thread::current().id();
    let (dropped, receive) = oneshot::channel();
    let (entered, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _owned = BlockingDrop::new(Probe(Some(dropped)));
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let thread = tokio::time::timeout(std::time::Duration::from_secs(2), receive)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(thread, runtime_thread);
}

#[tokio::test(flavor = "current_thread")]
async fn successful_handoff_keeps_the_payload_for_its_caller() {
    let (dropped, mut receive) = oneshot::channel();
    let owned = BlockingDrop::new(Probe(Some(dropped))).into_inner();
    assert!(matches!(
        receive.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let runtime_thread = std::thread::current().id();
    tokio::task::spawn_blocking(move || drop(owned))
        .await
        .unwrap();
    assert_ne!(receive.await.unwrap(), runtime_thread);
}

#[tokio::test]
async fn blocking_panic_is_task_failed_not_corruption() {
    let error = BlockingDrop::new(())
        .map(|()| -> Result<(), WindowError> { panic!("controlled mapping panic") })
        .await
        .err()
        .expect("mapping must fail");
    assert!(matches!(error, WindowError::TaskFailed(join) if join.is_panic()));
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_cleanup_retains_admission_during_payload_drop() {
    struct Probe {
        admission: Arc<Semaphore>,
        observed: Option<oneshot::Sender<usize>>,
        drops: Arc<AtomicUsize>,
        panic: bool,
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            let _ = self
                .observed
                .take()
                .unwrap()
                .send(self.admission.available_permits());
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.panic {
                panic!("controlled shutdown cleanup panic");
            }
        }
    }

    for panic in [false, true] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (entered, ready) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        runtime.spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv_timeout(WATCHDOG).unwrap();
        });
        receive(ready).await;
        let admission = Arc::new(Semaphore::new(1));
        let drops = Arc::new(AtomicUsize::new(0));
        let (observed, dropped) = oneshot::channel();
        let owned = {
            let _context = runtime.enter();
            ReadAdmission::new(admission.clone().try_acquire_owned().unwrap()).own(Probe {
                admission: admission.clone(),
                observed: Some(observed),
                drops: drops.clone(),
                panic,
            })
        };
        // Queue cleanup behind the held worker, leaving its capture as the
        // only admission owner. Shutdown discards that queued task's closure.
        drop(owned);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        runtime.shutdown_background();
        release.send(()).unwrap();
        assert_eq!(receive(dropped).await, 0);
        let recovered = tokio::time::timeout(WATCHDOG, admission.clone().acquire_owned())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(recovered);
        assert_eq!(admission.available_permits(), 1);
    }
}
