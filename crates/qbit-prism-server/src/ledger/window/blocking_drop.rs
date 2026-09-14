//! Keep a cancelled read's accumulated window off the runtime's drop path.
pub(super) struct BlockingDrop<T: Send + 'static> {
    value: Option<T>,
    runtime: tokio::runtime::Handle,
}

impl<T: Send + 'static> BlockingDrop<T> {
    pub(super) fn new(value: T) -> Self {
        Self {
            value: Some(value),
            runtime: tokio::runtime::Handle::current(),
        }
    }

    pub(super) fn get(&self) -> &T {
        self.value.as_ref().expect("owned until taken")
    }

    pub(super) fn into_inner(mut self) -> T {
        self.value.take().expect("owned until taken")
    }
}

impl<T: Send + 'static> Drop for BlockingDrop<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            // Use the captured handle: cancellation can drop a future outside
            // the context in which it was polled. A running blocking closure
            // owns its input until actual exit, even if its waiter disappears.
            self.runtime.spawn_blocking(move || drop(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

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
}
