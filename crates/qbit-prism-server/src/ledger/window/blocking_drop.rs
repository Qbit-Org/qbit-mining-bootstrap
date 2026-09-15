//! Keep a cancelled read's accumulated window off the runtime's drop path.
use super::WindowError;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

/// One admission shared by the read, its blocking work and its cleanup.
/// The final owner releases it; blocking-pool queue order is irrelevant.
#[derive(Clone, Default)]
pub(crate) struct ReadAdmission {
    _owner: Option<Arc<AdmissionOwner>>,
}

struct AdmissionOwner {
    permit: Option<Arc<OwnedSemaphorePermit>>,
    changed: Option<Arc<tokio::sync::Notify>>,
}

impl Drop for AdmissionOwner {
    fn drop(&mut self) {
        drop(self.permit.take());
        if let Some(changed) = &self.changed {
            changed.notify_waiters();
        }
    }
}

impl ReadAdmission {
    pub(crate) fn new(permit: OwnedSemaphorePermit) -> Self {
        Self::shared(Arc::new(permit))
    }

    pub(crate) fn shared(permit: Arc<OwnedSemaphorePermit>) -> Self {
        Self {
            _owner: Some(Arc::new(AdmissionOwner {
                permit: Some(permit),
                changed: None,
            })),
        }
    }

    /// Coalescer waiters wake when actual work/cleanup releases the last owner,
    /// including when the async flight disappeared before its blocking task.
    pub(crate) fn notifying(
        permit: OwnedSemaphorePermit,
        changed: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            _owner: Some(Arc::new(AdmissionOwner {
                permit: Some(Arc::new(permit)),
                changed: Some(changed),
            })),
        }
    }

    pub(crate) fn own<T: Send + 'static>(&self, value: T) -> BlockingDrop<T> {
        BlockingDrop {
            value: Some(value),
            completion: self.clone(),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

pub(crate) struct BlockingDrop<T: Send + 'static> {
    value: Option<T>,
    completion: ReadAdmission,
    runtime: tokio::runtime::Handle,
}

impl<T: Send + 'static> BlockingDrop<T> {
    pub(crate) fn new(value: T) -> Self {
        ReadAdmission::default().own(value)
    }

    pub(crate) fn get(&self) -> &T {
        self.value.as_ref().expect("owned until taken")
    }

    pub(crate) fn into_inner(mut self) -> T {
        self.value.take().expect("owned until taken")
    }

    /// Retain admission while mapping, including an error or panic, and
    /// transfer it into the output before the blocking task can complete.
    pub(crate) async fn map<U: Send + 'static>(
        self,
        map: impl FnOnce(T) -> Result<U, WindowError> + Send + 'static,
    ) -> Result<BlockingDrop<U>, WindowError> {
        self.runtime
            .clone()
            .spawn_blocking(move || {
                let completion = self.completion.clone();
                let value = map(self.into_inner())?;
                Ok(completion.own(value))
            })
            .await
            .map_err(WindowError::TaskFailed)?
    }

    /// The generic ledger readers retain their existing anyhow/source contract.
    pub(crate) async fn map_anyhow<U: Send + 'static>(
        self,
        map: impl FnOnce(T) -> anyhow::Result<U> + Send + 'static,
    ) -> anyhow::Result<BlockingDrop<U>> {
        self.map(move |value| map(value).map_err(WindowError::Decode))
            .await
            .map_err(|error| match error {
                WindowError::Decode(error) => error,
                WindowError::TaskFailed(error) => error.into(),
                error => error.into(),
            })
    }
}

impl<T: Send + 'static> std::ops::Deref for BlockingDrop<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.get()
    }
}

impl<T: Send + 'static> Drop for BlockingDrop<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            // Use the captured handle: cancellation can drop a future outside
            // the context in which it was polled. A running blocking closure
            // owns its input until actual exit, even if its waiter disappears.
            // Field order retains admission if the payload destructor unwinds
            // or shutdown drops the closure without running it. Capture the
            // whole owner: separate closure captures have no defined drop order.
            struct Cleanup<T> {
                _value: T,
                _completion: ReadAdmission,
            }
            let cleanup = Cleanup {
                _value: value,
                _completion: self.completion.clone(),
            };
            self.runtime.spawn_blocking(move || drop(cleanup));
        }
    }
}

#[cfg(test)]
mod tests;
