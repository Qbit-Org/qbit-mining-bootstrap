//! Keep a cancelled read's accumulated window off the runtime's drop path.
use super::WindowError;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

/// One admission shared by the read, its blocking work and its cleanup.
/// The final owner releases it; blocking-pool queue order is irrelevant.
#[derive(Clone, Default)]
pub(super) struct ReadAdmission {
    _permit: Option<Arc<OwnedSemaphorePermit>>,
}

impl ReadAdmission {
    pub(super) fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            _permit: Some(Arc::new(permit)),
        }
    }

    pub(super) fn own<T: Send + 'static>(&self, value: T) -> BlockingDrop<T> {
        BlockingDrop {
            value: Some(value),
            completion: self.clone(),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

pub(super) struct BlockingDrop<T: Send + 'static> {
    value: Option<T>,
    completion: ReadAdmission,
    runtime: tokio::runtime::Handle,
}

impl<T: Send + 'static> BlockingDrop<T> {
    pub(super) fn new(value: T) -> Self {
        ReadAdmission::default().own(value)
    }

    pub(super) fn get(&self) -> &T {
        self.value.as_ref().expect("owned until taken")
    }

    pub(super) fn into_inner(mut self) -> T {
        self.value.take().expect("owned until taken")
    }

    /// Retain admission while mapping, including an error or panic, and
    /// transfer it into the output before the blocking task can complete.
    pub(super) async fn map<U: Send + 'static>(
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
