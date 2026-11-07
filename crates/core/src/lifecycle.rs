use crate::error::{CoreError, Result};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard, RwLock};
use tokio_util::sync::CancellationToken;

/// Serializes lifecycle operations without preventing stop from cancelling the
/// operation that currently owns the gate. High-frequency audio never uses it.
pub struct Lifecycle {
    gate: Mutex<()>,
    current: RwLock<CancellationToken>,
    next_operation_id: AtomicU64,
}

pub struct Operation<'a> {
    _gate: MutexGuard<'a, ()>,
    id: u64,
    token: CancellationToken,
}

impl Lifecycle {
    pub fn new() -> Self {
        Self {
            gate: Mutex::new(()),
            current: RwLock::new(CancellationToken::new()),
            next_operation_id: AtomicU64::new(0),
        }
    }

    pub async fn begin(&self) -> Operation<'_> {
        let gate = self.gate.lock().await;
        let root = {
            let mut current = self.current.write().await;
            if current.is_cancelled() {
                *current = CancellationToken::new();
            }
            current.clone()
        };
        Operation {
            _gate: gate,
            id: self.next_operation_id.fetch_add(1, Ordering::AcqRel) + 1,
            token: root.child_token(),
        }
    }

    /// Cancels the active run or lifecycle operation before waiting for its gate.
    pub async fn cancel_current(&self) {
        self.current.read().await.cancel();
    }
}

impl Operation<'_> {
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub async fn run_with_timeout<F, T>(
        &self,
        duration: Duration,
        description: &str,
        future: F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        tokio::select! {
            _ = self.token.cancelled() => Err(CoreError::InvalidState(
                format!("lifecycle operation {} was cancelled", self.id),
            )),
            result = tokio::time::timeout(duration, future) => {
                result.map_err(|_| CoreError::InvalidState(format!(
                    "lifecycle operation {} timed out while {description}", self.id,
                )))?
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_interrupts_active_operation() {
        let lifecycle = Lifecycle::new();
        let operation = lifecycle.begin().await;
        let token = operation.token();
        lifecycle.cancel_current().await;
        let result = operation
            .run_with_timeout(Duration::from_secs(60), "waiting in a test", async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            })
            .await;
        assert!(result.is_err());
        assert!(token.is_cancelled());
    }
}
