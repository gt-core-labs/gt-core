//! Branch reaper port (epic hq-branch-gc) — the delivery-time edge that drops a merged Dolt
//! branch so delivered branches stop accumulating.
//!
//! A **separate function** from the merge board/actor on purpose: the actor stays a pure state
//! machine (`Ready → Merging → Merged`), and *deleting the branch* is an I/O effect the
//! composition root fires when it observes `merge.merged.v1`. No cron, no reaper pass — the
//! delivery event is the trigger. This mirrors [`MergeRepository`](crate::MergeRepository):
//! `impl Future + Send` (no `async_trait` boxing), an `Arc` blanket impl, and an in-memory
//! adapter for tests / the no-Dolt path.

use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

/// Port: delete a delivered branch. **Idempotent** — reaping an absent branch returns
/// `Ok(false)`, a present one `Ok(true)`; errors are reserved for real I/O failures. That keeps
/// a replayed `Merged` (boot hydration re-emitting) or a second pass from aborting the reaction
/// chain.
pub trait BranchReaper: Send + Sync {
    fn reap(&self, branch: &str) -> impl Future<Output = Result<bool, AppError>> + Send;
}

impl<R: BranchReaper + ?Sized> BranchReaper for std::sync::Arc<R> {
    fn reap(&self, branch: &str) -> impl Future<Output = Result<bool, AppError>> + Send {
        async move { (**self).reap(branch).await }
    }
}

/// In-memory adapter for domain tests and the bin when no Dolt server is wired. Records each
/// branch reaped; a repeat reap of the same name returns `false`, modelling the Dolt adapter's
/// idempotency without a server.
#[derive(Default)]
pub struct InMemoryBranchReaper {
    reaped: Mutex<Vec<String>>,
}

impl InMemoryBranchReaper {
    pub fn new() -> Self {
        Self::default()
    }

    /// Branches reaped so far, in reap order. Test introspection.
    pub fn reaped(&self) -> Vec<String> {
        self.reaped.lock().unwrap().clone()
    }
}

impl BranchReaper for InMemoryBranchReaper {
    async fn reap(&self, branch: &str) -> Result<bool, AppError> {
        let mut guard = self.reaped.lock().unwrap();
        if guard.iter().any(|b| b == branch) {
            return Ok(false);
        }
        guard.push(branch.to_owned());
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reaps_then_is_idempotent() {
        let reaper = InMemoryBranchReaper::new();
        assert!(reaper.reap("feat/b1").await.unwrap(), "first reap deletes");
        assert!(
            !reaper.reap("feat/b1").await.unwrap(),
            "second reap of same branch is a no-op"
        );
        assert_eq!(reaper.reaped(), vec!["feat/b1".to_string()]);
    }
}
