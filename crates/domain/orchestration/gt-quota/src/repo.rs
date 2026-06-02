//! Domain persistence port + in-memory implementation.
//!
//! Append-only by design: the real table (`token_usage` in Postgres) is heavy-write and only
//! inserts rows; the aggregate comes from a query/rollup, never from an `UPDATE` of a counter
//! (avoids lost-update; see `docs/04-persistence.md`).

use std::future::Future;
use std::sync::Mutex;

use gt_events::AppError;

use crate::state::{Account, AccountQuotaStatus};

/// A sample persisted in the `token_usage` table. The `id` is assigned by the adapter
/// (BIGSERIAL in Postgres, position in `InMemoryQuota`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSample {
    pub account: String,
    pub session: String,
    pub model: String,
    pub ts_secs: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read: u64,
    pub cache_creation: u64,
}

/// Port: sample persistence + minimal account state. RPITIT with `+ Send` so the port needs
/// no `async_trait` or boxing (same pattern as `BeadRepository`).
pub trait QuotaRepository: Send + Sync {
    /// Insert a usage sample. Append-only.
    fn insert_sample(
        &self,
        sample: &UsageSample,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Sum of raw tokens (input+output) per account in the range `[from_secs, to_secs)`.
    /// Used by the rollup and the panel; the **cost normalization** is the domain's job.
    fn account_window_tokens(
        &self,
        account: &str,
        from_secs: u64,
        to_secs: u64,
    ) -> impl Future<Output = Result<u64, AppError>> + Send;

    /// Same but attributed to a session: what the goal asked for ("which session is burning
    /// the account").
    fn session_window_tokens(
        &self,
        account: &str,
        session: &str,
        from_secs: u64,
        to_secs: u64,
    ) -> impl Future<Output = Result<u64, AppError>> + Send;

    /// Upsert the account state (status + window). The status changes via events, not by
    /// direct DB edits; this method exists for snapshots and migrations.
    fn upsert_account(
        &self,
        account: &Account,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    fn get_account(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Option<Account>, AppError>> + Send;

    /// Update only the `status` (the transition was already decided by the domain).
    fn set_account_status(
        &self,
        id: &str,
        status: AccountQuotaStatus,
    ) -> impl Future<Output = Result<(), AppError>> + Send;
}

/// Delegates through `Arc`.
impl<R: QuotaRepository + ?Sized> QuotaRepository for std::sync::Arc<R> {
    fn insert_sample(
        &self,
        sample: &UsageSample,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).insert_sample(sample).await }
    }
    fn account_window_tokens(
        &self,
        account: &str,
        from_secs: u64,
        to_secs: u64,
    ) -> impl Future<Output = Result<u64, AppError>> + Send {
        async move {
            (**self)
                .account_window_tokens(account, from_secs, to_secs)
                .await
        }
    }
    fn session_window_tokens(
        &self,
        account: &str,
        session: &str,
        from_secs: u64,
        to_secs: u64,
    ) -> impl Future<Output = Result<u64, AppError>> + Send {
        async move {
            (**self)
                .session_window_tokens(account, session, from_secs, to_secs)
                .await
        }
    }
    fn upsert_account(
        &self,
        account: &Account,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).upsert_account(account).await }
    }
    fn get_account(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Option<Account>, AppError>> + Send {
        async move { (**self).get_account(id).await }
    }
    fn set_account_status(
        &self,
        id: &str,
        status: AccountQuotaStatus,
    ) -> impl Future<Output = Result<(), AppError>> + Send {
        async move { (**self).set_account_status(id, status).await }
    }
}

/// In-memory implementation: the safety net for tests without a DB and the first half of the
/// Step 6.c contract. The second is `gt-store-pg::PgQuota` running the same contract.
#[derive(Default)]
pub struct InMemoryQuota {
    samples: Mutex<Vec<UsageSample>>,
    accounts: Mutex<std::collections::HashMap<String, Account>>,
}

impl QuotaRepository for InMemoryQuota {
    async fn insert_sample(&self, sample: &UsageSample) -> Result<(), AppError> {
        self.samples.lock().unwrap().push(sample.clone());
        Ok(())
    }

    async fn account_window_tokens(
        &self,
        account: &str,
        from_secs: u64,
        to_secs: u64,
    ) -> Result<u64, AppError> {
        Ok(self
            .samples
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.account == account && s.ts_secs >= from_secs && s.ts_secs < to_secs)
            .map(|s| s.input_tokens + s.output_tokens)
            .sum())
    }

    async fn session_window_tokens(
        &self,
        account: &str,
        session: &str,
        from_secs: u64,
        to_secs: u64,
    ) -> Result<u64, AppError> {
        Ok(self
            .samples
            .lock()
            .unwrap()
            .iter()
            .filter(|s| {
                s.account == account
                    && s.session == session
                    && s.ts_secs >= from_secs
                    && s.ts_secs < to_secs
            })
            .map(|s| s.input_tokens + s.output_tokens)
            .sum())
    }

    async fn upsert_account(&self, account: &Account) -> Result<(), AppError> {
        self.accounts
            .lock()
            .unwrap()
            .insert(account.id.clone(), account.clone());
        Ok(())
    }

    async fn get_account(&self, id: &str) -> Result<Option<Account>, AppError> {
        Ok(self.accounts.lock().unwrap().get(id).cloned())
    }

    async fn set_account_status(
        &self,
        id: &str,
        status: AccountQuotaStatus,
    ) -> Result<(), AppError> {
        let mut g = self.accounts.lock().unwrap();
        match g.get_mut(id) {
            Some(a) => {
                a.status = status;
                Ok(())
            }
            None => Err(AppError::NotFound(format!("account {id}"))),
        }
    }
}
