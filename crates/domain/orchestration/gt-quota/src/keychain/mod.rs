//! Keychain port: stores per-account API credentials behind a *live pointer* so the
//! rotation chain (`docs/features/token-tracking-prediction.md`) can swap which credential
//! is in effect without re-spawning sessions.
//!
//! Two adapters live behind this port:
//! - [`InMemoryKeychain`] — the in-process default. Used by tests and by the bin until the
//!   platform adapter is configured.
//! - [`linux::LinuxKeychain`] — Secret Service / kwallet / fallback file via the `keyring`
//!   crate. Selected at runtime by the bin when the env asks for it.
//!
//! The trait keeps the domain free of platform deps: `gt-quota` does not link `keyring`;
//! the bin chooses an adapter and hands it through `Effects` (`bins/gt/src/root.rs`).

#[cfg(target_os = "linux")]
pub mod linux;

use std::collections::HashMap;
use std::sync::Mutex;

use gt_events::AppError;

/// Per-account credential record. `secret` is opaque to the domain; only the keychain
/// adapter knows whether it's an API key, an oauth token, or a file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRecord {
    pub account: String,
    pub secret: String,
}

/// Live-pointer credential store. The pointer (`active`) is the credential the runtime uses
/// **now**; rotation flips it atomically. Implementations are `Sync + Send` so the
/// composition root can share one across actor handles.
pub trait Keychain: Send + Sync {
    /// Upsert an account's credential. Idempotent.
    fn put(&self, record: CredentialRecord) -> Result<(), AppError>;

    /// Read a stored credential. `None` for an unknown account.
    fn get(&self, account: &str) -> Result<Option<CredentialRecord>, AppError>;

    /// Remove an account's credential. No-op if absent.
    fn delete(&self, account: &str) -> Result<(), AppError>;

    /// Read the live-pointer: which account's credential is currently in effect. `None`
    /// before the first `set_active` call.
    fn active(&self) -> Result<Option<String>, AppError>;

    /// Flip the live pointer to `account`. Fails with `AppError::NotFound` if the account
    /// has no stored credential (so a typo cannot strand the runtime mid-rotation).
    fn set_active(&self, account: &str) -> Result<(), AppError>;

    /// List the accounts the keychain knows about. Order is implementation-defined; callers
    /// must not depend on it (sort if needed for snapshots).
    fn accounts(&self) -> Result<Vec<String>, AppError>;
}

/// Thin in-process adapter: a `HashMap<account, secret>` + a single live pointer. The
/// safety net for tests without a platform keyring and the first half of the keychain
/// contract (the second is `LinuxKeychain` running the same trait).
#[derive(Default)]
pub struct InMemoryKeychain {
    inner: Mutex<KeychainState>,
}

#[derive(Default)]
struct KeychainState {
    creds: HashMap<String, String>,
    active: Option<String>,
}

impl InMemoryKeychain {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test helper: seed N accounts in one call without locking churn.
    pub fn seeded<I, A, S>(records: I) -> Self
    where
        I: IntoIterator<Item = (A, S)>,
        A: Into<String>,
        S: Into<String>,
    {
        let mut state = KeychainState::default();
        for (a, s) in records {
            state.creds.insert(a.into(), s.into());
        }
        Self {
            inner: Mutex::new(state),
        }
    }
}

impl Keychain for InMemoryKeychain {
    fn put(&self, record: CredentialRecord) -> Result<(), AppError> {
        self.inner
            .lock()
            .unwrap()
            .creds
            .insert(record.account, record.secret);
        Ok(())
    }

    fn get(&self, account: &str) -> Result<Option<CredentialRecord>, AppError> {
        Ok(self.inner.lock().unwrap().creds.get(account).map(|s| {
            CredentialRecord {
                account: account.to_string(),
                secret: s.clone(),
            }
        }))
    }

    fn delete(&self, account: &str) -> Result<(), AppError> {
        let mut g = self.inner.lock().unwrap();
        g.creds.remove(account);
        if g.active.as_deref() == Some(account) {
            g.active = None;
        }
        Ok(())
    }

    fn active(&self) -> Result<Option<String>, AppError> {
        Ok(self.inner.lock().unwrap().active.clone())
    }

    fn set_active(&self, account: &str) -> Result<(), AppError> {
        let mut g = self.inner.lock().unwrap();
        if !g.creds.contains_key(account) {
            return Err(AppError::NotFound(format!("account {account}")));
        }
        g.active = Some(account.to_string());
        Ok(())
    }

    fn accounts(&self) -> Result<Vec<String>, AppError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .creds
            .keys()
            .cloned()
            .collect())
    }
}

/// Delegating impl so the bin can hand `Arc<dyn Keychain>` around without boilerplate.
impl<K: Keychain + ?Sized> Keychain for std::sync::Arc<K> {
    fn put(&self, record: CredentialRecord) -> Result<(), AppError> {
        (**self).put(record)
    }
    fn get(&self, account: &str) -> Result<Option<CredentialRecord>, AppError> {
        (**self).get(account)
    }
    fn delete(&self, account: &str) -> Result<(), AppError> {
        (**self).delete(account)
    }
    fn active(&self) -> Result<Option<String>, AppError> {
        (**self).active()
    }
    fn set_active(&self, account: &str) -> Result<(), AppError> {
        (**self).set_active(account)
    }
    fn accounts(&self) -> Result<Vec<String>, AppError> {
        (**self).accounts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete_round_trip() {
        let k = InMemoryKeychain::new();
        k.put(CredentialRecord {
            account: "acc-1".into(),
            secret: "sk-1".into(),
        })
        .unwrap();
        assert_eq!(
            k.get("acc-1").unwrap().unwrap().secret,
            "sk-1"
        );
        k.delete("acc-1").unwrap();
        assert!(k.get("acc-1").unwrap().is_none());
    }

    #[test]
    fn set_active_requires_known_account_and_flips_pointer() {
        let k = InMemoryKeychain::seeded([("acc-1", "sk-1"), ("acc-2", "sk-2")]);
        assert!(k.active().unwrap().is_none());
        k.set_active("acc-1").unwrap();
        assert_eq!(k.active().unwrap().as_deref(), Some("acc-1"));
        k.set_active("acc-2").unwrap();
        assert_eq!(k.active().unwrap().as_deref(), Some("acc-2"));
        // Unknown account fails — typo cannot strand the runtime.
        assert!(matches!(
            k.set_active("acc-bogus"),
            Err(AppError::NotFound(_))
        ));
        // Pointer untouched after the failed flip.
        assert_eq!(k.active().unwrap().as_deref(), Some("acc-2"));
    }

    #[test]
    fn delete_clears_active_pointer_when_it_targets_the_removed_account() {
        let k = InMemoryKeychain::seeded([("acc-1", "sk-1")]);
        k.set_active("acc-1").unwrap();
        k.delete("acc-1").unwrap();
        assert!(k.active().unwrap().is_none());
    }
}
