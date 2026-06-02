//! Linux keychain adapter (`Secret Service` / kwallet via `keyring` v3).
//!
//! Two entry kinds live in the OS keyring:
//! - `gastown:<account>` — credential blob (the API key).
//! - `gastown:__active__` — the live-pointer (account name only).
//!
//! On a host with no D-Bus secret store, the bin should fall back to [`InMemoryKeychain`]
//! (the caller decides — this adapter just surfaces the error, it does not auto-fallback).

use keyring::Entry;

use gt_events::AppError;

use super::{CredentialRecord, Keychain};

const SERVICE: &str = "gastown";
const ACTIVE_USER: &str = "__active__";
/// Index entry name: comma-separated list of accounts stored in the keyring (the OS-level
/// keyring does not expose enumeration, so the domain maintains its own index).
const INDEX_USER: &str = "__index__";

/// Talks to the platform secret store via the `keyring` crate. Stateless — every call goes
/// through a fresh [`Entry`]; the underlying crate caches the D-Bus connection per process.
#[derive(Debug, Default)]
pub struct LinuxKeychain;

impl LinuxKeychain {
    pub fn new() -> Self {
        Self
    }

    fn entry(account: &str) -> Result<Entry, AppError> {
        Entry::new(SERVICE, account).map_err(|e| AppError::Other(format!("keyring entry: {e}")))
    }

    fn read_index(&self) -> Result<Vec<String>, AppError> {
        let entry = Self::entry(INDEX_USER)?;
        match entry.get_password() {
            Ok(s) if s.is_empty() => Ok(Vec::new()),
            Ok(s) => Ok(s.split(',').map(|p| p.to_string()).collect()),
            Err(keyring::Error::NoEntry) => Ok(Vec::new()),
            Err(e) => Err(AppError::Other(format!("read index: {e}"))),
        }
    }

    fn write_index(&self, accounts: &[String]) -> Result<(), AppError> {
        let entry = Self::entry(INDEX_USER)?;
        entry
            .set_password(&accounts.join(","))
            .map_err(|e| AppError::Other(format!("write index: {e}")))
    }
}

impl Keychain for LinuxKeychain {
    fn put(&self, record: CredentialRecord) -> Result<(), AppError> {
        let entry = Self::entry(&record.account)?;
        entry
            .set_password(&record.secret)
            .map_err(|e| AppError::Other(format!("set credential: {e}")))?;
        let mut index = self.read_index()?;
        if !index.iter().any(|a| a == &record.account) {
            index.push(record.account.clone());
            self.write_index(&index)?;
        }
        Ok(())
    }

    fn get(&self, account: &str) -> Result<Option<CredentialRecord>, AppError> {
        let entry = Self::entry(account)?;
        match entry.get_password() {
            Ok(secret) => Ok(Some(CredentialRecord {
                account: account.to_string(),
                secret,
            })),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(AppError::Other(format!("get credential: {e}"))),
        }
    }

    fn delete(&self, account: &str) -> Result<(), AppError> {
        let entry = Self::entry(account)?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => {}
            Err(e) => return Err(AppError::Other(format!("delete credential: {e}"))),
        }
        let mut index = self.read_index()?;
        index.retain(|a| a != account);
        self.write_index(&index)?;
        // Clear the live pointer if it targeted the removed account, matching `InMemoryKeychain`.
        if self.active()?.as_deref() == Some(account) {
            let active = Self::entry(ACTIVE_USER)?;
            let _ = active.delete_credential();
        }
        Ok(())
    }

    fn active(&self) -> Result<Option<String>, AppError> {
        let entry = Self::entry(ACTIVE_USER)?;
        match entry.get_password() {
            Ok(s) if s.is_empty() => Ok(None),
            Ok(s) => Ok(Some(s)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(AppError::Other(format!("get active: {e}"))),
        }
    }

    fn set_active(&self, account: &str) -> Result<(), AppError> {
        // Require the credential to exist — same guarantee as `InMemoryKeychain` so a typo
        // cannot strand the runtime mid-rotation.
        if self.get(account)?.is_none() {
            return Err(AppError::NotFound(format!("account {account}")));
        }
        let entry = Self::entry(ACTIVE_USER)?;
        entry
            .set_password(account)
            .map_err(|e| AppError::Other(format!("set active: {e}")))
    }

    fn accounts(&self) -> Result<Vec<String>, AppError> {
        self.read_index()
    }
}
