//! Per-workspace Dolt data-dir guard against split-brain (hq-mt-data.12).
//!
//! In the DB-per-workspace model each tenant's Dolt state lives under its own
//! data directory (`<data_root>/<ws>/`). If two processes ever open the same
//! data dir at once they fork the history — a *split-brain* whose merge is
//! manual and lossy. This module mirrors the single-tenant Dolt guard but
//! scopes it per workspace: an advisory `flock(2)` on a lockfile
//! `<data_root>/<ws>/.dolt.lock`, held for the lifetime of a [`WorkspaceLock`].
//!
//! Slug-typed, not [`WorkspaceId`](../../gt-workspace)-typed — `gt-store-dolt`
//! is a kernel crate and may not depend on the platform tier (docs/03 Rule 4).
//! The slug is re-validated before it is joined into a filesystem path, which
//! also forecloses `..`/`/` traversal out of the data root.
//!
//! The boot path should [`acquire`](WorkspaceLock::acquire) the lock *before*
//! initialising the workspace's connection pool ([`crate::WorkspacePools`]); a
//! failure means another process already owns that data dir and this one must
//! not proceed.

use std::fs::{self, File};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use crate::error::AppError;
use crate::workspace::validate_slug;

/// Name of the lockfile dropped inside each workspace's data directory.
const LOCKFILE_NAME: &str = ".dolt.lock";

/// An exclusive, advisory lock on a workspace's Dolt data directory.
///
/// Holds the open lockfile; the `flock(2)` is released automatically when the
/// fd closes on drop (kernel-released, so it survives even a panic and is freed
/// if the process dies). Hold this for as long as the process keeps the
/// workspace's Dolt data dir open.
#[derive(Debug)]
pub struct WorkspaceLock {
    /// Kept open to retain the lock; closing the fd releases the `flock`. Named
    /// with a leading underscore because it is never read — only its `Drop`
    /// matters.
    _file: File,
    path: PathBuf,
}

impl WorkspaceLock {
    /// Acquire the exclusive lock for `ws` under `data_root`, creating the
    /// workspace directory and lockfile if needed.
    ///
    /// Non-blocking: if another process already holds the lock this returns an
    /// error naming the path rather than waiting. Validates the slug first so a
    /// hostile id cannot escape `data_root`.
    pub fn acquire(data_root: impl AsRef<Path>, ws: &str) -> Result<Self, AppError> {
        validate_slug(ws)?;
        let dir = data_root.as_ref().join(ws);
        fs::create_dir_all(&dir)
            .map_err(|e| AppError::Other(format!("create dolt data dir {dir:?}: {e}")))?;
        let path = dir.join(LOCKFILE_NAME);
        let file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| AppError::Other(format!("open lockfile {path:?}: {e}")))?;

        flock_nb(&file).map_err(|e| match e {
            FlockError::WouldBlock => AppError::Other(format!(
                "workspace {ws:?} data dir already locked by another process ({path:?})"
            )),
            FlockError::Io(msg) => AppError::Other(format!("flock {path:?}: {msg}")),
        })?;

        Ok(Self { _file: file, path })
    }

    /// Boot probe: report whether the lock for `ws` is currently acquirable
    /// *without* retaining it. Acquires then immediately releases. A `false`
    /// (and the `Ok(false)` it returns) means another process holds it.
    pub fn is_acquirable(data_root: impl AsRef<Path>, ws: &str) -> Result<bool, AppError> {
        match Self::acquire(data_root, ws) {
            Ok(_lock) => Ok(true), // dropped here → released
            Err(AppError::Other(msg)) if msg.contains("already locked") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Path of the held lockfile.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Outcome of a non-blocking `flock`.
enum FlockError {
    /// The lock is held by another open file description.
    WouldBlock,
    /// A real I/O failure from the syscall.
    Io(String),
}

/// Take an exclusive, non-blocking advisory lock on `file`'s descriptor via
/// `flock(2)`. Returns `WouldBlock` when the lock is already held.
fn flock_nb(file: &File) -> Result<(), FlockError> {
    // SAFETY: `fd` is a valid descriptor for the lifetime of this call because
    // `file` is borrowed; `flock` only consults the descriptor and never
    // retains it.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(code) if code == libc::EWOULDBLOCK => Err(FlockError::WouldBlock),
        _ => Err(FlockError::Io(err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique throwaway data root per test, under the system temp dir.
    fn tmp_root(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("gt-dolt-lock-{tag}-{nanos}"));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn acquire_creates_dir_and_lockfile() {
        let root = tmp_root("acq");
        let lock = WorkspaceLock::acquire(&root, "acme").unwrap();
        assert!(lock.path().exists());
        assert_eq!(lock.path(), root.join("acme").join(LOCKFILE_NAME));
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn second_acquire_blocks_while_held() {
        let root = tmp_root("contend");
        let held = WorkspaceLock::acquire(&root, "acme").unwrap();
        // Same data dir, different open file description → must fail fast.
        let err = WorkspaceLock::acquire(&root, "acme").unwrap_err();
        assert!(matches!(err, AppError::Other(ref m) if m.contains("already locked")));
        // A different workspace is independent.
        let _other = WorkspaceLock::acquire(&root, "beta").unwrap();
        drop(held);
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn lock_released_on_drop() {
        let root = tmp_root("release");
        {
            let _l = WorkspaceLock::acquire(&root, "acme").unwrap();
        }
        // Previous lock dropped → re-acquire succeeds.
        let _again = WorkspaceLock::acquire(&root, "acme").unwrap();
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn is_acquirable_reflects_held_state() {
        let root = tmp_root("probe");
        assert!(WorkspaceLock::is_acquirable(&root, "acme").unwrap());
        let _held = WorkspaceLock::acquire(&root, "acme").unwrap();
        assert!(!WorkspaceLock::is_acquirable(&root, "acme").unwrap());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn rejects_bad_slug() {
        let root = tmp_root("badslug");
        assert!(WorkspaceLock::acquire(&root, "../escape").is_err());
        assert!(WorkspaceLock::acquire(&root, "Bad_Slug").is_err());
        fs::remove_dir_all(&root).ok();
    }
}
