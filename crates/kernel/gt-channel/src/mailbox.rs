//! File-backed mailbox: cross-process at-least-once delivery via `notify`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use notify::{event::ModifyKind, EventKind as NotifyEventKind, RecursiveMode, Watcher};
use tokio::sync::mpsc;

/// Errors at the mailbox boundary. I/O is mapped to a small enum so callers don't drag in
/// `std::io::Error` everywhere; `notify` failures collapse into `Watcher`.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("channel io: {0}")]
    Io(#[from] std::io::Error),
    #[error("channel watcher: {0}")]
    Watcher(String),
}

/// Handle to a single named channel (one directory on disk). Cheap to clone — the
/// directory is shared, not the watcher.
#[derive(Clone)]
pub struct Channel {
    dir: PathBuf,
}

/// One message read off a channel. The consumer must call [`Channel::ack`] after the
/// effect is committed; otherwise the message is re-delivered on the next subscribe
/// (at-least-once). `payload` is the raw file bytes; encoding (JSON, etc.) is the
/// caller's contract.
#[derive(Debug, Clone)]
pub struct ChannelMessage {
    pub id: String,
    pub path: PathBuf,
    pub payload: Vec<u8>,
}

impl Channel {
    /// Open (creating if missing) the directory `<root>/<name>`.
    pub fn open(root: impl AsRef<Path>, name: &str) -> Result<Self, ChannelError> {
        let dir = root.as_ref().join(name);
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Append a message to the channel. Atomic: writes a hidden `.<ulid>.tmp` first then
    /// renames, so a subscribed watcher never sees a half-written file. Returns the
    /// message id (the basename without extension).
    pub fn emit(&self, payload: &[u8]) -> Result<String, ChannelError> {
        let id = ulid::Ulid::new().to_string();
        let final_path = self.dir.join(format!("{id}.event"));
        let tmp_path = self.dir.join(format!(".{id}.tmp"));
        std::fs::write(&tmp_path, payload)?;
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(id)
    }

    /// Acknowledge a message: delete the backing file. Idempotent (missing file = Ok).
    pub fn ack(&self, msg: &ChannelMessage) -> Result<(), ChannelError> {
        match std::fs::remove_file(&msg.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Drain existing messages and stream new ones into an mpsc receiver. The watcher
    /// runs on a dedicated std thread (notify's callback thread isn't a tokio worker,
    /// so we can `blocking_send` safely). Dropping the receiver tears the thread down.
    ///
    /// Each `*.event` file is delivered at most once per subscription (a HashSet de-dupes
    /// the Create/Modify(Name(To)) burst inotify emits on atomic rename). Across
    /// subscriptions, an un-acked file is re-delivered — that's the at-least-once
    /// guarantee callers rely on.
    ///
    /// Delivery is **unordered**, not FIFO. The drain sorts filenames so the order is
    /// deterministic *given a set of files*, but ULID ids only sort by emit order when the
    /// emits land in distinct milliseconds — within the same ms the time bits tie and the
    /// random suffix decides, so two same-ms emits may drain in either order. This is fine
    /// for the only consumer (the `gt-merge` refinery turns each message into an
    /// independent `MergeSlot`); callers that need total order must not rely on the channel.
    pub fn subscribe(&self, buffer: usize) -> Result<mpsc::Receiver<ChannelMessage>, ChannelError> {
        let (tx, rx) = mpsc::channel::<ChannelMessage>(buffer);

        // Snapshot of existing files — drained before live events.
        let mut existing: Vec<PathBuf> = std::fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| is_event_file(p))
            .collect();
        existing.sort();

        // Bridge: notify callback thread -> std mpsc -> our forwarder thread.
        let (raw_tx, raw_rx) = std::sync::mpsc::channel::<notify::Result<notify::Event>>();
        let mut watcher = notify::recommended_watcher(move |res| {
            let _ = raw_tx.send(res);
        })
        .map_err(|e| ChannelError::Watcher(e.to_string()))?;
        watcher
            .watch(&self.dir, RecursiveMode::NonRecursive)
            .map_err(|e| ChannelError::Watcher(e.to_string()))?;

        std::thread::spawn(move || {
            // Hold the watcher inside the thread: dropping it stops the OS watch.
            let _watcher = watcher;
            let mut delivered: HashSet<String> = HashSet::new();

            for path in existing {
                if !try_forward(&path, &tx, &mut delivered) {
                    return;
                }
            }

            while let Ok(event) = raw_rx.recv() {
                let Ok(event) = event else { continue };
                if !is_arrival(&event.kind) {
                    continue;
                }
                for path in event.paths {
                    if !is_event_file(&path) {
                        continue;
                    }
                    if !try_forward(&path, &tx, &mut delivered) {
                        return;
                    }
                }
            }
        });

        Ok(rx)
    }
}

fn is_event_file(path: &Path) -> bool {
    if path.extension().and_then(|s| s.to_str()) != Some("event") {
        return false;
    }
    // Skip the atomic-write tmp (".<ulid>.tmp"): different extension, but guard against
    // hidden files just in case the platform reports them.
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| !n.starts_with('.'))
        .unwrap_or(false)
}

fn is_arrival(kind: &NotifyEventKind) -> bool {
    matches!(
        kind,
        NotifyEventKind::Create(_) | NotifyEventKind::Modify(ModifyKind::Name(_))
    )
}

fn try_forward(
    path: &Path,
    tx: &mpsc::Sender<ChannelMessage>,
    delivered: &mut HashSet<String>,
) -> bool {
    let Some(id) = path.file_stem().and_then(|s| s.to_str()).map(str::to_owned) else {
        return true;
    };
    if !delivered.insert(id.clone()) {
        return true; // duplicate event from inotify burst
    }
    let payload = match std::fs::read(path) {
        Ok(b) => b,
        // The file vanished between the notify event and our read — likely already
        // acked by another consumer; treat as already-delivered.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return true,
    };
    let msg = ChannelMessage {
        id,
        path: path.to_path_buf(),
        payload,
    };
    tx.blocking_send(msg).is_ok()
}
