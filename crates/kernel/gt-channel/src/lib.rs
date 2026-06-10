//! `gt-channel` — channel events por archivo (`*.event`): mailbox **entre procesos**
//! (await MERGE_READY, etc.; ver `docs/05-queues.md`). Entrega at-least-once por canal.
//!
//! Por qué archivos en disco y no un broker: el orquestador es local-first; cada productor
//! (un polecat que terminó trabajo) y cada consumidor (gt-merge) vive en un proceso
//! distinto y puede caer/reiniciarse independientemente. Un archivo en una carpeta del town
//! root sobrevive a ambos. Despertar sin polling vía `notify` (inotify en Linux).
//!
//! Forma del cable: `<root>/<name>/<ulid>.event`. Escritura atómica (write a `.<ulid>.tmp`
//! → `rename` al destino) para que el watcher nunca observe contenido parcial. El consumidor
//! aplica el efecto y luego llama a [`Channel::ack`], que borra el archivo; sin ack, el
//! mensaje se re-entrega tras reiniciar el subscriber (at-least-once).

mod mailbox;

pub use mailbox::{Channel, ChannelError, ChannelMessage};

// Postgres-backed dispatch queue (hq-talos-migration.11), off by default behind `pg`. The blocking
// `postgres` client + an `r2d2` pool give concurrent producers (N mcp-server replicas) + a separate
// orchd consumer over one DB — no shared-volume coupling — while the file-based `Channel` stays the
// default. Selected at the composition root by the SAME `GT_EVENTLOG_PG` opt-in the event log uses.
#[cfg(feature = "pg")]
pub mod pg;

#[cfg(feature = "pg")]
pub use pg::{dispatch_migration_sql, PgQueue, DISPATCH_TABLE};

use tokio::sync::mpsc;

/// The consume side of a channel, abstracted over the backend (hq-talos-migration.11). A consumer
/// loop (`gt_scheduling::dispatch::run`, the convoy→scheduler bridge) is written once against this
/// trait and runs on EITHER the file-based [`Channel`] (default / shared volume) or the
/// Postgres-backed [`PgQueue`] (concurrent consumers, no shared volume), selected at the
/// composition root by the `GT_EVENTLOG_PG` opt-in.
///
/// The contract is exactly the file path's: `subscribe` drains existing + streams new messages
/// into an mpsc receiver, and `ack` commits the delivery (deletes the file on the file backend;
/// a no-op on the PG backend where the SKIP-LOCKED claim already removed the row). Keeping it a
/// trait (not an enum) means a downstream crate that only ever uses the file backend
/// (`gt-scheduling`) never compiles the `pg` feature — the PG impl lives behind `#[cfg(pg)]`.
pub trait DispatchConsumer: Clone + Send + 'static {
    /// Drain existing + stream new messages into an mpsc receiver (the file path's `subscribe`).
    fn subscribe(&self, buffer: usize) -> Result<mpsc::Receiver<ChannelMessage>, ChannelError>;
    /// Commit a delivered message (the file path's `ack`).
    fn ack(&self, msg: &ChannelMessage) -> Result<(), ChannelError>;
}

impl DispatchConsumer for Channel {
    fn subscribe(&self, buffer: usize) -> Result<mpsc::Receiver<ChannelMessage>, ChannelError> {
        Channel::subscribe(self, buffer)
    }
    fn ack(&self, msg: &ChannelMessage) -> Result<(), ChannelError> {
        Channel::ack(self, msg)
    }
}

#[cfg(feature = "pg")]
impl DispatchConsumer for PgQueue {
    fn subscribe(&self, buffer: usize) -> Result<mpsc::Receiver<ChannelMessage>, ChannelError> {
        PgQueue::subscribe(self, buffer)
    }
    fn ack(&self, msg: &ChannelMessage) -> Result<(), ChannelError> {
        PgQueue::ack(self, msg)
    }
}

/// The produce side of a channel, abstracted over the backend (hq-talos-migration.11). A producer
/// (the `ConvoyHandler` dropping a `{bead,priority}` request) holds one of these and emits without
/// caring whether the message lands as a `*.event` file on the shared volume (default) or a row in
/// `public.dispatch_jobs` (Postgres). An enum, not a trait, because the producer is held in a
/// struct field where a concrete sized type is simpler than a boxed `dyn` — and there are exactly
/// two backends. The `Pg` arm is `#[cfg(pg)]`, so a producer compiled without the feature is
/// file-only.
#[derive(Clone)]
pub enum DispatchSink {
    /// File-based channel (default / shared volume). Atomic tmp→rename emit, `notify`-watched.
    File(Channel),
    /// Postgres-backed queue (concurrent producers, no shared volume): INSERT + `pg_notify`.
    #[cfg(feature = "pg")]
    Pg(PgQueue),
}

impl DispatchSink {
    /// Emit one message onto the channel — the file path's atomic `*.event` write or the PG path's
    /// INSERT + NOTIFY. The payload bytes are the producer's contract (a `{bead,priority}` JSON),
    /// identical on both backends. Returns `Ok(())` on success (the backends' distinct id types are
    /// not surfaced — the producer is fire-and-forget best-effort).
    pub fn emit(&self, payload: &[u8]) -> Result<(), ChannelError> {
        match self {
            DispatchSink::File(c) => c.emit(payload).map(|_| ()),
            #[cfg(feature = "pg")]
            DispatchSink::Pg(q) => q.emit(payload).map(|_| ()),
        }
    }
}
