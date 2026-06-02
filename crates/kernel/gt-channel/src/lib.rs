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
