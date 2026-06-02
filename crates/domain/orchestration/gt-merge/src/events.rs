use serde::{Deserialize, Serialize};

use gt_events::EventKind;

/// Eventos del dominio merge. `Serialize`/`Deserialize` para el audit log + replay.
///
/// `Ready` es la **observación** que hace la refinery al consumir un `MERGE_READY` del
/// canal: queda en el log para que el replay reconstruya qué slots existieron alguna vez.
/// `Started` lo emite el actor al transicionar `Ready → Merging`. `Merged`/`Failed` son
/// los terminadores. El estado del slot es derivable del log (event-sourcing puro).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeEvent {
    /// Refinery observó `MERGE_READY` en el channel-events mailbox.
    /// `channel_msg_id` referencia el archivo `<ulid>.event` consumido (auditoría).
    Ready {
        bead: String,
        branch: String,
        channel_msg_id: String,
    },
    /// Actor tomó el slot: transición `Ready → Merging`.
    Started { bead: String },
    /// Merge completado: `Merging → Merged`. `sha` es el resultado de la fusión.
    Merged { bead: String, sha: String },
    /// Merge falló (conflictos, hook, etc): `Merging → Failed`.
    Failed { bead: String, reason: String },
}

impl EventKind for MergeEvent {
    fn kind(&self) -> &'static str {
        match self {
            MergeEvent::Ready { .. } => "merge.ready.v1",
            MergeEvent::Started { .. } => "merge.started.v1",
            MergeEvent::Merged { .. } => "merge.merged.v1",
            MergeEvent::Failed { .. } => "merge.failed.v1",
        }
    }
}

/// Formato del payload que la refinery espera en el channel mailbox `merge`. JSON-encoded.
/// Estable: si cambia, los productores (polecats refinadores) deben actualizar a la vez.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeReadyPayload {
    pub bead: String,
    pub branch: String,
}
