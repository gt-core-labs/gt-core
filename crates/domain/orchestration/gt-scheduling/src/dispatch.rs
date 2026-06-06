//! `dispatch` — productor que **await peticiones de dispatch** sobre un `gt-channel::Channel` y
//! las traduce a `create_bead` + `enqueue` para el dispatcher. El dispatcher auto-despacha el bead
//! (emite `scheduling.dispatched.v1`) cuando hay capacidad — ése es el evento que el supervisor de
//! polecats observa para slingar un agente.
//!
//! Es el hermano de la `refinery` de `gt-merge`: la frontera de I/O entre el mailbox de archivo
//! (un operador, o el control plane, emite una petición) y el dominio interno. Deliberadamente
//! fino: no decide capacidad ni prioridad — eso vive en el actor. El payload se decodifica como
//! [`DispatchPayload`] (JSON); un mensaje malformado se ackea silenciosamente (no se reentrega,
//! para evitar un loop de veneno).
//!
//! At-least-once: el `ack` ocurre **después** de empujar el trabajo al actor. `create_bead` siembra
//! el bead como `Pending` (schedulable) para que el CAS-claim del bombeo lo tome; `enqueue` lo
//! encola y gatilla el bombeo. Si el proceso muere entre recv y ack, el archivo sigue ahí y la
//! próxima suscripción reanuda (create_bead/enqueue son idempotentes a nivel de efecto observable:
//! un re-upsert de un bead ya `Dispatched` no lo re-despacha porque el CAS-claim ya no lo ve
//! `Pending`).

use gt_beads::{Bead, BeadStatus};
use gt_channel::Channel;
use serde::{Deserialize, Serialize};

use crate::actor::SchedHandle;

/// Petición de dispatch leída del canal: el bead a despachar y su prioridad.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchPayload {
    /// Bead id a despachar.
    pub bead: String,
    /// Título informativo del bead en el repo del dispatcher; por defecto el id.
    #[serde(default)]
    pub title: Option<String>,
    /// Prioridad de cola, 0 = más alta (P0). Por defecto 1.
    #[serde(default = "default_priority")]
    pub priority: u8,
}

fn default_priority() -> u8 {
    1
}

/// Run the dispatch recv loop **inline** (awaitable). Returns `Ok(())` when the channel
/// `Receiver` closes (canal abandonado), or `Err` si la suscripción inicial falla — esa forma lo
/// hace usable bajo `gt_polecat::supervise_daemon`, que lo reinicia con backoff. Hermano de
/// [`crate::dispatch::spawn`] (la forma fire-and-forget).
pub async fn run(channel: Channel, sched: SchedHandle) -> Result<(), gt_channel::ChannelError> {
    let mut rx = channel.subscribe(64)?;
    while let Some(msg) = rx.recv().await {
        match serde_json::from_slice::<DispatchPayload>(&msg.payload) {
            Ok(p) => {
                apply(&sched, p).await;
                let _ = channel.ack(&msg);
            }
            Err(_) => {
                // Mensaje corrupto: ackea para evitar reentrega infinita.
                let _ = channel.ack(&msg);
            }
        }
    }
    Ok(())
}

/// Lanza el loop de dispatch como task fire-and-forget. Hermano de [`run`].
pub fn spawn(channel: Channel, sched: SchedHandle) -> Result<(), gt_channel::ChannelError> {
    let mut rx = channel.subscribe(64)?;
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match serde_json::from_slice::<DispatchPayload>(&msg.payload) {
                Ok(p) => {
                    apply(&sched, p).await;
                    let _ = channel.ack(&msg);
                }
                Err(_) => {
                    let _ = channel.ack(&msg);
                }
            }
        }
    });
    Ok(())
}

/// Siembra el bead como `Pending` y lo encola, gatillando el bombeo del dispatcher.
async fn apply(sched: &SchedHandle, p: DispatchPayload) {
    let title = p.title.unwrap_or_else(|| p.bead.clone());
    // El CAS-claim del bombeo sólo toma beads `Pending`; sembrar primero es lo que hace que un
    // bead recién pedido sea schedulable (sin esto, enqueue + pump => DispatchFailed).
    let _ = sched
        .create_bead(Bead::new(
            p.bead.clone(),
            title,
            BeadStatus::Pending,
            p.priority,
        ))
        .await;
    sched.enqueue(p.bead, p.priority).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_defaults_priority_and_title() {
        let p: DispatchPayload = serde_json::from_str(r#"{"bead":"hq-x.1"}"#).unwrap();
        assert_eq!(p.bead, "hq-x.1");
        assert_eq!(p.priority, 1);
        assert!(p.title.is_none());
    }

    #[test]
    fn payload_reads_explicit_fields() {
        let p: DispatchPayload =
            serde_json::from_str(r#"{"bead":"hq-x.2","title":"T","priority":0}"#).unwrap();
        assert_eq!(p.priority, 0);
        assert_eq!(p.title.as_deref(), Some("T"));
    }
}
