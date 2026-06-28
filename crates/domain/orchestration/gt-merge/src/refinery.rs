//! `refinery` — productor que **await MERGE_READY** sobre un `gt-channel::Channel` y lo
//! traduce a `Submit` para el actor de merge.
//!
//! Es deliberadamente fino: no toca estado, no decide nada. La refinery es la frontera de
//! I/O entre el mailbox de archivo (proceso externo: un polecat refinador) y el dominio
//! interno. El payload del mensaje se decodifica como `MergeReadyPayload` (JSON); un
//! mensaje malformado se acka silenciosamente (no se reentrega para evitar loop).
//!
//! At-least-once: el `ack` ocurre **después** de empujar el `Submit` al actor. Si el
//! proceso muere entre ambos pasos, el archivo sigue ahí y la próxima refinery reanudará.
//!
//! ## Criterio: por qué la refinery NO es un agente (gtcore-f2ac1d)
//!
//! El epic `orchd→mayor→polecats` pregunta *dónde aporta un agente con criterio por encima del
//! loop in-process*. La refinery es el caso **negativo** canónico de ese criterio: para cada
//! mensaje MERGE_READY existe **exactamente una acción correcta** — decodificar el payload y
//! empujar un `Submit` al actor (o ackear un payload corrupto). No hay juicio que ejercer,
//! ningún estado que interpretar, ninguna decisión no trivial; un agente razonando aquí solo
//! quemaría tokens para reproducir lo que un `match` de tres ramas ya hace de forma determinista
//! y barata. Por eso la refinery se queda **in-process** (puente de I/O puro), supervisada por
//! `gt_polecat::supervise_daemon` para la única recuperación que necesita —la *mecánica*:
//! reinicio con backoff si la subscripción al canal cae.
//!
//! El juicio vive una capa **más arriba**, en el `sheriff` (ver `gt_composition::role_agent`):
//! el sheriff es el agente con criterio que se dispara por `merge.failed.v1`/`merge.ready.v1` y
//! decide lo que un loop no puede —si un slot fallido es recuperable con seguridad, si reordenar
//! o desbloquear la cola, o si escalar a un humano. Reparto resultante: **refinery = bridge
//! determinista in-process; sheriff = criterio event-triggered single-shot**. El sheriff
//! conserva la refinery in-process como fallback mecánico, nunca la reemplaza — que es justo el
//! "manteniendo el fallback in-process" del criterio de aceptación.

use gt_channel::Channel;

use crate::actor::MergeHandle;
use crate::events::MergeReadyPayload;

/// Run the refinery recv loop **inline** (awaitable). Returns `Ok(())` when the channel
/// `Receiver` closes (canal abandonado), or `Err` if the initial subscribe fails — that shape
/// makes the loop usable under `gt_polecat::supervise_daemon`, which restarts it with backoff
/// (hq-mc72.12 C2). Sibling of [`spawn`] (which is the fire-and-forget shape pre-C2).
pub async fn run(channel: Channel, merge: MergeHandle) -> Result<(), gt_channel::ChannelError> {
    let mut rx = channel.subscribe(64)?;
    while let Some(msg) = rx.recv().await {
        match serde_json::from_slice::<MergeReadyPayload>(&msg.payload) {
            Ok(payload) => {
                merge.submit(payload.bead, payload.branch, msg.id.clone()).await;
                let _ = channel.ack(&msg);
            }
            Err(_) => {
                // Mensaje corrupto: ackea para evitar reentrega infinita. (En producción
                // querríamos un evento `RefineryReject` en el bus; queda como sucesor.)
                let _ = channel.ack(&msg);
            }
        }
    }
    Ok(())
}

/// Lanza la refinery. Devuelve cuando la subscripción al canal falla; en el camino feliz
/// la task vive hasta que el `Receiver` se cierra (canal abandonado).
pub fn spawn(channel: Channel, merge: MergeHandle) -> Result<(), gt_channel::ChannelError> {
    let mut rx = channel.subscribe(64)?;
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match serde_json::from_slice::<MergeReadyPayload>(&msg.payload) {
                Ok(payload) => {
                    merge.submit(payload.bead, payload.branch, msg.id.clone()).await;
                    let _ = channel.ack(&msg);
                }
                Err(_) => {
                    // Mensaje corrupto: ackea para evitar reentrega infinita. (En producción
                    // querríamos un evento `RefineryReject` en el bus; queda como sucesor.)
                    let _ = channel.ack(&msg);
                }
            }
        }
    });
    Ok(())
}
