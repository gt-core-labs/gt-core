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
