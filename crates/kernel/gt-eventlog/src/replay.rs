use serde::Deserialize;

use gt_events::AppError;

use crate::record::EventRecord;

/// Re-corre el log por la lógica **pura** de un dominio y reconstruye su estado final.
///
/// `apply` es el reducer del dominio (event-sourcing: los eventos son hechos, se pliegan
/// incondicionalmente; la validación vive en el camino de escritura, no aquí). Como `apply`
/// es puro y no lee reloj/random, el estado reconstruido es **determinista** respecto al
/// log: misma entrada → mismo estado, byte a byte (gate del Paso 3).
pub fn replay<S, E, F>(records: &[EventRecord], initial: S, mut apply: F) -> Result<S, AppError>
where
    E: for<'de> Deserialize<'de>,
    F: FnMut(&mut S, &E),
{
    let mut state = initial;
    for record in records {
        let event: E = record.decode()?;
        apply(&mut state, &event);
    }
    Ok(state)
}
