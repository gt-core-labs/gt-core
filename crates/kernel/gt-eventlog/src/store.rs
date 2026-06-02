use gt_events::AppError;

use crate::record::EventRecord;

/// Puerto del log de eventos. Lo implementa `JsonlWriter` (Paso 3) y, en producción, el
/// adaptador a Postgres `JSONB` (ver `docs/04-persistence.md`). Sync: el drenado async vive
/// en el borde.
pub trait EventStore {
    /// Añade un record al final del log (append-only).
    fn append(&self, record: &EventRecord) -> Result<(), AppError>;

    /// Lee todos los records en orden de log.
    fn read_all(&self) -> Result<Vec<EventRecord>, AppError>;
}
