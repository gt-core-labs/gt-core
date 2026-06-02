use time::OffsetDateTime;
use ulid::Ulid;

use crate::kind::EventKind;

/// Todo evento viaja envuelto. La **causación** (`causation_id`), no solo la correlación,
/// es lo que permite reconstruir cadenas rotas en el replay (ver `docs/06-observability.md`).
///
/// Los constructores [`Envelope::root`] / [`Envelope::caused_by`] generan ids y timestamp:
/// son helpers **de borde** (productores). El replay NO los usa — reconstruye envelopes a
/// partir de lo grabado, manteniendo el determinismo del núcleo.
#[derive(Debug, Clone)]
pub struct Envelope<E: EventKind> {
    /// Identidad de este evento (idempotencia / de-dup).
    pub event_id: Ulid,
    /// Workflow originante: comparte valor toda la cadena causal.
    pub correlation_id: Ulid,
    /// Evento padre que lo disparó. `None` para el evento raíz de una cadena.
    pub causation_id: Option<Ulid>,
    /// Momento de creación (en el borde).
    pub ts: OffsetDateTime,
    /// El evento de dominio en sí.
    pub payload: E,
}

impl<E: EventKind> Envelope<E> {
    /// Evento raíz de una cadena nueva: `correlation_id == event_id`, sin causa.
    pub fn root(payload: E) -> Self {
        let event_id = Ulid::new();
        Self {
            event_id,
            correlation_id: event_id,
            causation_id: None,
            ts: OffsetDateTime::now_utc(),
            payload,
        }
    }

    /// Evento causado por otro: hereda `correlation_id`, apunta su causa al padre.
    pub fn caused_by<P: EventKind>(parent: &Envelope<P>, payload: E) -> Self {
        Self {
            event_id: Ulid::new(),
            correlation_id: parent.correlation_id,
            causation_id: Some(parent.event_id),
            ts: OffsetDateTime::now_utc(),
            payload,
        }
    }

    /// Clave de ruteo del payload (atajo a `payload.kind()`).
    pub fn kind(&self) -> &'static str {
        self.payload.kind()
    }
}
