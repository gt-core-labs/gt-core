use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;

use gt_events::{AppError, Envelope, EventKind};

/// Representación **type-erased** de un evento en el cable / en el log. Es como funciona el
/// `.events.jsonl` hoy en Go: `type` para ruteo, `payload` como objeto JSON sin tipar.
/// `gt-feed` y el replay consumen esto, nunca el enum tipado.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventRecord {
    pub event_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub ts: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub payload: serde_json::Value,
}

impl EventRecord {
    /// Convierte un envelope tipado a su forma de cable. Requiere `E: Serialize`.
    pub fn from_envelope<E>(env: &Envelope<E>) -> Result<Self, AppError>
    where
        E: EventKind + Serialize,
    {
        Ok(Self {
            event_id: env.event_id.to_string(),
            correlation_id: env.correlation_id.to_string(),
            causation_id: env.causation_id.map(|c| c.to_string()),
            ts: env
                .ts
                .format(&Rfc3339)
                .map_err(|e| AppError::Other(format!("ts format: {e}")))?,
            kind: env.kind().to_string(),
            payload: serde_json::to_value(&env.payload)
                .map_err(|e| AppError::Other(format!("payload encode: {e}")))?,
        })
    }

    /// Decodifica el payload de vuelta a un evento tipado (usado por el replay).
    pub fn decode<E>(&self) -> Result<E, AppError>
    where
        E: for<'de> Deserialize<'de>,
    {
        serde_json::from_value(self.payload.clone())
            .map_err(|e| AppError::Other(format!("payload decode ({}): {e}", self.kind)))
    }
}
