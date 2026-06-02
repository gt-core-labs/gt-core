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

/// Re-corre un log **heterogéneo** plegando cada record por su `(kind, version)`.
///
/// A diferencia de [`replay`] — que decodifica *todos* los records al mismo tipo `E` y
/// por tanto solo sirve para un stream de un único kind — esta variante entrega al reducer
/// el [`EventRecord`] type-erased completo. El reducer despacha sobre
/// [`EventRecord::kind`] (el string `<module>.<noun>.v<N>`, que **ya lleva la versión
/// embebida**) y decodifica el payload del arm que toca. Eso permite que `bead.created.v1`
/// y `bead.created.v2` **coexistan** en un mismo log append-only y cada uno pliegue por su
/// reductor (non-negotiable #5 / guardrail Rule 5: los kinds versionados coexisten).
///
/// La "tabla de dispatch keyed `(kind, version)`" es idiomáticamente un `match`
/// `record.kind.as_str()` dentro de `dispatch`: un mapa `HashMap<_, Box<dyn Fn…>>` violaría
/// la regla kernel de *no `dyn` fuera de plugins observadores*. El reducer es **puro** (no
/// lee reloj/random), así que el estado reconstruido es **determinista** respecto al log:
/// misma entrada → mismo estado, byte a byte (gate del Paso 3). Errores de decodificación
/// del arm se propagan vía `AppError`.
pub fn replay_dispatch<S, F>(
    records: &[EventRecord],
    initial: S,
    mut dispatch: F,
) -> Result<S, AppError>
where
    F: FnMut(&mut S, &EventRecord) -> Result<(), AppError>,
{
    let mut state = initial;
    for record in records {
        dispatch(&mut state, record)?;
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // Dos versiones del MISMO noun `demo.created`: v2 añade un campo. En el cable cada una
    // viaja como su propio `EventRecord` con su `kind` versionado.
    #[derive(Serialize, Deserialize)]
    struct CreatedV1 {
        name: String,
    }
    #[derive(Serialize, Deserialize)]
    struct CreatedV2 {
        name: String,
        #[allow(dead_code)]
        color: String,
    }

    fn record(kind: &str, payload: serde_json::Value) -> EventRecord {
        EventRecord {
            event_id: format!("evt-{kind}"),
            correlation_id: "corr-1".into(),
            causation_id: None,
            ts: "2026-06-02T00:00:00Z".into(),
            kind: kind.into(),
            payload,
        }
    }

    // Estado reconstruido: nombres en orden de log + cuántos traían color (solo v2).
    #[derive(Default, PartialEq, Debug, Clone)]
    struct State {
        names: Vec<String>,
        with_color: usize,
    }

    // La "tabla de dispatch" keyed por `(kind, version)`: un match sobre el kind versionado.
    fn dispatch(state: &mut State, rec: &EventRecord) -> Result<(), AppError> {
        match rec.kind.as_str() {
            "demo.created.v1" => {
                let e: CreatedV1 = rec.decode()?;
                state.names.push(e.name);
            }
            "demo.created.v2" => {
                let e: CreatedV2 = rec.decode()?;
                state.names.push(e.name);
                state.with_color += 1;
            }
            other => return Err(AppError::Other(format!("unknown kind {other:?}"))),
        }
        Ok(())
    }

    #[test]
    fn mixed_v1_v2_same_noun_both_apply() {
        let log = vec![
            record("demo.created.v1", serde_json::json!({ "name": "alpha" })),
            record(
                "demo.created.v2",
                serde_json::json!({ "name": "beta", "color": "red" }),
            ),
            record("demo.created.v1", serde_json::json!({ "name": "gamma" })),
        ];

        let state = replay_dispatch(&log, State::default(), dispatch).unwrap();

        // Ambas versiones plegaron: los 3 nombres en orden, 1 con color (la v2).
        assert_eq!(state.names, vec!["alpha", "beta", "gamma"]);
        assert_eq!(state.with_color, 1);
    }

    #[test]
    fn replay_dispatch_is_deterministic() {
        let log = vec![
            record(
                "demo.created.v2",
                serde_json::json!({ "name": "a", "color": "g" }),
            ),
            record("demo.created.v1", serde_json::json!({ "name": "b" })),
        ];

        let first = replay_dispatch(&log, State::default(), dispatch).unwrap();
        let second = replay_dispatch(&log, State::default(), dispatch).unwrap();

        // Misma entrada → mismo estado (gate de replay determinista del Paso 3).
        assert_eq!(first, second);
    }

    #[test]
    fn unknown_kind_propagates_error() {
        let log = vec![record(
            "demo.created.v9",
            serde_json::json!({ "name": "x" }),
        )];
        let err = replay_dispatch(&log, State::default(), dispatch);
        assert!(err.is_err(), "un kind sin arm debe propagar AppError");
    }
}
