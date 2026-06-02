use thiserror::Error;

/// Error de dominio del núcleo. Sin variantes de I/O aquí: la I/O vive en los bordes
/// (`gt-store-*`, supervisores) y mapea sus errores a estas variantes al cruzar la frontera.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AppError {
    /// Transición de estado ilegal rechazada por una state machine.
    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    /// Entidad no encontrada.
    #[error("not found: {0}")]
    NotFound(String),

    /// Un command no pasó la validación.
    #[error("validation failed: {0}")]
    Validation(String),

    /// Un handler del bus devolvió error (va a dead-letter, no rompe el fan-out).
    #[error("handler error: {0}")]
    Handler(String),

    /// Cajón de sastre para errores aún sin clasificar.
    #[error("{0}")]
    Other(String),
}
