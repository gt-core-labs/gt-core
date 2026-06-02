use crate::error::AppError;

/// Patrón `Command { validate, execute }` — **síncrono** (ver `docs/09-llm-integration.md`).
///
/// `validate` permite a un cliente (p. ej. un modelo vía `gt-mcp`) "preguntar sin hacer":
/// comprobar si el command sería aceptado **sin efectos colaterales**. `execute` aplica el
/// command al estado en memoria. Ambos son puros y sync; la I/O (persistir el efecto,
/// spawnear procesos) la hace el actor/adaptador en el borde, no el command. Por eso este
/// trait NO lleva `#[async_trait]` — eso queda confinado a `gt-plugin`.
pub trait Command {
    type Output;
    type State;

    /// ¿Sería aceptado el command dado el estado actual? SIN EFECTOS, SIN I/O.
    fn validate(&self, state: &Self::State) -> Result<(), AppError>;

    /// Aplica el command al estado en memoria. Solo tras `validate` exitoso; el actor
    /// revalida al ejecutar para cerrar la ventana TOCTOU.
    fn execute(&self, state: &mut Self::State) -> Result<Self::Output, AppError>;
}
