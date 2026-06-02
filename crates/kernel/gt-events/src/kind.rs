/// Contrato base de todo evento. La clave `kind()` es la ruta del bus.
///
/// No hay `trait Event` global con `Box<dyn Event>`: eso arrastraría `#[async_trait]`,
/// bounds `Send`/`Sync` y boxing. En su lugar cada dominio define un **enum owned** que
/// implementa este trait (ver `docs/03-events.md`).
pub trait EventKind: Send + Sync + 'static {
    /// Clave de ruteo del evento, p. ej. `"agent.spawned"`. Debe ser estable.
    fn kind(&self) -> &'static str;
}
