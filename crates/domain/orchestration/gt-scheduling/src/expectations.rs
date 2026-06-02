//! Expectations / SLAs como eventos: las ausencias se vuelven de primera clase.
//!
//! El lease / visibility-timeout es la red para polecats muertos: un bead `dispatched` cuyo
//! polecat no da señales vuelve a `pending`. La detección es **pura** (compara edades dadas
//! como datos, no lee el reloj) → replay-able (regla de determinismo, `docs/06`).

/// Dado los leases vivos como `(bead_id, edad_segundos)` y un timeout, devuelve los beads
/// cuyo lease expiró. El `now`/edades entran como datos; el productor (tick async) los
/// calcula en el borde y llama a esto.
pub fn expired(leases: &[(String, u64)], timeout_secs: u64) -> Vec<String> {
    leases
        .iter()
        .filter(|(_, age)| *age > timeout_secs)
        .map(|(id, _)| id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_expired_leases() {
        let leases = vec![
            ("fresh".to_string(), 10),
            ("stale".to_string(), 120),
            ("edge".to_string(), 60),
        ];
        let out = expired(&leases, 60);
        assert_eq!(out, vec!["stale".to_string()]); // >60 estricto; edge==60 no expira
    }
}
