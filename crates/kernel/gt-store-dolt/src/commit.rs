use mysql_async::prelude::*;
use mysql_async::{params, Pool};

use crate::error::AppError;

use crate::conn::map_err;

/// `dolt commit` vía procedimiento stored: versiona el estado actual (la cola entera queda
/// reconstruible en el tiempo, ver `docs/05-queues.md`).
pub async fn commit(pool: &Pool, message: &str) -> Result<(), AppError> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    conn.exec_drop("CALL DOLT_COMMIT('-A', '-m', :m)", params! { "m" => message })
        .await
        .map_err(map_err)
}

/// `dolt reset --hard`: descarta cambios no committeados (rollback).
pub async fn rollback(pool: &Pool) -> Result<(), AppError> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    conn.query_drop("CALL DOLT_RESET('--hard')")
        .await
        .map_err(map_err)
}

/// Nº de filas cambiadas respecto al `HEAD` (resumen de diff de la working set).
pub async fn diff_summary(pool: &Pool, table: &str) -> Result<u64, AppError> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let count: Option<i64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM DOLT_DIFF('HEAD', 'WORKING', :t)",
            params! { "t" => table },
        )
        .await
        .map_err(map_err)?;
    Ok(count.unwrap_or(0) as u64)
}
