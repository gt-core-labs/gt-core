use mysql_async::Pool;

use crate::error::AppError;

/// Crea un pool de conexiones al servidor Dolt vía el cable MySQL.
/// `url` p. ej. `mysql://root@127.0.0.1:3307/hq`.
pub fn connect(url: &str) -> Result<Pool, AppError> {
    Pool::from_url(url).map_err(|e| AppError::Other(format!("dolt connect: {e}")))
}

/// Mapea un error de mysql_async a `AppError`.
pub(crate) fn map_err(e: impl std::fmt::Display) -> AppError {
    AppError::Other(format!("dolt: {e}"))
}
