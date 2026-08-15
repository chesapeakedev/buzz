use thiserror::Error;

/// Errors produced by the FTS service.
#[derive(Debug, Error)]
pub enum SearchError {
    /// The embedded search bulkhead could not admit work before its deadline.
    #[error("embedded search temporarily unavailable: {0}")]
    ReadUnavailable(String),

    /// A database error from sqlx.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}
