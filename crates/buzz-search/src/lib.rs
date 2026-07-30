#![deny(unsafe_code)]
#![warn(missing_docs)]
//! Buzz search — community-scoped full-text search over Buzz events.
//!
//! PostgreSQL uses an `events.search_tsv` generated column and GIN index.
//! SQLite uses an external-content FTS5 table maintained by database triggers
//! in the event mutation transaction. In both adapters every row write is the
//! index update—there is no separate indexer or consistency window.
//!
//! This crate is the **query** side. Indexing is the SQL row insert — owned
//! by `buzz-db`. The relay refetches canonical events through `buzz-db`'s
//! scoped fetcher and runs access checks per hit; search is never the access
//! boundary (conformance row 50).
//!
//! ## Multi-tenant fence
//!
//! Every [`SearchQuery`] carries a [`CommunityId`]. There is no construction
//! path through this crate that omits it, and every SQL execution binds
//! `community_id = $ctx` as the first predicate. A query bound to community A
//! cannot return events stored under community B, by construction.

/// Search error types.
pub mod error;
/// Search query execution.
pub mod query;

pub use buzz_core::CommunityId;
pub use error::SearchError;
pub use query::{search, ChannelScope, SearchHit, SearchMode, SearchQuery, SearchResult};

use sqlx::{PgPool, SqlitePool};

/// Backend-specific search resources.
#[derive(Debug, Clone)]
enum SearchBackend {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

/// Backend-neutral handle for community-scoped full-text search.
#[derive(Debug, Clone)]
pub struct SearchService {
    backend: SearchBackend,
}

impl SearchService {
    /// Build a search service over an existing PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            backend: SearchBackend::Postgres(pool),
        }
    }

    /// Build a search service over an existing SQLite pool.
    pub fn new_sqlite(pool: SqlitePool) -> Self {
        Self {
            backend: SearchBackend::Sqlite(pool),
        }
    }

    /// Connect the PostgreSQL search adapter.
    ///
    /// Keeping pool construction inside the owning adapter lets relay startup
    /// select this implementation without importing PostgreSQL driver types.
    pub async fn connect_postgres(database_url: &str) -> Result<Self, SearchError> {
        Ok(Self::new(PgPool::connect(database_url).await?))
    }

    /// Connect the SQLite search adapter.
    ///
    /// The caller owns migration and connection-policy setup; this constructor
    /// only creates the search-side pool.
    pub async fn connect_sqlite(database_url: &str) -> Result<Self, SearchError> {
        Ok(Self::new_sqlite(SqlitePool::connect(database_url).await?))
    }

    /// Execute a community-scoped FTS query.
    pub async fn search(&self, query: &SearchQuery) -> Result<SearchResult, SearchError> {
        match &self.backend {
            SearchBackend::Postgres(pool) => query::search(pool, query).await,
            SearchBackend::Sqlite(pool) => query::search_sqlite(pool, query).await,
        }
    }
}
