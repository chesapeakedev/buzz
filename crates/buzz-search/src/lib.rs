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

use std::time::Duration;

use sqlx::{PgPool, SqlitePool};

/// Backend-specific search resources.
#[derive(Debug, Clone)]
enum SearchBackend {
    Postgres(PgPool),
    Sqlite {
        pool: SqlitePool,
        cache: Option<moka::future::Cache<SearchCacheKey, query::SearchResult>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SearchCacheKey {
    community: CommunityId,
    text: String,
    mode: u8,
    scope: u8,
    channels: Vec<uuid::Uuid>,
    kinds: Vec<i32>,
    authors: Vec<Vec<u8>>,
    since: Option<i64>,
    until: Option<i64>,
    page: u32,
    per_page: u32,
}

impl From<&query::SearchQuery> for SearchCacheKey {
    fn from(value: &query::SearchQuery) -> Self {
        let (scope, mut channels) = match &value.channel_scope {
            query::ChannelScope::Any => (0, Vec::new()),
            query::ChannelScope::ChannelLessOnly => (1, Vec::new()),
            query::ChannelScope::Channels(channels) => (2, channels.clone()),
            query::ChannelScope::ChannelsOrChannelLess(channels) => (3, channels.clone()),
        };
        channels.sort_unstable();
        channels.dedup();
        let mut kinds = value.kinds.clone().unwrap_or_default();
        kinds.sort_unstable();
        kinds.dedup();
        let mut authors = value.authors.clone().unwrap_or_default();
        authors.sort_unstable();
        authors.dedup();
        Self {
            community: value.community,
            text: value.q.trim().to_owned(),
            mode: match value.mode {
                query::SearchMode::FullText => 0,
                query::SearchMode::Prefix => 1,
            },
            scope,
            channels,
            kinds,
            authors,
            since: value.since,
            until: value.until,
            page: value.page,
            per_page: value.per_page,
        }
    }
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
        Self::new_sqlite_with_ttl(pool, Duration::ZERO)
    }

    /// Build a SQLite search service with a bounded embedded result cache.
    pub fn new_sqlite_with_ttl(pool: SqlitePool, ttl: Duration) -> Self {
        let cache = (!ttl.is_zero()).then(|| {
            moka::future::Cache::builder()
                .max_capacity(512)
                .time_to_live(ttl)
                .build()
        });
        Self {
            backend: SearchBackend::Sqlite { pool, cache },
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
        if matches!(&self.backend, SearchBackend::Sqlite { .. }) {
            metrics::counter!("buzz_sqlite_read_requests_total", "lane" => "search", "outcome" => "started").increment(1);
        }
        match &self.backend {
            SearchBackend::Postgres(pool) => query::search(pool, query).await,
            SearchBackend::Sqlite { pool, cache } => {
                let Some(cache) = cache else {
                    return query::search_sqlite(pool, query).await;
                };
                let key = SearchCacheKey::from(query);
                if let Some(result) = cache.get(&key).await {
                    metrics::counter!("buzz_sqlite_read_cache_total", "lane" => "search", "outcome" => "hit").increment(1);
                    return Ok(result);
                }
                metrics::counter!("buzz_sqlite_read_cache_total", "lane" => "search", "outcome" => "miss").increment(1);
                let pool = pool.clone();
                let query = query.clone();
                cache
                    .try_get_with(key, async move {
                        metrics::gauge!("buzz_sqlite_read_loaders_in_flight", "lane" => "search")
                            .increment(1.0);
                        let started = std::time::Instant::now();
                        let result = tokio::time::timeout(
                            Duration::from_secs(5),
                            query::search_sqlite(&pool, &query),
                        )
                        .await;
                        metrics::gauge!("buzz_sqlite_read_loaders_in_flight", "lane" => "search")
                            .decrement(1.0);
                        metrics::histogram!("buzz_sqlite_read_loader_seconds", "lane" => "search")
                            .record(started.elapsed().as_secs_f64());
                        result.map_err(|_| {
                            SearchError::ReadUnavailable(
                                "search loader deadline exceeded".to_owned(),
                            )
                        })?
                    })
                    .await
                    .map_err(|error| SearchError::ReadUnavailable(error.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn query(community: CommunityId, channels: Vec<Uuid>) -> query::SearchQuery {
        query::SearchQuery {
            community,
            q: " hello ".to_owned(),
            channel_scope: query::ChannelScope::Channels(channels),
            kinds: Some(vec![9, 1, 9]),
            authors: Some(vec![vec![2; 32], vec![1; 32]]),
            since: Some(1),
            until: Some(2),
            page: 1,
            per_page: 100,
            mode: query::SearchMode::FullText,
        }
    }

    #[test]
    fn embedded_cache_key_normalizes_filters_and_preserves_tenant_scope() {
        let community_a = CommunityId::from_uuid(Uuid::new_v4());
        let community_b = CommunityId::from_uuid(Uuid::new_v4());
        let channel_a = Uuid::new_v4();
        let channel_b = Uuid::new_v4();
        let left = SearchCacheKey::from(&query(community_a, vec![channel_b, channel_a]));
        let right = SearchCacheKey::from(&query(community_a, vec![channel_a, channel_b]));
        assert_eq!(left, right);
        assert_ne!(
            left,
            SearchCacheKey::from(&query(community_b, vec![channel_a, channel_b]))
        );
        assert_ne!(
            left,
            SearchCacheKey::from(&query(community_a, vec![channel_a]))
        );
    }
}
