//! Shared behavioral contract for the PostgreSQL and SQLite search adapters.
//!
//! SQLite runs in the default test suite. The PostgreSQL case uses the same
//! contract and is opt-in because it requires local infrastructure:
//!
//! `BUZZ_TEST_DATABASE_URL=postgres://buzz:buzz_dev@localhost:5432/buzz cargo test -p buzz-search --test backend_contract postgres_search_contract -- --ignored`

use std::{collections::BTreeSet, time::Duration};

use buzz_core::CommunityId;
use buzz_search::{ChannelScope, SearchMode, SearchQuery, SearchService};
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Executor, PgPool, SqlitePool,
};
use uuid::Uuid;

const TEST_DB_URL: &str = "postgres://buzz:buzz_dev@localhost:5432/buzz";
const SQLITE_SEARCH_MIGRATION: &str = include_str!("../../../migrations/sqlite/0008_search.sql");

#[derive(Debug)]
enum ContractBackend {
    Postgres { pool: PgPool, schema: String },
    Sqlite { pool: SqlitePool },
}

impl ContractBackend {
    async fn postgres() -> Self {
        let url =
            std::env::var("BUZZ_TEST_DATABASE_URL").unwrap_or_else(|_| TEST_DB_URL.to_string());
        let schema = format!("search_contract_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect to PostgreSQL");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin)
            .await
            .expect("create PostgreSQL contract schema");
        admin.close().await;

        let scoped_url = format!("{url}?options=-c%20search_path%3D{schema}");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&scoped_url)
            .await
            .expect("connect to PostgreSQL contract schema");
        pool.execute(
            "CREATE TABLE events (
                community_id UUID NOT NULL,
                id BYTEA NOT NULL,
                pubkey BYTEA NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                kind INT NOT NULL,
                content TEXT NOT NULL,
                channel_id UUID,
                deleted_at TIMESTAMPTZ,
                search_tsv TSVECTOR GENERATED ALWAYS AS (
                    CASE WHEN kind IN (0, 9, 40002, 45001, 45003)
                         THEN to_tsvector('simple', content)
                         ELSE NULL::tsvector
                    END
                ) STORED,
                PRIMARY KEY (community_id, id)
            );
            CREATE INDEX idx_search_contract_fts ON events USING GIN (search_tsv);",
        )
        .await
        .expect("create PostgreSQL contract table");

        Self::Postgres { pool, schema }
    }

    async fn sqlite() -> Self {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect to SQLite");
        pool.execute(
            "CREATE TABLE events (
                community_id TEXT NOT NULL,
                id BLOB NOT NULL CHECK (length(id) = 32),
                pubkey BLOB NOT NULL CHECK (length(pubkey) = 32),
                created_at INTEGER NOT NULL,
                kind INTEGER NOT NULL,
                content TEXT NOT NULL,
                channel_id TEXT,
                deleted_at INTEGER,
                PRIMARY KEY (community_id, id)
            ) STRICT;",
        )
        .await
        .expect("create SQLite contract table");
        sqlx::raw_sql(SQLITE_SEARCH_MIGRATION)
            .execute(&pool)
            .await
            .expect("apply SQLite search migration");

        Self::Sqlite { pool }
    }

    fn service(&self) -> SearchService {
        match self {
            Self::Postgres { pool, .. } => SearchService::new(pool.clone()),
            Self::Sqlite { pool } => SearchService::new_sqlite(pool.clone()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_event(
        &self,
        community: CommunityId,
        id: [u8; 32],
        pubkey: [u8; 32],
        kind: i32,
        content: &str,
        channel_id: Option<Uuid>,
        created_at: i64,
    ) {
        match self {
            Self::Postgres { pool, .. } => {
                sqlx::query(
                    "INSERT INTO events (
                        community_id, id, pubkey, created_at, kind, content, channel_id
                     ) VALUES ($1, $2, $3, to_timestamp($4), $5, $6, $7)",
                )
                .bind(community.as_uuid())
                .bind(id.as_slice())
                .bind(pubkey.as_slice())
                .bind(created_at)
                .bind(kind)
                .bind(content)
                .bind(channel_id)
                .execute(pool)
                .await
                .expect("insert PostgreSQL contract event");
            }
            Self::Sqlite { pool } => {
                sqlx::query(
                    "INSERT INTO events (
                        community_id, id, pubkey, created_at, kind, content, channel_id
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(community.as_uuid().to_string())
                .bind(id.as_slice())
                .bind(pubkey.as_slice())
                .bind(created_at.saturating_mul(1_000_000))
                .bind(kind)
                .bind(content)
                .bind(channel_id.map(|id| id.to_string()))
                .execute(pool)
                .await
                .expect("insert SQLite contract event");
            }
        }
    }

    async fn tombstone(&self, community: CommunityId, id: [u8; 32]) {
        match self {
            Self::Postgres { pool, .. } => {
                sqlx::query(
                    "UPDATE events SET deleted_at = NOW()
                     WHERE community_id = $1 AND id = $2",
                )
                .bind(community.as_uuid())
                .bind(id.as_slice())
                .execute(pool)
                .await
                .expect("tombstone PostgreSQL contract event");
            }
            Self::Sqlite { pool } => {
                sqlx::query(
                    "UPDATE events SET deleted_at = 1
                     WHERE community_id = ? AND id = ?",
                )
                .bind(community.as_uuid().to_string())
                .bind(id.as_slice())
                .execute(pool)
                .await
                .expect("tombstone SQLite contract event");
            }
        }
    }

    async fn teardown(self) {
        match self {
            Self::Sqlite { pool } => pool.close().await,
            Self::Postgres { pool, schema } => {
                pool.close().await;
                let url = std::env::var("BUZZ_TEST_DATABASE_URL")
                    .unwrap_or_else(|_| TEST_DB_URL.to_string());
                let admin = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await
                    .expect("reconnect to PostgreSQL for teardown");
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP SCHEMA \"{schema}\" CASCADE"
                )))
                .execute(&admin)
                .await
                .expect("drop PostgreSQL contract schema");
                admin.close().await;
            }
        }
    }
}

fn event_id(value: u8) -> [u8; 32] {
    [value; 32]
}

fn query(community: CommunityId, text: &str) -> SearchQuery {
    SearchQuery {
        community,
        q: text.to_string(),
        channel_scope: ChannelScope::Any,
        kinds: None,
        authors: None,
        since: None,
        until: None,
        page: 1,
        per_page: 100,
        mode: SearchMode::FullText,
    }
}

fn ids(result: &buzz_search::SearchResult) -> BTreeSet<[u8; 32]> {
    result.hits.iter().map(|hit| hit.event_id).collect()
}

async fn run_search_contract(backend: ContractBackend) {
    let community = CommunityId::from_uuid(Uuid::new_v4());
    let other_community = CommunityId::from_uuid(Uuid::new_v4());
    let channel_a = Uuid::new_v4();
    let channel_b = Uuid::new_v4();
    let author_a = event_id(101);
    let author_b = event_id(102);

    backend
        .insert_event(
            community,
            event_id(1),
            author_a,
            9,
            "alpha quick brown fox",
            None,
            100,
        )
        .await;
    backend
        .insert_event(
            community,
            event_id(2),
            author_b,
            40002,
            "alpha project planning",
            Some(channel_a),
            200,
        )
        .await;
    backend
        .insert_event(
            community,
            event_id(3),
            author_a,
            9,
            "alpha projectile archive",
            Some(channel_b),
            300,
        )
        .await;
    backend
        .insert_event(
            community,
            event_id(4),
            author_a,
            9,
            "alpha deleted marker",
            None,
            400,
        )
        .await;
    backend.tombstone(community, event_id(4)).await;
    backend
        .insert_event(
            community,
            event_id(5),
            author_a,
            1059,
            "alpha encrypted secret",
            None,
            500,
        )
        .await;
    backend
        .insert_event(
            other_community,
            event_id(6),
            author_a,
            9,
            "alpha tenant secret",
            None,
            600,
        )
        .await;

    let service = backend.service();

    let all = service
        .search(&query(community, "alpha"))
        .await
        .expect("search all contract events");
    assert_eq!(
        ids(&all),
        BTreeSet::from([event_id(1), event_id(2), event_id(3)]),
        "tenant, tombstone, and storage-level kind fences must hold"
    );
    assert!(all.hits.iter().all(|hit| hit.rank > 0.0));

    let phrase = service
        .search(&query(community, "\"quick brown\""))
        .await
        .expect("phrase search");
    assert_eq!(ids(&phrase), BTreeSet::from([event_id(1)]));

    let mut prefix = query(community, "alpha proj");
    prefix.mode = SearchMode::Prefix;
    assert_eq!(
        ids(&service.search(&prefix).await.expect("prefix search")),
        BTreeSet::from([event_id(2), event_id(3)])
    );

    let mut channel_less = query(community, "alpha");
    channel_less.channel_scope = ChannelScope::ChannelLessOnly;
    assert_eq!(
        ids(&service
            .search(&channel_less)
            .await
            .expect("channel-less search")),
        BTreeSet::from([event_id(1)])
    );

    let mut one_channel = query(community, "alpha");
    one_channel.channel_scope = ChannelScope::Channels(vec![channel_a]);
    assert_eq!(
        ids(&service
            .search(&one_channel)
            .await
            .expect("one-channel search")),
        BTreeSet::from([event_id(2)])
    );

    let mut no_channels = query(community, "alpha");
    no_channels.channel_scope = ChannelScope::Channels(Vec::new());
    assert!(service
        .search(&no_channels)
        .await
        .expect("empty-channel search")
        .hits
        .is_empty());

    let mut channel_or_global = query(community, "alpha");
    channel_or_global.channel_scope = ChannelScope::ChannelsOrChannelLess(vec![channel_a]);
    assert_eq!(
        ids(&service
            .search(&channel_or_global)
            .await
            .expect("channel-or-global search")),
        BTreeSet::from([event_id(1), event_id(2)])
    );

    let mut empty_channel_or_global = query(community, "alpha");
    empty_channel_or_global.channel_scope = ChannelScope::ChannelsOrChannelLess(Vec::new());
    assert_eq!(
        ids(&service
            .search(&empty_channel_or_global)
            .await
            .expect("empty-channel-or-global search")),
        BTreeSet::from([event_id(1)])
    );

    let mut kind_filter = query(community, "alpha");
    kind_filter.kinds = Some(vec![40002]);
    assert_eq!(
        ids(&service
            .search(&kind_filter)
            .await
            .expect("kind-filtered search")),
        BTreeSet::from([event_id(2)])
    );

    let mut author_filter = query(community, "alpha");
    author_filter.authors = Some(vec![author_a.to_vec()]);
    assert_eq!(
        ids(&service
            .search(&author_filter)
            .await
            .expect("author-filtered search")),
        BTreeSet::from([event_id(1), event_id(3)])
    );

    let mut time_filter = query(community, "alpha");
    time_filter.since = Some(200);
    time_filter.until = Some(300);
    assert_eq!(
        ids(&service
            .search(&time_filter)
            .await
            .expect("time-filtered search")),
        BTreeSet::from([event_id(2), event_id(3)])
    );

    let mut first_page = query(community, "alpha");
    first_page.per_page = 2;
    let first = service
        .search(&first_page)
        .await
        .expect("first search page");
    let mut second_page = first_page;
    second_page.page = 2;
    let second = service
        .search(&second_page)
        .await
        .expect("second search page");
    assert_eq!(first.hits.len(), 2);
    assert_eq!(second.hits.len(), 1);
    let first_ids = ids(&first);
    let second_ids = ids(&second);
    assert!(first_ids.is_disjoint(&second_ids));
    assert_eq!(
        first_ids
            .union(&second_ids)
            .copied()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([event_id(1), event_id(2), event_id(3)])
    );

    let mut clamped = query(community, "alpha");
    clamped.page = 0;
    clamped.per_page = 0;
    let clamped_result = service.search(&clamped).await.expect("clamped search");
    assert_eq!(clamped_result.page, 1);
    assert_eq!(clamped_result.hits.len(), 3);

    let mut empty = query(community, " \0 ");
    empty.page = u32::MAX;
    assert_eq!(
        service.search(&empty).await.expect("empty search").page,
        1_000
    );
    assert!(service
        .search(&empty)
        .await
        .expect("empty search")
        .hits
        .is_empty());

    backend.teardown().await;
}

#[tokio::test]
async fn sqlite_search_contract() {
    run_search_contract(ContractBackend::sqlite().await).await;
}

#[tokio::test]
async fn sqlite_search_index_changes_at_the_event_transaction_boundary() {
    let temp = tempfile::tempdir().expect("create SQLite search tempdir");
    let options = SqliteConnectOptions::new()
        .filename(temp.path().join("search.sqlite3"))
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("connect to WAL-backed SQLite");
    pool.execute(
        "CREATE TABLE events (
            community_id TEXT NOT NULL,
            id BLOB NOT NULL CHECK (length(id) = 32),
            pubkey BLOB NOT NULL CHECK (length(pubkey) = 32),
            created_at INTEGER NOT NULL,
            kind INTEGER NOT NULL,
            content TEXT NOT NULL,
            channel_id TEXT,
            deleted_at INTEGER,
            PRIMARY KEY (community_id, id)
        ) STRICT;",
    )
    .await
    .expect("create SQLite transaction contract table");
    sqlx::raw_sql(SQLITE_SEARCH_MIGRATION)
        .execute(&pool)
        .await
        .expect("apply SQLite search migration");

    let community = CommunityId::from_uuid(Uuid::new_v4());
    let id = event_id(42);
    sqlx::query(
        "INSERT INTO events (
            community_id, id, pubkey, created_at, kind, content
         ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(community.as_uuid().to_string())
    .bind(id.as_slice())
    .bind(event_id(99).as_slice())
    .bind(1_000_000_i64)
    .bind(9_i32)
    .bind("alpha before commit")
    .execute(&pool)
    .await
    .expect("insert transaction contract event");

    let service = SearchService::new_sqlite(pool.clone());
    let mut transaction = pool.begin().await.expect("begin event update");
    sqlx::query(
        "UPDATE events SET content = ?
         WHERE community_id = ? AND id = ?",
    )
    .bind("omega after commit")
    .bind(community.as_uuid().to_string())
    .bind(id.as_slice())
    .execute(&mut *transaction)
    .await
    .expect("update event and FTS index in one transaction");

    assert_eq!(
        ids(&service
            .search(&query(community, "alpha"))
            .await
            .expect("read old index state during update")),
        BTreeSet::from([id])
    );
    assert!(service
        .search(&query(community, "omega"))
        .await
        .expect("hide uncommitted index state")
        .hits
        .is_empty());

    transaction.commit().await.expect("commit event update");

    assert!(service
        .search(&query(community, "alpha"))
        .await
        .expect("remove old index state after commit")
        .hits
        .is_empty());
    assert_eq!(
        ids(&service
            .search(&query(community, "omega"))
            .await
            .expect("publish new index state after commit")),
        BTreeSet::from([id])
    );

    sqlx::query("DELETE FROM events WHERE community_id = ? AND id = ?")
        .bind(community.as_uuid().to_string())
        .bind(id.as_slice())
        .execute(&pool)
        .await
        .expect("hard-delete transaction contract event");
    assert!(service
        .search(&query(community, "omega"))
        .await
        .expect("remove hard-deleted index state")
        .hits
        .is_empty());

    pool.close().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_search_contract() {
    run_search_contract(ContractBackend::postgres().await).await;
}
