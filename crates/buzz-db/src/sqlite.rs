//! SQLite connection policy and serialized writer boundary.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{DbError, Result};

mod admin;
mod api_tokens;
mod channels;
mod community_auth;
#[cfg(test)]
mod contracts;
#[cfg(test)]
mod contracts_admin;
#[cfg(test)]
mod contracts_channel_window;
#[cfg(test)]
mod contracts_channels;
#[cfg(test)]
mod contracts_dms;
#[cfg(test)]
mod contracts_events;
#[cfg(test)]
mod contracts_feeds;
#[cfg(test)]
mod contracts_identity;
#[cfg(test)]
mod contracts_moderation;
#[cfg(test)]
mod contracts_push_acceptance;
#[cfg(test)]
mod contracts_push_leases;
#[cfg(test)]
mod contracts_push_matcher;
#[cfg(test)]
mod contracts_push_outbox;
#[cfg(test)]
mod contracts_reactions;
#[cfg(test)]
mod contracts_reminders;
#[cfg(test)]
mod contracts_threads;
#[cfg(test)]
mod contracts_workflow_coordination;
#[cfg(test)]
mod contracts_workflows;
mod dms;
mod events;
mod feeds;
mod identity_admin;
mod moderation;
mod product_feedback;
mod push;
mod reactions;
mod threads;
mod users;
mod workflows;

/// SQLite connection and concurrency settings.
#[derive(Debug, Clone)]
pub struct SqliteConfig {
    /// Maximum number of pooled read connections.
    pub max_connections: u32,
    /// Maximum wait for a pooled connection.
    pub acquire_timeout: Duration,
    /// Maximum wait for a locked SQLite database.
    pub busy_timeout: Duration,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            max_connections: 4,
            acquire_timeout: Duration::from_secs(3),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

/// SQLite storage resources shared by backend-specific adapters.
#[derive(Debug)]
pub struct SqliteStore {
    pool: SqlitePool,
    writer: Arc<Mutex<()>>,
}

impl SqliteStore {
    /// Open a fresh-install SQLite database with the required safety pragmas.
    pub async fn connect(path: &Path, config: &SqliteConfig) -> Result<Self> {
        if config.max_connections == 0 {
            return Err(DbError::InvalidData(
                "SQLite max_connections must be greater than zero".to_owned(),
            ));
        }
        if config.acquire_timeout.is_zero() {
            return Err(DbError::InvalidData(
                "SQLite acquire_timeout must be greater than zero".to_owned(),
            ));
        }
        if config.busy_timeout.is_zero() {
            return Err(DbError::InvalidData(
                "SQLite busy_timeout must be greater than zero".to_owned(),
            ));
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(config.busy_timeout);
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .connect_with(options)
            .await?;

        Ok(Self {
            pool,
            writer: Arc::new(Mutex::new(())),
        })
    }

    /// Apply the independent fresh-install SQLite migration stream.
    pub async fn migrate(&self) -> Result<()> {
        crate::migration::run_sqlite_migrations(&self.pool).await
    }

    #[cfg(test)]
    fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Clone the configured pool for a backend adapter sharing this database.
    ///
    /// This is an adapter-construction seam, not a general query escape hatch;
    /// relay handlers continue to use domain operations.
    pub fn adapter_pool(&self) -> SqlitePool {
        self.pool.clone()
    }

    /// Clone the single writer gate for another SQLite backend adapter.
    pub fn adapter_writer_gate(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.writer)
    }

    /// Acquire the process-local gate that serializes SQLite mutations.
    pub async fn acquire_writer(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.writer).lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use sqlx::Row;
    use tempfile::TempDir;
    use tokio::time::timeout;

    use super::*;

    async fn test_store() -> (TempDir, SqliteStore) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = SqliteStore::connect(
            &directory.path().join("buzz.sqlite3"),
            &SqliteConfig::default(),
        )
        .await
        .expect("SQLite fixture should connect");
        (directory, store)
    }

    async fn migrated_store() -> (TempDir, SqliteStore) {
        let (directory, store) = test_store().await;
        store.migrate().await.expect("SQLite migrations");
        (directory, store)
    }

    #[tokio::test]
    async fn connection_policy_is_applied_to_each_pool_connection() {
        let (_directory, store) = test_store().await;
        let mut connections = Vec::new();
        for _ in 0..SqliteConfig::default().max_connections {
            connections.push(store.pool().acquire().await.expect("pool connection"));
        }

        for mut connection in connections {
            let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
                .fetch_one(&mut *connection)
                .await
                .expect("journal mode");
            let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
                .fetch_one(&mut *connection)
                .await
                .expect("foreign keys");
            let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
                .fetch_one(&mut *connection)
                .await
                .expect("synchronous mode");
            let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
                .fetch_one(&mut *connection)
                .await
                .expect("busy timeout");

            assert_eq!(journal_mode, "wal");
            assert_eq!(foreign_keys, 1);
            assert_eq!(synchronous, 1);
            assert_eq!(busy_timeout, 5_000);
        }
    }

    #[tokio::test]
    async fn migrations_are_idempotent_and_survive_reopen() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("buzz.sqlite3");

        let store = SqliteStore::connect(&path, &SqliteConfig::default())
            .await
            .expect("initial SQLite connection");
        store.migrate().await.expect("initial migration");
        store.migrate().await.expect("idempotent migration");
        let applied: i64 =
            sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success = 1")
                .fetch_one(store.pool())
                .await
                .expect("migration count");
        assert_eq!(applied, 15);
        store.pool().close().await;

        let reopened = SqliteStore::connect(&path, &SqliteConfig::default())
            .await
            .expect("reopened SQLite connection");
        reopened.migrate().await.expect("migration after reopen");
        let row = sqlx::query("SELECT version, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(reopened.pool())
            .await
            .expect("persisted migration rows");
        assert_eq!(row.len(), 15);
        assert_eq!(row[0].get::<i64, _>("version"), 1);
        assert_eq!(row[1].get::<i64, _>("version"), 2);
        assert_eq!(row[2].get::<i64, _>("version"), 3);
        assert_eq!(row[3].get::<i64, _>("version"), 4);
        assert_eq!(row[4].get::<i64, _>("version"), 5);
        assert_eq!(row[5].get::<i64, _>("version"), 6);
        assert_eq!(row[6].get::<i64, _>("version"), 7);
        assert_eq!(row[7].get::<i64, _>("version"), 8);
        assert_eq!(row[8].get::<i64, _>("version"), 9);
        assert_eq!(row[9].get::<i64, _>("version"), 10);
        assert_eq!(row[10].get::<i64, _>("version"), 11);
        assert_eq!(row[11].get::<i64, _>("version"), 12);
        assert_eq!(row[12].get::<i64, _>("version"), 13);
        assert_eq!(row[13].get::<i64, _>("version"), 14);
        assert_eq!(row[14].get::<i64, _>("version"), 15);
        assert!(row.iter().all(|row| row.get::<bool, _>("success")));
    }

    #[tokio::test]
    async fn writer_gate_serializes_mutations() {
        let (_directory, store) = test_store().await;
        let first = store.acquire_writer().await;
        let writer = Arc::clone(&store.writer);
        let mut waiting = tokio::spawn(async move { writer.lock_owned().await });

        assert!(
            timeout(Duration::from_millis(50), &mut waiting)
                .await
                .is_err(),
            "a second writer must wait while the first holds the gate"
        );
        drop(first);

        let acquired = timeout(Duration::from_secs(1), waiting)
            .await
            .expect("writer gate must reopen after release")
            .expect("writer task");
        drop(acquired);
    }

    #[tokio::test]
    async fn invalid_connection_settings_fail_before_opening_a_database() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("buzz.sqlite3");

        for config in [
            SqliteConfig {
                max_connections: 0,
                ..SqliteConfig::default()
            },
            SqliteConfig {
                acquire_timeout: Duration::ZERO,
                ..SqliteConfig::default()
            },
            SqliteConfig {
                busy_timeout: Duration::ZERO,
                ..SqliteConfig::default()
            },
        ] {
            let error = SqliteStore::connect(&path, &config)
                .await
                .expect_err("invalid config must fail");
            assert!(matches!(error, DbError::InvalidData(_)));
        }
    }

    #[tokio::test]
    async fn relational_schema_has_tenant_leading_primary_keys() {
        let (_directory, store) = migrated_store().await;
        let expected = BTreeSet::from([
            "api_tokens".to_owned(),
            "archived_identities".to_owned(),
            "audit_log".to_owned(),
            "channel_members".to_owned(),
            "channels".to_owned(),
            "community_bans".to_owned(),
            "communities".to_owned(),
            "event_mentions".to_owned(),
            "events".to_owned(),
            "join_policy_acceptances".to_owned(),
            "moderation_actions".to_owned(),
            "moderation_reports".to_owned(),
            "parameterized_event_watermarks".to_owned(),
            "pubkey_allowlist".to_owned(),
            "product_feedback".to_owned(),
            "push_leases".to_owned(),
            "push_match_queue".to_owned(),
            "push_wake_outbox".to_owned(),
            "relay_members".to_owned(),
            "reactions".to_owned(),
            "thread_metadata".to_owned(),
            "users".to_owned(),
            "workflow_approvals".to_owned(),
            "workflow_runs".to_owned(),
            "workflows".to_owned(),
            "scheduled_workflow_fires".to_owned(),
        ]);
        let actual: BTreeSet<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_schema \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
               AND name NOT LIKE 'events_fts%' AND name <> '_sqlx_migrations'",
        )
        .fetch_all(store.pool())
        .await
        .expect("schema tables")
        .into_iter()
        .collect();
        assert_eq!(actual, expected);
        let search_table: Option<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_schema \
             WHERE type = 'table' AND name = 'events_fts' \
               AND sql LIKE 'CREATE VIRTUAL TABLE%fts5%'",
        )
        .fetch_optional(store.pool())
        .await
        .expect("search table metadata");
        assert_eq!(search_table.as_deref(), Some("events_fts"));

        // product_feedback is the documented deployment-global operator
        // inbox: community_id is provenance, not its identity namespace.
        for table in expected
            .iter()
            .filter(|table| !matches!(table.as_str(), "communities" | "product_feedback"))
        {
            // `table` comes from the fixed test-owned set above, never external input.
            let pragma = format!("PRAGMA table_info({table})");
            let columns = sqlx::query(sqlx::AssertSqlSafe(pragma))
                .fetch_all(store.pool())
                .await
                .expect("table metadata");
            let first_primary_key = columns
                .iter()
                .filter(|row| row.get::<i64, _>("pk") > 0)
                .min_by_key(|row| row.get::<i64, _>("pk"))
                .expect("tenant table primary key");
            assert_eq!(
                first_primary_key.get::<String, _>("name"),
                "community_id",
                "{table} primary key must lead with community_id"
            );
        }
    }

    #[tokio::test]
    async fn event_schema_rejects_cross_tenant_mentions_and_malformed_wire_values() {
        let (_directory, store) = migrated_store().await;
        let community_a = "10000000-0000-4000-8000-000000000001";
        let community_b = "20000000-0000-4000-8000-000000000002";
        for (id, host) in [
            (community_a, "event-a.example.test"),
            (community_b, "event-b.example.test"),
        ] {
            sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, 1)")
                .bind(id)
                .bind(host)
                .execute(store.pool())
                .await
                .expect("community");
        }

        let event_id = vec![0x11; 32];
        sqlx::query(
            "INSERT INTO events \
             (community_id, id, pubkey, created_at, kind, tags, content, sig, received_at) \
             VALUES (?, ?, ?, 1, 1, '[]', '', ?, 1)",
        )
        .bind(community_a)
        .bind(&event_id)
        .bind(vec![0x22; 32])
        .bind(vec![0x33; 64])
        .execute(store.pool())
        .await
        .expect("valid event");

        let cross_tenant = sqlx::query(
            "INSERT INTO event_mentions \
             (community_id, pubkey_hex, event_id, event_created_at) \
             VALUES (?, ?, ?, 1)",
        )
        .bind(community_b)
        .bind("44".repeat(32))
        .bind(&event_id)
        .execute(store.pool())
        .await;
        assert!(cross_tenant.is_err());

        let malformed = sqlx::query(
            "INSERT INTO events \
             (community_id, id, pubkey, created_at, kind, tags, content, sig, received_at) \
             VALUES (?, ?, ?, 1, 1, 'not-json', '', ?, 1)",
        )
        .bind(community_a)
        .bind(vec![0x55; 31])
        .bind(vec![0x66; 32])
        .bind(vec![0x77; 64])
        .execute(store.pool())
        .await;
        assert!(malformed.is_err());
    }

    #[tokio::test]
    async fn community_auth_constraints_preserve_tenant_isolation() {
        let (_directory, store) = migrated_store().await;
        let community_a = "10000000-0000-4000-8000-000000000001";
        let community_b = "20000000-0000-4000-8000-000000000002";
        for (id, host) in [
            (community_a, "a.example.test"),
            (community_b, "b.example.test"),
        ] {
            sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, ?)")
                .bind(id)
                .bind(host)
                .bind(1_i64)
                .execute(store.pool())
                .await
                .expect("community");
        }

        let member = "11".repeat(32);
        for community in [community_a, community_b] {
            sqlx::query(
                "INSERT INTO relay_members \
                 (community_id, pubkey, role, created_at, updated_at) VALUES (?, ?, 'member', ?, ?)",
            )
            .bind(community)
            .bind(&member)
            .bind(2_i64)
            .bind(2_i64)
            .execute(store.pool())
            .await
            .expect("same member identity is valid in distinct communities");
        }
        assert!(
            sqlx::query(
                "INSERT INTO relay_members \
                 (community_id, pubkey, role, created_at, updated_at) VALUES (?, ?, 'member', ?, ?)",
            )
            .bind(community_a)
            .bind(&member)
            .bind(3_i64)
            .bind(3_i64)
            .execute(store.pool())
            .await
            .is_err(),
            "membership must be unique within one community"
        );

        let owner_a = vec![0x21; 32];
        let owner_b = vec![0x22; 32];
        for (community, owner) in [(community_a, &owner_a), (community_b, &owner_b)] {
            sqlx::query(
                "INSERT INTO users \
                 (community_id, pubkey, created_at, updated_at) VALUES (?, ?, ?, ?)",
            )
            .bind(community)
            .bind(owner)
            .bind(4_i64)
            .bind(4_i64)
            .execute(store.pool())
            .await
            .expect("user");
        }

        let token_hash = vec![0x31; 32];
        for (community, id, owner) in [
            (
                community_a,
                "30000000-0000-4000-8000-000000000003",
                &owner_a,
            ),
            (
                community_b,
                "40000000-0000-4000-8000-000000000004",
                &owner_b,
            ),
        ] {
            sqlx::query(
                "INSERT INTO api_tokens \
                 (community_id, id, token_hash, owner_pubkey, name, scopes, created_at) \
                 VALUES (?, ?, ?, ?, 'test', '[]', ?)",
            )
            .bind(community)
            .bind(id)
            .bind(&token_hash)
            .bind(owner)
            .bind(5_i64)
            .execute(store.pool())
            .await
            .expect("same token hash is valid in distinct communities");
        }

        assert!(
            sqlx::query(
                "INSERT INTO api_tokens \
                 (community_id, id, token_hash, owner_pubkey, name, scopes, created_at) \
                 VALUES (?, ?, ?, ?, 'cross-tenant', '[]', ?)",
            )
            .bind(community_a)
            .bind("50000000-0000-4000-8000-000000000005")
            .bind(vec![0x32; 32])
            .bind(&owner_b)
            .bind(6_i64)
            .execute(store.pool())
            .await
            .is_err(),
            "a token owner must exist in the same community"
        );
    }

    #[tokio::test]
    async fn community_auth_checks_reject_noncanonical_values() {
        let (_directory, store) = migrated_store().await;
        let community = "60000000-0000-4000-8000-000000000006";
        sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, ?)")
            .bind(community)
            .bind("canonical.example.test")
            .bind(1_i64)
            .execute(store.pool())
            .await
            .expect("community");

        assert!(
            sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, ?)")
                .bind("NOT-A-UUID")
                .bind("invalid.example.test")
                .bind(1_i64)
                .execute(store.pool())
                .await
                .is_err()
        );
        assert!(
            sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, ?)")
                .bind("70000000-0000-4000-8000-000000000007")
                .bind("CANONICAL.EXAMPLE.TEST")
                .bind(1_i64)
                .execute(store.pool())
                .await
                .is_err(),
            "normalized hosts are unique case-insensitively"
        );
        assert!(
            sqlx::query(
                "INSERT INTO users \
                 (community_id, pubkey, capabilities, created_at, updated_at) \
                 VALUES (?, ?, 'not-json', ?, ?)",
            )
            .bind(community)
            .bind(vec![0x41; 32])
            .bind(2_i64)
            .bind(2_i64)
            .execute(store.pool())
            .await
            .is_err(),
            "JSON text must be valid"
        );
        assert!(
            sqlx::query(
                "INSERT INTO relay_members \
                 (community_id, pubkey, role, created_at, updated_at) \
                 VALUES (?, ?, 'guest', ?, ?)",
            )
            .bind(community)
            .bind("zz".repeat(32))
            .bind(2_i64)
            .bind(2_i64)
            .execute(store.pool())
            .await
            .is_err(),
            "member keys and roles must use the canonical supported domain"
        );
    }

    #[tokio::test]
    async fn policy_acceptance_is_deleted_with_membership() {
        let (_directory, store) = migrated_store().await;
        let community = "80000000-0000-4000-8000-000000000008";
        let member = "51".repeat(32);
        let policy = "61".repeat(32);
        sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, ?)")
            .bind(community)
            .bind("policy.example.test")
            .bind(1_i64)
            .execute(store.pool())
            .await
            .expect("community");
        sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, created_at, updated_at) VALUES (?, ?, 'member', ?, ?)",
        )
        .bind(community)
        .bind(&member)
        .bind(2_i64)
        .bind(2_i64)
        .execute(store.pool())
        .await
        .expect("member");
        sqlx::query(
            "INSERT INTO join_policy_acceptances \
             (community_id, pubkey, policy_version, accepted_at) VALUES (?, ?, ?, ?)",
        )
        .bind(community)
        .bind(&member)
        .bind(&policy)
        .bind(3_i64)
        .execute(store.pool())
        .await
        .expect("policy acceptance");

        sqlx::query("DELETE FROM relay_members WHERE community_id = ? AND pubkey = ?")
            .bind(community)
            .bind(&member)
            .execute(store.pool())
            .await
            .expect("remove member");
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM join_policy_acceptances")
            .fetch_one(store.pool())
            .await
            .expect("policy count");
        assert_eq!(remaining, 0);
    }
}
