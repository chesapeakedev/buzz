//! SQLite connection policy and serialized writer boundary.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{DbError, Result};

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

    /// Acquire the process-local gate that serializes SQLite mutations.
    pub async fn acquire_writer(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.writer).lock_owned().await
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(applied, 1);
        store.pool().close().await;

        let reopened = SqliteStore::connect(&path, &SqliteConfig::default())
            .await
            .expect("reopened SQLite connection");
        reopened.migrate().await.expect("migration after reopen");
        let row = sqlx::query("SELECT version, success FROM _sqlx_migrations ORDER BY version")
            .fetch_one(reopened.pool())
            .await
            .expect("persisted migration row");
        assert_eq!(row.get::<i64, _>("version"), 1);
        assert!(row.get::<bool, _>("success"));
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
}
