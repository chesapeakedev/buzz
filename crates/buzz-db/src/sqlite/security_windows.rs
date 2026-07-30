//! Durable SQLite replay claims and fixed-window counters.

use chrono::Utc;
use sqlx::Row as _;

use super::SqliteStore;
use crate::{DbError, Result};

const SECURITY_CLEANUP_BATCH: i64 = 1_000;

/// State returned after atomically incrementing one fixed rate window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityRateWindow {
    /// Counter value after the increment.
    pub current: u64,
    /// Whole seconds until the current window expires.
    pub reset_in_secs: u64,
}

impl SqliteStore {
    /// Atomically claim an event ID in one explicit security scope.
    ///
    /// An expired row may be reclaimed in the same statement. Returns `true`
    /// only when this call owns the active claim.
    pub async fn try_claim_security_replay(
        &self,
        scope: &str,
        event_id: &[u8],
        expires_at_micros: i64,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let now = Utc::now().timestamp_micros();
        if expires_at_micros <= now {
            return Err(DbError::InvalidData(
                "security replay expiry must be in the future".to_owned(),
            ));
        }
        let row = sqlx::query(
            "INSERT INTO security_replay_claims ( \
                scope, event_id, expires_at, created_at \
             ) VALUES (?, ?, ?, ?) \
             ON CONFLICT (scope, event_id) DO UPDATE SET \
                expires_at = excluded.expires_at, created_at = excluded.created_at \
             WHERE security_replay_claims.expires_at <= ? \
             RETURNING event_id",
        )
        .bind(scope)
        .bind(event_id)
        .bind(expires_at_micros)
        .bind(now)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    /// Atomically increment one durable fixed-window counter.
    pub async fn increment_security_rate_window(
        &self,
        window_key: &str,
        window_secs: u64,
    ) -> Result<SecurityRateWindow> {
        let window_micros = i64::try_from(window_secs)
            .ok()
            .and_then(|seconds| seconds.checked_mul(1_000_000))
            .filter(|window| *window > 0)
            .ok_or_else(|| {
                DbError::InvalidData("security rate window must be positive and bounded".to_owned())
            })?;
        let _writer = self.acquire_writer().await;
        let now = Utc::now().timestamp_micros();
        let expires_at = now
            .checked_add(window_micros)
            .ok_or_else(|| DbError::InvalidData("security rate window overflow".to_owned()))?;
        let row = sqlx::query(
            "INSERT INTO security_rate_windows ( \
                window_key, count, expires_at, updated_at \
             ) VALUES (?, 1, ?, ?) \
             ON CONFLICT (window_key) DO UPDATE SET \
                count = CASE \
                    WHEN security_rate_windows.expires_at <= ? THEN 1 \
                    ELSE security_rate_windows.count + 1 \
                END, \
                expires_at = CASE \
                    WHEN security_rate_windows.expires_at <= ? \
                    THEN excluded.expires_at \
                    ELSE security_rate_windows.expires_at \
                END, \
                updated_at = excluded.updated_at \
             RETURNING count, expires_at",
        )
        .bind(window_key)
        .bind(expires_at)
        .bind(now)
        .bind(now)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        let current = u64::try_from(row.try_get::<i64, _>("count")?)
            .map_err(|_| DbError::InvalidData("negative security rate count".to_owned()))?;
        let stored_expiry: i64 = row.try_get("expires_at")?;
        let remaining_micros = stored_expiry.saturating_sub(now);
        let reset_in_secs = u64::try_from(remaining_micros)
            .unwrap_or_default()
            .saturating_add(999_999)
            / 1_000_000;
        Ok(SecurityRateWindow {
            current,
            reset_in_secs,
        })
    }

    /// Delete bounded batches of expired replay claims and rate windows.
    pub async fn cleanup_security_windows(&self) -> Result<u64> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let now = Utc::now().timestamp_micros();
        let replay = sqlx::query(
            "DELETE FROM security_replay_claims WHERE rowid IN ( \
                SELECT rowid FROM security_replay_claims \
                WHERE expires_at <= ? ORDER BY expires_at LIMIT ? \
             )",
        )
        .bind(now)
        .bind(SECURITY_CLEANUP_BATCH)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let rates = sqlx::query(
            "DELETE FROM security_rate_windows WHERE rowid IN ( \
                SELECT rowid FROM security_rate_windows \
                WHERE expires_at <= ? ORDER BY expires_at LIMIT ? \
             )",
        )
        .bind(now)
        .bind(SECURITY_CLEANUP_BATCH)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(replay + rates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::SqliteConfig;

    #[tokio::test]
    async fn security_windows_are_atomic_scoped_and_restart_durable() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("buzz.sqlite3");
        let store = SqliteStore::connect(&path, &SqliteConfig::default())
            .await
            .expect("SQLite connection");
        store.migrate().await.expect("SQLite migrations");
        let expiry = Utc::now().timestamp_micros() + 3_600_000_000;
        let event = [0x61; 32];
        assert!(store
            .try_claim_security_replay("scope-a", &event, expiry)
            .await
            .expect("first replay claim"));
        assert!(!store
            .try_claim_security_replay("scope-a", &event, expiry)
            .await
            .expect("same-scope replay"));
        assert!(store
            .try_claim_security_replay("scope-b", &event, expiry)
            .await
            .expect("other scope"));

        let race_event = [0x62; 32];
        let race_a = store.try_claim_security_replay("race", &race_event, expiry);
        let race_b = store.try_claim_security_replay("race", &race_event, expiry);
        let (race_a, race_b) = tokio::join!(race_a, race_b);
        assert_eq!(
            [race_a.expect("race A"), race_b.expect("race B")]
                .into_iter()
                .filter(|claimed| *claimed)
                .count(),
            1
        );

        let first = store
            .increment_security_rate_window("scope-a:rate", 60)
            .await
            .expect("first rate increment");
        let second = store
            .increment_security_rate_window("scope-a:rate", 60)
            .await
            .expect("second rate increment");
        assert_eq!((first.current, second.current), (1, 2));
        assert!((1..=60).contains(&second.reset_in_secs));
        assert_eq!(
            store
                .increment_security_rate_window("scope-b:rate", 60)
                .await
                .expect("independent rate key")
                .current,
            1
        );

        store.pool().close().await;
        let reopened = SqliteStore::connect(&path, &SqliteConfig::default())
            .await
            .expect("reopened SQLite connection");
        reopened.migrate().await.expect("reopened migrations");
        assert!(!reopened
            .try_claim_security_replay("scope-a", &event, expiry)
            .await
            .expect("replay survives restart"));
        assert_eq!(
            reopened
                .increment_security_rate_window("scope-a:rate", 60)
                .await
                .expect("rate survives restart")
                .current,
            3
        );

        let expired = Utc::now().timestamp_micros() - 1;
        sqlx::query(
            "UPDATE security_replay_claims SET expires_at = ? \
             WHERE scope = 'scope-a' AND event_id = ?",
        )
        .bind(expired)
        .bind(event.as_slice())
        .execute(reopened.pool())
        .await
        .expect("expire replay");
        assert!(reopened
            .try_claim_security_replay("scope-a", &event, expiry)
            .await
            .expect("reclaim expired replay"));
        sqlx::query(
            "UPDATE security_rate_windows SET expires_at = ? \
             WHERE window_key = 'scope-a:rate'",
        )
        .bind(expired)
        .execute(reopened.pool())
        .await
        .expect("expire rate");
        assert_eq!(
            reopened
                .increment_security_rate_window("scope-a:rate", 60)
                .await
                .expect("reset expired rate")
                .current,
            1
        );

        sqlx::query("UPDATE security_replay_claims SET expires_at = ?")
            .bind(expired)
            .execute(reopened.pool())
            .await
            .expect("expire replay rows");
        sqlx::query("UPDATE security_rate_windows SET expires_at = ?")
            .bind(expired)
            .execute(reopened.pool())
            .await
            .expect("expire rate rows");
        assert!(
            reopened
                .cleanup_security_windows()
                .await
                .expect("bounded cleanup")
                >= 4
        );
    }
}
