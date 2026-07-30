//! SQLite NIP-PL effective lease and matcher persistence.

use chrono::Utc;
use sqlx::Row as _;

use buzz_core::CommunityId;

use super::SqliteStore;
use crate::push::{
    ActiveLease, LeaseVersion, MatchLease, ReplaceLeaseOutcome, PUSH_GATE_BACKFILL_SECS,
};
use crate::Result;

async fn backfill_push_match_jobs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    community: CommunityId,
    now_micros: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO push_match_queue ( \
            community_id, event_id, next_attempt_at, created_at \
         ) SELECT community_id, id, ?, ? FROM events \
           WHERE community_id = ? \
             AND kind IN (7, 9, 1059, 40007, 46010) \
             AND deleted_at IS NULL AND received_at > ? \
         ON CONFLICT DO NOTHING",
    )
    .bind(now_micros)
    .bind(now_micros)
    .bind(community.as_uuid().to_string())
    .bind(now_micros - PUSH_GATE_BACKFILL_SECS * 1_000_000)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

impl SqliteStore {
    /// Create or rotate an active lease when event and generation ordering win.
    #[allow(clippy::too_many_arguments)]
    pub async fn replace_active_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        version: LeaseVersion<'_>,
        active: ActiveLease<'_>,
    ) -> Result<ReplaceLeaseOutcome> {
        self.replace_lease(community, author, installation_id, version, Some(active))
            .await
    }

    /// Revoke one installation with a higher-generation inactive replacement.
    pub async fn revoke_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        version: LeaseVersion<'_>,
    ) -> Result<ReplaceLeaseOutcome> {
        self.replace_lease(community, author, installation_id, version, None)
            .await
    }

    async fn replace_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        version: LeaseVersion<'_>,
        active: Option<ActiveLease<'_>>,
    ) -> Result<ReplaceLeaseOutcome> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let now_micros = Utc::now().timestamp_micros();
        let (is_active, app_profile, endpoint_hash, endpoint_grant, max_class, subscriptions) =
            match active {
                Some(active) => (
                    1_i64,
                    Some(active.app_profile),
                    Some(active.endpoint_hash),
                    Some(active.endpoint_grant),
                    Some(active.max_class),
                    Some(serde_json::to_string(active.subscriptions)?),
                ),
                None => (0, None, None, None, None, None),
            };

        let accepted = sqlx::query(
            "INSERT INTO push_leases ( \
                community_id, author, installation_id, source_event_id, \
                source_created_at, generation, active, app_profile, \
                endpoint_hash, endpoint_grant, max_class, subscriptions, \
                expires_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, author, installation_id) DO UPDATE SET \
                source_event_id = excluded.source_event_id, \
                source_created_at = excluded.source_created_at, \
                generation = excluded.generation, active = excluded.active, \
                endpoint_enabled = 1, app_profile = excluded.app_profile, \
                endpoint_hash = excluded.endpoint_hash, \
                endpoint_grant = excluded.endpoint_grant, \
                max_class = excluded.max_class, \
                subscriptions = excluded.subscriptions, \
                expires_at = excluded.expires_at, updated_at = excluded.updated_at \
             WHERE ( \
                    excluded.source_created_at > push_leases.source_created_at \
                    OR ( \
                        excluded.source_created_at = push_leases.source_created_at \
                        AND excluded.source_event_id < push_leases.source_event_id \
                    ) \
                   ) \
               AND excluded.generation > push_leases.generation \
             RETURNING generation",
        )
        .bind(community.as_uuid().to_string())
        .bind(author)
        .bind(installation_id)
        .bind(version.source_event_id)
        .bind(version.source_created_at)
        .bind(version.generation)
        .bind(is_active)
        .bind(app_profile)
        .bind(endpoint_hash)
        .bind(endpoint_grant)
        .bind(max_class)
        .bind(subscriptions)
        .bind(version.expires_at)
        .bind(now_micros)
        .fetch_optional(&mut *transaction)
        .await?;

        if accepted.is_some() {
            if is_active == 1 {
                backfill_push_match_jobs(&mut transaction, community, now_micros).await?;
            }
            transaction.commit().await?;
            return Ok(ReplaceLeaseOutcome::Accepted);
        }

        let current = sqlx::query(
            "SELECT source_event_id, source_created_at, generation \
             FROM push_leases \
             WHERE community_id = ? AND author = ? AND installation_id = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(author)
        .bind(installation_id)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        let current_created_at: i64 = current.try_get("source_created_at")?;
        let current_event_id: Vec<u8> = current.try_get("source_event_id")?;
        let wins_event_order = version.source_created_at > current_created_at
            || (version.source_created_at == current_created_at
                && version.source_event_id < current_event_id.as_slice());
        if wins_event_order {
            Ok(ReplaceLeaseOutcome::StaleGeneration)
        } else {
            Ok(ReplaceLeaseOutcome::StaleEvent)
        }
    }

    /// Load active endpoint-enabled, unexpired leases for one community.
    pub async fn active_push_match_leases(
        &self,
        community: CommunityId,
    ) -> Result<Vec<MatchLease>> {
        let rows = sqlx::query(
            "SELECT author, installation_id, generation, subscriptions, expires_at \
             FROM push_leases \
             WHERE community_id = ? AND active = 1 AND endpoint_enabled = 1 \
               AND expires_at > ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(Utc::now().timestamp())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MatchLease {
                    author: row.try_get("author")?,
                    installation_id: row.try_get("installation_id")?,
                    generation: row.try_get("generation")?,
                    subscriptions: serde_json::from_str(
                        row.try_get::<String, _>("subscriptions")?.as_str(),
                    )?,
                    expires_at: row.try_get("expires_at")?,
                })
            })
            .collect()
    }
}
