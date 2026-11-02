//! SQLite reaction lifecycle and atomic kind-7 event persistence.

use chrono::{DateTime, Utc};
use nostr::Event;
use sqlx::Row as _;
use uuid::Uuid;

use buzz_core::CommunityId;

use super::{events, threads, SqliteStore};
use crate::event::{ReactionEventInsertOutcome, ThreadMetadataParams};
use crate::reaction::{
    ActiveReactionRecord, BulkReactionEntry, ReactionGroup, ReactionSummary, ReactionUser,
};
use crate::Result;

async fn add_reaction_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    community: CommunityId,
    event_id: &[u8],
    event_created_at: DateTime<Utc>,
    pubkey: &[u8],
    emoji: &str,
    reaction_event_id: Option<&[u8]>,
) -> Result<bool> {
    let result = sqlx::query(
        "INSERT INTO reactions \
         (community_id, event_created_at, event_id, pubkey, emoji, \
          created_at, reaction_event_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (community_id, event_created_at, event_id, pubkey, emoji) \
         DO UPDATE SET created_at = excluded.created_at, removed_at = NULL, \
           reaction_event_id = COALESCE( \
               excluded.reaction_event_id, reactions.reaction_event_id \
           ) \
         WHERE reactions.removed_at IS NOT NULL",
    )
    .bind(community.as_uuid().to_string())
    .bind(event_created_at.timestamp_micros())
    .bind(event_id)
    .bind(pubkey)
    .bind(emoji)
    .bind(Utc::now().timestamp_micros())
    .bind(reaction_event_id)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() != 0)
}

impl SqliteStore {
    /// Add or reactivate one tenant-scoped reaction without TOCTOU races.
    pub async fn add_reaction(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
        reaction_event_id: Option<&[u8]>,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let inserted = add_reaction_transaction(
            &mut transaction,
            community,
            event_id,
            event_created_at,
            pubkey,
            emoji,
            reaction_event_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    /// Soft-delete an active reaction tuple.
    pub async fn remove_reaction(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE reactions SET removed_at = ? \
             WHERE community_id = ? AND event_created_at = ? AND event_id = ? \
               AND pubkey = ? AND emoji = ? AND removed_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(event_created_at.timestamp_micros())
        .bind(event_id)
        .bind(pubkey)
        .bind(emoji)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Soft-delete an active reaction by its signed source event ID.
    pub async fn remove_reaction_by_source_event_id(
        &self,
        community: CommunityId,
        reaction_event_id: &[u8],
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE reactions SET removed_at = ? \
             WHERE community_id = ? AND reaction_event_id = ? \
               AND removed_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(reaction_event_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Look up the active reaction for one target, actor, and emoji.
    pub async fn get_active_reaction_record(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
    ) -> Result<Option<ActiveReactionRecord>> {
        sqlx::query(
            "SELECT reaction_event_id FROM reactions \
             WHERE community_id = ? AND event_id = ? AND event_created_at = ? \
               AND pubkey = ? AND emoji = ? AND removed_at IS NULL LIMIT 1",
        )
        .bind(community.as_uuid().to_string())
        .bind(event_id)
        .bind(event_created_at.timestamp_micros())
        .bind(pubkey)
        .bind(emoji)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            Ok(ActiveReactionRecord {
                reaction_event_id: row.try_get("reaction_event_id")?,
            })
        })
        .transpose()
    }

    /// Attach a signed source event ID to an active reaction.
    pub async fn set_reaction_event_id(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
        reaction_event_id: &[u8],
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE reactions SET reaction_event_id = ? \
             WHERE community_id = ? AND event_created_at = ? AND event_id = ? \
               AND pubkey = ? AND emoji = ? AND removed_at IS NULL",
        )
        .bind(reaction_event_id)
        .bind(community.as_uuid().to_string())
        .bind(event_created_at.timestamp_micros())
        .bind(event_id)
        .bind(pubkey)
        .bind(emoji)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Return active reactions grouped by emoji.
    pub async fn get_reactions(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        limit: u32,
        _cursor: Option<&str>,
    ) -> Result<Vec<ReactionGroup>> {
        let rows = sqlx::query(
            "SELECT r.emoji, r.pubkey, r.reaction_event_id \
             FROM reactions r INNER JOIN ( \
               SELECT DISTINCT emoji FROM reactions \
               WHERE community_id = ? AND event_id = ? \
                 AND event_created_at = ? AND removed_at IS NULL \
               ORDER BY emoji LIMIT ? \
             ) grouped ON grouped.emoji = r.emoji \
             WHERE r.community_id = ? AND r.event_id = ? \
               AND r.event_created_at = ? AND r.removed_at IS NULL \
             ORDER BY r.emoji, r.created_at",
        )
        .bind(community.as_uuid().to_string())
        .bind(event_id)
        .bind(event_created_at.timestamp_micros())
        .bind(i64::from(limit))
        .bind(community.as_uuid().to_string())
        .bind(event_id)
        .bind(event_created_at.timestamp_micros())
        .fetch_all(&self.pool)
        .await?;
        let mut groups = Vec::new();
        let mut current_emoji = None;
        let mut current_users = Vec::new();
        for row in rows {
            let emoji: String = row.try_get("emoji")?;
            if current_emoji.as_ref() != Some(&emoji) {
                if let Some(previous) = current_emoji.take() {
                    groups.push(ReactionGroup {
                        emoji: previous,
                        count: current_users.len() as i64,
                        users: std::mem::take(&mut current_users),
                    });
                }
                current_emoji = Some(emoji);
            }
            current_users.push(ReactionUser {
                pubkey: row.try_get("pubkey")?,
                display_name: None,
                reaction_event_id: row.try_get("reaction_event_id")?,
            });
        }
        if let Some(emoji) = current_emoji {
            groups.push(ReactionGroup {
                emoji,
                count: current_users.len() as i64,
                users: current_users,
            });
        }
        Ok(groups)
    }

    /// Batch-fetch active emoji counts for target event coordinates.
    pub async fn get_reactions_bulk(
        &self,
        community: CommunityId,
        event_ids: &[(&[u8], DateTime<Utc>)],
    ) -> Result<Vec<BulkReactionEntry>> {
        let mut entries = Vec::new();
        for (event_id, event_created_at) in event_ids {
            let rows = sqlx::query(
                "SELECT emoji, COUNT(*) AS count FROM reactions \
                 WHERE community_id = ? AND event_id = ? \
                   AND event_created_at = ? AND removed_at IS NULL \
                 GROUP BY emoji ORDER BY emoji",
            )
            .bind(community.as_uuid().to_string())
            .bind(*event_id)
            .bind(event_created_at.timestamp_micros())
            .fetch_all(&self.pool)
            .await?;
            if rows.is_empty() {
                continue;
            }
            let reactions = rows
                .into_iter()
                .map(|row| {
                    Ok(ReactionSummary {
                        emoji: row.try_get("emoji")?,
                        count: row.try_get("count")?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            entries.push(BulkReactionEntry {
                event_id: event_id.to_vec(),
                event_created_at: *event_created_at,
                reactions,
            });
        }
        Ok(entries)
    }

    /// Atomically persist a kind-7 event, reaction row, and optional thread metadata.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_reaction_event_with_thread_metadata(
        &self,
        community: CommunityId,
        reaction_event: &Event,
        channel_id: Option<Uuid>,
        thread_meta: Option<ThreadMetadataParams<'_>>,
        target_event_id: &[u8],
        actor_pubkey: &[u8],
        emoji: &str,
    ) -> Result<ReactionEventInsertOutcome> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let target_created_at: Option<i64> = sqlx::query_scalar(
            "SELECT created_at FROM events \
             WHERE community_id = ? AND id = ? AND deleted_at IS NULL \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(community.as_uuid().to_string())
        .bind(target_event_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(target_created_at) = target_created_at else {
            transaction.rollback().await?;
            return Ok(ReactionEventInsertOutcome::TargetMissing);
        };
        let target_created_at = DateTime::from_timestamp_micros(target_created_at)
            .ok_or(crate::DbError::InvalidTimestamp(target_created_at))?;
        if !add_reaction_transaction(
            &mut transaction,
            community,
            target_event_id,
            target_created_at,
            actor_pubkey,
            emoji,
            Some(reaction_event.id.as_bytes()),
        )
        .await?
        {
            transaction.rollback().await?;
            return Ok(ReactionEventInsertOutcome::Duplicate);
        }

        let (stored_event, was_inserted) = events::insert_event_transaction(
            &mut transaction,
            community,
            reaction_event,
            channel_id,
        )
        .await?;
        if was_inserted {
            if let Some(metadata) = thread_meta {
                threads::insert_thread_metadata_transaction(
                    &mut transaction,
                    community,
                    metadata.event_id,
                    metadata.event_created_at,
                    metadata.channel_id,
                    metadata.parent_event_id,
                    metadata.parent_event_created_at,
                    metadata.root_event_id,
                    metadata.root_event_created_at,
                    metadata.depth,
                    metadata.broadcast,
                )
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(ReactionEventInsertOutcome::Inserted {
            stored_event: Box::new(stored_event),
            was_inserted,
        })
    }
}
