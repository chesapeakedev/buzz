//! SQLite direct-message channel operations.

use chrono::{DateTime, Utc};
use sqlx::Row as _;
use uuid::Uuid;

use super::SqliteStore;
use crate::channel::ChannelRecord;
use crate::dm::{compute_participant_hash, DmParticipant, DmRecord};
use crate::{CommunityId, DbError, Result};

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or(DbError::InvalidTimestamp(value))
}

fn validate_participants(participants: &[&[u8]]) -> Result<()> {
    if participants.len() < 2 {
        return Err(DbError::InvalidData(
            "DM requires at least 2 participants".to_owned(),
        ));
    }
    if participants.len() > 9 {
        return Err(DbError::InvalidData(
            "DM supports at most 9 participants".to_owned(),
        ));
    }
    for pubkey in participants {
        if pubkey.len() != 32 {
            return Err(DbError::InvalidData(format!(
                "pubkey must be 32 bytes, got {}",
                pubkey.len()
            )));
        }
    }
    Ok(())
}

impl SqliteStore {
    /// Find a live DM by its tenant-local participant hash.
    pub async fn find_dm_by_participants(
        &self,
        community: CommunityId,
        participant_hash: &[u8],
    ) -> Result<Option<ChannelRecord>> {
        let channel_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM channels \
             WHERE community_id = ? AND participant_hash = ? \
               AND channel_type = 'dm' AND deleted_at IS NULL LIMIT 1",
        )
        .bind(community.as_uuid().to_string())
        .bind(participant_hash)
        .fetch_optional(&self.pool)
        .await?;
        match channel_id {
            Some(channel_id) => {
                let channel_id = Uuid::parse_str(&channel_id)
                    .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))?;
                self.get_channel(community, channel_id).await.map(Some)
            }
            None => Ok(None),
        }
    }

    /// Create or return the DM for an exact participant set.
    pub async fn create_dm(
        &self,
        community: CommunityId,
        participants: &[&[u8]],
        created_by: &[u8],
    ) -> Result<ChannelRecord> {
        validate_participants(participants)?;
        self.open_or_create_dm(community, participants, created_by, false)
            .await
            .map(|(channel, _)| channel)
    }

    /// Open or return a DM, adding the creator to the participant set.
    pub async fn open_dm(
        &self,
        community: CommunityId,
        pubkeys: &[&[u8]],
        created_by: &[u8],
    ) -> Result<(ChannelRecord, bool)> {
        let mut participants = pubkeys.to_vec();
        if !participants.contains(&created_by) {
            participants.push(created_by);
        }
        if participants.len() > 9 {
            return Err(DbError::InvalidData(
                "DM supports at most 9 participants".to_owned(),
            ));
        }
        validate_participants(&participants)?;
        self.open_or_create_dm(community, &participants, created_by, true)
            .await
    }

    async fn open_or_create_dm(
        &self,
        community: CommunityId,
        participants: &[&[u8]],
        created_by: &[u8],
        unhide_creator: bool,
    ) -> Result<(ChannelRecord, bool)> {
        let participant_hash = compute_participant_hash(participants);
        let community_id = community.as_uuid().to_string();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT id FROM channels \
             WHERE community_id = ? AND participant_hash = ? \
               AND channel_type = 'dm' AND deleted_at IS NULL LIMIT 1",
        )
        .bind(&community_id)
        .bind(participant_hash.as_slice())
        .fetch_optional(&mut *transaction)
        .await?;
        let (channel_id, created) = if let Some(channel_id) = existing {
            if unhide_creator {
                sqlx::query(
                    "UPDATE channel_members SET hidden_at = NULL \
                     WHERE community_id = ? AND channel_id = ? AND pubkey = ? \
                       AND removed_at IS NULL",
                )
                .bind(&community_id)
                .bind(&channel_id)
                .bind(created_by)
                .execute(&mut *transaction)
                .await?;
            }
            (channel_id, false)
        } else {
            let channel_id = Uuid::new_v4().to_string();
            let now = Utc::now().timestamp_micros();
            let name = if participants.len() == 2 {
                "DM".to_owned()
            } else {
                format!("Group DM ({})", participants.len())
            };
            sqlx::query(
                "INSERT INTO channels \
                 (community_id, id, name, channel_type, visibility, created_by, \
                  created_at, updated_at, participant_hash) \
                 VALUES (?, ?, ?, 'dm', 'private', ?, ?, ?, ?)",
            )
            .bind(&community_id)
            .bind(&channel_id)
            .bind(name)
            .bind(created_by)
            .bind(now)
            .bind(now)
            .bind(participant_hash.as_slice())
            .execute(&mut *transaction)
            .await?;
            for pubkey in participants {
                sqlx::query(
                    "INSERT INTO channel_members \
                     (community_id, channel_id, pubkey, role, joined_at, invited_by) \
                     VALUES (?, ?, ?, 'member', ?, ?) \
                     ON CONFLICT (community_id, channel_id, pubkey) DO UPDATE SET \
                       removed_at = NULL, removed_by = NULL, role = excluded.role",
                )
                .bind(&community_id)
                .bind(&channel_id)
                .bind(*pubkey)
                .bind(now)
                .bind(created_by)
                .execute(&mut *transaction)
                .await?;
            }
            (channel_id, true)
        };
        transaction.commit().await?;
        let channel_id = Uuid::parse_str(&channel_id)
            .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))?;
        Ok((self.get_channel(community, channel_id).await?, created))
    }

    /// List visible DMs for one active participant.
    pub async fn list_dms_for_user(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        limit: u32,
        cursor: Option<Uuid>,
    ) -> Result<Vec<DmRecord>> {
        let community_id = community.as_uuid().to_string();
        let cursor_timestamp: Option<i64> = match cursor {
            Some(cursor) => {
                sqlx::query_scalar(
                    "SELECT updated_at FROM channels WHERE community_id = ? AND id = ?",
                )
                .bind(&community_id)
                .bind(cursor.to_string())
                .fetch_optional(&self.pool)
                .await?
            }
            None => None,
        };
        let rows = sqlx::query(
            "SELECT c.id, c.created_at, c.updated_at \
             FROM channels c \
             JOIN channel_members cm \
               ON c.community_id = cm.community_id AND c.id = cm.channel_id \
              AND cm.pubkey = ? AND cm.removed_at IS NULL AND cm.hidden_at IS NULL \
             WHERE c.community_id = ? AND c.channel_type = 'dm' \
               AND c.deleted_at IS NULL AND (? IS NULL OR c.updated_at < ?) \
             ORDER BY c.updated_at DESC LIMIT ?",
        )
        .bind(pubkey)
        .bind(&community_id)
        .bind(cursor_timestamp)
        .bind(cursor_timestamp)
        .bind(i64::from(limit.min(200)))
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let channel_id: String = row.try_get("id")?;
            let participant_rows = sqlx::query(
                "SELECT cm.pubkey, cm.role, u.display_name \
                 FROM channel_members cm \
                 LEFT JOIN users u \
                   ON u.community_id = cm.community_id AND u.pubkey = cm.pubkey \
                 WHERE cm.community_id = ? AND cm.channel_id = ? \
                   AND cm.removed_at IS NULL \
                 ORDER BY cm.joined_at, cm.pubkey",
            )
            .bind(&community_id)
            .bind(&channel_id)
            .fetch_all(&self.pool)
            .await?;
            let participants = participant_rows
                .into_iter()
                .map(|participant| {
                    Ok(DmParticipant {
                        pubkey: participant.try_get("pubkey")?,
                        display_name: participant.try_get("display_name")?,
                        role: participant.try_get("role")?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let created_at: i64 = row.try_get("created_at")?;
            let updated_at: i64 = row.try_get("updated_at")?;
            records.push(DmRecord {
                channel_id: Uuid::parse_str(&channel_id)
                    .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))?,
                participants,
                last_message_at: Some(parse_timestamp(updated_at)?),
                created_at: parse_timestamp(created_at)?,
            });
        }
        Ok(records)
    }

    /// Hide a DM from one active participant.
    pub async fn hide_dm(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE channel_members SET hidden_at = ? \
             WHERE community_id = ? AND channel_id = ? AND pubkey = ? \
               AND removed_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .bind(pubkey)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!(
                "no active membership for channel {channel_id}"
            )));
        }
        Ok(())
    }

    /// Clear one active participant's hidden DM state.
    pub async fn unhide_dm(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        sqlx::query(
            "UPDATE channel_members SET hidden_at = NULL \
             WHERE community_id = ? AND channel_id = ? AND pubkey = ? \
               AND removed_at IS NULL",
        )
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .bind(pubkey)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// List live DMs hidden by one active participant.
    pub async fn list_hidden_dms(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<Vec<Uuid>> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT cm.channel_id FROM channel_members cm \
             JOIN channels c \
               ON c.community_id = cm.community_id AND c.id = cm.channel_id \
             WHERE cm.community_id = ? AND cm.pubkey = ? \
               AND cm.removed_at IS NULL AND cm.hidden_at IS NOT NULL \
               AND c.channel_type = 'dm' AND c.deleted_at IS NULL \
             ORDER BY cm.channel_id",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|channel_id| {
                Uuid::parse_str(&channel_id)
                    .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))
            })
            .collect()
    }
}
