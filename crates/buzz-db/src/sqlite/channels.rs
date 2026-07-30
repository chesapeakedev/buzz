//! SQLite channel creation and lookup operations.

use chrono::{DateTime, Utc};
use sqlx::Row as _;
use uuid::Uuid;

use super::SqliteStore;
use crate::channel::{ChannelRecord, ChannelType, ChannelVisibility, MemberRecord};
use crate::{CommunityId, DbError, Result};

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or(DbError::InvalidTimestamp(value))
}

fn parse_optional_timestamp(value: Option<i64>) -> Result<Option<DateTime<Utc>>> {
    value.map(parse_timestamp).transpose()
}

fn parse_channel(row: sqlx::sqlite::SqliteRow) -> Result<ChannelRecord> {
    let id: String = row.try_get("id")?;
    Ok(ChannelRecord {
        id: Uuid::parse_str(&id)
            .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))?,
        name: row.try_get("name")?,
        channel_type: row.try_get("channel_type")?,
        visibility: row.try_get("visibility")?,
        description: row.try_get("description")?,
        canvas: row.try_get("canvas")?,
        created_by: row.try_get("created_by")?,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?,
        archived_at: parse_optional_timestamp(row.try_get("archived_at")?)?,
        deleted_at: parse_optional_timestamp(row.try_get("deleted_at")?)?,
        nip29_group_id: row.try_get("nip29_group_id")?,
        topic_required: row.try_get::<i64, _>("topic_required")? != 0,
        max_members: row.try_get("max_members")?,
        topic: row.try_get("topic")?,
        topic_set_by: row.try_get("topic_set_by")?,
        topic_set_at: parse_optional_timestamp(row.try_get("topic_set_at")?)?,
        purpose: row.try_get("purpose")?,
        purpose_set_by: row.try_get("purpose_set_by")?,
        purpose_set_at: parse_optional_timestamp(row.try_get("purpose_set_at")?)?,
        ttl_seconds: row.try_get("ttl_seconds")?,
        ttl_deadline: parse_optional_timestamp(row.try_get("ttl_deadline")?)?,
    })
}

fn parse_member(row: sqlx::sqlite::SqliteRow) -> Result<MemberRecord> {
    let channel_id: String = row.try_get("channel_id")?;
    Ok(MemberRecord {
        channel_id: Uuid::parse_str(&channel_id)
            .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))?,
        pubkey: row.try_get("pubkey")?,
        role: row.try_get("role")?,
        joined_at: parse_timestamp(row.try_get("joined_at")?)?,
        invited_by: row.try_get("invited_by")?,
        removed_at: parse_optional_timestamp(row.try_get("removed_at")?)?,
    })
}

impl SqliteStore {
    /// Create a channel with an application-generated tenant-local UUID.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_channel(
        &self,
        community: CommunityId,
        name: &str,
        channel_type: ChannelType,
        visibility: ChannelVisibility,
        description: Option<&str>,
        created_by: &[u8],
        ttl_seconds: Option<i32>,
    ) -> Result<ChannelRecord> {
        self.create_channel_with_id(
            community,
            Uuid::new_v4(),
            name,
            channel_type,
            visibility,
            description,
            created_by,
            ttl_seconds,
        )
        .await
        .map(|(record, _)| record)
    }

    /// Create a channel with a caller-supplied tenant-local UUID.
    ///
    /// The creator membership is inserted in the same immediate transaction.
    /// A duplicate `(community, channel)` returns the existing record without
    /// changing its metadata or membership.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_channel_with_id(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        name: &str,
        channel_type: ChannelType,
        visibility: ChannelVisibility,
        description: Option<&str>,
        created_by: &[u8],
        ttl_seconds: Option<i32>,
    ) -> Result<(ChannelRecord, bool)> {
        if created_by.len() != 32 {
            return Err(DbError::InvalidData(format!(
                "pubkey must be 32 bytes, got {}",
                created_by.len()
            )));
        }
        if channel_id.is_nil() {
            return Err(DbError::InvalidData(
                "channel_id must not be nil (reserved for global fan-out)".to_owned(),
            ));
        }
        let name = buzz_core::channel::canonical_channel_name(name);
        if name.is_empty() {
            return Err(DbError::InvalidData("channel name is required".to_owned()));
        }

        let now = Utc::now().timestamp_micros();
        let ttl_deadline = match ttl_seconds {
            Some(ttl) => Some(
                i64::from(ttl)
                    .checked_mul(1_000_000)
                    .and_then(|duration| now.checked_add(duration))
                    .ok_or_else(|| {
                        DbError::InvalidData("ttl_seconds produces an invalid deadline".to_owned())
                    })?,
            ),
            None => None,
        };
        let community_id = community.as_uuid().to_string();
        let channel_id = channel_id.to_string();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let inserted = sqlx::query(
            "INSERT INTO channels \
             (community_id, id, name, channel_type, visibility, description, \
              created_by, created_at, updated_at, ttl_seconds, ttl_deadline) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, id) DO NOTHING",
        )
        .bind(&community_id)
        .bind(&channel_id)
        .bind(name)
        .bind(channel_type.as_str())
        .bind(visibility.as_str())
        .bind(description)
        .bind(created_by)
        .bind(now)
        .bind(now)
        .bind(ttl_seconds)
        .bind(ttl_deadline)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;

        if inserted {
            sqlx::query(
                "INSERT INTO channel_members \
                 (community_id, channel_id, pubkey, role, joined_at, invited_by) \
                 VALUES (?, ?, ?, 'owner', ?, ?)",
            )
            .bind(&community_id)
            .bind(&channel_id)
            .bind(created_by)
            .bind(now)
            .bind(created_by)
            .execute(&mut *transaction)
            .await?;
        }

        let row = sqlx::query(
            "SELECT id, name, channel_type, visibility, description, canvas, \
                    created_by, created_at, updated_at, archived_at, deleted_at, \
                    nip29_group_id, topic_required, max_members, topic, \
                    topic_set_by, topic_set_at, purpose, purpose_set_by, \
                    purpose_set_at, ttl_seconds, ttl_deadline \
             FROM channels WHERE community_id = ? AND id = ?",
        )
        .bind(&community_id)
        .bind(&channel_id)
        .fetch_one(&mut *transaction)
        .await?;
        let record = parse_channel(row)?;
        transaction.commit().await?;
        Ok((record, inserted))
    }

    /// Return a live channel in one community.
    pub async fn get_channel(
        &self,
        community: CommunityId,
        channel_id: Uuid,
    ) -> Result<ChannelRecord> {
        let row = sqlx::query(
            "SELECT id, name, channel_type, visibility, description, canvas, \
                    created_by, created_at, updated_at, archived_at, deleted_at, \
                    nip29_group_id, topic_required, max_members, topic, \
                    topic_set_by, topic_set_at, purpose, purpose_set_by, \
                    purpose_set_at, ttl_seconds, ttl_deadline \
             FROM channels \
             WHERE community_id = ? AND id = ? AND deleted_at IS NULL",
        )
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(DbError::ChannelNotFound(channel_id))?;
        parse_channel(row)
    }

    /// List live channels in one community, optionally filtered by visibility.
    pub async fn list_channels(
        &self,
        community: CommunityId,
        visibility: Option<&str>,
    ) -> Result<Vec<ChannelRecord>> {
        let rows = sqlx::query(
            "SELECT id, name, channel_type, visibility, description, canvas, \
                    created_by, created_at, updated_at, archived_at, deleted_at, \
                    nip29_group_id, topic_required, max_members, topic, \
                    topic_set_by, topic_set_at, purpose, purpose_set_by, \
                    purpose_set_at, ttl_seconds, ttl_deadline \
             FROM channels \
             WHERE community_id = ? AND deleted_at IS NULL \
               AND (? IS NULL OR visibility = ?) \
             ORDER BY created_at DESC, id LIMIT 1000",
        )
        .bind(community.as_uuid().to_string())
        .bind(visibility)
        .bind(visibility)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_channel).collect()
    }

    /// Return the active members of a live channel.
    pub async fn get_members(
        &self,
        community: CommunityId,
        channel_id: Uuid,
    ) -> Result<Vec<MemberRecord>> {
        let rows = sqlx::query(
            "SELECT cm.channel_id, cm.pubkey, cm.role, cm.joined_at, \
                    cm.invited_by, cm.removed_at \
             FROM channel_members cm \
             JOIN channels c \
               ON cm.community_id = c.community_id AND cm.channel_id = c.id \
             WHERE cm.community_id = ? AND cm.channel_id = ? \
               AND cm.removed_at IS NULL AND c.deleted_at IS NULL \
             ORDER BY cm.joined_at, cm.pubkey LIMIT 1000",
        )
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_member).collect()
    }
}
