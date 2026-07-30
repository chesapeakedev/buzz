//! SQLite channel creation and lookup operations.

use chrono::{DateTime, Utc};
use sqlx::{QueryBuilder, Row as _, Sqlite};
use uuid::Uuid;

use super::SqliteStore;
use crate::channel::{ChannelRecord, ChannelType, ChannelVisibility, MemberRecord, MemberRole};
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

async fn active_role(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    community_id: &str,
    channel_id: &str,
    pubkey: &[u8],
) -> Result<Option<String>> {
    sqlx::query_scalar(
        "SELECT role FROM channel_members \
         WHERE community_id = ? AND channel_id = ? AND pubkey = ? \
           AND removed_at IS NULL",
    )
    .bind(community_id)
    .bind(channel_id)
    .bind(pubkey)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(Into::into)
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

    /// Return active membership pairs for the supplied channel and pubkey sets.
    pub async fn membership_pairs(
        &self,
        community: CommunityId,
        channel_ids: &[Uuid],
        pubkeys: &[Vec<u8>],
    ) -> Result<Vec<(Uuid, Vec<u8>)>> {
        if channel_ids.is_empty() || pubkeys.is_empty() {
            return Ok(Vec::new());
        }
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT cm.channel_id, cm.pubkey FROM channel_members cm \
             JOIN channels c \
               ON cm.community_id = c.community_id AND cm.channel_id = c.id \
             WHERE cm.community_id = ",
        );
        builder
            .push_bind(community.as_uuid().to_string())
            .push(" AND cm.channel_id IN (");
        let mut channels = builder.separated(", ");
        for channel_id in channel_ids {
            channels.push_bind(channel_id.to_string());
        }
        builder.push(") AND cm.pubkey IN (");
        let mut keys = builder.separated(", ");
        for pubkey in pubkeys {
            keys.push_bind(pubkey);
        }
        builder.push(
            ") AND cm.removed_at IS NULL AND c.deleted_at IS NULL \
             ORDER BY cm.channel_id, cm.pubkey",
        );
        let rows = builder.build().fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                let channel_id: String = row.try_get("channel_id")?;
                let channel_id = Uuid::parse_str(&channel_id)
                    .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))?;
                Ok((channel_id, row.try_get("pubkey")?))
            })
            .collect()
    }

    /// Return active members for the supplied live channels.
    pub async fn get_members_bulk(
        &self,
        community: CommunityId,
        channel_ids: &[Uuid],
    ) -> Result<Vec<MemberRecord>> {
        if channel_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT cm.channel_id, cm.pubkey, cm.role, cm.joined_at, \
                    cm.invited_by, cm.removed_at \
             FROM channel_members cm \
             JOIN channels c \
               ON cm.community_id = c.community_id AND cm.channel_id = c.id \
             WHERE cm.community_id = ",
        );
        builder
            .push_bind(community.as_uuid().to_string())
            .push(" AND cm.channel_id IN (");
        let mut channels = builder.separated(", ");
        for channel_id in channel_ids {
            channels.push_bind(channel_id.to_string());
        }
        builder.push(
            ") AND cm.removed_at IS NULL AND c.deleted_at IS NULL \
             ORDER BY cm.joined_at, cm.pubkey",
        );
        builder
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(parse_member)
            .collect()
    }

    /// Add or reactivate a channel member with PostgreSQL-equivalent role checks.
    ///
    /// The process-wide writer gate and immediate transaction serialize the
    /// authorization read, last-owner check, and membership upsert.
    pub async fn add_member(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
        role: MemberRole,
        invited_by: Option<&[u8]>,
    ) -> Result<MemberRecord> {
        if pubkey.len() != 32 {
            return Err(DbError::InvalidData(format!(
                "pubkey must be 32 bytes, got {}",
                pubkey.len()
            )));
        }

        let community_id = community.as_uuid().to_string();
        let channel_id_text = channel_id.to_string();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let channel = sqlx::query(
            "SELECT visibility, created_by FROM channels \
             WHERE community_id = ? AND id = ? AND deleted_at IS NULL",
        )
        .bind(&community_id)
        .bind(&channel_id_text)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::ChannelNotFound(channel_id))?;
        let visibility: String = channel.try_get("visibility")?;
        let created_by: Vec<u8> = channel.try_get("created_by")?;

        let effective_role = if visibility == ChannelVisibility::Private.as_str() {
            let inviter = invited_by.ok_or_else(|| {
                DbError::AccessDenied("private channel requires an invite".to_owned())
            })?;
            let creator_bootstrap = inviter == pubkey && inviter == created_by.as_slice();
            if !creator_bootstrap {
                let inviter_role =
                    active_role(&mut transaction, &community_id, &channel_id_text, inviter)
                        .await?
                        .ok_or_else(|| {
                            DbError::AccessDenied("inviter is not an active member".to_owned())
                        })?;
                let inviter_role: MemberRole = inviter_role.parse().map_err(|_| {
                    DbError::InvalidData(format!("invalid role in database: {inviter_role}"))
                })?;
                if role.is_elevated() && !inviter_role.is_elevated() {
                    return Err(DbError::AccessDenied(
                        "only owners/admins may grant elevated roles".to_owned(),
                    ));
                }
            }
            role
        } else if role.is_elevated() {
            let granter_role = match invited_by {
                Some(inviter) => {
                    active_role(&mut transaction, &community_id, &channel_id_text, inviter).await?
                }
                None => None,
            };
            match granter_role.as_deref() {
                Some("owner") | Some("admin") => role,
                _ => {
                    return Err(DbError::AccessDenied(
                        "only owners/admins may grant elevated roles".to_owned(),
                    ))
                }
            }
        } else {
            role
        };

        let current_role =
            active_role(&mut transaction, &community_id, &channel_id_text, pubkey).await?;
        if let Some(current_role) =
            current_role.filter(|current| current != effective_role.as_str())
        {
            let actor_role = match invited_by {
                Some(inviter) => {
                    active_role(&mut transaction, &community_id, &channel_id_text, inviter).await?
                }
                None => None,
            };
            let actor_is_elevated = actor_role
                .as_deref()
                .and_then(|value| value.parse::<MemberRole>().ok())
                .is_some_and(|value| value.is_elevated());
            if !actor_is_elevated {
                return Err(DbError::AccessDenied(
                    "only owners/admins may change an active member's role".to_owned(),
                ));
            }
            if current_role == MemberRole::Owner.as_str() && effective_role != MemberRole::Owner {
                let owner_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM channel_members \
                     WHERE community_id = ? AND channel_id = ? \
                       AND role = 'owner' AND removed_at IS NULL",
                )
                .bind(&community_id)
                .bind(&channel_id_text)
                .fetch_one(&mut *transaction)
                .await?;
                if owner_count <= 1 {
                    return Err(DbError::AccessDenied(
                        "cannot demote the last owner — transfer ownership first".to_owned(),
                    ));
                }
            }
        }

        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "INSERT INTO channel_members \
             (community_id, channel_id, pubkey, role, joined_at, invited_by) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, channel_id, pubkey) DO UPDATE SET \
               removed_at = NULL, removed_by = NULL, role = excluded.role",
        )
        .bind(&community_id)
        .bind(&channel_id_text)
        .bind(pubkey)
        .bind(effective_role.as_str())
        .bind(now)
        .bind(invited_by)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "SELECT channel_id, pubkey, role, joined_at, invited_by, removed_at \
             FROM channel_members \
             WHERE community_id = ? AND channel_id = ? AND pubkey = ?",
        )
        .bind(&community_id)
        .bind(&channel_id_text)
        .bind(pubkey)
        .fetch_one(&mut *transaction)
        .await?;
        let member = parse_member(row)?;
        transaction.commit().await?;
        Ok(member)
    }

    /// Soft-remove a member after serialized authorization and last-owner checks.
    pub async fn remove_member(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
        actor_pubkey: &[u8],
    ) -> Result<()> {
        let community_id = community.as_uuid().to_string();
        let channel_id_text = channel_id.to_string();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let self_remove = pubkey == actor_pubkey;
        if !self_remove {
            let actor_role = active_role(
                &mut transaction,
                &community_id,
                &channel_id_text,
                actor_pubkey,
            )
            .await?
            .ok_or_else(|| DbError::AccessDenied("actor is not an active member".to_owned()))?;
            let actor_role: MemberRole = actor_role.parse().map_err(|_| {
                DbError::InvalidData(format!("invalid role in database: {actor_role}"))
            })?;
            let actor_is_agent_owner: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM users \
                 WHERE community_id = ? AND pubkey = ? AND agent_owner_pubkey = ?)",
            )
            .bind(&community_id)
            .bind(pubkey)
            .bind(actor_pubkey)
            .fetch_one(&mut *transaction)
            .await?;
            if !actor_role.is_elevated() && !actor_is_agent_owner {
                return Err(DbError::AccessDenied(
                    "only owners/admins or the agent's owner may remove other members".to_owned(),
                ));
            }
        }

        let target_role =
            active_role(&mut transaction, &community_id, &channel_id_text, pubkey).await?;
        if target_role.as_deref() == Some(MemberRole::Owner.as_str()) {
            let owner_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM channel_members \
                 WHERE community_id = ? AND channel_id = ? \
                   AND role = 'owner' AND removed_at IS NULL",
            )
            .bind(&community_id)
            .bind(&channel_id_text)
            .fetch_one(&mut *transaction)
            .await?;
            if owner_count <= 1 {
                return Err(DbError::AccessDenied(
                    "cannot remove the last owner — transfer ownership first".to_owned(),
                ));
            }
        }

        let result = sqlx::query(
            "UPDATE channel_members SET removed_at = ?, removed_by = ? \
             WHERE community_id = ? AND channel_id = ? AND pubkey = ? \
               AND removed_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(actor_pubkey)
        .bind(&community_id)
        .bind(&channel_id_text)
        .bind(pubkey)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::MemberNotFound(channel_id));
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Return whether a pubkey is an active member of a live channel.
    pub async fn is_member(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS( \
               SELECT 1 FROM channel_members cm \
               JOIN channels c \
                 ON cm.community_id = c.community_id AND cm.channel_id = c.id \
               WHERE cm.community_id = ? AND cm.channel_id = ? AND cm.pubkey = ? \
                 AND cm.removed_at IS NULL AND c.deleted_at IS NULL)",
        )
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .bind(pubkey)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Return an active member's role in a live channel.
    pub async fn get_member_role(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<Option<String>> {
        sqlx::query_scalar(
            "SELECT cm.role FROM channel_members cm \
             JOIN channels c \
               ON cm.community_id = c.community_id AND cm.channel_id = c.id \
             WHERE cm.community_id = ? AND cm.channel_id = ? AND cm.pubkey = ? \
               AND cm.removed_at IS NULL AND c.deleted_at IS NULL",
        )
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .bind(pubkey)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Return the active member count for one channel.
    pub async fn get_member_count(&self, community: CommunityId, channel_id: Uuid) -> Result<i64> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM channel_members \
             WHERE community_id = ? AND channel_id = ? AND removed_at IS NULL",
        )
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Return active member counts for the supplied channels.
    pub async fn get_member_counts_bulk(
        &self,
        community: CommunityId,
        channel_ids: &[Uuid],
    ) -> Result<std::collections::HashMap<Uuid, i64>> {
        if channel_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT channel_id, COUNT(*) AS member_count FROM channel_members \
             WHERE community_id = ",
        );
        builder
            .push_bind(community.as_uuid().to_string())
            .push(" AND removed_at IS NULL AND channel_id IN (");
        let mut channels = builder.separated(", ");
        for channel_id in channel_ids {
            channels.push_bind(channel_id.to_string());
        }
        builder.push(") GROUP BY channel_id");
        let rows = builder.build().fetch_all(&self.pool).await?;
        let mut counts = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            let channel_id: String = row.try_get("channel_id")?;
            let channel_id = Uuid::parse_str(&channel_id)
                .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))?;
            counts.insert(channel_id, row.try_get("member_count")?);
        }
        Ok(counts)
    }

    /// Return live open channels plus live channels where a pubkey is a member.
    pub async fn get_accessible_channel_ids(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<Vec<Uuid>> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT cm.channel_id FROM channel_members cm \
             JOIN channels c \
               ON cm.community_id = c.community_id AND cm.channel_id = c.id \
             WHERE cm.community_id = ? AND cm.pubkey = ? \
               AND cm.removed_at IS NULL AND c.deleted_at IS NULL \
             UNION \
             SELECT id FROM channels \
             WHERE community_id = ? AND visibility = 'open' AND deleted_at IS NULL",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(community.as_uuid().to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|value| {
                Uuid::parse_str(&value)
                    .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))
            })
            .collect()
    }
}
