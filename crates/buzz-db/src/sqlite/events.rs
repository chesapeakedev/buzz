//! SQLite event insertion and direct lifecycle operations.

use chrono::{DateTime, Utc};
use nostr::Event;
use sqlx::Row as _;
use uuid::Uuid;

use buzz_core::kind::{event_kind_i32, is_ephemeral, KIND_AUTH};
use buzz_core::{CommunityId, StoredEvent};

use super::SqliteStore;
use crate::event::{extract_d_tag, extract_not_before};
use crate::{DbError, Result};

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or(DbError::InvalidTimestamp(value))
}

fn row_to_stored_event(row: sqlx::sqlite::SqliteRow) -> Result<Option<StoredEvent>> {
    let id: Vec<u8> = row.try_get("id")?;
    let pubkey: Vec<u8> = row.try_get("pubkey")?;
    let created_at: i64 = row.try_get("created_at")?;
    let kind: i64 = row.try_get("kind")?;
    let tags: String = row.try_get("tags")?;
    let content: String = row.try_get("content")?;
    let sig: Vec<u8> = row.try_get("sig")?;
    let received_at: i64 = row.try_get("received_at")?;
    let channel_id: Option<String> = row.try_get("channel_id")?;

    let kind = u16::try_from(kind)
        .map_err(|_| DbError::InvalidData(format!("kind out of u16 range: {kind}")))?;
    let tags: serde_json::Value = serde_json::from_str(&tags)?;
    let channel_id = channel_id
        .map(|value| {
            Uuid::parse_str(&value)
                .map_err(|error| DbError::InvalidData(format!("channel UUID: {error}")))
        })
        .transpose()?;
    let event_json = serde_json::json!({
        "id": hex::encode(id),
        "pubkey": hex::encode(pubkey),
        "created_at": created_at / 1_000_000,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": hex::encode(sig),
    });
    let event = match serde_json::from_value(event_json) {
        Ok(event) => event,
        Err(error) => {
            tracing::warn!("failed to reconstruct event from SQLite row: {error}");
            return Ok(None);
        }
    };

    Ok(Some(StoredEvent::with_received_at(
        event,
        parse_timestamp(received_at)?,
        channel_id,
        true,
    )))
}

impl SqliteStore {
    /// Insert a durable Nostr event, returning `false` for a tenant-local duplicate.
    pub async fn insert_event(
        &self,
        community_id: CommunityId,
        event: &Event,
        channel_id: Option<Uuid>,
    ) -> Result<(StoredEvent, bool)> {
        let kind = event.kind.as_u16();
        if u32::from(kind) == KIND_AUTH {
            return Err(DbError::AuthEventRejected);
        }
        if is_ephemeral(u32::from(kind)) {
            return Err(DbError::EphemeralEventRejected(kind));
        }

        let created_at_seconds = i64::try_from(event.created_at.as_secs())
            .map_err(|_| DbError::InvalidTimestamp(i64::MAX))?;
        let created_at = created_at_seconds
            .checked_mul(1_000_000)
            .ok_or(DbError::InvalidTimestamp(created_at_seconds))?;
        let received_at = Utc::now();
        let tags = serde_json::to_string(&event.tags)?;
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "INSERT INTO events \
             (community_id, id, pubkey, created_at, kind, tags, content, sig, \
              received_at, channel_id, d_tag, not_before) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, id) DO NOTHING",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(event.id.as_bytes().as_slice())
        .bind(event.pubkey.to_bytes().as_slice())
        .bind(created_at)
        .bind(event_kind_i32(event))
        .bind(tags)
        .bind(&event.content)
        .bind(event.sig.serialize().as_slice())
        .bind(received_at.timestamp_micros())
        .bind(channel_id.map(|id| id.to_string()))
        .bind(extract_d_tag(event))
        .bind(extract_not_before(event))
        .execute(&self.pool)
        .await?;

        Ok((
            StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
            result.rows_affected() > 0,
        ))
    }

    /// Fetch one live event by its tenant-scoped raw identifier.
    pub async fn get_event_by_id(
        &self,
        community_id: CommunityId,
        id: &[u8],
    ) -> Result<Option<StoredEvent>> {
        self.get_event_by_id_inner(community_id, id, false).await
    }

    /// Fetch one event by its tenant-scoped raw identifier, including tombstones.
    pub async fn get_event_by_id_including_deleted(
        &self,
        community_id: CommunityId,
        id: &[u8],
    ) -> Result<Option<StoredEvent>> {
        self.get_event_by_id_inner(community_id, id, true).await
    }

    async fn get_event_by_id_inner(
        &self,
        community_id: CommunityId,
        id: &[u8],
        include_deleted: bool,
    ) -> Result<Option<StoredEvent>> {
        let sql = if include_deleted {
            "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
             FROM events WHERE community_id = ? AND id = ?"
        } else {
            "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
             FROM events WHERE community_id = ? AND id = ? AND deleted_at IS NULL"
        };
        sqlx::query(sql)
            .bind(community_id.as_uuid().to_string())
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(row_to_stored_event)
            .transpose()
            .map(Option::flatten)
    }

    /// Idempotently soft-delete one tenant-scoped event.
    pub async fn soft_delete_event(&self, community_id: CommunityId, id: &[u8]) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE events SET deleted_at = ? \
             WHERE community_id = ? AND id = ? AND deleted_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(community_id.as_uuid().to_string())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }
}
