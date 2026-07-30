//! SQLite event insertion and direct lifecycle operations.

use chrono::{DateTime, Utc};
use nostr::Event;
use sqlx::{QueryBuilder, Row as _};
use uuid::Uuid;

use buzz_core::kind::{
    event_kind_i32, is_ephemeral, KIND_AUTH, KIND_BOOKMARK_SET, KIND_EVENT_REMINDER,
    KIND_READ_STATE,
};
use buzz_core::{CommunityId, StoredEvent};

use super::SqliteStore;
use crate::event::{extract_d_tag, extract_not_before, DueReminder, EventQuery};
use crate::{DbError, Result};

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or(DbError::InvalidTimestamp(value))
}

fn event_timestamp_micros(event: &Event) -> Result<i64> {
    let seconds = i64::try_from(event.created_at.as_secs())
        .map_err(|_| DbError::InvalidTimestamp(i64::MAX))?;
    seconds
        .checked_mul(1_000_000)
        .ok_or(DbError::InvalidTimestamp(seconds))
}

async fn insert_mentions(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    community_id: CommunityId,
    event: &Event,
    channel_id: Option<Uuid>,
    created_at: i64,
) -> Result<()> {
    let valid_pubkeys = event.tags.iter().filter_map(|tag| {
        let parts = tag.as_slice();
        let pubkey = parts.get(1)?;
        (parts.first().is_some_and(|kind| kind == "p")
            && pubkey.len() == 64
            && pubkey
                .chars()
                .all(|character| character.is_ascii_hexdigit()))
        .then(|| pubkey.to_ascii_lowercase())
    });
    for pubkey in valid_pubkeys {
        sqlx::query(
            "INSERT INTO event_mentions \
             (community_id, pubkey_hex, event_id, event_created_at, channel_id, event_kind) \
             VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(pubkey)
        .bind(event.id.as_bytes().as_slice())
        .bind(created_at)
        .bind(channel_id.map(|id| id.to_string()))
        .bind(event_kind_i32(event))
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

pub(super) async fn insert_event_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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

    let created_at = event_timestamp_micros(event)?;
    let received_at = Utc::now();
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
    .bind(serde_json::to_string(&event.tags)?)
    .bind(&event.content)
    .bind(event.sig.serialize().as_slice())
    .bind(received_at.timestamp_micros())
    .bind(channel_id.map(|id| id.to_string()))
    .bind(extract_d_tag(event))
    .bind(extract_not_before(event))
    .execute(&mut **transaction)
    .await?;
    let was_inserted = result.rows_affected() > 0;

    if was_inserted {
        insert_mentions(transaction, community_id, event, channel_id, created_at).await?;
    }

    Ok((
        StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
        was_inserted,
    ))
}

fn is_nip_rs(event: &Event, d_tag: &str) -> bool {
    let d_tag_count = event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().is_some_and(|part| part == "d"))
        .count();
    let has_exact_d_tag = event.tags.iter().any(|tag| {
        let parts = tag.as_slice();
        parts.len() >= 2 && parts[0] == "d" && parts[1] == d_tag
    });
    let read_state_tags = event
        .tags
        .iter()
        .filter(|tag| {
            let parts = tag.as_slice();
            parts.len() == 2 && parts[0] == "t" && parts[1] == "read-state"
        })
        .count();

    event_kind_i32(event) == KIND_READ_STATE as i32
        && d_tag_count == 1
        && has_exact_d_tag
        && d_tag.strip_prefix("read-state:").is_some_and(|slot| {
            slot.len() == 32
                && slot
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
        && read_state_tags == 1
}

fn is_buzz_mesh_status(event: &Event, d_tag: &str) -> bool {
    event_kind_i32(event) == KIND_BOOKMARK_SET as i32
        && d_tag.starts_with("buzz-mesh-member-status:")
        && event.tags.iter().any(|tag| {
            let parts = tag.as_slice();
            parts.len() == 2 && parts[0] == "k" && parts[1] == "buzz-mesh-status"
        })
}

pub(super) fn row_to_stored_event(row: sqlx::sqlite::SqliteRow) -> Result<Option<StoredEvent>> {
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
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let result =
            insert_event_transaction(&mut transaction, community_id, event, channel_id).await?;
        transaction.commit().await?;
        Ok(result)
    }

    /// Atomically replace a NIP-16 or channel-scoped relay state event.
    ///
    /// The newest timestamp wins; same-second ties choose the lowest event ID.
    pub async fn replace_addressable_event(
        &self,
        community_id: CommunityId,
        event: &Event,
        channel_id: Option<Uuid>,
    ) -> Result<(StoredEvent, bool)> {
        let created_at = event_timestamp_micros(event)?;
        let received_at = Utc::now();
        let kind = event_kind_i32(event);
        let pubkey = event.pubkey.to_bytes();
        let channel = channel_id.map(|id| id.to_string());
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;

        let existing = sqlx::query(
            "SELECT created_at, id FROM events \
             WHERE community_id = ? AND kind = ? AND pubkey = ? \
               AND (channel_id = ? OR (channel_id IS NULL AND ? IS NULL)) \
               AND deleted_at IS NULL \
             ORDER BY created_at DESC, id ASC LIMIT 1",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(kind)
        .bind(pubkey.as_slice())
        .bind(channel.as_deref())
        .bind(channel.as_deref())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let current_created_at: i64 = row.try_get("created_at")?;
            let current_id: Vec<u8> = row.try_get("id")?;
            if created_at < current_created_at
                || (created_at == current_created_at
                    && event.id.as_bytes().as_slice() >= current_id.as_slice())
            {
                transaction.rollback().await?;
                return Ok((
                    StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
                    false,
                ));
            }
        }

        sqlx::query(
            "UPDATE events SET deleted_at = ? \
             WHERE community_id = ? AND kind = ? AND pubkey = ? \
               AND (channel_id = ? OR (channel_id IS NULL AND ? IS NULL)) \
               AND deleted_at IS NULL",
        )
        .bind(received_at.timestamp_micros())
        .bind(community_id.as_uuid().to_string())
        .bind(kind)
        .bind(pubkey.as_slice())
        .bind(channel.as_deref())
        .bind(channel.as_deref())
        .execute(&mut *transaction)
        .await?;

        let result = sqlx::query(
            "INSERT INTO events \
             (community_id, id, pubkey, created_at, kind, tags, content, sig, \
              received_at, channel_id, d_tag, not_before) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, id) DO NOTHING",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(event.id.as_bytes().as_slice())
        .bind(pubkey.as_slice())
        .bind(created_at)
        .bind(kind)
        .bind(serde_json::to_string(&event.tags)?)
        .bind(&event.content)
        .bind(event.sig.serialize().as_slice())
        .bind(received_at.timestamp_micros())
        .bind(channel)
        .bind(extract_d_tag(event))
        .bind(extract_not_before(event))
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok((
                StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
                false,
            ));
        }
        insert_mentions(
            &mut transaction,
            community_id,
            event,
            channel_id,
            created_at,
        )
        .await?;
        transaction.commit().await?;

        Ok((
            StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
            true,
        ))
    }

    /// Atomically replace one global NIP-33 coordinate.
    ///
    /// Conforming NIP-RS and mesh-status coordinates remove superseded payloads;
    /// NIP-RS additionally retains a compact durable watermark to reject replay.
    pub async fn replace_parameterized_event(
        &self,
        community_id: CommunityId,
        event: &Event,
        d_tag: &str,
        channel_id: Option<Uuid>,
    ) -> Result<(StoredEvent, bool)> {
        let created_at = event_timestamp_micros(event)?;
        let received_at = Utc::now();
        let kind = event_kind_i32(event);
        let pubkey = event.pubkey.to_bytes();
        let nip_rs = is_nip_rs(event, d_tag);
        let hard_delete = nip_rs || is_buzz_mesh_status(event, d_tag);
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;

        let existing = sqlx::query(
            "SELECT created_at, id FROM events \
             WHERE community_id = ? AND kind = ? AND pubkey = ? AND d_tag = ? \
               AND deleted_at IS NULL \
             ORDER BY created_at DESC, id ASC LIMIT 1",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(kind)
        .bind(pubkey.as_slice())
        .bind(d_tag)
        .fetch_optional(&mut *transaction)
        .await?;
        let watermark = if nip_rs {
            sqlx::query(
                "SELECT created_at, event_id AS id FROM parameterized_event_watermarks \
                 WHERE community_id = ? AND kind = ? AND pubkey = ? AND d_tag = ?",
            )
            .bind(community_id.as_uuid().to_string())
            .bind(kind)
            .bind(pubkey.as_slice())
            .bind(d_tag)
            .fetch_optional(&mut *transaction)
            .await?
        } else {
            None
        };
        for row in existing.iter().chain(watermark.iter()) {
            let accepted_at: i64 = row.try_get("created_at")?;
            let accepted_id: Vec<u8> = row.try_get("id")?;
            if created_at < accepted_at
                || (created_at == accepted_at
                    && event.id.as_bytes().as_slice() >= accepted_id.as_slice())
            {
                transaction.rollback().await?;
                return Ok((
                    StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
                    false,
                ));
            }
        }

        if existing.is_some() {
            let statement = if hard_delete {
                "DELETE FROM events \
                 WHERE community_id = ? AND kind = ? AND pubkey = ? AND d_tag = ? \
                   AND deleted_at IS NULL"
            } else {
                "UPDATE events SET deleted_at = ? \
                 WHERE community_id = ? AND kind = ? AND pubkey = ? AND d_tag = ? \
                   AND deleted_at IS NULL"
            };
            let mut query = sqlx::query(statement);
            if !hard_delete {
                query = query.bind(received_at.timestamp_micros());
            }
            query
                .bind(community_id.as_uuid().to_string())
                .bind(kind)
                .bind(pubkey.as_slice())
                .bind(d_tag)
                .execute(&mut *transaction)
                .await?;
        }

        let result = sqlx::query(
            "INSERT INTO events \
             (community_id, id, pubkey, created_at, kind, tags, content, sig, \
              received_at, channel_id, d_tag, not_before) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, id) DO NOTHING",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(event.id.as_bytes().as_slice())
        .bind(pubkey.as_slice())
        .bind(created_at)
        .bind(kind)
        .bind(serde_json::to_string(&event.tags)?)
        .bind(&event.content)
        .bind(event.sig.serialize().as_slice())
        .bind(received_at.timestamp_micros())
        .bind(channel_id.map(|id| id.to_string()))
        .bind(d_tag)
        .bind(extract_not_before(event))
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok((
                StoredEvent::with_received_at(event.clone(), received_at, channel_id, false),
                false,
            ));
        }

        if nip_rs {
            sqlx::query(
                "INSERT INTO parameterized_event_watermarks \
                 (community_id, kind, pubkey, d_tag, created_at, event_id) \
                 VALUES (?, ?, ?, ?, ?, ?) \
                 ON CONFLICT (community_id, kind, pubkey, d_tag) DO UPDATE SET \
                   created_at = excluded.created_at, event_id = excluded.event_id",
            )
            .bind(community_id.as_uuid().to_string())
            .bind(kind)
            .bind(pubkey.as_slice())
            .bind(d_tag)
            .bind(created_at)
            .bind(event.id.as_bytes().as_slice())
            .execute(&mut *transaction)
            .await?;
        }
        insert_mentions(
            &mut transaction,
            community_id,
            event,
            channel_id,
            created_at,
        )
        .await?;
        transaction.commit().await?;

        Ok((
            StoredEvent::with_received_at(event.clone(), received_at, channel_id, true),
            true,
        ))
    }

    /// Query live events with the same filter and pagination semantics as PostgreSQL.
    pub async fn query_events(&self, query: &EventQuery) -> Result<Vec<StoredEvent>> {
        if query.before_id.is_some() && query.until.is_none() {
            return Err(DbError::InvalidData(
                "before_id requires until to be set".to_owned(),
            ));
        }
        if query.global_only && query.channel_id.is_some() {
            return Err(DbError::InvalidData(
                "global_only and channel_id are mutually exclusive".to_owned(),
            ));
        }
        if query.kinds.as_deref().is_some_and(<[i32]>::is_empty)
            || query.authors.as_deref().is_some_and(<[Vec<u8>]>::is_empty)
            || query.ids.as_deref().is_some_and(<[Vec<u8>]>::is_empty)
            || query.e_tags.as_deref().is_some_and(<[String]>::is_empty)
        {
            return Ok(Vec::new());
        }

        let community = query.community_id.as_uuid().to_string();
        let mut builder: QueryBuilder<sqlx::Sqlite> = if let Some(pubkey) = &query.p_tag_hex {
            let mut builder = QueryBuilder::new(
                "SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags, e.content, \
                 e.sig, e.received_at, e.channel_id \
                 FROM events e INNER JOIN event_mentions m \
                   ON e.community_id = m.community_id AND e.id = m.event_id \
                 WHERE e.community_id = ",
            );
            builder.push_bind(community.clone());
            builder.push(" AND m.community_id = ").push_bind(community);
            builder
                .push(" AND e.deleted_at IS NULL AND m.pubkey_hex = ")
                .push_bind(pubkey.to_ascii_lowercase());
            builder
        } else {
            let mut builder = QueryBuilder::new(
                "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
                 FROM events WHERE community_id = ",
            );
            builder.push_bind(community);
            builder.push(" AND deleted_at IS NULL");
            builder
        };
        let prefix = if query.p_tag_hex.is_some() { "e." } else { "" };

        if let Some(channel) = query.channel_id {
            builder
                .push(format!(" AND {prefix}channel_id = "))
                .push_bind(channel.to_string());
        } else if query.global_only {
            builder.push(format!(" AND {prefix}channel_id IS NULL"));
        }
        if let Some(channels) = &query.channel_ids {
            if channels.is_empty() {
                builder.push(format!(" AND {prefix}channel_id IS NULL"));
            } else {
                builder.push(format!(
                    " AND ({prefix}channel_id IS NULL OR {prefix}channel_id IN ("
                ));
                let mut separated = builder.separated(", ");
                for channel in channels {
                    separated.push_bind(channel.to_string());
                }
                builder.push("))");
            }
        }
        if let Some(kinds) = query.kinds.as_deref().filter(|kinds| !kinds.is_empty()) {
            builder.push(format!(" AND {prefix}kind IN ("));
            let mut separated = builder.separated(", ");
            for kind in kinds {
                separated.push_bind(*kind);
            }
            builder.push(")");
        }
        if let Some(pubkey) = &query.pubkey {
            builder
                .push(format!(" AND {prefix}pubkey = "))
                .push_bind(pubkey.clone());
        }
        if let Some(authors) = query
            .authors
            .as_deref()
            .filter(|authors| !authors.is_empty())
        {
            builder.push(format!(" AND {prefix}pubkey IN ("));
            let mut separated = builder.separated(", ");
            for author in authors {
                separated.push_bind(author.clone());
            }
            builder.push(")");
        }
        if let Some(ids) = query.ids.as_deref().filter(|ids| !ids.is_empty()) {
            builder.push(format!(" AND {prefix}id IN ("));
            let mut separated = builder.separated(", ");
            for id in ids {
                separated.push_bind(id.clone());
            }
            builder.push(")");
        }
        if let Some(event_ids) = query.e_tags.as_deref().filter(|ids| !ids.is_empty()) {
            builder.push(format!(
                " AND EXISTS (SELECT 1 FROM json_each({prefix}tags) AS tag \
                 WHERE json_extract(tag.value, '$[0]') = 'e' \
                   AND json_extract(tag.value, '$[1]') IN ("
            ));
            let mut separated = builder.separated(", ");
            for id in event_ids {
                separated.push_bind(id);
            }
            builder.push("))");
        }
        if let Some(since) = query.since {
            builder
                .push(format!(" AND {prefix}created_at >= "))
                .push_bind(since.timestamp_micros());
        }
        if let Some(until) = query.until {
            let until = until.timestamp_micros();
            if let Some(before_id) = &query.before_id {
                builder
                    .push(format!(" AND ({prefix}created_at < "))
                    .push_bind(until)
                    .push(format!(" OR ({prefix}created_at = "))
                    .push_bind(until)
                    .push(format!(" AND {prefix}id > "))
                    .push_bind(before_id.clone())
                    .push("))");
            } else {
                builder
                    .push(format!(" AND {prefix}created_at <= "))
                    .push_bind(until);
            }
        }
        if let Some(d_tag) = &query.d_tag {
            builder
                .push(format!(" AND {prefix}d_tag = "))
                .push_bind(d_tag);
        } else if let Some(d_tags) = query.d_tags.as_deref().filter(|tags| !tags.is_empty()) {
            builder.push(format!(" AND {prefix}d_tag IN ("));
            let mut separated = builder.separated(", ");
            for tag in d_tags {
                separated.push_bind(tag);
            }
            builder.push(")");
        }
        if let Some(reader) = &query.persona_reader {
            builder
                .push(format!(" AND ({prefix}kind != 30175 OR {prefix}pubkey = "))
                .push_bind(reader.clone())
                .push(format!(
                    " OR EXISTS (SELECT 1 FROM json_each({prefix}tags) AS tag \
                     WHERE json_extract(tag.value, '$[0]') = 'shared' \
                       AND json_extract(tag.value, '$[1]') = 'true'))"
                ));
        }

        let limit = query
            .limit
            .unwrap_or(100)
            .min(query.max_limit.unwrap_or(1000));
        builder.push(format!(
            " ORDER BY {prefix}created_at DESC, {prefix}id ASC LIMIT "
        ));
        builder
            .push_bind(limit)
            .push(" OFFSET ")
            .push_bind(query.offset.unwrap_or(0));

        let rows = builder.build().fetch_all(&self.pool).await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(event) = row_to_stored_event(row)? {
                events.push(event);
            }
        }
        Ok(events)
    }

    /// Count matching events using the same tenant and filter semantics as
    /// [`Self::query_events`]. SQLite keeps this backend-neutral fallback
    /// bounded by the caller's database workload rather than introducing a
    /// second, independently maintained filter builder.
    pub async fn count_events(&self, query: &EventQuery) -> Result<i64> {
        let mut unbounded = query.clone();
        unbounded.limit = Some(i64::MAX);
        unbounded.max_limit = Some(i64::MAX);
        let count = self.query_events(&unbounded).await?.len();
        i64::try_from(count)
            .map_err(|_| DbError::InvalidData("SQLite event count exceeds i64".to_owned()))
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

    /// Batch-fetch live tenant-scoped events by raw identifier.
    ///
    /// Result order is intentionally unspecified, matching PostgreSQL.
    pub async fn get_events_by_ids(
        &self,
        community_id: CommunityId,
        ids: &[&[u8]],
    ) -> Result<Vec<StoredEvent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        debug_assert!(ids.len() <= 500, "batch fetch should be bounded by caller");

        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT id, pubkey, created_at, kind, tags, content, sig, \
             received_at, channel_id FROM events WHERE community_id = ",
        );
        builder
            .push_bind(community_id.as_uuid().to_string())
            .push(" AND deleted_at IS NULL AND id IN (");
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(id.to_vec());
        }
        builder.push(")");

        let rows = builder.build().fetch_all(&self.pool).await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(event) = row_to_stored_event(row)? {
                events.push(event);
            }
        }
        Ok(events)
    }

    /// Fetch the latest live global replaceable event for an author and kind.
    pub async fn get_latest_global_replaceable(
        &self,
        community_id: CommunityId,
        kind: i32,
        pubkey: &[u8],
    ) -> Result<Option<StoredEvent>> {
        sqlx::query(
            "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
             FROM events \
             WHERE community_id = ? AND kind = ? AND pubkey = ? \
               AND channel_id IS NULL AND deleted_at IS NULL \
             ORDER BY created_at DESC, id ASC LIMIT 1",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(kind)
        .bind(pubkey)
        .fetch_optional(&self.pool)
        .await?
        .map(row_to_stored_event)
        .transpose()
        .map(Option::flatten)
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
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let row = sqlx::query(
            "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id, d_tag \
             FROM events WHERE community_id = ? AND id = ? AND deleted_at IS NULL",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let d_tag: Option<String> = row.try_get("d_tag")?;
        let hard_delete = if let Some(d_tag) = d_tag {
            let event = row_to_stored_event(row)?.ok_or_else(|| {
                DbError::InvalidData("failed to reconstruct live event for deletion".to_owned())
            })?;
            is_nip_rs(&event.event, &d_tag)
        } else {
            false
        };
        let statement = if hard_delete {
            "DELETE FROM events WHERE community_id = ? AND id = ?"
        } else {
            "UPDATE events SET deleted_at = ? \
             WHERE community_id = ? AND id = ? AND deleted_at IS NULL"
        };
        let mut query = sqlx::query(statement);
        if !hard_delete {
            query = query.bind(Utc::now().timestamp_micros());
        }
        let result = query
            .bind(community_id.as_uuid().to_string())
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    /// Delete one live NIP-33 coordinate while preserving NIP-RS replay state.
    pub async fn soft_delete_by_coordinate(
        &self,
        community_id: CommunityId,
        kind: i32,
        pubkey: &[u8],
        d_tag: &str,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let rows = sqlx::query(
            "SELECT id, pubkey, created_at, kind, tags, content, sig, received_at, channel_id \
             FROM events \
             WHERE community_id = ? AND kind = ? AND pubkey = ? AND d_tag = ? \
               AND deleted_at IS NULL",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(kind)
        .bind(pubkey)
        .bind(d_tag)
        .fetch_all(&mut *transaction)
        .await?;
        if rows.is_empty() {
            transaction.rollback().await?;
            return Ok(false);
        }

        for row in rows {
            let Some(stored) = row_to_stored_event(row)? else {
                transaction.rollback().await?;
                return Err(DbError::InvalidData(
                    "failed to reconstruct live coordinate event".to_owned(),
                ));
            };
            if is_nip_rs(&stored.event, d_tag) {
                sqlx::query("DELETE FROM events WHERE community_id = ? AND id = ?")
                    .bind(community_id.as_uuid().to_string())
                    .bind(stored.event.id.as_bytes().as_slice())
                    .execute(&mut *transaction)
                    .await?;
            }
        }
        sqlx::query(
            "UPDATE events SET deleted_at = ? \
             WHERE community_id = ? AND kind = ? AND pubkey = ? AND d_tag = ? \
               AND deleted_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(community_id.as_uuid().to_string())
        .bind(kind)
        .bind(pubkey)
        .bind(d_tag)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Query latest-per-address reminders that are due and not yet claimed.
    pub async fn query_due_reminders(
        &self,
        now_secs: i64,
        batch_limit: i64,
    ) -> Result<Vec<DueReminder>> {
        let rows = sqlx::query(
            "SELECT community_id, host, id, pubkey, created_at, kind, tags, \
                    content, sig, channel_id \
             FROM ( \
                SELECT e.community_id, c.host, e.id, e.pubkey, e.created_at, \
                       e.kind, e.tags, e.content, e.sig, e.channel_id, e.d_tag, \
                       ROW_NUMBER() OVER ( \
                           PARTITION BY e.community_id, e.pubkey, e.d_tag \
                           ORDER BY e.created_at DESC, e.id ASC \
                       ) AS address_rank \
                FROM events e \
                JOIN communities c ON c.id = e.community_id \
                WHERE e.kind = ? AND e.not_before IS NOT NULL \
                  AND e.not_before <= ? AND e.deleted_at IS NULL \
                  AND e.delivered_at IS NULL AND c.archived_at IS NULL \
             ) due \
             WHERE address_rank = 1 \
             ORDER BY community_id, pubkey, d_tag, created_at DESC, id ASC \
             LIMIT ?",
        )
        .bind(KIND_EVENT_REMINDER as i32)
        .bind(now_secs)
        .bind(batch_limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let community_id: String = row.try_get("community_id")?;
                let community_id = Uuid::parse_str(&community_id).map_err(|error| {
                    DbError::InvalidData(format!("invalid reminder community UUID: {error}"))
                })?;
                let channel_id = row
                    .try_get::<Option<String>, _>("channel_id")?
                    .map(|value| {
                        Uuid::parse_str(&value).map_err(|error| {
                            DbError::InvalidData(format!("invalid reminder channel UUID: {error}"))
                        })
                    })
                    .transpose()?;
                Ok(DueReminder {
                    community_id: CommunityId::from_uuid(community_id),
                    host: row.try_get("host")?,
                    id: row.try_get("id")?,
                    pubkey: row.try_get("pubkey")?,
                    created_at: parse_timestamp(row.try_get("created_at")?)?,
                    kind: row.try_get("kind")?,
                    tags: serde_json::from_str(row.try_get::<String, _>("tags")?.as_str())?,
                    content: row.try_get("content")?,
                    sig: row.try_get("sig")?,
                    channel_id,
                })
            })
            .collect()
    }

    /// Atomically claim a due reminder using the current Unix-second stamp.
    pub async fn claim_due_reminder(
        &self,
        community_id: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
    ) -> Result<bool> {
        self.claim_due_reminder_with_stamp(
            community_id,
            event_id,
            event_created_at,
            Utc::now().timestamp(),
        )
        .await
    }

    /// Atomically claim one tenant-scoped reminder with a caller-owned stamp.
    pub async fn claim_due_reminder_with_stamp(
        &self,
        community_id: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        delivery_stamp: i64,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE events SET delivered_at = ? \
             WHERE community_id = ? AND created_at = ? AND id = ? \
               AND delivered_at IS NULL",
        )
        .bind(delivery_stamp)
        .bind(community_id.as_uuid().to_string())
        .bind(event_created_at.timestamp_micros())
        .bind(event_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Compare-and-clear a failed reminder delivery claim.
    pub async fn release_due_reminder(
        &self,
        community_id: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        delivery_stamp: i64,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE events SET delivered_at = NULL \
             WHERE community_id = ? AND created_at = ? AND id = ? \
               AND delivered_at = ?",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(event_created_at.timestamp_micros())
        .bind(event_id)
        .bind(delivery_stamp)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }
}
