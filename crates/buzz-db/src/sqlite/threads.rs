//! SQLite thread metadata, materialized counters, and reply queries.

use chrono::{DateTime, Utc};
use nostr::Event;
use sqlx::{QueryBuilder, Row as _};
use uuid::Uuid;

use buzz_core::{CommunityId, StoredEvent};

use super::{events, SqliteStore};
use crate::event::ThreadMetadataParams;
use crate::thread::{
    ChannelWindow, ChannelWindowRow, ThreadMetadataRecord, ThreadReply, ThreadSummary,
};
use crate::{DbError, Result};

fn timestamp_micros(timestamp: DateTime<Utc>) -> i64 {
    timestamp.timestamp_micros()
}

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or(DbError::InvalidTimestamp(value))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_thread_metadata_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    community_id: CommunityId,
    event_id: &[u8],
    event_created_at: DateTime<Utc>,
    channel_id: Uuid,
    parent_event_id: Option<&[u8]>,
    parent_event_created_at: Option<DateTime<Utc>>,
    root_event_id: Option<&[u8]>,
    root_event_created_at: Option<DateTime<Utc>>,
    depth: i32,
    broadcast: bool,
) -> Result<()> {
    let community = community_id.as_uuid().to_string();
    let result = sqlx::query(
        "INSERT INTO thread_metadata \
         (community_id, event_created_at, event_id, channel_id, \
          parent_event_id, parent_event_created_at, \
          root_event_id, root_event_created_at, depth, broadcast) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&community)
    .bind(timestamp_micros(event_created_at))
    .bind(event_id)
    .bind(channel_id.to_string())
    .bind(parent_event_id)
    .bind(parent_event_created_at.map(timestamp_micros))
    .bind(root_event_id)
    .bind(root_event_created_at.map(timestamp_micros))
    .bind(depth)
    .bind(broadcast)
    .execute(&mut **transaction)
    .await?;

    if result.rows_affected() == 0 {
        return Ok(());
    }

    let Some(parent_id) = parent_event_id else {
        return Ok(());
    };
    let parent_created_at = parent_event_created_at.unwrap_or(event_created_at);
    sqlx::query(
        "INSERT INTO thread_metadata \
         (community_id, event_created_at, event_id, channel_id, depth, broadcast) \
         VALUES (?, ?, ?, ?, 0, 0) ON CONFLICT DO NOTHING",
    )
    .bind(&community)
    .bind(timestamp_micros(parent_created_at))
    .bind(parent_id)
    .bind(channel_id.to_string())
    .execute(&mut **transaction)
    .await?;

    if let Some(root_id) = root_event_id.filter(|root_id| *root_id != parent_id) {
        let root_created_at = root_event_created_at.unwrap_or(event_created_at);
        sqlx::query(
            "INSERT INTO thread_metadata \
             (community_id, event_created_at, event_id, channel_id, depth, broadcast) \
             VALUES (?, ?, ?, ?, 0, 0) ON CONFLICT DO NOTHING",
        )
        .bind(&community)
        .bind(timestamp_micros(root_created_at))
        .bind(root_id)
        .bind(channel_id.to_string())
        .execute(&mut **transaction)
        .await?;
    }

    sqlx::query(
        "UPDATE thread_metadata \
         SET reply_count = reply_count + 1, last_reply_at = ? \
         WHERE community_id = ? AND event_id = ?",
    )
    .bind(Utc::now().timestamp_micros())
    .bind(&community)
    .bind(parent_id)
    .execute(&mut **transaction)
    .await?;

    if let Some(root_id) = root_event_id {
        sqlx::query(
            "UPDATE thread_metadata \
             SET descendant_count = descendant_count + 1 \
             WHERE community_id = ? AND event_id = ?",
        )
        .bind(&community)
        .bind(root_id)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(())
}

impl SqliteStore {
    /// Insert thread metadata and update its parent/root counters atomically.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_thread_metadata(
        &self,
        community_id: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        channel_id: Uuid,
        parent_event_id: Option<&[u8]>,
        parent_event_created_at: Option<DateTime<Utc>>,
        root_event_id: Option<&[u8]>,
        root_event_created_at: Option<DateTime<Utc>>,
        depth: i32,
        broadcast: bool,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        insert_thread_metadata_transaction(
            &mut transaction,
            community_id,
            event_id,
            event_created_at,
            channel_id,
            parent_event_id,
            parent_event_created_at,
            root_event_id,
            root_event_created_at,
            depth,
            broadcast,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically insert an event, optional thread metadata, and counter updates.
    pub async fn insert_event_with_thread_metadata(
        &self,
        community_id: CommunityId,
        event: &Event,
        channel_id: Option<Uuid>,
        thread_meta: Option<ThreadMetadataParams<'_>>,
    ) -> Result<(StoredEvent, bool)> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let result =
            events::insert_event_transaction(&mut transaction, community_id, event, channel_id)
                .await?;

        if result.1 {
            if let Some(metadata) = thread_meta {
                insert_thread_metadata_transaction(
                    &mut transaction,
                    community_id,
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
        Ok(result)
    }

    /// Fetch replies under a root in chronological composite-key order.
    pub async fn get_thread_replies(
        &self,
        community_id: CommunityId,
        root_event_id: &[u8],
        depth_limit: Option<u32>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> Result<Vec<ThreadReply>> {
        let cursor = cursor.and_then(|bytes| {
            let seconds = bytes.get(..8)?;
            let seconds = i64::from_be_bytes(seconds.try_into().ok()?);
            let timestamp = seconds.checked_mul(1_000_000)?;
            let event_id = (bytes.len() > 8).then(|| bytes[8..].to_vec());
            Some((timestamp, event_id))
        });
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT tm.event_id, tm.parent_event_id, tm.root_event_id, \
             tm.channel_id, tm.depth, tm.event_created_at, tm.broadcast, \
             e.id, e.pubkey, e.created_at, e.kind, e.tags, e.content, e.sig, \
             e.received_at, e.channel_id \
             FROM thread_metadata tm JOIN events e \
               ON e.community_id = tm.community_id \
              AND e.created_at = tm.event_created_at \
              AND e.id = tm.event_id \
             WHERE tm.community_id = ",
        );
        builder
            .push_bind(community_id.as_uuid().to_string())
            .push(" AND tm.root_event_id = ")
            .push_bind(root_event_id.to_vec())
            .push(" AND e.deleted_at IS NULL");
        if let Some(depth) = depth_limit {
            builder
                .push(" AND tm.depth <= ")
                .push_bind(i64::from(depth));
        }
        match cursor {
            Some((timestamp, Some(event_id))) => {
                builder
                    .push(" AND (tm.event_created_at > ")
                    .push_bind(timestamp)
                    .push(" OR (tm.event_created_at = ")
                    .push_bind(timestamp)
                    .push(" AND tm.event_id > ")
                    .push_bind(event_id)
                    .push("))");
            }
            Some((timestamp, None)) => {
                builder
                    .push(" AND tm.event_created_at > ")
                    .push_bind(timestamp);
            }
            None => {}
        }
        builder
            .push(" ORDER BY tm.event_created_at ASC, tm.event_id ASC LIMIT ")
            .push_bind(i64::from(limit));

        let rows = builder.build().fetch_all(&self.pool).await?;
        let mut replies = Vec::with_capacity(rows.len());
        for row in rows {
            let event_id = row.try_get("event_id")?;
            let parent_event_id = row.try_get("parent_event_id")?;
            let root_event_id = row.try_get("root_event_id")?;
            let channel_id: String = row.try_get("channel_id")?;
            let depth = i32::try_from(row.try_get::<i64, _>("depth")?)
                .map_err(|_| DbError::InvalidData("thread depth out of i32 range".to_owned()))?;
            let created_at = parse_timestamp(row.try_get("event_created_at")?)?;
            let broadcast = row.try_get::<bool, _>("broadcast")?;
            let pubkey = row.try_get("pubkey")?;
            let tags: String = row.try_get("tags")?;
            let tags = serde_json::from_str(&tags)?;
            let Some(stored_event) = events::row_to_stored_event(row)? else {
                continue;
            };
            replies.push(ThreadReply {
                event_id,
                parent_event_id,
                root_event_id,
                channel_id: Uuid::parse_str(&channel_id).map_err(|error| {
                    DbError::InvalidData(format!("thread channel UUID: {error}"))
                })?,
                pubkey,
                tags,
                content: stored_event.event.content.clone(),
                stored_event,
                depth,
                created_at,
                broadcast,
            });
        }
        Ok(replies)
    }

    /// Fetch materialized counters and the ten most recent thread participants.
    pub async fn get_thread_summary(
        &self,
        community_id: CommunityId,
        event_id: &[u8],
    ) -> Result<Option<ThreadSummary>> {
        let community = community_id.as_uuid().to_string();
        let Some(row) = sqlx::query(
            "SELECT reply_count, descendant_count, last_reply_at \
             FROM thread_metadata WHERE community_id = ? AND event_id = ? LIMIT 1",
        )
        .bind(&community)
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let reply_count = i32::try_from(row.try_get::<i64, _>("reply_count")?)
            .map_err(|_| DbError::InvalidData("reply count out of i32 range".to_owned()))?;
        let descendant_count = i32::try_from(row.try_get::<i64, _>("descendant_count")?)
            .map_err(|_| DbError::InvalidData("descendant count out of i32 range".to_owned()))?;
        let last_reply_at = row
            .try_get::<Option<i64>, _>("last_reply_at")?
            .map(parse_timestamp)
            .transpose()?;
        let participants = sqlx::query_scalar(
            "SELECT pubkey FROM ( \
               SELECT e.pubkey AS pubkey, MAX(e.created_at) AS last_seen \
               FROM thread_metadata tm JOIN events e \
                 ON e.community_id = tm.community_id \
                AND e.created_at = tm.event_created_at \
                AND e.id = tm.event_id \
               WHERE tm.community_id = ? AND tm.root_event_id = ? \
                 AND e.deleted_at IS NULL GROUP BY e.pubkey \
             ) ORDER BY last_seen DESC LIMIT 10",
        )
        .bind(&community)
        .bind(event_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(Some(ThreadSummary {
            reply_count,
            descendant_count,
            last_reply_at,
            participants,
        }))
    }

    /// Fetch one top-level channel window with thread summaries and exhaustion evidence.
    pub async fn get_channel_window(
        &self,
        community_id: CommunityId,
        channel_id: Uuid,
        limit: u32,
        cursor: Option<(DateTime<Utc>, Vec<u8>)>,
        kind_filter: Option<&[u32]>,
    ) -> Result<ChannelWindow> {
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags, e.content, \
             e.sig, e.received_at, e.channel_id, tm.reply_count, \
             tm.descendant_count, tm.last_reply_at \
             FROM events e LEFT JOIN thread_metadata tm \
               ON tm.community_id = e.community_id \
              AND tm.event_created_at = e.created_at \
              AND tm.event_id = e.id \
             WHERE e.community_id = ",
        );
        builder
            .push_bind(community_id.as_uuid().to_string())
            .push(" AND e.channel_id = ")
            .push_bind(channel_id.to_string())
            .push(
                " AND e.deleted_at IS NULL AND ( \
                    tm.depth IS NULL OR tm.depth = 0 \
                    OR (tm.depth = 1 AND tm.broadcast = 1) \
                  )",
            );
        if let Some((timestamp, event_id)) = &cursor {
            let timestamp = timestamp.timestamp_micros();
            builder
                .push(" AND (e.created_at < ")
                .push_bind(timestamp)
                .push(" OR (e.created_at = ")
                .push_bind(timestamp)
                .push(" AND e.id > ")
                .push_bind(event_id.clone())
                .push("))");
        }
        if let Some(kinds) = kind_filter.filter(|kinds| !kinds.is_empty()) {
            builder.push(" AND e.kind IN (");
            let mut separated = builder.separated(", ");
            for kind in kinds {
                separated.push_bind(i64::from(*kind));
            }
            builder.push(")");
        }
        builder
            .push(" ORDER BY e.created_at DESC, e.id ASC LIMIT ")
            .push_bind(i64::from(limit) + 1);

        let mut database_rows = builder.build().fetch_all(&self.pool).await?;
        let has_more = database_rows.len() > limit as usize;
        database_rows.truncate(limit as usize);
        let next_cursor = if has_more {
            match database_rows.last() {
                Some(row) => Some((
                    parse_timestamp(row.try_get("created_at")?)?,
                    row.try_get("id")?,
                )),
                None => None,
            }
        } else {
            None
        };

        let mut rows = Vec::with_capacity(database_rows.len());
        for row in database_rows {
            let reply_count = row
                .try_get::<Option<i64>, _>("reply_count")?
                .map(i32::try_from)
                .transpose()
                .map_err(|_| DbError::InvalidData("reply count out of i32 range".to_owned()))?;
            let descendant_count = row
                .try_get::<Option<i64>, _>("descendant_count")?
                .map(i32::try_from)
                .transpose()
                .map_err(|_| {
                    DbError::InvalidData("descendant count out of i32 range".to_owned())
                })?;
            let last_reply_at = row
                .try_get::<Option<i64>, _>("last_reply_at")?
                .map(parse_timestamp)
                .transpose()?;
            let Some(stored_event) = events::row_to_stored_event(row)? else {
                continue;
            };
            let thread_summary =
                reply_count
                    .filter(|count| *count > 0)
                    .map(|count| ThreadSummary {
                        reply_count: count,
                        descendant_count: descendant_count.unwrap_or(0),
                        last_reply_at,
                        participants: Vec::new(),
                    });
            rows.push(ChannelWindowRow {
                stored_event,
                thread_summary,
            });
        }

        let roots: Vec<Vec<u8>> = rows
            .iter()
            .filter(|row| row.thread_summary.is_some())
            .map(|row| row.stored_event.event.id.as_bytes().to_vec())
            .collect();
        if !roots.is_empty() {
            let mut participants: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
                "SELECT root_event_id, pubkey FROM ( \
                   SELECT tm.root_event_id, e.pubkey, MAX(e.created_at) AS last_seen, \
                     ROW_NUMBER() OVER ( \
                       PARTITION BY tm.root_event_id \
                       ORDER BY MAX(e.created_at) DESC \
                     ) AS row_number \
                   FROM thread_metadata tm JOIN events e \
                     ON e.community_id = tm.community_id \
                    AND e.created_at = tm.event_created_at \
                    AND e.id = tm.event_id \
                   WHERE tm.community_id = ",
            );
            participants
                .push_bind(community_id.as_uuid().to_string())
                .push(" AND tm.root_event_id IN (");
            let mut separated = participants.separated(", ");
            for root in &roots {
                separated.push_bind(root.clone());
            }
            participants.push(
                ") AND e.deleted_at IS NULL \
                 GROUP BY tm.root_event_id, e.pubkey \
                 ) WHERE row_number <= 10 ORDER BY root_event_id, row_number",
            );
            let participant_rows = participants.build().fetch_all(&self.pool).await?;
            let mut by_root: std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>> =
                std::collections::HashMap::new();
            for row in participant_rows {
                by_root
                    .entry(row.try_get("root_event_id")?)
                    .or_default()
                    .push(row.try_get("pubkey")?);
            }
            for row in &mut rows {
                if let Some(summary) = &mut row.thread_summary {
                    if let Some(root_participants) =
                        by_root.remove(row.stored_event.event.id.as_bytes().as_slice())
                    {
                        summary.participants = root_participants;
                    }
                }
            }
        }

        Ok(ChannelWindow {
            rows,
            has_more,
            next_cursor,
        })
    }

    /// Look up one tenant-scoped thread metadata row by event identifier.
    pub async fn get_thread_metadata_by_event(
        &self,
        community_id: CommunityId,
        event_id: &[u8],
    ) -> Result<Option<ThreadMetadataRecord>> {
        let Some(row) = sqlx::query(
            "SELECT event_id, event_created_at, channel_id, parent_event_id, \
             root_event_id, depth, reply_count, descendant_count, broadcast \
             FROM thread_metadata WHERE community_id = ? AND event_id = ? LIMIT 1",
        )
        .bind(community_id.as_uuid().to_string())
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let channel_id: String = row.try_get("channel_id")?;
        Ok(Some(ThreadMetadataRecord {
            event_id: row.try_get("event_id")?,
            event_created_at: parse_timestamp(row.try_get("event_created_at")?)?,
            channel_id: Uuid::parse_str(&channel_id)
                .map_err(|error| DbError::InvalidData(format!("thread channel UUID: {error}")))?,
            parent_event_id: row.try_get("parent_event_id")?,
            root_event_id: row.try_get("root_event_id")?,
            depth: i32::try_from(row.try_get::<i64, _>("depth")?)
                .map_err(|_| DbError::InvalidData("thread depth out of i32 range".to_owned()))?,
            reply_count: i32::try_from(row.try_get::<i64, _>("reply_count")?)
                .map_err(|_| DbError::InvalidData("reply count out of i32 range".to_owned()))?,
            descendant_count: i32::try_from(row.try_get::<i64, _>("descendant_count")?).map_err(
                |_| DbError::InvalidData("descendant count out of i32 range".to_owned()),
            )?,
            broadcast: row.try_get("broadcast")?,
        }))
    }

    /// Decrement direct and descendant counters, flooring both at zero.
    pub async fn decrement_reply_count(
        &self,
        community_id: CommunityId,
        parent_event_id: &[u8],
        root_event_id: Option<&[u8]>,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let community = community_id.as_uuid().to_string();
        sqlx::query(
            "UPDATE thread_metadata SET reply_count = max(reply_count - 1, 0) \
             WHERE community_id = ? AND event_id = ?",
        )
        .bind(&community)
        .bind(parent_event_id)
        .execute(&mut *transaction)
        .await?;
        if let Some(root_id) = root_event_id {
            sqlx::query(
                "UPDATE thread_metadata \
                 SET descendant_count = max(descendant_count - 1, 0) \
                 WHERE community_id = ? AND event_id = ?",
            )
            .bind(&community)
            .bind(root_id)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}
