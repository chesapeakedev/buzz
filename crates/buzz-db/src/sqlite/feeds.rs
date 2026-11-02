//! SQLite home-feed queries over indexed event and mention columns.

use chrono::{DateTime, Utc};
use sqlx::QueryBuilder;
use uuid::Uuid;

use buzz_core::kind::{
    KIND_FORUM_COMMENT, KIND_FORUM_POST, KIND_GIT_ISSUE, KIND_GIT_PR_UPDATE, KIND_GIT_PULL_REQUEST,
    KIND_GIT_STATUS_CLOSED, KIND_GIT_STATUS_DRAFT, KIND_GIT_STATUS_MERGED, KIND_GIT_STATUS_OPEN,
    KIND_JOB_PROGRESS, KIND_JOB_REQUEST, KIND_JOB_RESULT, KIND_STREAM_MESSAGE,
    KIND_STREAM_MESSAGE_V2, KIND_STREAM_REMINDER, KIND_TEXT_NOTE, KIND_WORKFLOW_APPROVAL_REQUESTED,
};
use buzz_core::{CommunityId, StoredEvent};

use super::{events, SqliteStore};
use crate::feed::FEED_MAX_LIMIT;
use crate::Result;

const MENTION_KINDS: &[u32] = &[
    KIND_STREAM_MESSAGE,
    KIND_STREAM_MESSAGE_V2,
    KIND_TEXT_NOTE,
    KIND_FORUM_POST,
    KIND_FORUM_COMMENT,
    KIND_GIT_PULL_REQUEST,
    KIND_GIT_PR_UPDATE,
    KIND_GIT_ISSUE,
    KIND_GIT_STATUS_OPEN,
    KIND_GIT_STATUS_MERGED,
    KIND_GIT_STATUS_CLOSED,
    KIND_GIT_STATUS_DRAFT,
];

const NEEDS_ACTION_KINDS: &[u32] = &[KIND_WORKFLOW_APPROVAL_REQUESTED, KIND_STREAM_REMINDER];

const ACTIVITY_KINDS: &[u32] = &[
    KIND_STREAM_MESSAGE,
    KIND_STREAM_MESSAGE_V2,
    KIND_FORUM_POST,
    KIND_JOB_REQUEST,
    KIND_JOB_PROGRESS,
    KIND_JOB_RESULT,
];

fn push_visible_channels(
    builder: &mut QueryBuilder<sqlx::Sqlite>,
    column: &str,
    channels: &[Uuid],
) {
    if channels.is_empty() {
        builder.push(format!(" AND {column} IS NULL"));
        return;
    }
    builder.push(format!(" AND ({column} IS NULL OR {column} IN ("));
    let mut separated = builder.separated(", ");
    for channel in channels {
        separated.push_bind(channel.to_string());
    }
    builder.push("))");
}

fn push_kinds(builder: &mut QueryBuilder<sqlx::Sqlite>, column: &str, kinds: &[u32]) {
    builder.push(format!(" AND {column} IN ("));
    let mut separated = builder.separated(", ");
    for kind in kinds {
        separated.push_bind(i64::from(*kind));
    }
    builder.push(")");
}

async fn collect(
    builder: &mut QueryBuilder<sqlx::Sqlite>,
    store: &SqliteStore,
) -> Result<Vec<StoredEvent>> {
    let rows = builder.build().fetch_all(&store.pool).await?;
    let mut events_out = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(event) = events::row_to_stored_event(row)? {
            events_out.push(event);
        }
    }
    Ok(events_out)
}

impl SqliteStore {
    /// Find allowed event kinds that mention a pubkey in visible channels.
    pub async fn query_feed_mentions(
        &self,
        community: CommunityId,
        pubkey_bytes: &[u8],
        accessible_channel_ids: &[Uuid],
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<StoredEvent>> {
        let community = community.as_uuid().to_string();
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags, e.content, \
             e.sig, e.received_at, e.channel_id \
             FROM events e INNER JOIN event_mentions m \
               ON e.community_id = m.community_id AND e.id = m.event_id \
             WHERE e.community_id = ",
        );
        builder
            .push_bind(&community)
            .push(" AND m.community_id = ")
            .push_bind(&community)
            .push(" AND m.pubkey_hex = ")
            .push_bind(hex::encode(pubkey_bytes))
            .push(" AND e.deleted_at IS NULL");
        push_kinds(&mut builder, "e.kind", MENTION_KINDS);
        push_visible_channels(&mut builder, "e.channel_id", accessible_channel_ids);
        if let Some(timestamp) = since {
            builder
                .push(" AND m.event_created_at >= ")
                .push_bind(timestamp.timestamp_micros());
        }
        builder
            .push(" ORDER BY m.event_created_at DESC LIMIT ")
            .push_bind(limit.clamp(0, FEED_MAX_LIMIT));
        collect(&mut builder, self).await
    }

    /// Find approval and reminder events addressed to a pubkey in visible channels.
    pub async fn query_feed_needs_action(
        &self,
        community: CommunityId,
        pubkey_bytes: &[u8],
        accessible_channel_ids: &[Uuid],
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<StoredEvent>> {
        let community = community.as_uuid().to_string();
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT e.id, e.pubkey, e.created_at, e.kind, e.tags, e.content, \
             e.sig, e.received_at, e.channel_id \
             FROM events e INNER JOIN event_mentions m \
               ON e.community_id = m.community_id AND e.id = m.event_id \
             WHERE e.community_id = ",
        );
        builder
            .push_bind(&community)
            .push(" AND m.community_id = ")
            .push_bind(&community)
            .push(" AND m.pubkey_hex = ")
            .push_bind(hex::encode(pubkey_bytes))
            .push(" AND e.deleted_at IS NULL");
        push_kinds(&mut builder, "e.kind", NEEDS_ACTION_KINDS);
        push_visible_channels(&mut builder, "e.channel_id", accessible_channel_ids);
        if let Some(timestamp) = since {
            builder
                .push(" AND m.event_created_at >= ")
                .push_bind(timestamp.timestamp_micros());
        }
        builder
            .push(" ORDER BY m.event_created_at DESC LIMIT ")
            .push_bind(limit.clamp(0, FEED_MAX_LIMIT));
        collect(&mut builder, self).await
    }

    /// Find recent activity kinds from global and accessible channels.
    pub async fn query_feed_activity(
        &self,
        community: CommunityId,
        accessible_channel_ids: &[Uuid],
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<StoredEvent>> {
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT id, pubkey, created_at, kind, tags, content, sig, \
             received_at, channel_id FROM events WHERE community_id = ",
        );
        builder
            .push_bind(community.as_uuid().to_string())
            .push(" AND deleted_at IS NULL");
        push_kinds(&mut builder, "kind", ACTIVITY_KINDS);
        push_visible_channels(&mut builder, "channel_id", accessible_channel_ids);
        if let Some(timestamp) = since {
            builder
                .push(" AND created_at >= ")
                .push_bind(timestamp.timestamp_micros());
        }
        builder
            .push(" ORDER BY created_at DESC LIMIT ")
            .push_bind(limit.clamp(0, FEED_MAX_LIMIT));
        collect(&mut builder, self).await
    }
}
