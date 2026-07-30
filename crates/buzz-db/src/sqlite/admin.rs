//! SQLite projections for the deployment-global private admin plane.

use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row as _};
use uuid::Uuid;

use super::SqliteStore;
use crate::admin_moderation::{
    bounded_limit, AdminFeedback, AdminReport, AdminReportDetail, AdminReportedMessage,
};
use crate::{DbError, Result};

fn parse_uuid(value: String, column: &str) -> Result<Uuid> {
    Uuid::parse_str(&value)
        .map_err(|error| DbError::InvalidData(format!("invalid {column} UUID: {error}")))
}

fn parse_optional_uuid(value: Option<String>, column: &str) -> Result<Option<Uuid>> {
    value.map(|value| parse_uuid(value, column)).transpose()
}

fn parse_timestamp(value: i64, column: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value)
        .ok_or_else(|| DbError::InvalidData(format!("invalid {column} timestamp: {value}")))
}

fn parse_optional_timestamp(value: Option<i64>, column: &str) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|value| parse_timestamp(value, column))
        .transpose()
}

fn row_to_report(row: &SqliteRow) -> Result<AdminReport> {
    let target_kind: String = row.try_get("target_kind")?;
    let target = match target_kind.as_str() {
        "event" => row.try_get::<Vec<u8>, _>("target_event_id")?,
        "pubkey" => row.try_get::<Vec<u8>, _>("target_pubkey")?,
        "blob" => row.try_get::<Vec<u8>, _>("target_blob_sha256")?,
        _ => Vec::new(),
    };
    Ok(AdminReport {
        id: parse_uuid(row.try_get("id")?, "report")?,
        community_id: parse_uuid(row.try_get("community_id")?, "community")?,
        community_host: row.try_get("community_host")?,
        report_event_id: hex::encode(row.try_get::<Vec<u8>, _>("report_event_id")?),
        reporter_pubkey: hex::encode(row.try_get::<Vec<u8>, _>("reporter_pubkey")?),
        target_kind,
        target: hex::encode(target),
        channel_id: parse_optional_uuid(row.try_get("channel_id")?, "channel")?,
        report_type: row.try_get("report_type")?,
        note: row.try_get("note")?,
        status: row.try_get("status")?,
        resolved_by: row
            .try_get::<Option<Vec<u8>>, _>("resolved_by")?
            .map(hex::encode),
        resolved_at: parse_optional_timestamp(row.try_get("resolved_at")?, "resolved_at")?,
        action_id: parse_optional_uuid(row.try_get("action_id")?, "action")?,
        created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
    })
}

fn row_to_feedback(row: &SqliteRow) -> Result<AdminFeedback> {
    Ok(AdminFeedback {
        id: parse_uuid(row.try_get("id")?, "feedback")?,
        community_id: parse_uuid(row.try_get("community_id")?, "community")?,
        community_host: row.try_get("community_host")?,
        event_id: hex::encode(row.try_get::<Vec<u8>, _>("event_id")?),
        submitter_pubkey: hex::encode(row.try_get::<Vec<u8>, _>("submitter_pubkey")?),
        category: row.try_get("category")?,
        body: row.try_get("body")?,
        tags: serde_json::from_str(row.try_get::<String, _>("tags")?.as_str())?,
        event_created_at: parse_timestamp(row.try_get("event_created_at")?, "event_created_at")?,
        received_at: parse_timestamp(row.try_get("received_at")?, "received_at")?,
    })
}

impl SqliteStore {
    /// List reports across all communities using the admin keyset order.
    #[allow(clippy::too_many_arguments)]
    pub async fn admin_list_reports(
        &self,
        community_id: Option<Uuid>,
        status: Option<&str>,
        report_type: Option<&str>,
        target_kind: Option<&str>,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<AdminReport>> {
        let (cursor_time, cursor_id) = cursor.unzip();
        let rows = sqlx::query(
            "SELECT r.id, r.community_id, c.host AS community_host, \
                    r.report_event_id, r.reporter_pubkey, r.target_kind, \
                    r.target_event_id, r.target_pubkey, r.target_blob_sha256, \
                    r.channel_id, r.report_type, r.note, r.status, r.resolved_by, \
                    r.resolved_at, r.action_id, r.created_at \
             FROM moderation_reports r \
             JOIN communities c ON c.id = r.community_id \
             WHERE (? IS NULL OR r.community_id = ?) \
               AND (? IS NULL OR r.status = ?) \
               AND (? IS NULL OR r.report_type = ?) \
               AND (? IS NULL OR r.target_kind = ?) \
               AND (? IS NULL OR r.created_at >= ?) \
               AND (? IS NULL OR r.created_at < ?) \
               AND (? IS NULL OR (r.created_at, r.id) < (?, ?)) \
             ORDER BY r.created_at DESC, r.id DESC LIMIT ?",
        )
        .bind(community_id.map(|id| id.to_string()))
        .bind(community_id.map(|id| id.to_string()))
        .bind(status)
        .bind(status)
        .bind(report_type)
        .bind(report_type)
        .bind(target_kind)
        .bind(target_kind)
        .bind(after.map(|time| time.timestamp_micros()))
        .bind(after.map(|time| time.timestamp_micros()))
        .bind(before.map(|time| time.timestamp_micros()))
        .bind(before.map(|time| time.timestamp_micros()))
        .bind(cursor_time.map(|time| time.timestamp_micros()))
        .bind(cursor_time.map(|time| time.timestamp_micros()))
        .bind(cursor_id.map(|id| id.to_string()))
        .bind(bounded_limit(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_report).collect()
    }

    /// Fetch one globally addressed report and its same-community event target.
    pub async fn admin_get_report(&self, id: Uuid) -> Result<Option<AdminReportDetail>> {
        let row = sqlx::query(
            "SELECT r.id, r.community_id, c.host AS community_host, \
                    r.report_event_id, r.reporter_pubkey, r.target_kind, \
                    r.target_event_id, r.target_pubkey, r.target_blob_sha256, \
                    r.channel_id, r.report_type, r.note, r.status, r.resolved_by, \
                    r.resolved_at, r.action_id, r.created_at, \
                    e.pubkey AS message_author_pubkey, \
                    e.content AS message_content, \
                    e.created_at AS message_created_at, \
                    e.deleted_at AS message_deleted_at \
             FROM moderation_reports r \
             JOIN communities c ON c.id = r.community_id \
             LEFT JOIN events e \
               ON r.target_kind = 'event' \
              AND e.community_id = r.community_id \
              AND e.id = r.target_event_id \
             WHERE r.id = ? LIMIT 1",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let message = row
                .try_get::<Option<Vec<u8>>, _>("message_author_pubkey")?
                .map(|author_pubkey| -> Result<AdminReportedMessage> {
                    Ok(AdminReportedMessage {
                        author_pubkey: hex::encode(author_pubkey),
                        content: row.try_get("message_content")?,
                        created_at: parse_timestamp(
                            row.try_get("message_created_at")?,
                            "message_created_at",
                        )?,
                        deleted_at: parse_optional_timestamp(
                            row.try_get("message_deleted_at")?,
                            "message_deleted_at",
                        )?,
                    })
                })
                .transpose()?;
            Ok(AdminReportDetail {
                report: row_to_report(&row)?,
                message,
            })
        })
        .transpose()
    }

    /// List feedback across all communities in the private admin plane.
    pub async fn admin_list_feedback(&self, limit: i64) -> Result<Vec<AdminFeedback>> {
        let rows = sqlx::query(
            "SELECT f.id, f.community_id, c.host AS community_host, f.event_id, \
                    f.submitter_pubkey, f.category, f.body, f.tags, \
                    f.event_created_at, f.received_at \
             FROM product_feedback f \
             JOIN communities c ON c.id = f.community_id \
             ORDER BY f.received_at DESC, f.id DESC LIMIT ?",
        )
        .bind(bounded_limit(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_feedback).collect()
    }

    /// Fetch one feedback submission from the private admin plane.
    pub async fn admin_get_feedback(&self, id: Uuid) -> Result<Option<AdminFeedback>> {
        let row = sqlx::query(
            "SELECT f.id, f.community_id, c.host AS community_host, f.event_id, \
                    f.submitter_pubkey, f.category, f.body, f.tags, \
                    f.event_created_at, f.received_at \
             FROM product_feedback f \
             JOIN communities c ON c.id = f.community_id WHERE f.id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_feedback).transpose()
    }
}
