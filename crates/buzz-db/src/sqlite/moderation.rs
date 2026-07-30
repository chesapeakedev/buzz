//! SQLite community-moderation persistence.

use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row as _};
use uuid::Uuid;

use buzz_core::CommunityId;

use super::SqliteStore;
use crate::moderation::{
    ActionRecord, BanRecord, NewAction, NewReport, ReportRecord, ReportTarget, RestrictionState,
};
use crate::{DbError, Result};

const REPORT_COLUMNS: &str = "id, report_event_id, reporter_pubkey, target_kind, \
    target_event_id, target_pubkey, target_blob_sha256, channel_id, report_type, \
    note, status, resolved_by, resolved_at, action_id, created_at";

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

fn row_to_report(row: SqliteRow) -> Result<ReportRecord> {
    let target_kind: String = row.try_get("target_kind")?;
    let target = match target_kind.as_str() {
        "event" => ReportTarget::Event(row.try_get("target_event_id")?),
        "pubkey" => ReportTarget::Pubkey(row.try_get("target_pubkey")?),
        "blob" => ReportTarget::Blob(row.try_get("target_blob_sha256")?),
        other => {
            return Err(DbError::InvalidData(format!(
                "invalid report target_kind: {other}"
            )))
        }
    };

    Ok(ReportRecord {
        id: parse_uuid(row.try_get("id")?, "moderation report")?,
        report_event_id: row.try_get("report_event_id")?,
        reporter_pubkey: row.try_get("reporter_pubkey")?,
        target,
        channel_id: parse_optional_uuid(row.try_get("channel_id")?, "channel")?,
        report_type: row.try_get("report_type")?,
        note: row.try_get("note")?,
        status: row.try_get("status")?,
        resolved_by: row.try_get("resolved_by")?,
        resolved_at: parse_optional_timestamp(row.try_get("resolved_at")?, "resolved_at")?,
        action_id: parse_optional_uuid(row.try_get("action_id")?, "moderation action")?,
        created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
    })
}

fn row_to_ban(row: SqliteRow) -> Result<BanRecord> {
    Ok(BanRecord {
        pubkey: row.try_get("pubkey")?,
        banned: row.try_get::<i64, _>("banned")? != 0,
        ban_expires_at: parse_optional_timestamp(row.try_get("ban_expires_at")?, "ban_expires_at")?,
        ban_reason: row.try_get("ban_reason")?,
        muted_until: parse_optional_timestamp(row.try_get("muted_until")?, "muted_until")?,
        mute_reason: row.try_get("mute_reason")?,
        actor_pubkey: row.try_get("actor_pubkey")?,
        updated_at: parse_timestamp(row.try_get("updated_at")?, "updated_at")?,
    })
}

fn row_to_action(row: SqliteRow) -> Result<ActionRecord> {
    Ok(ActionRecord {
        id: parse_uuid(row.try_get("id")?, "moderation action")?,
        actor_pubkey: row.try_get("actor_pubkey")?,
        action: row.try_get("action")?,
        target_pubkey: row.try_get("target_pubkey")?,
        target_event_id: row.try_get("target_event_id")?,
        channel_id: parse_optional_uuid(row.try_get("channel_id")?, "channel")?,
        reason_code: row.try_get("reason_code")?,
        public_reason: row.try_get("public_reason")?,
        private_reason: row.try_get("private_reason")?,
        matched_principal: row.try_get("matched_principal")?,
        created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
    })
}

impl SqliteStore {
    /// Insert a tenant-scoped report idempotently by signed report event ID.
    pub async fn insert_moderation_report(
        &self,
        community: CommunityId,
        report: NewReport<'_>,
    ) -> Result<Uuid> {
        let _writer = self.acquire_writer().await;
        let (target_kind, target_event_id, target_pubkey, target_blob_sha256) = match &report.target
        {
            ReportTarget::Event(id) => ("event", Some(id.as_slice()), None, None),
            ReportTarget::Pubkey(pubkey) => ("pubkey", None, Some(pubkey.as_slice()), None),
            ReportTarget::Blob(sha256) => ("blob", None, None, Some(sha256.as_slice())),
        };
        let id = Uuid::new_v4().to_string();
        let row = sqlx::query(
            "INSERT INTO moderation_reports ( \
                community_id, id, report_event_id, reporter_pubkey, target_kind, \
                target_event_id, target_pubkey, target_blob_sha256, channel_id, \
                report_type, note, created_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, report_event_id) DO UPDATE SET \
                report_event_id = excluded.report_event_id \
             RETURNING id",
        )
        .bind(community.as_uuid().to_string())
        .bind(id)
        .bind(report.report_event_id)
        .bind(report.reporter_pubkey)
        .bind(target_kind)
        .bind(target_event_id)
        .bind(target_pubkey)
        .bind(target_blob_sha256)
        .bind(report.channel_id.map(|id| id.to_string()))
        .bind(report.report_type)
        .bind(report.note)
        .bind(Utc::now().timestamp_micros())
        .fetch_one(&self.pool)
        .await?;

        parse_uuid(row.try_get("id")?, "moderation report")
    }

    /// List one community's reports, optionally filtered by status.
    pub async fn list_moderation_reports(
        &self,
        community: CommunityId,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ReportRecord>> {
        let sql = format!(
            "SELECT {REPORT_COLUMNS} FROM moderation_reports \
             WHERE community_id = ? AND (? IS NULL OR status = ?) \
             ORDER BY created_at DESC LIMIT ?"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(status)
            .bind(status)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(row_to_report).collect()
    }

    /// Fetch one community's report by row ID.
    pub async fn get_moderation_report(
        &self,
        community: CommunityId,
        report_id: Uuid,
    ) -> Result<Option<ReportRecord>> {
        let sql = format!(
            "SELECT {REPORT_COLUMNS} FROM moderation_reports \
             WHERE community_id = ? AND id = ?"
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(report_id.to_string())
            .fetch_optional(&self.pool)
            .await?;

        row.map(row_to_report).transpose()
    }

    /// Fetch one community's report by signed report event ID.
    pub async fn get_moderation_report_by_event(
        &self,
        community: CommunityId,
        report_event_id: &[u8],
    ) -> Result<Option<ReportRecord>> {
        let sql = format!(
            "SELECT {REPORT_COLUMNS} FROM moderation_reports \
             WHERE community_id = ? AND report_event_id = ?"
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(report_event_id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(row_to_report).transpose()
    }

    /// Guardedly resolve, dismiss, or escalate an open report.
    pub async fn resolve_moderation_report(
        &self,
        community: CommunityId,
        report_id: Uuid,
        status: &str,
        resolved_by: &[u8],
        action_id: Option<Uuid>,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE moderation_reports \
             SET status = ?, resolved_by = ?, resolved_at = ?, action_id = ? \
             WHERE community_id = ? AND id = ? AND status = 'open'",
        )
        .bind(status)
        .bind(resolved_by)
        .bind(Utc::now().timestamp_micros())
        .bind(action_id.map(|id| id.to_string()))
        .bind(community.as_uuid().to_string())
        .bind(report_id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Upsert a tenant-scoped member ban.
    pub async fn ban_community_member(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        actor: &[u8],
        reason: Option<&str>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "INSERT INTO community_bans ( \
                community_id, pubkey, banned, ban_expires_at, ban_reason, \
                actor_pubkey, created_at, updated_at \
             ) VALUES (?, ?, 1, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, pubkey) DO UPDATE SET \
                banned = 1, ban_expires_at = excluded.ban_expires_at, \
                ban_reason = excluded.ban_reason, \
                actor_pubkey = excluded.actor_pubkey, updated_at = excluded.updated_at",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(expires_at.map(|value| value.timestamp_micros()))
        .bind(reason)
        .bind(actor)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lift a tenant-scoped member ban.
    pub async fn unban_community_member(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        actor: &[u8],
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE community_bans \
             SET banned = 0, ban_expires_at = NULL, ban_reason = NULL, \
                 actor_pubkey = ?, updated_at = ? \
             WHERE community_id = ? AND pubkey = ? AND banned = 1",
        )
        .bind(actor)
        .bind(Utc::now().timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Upsert a tenant-scoped write timeout.
    pub async fn timeout_community_member(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        actor: &[u8],
        muted_until: DateTime<Utc>,
        reason: Option<&str>,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "INSERT INTO community_bans ( \
                community_id, pubkey, muted_until, mute_reason, actor_pubkey, \
                created_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, pubkey) DO UPDATE SET \
                muted_until = excluded.muted_until, \
                mute_reason = excluded.mute_reason, \
                actor_pubkey = excluded.actor_pubkey, updated_at = excluded.updated_at",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(muted_until.timestamp_micros())
        .bind(reason)
        .bind(actor)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Clear an active tenant-scoped write timeout.
    pub async fn untimeout_community_member(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        actor: &[u8],
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let now = Utc::now().timestamp_micros();
        let result = sqlx::query(
            "UPDATE community_bans \
             SET muted_until = NULL, mute_reason = NULL, \
                 actor_pubkey = ?, updated_at = ? \
             WHERE community_id = ? AND pubkey = ? AND muted_until > ?",
        )
        .bind(actor)
        .bind(now)
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Fetch the active restriction state for one community member.
    pub async fn moderation_restriction_state(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<RestrictionState> {
        let now = Utc::now().timestamp_micros();
        let row = sqlx::query(
            "SELECT \
                CASE WHEN banned = 1 \
                           AND (ban_expires_at IS NULL OR ban_expires_at > ?) \
                     THEN 1 ELSE 0 END AS banned, \
                CASE WHEN muted_until > ? THEN muted_until ELSE NULL END AS muted_until \
             FROM community_bans WHERE community_id = ? AND pubkey = ?",
        )
        .bind(now)
        .bind(now)
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(RestrictionState {
                banned: row.try_get::<i64, _>("banned")? != 0,
                muted_until: parse_optional_timestamp(row.try_get("muted_until")?, "muted_until")?,
            }),
            None => Ok(RestrictionState::default()),
        }
    }

    /// Fetch one member's complete moderation restriction row.
    pub async fn get_community_ban(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<Option<BanRecord>> {
        let now = Utc::now().timestamp_micros();
        let row = sqlx::query(
            "SELECT pubkey, \
                CASE WHEN banned = 1 \
                           AND (ban_expires_at IS NULL OR ban_expires_at > ?) \
                     THEN 1 ELSE 0 END AS banned, \
                ban_expires_at, ban_reason, muted_until, mute_reason, \
                actor_pubkey, updated_at \
             FROM community_bans WHERE community_id = ? AND pubkey = ?",
        )
        .bind(now)
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_ban).transpose()
    }

    /// List currently restricted members in one community.
    pub async fn list_community_restrictions(
        &self,
        community: CommunityId,
    ) -> Result<Vec<BanRecord>> {
        let now = Utc::now().timestamp_micros();
        let rows = sqlx::query(
            "SELECT pubkey, \
                CASE WHEN banned = 1 \
                           AND (ban_expires_at IS NULL OR ban_expires_at > ?) \
                     THEN 1 ELSE 0 END AS banned, \
                ban_expires_at, ban_reason, muted_until, mute_reason, \
                actor_pubkey, updated_at \
             FROM community_bans \
             WHERE community_id = ? \
               AND ((banned = 1 \
                     AND (ban_expires_at IS NULL OR ban_expires_at > ?)) \
                    OR muted_until > ?) \
             ORDER BY updated_at DESC",
        )
        .bind(now)
        .bind(community.as_uuid().to_string())
        .bind(now)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_ban).collect()
    }

    /// Insert one tenant-scoped moderation action.
    pub async fn insert_moderation_action(
        &self,
        community: CommunityId,
        action: NewAction<'_>,
    ) -> Result<Uuid> {
        let _writer = self.acquire_writer().await;
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO moderation_actions ( \
                community_id, id, actor_pubkey, action, target_pubkey, \
                target_event_id, channel_id, reason_code, public_reason, \
                private_reason, matched_principal, created_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(action.actor_pubkey)
        .bind(action.action)
        .bind(action.target_pubkey)
        .bind(action.target_event_id)
        .bind(action.channel_id.map(|id| id.to_string()))
        .bind(action.reason_code)
        .bind(action.public_reason)
        .bind(action.private_reason)
        .bind(action.matched_principal)
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// List one community's moderation actions, newest first.
    pub async fn list_moderation_actions(
        &self,
        community: CommunityId,
        limit: i64,
    ) -> Result<Vec<ActionRecord>> {
        let rows = sqlx::query(
            "SELECT id, actor_pubkey, action, target_pubkey, target_event_id, \
                channel_id, reason_code, public_reason, private_reason, \
                matched_principal, created_at \
             FROM moderation_actions WHERE community_id = ? \
             ORDER BY created_at DESC LIMIT ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_action).collect()
    }
}
