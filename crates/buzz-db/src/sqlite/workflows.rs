//! SQLite workflow definitions and execution-run persistence.

use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row as _};
use uuid::Uuid;

use buzz_core::CommunityId;

use super::SqliteStore;
use crate::workflow::{
    RunStatus, WorkflowRecord, WorkflowRunRecord, WorkflowStatus, LIST_DEFAULT_LIMIT,
    LIST_MAX_LIMIT,
};
use crate::{DbError, Result};

const WORKFLOW_COLUMNS: &str = "id, community_id, name, owner_pubkey, channel_id, \
    definition, definition_hash, status, enabled, created_at, updated_at";
const QUALIFIED_WORKFLOW_COLUMNS: &str = "w.id, w.community_id, w.name, \
    w.owner_pubkey, w.channel_id, w.definition, w.definition_hash, w.status, \
    w.enabled, w.created_at, w.updated_at";
const RUN_COLUMNS: &str = "community_id, id, workflow_id, status, trigger_event_id, \
    current_step, execution_trace, trigger_context, started_at, completed_at, \
    error_message, created_at";

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

fn row_to_workflow(row: SqliteRow) -> Result<WorkflowRecord> {
    let community = parse_uuid(row.try_get("community_id")?, "community")?;
    Ok(WorkflowRecord {
        id: parse_uuid(row.try_get("id")?, "workflow")?,
        community_id: CommunityId::from_uuid(community),
        name: row.try_get("name")?,
        owner_pubkey: row.try_get("owner_pubkey")?,
        channel_id: parse_optional_uuid(row.try_get("channel_id")?, "channel")?,
        definition: serde_json::from_str(row.try_get::<String, _>("definition")?.as_str())?,
        definition_hash: row.try_get("definition_hash")?,
        status: row.try_get::<String, _>("status")?.parse()?,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
        updated_at: parse_timestamp(row.try_get("updated_at")?, "updated_at")?,
    })
}

fn row_to_run(row: SqliteRow) -> Result<WorkflowRunRecord> {
    let community = parse_uuid(row.try_get("community_id")?, "community")?;
    let trigger_context = row
        .try_get::<Option<String>, _>("trigger_context")?
        .map(|value| serde_json::from_str(&value))
        .transpose()?;
    Ok(WorkflowRunRecord {
        id: parse_uuid(row.try_get("id")?, "workflow run")?,
        community_id: CommunityId::from_uuid(community),
        workflow_id: parse_uuid(row.try_get("workflow_id")?, "workflow")?,
        status: row.try_get::<String, _>("status")?.parse()?,
        trigger_event_id: row.try_get("trigger_event_id")?,
        current_step: row.try_get("current_step")?,
        execution_trace: serde_json::from_str(
            row.try_get::<String, _>("execution_trace")?.as_str(),
        )?,
        trigger_context,
        started_at: parse_optional_timestamp(row.try_get("started_at")?, "started_at")?,
        completed_at: parse_optional_timestamp(row.try_get("completed_at")?, "completed_at")?,
        error_message: row.try_get("error_message")?,
        created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
    })
}

impl SqliteStore {
    /// Create an active, enabled workflow with an application-generated ID.
    pub async fn create_workflow(
        &self,
        community: CommunityId,
        channel_id: Option<Uuid>,
        owner_pubkey: &[u8],
        name: &str,
        definition_json: &str,
        definition_hash: &[u8],
    ) -> Result<Uuid> {
        let _writer = self.acquire_writer().await;
        let id = Uuid::new_v4();
        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "INSERT INTO workflows ( \
                community_id, id, name, owner_pubkey, channel_id, definition, \
                definition_hash, status, enabled, created_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?)",
        )
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(name)
        .bind(owner_pubkey)
        .bind(channel_id.map(|id| id.to_string()))
        .bind(definition_json)
        .bind(definition_hash)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// Insert or owner/channel-guardedly update a NIP-33 workflow definition.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_workflow(
        &self,
        community: CommunityId,
        id: Uuid,
        channel_id: Option<Uuid>,
        owner_pubkey: &[u8],
        name: &str,
        definition_json: &str,
        definition_hash: &[u8],
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let now = Utc::now().timestamp_micros();
        let row = sqlx::query(
            "INSERT INTO workflows ( \
                community_id, id, name, owner_pubkey, channel_id, definition, \
                definition_hash, status, enabled, created_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?) \
             ON CONFLICT (community_id, id) DO UPDATE SET \
                name = excluded.name, definition = excluded.definition, \
                definition_hash = excluded.definition_hash, \
                updated_at = excluded.updated_at \
             WHERE workflows.owner_pubkey = excluded.owner_pubkey \
               AND workflows.channel_id IS excluded.channel_id \
             RETURNING id",
        )
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(name)
        .bind(owner_pubkey)
        .bind(channel_id.map(|id| id.to_string()))
        .bind(definition_json)
        .bind(definition_hash)
        .bind(now)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        if row.is_none() {
            return Err(DbError::AccessDenied(format!(
                "workflow {id} belongs to a different owner or channel"
            )));
        }
        Ok(())
    }

    /// Fetch one workflow inside its owning community.
    pub async fn get_workflow(&self, community: CommunityId, id: Uuid) -> Result<WorkflowRecord> {
        let sql = format!(
            "SELECT {WORKFLOW_COLUMNS} FROM workflows \
             WHERE community_id = ? AND id = ?"
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("workflow {id}")))?;
        row_to_workflow(row)
    }

    /// List one channel's workflows, newest first, with bounded pagination.
    pub async fn list_channel_workflows(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<WorkflowRecord>> {
        let limit = limit.unwrap_or(LIST_DEFAULT_LIMIT).clamp(1, LIST_MAX_LIMIT);
        let offset = offset.unwrap_or(0).max(0);
        let sql = format!(
            "SELECT {WORKFLOW_COLUMNS} FROM workflows \
             WHERE community_id = ? AND channel_id = ? \
             ORDER BY created_at DESC LIMIT ? OFFSET ?"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(channel_id.to_string())
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_workflow).collect()
    }

    /// List active, enabled workflows for one channel.
    pub async fn list_enabled_channel_workflows(
        &self,
        community: CommunityId,
        channel_id: Uuid,
    ) -> Result<Vec<WorkflowRecord>> {
        let sql = format!(
            "SELECT {WORKFLOW_COLUMNS} FROM workflows \
             WHERE community_id = ? AND channel_id = ? \
               AND status = 'active' AND enabled = 1 \
             ORDER BY created_at DESC LIMIT ?"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(channel_id.to_string())
            .bind(LIST_MAX_LIMIT)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_workflow).collect()
    }

    /// List active schedule-triggered workflows from non-archived communities.
    pub async fn list_all_enabled_workflows(&self) -> Result<Vec<WorkflowRecord>> {
        let sql = format!(
            "SELECT {QUALIFIED_WORKFLOW_COLUMNS} FROM workflows w \
             JOIN communities c ON c.id = w.community_id \
             WHERE w.status = 'active' AND w.enabled = 1 \
               AND json_extract(w.definition, '$.trigger.on') = 'schedule' \
               AND c.archived_at IS NULL \
             ORDER BY w.created_at ASC LIMIT ?"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(LIST_MAX_LIMIT)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_workflow).collect()
    }

    /// Update a workflow definition under its community key.
    pub async fn update_workflow(
        &self,
        community: CommunityId,
        id: Uuid,
        name: &str,
        definition_json: &str,
        definition_hash: &[u8],
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let affected = sqlx::query(
            "UPDATE workflows SET name = ?, definition = ?, definition_hash = ? \
             WHERE community_id = ? AND id = ?",
        )
        .bind(name)
        .bind(definition_json)
        .bind(definition_hash)
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(DbError::NotFound(format!("workflow {id}")));
        }
        Ok(())
    }

    /// Update a workflow's lifecycle status.
    pub async fn update_workflow_status(
        &self,
        community: CommunityId,
        id: Uuid,
        status: WorkflowStatus,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let affected =
            sqlx::query("UPDATE workflows SET status = ? WHERE community_id = ? AND id = ?")
                .bind(status.to_string())
                .bind(community.as_uuid().to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?
                .rows_affected();
        if affected == 0 {
            return Err(DbError::NotFound(format!("workflow {id}")));
        }
        Ok(())
    }

    /// Enable or disable a workflow without changing its status.
    pub async fn set_workflow_enabled(
        &self,
        community: CommunityId,
        id: Uuid,
        enabled: bool,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let affected =
            sqlx::query("UPDATE workflows SET enabled = ? WHERE community_id = ? AND id = ?")
                .bind(i64::from(enabled))
                .bind(community.as_uuid().to_string())
                .bind(id.to_string())
                .execute(&self.pool)
                .await?
                .rows_affected();
        if affected == 0 {
            return Err(DbError::NotFound(format!("workflow {id}")));
        }
        Ok(())
    }

    /// Disable an owner's enabled workflows in one channel.
    pub async fn disable_workflows_for_owner_in_channel(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        owner_pubkey: &[u8],
    ) -> Result<u64> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE workflows SET enabled = 0 \
             WHERE community_id = ? AND channel_id = ? \
               AND owner_pubkey = ? AND enabled = 1",
        )
        .bind(community.as_uuid().to_string())
        .bind(channel_id.to_string())
        .bind(owner_pubkey)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Delete a workflow and its cascading runs.
    pub async fn delete_workflow(&self, community: CommunityId, id: Uuid) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let affected = sqlx::query("DELETE FROM workflows WHERE community_id = ? AND id = ?")
            .bind(community.as_uuid().to_string())
            .bind(id.to_string())
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(DbError::NotFound(format!("workflow {id}")));
        }
        Ok(())
    }

    /// Delete a workflow only when the supplied owner matches atomically.
    pub async fn delete_workflow_for_owner(
        &self,
        community: CommunityId,
        id: Uuid,
        owner_pubkey: &[u8],
    ) -> Result<Option<Uuid>> {
        let _writer = self.acquire_writer().await;
        let row = sqlx::query(
            "DELETE FROM workflows \
             WHERE community_id = ? AND id = ? AND owner_pubkey = ? \
             RETURNING channel_id",
        )
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(owner_pubkey)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => parse_optional_uuid(row.try_get("channel_id")?, "channel"),
            None => Err(DbError::NotFound(format!("workflow {id}"))),
        }
    }

    /// Find one workflow by owner and name within a community.
    pub async fn find_workflow_by_owner_and_name(
        &self,
        community: CommunityId,
        owner_pubkey: &[u8],
        name: &str,
    ) -> Result<Option<WorkflowRecord>> {
        let sql = format!(
            "SELECT {WORKFLOW_COLUMNS} FROM workflows \
             WHERE community_id = ? AND owner_pubkey = ? AND name = ? LIMIT 1"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(owner_pubkey)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
            .map(row_to_workflow)
            .transpose()
    }

    /// Create a pending execution run for one tenant-scoped workflow.
    pub async fn create_workflow_run(
        &self,
        community: CommunityId,
        workflow_id: Uuid,
        trigger_event_id: Option<&[u8]>,
        trigger_context: Option<&serde_json::Value>,
    ) -> Result<Uuid> {
        let _writer = self.acquire_writer().await;
        let id = Uuid::new_v4();
        let trigger_context = trigger_context.map(serde_json::to_string).transpose()?;
        sqlx::query(
            "INSERT INTO workflow_runs ( \
                community_id, id, workflow_id, status, trigger_event_id, \
                current_step, execution_trace, trigger_context, created_at \
             ) VALUES (?, ?, ?, 'pending', ?, 0, '[]', ?, ?)",
        )
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(workflow_id.to_string())
        .bind(trigger_event_id)
        .bind(trigger_context)
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// Fetch one execution run inside its owning community.
    pub async fn get_workflow_run(
        &self,
        community: CommunityId,
        id: Uuid,
    ) -> Result<WorkflowRunRecord> {
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM workflow_runs \
             WHERE community_id = ? AND id = ?"
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("workflow_run {id}")))?;
        row_to_run(row)
    }

    /// List one workflow's runs, newest first.
    pub async fn list_workflow_runs(
        &self,
        community: CommunityId,
        workflow_id: Uuid,
        limit: i64,
    ) -> Result<Vec<WorkflowRunRecord>> {
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM workflow_runs \
             WHERE community_id = ? AND workflow_id = ? \
             ORDER BY created_at DESC LIMIT ?"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(community.as_uuid().to_string())
            .bind(workflow_id.to_string())
            .bind(limit.min(1000))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_run).collect()
    }

    /// Update run state and stamp first-start and terminal completion times.
    pub async fn update_workflow_run(
        &self,
        community: CommunityId,
        id: Uuid,
        status: RunStatus,
        current_step: i32,
        trace: &serde_json::Value,
        error: Option<&str>,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        let status = status.to_string();
        let now = Utc::now().timestamp_micros();
        let trace = serde_json::to_string(trace)?;
        let affected = sqlx::query(
            "UPDATE workflow_runs SET \
                status = ?, current_step = ?, execution_trace = ?, \
                error_message = ?, \
                started_at = CASE \
                    WHEN ? = 'running' AND started_at IS NULL THEN ? \
                    ELSE started_at END, \
                completed_at = CASE \
                    WHEN ? IN ('completed', 'failed', 'cancelled') THEN ? \
                    ELSE completed_at END \
             WHERE community_id = ? AND id = ?",
        )
        .bind(&status)
        .bind(current_step)
        .bind(trace)
        .bind(error)
        .bind(&status)
        .bind(now)
        .bind(&status)
        .bind(now)
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(DbError::NotFound(format!("workflow_run {id}")));
        }
        Ok(())
    }
}
