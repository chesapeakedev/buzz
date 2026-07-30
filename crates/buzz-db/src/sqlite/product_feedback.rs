//! SQLite persistence for the deployment-global product-feedback inbox.

use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row as _};
use uuid::Uuid;

use super::SqliteStore;
use crate::product_feedback::{NewProductFeedback, ProductFeedbackRecord};
use crate::{CommunityId, DbError, Result};

fn parse_uuid(value: String) -> Result<Uuid> {
    Uuid::parse_str(&value)
        .map_err(|error| DbError::InvalidData(format!("product feedback UUID: {error}")))
}

fn parse_timestamp(value: i64, column: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value)
        .ok_or_else(|| DbError::InvalidData(format!("invalid {column} timestamp: {value}")))
}

fn row_to_feedback(row: SqliteRow) -> Result<ProductFeedbackRecord> {
    Ok(ProductFeedbackRecord {
        id: parse_uuid(row.try_get("id")?)?,
        community_id: parse_uuid(row.try_get("community_id")?)?,
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
    /// Sidecar accepted product feedback, idempotent deployment-wide by event.
    pub async fn insert_product_feedback(
        &self,
        community: CommunityId,
        feedback: NewProductFeedback<'_>,
    ) -> Result<Uuid> {
        let _writer = self.acquire_writer().await;
        let id = Uuid::new_v4().to_string();
        let tags = serde_json::to_string(feedback.tags)?;
        let row = sqlx::query(
            "INSERT INTO product_feedback ( \
                id, community_id, event_id, submitter_pubkey, category, body, \
                tags, event_created_at, received_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (event_id) DO UPDATE SET event_id = excluded.event_id \
             RETURNING id",
        )
        .bind(id)
        .bind(community.as_uuid().to_string())
        .bind(feedback.event_id)
        .bind(feedback.submitter_pubkey)
        .bind(feedback.category)
        .bind(feedback.body)
        .bind(tags)
        .bind(feedback.event_created_at.timestamp_micros())
        .bind(Utc::now().timestamp_micros())
        .fetch_one(&self.pool)
        .await?;
        parse_uuid(row.try_get("id")?)
    }

    /// List product feedback across the deployment, newest first.
    pub async fn list_product_feedback(&self, limit: i64) -> Result<Vec<ProductFeedbackRecord>> {
        let rows = sqlx::query(
            "SELECT id, community_id, event_id, submitter_pubkey, category, \
                    body, tags, event_created_at, received_at \
             FROM product_feedback ORDER BY received_at DESC, id LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_feedback).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::SqliteConfig;

    #[tokio::test]
    async fn schema_rejects_malformed_feedback_rows() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = SqliteStore::connect(
            &directory.path().join("buzz.sqlite3"),
            &SqliteConfig::default(),
        )
        .await
        .expect("SQLite connection");
        store.migrate().await.expect("SQLite migrations");
        let community = store
            .ensure_configured_community("feedback-schema.example.test")
            .await
            .expect("community")
            .id;

        for (event_id, category, body, tags) in [
            (vec![1; 31], Some("bug"), "body", "[]"),
            (vec![2; 32], Some("unknown"), "body", "[]"),
            (vec![3; 32], Some("bug"), "   ", "[]"),
            (vec![4; 32], Some("bug"), "body", "{}"),
        ] {
            let result = sqlx::query(
                "INSERT INTO product_feedback ( \
                    id, community_id, event_id, submitter_pubkey, category, \
                    body, tags, event_created_at, received_at \
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(community.as_uuid().to_string())
            .bind(event_id)
            .bind(vec![5; 32])
            .bind(category)
            .bind(body)
            .bind(tags)
            .execute(&store.pool)
            .await;
            assert!(result.is_err(), "malformed feedback row must fail");
        }
    }
}
