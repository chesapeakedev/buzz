//! SQLite NIP-34 repository-name registry.

use chrono::Utc;
use sqlx::Row as _;

use super::SqliteStore;
use crate::git_repo::ReserveOutcome;
use crate::{CommunityId, Result};

impl SqliteStore {
    /// Return the owner of a repository name within one community.
    pub async fn repo_name_owner(
        &self,
        community: CommunityId,
        repo_id: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT owner_pubkey FROM git_repo_names \
             WHERE community_id = ? AND repo_id = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(repo_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| row.try_get("owner_pubkey"))
            .transpose()
            .map_err(Into::into)
    }

    /// Atomically reserve a repository name or classify its current owner.
    pub async fn reserve_repo_name(
        &self,
        community: CommunityId,
        repo_id: &str,
        owner_pubkey: &str,
    ) -> Result<ReserveOutcome> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let inserted = sqlx::query(
            "INSERT INTO git_repo_names ( \
                community_id, repo_id, owner_pubkey, created_at \
             ) VALUES (?, ?, ?, ?) \
             ON CONFLICT (community_id, repo_id) DO NOTHING \
             RETURNING owner_pubkey",
        )
        .bind(community.as_uuid().to_string())
        .bind(repo_id)
        .bind(owner_pubkey)
        .bind(Utc::now().timestamp_micros())
        .fetch_optional(&mut *transaction)
        .await?;
        if inserted.is_some() {
            transaction.commit().await?;
            return Ok(ReserveOutcome::Reserved);
        }
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT owner_pubkey FROM git_repo_names \
             WHERE community_id = ? AND repo_id = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(repo_id)
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(match existing.as_deref() {
            Some(existing) if existing == owner_pubkey => ReserveOutcome::AlreadyOwned,
            Some(_) | None => ReserveOutcome::TakenByOther,
        })
    }

    /// Count repository names owned by one identity within one community.
    pub async fn count_repos_for_owner(
        &self,
        community: CommunityId,
        owner_pubkey: &str,
    ) -> Result<i64> {
        sqlx::query_scalar(
            "SELECT count(*) FROM git_repo_names \
             WHERE community_id = ? AND owner_pubkey = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(owner_pubkey)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Release a repository name only when the supplied owner still holds it.
    pub async fn release_repo_name(
        &self,
        community: CommunityId,
        repo_id: &str,
        owner_pubkey: &str,
    ) -> Result<u64> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "DELETE FROM git_repo_names \
             WHERE community_id = ? AND repo_id = ? AND owner_pubkey = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(repo_id)
        .bind(owner_pubkey)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
