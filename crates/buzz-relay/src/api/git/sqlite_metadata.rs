//! SQLite publication gate for embedded Git pointers.

use buzz_core::CommunityId;
use buzz_db::sqlite::git_pointers::{self, PointerCasOutcome};
use buzz_db::sqlite::SqliteStore;

use super::store::{CasOutcome, ETag, GitPointerMetadata, GitPointerMetadataStore, StoreError};

/// Adapter from the relay Git metadata seam to the SQLite data layer.
#[derive(Clone)]
pub struct SqliteGitPointerMetadata {
    store: SqliteStore,
}

impl SqliteGitPointerMetadata {
    /// Create an adapter sharing the relay's SQLite pool and writer gate.
    pub fn new(store: SqliteStore) -> Self {
        Self { store }
    }
}

fn storage_error(error: impl std::fmt::Display) -> StoreError {
    StoreError::Storage(error.to_string())
}

#[async_trait::async_trait]
impl GitPointerMetadataStore for SqliteGitPointerMetadata {
    async fn get_pointer_metadata(
        &self,
        community: CommunityId,
        owner: &str,
        repo: &str,
    ) -> Result<Option<GitPointerMetadata>, StoreError> {
        git_pointers::get_pointer_metadata(&self.store.adapter_pool(), community, owner, repo)
            .await
            .map_err(storage_error)?
            .map(|row| {
                Ok(GitPointerMetadata {
                    content_digest: row.content_digest,
                    size: row.size,
                    etag: row
                        .etag
                        .ok_or_else(|| storage_error("SQLite Git pointer has no etag"))?,
                })
            })
            .transpose()
    }

    async fn cas_pointer(
        &self,
        community: CommunityId,
        owner: &str,
        repo: &str,
        content_digest: &str,
        size: i64,
        expected_etag: Option<&str>,
    ) -> Result<CasOutcome, StoreError> {
        let _writer = self.store.acquire_writer().await;
        let outcome = git_pointers::cas_swap_pointer(
            &self.store.adapter_pool(),
            community,
            owner,
            repo,
            content_digest,
            size,
            expected_etag,
            None,
            chrono::Utc::now().timestamp_micros(),
        )
        .await
        .map_err(storage_error)?;
        Ok(match outcome {
            PointerCasOutcome::Applied(etag) => CasOutcome::Won(ETag(etag)),
            PointerCasOutcome::PreconditionFailed => CasOutcome::LostRace,
        })
    }
}
