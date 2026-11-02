use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use buzz_core::CommunityId;

use crate::Result;

/// Row shape returned by [`get_pointer_metadata`].
#[derive(Debug, Clone)]
pub struct GitPointerRow {
    /// Manifest digest (hex SHA-256) the pointer currently resolves to.
    pub content_digest: String,
    /// Uncompressed size of the manifest bytes.
    pub size: i64,
    /// Opaque CAS version token; `None` before the first write.
    pub etag: Option<String>,
    /// 32-byte nostr pubkey of the uploader.
    pub uploader_pubkey: Option<Vec<u8>>,
    /// UTC microsecond timestamp of the last update.
    pub updated_at: i64,
}

/// Outcome of a CAS pointer swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointerCasOutcome {
    /// Swap succeeded; the inner value is the new opaque etag.
    Applied(String),
    /// Precondition not met (row already exists for IfNoneMatchStar, or etag
    /// mismatch for IfMatch).
    PreconditionFailed,
}

fn community_text(community: CommunityId) -> String {
    community.as_uuid().to_string()
}

/// Read the current pointer metadata for a (community, owner, repo) tuple.
///
/// Returns `Ok(None)` when no pointer row exists (first push).
pub async fn get_pointer_metadata(
    pool: &SqlitePool,
    community_id: CommunityId,
    owner: &str,
    repo: &str,
) -> Result<Option<GitPointerRow>> {
    let row = sqlx::query(
        "SELECT content_digest, size, etag, uploader_pubkey, updated_at \
         FROM git_pointers \
         WHERE community_id = ? AND owner = ? AND repo = ?",
    )
    .bind(community_text(community_id))
    .bind(owner)
    .bind(repo)
    .fetch_optional(pool)
    .await?;

    row.map(|r| {
        Ok(GitPointerRow {
            content_digest: r.try_get("content_digest")?,
            size: r.try_get("size")?,
            etag: r.try_get("etag")?,
            uploader_pubkey: r.try_get("uploader_pubkey")?,
            updated_at: r.try_get("updated_at")?,
        })
    })
    .transpose()
}

/// Atomically swap a git pointer row under a precondition.
///
/// - When `expected_etag` is `None` (IfNoneMatchStar), the row is created only
///   if no row exists for the (community, owner, repo) tuple. Returns
///   `PreconditionFailed` if a row already exists.
/// - When `expected_etag` is `Some(etag)` (IfMatch), the row is updated only
///   if the stored etag matches. Returns `PreconditionFailed` on mismatch.
///
/// On success, a new opaque etag (v4 UUID) is generated and returned inside
/// `PointerCasOutcome::Applied`.
///
/// The caller must hold the writer gate for the serialized SQLite mutation
/// boundary. This function does not acquire it internally so callers can batch
/// multiple operations under one gate acquisition.
#[allow(clippy::too_many_arguments)]
pub async fn cas_swap_pointer(
    pool: &SqlitePool,
    community_id: CommunityId,
    owner: &str,
    repo: &str,
    content_digest: &str,
    size: i64,
    expected_etag: Option<&str>,
    uploader_pubkey: Option<&[u8]>,
    updated_at: i64,
) -> Result<PointerCasOutcome> {
    let new_etag = Uuid::new_v4().to_string();

    match expected_etag {
        None => {
            let result = sqlx::query(
                "INSERT INTO git_pointers \
                 (community_id, owner, repo, content_digest, size, etag, \
                  uploader_pubkey, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT (community_id, owner, repo) DO NOTHING",
            )
            .bind(community_text(community_id))
            .bind(owner)
            .bind(repo)
            .bind(content_digest)
            .bind(size)
            .bind(&new_etag)
            .bind(uploader_pubkey)
            .bind(updated_at)
            .execute(pool)
            .await?;

            if result.rows_affected() == 0 {
                Ok(PointerCasOutcome::PreconditionFailed)
            } else {
                Ok(PointerCasOutcome::Applied(new_etag))
            }
        }
        Some(expected) => {
            let result = sqlx::query(
                "UPDATE git_pointers \
                 SET content_digest = ?, size = ?, etag = ?, \
                     uploader_pubkey = ?, updated_at = ? \
                 WHERE community_id = ? AND owner = ? AND repo = ? \
                   AND etag = ?",
            )
            .bind(content_digest)
            .bind(size)
            .bind(&new_etag)
            .bind(uploader_pubkey)
            .bind(updated_at)
            .bind(community_text(community_id))
            .bind(owner)
            .bind(repo)
            .bind(expected)
            .execute(pool)
            .await?;

            if result.rows_affected() == 0 {
                Ok(PointerCasOutcome::PreconditionFailed)
            } else {
                Ok(PointerCasOutcome::Applied(new_etag))
            }
        }
    }
}
