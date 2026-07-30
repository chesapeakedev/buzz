use std::sync::Arc;

use sqlx::{Row, SqlitePool};
use tokio::sync::Mutex;

use buzz_core::tenant::{CommunityId, TenantContext};
use buzz_media::{BlobMeta, BlobMetadata, MediaError};

/// SQLite-backed blob metadata adapter.
///
/// Stores community-scoped blob metadata in the `media_objects` table as the
/// atomic publication gate for a filesystem blob write. The row is created
/// before publishing the serve gate and deleted with the blob.
///
/// Clone is cheap (inner Arcs).
#[derive(Debug, Clone)]
pub struct SqliteBlobMetadata {
    pool: SqlitePool,
    writer: Arc<Mutex<()>>,
}

impl SqliteBlobMetadata {
    /// Create a new SQLite blob metadata adapter sharing the given pool and writer gate.
    pub fn new(pool: SqlitePool, writer: Arc<Mutex<()>>) -> Self {
        Self { pool, writer }
    }
}

fn community_text(community: CommunityId) -> String {
    community.as_uuid().to_string()
}

fn or_empty(s: Option<String>) -> String {
    s.unwrap_or_default()
}

#[async_trait::async_trait]
impl BlobMetadata for SqliteBlobMetadata {
    async fn get_metadata(
        &self,
        ctx: &TenantContext,
        sha256: &str,
    ) -> Result<Option<BlobMeta>, MediaError> {
        let row = sqlx::query(
            "SELECT mime_type, size, ext, dim, blurhash, thumb_url, \
                    duration_secs, uploaded_at \
             FROM media_objects \
             WHERE community_id = ? AND sha256 = ?",
        )
        .bind(community_text(ctx.community()))
        .bind(sha256)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MediaError::StorageError(e.to_string()))?;

        row.map(|r| {
            Ok(BlobMeta {
                mime_type: r
                    .try_get::<Option<String>, _>("mime_type")?
                    .unwrap_or_default(),
                size: r.try_get::<i64, _>("size")? as u64,
                ext: or_empty(r.try_get("ext")?),
                dim: or_empty(r.try_get("dim")?),
                blurhash: or_empty(r.try_get("blurhash")?),
                thumb_url: or_empty(r.try_get("thumb_url")?),
                duration_secs: r.try_get("duration_secs")?,
                uploaded_at: r.try_get("uploaded_at")?,
            })
        })
        .transpose()
        .map_err(|e: sqlx::Error| MediaError::StorageError(e.to_string()))
    }

    async fn put_metadata(
        &self,
        ctx: &TenantContext,
        sha256: &str,
        meta: &BlobMeta,
    ) -> Result<(), MediaError> {
        let _writer = self.writer.lock().await;
        let ext = if meta.ext.is_empty() {
            None
        } else {
            Some(&meta.ext)
        };
        let dim = if meta.dim.is_empty() {
            None
        } else {
            Some(&meta.dim)
        };
        let blurhash = if meta.blurhash.is_empty() {
            None
        } else {
            Some(&meta.blurhash)
        };
        let thumb_url = if meta.thumb_url.is_empty() {
            None
        } else {
            Some(&meta.thumb_url)
        };
        sqlx::query(
            "INSERT INTO media_objects \
             (community_id, sha256, mime_type, size, ext, dim, blurhash, \
              thumb_url, duration_secs, uploaded_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, sha256) DO UPDATE SET \
               mime_type      = excluded.mime_type, \
               size           = excluded.size, \
               ext            = excluded.ext, \
               dim            = excluded.dim, \
               blurhash       = excluded.blurhash, \
               thumb_url      = excluded.thumb_url, \
               duration_secs  = excluded.duration_secs, \
               uploaded_at    = excluded.uploaded_at",
        )
        .bind(community_text(ctx.community()))
        .bind(sha256)
        .bind(&meta.mime_type)
        .bind(meta.size as i64)
        .bind(ext)
        .bind(dim)
        .bind(blurhash)
        .bind(thumb_url)
        .bind(meta.duration_secs)
        .bind(meta.uploaded_at)
        .execute(&self.pool)
        .await
        .map_err(|e| MediaError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn read_mime(&self, ctx: &TenantContext, sha256_ext: &str) -> Option<String> {
        let sha256 = sha256_ext.split('.').next().unwrap_or(sha256_ext);
        let row: Result<Option<String>, _> = sqlx::query_scalar(
            "SELECT mime_type FROM media_objects \
             WHERE community_id = ? AND sha256 = ?",
        )
        .bind(community_text(ctx.community()))
        .bind(sha256)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "read_mime: media_objects query failed");
            e
        });
        row.ok().flatten()
    }

    async fn delete_metadata(&self, ctx: &TenantContext, sha256: &str) -> Result<(), MediaError> {
        let _writer = self.writer.lock().await;
        sqlx::query(
            "DELETE FROM media_objects \
             WHERE community_id = ? AND sha256 = ?",
        )
        .bind(community_text(ctx.community()))
        .bind(sha256)
        .execute(&self.pool)
        .await
        .map_err(|e| MediaError::StorageError(e.to_string()))?;
        Ok(())
    }
}

/// Standalone delete helper for callers without a `SqliteBlobMetadata` reference.
///
/// Used by the filesystem sweep path that removes the metadata row with the blob.
pub async fn delete_media_object(
    pool: &SqlitePool,
    community_id: CommunityId,
    sha256: &str,
) -> Result<(), MediaError> {
    sqlx::query("DELETE FROM media_objects WHERE community_id = ? AND sha256 = ?")
        .bind(community_text(community_id))
        .bind(sha256)
        .execute(pool)
        .await
        .map_err(|e| MediaError::StorageError(e.to_string()))?;
    Ok(())
}
