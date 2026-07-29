//! SQLite API-token persistence.

use chrono::{DateTime, Utc};
use sqlx::{Connection as _, Row as _};
use uuid::Uuid;

use super::SqliteStore;
use crate::{ApiTokenRecord, CommunityId, DbError, Result, TokenSummary};

fn timestamp_micros(value: Option<DateTime<Utc>>) -> Option<i64> {
    value.map(|value| value.timestamp_micros())
}

fn parse_timestamp(value: i64, field: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or_else(|| {
        DbError::InvalidData(format!(
            "{field} timestamp outside supported range: {value}"
        ))
    })
}

fn parse_optional_timestamp(value: Option<i64>, field: &str) -> Result<Option<DateTime<Utc>>> {
    value.map(|value| parse_timestamp(value, field)).transpose()
}

fn serialize_token_claims(
    scopes: &[String],
    channel_ids: Option<&[Uuid]>,
) -> Result<(String, Option<String>)> {
    let scopes =
        serde_json::to_string(scopes).map_err(|error| DbError::InvalidData(error.to_string()))?;
    let channel_ids = channel_ids
        .map(|ids| {
            serde_json::to_string(&ids.iter().map(Uuid::to_string).collect::<Vec<_>>()).map_err(
                |error| DbError::InvalidData(format!("channel_ids serialization: {error}")),
            )
        })
        .transpose()?;
    Ok((scopes, channel_ids))
}

fn parse_token(row: sqlx::sqlite::SqliteRow) -> Result<ApiTokenRecord> {
    let id: String = row.try_get("id")?;
    let id = Uuid::parse_str(&id)
        .map_err(|error| DbError::InvalidData(format!("token UUID: {error}")))?;
    let scopes: String = row.try_get("scopes")?;
    let scopes = serde_json::from_str(&scopes)
        .map_err(|error| DbError::InvalidData(format!("scopes JSON: {error}")))?;
    let channel_ids: Option<String> = row.try_get("channel_ids")?;
    let channel_ids = channel_ids
        .map(|value| {
            let values: Vec<String> = serde_json::from_str(&value)
                .map_err(|error| DbError::InvalidData(format!("channel_ids JSON: {error}")))?;
            values
                .into_iter()
                .map(|value| {
                    Uuid::parse_str(&value)
                        .map_err(|error| DbError::InvalidData(format!("channel_ids UUID: {error}")))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    Ok(ApiTokenRecord {
        id,
        token_hash: row.try_get("token_hash")?,
        owner_pubkey: row.try_get("owner_pubkey")?,
        name: row.try_get("name")?,
        scopes,
        channel_ids,
        created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
        expires_at: parse_optional_timestamp(row.try_get("expires_at")?, "expires_at")?,
        last_used_at: parse_optional_timestamp(row.try_get("last_used_at")?, "last_used_at")?,
        revoked_at: parse_optional_timestamp(row.try_get("revoked_at")?, "revoked_at")?,
    })
}

impl SqliteStore {
    /// Create an API token for an existing user.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_api_token(
        &self,
        community: CommunityId,
        token_hash: &[u8],
        owner_pubkey: &[u8],
        name: &str,
        scopes: &[String],
        channel_ids: Option<&[Uuid]>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let (scopes, channel_ids) = serialize_token_claims(scopes, channel_ids)?;
        let _writer = self.acquire_writer().await;
        sqlx::query(
            "INSERT INTO api_tokens \
             (community_id, id, token_hash, owner_pubkey, name, scopes, channel_ids, \
              created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(token_hash)
        .bind(owner_pubkey)
        .bind(name)
        .bind(scopes)
        .bind(channel_ids)
        .bind(Utc::now().timestamp_micros())
        .bind(timestamp_micros(expires_at))
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// Create an API token only while the owner has fewer than ten active tokens.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_api_token_if_under_limit(
        &self,
        community: CommunityId,
        token_hash: &[u8],
        owner_pubkey: &[u8],
        name: &str,
        scopes: &[String],
        channel_ids: Option<&[Uuid]>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<Option<Uuid>> {
        let id = Uuid::new_v4();
        let (scopes, channel_ids) = serialize_token_claims(scopes, channel_ids)?;
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let community = community.as_uuid().to_string();
        let timestamp = Utc::now().timestamp_micros();
        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM api_tokens \
             WHERE community_id = ? AND owner_pubkey = ? AND revoked_at IS NULL \
               AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(&community)
        .bind(owner_pubkey)
        .bind(timestamp)
        .fetch_one(&mut *transaction)
        .await?;
        if active >= 10 {
            transaction.rollback().await?;
            return Ok(None);
        }
        sqlx::query(
            "INSERT INTO api_tokens \
             (community_id, id, token_hash, owner_pubkey, name, scopes, channel_ids, \
              created_at, expires_at, created_by_self_mint) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        )
        .bind(&community)
        .bind(id.to_string())
        .bind(token_hash)
        .bind(owner_pubkey)
        .bind(name)
        .bind(scopes)
        .bind(channel_ids)
        .bind(timestamp)
        .bind(timestamp_micros(expires_at))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(id))
    }

    /// Return a non-revoked token by community-scoped hash.
    pub async fn get_api_token_by_hash(
        &self,
        community: CommunityId,
        hash: &[u8],
    ) -> Result<Option<ApiTokenRecord>> {
        self.get_api_token_by_hash_inner(community, hash, false)
            .await
    }

    /// Return a token by community-scoped hash, including revoked records.
    pub async fn get_api_token_by_hash_including_revoked(
        &self,
        community: CommunityId,
        hash: &[u8],
    ) -> Result<Option<ApiTokenRecord>> {
        self.get_api_token_by_hash_inner(community, hash, true)
            .await
    }

    async fn get_api_token_by_hash_inner(
        &self,
        community: CommunityId,
        hash: &[u8],
        include_revoked: bool,
    ) -> Result<Option<ApiTokenRecord>> {
        let sql = if include_revoked {
            "SELECT id, token_hash, owner_pubkey, name, scopes, channel_ids, \
                    created_at, expires_at, last_used_at, revoked_at \
             FROM api_tokens WHERE community_id = ? AND token_hash = ?"
        } else {
            "SELECT id, token_hash, owner_pubkey, name, scopes, channel_ids, \
                    created_at, expires_at, last_used_at, revoked_at \
             FROM api_tokens \
             WHERE community_id = ? AND token_hash = ? AND revoked_at IS NULL"
        };
        sqlx::query(sql)
            .bind(community.as_uuid().to_string())
            .bind(hash)
            .fetch_optional(&self.pool)
            .await?
            .map(parse_token)
            .transpose()
    }

    /// Record the latest successful use of a community-scoped token.
    pub async fn touch_api_token(&self, community: CommunityId, hash: &[u8]) -> Result<()> {
        let _writer = self.acquire_writer().await;
        sqlx::query(
            "UPDATE api_tokens SET last_used_at = ? \
             WHERE community_id = ? AND token_hash = ?",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// List non-revoked tokens in one community, newest first.
    pub async fn list_active_tokens(&self, community: CommunityId) -> Result<Vec<TokenSummary>> {
        let rows = sqlx::query(
            "SELECT id, name, owner_pubkey, scopes, created_at, expires_at \
             FROM api_tokens WHERE community_id = ? AND revoked_at IS NULL \
             ORDER BY created_at DESC LIMIT 1000",
        )
        .bind(community.as_uuid().to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let id: String = row.try_get("id")?;
                let scopes: String = row.try_get("scopes")?;
                Ok(TokenSummary {
                    id: Uuid::parse_str(&id)
                        .map_err(|error| DbError::InvalidData(format!("token UUID: {error}")))?,
                    name: row.try_get("name")?,
                    owner_pubkey: row.try_get("owner_pubkey")?,
                    scopes: serde_json::from_str(&scopes)
                        .map_err(|error| DbError::InvalidData(format!("scopes JSON: {error}")))?,
                    created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
                    expires_at: parse_optional_timestamp(row.try_get("expires_at")?, "expires_at")?,
                })
            })
            .collect()
    }

    /// List all tokens owned by one user, including revoked records.
    pub async fn list_tokens_by_owner(
        &self,
        community: CommunityId,
        owner_pubkey: &[u8],
    ) -> Result<Vec<ApiTokenRecord>> {
        sqlx::query(
            "SELECT id, token_hash, owner_pubkey, name, scopes, channel_ids, \
                    created_at, expires_at, last_used_at, revoked_at \
             FROM api_tokens WHERE community_id = ? AND owner_pubkey = ? \
             ORDER BY created_at DESC",
        )
        .bind(community.as_uuid().to_string())
        .bind(owner_pubkey)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(parse_token)
        .collect()
    }

    /// Revoke one active token owned by the asserted user.
    pub async fn revoke_token(
        &self,
        community: CommunityId,
        id: Uuid,
        owner_pubkey: &[u8],
        revoked_by: &[u8],
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE api_tokens SET revoked_at = ?, revoked_by = ? \
             WHERE community_id = ? AND id = ? AND owner_pubkey = ? AND revoked_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(revoked_by)
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(owner_pubkey)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Revoke every active token owned by one user.
    pub async fn revoke_all_tokens(
        &self,
        community: CommunityId,
        owner_pubkey: &[u8],
        revoked_by: &[u8],
    ) -> Result<u64> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE api_tokens SET revoked_at = ?, revoked_by = ? \
             WHERE community_id = ? AND owner_pubkey = ? AND revoked_at IS NULL",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(revoked_by)
        .bind(community.as_uuid().to_string())
        .bind(owner_pubkey)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::sqlite::SqliteConfig;

    async fn fixture() -> (TempDir, SqliteStore, CommunityId, CommunityId, Vec<u8>) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = SqliteStore::connect(
            &directory.path().join("buzz.sqlite3"),
            &SqliteConfig::default(),
        )
        .await
        .expect("SQLite connection");
        store.migrate().await.expect("SQLite migrations");
        let first = store
            .ensure_configured_community("tokens-a.example.test")
            .await
            .expect("community A")
            .id;
        let second = store
            .ensure_configured_community("tokens-b.example.test")
            .await
            .expect("community B")
            .id;
        let owner = vec![0xd1; 32];
        store.ensure_user(first, &owner).await.expect("user A");
        store.ensure_user(second, &owner).await.expect("user B");
        (directory, store, first, second, owner)
    }

    #[tokio::test]
    async fn hash_lookup_and_revocation_are_tenant_scoped() {
        let (_directory, store, community_a, community_b, owner) = fixture().await;
        let hash = vec![0xe1; 32];
        let scopes = vec!["files:read".to_owned(), "files:write".to_owned()];
        let channel = Uuid::new_v4();
        let id_a = store
            .create_api_token(
                community_a,
                &hash,
                &owner,
                "token A",
                &scopes,
                Some(&[channel]),
                None,
            )
            .await
            .expect("token A");
        store
            .create_api_token(community_b, &hash, &owner, "token B", &scopes, None, None)
            .await
            .expect("token B");

        let token_a = store
            .get_api_token_by_hash(community_a, &hash)
            .await
            .expect("lookup A")
            .expect("token A");
        let token_b = store
            .get_api_token_by_hash(community_b, &hash)
            .await
            .expect("lookup B")
            .expect("token B");
        assert_eq!(token_a.name, "token A");
        assert_eq!(token_b.name, "token B");
        assert_eq!(token_a.channel_ids, Some(vec![channel]));

        assert!(!store
            .revoke_token(community_b, id_a, &owner, &owner)
            .await
            .expect("foreign revoke"));
        assert!(store
            .revoke_token(community_a, id_a, &owner, &owner)
            .await
            .expect("revoke A"));
        assert!(store
            .get_api_token_by_hash(community_a, &hash)
            .await
            .expect("active lookup")
            .is_none());
        assert!(store
            .get_api_token_by_hash_including_revoked(community_a, &hash)
            .await
            .expect("revoked lookup")
            .expect("revoked token")
            .revoked_at
            .is_some());
        assert!(store
            .get_api_token_by_hash(community_b, &hash)
            .await
            .expect("B remains active")
            .is_some());
    }

    #[tokio::test]
    async fn concurrent_self_mint_enforces_per_tenant_limit() {
        let (_directory, store, community_a, community_b, owner) = fixture().await;
        let store = Arc::new(store);
        let mut tasks = Vec::new();
        for byte in 0_u8..20 {
            let store = Arc::clone(&store);
            let owner = owner.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .create_api_token_if_under_limit(
                        community_a,
                        &[byte; 32],
                        &owner,
                        "limited",
                        &["read".to_owned()],
                        None,
                        None,
                    )
                    .await
            }));
        }
        let mut created = 0;
        for task in tasks {
            if task
                .await
                .expect("mint task")
                .expect("mint result")
                .is_some()
            {
                created += 1;
            }
        }
        assert_eq!(created, 10);
        assert!(store
            .create_api_token_if_under_limit(
                community_b,
                &[0xf1; 32],
                &owner,
                "other tenant",
                &["read".to_owned()],
                None,
                None,
            )
            .await
            .expect("tenant B mint")
            .is_some());
    }

    #[tokio::test]
    async fn touch_list_and_bulk_revoke_preserve_record_history() {
        let (_directory, store, community, _, owner) = fixture().await;
        let first_hash = vec![0xf2; 32];
        let second_hash = vec![0xf3; 32];
        for (hash, name) in [(&first_hash, "first"), (&second_hash, "second")] {
            store
                .create_api_token(
                    community,
                    hash,
                    &owner,
                    name,
                    &["read".to_owned()],
                    None,
                    None,
                )
                .await
                .expect("token");
        }
        store
            .touch_api_token(community, &first_hash)
            .await
            .expect("touch");
        assert!(store
            .get_api_token_by_hash(community, &first_hash)
            .await
            .expect("lookup")
            .expect("token")
            .last_used_at
            .is_some());
        assert_eq!(
            store
                .list_active_tokens(community)
                .await
                .expect("active tokens")
                .len(),
            2
        );
        assert_eq!(
            store
                .revoke_all_tokens(community, &owner, &owner)
                .await
                .expect("bulk revoke"),
            2
        );
        assert!(store
            .list_active_tokens(community)
            .await
            .expect("active after revoke")
            .is_empty());
        assert_eq!(
            store
                .list_tokens_by_owner(community, &owner)
                .await
                .expect("history")
                .len(),
            2
        );
    }
}
