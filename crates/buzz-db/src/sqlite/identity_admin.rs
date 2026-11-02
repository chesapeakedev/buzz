//! SQLite community lifecycle, allowlist, and archived-identity operations.

use chrono::{DateTime, Utc};
use sqlx::Row as _;
use uuid::Uuid;

use super::SqliteStore;
use crate::archived_identities::ArchivedIdentity;
use crate::{
    AllowlistEntry, ArchivedCommunityRecord, CommunityId, CommunityRecord, DbError,
    OwnedCommunityRecord, Result, UnarchivedCommunityRecord,
};

fn parse_community(value: String) -> Result<CommunityId> {
    Uuid::parse_str(&value)
        .map(CommunityId::from_uuid)
        .map_err(|error| DbError::InvalidData(format!("community UUID: {error}")))
}

fn parse_timestamp(value: i64, field: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or_else(|| {
        DbError::InvalidData(format!(
            "{field} timestamp outside supported range: {value}"
        ))
    })
}

impl SqliteStore {
    /// Return a community by host regardless of archival state.
    pub async fn lookup_community_by_host_for_management(
        &self,
        normalized_host: &str,
    ) -> Result<Option<CommunityRecord>> {
        sqlx::query("SELECT id, host FROM communities WHERE lower(host) = lower(?)")
            .bind(normalized_host)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| {
                Ok(CommunityRecord {
                    id: parse_community(row.try_get("id")?)?,
                    host: row.try_get("host")?,
                })
            })
            .transpose()
    }

    /// List communities currently owned by a relay pubkey.
    pub async fn list_communities_owned_by(
        &self,
        owner_pubkey: &str,
    ) -> Result<Vec<OwnedCommunityRecord>> {
        let rows = sqlx::query(
            "SELECT c.id, c.host, c.created_at, c.archived_at \
             FROM communities c \
             JOIN relay_members rm ON rm.community_id = c.id \
             WHERE rm.pubkey = ? AND rm.role = 'owner' \
             ORDER BY c.created_at ASC, c.host ASC",
        )
        .bind(owner_pubkey.to_ascii_lowercase())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let archived_at: Option<i64> = row.try_get("archived_at")?;
                Ok(OwnedCommunityRecord {
                    id: parse_community(row.try_get("id")?)?,
                    host: row.try_get("host")?,
                    created_at: parse_timestamp(row.try_get("created_at")?, "created_at")?,
                    archived_at: archived_at
                        .map(|value| parse_timestamp(value, "archived_at"))
                        .transpose()?,
                })
            })
            .collect()
    }

    /// Return the active host mapped to a community id.
    pub async fn lookup_community_host(&self, community: CommunityId) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT host FROM communities WHERE id = ? AND archived_at IS NULL")
                .bind(community.as_uuid().to_string())
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    /// Return a community's non-empty NIP-11 icon.
    pub async fn get_community_icon(&self, community: CommunityId) -> Result<Option<String>> {
        let icon: Option<Option<String>> =
            sqlx::query_scalar("SELECT icon FROM communities WHERE id = ?")
                .bind(community.as_uuid().to_string())
                .fetch_optional(&self.pool)
                .await?;
        Ok(icon.flatten().filter(|icon| !icon.is_empty()))
    }

    /// Set or clear a community's NIP-11 icon.
    pub async fn set_community_icon(
        &self,
        community: CommunityId,
        icon: Option<&str>,
    ) -> Result<()> {
        let _writer = self.acquire_writer().await;
        sqlx::query("UPDATE communities SET icon = ? WHERE id = ?")
            .bind(icon)
            .bind(community.as_uuid().to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Archive a community only when the asserted pubkey is its owner.
    pub async fn archive_community_owned_by(
        &self,
        normalized_host: &str,
        owner_pubkey: &str,
        protected_deployment_host: &str,
    ) -> Result<Option<ArchivedCommunityRecord>> {
        let _writer = self.acquire_writer().await;
        let timestamp = Utc::now().timestamp_micros();
        let row = sqlx::query(
            "UPDATE communities AS c \
             SET archived_at = coalesce(c.archived_at, ?) \
             WHERE lower(c.host) = lower(?) \
               AND lower(c.host) <> lower(?) \
               AND EXISTS ( \
                 SELECT 1 FROM relay_members rm \
                 WHERE rm.community_id = c.id \
                   AND lower(rm.pubkey) = lower(?) AND rm.role = 'owner' \
               ) \
             RETURNING id, host, archived_at",
        )
        .bind(timestamp)
        .bind(normalized_host)
        .bind(protected_deployment_host)
        .bind(owner_pubkey)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(ArchivedCommunityRecord {
                id: parse_community(row.try_get("id")?)?,
                host: row.try_get("host")?,
                archived_at: parse_timestamp(row.try_get("archived_at")?, "archived_at")?,
            })
        })
        .transpose()
    }

    /// Restore a community only when the asserted pubkey is its owner.
    pub async fn unarchive_community_owned_by(
        &self,
        normalized_host: &str,
        owner_pubkey: &str,
    ) -> Result<Option<UnarchivedCommunityRecord>> {
        let _writer = self.acquire_writer().await;
        let row = sqlx::query(
            "UPDATE communities AS c SET archived_at = NULL \
             WHERE lower(c.host) = lower(?) \
               AND EXISTS ( \
                 SELECT 1 FROM relay_members rm \
                 WHERE rm.community_id = c.id \
                   AND lower(rm.pubkey) = lower(?) AND rm.role = 'owner' \
               ) \
             RETURNING id, host",
        )
        .bind(normalized_host)
        .bind(owner_pubkey)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(UnarchivedCommunityRecord {
                id: parse_community(row.try_get("id")?)?,
                host: row.try_get("host")?,
            })
        })
        .transpose()
    }

    /// Return whether a pubkey is present in a community's legacy allowlist.
    pub async fn is_pubkey_allowed(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
        let row =
            sqlx::query("SELECT 1 FROM pubkey_allowlist WHERE community_id = ? AND pubkey = ?")
                .bind(community.as_uuid().to_string())
                .bind(pubkey)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.is_some())
    }

    /// Return whether a community has any legacy allowlist entries.
    pub async fn has_allowlist_entries(&self, community: CommunityId) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM pubkey_allowlist WHERE community_id = ? LIMIT 1")
            .bind(community.as_uuid().to_string())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// Idempotently add a pubkey to a community's legacy allowlist.
    pub async fn add_to_allowlist(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        added_by: &[u8],
        note: Option<&str>,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "INSERT INTO pubkey_allowlist \
             (community_id, pubkey, added_by, added_at, note) VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT DO NOTHING",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(added_by)
        .bind(Utc::now().timestamp_micros())
        .bind(note)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Remove a pubkey from one community's legacy allowlist.
    pub async fn remove_from_allowlist(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result =
            sqlx::query("DELETE FROM pubkey_allowlist WHERE community_id = ? AND pubkey = ?")
                .bind(community.as_uuid().to_string())
                .bind(pubkey)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }

    /// List one community's legacy allowlist, newest first.
    pub async fn list_allowlist(&self, community: CommunityId) -> Result<Vec<AllowlistEntry>> {
        let rows = sqlx::query(
            "SELECT pubkey, added_by, added_at, note FROM pubkey_allowlist \
             WHERE community_id = ? ORDER BY added_at DESC",
        )
        .bind(community.as_uuid().to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AllowlistEntry {
                    pubkey: row.try_get("pubkey")?,
                    added_by: row.try_get("added_by")?,
                    added_at: parse_timestamp(row.try_get("added_at")?, "added_at")?,
                    note: row.try_get("note")?,
                })
            })
            .collect()
    }

    /// Return whether an identity is archived in one community.
    pub async fn is_archived(&self, community: CommunityId, pubkey: &str) -> Result<bool> {
        let row =
            sqlx::query("SELECT 1 FROM archived_identities WHERE community_id = ? AND pubkey = ?")
                .bind(community.as_uuid().to_string())
                .bind(pubkey)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.is_some())
    }

    /// Idempotently archive an identity within one community.
    #[allow(clippy::too_many_arguments)]
    pub async fn archive(
        &self,
        community: CommunityId,
        pubkey: &str,
        consent_path: &str,
        actor: &str,
        reason: Option<&str>,
        replaced_by: Option<&str>,
        request_event_id: &str,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "INSERT INTO archived_identities \
             (community_id, pubkey, consent_path, actor, reason, replaced_by, \
              request_event_id, archived_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, pubkey) DO NOTHING",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(consent_path)
        .bind(actor)
        .bind(reason)
        .bind(replaced_by)
        .bind(request_event_id)
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Remove an archived-identity marker from one community.
    pub async fn unarchive(&self, community: CommunityId, pubkey: &str) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result =
            sqlx::query("DELETE FROM archived_identities WHERE community_id = ? AND pubkey = ?")
                .bind(community.as_uuid().to_string())
                .bind(pubkey)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }

    /// List archived identities in a community, oldest first.
    pub async fn list_archived(&self, community: CommunityId) -> Result<Vec<ArchivedIdentity>> {
        let rows = sqlx::query(
            "SELECT pubkey, consent_path, actor, reason, replaced_by, \
                    request_event_id, archived_at \
             FROM archived_identities WHERE community_id = ? ORDER BY archived_at ASC",
        )
        .bind(community.as_uuid().to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ArchivedIdentity {
                    pubkey: row.try_get("pubkey")?,
                    consent_path: row.try_get("consent_path")?,
                    actor: row.try_get("actor")?,
                    reason: row.try_get("reason")?,
                    replaced_by: row.try_get("replaced_by")?,
                    request_event_id: row.try_get("request_event_id")?,
                    archived_at: parse_timestamp(row.try_get("archived_at")?, "archived_at")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::sqlite::SqliteConfig;

    async fn fixture() -> (TempDir, SqliteStore, CommunityId, CommunityId, String) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = SqliteStore::connect(
            &directory.path().join("buzz.sqlite3"),
            &SqliteConfig::default(),
        )
        .await
        .expect("SQLite connection");
        store.migrate().await.expect("SQLite migrations");
        let owner = "12".repeat(32);
        let first = match store
            .create_community_with_owner("admin-a.example.test", &owner)
            .await
            .expect("community A")
        {
            crate::CreateCommunityWithOwnerResult::Created(record) => record.id,
            result => panic!("unexpected create result: {result:?}"),
        };
        let second = store
            .ensure_configured_community("admin-b.example.test")
            .await
            .expect("community B")
            .id;
        (directory, store, first, second, owner)
    }

    #[tokio::test]
    async fn community_lifecycle_requires_owner_and_protects_deployment_host() {
        let (_directory, store, community, _, owner) = fixture().await;
        store
            .set_community_icon(community, Some("https://example.test/icon.png"))
            .await
            .expect("set icon");
        assert_eq!(
            store
                .get_community_icon(community)
                .await
                .expect("get icon")
                .as_deref(),
            Some("https://example.test/icon.png")
        );
        assert!(store
            .archive_community_owned_by("admin-a.example.test", &owner, "admin-a.example.test",)
            .await
            .expect("protected archive")
            .is_none());
        assert!(store
            .archive_community_owned_by(
                "admin-a.example.test",
                &"13".repeat(32),
                "other.example.test",
            )
            .await
            .expect("foreign archive")
            .is_none());
        let archived = store
            .archive_community_owned_by("admin-a.example.test", &owner, "other.example.test")
            .await
            .expect("archive")
            .expect("archived");
        assert_eq!(archived.id, community);
        assert!(store
            .lookup_community_by_host("admin-a.example.test")
            .await
            .expect("active lookup")
            .is_none());
        assert!(store
            .lookup_community_by_host_for_management("admin-a.example.test")
            .await
            .expect("management lookup")
            .is_some());
        assert_eq!(
            store
                .list_communities_owned_by(&owner)
                .await
                .expect("owned communities")[0]
                .archived_at,
            Some(archived.archived_at)
        );
        assert!(store
            .unarchive_community_owned_by("admin-a.example.test", &owner)
            .await
            .expect("unarchive")
            .is_some());
        assert_eq!(
            store
                .lookup_community_host(community)
                .await
                .expect("active host")
                .as_deref(),
            Some("admin-a.example.test")
        );
    }

    #[tokio::test]
    async fn allowlist_lifecycle_is_tenant_scoped() {
        let (_directory, store, community_a, community_b, _) = fixture().await;
        let pubkey = vec![0x21; 32];
        let actor = vec![0x22; 32];
        assert!(store
            .add_to_allowlist(community_a, &pubkey, &actor, Some("legacy"))
            .await
            .expect("add A"));
        assert!(!store
            .add_to_allowlist(community_a, &pubkey, &actor, None)
            .await
            .expect("repeat A"));
        assert!(store
            .is_pubkey_allowed(community_a, &pubkey)
            .await
            .expect("allowed A"));
        assert!(!store
            .is_pubkey_allowed(community_b, &pubkey)
            .await
            .expect("allowed B"));
        assert!(store
            .has_allowlist_entries(community_a)
            .await
            .expect("entries A"));
        let entries = store.list_allowlist(community_a).await.expect("list A");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].note.as_deref(), Some("legacy"));
        assert!(!store
            .remove_from_allowlist(community_b, &pubkey)
            .await
            .expect("remove B"));
        assert!(store
            .remove_from_allowlist(community_a, &pubkey)
            .await
            .expect("remove A"));
    }

    #[tokio::test]
    async fn archived_identity_lifecycle_is_tenant_scoped_and_idempotent() {
        let (_directory, store, community_a, community_b, owner) = fixture().await;
        let pubkey = "31".repeat(32);
        let event = "32".repeat(32);
        assert!(store
            .archive(
                community_a,
                &pubkey,
                "owner",
                &owner,
                Some("retired"),
                None,
                &event,
            )
            .await
            .expect("archive A"));
        assert!(!store
            .archive(community_a, &pubkey, "self", &pubkey, None, None, &event,)
            .await
            .expect("repeat A"));
        assert!(store
            .is_archived(community_a, &pubkey)
            .await
            .expect("archived A"));
        assert!(!store
            .is_archived(community_b, &pubkey)
            .await
            .expect("archived B"));
        let records = store.list_archived(community_a).await.expect("list A");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].reason.as_deref(), Some("retired"));
        assert!(!store
            .unarchive(community_b, &pubkey)
            .await
            .expect("unarchive B"));
        assert!(store
            .unarchive(community_a, &pubkey)
            .await
            .expect("unarchive A"));
    }
}
