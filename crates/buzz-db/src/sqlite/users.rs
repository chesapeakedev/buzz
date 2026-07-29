//! SQLite user-profile operations.

use chrono::Utc;
use sqlx::{QueryBuilder, Row as _, Sqlite};

use super::SqliteStore;
use crate::user::{UserProfile, UserSearchProfile};
use crate::{CommunityId, Result};

fn empty_to_none(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn parse_profile(row: sqlx::sqlite::SqliteRow) -> Result<UserProfile> {
    Ok(UserProfile {
        pubkey: row.try_get("pubkey")?,
        display_name: row.try_get("display_name")?,
        avatar_url: row.try_get("avatar_url")?,
        about: row.try_get("about")?,
        nip05_handle: row.try_get("nip05_handle")?,
    })
}

impl SqliteStore {
    /// Ensure a minimal user row exists within a community.
    pub async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let timestamp = Utc::now().timestamp_micros();
        let result = sqlx::query(
            "INSERT INTO users (community_id, pubkey, created_at, updated_at) \
             VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Return a community-scoped user profile.
    pub async fn get_user(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<Option<UserProfile>> {
        sqlx::query(
            "SELECT pubkey, display_name, avatar_url, about, nip05_handle \
             FROM users WHERE community_id = ? AND pubkey = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .fetch_optional(&self.pool)
        .await?
        .map(parse_profile)
        .transpose()
    }

    /// Update only the supplied profile fields.
    ///
    /// Empty strings clear a field to `NULL`, matching kind-0 absolute-state
    /// behavior and avoiding collisions on optional unique fields.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_user_profile(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        display_name: Option<&str>,
        avatar_url: Option<&str>,
        about: Option<&str>,
        nip05_handle: Option<&str>,
    ) -> Result<()> {
        let fields = [
            ("display_name", display_name),
            ("avatar_url", avatar_url),
            ("about", about),
            ("nip05_handle", nip05_handle),
        ];
        if fields.iter().all(|(_, value)| value.is_none()) {
            return Ok(());
        }

        let _writer = self.acquire_writer().await;
        let mut builder = QueryBuilder::<Sqlite>::new("UPDATE users SET ");
        let mut assignments = builder.separated(", ");
        for (column, value) in fields {
            if value.is_some() {
                assignments
                    .push(column)
                    .push_unseparated(" = ")
                    .push_bind_unseparated(empty_to_none(value));
            }
        }
        builder
            .push(" WHERE community_id = ")
            .push_bind(community.as_uuid().to_string())
            .push(" AND pubkey = ")
            .push_bind(pubkey);
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Look up a user by full NIP-05 handle, case-insensitively.
    pub async fn get_user_by_nip05(
        &self,
        community: CommunityId,
        local_part: &str,
        domain: &str,
    ) -> Result<Option<UserProfile>> {
        let handle = format!("{local_part}@{domain}");
        sqlx::query(
            "SELECT pubkey, display_name, avatar_url, about, nip05_handle \
             FROM users \
             WHERE community_id = ? AND lower(nip05_handle) = lower(?) LIMIT 1",
        )
        .bind(community.as_uuid().to_string())
        .bind(handle)
        .fetch_optional(&self.pool)
        .await?
        .map(parse_profile)
        .transpose()
    }

    /// Search community users by display name, NIP-05 handle, or pubkey prefix.
    pub async fn search_users(
        &self,
        community: CommunityId,
        query: &str,
        limit: u32,
    ) -> Result<Vec<UserSearchProfile>> {
        let normalized = query.trim().to_lowercase();
        if normalized.is_empty() {
            return Ok(Vec::new());
        }
        let escaped = escape_like(&normalized);
        let contains_pattern = format!("%{escaped}%");
        let prefix_pattern = format!("{escaped}%");
        let rows = sqlx::query(
            "SELECT pubkey, display_name, avatar_url, nip05_handle \
             FROM users \
             WHERE community_id = ? \
               AND (lower(coalesce(display_name, '')) LIKE ? ESCAPE '\\' \
                OR lower(coalesce(nip05_handle, '')) LIKE ? ESCAPE '\\' \
                OR lower(hex(pubkey)) LIKE ? ESCAPE '\\') \
             ORDER BY \
               CASE \
                 WHEN lower(coalesce(display_name, '')) = ? THEN 0 \
                 WHEN lower(coalesce(nip05_handle, '')) = ? THEN 1 \
                 WHEN lower(hex(pubkey)) = ? THEN 2 \
                 WHEN lower(coalesce(display_name, '')) LIKE ? ESCAPE '\\' THEN 3 \
                 WHEN lower(coalesce(nip05_handle, '')) LIKE ? ESCAPE '\\' THEN 4 \
                 WHEN lower(hex(pubkey)) LIKE ? ESCAPE '\\' THEN 5 \
                 ELSE 6 \
               END, \
               coalesce(nullif(display_name, ''), nullif(nip05_handle, ''), lower(hex(pubkey))) \
             LIMIT ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(&contains_pattern)
        .bind(&contains_pattern)
        .bind(&contains_pattern)
        .bind(&normalized)
        .bind(&normalized)
        .bind(&normalized)
        .bind(&prefix_pattern)
        .bind(&prefix_pattern)
        .bind(&prefix_pattern)
        .bind(i64::from(limit.clamp(1, 500)))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(UserSearchProfile {
                    pubkey: row.try_get("pubkey")?,
                    display_name: row.try_get("display_name")?,
                    avatar_url: row.try_get("avatar_url")?,
                    nip05_handle: row.try_get("nip05_handle")?,
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

    async fn fixture() -> (TempDir, SqliteStore, CommunityId, CommunityId) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = SqliteStore::connect(
            &directory.path().join("buzz.sqlite3"),
            &SqliteConfig::default(),
        )
        .await
        .expect("SQLite connection");
        store.migrate().await.expect("SQLite migrations");
        let first = store
            .ensure_configured_community("users-a.example.test")
            .await
            .expect("community A")
            .id;
        let second = store
            .ensure_configured_community("users-b.example.test")
            .await
            .expect("community B")
            .id;
        (directory, store, first, second)
    }

    #[tokio::test]
    async fn profile_lifecycle_is_community_scoped() {
        let (_directory, store, community_a, community_b) = fixture().await;
        let pubkey = vec![0xb1; 32];
        assert!(store
            .ensure_user(community_a, &pubkey)
            .await
            .expect("user A"));
        assert!(!store
            .ensure_user(community_a, &pubkey)
            .await
            .expect("user A repeat"));
        assert!(store
            .ensure_user(community_b, &pubkey)
            .await
            .expect("user B"));
        store
            .update_user_profile(
                community_a,
                &pubkey,
                Some("Alice"),
                Some("https://example.test/alice.png"),
                Some("Builder"),
                Some("alice@example.test"),
            )
            .await
            .expect("profile update");

        let profile = store
            .get_user(community_a, &pubkey)
            .await
            .expect("profile A")
            .expect("profile exists");
        assert_eq!(profile.display_name.as_deref(), Some("Alice"));
        assert_eq!(profile.about.as_deref(), Some("Builder"));
        assert_eq!(
            store
                .get_user(community_b, &pubkey)
                .await
                .expect("profile B")
                .expect("profile exists")
                .display_name,
            None
        );
        assert_eq!(
            store
                .get_user_by_nip05(community_a, "ALICE", "EXAMPLE.TEST")
                .await
                .expect("NIP-05 lookup")
                .expect("NIP-05 profile")
                .pubkey,
            pubkey
        );
        assert!(store
            .get_user_by_nip05(community_b, "alice", "example.test")
            .await
            .expect("foreign NIP-05 lookup")
            .is_none());
    }

    #[tokio::test]
    async fn partial_updates_preserve_unspecified_fields_and_clear_empty_values() {
        let (_directory, store, community, _) = fixture().await;
        let pubkey = vec![0xb2; 32];
        store.ensure_user(community, &pubkey).await.expect("user");
        store
            .update_user_profile(
                community,
                &pubkey,
                Some("Before"),
                Some("https://example.test/avatar.png"),
                Some("About"),
                Some("before@example.test"),
            )
            .await
            .expect("initial profile");
        store
            .update_user_profile(community, &pubkey, Some("After"), None, None, Some(""))
            .await
            .expect("partial profile");

        let profile = store
            .get_user(community, &pubkey)
            .await
            .expect("profile")
            .expect("user");
        assert_eq!(profile.display_name.as_deref(), Some("After"));
        assert_eq!(
            profile.avatar_url.as_deref(),
            Some("https://example.test/avatar.png")
        );
        assert_eq!(profile.about.as_deref(), Some("About"));
        assert_eq!(profile.nip05_handle, None);
    }

    #[tokio::test]
    async fn search_is_literal_ranked_bounded_and_tenant_scoped() {
        let (_directory, store, community_a, community_b) = fixture().await;
        for (community, byte, name, nip05) in [
            (community_a, 0xc1, "alice", "alice@example.test"),
            (community_a, 0xc2, "alice cooper", "cooper@example.test"),
            (community_b, 0xc3, "alice foreign", "alice@foreign.test"),
            (community_a, 0xc4, "100% literal", "percent@example.test"),
        ] {
            let pubkey = vec![byte; 32];
            store.ensure_user(community, &pubkey).await.expect("user");
            store
                .update_user_profile(community, &pubkey, Some(name), None, None, Some(nip05))
                .await
                .expect("profile");
        }

        let results = store
            .search_users(community_a, "alice", 20)
            .await
            .expect("search");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].display_name.as_deref(), Some("alice"));
        assert_eq!(
            store
                .search_users(community_a, "%", 20)
                .await
                .expect("literal wildcard")
                .len(),
            1
        );
        assert!(store
            .search_users(community_a, "   ", 20)
            .await
            .expect("empty search")
            .is_empty());
    }
}
