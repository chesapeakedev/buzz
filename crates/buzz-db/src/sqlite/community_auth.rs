//! SQLite community registry and relay-membership operations.

use chrono::{DateTime, Utc};
use sqlx::{Connection as _, Row as _};
use uuid::Uuid;

use super::SqliteStore;
use crate::relay_members::{RelayMember, RemoveResult};
use crate::{
    CommunityId, CommunityRecord, CreateCommunityWithOwnerResult, CreatedCommunityRecord,
    EnsuredCommunityRecord, Result,
};

fn now_micros() -> i64 {
    Utc::now().timestamp_micros()
}

fn parse_community_id(value: String) -> Result<CommunityId> {
    let id = Uuid::parse_str(&value)
        .map_err(|error| crate::DbError::InvalidData(format!("community UUID: {error}")))?;
    Ok(CommunityId::from_uuid(id))
}

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or_else(|| {
        crate::DbError::InvalidData(format!("SQLite timestamp outside supported range: {value}"))
    })
}

fn parse_member(row: sqlx::sqlite::SqliteRow) -> Result<RelayMember> {
    Ok(RelayMember {
        pubkey: row.try_get("pubkey")?,
        role: row.try_get("role")?,
        added_by: row.try_get("added_by")?,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?,
    })
}

impl SqliteStore {
    /// Return the active community mapped to a normalized host.
    pub async fn lookup_community_by_host(
        &self,
        normalized_host: &str,
    ) -> Result<Option<CommunityRecord>> {
        let row = sqlx::query(
            "SELECT id, host FROM communities \
             WHERE lower(host) = lower(?) AND archived_at IS NULL",
        )
        .bind(normalized_host)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(CommunityRecord {
                id: parse_community_id(row.try_get("id")?)?,
                host: row.try_get("host")?,
            })
        })
        .transpose()
    }

    /// Return whether a community exists and is not archived.
    pub async fn is_community_active(&self, community_id: CommunityId) -> Result<bool> {
        let active: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM communities WHERE id = ? AND archived_at IS NULL)",
        )
        .bind(community_id.as_uuid().to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(active != 0)
    }

    /// Ensure that a normalized configured host has a stable community row.
    pub async fn ensure_configured_community(
        &self,
        normalized_host: &str,
    ) -> Result<EnsuredCommunityRecord> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let existing = sqlx::query("SELECT id, host FROM communities WHERE lower(host) = lower(?)")
            .bind(normalized_host)
            .fetch_optional(&mut *transaction)
            .await?;

        let record = match existing {
            Some(row) => EnsuredCommunityRecord {
                id: parse_community_id(row.try_get("id")?)?,
                host: row.try_get("host")?,
                created: false,
            },
            None => {
                let id = Uuid::new_v4();
                sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, ?)")
                    .bind(id.to_string())
                    .bind(normalized_host)
                    .bind(now_micros())
                    .execute(&mut *transaction)
                    .await?;
                EnsuredCommunityRecord {
                    id: CommunityId::from_uuid(id),
                    host: normalized_host.to_owned(),
                    created: true,
                }
            }
        };
        transaction.commit().await?;
        Ok(record)
    }

    /// Atomically create a community and its initial owner.
    pub async fn create_community_with_owner(
        &self,
        normalized_host: &str,
        owner_pubkey: &str,
    ) -> Result<CreateCommunityWithOwnerResult> {
        let owner_pubkey = owner_pubkey.to_ascii_lowercase();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;

        if let Some(row) = sqlx::query(
            "SELECT id, host, archived_at FROM communities WHERE lower(host) = lower(?)",
        )
        .bind(normalized_host)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let archived_at: Option<i64> = row.try_get("archived_at")?;
            let owned: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM relay_members \
                 WHERE community_id = ? AND lower(pubkey) = lower(?) AND role = 'owner'",
            )
            .bind(row.try_get::<String, _>("id")?)
            .bind(&owner_pubkey)
            .fetch_one(&mut *transaction)
            .await?;
            if owned == 0 || archived_at.is_some() {
                transaction.rollback().await?;
                return Ok(CreateCommunityWithOwnerResult::HostExists);
            }
            let id = parse_community_id(row.try_get("id")?)?;
            let host = row.try_get("host")?;
            transaction.commit().await?;
            return Ok(CreateCommunityWithOwnerResult::Created(
                CreatedCommunityRecord { id, host },
            ));
        }

        let owned_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM relay_members WHERE pubkey = ? AND role = 'owner'",
        )
        .bind(&owner_pubkey)
        .fetch_one(&mut *transaction)
        .await?;
        if owned_count >= crate::relay_members::max_communities_per_owner() {
            transaction.rollback().await?;
            return Ok(CreateCommunityWithOwnerResult::LimitReached);
        }

        let id = Uuid::new_v4();
        let timestamp = now_micros();
        sqlx::query("INSERT INTO communities (id, host, created_at) VALUES (?, ?, ?)")
            .bind(id.to_string())
            .bind(normalized_host)
            .bind(timestamp)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, added_by, created_at, updated_at) \
             VALUES (?, ?, 'owner', NULL, ?, ?)",
        )
        .bind(id.to_string())
        .bind(&owner_pubkey)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(CreateCommunityWithOwnerResult::Created(
            CreatedCommunityRecord {
                id: CommunityId::from_uuid(id),
                host: normalized_host.to_owned(),
            },
        ))
    }

    /// Return whether a pubkey is a member of a community.
    pub async fn is_relay_member(&self, community: CommunityId, pubkey: &str) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM relay_members WHERE community_id = ? AND pubkey = ?")
            .bind(community.as_uuid().to_string())
            .bind(pubkey)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// Return one community-scoped relay member.
    pub async fn get_relay_member(
        &self,
        community: CommunityId,
        pubkey: &str,
    ) -> Result<Option<RelayMember>> {
        sqlx::query(
            "SELECT pubkey, role, added_by, created_at, updated_at \
             FROM relay_members WHERE community_id = ? AND pubkey = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .fetch_optional(&self.pool)
        .await?
        .map(parse_member)
        .transpose()
    }

    /// List all relay members in creation order.
    pub async fn list_relay_members(&self, community: CommunityId) -> Result<Vec<RelayMember>> {
        sqlx::query(
            "SELECT pubkey, role, added_by, created_at, updated_at \
             FROM relay_members WHERE community_id = ? ORDER BY created_at ASC",
        )
        .bind(community.as_uuid().to_string())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(parse_member)
        .collect()
    }

    /// Idempotently add a relay member.
    pub async fn add_relay_member(
        &self,
        community: CommunityId,
        pubkey: &str,
        role: &str,
        added_by: Option<&str>,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let timestamp = now_micros();
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, added_by, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (community_id, pubkey) DO NOTHING",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(role)
        .bind(added_by)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    /// Atomically claim membership and record optional policy evidence.
    pub async fn claim_relay_membership(
        &self,
        community: CommunityId,
        pubkey: &str,
        role: &str,
        policy_version: Option<&str>,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let timestamp = now_micros();
        let inserted = sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, added_by, created_at, updated_at) \
             VALUES (?, ?, ?, 'invite', ?, ?) \
             ON CONFLICT (community_id, pubkey) DO NOTHING",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(role)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            > 0;
        if let Some(policy_version) = policy_version {
            sqlx::query(
                "INSERT INTO join_policy_acceptances \
                 (community_id, pubkey, policy_version, accepted_at) VALUES (?, ?, ?, ?) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(community.as_uuid().to_string())
            .bind(pubkey)
            .bind(policy_version)
            .bind(timestamp)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(inserted)
    }

    /// Return whether a member accepted a specific policy version.
    pub async fn has_join_policy_acceptance(
        &self,
        community: CommunityId,
        pubkey: &str,
        policy_version: &str,
    ) -> Result<bool> {
        let row = sqlx::query(
            "SELECT 1 FROM join_policy_acceptances \
             WHERE community_id = ? AND pubkey = ? AND policy_version = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .bind(policy_version)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    /// Remove a relay member while protecting owners.
    pub async fn remove_relay_member(
        &self,
        community: CommunityId,
        pubkey: &str,
    ) -> Result<RemoveResult> {
        self.remove_relay_member_if_role_inner(community, pubkey, None)
            .await
    }

    /// Remove a relay member only while its role matches the expected value.
    pub async fn remove_relay_member_if_role(
        &self,
        community: CommunityId,
        pubkey: &str,
        expected_role: &str,
    ) -> Result<RemoveResult> {
        self.remove_relay_member_if_role_inner(community, pubkey, Some(expected_role))
            .await
    }

    async fn remove_relay_member_if_role_inner(
        &self,
        community: CommunityId,
        pubkey: &str,
        expected_role: Option<&str>,
    ) -> Result<RemoveResult> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let community = community.as_uuid().to_string();
        let deleted = match expected_role {
            Some(role) => {
                sqlx::query(
                    "DELETE FROM relay_members \
                     WHERE community_id = ? AND pubkey = ? AND role = ? AND role <> 'owner'",
                )
                .bind(&community)
                .bind(pubkey)
                .bind(role)
                .execute(&mut *transaction)
                .await?
            }
            None => {
                sqlx::query(
                    "DELETE FROM relay_members \
                     WHERE community_id = ? AND pubkey = ? AND role <> 'owner'",
                )
                .bind(&community)
                .bind(pubkey)
                .execute(&mut *transaction)
                .await?
            }
        };
        let result = if deleted.rows_affected() > 0 {
            RemoveResult::Removed
        } else {
            let role: Option<String> = sqlx::query_scalar(
                "SELECT role FROM relay_members WHERE community_id = ? AND pubkey = ?",
            )
            .bind(&community)
            .bind(pubkey)
            .fetch_optional(&mut *transaction)
            .await?;
            match role.as_deref() {
                None => RemoveResult::NotFound,
                Some("owner") => RemoveResult::IsOwner,
                Some(_) if expected_role.is_some() => RemoveResult::RoleMismatch,
                Some(_) => RemoveResult::NotFound,
            }
        };
        transaction.commit().await?;
        Ok(result)
    }

    /// Update a non-owner relay member role.
    pub async fn update_relay_member_role(
        &self,
        community: CommunityId,
        pubkey: &str,
        new_role: &str,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = sqlx::query(
            "UPDATE relay_members SET role = ?, updated_at = ? \
             WHERE community_id = ? AND pubkey = ? AND role <> 'owner'",
        )
        .bind(new_role)
        .bind(now_micros())
        .bind(community.as_uuid().to_string())
        .bind(pubkey)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::sqlite::SqliteConfig;

    async fn fixture() -> (TempDir, SqliteStore) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = SqliteStore::connect(
            &directory.path().join("buzz.sqlite3"),
            &SqliteConfig::default(),
        )
        .await
        .expect("SQLite connection");
        store.migrate().await.expect("SQLite migrations");
        (directory, store)
    }

    #[tokio::test]
    async fn configured_community_is_idempotent_and_case_insensitive() {
        let (_directory, store) = fixture().await;
        let created = store
            .ensure_configured_community("relay.example.test")
            .await
            .expect("create community");
        assert!(created.created);
        let repeated = store
            .ensure_configured_community("RELAY.EXAMPLE.TEST")
            .await
            .expect("repeat community");
        assert!(!repeated.created);
        assert_eq!(repeated.id, created.id);
        assert_eq!(repeated.host, "relay.example.test");
        assert_eq!(
            store
                .lookup_community_by_host("Relay.Example.Test")
                .await
                .expect("lookup")
                .expect("community")
                .id,
            created.id
        );
        assert!(store
            .is_community_active(created.id)
            .await
            .expect("active status"));
    }

    #[tokio::test]
    async fn concurrent_community_ensure_creates_one_row() {
        let (_directory, store) = fixture().await;
        let store = Arc::new(store);
        let first_store = Arc::clone(&store);
        let first = tokio::spawn(async move {
            first_store
                .ensure_configured_community("concurrent.example.test")
                .await
        });
        let second_store = Arc::clone(&store);
        let second = tokio::spawn(async move {
            second_store
                .ensure_configured_community("concurrent.example.test")
                .await
        });
        let first = first.await.expect("first task").expect("first ensure");
        let second = second.await.expect("second task").expect("second ensure");

        assert_eq!(first.id, second.id);
        assert_ne!(first.created, second.created);
    }

    #[tokio::test]
    async fn create_with_owner_distinguishes_retry_collision_and_limit() {
        let (_directory, store) = fixture().await;
        let owner = "71".repeat(32);
        let other = "72".repeat(32);

        let first = store
            .create_community_with_owner("one.example.test", &owner)
            .await
            .expect("first create");
        let first_id = match first {
            CreateCommunityWithOwnerResult::Created(record) => record.id,
            result => panic!("unexpected first-create result: {result:?}"),
        };
        assert!(store
            .is_relay_member(first_id, &owner)
            .await
            .expect("owner"));
        assert!(matches!(
            store
                .create_community_with_owner("ONE.EXAMPLE.TEST", &owner)
                .await
                .expect("retry"),
            CreateCommunityWithOwnerResult::Created(_)
        ));
        assert_eq!(
            store
                .create_community_with_owner("one.example.test", &other)
                .await
                .expect("host collision"),
            CreateCommunityWithOwnerResult::HostExists
        );

        for host in ["two.example.test", "three.example.test"] {
            assert!(matches!(
                store
                    .create_community_with_owner(host, &owner)
                    .await
                    .expect("within owner limit"),
                CreateCommunityWithOwnerResult::Created(_)
            ));
        }
        assert_eq!(
            store
                .create_community_with_owner("four.example.test", &owner)
                .await
                .expect("owner limit"),
            CreateCommunityWithOwnerResult::LimitReached
        );
    }

    #[tokio::test]
    async fn membership_lifecycle_is_scoped_and_policy_atomic() {
        let (_directory, store) = fixture().await;
        let community_a = store
            .ensure_configured_community("a.members.example.test")
            .await
            .expect("community A")
            .id;
        let community_b = store
            .ensure_configured_community("b.members.example.test")
            .await
            .expect("community B")
            .id;
        let member = "81".repeat(32);
        let policy = "91".repeat(32);

        assert!(store
            .claim_relay_membership(community_a, &member, "member", Some(&policy),)
            .await
            .expect("claim"));
        assert!(store
            .has_join_policy_acceptance(community_a, &member, &policy)
            .await
            .expect("policy A"));
        assert!(!store
            .has_join_policy_acceptance(community_b, &member, &policy)
            .await
            .expect("policy B"));
        assert!(store
            .add_relay_member(community_b, &member, "member", None)
            .await
            .expect("member B"));
        assert!(!store
            .add_relay_member(community_b, &member, "admin", None)
            .await
            .expect("idempotent member B"));
        assert_eq!(
            store
                .remove_relay_member_if_role(community_a, &member, "admin")
                .await
                .expect("stale role"),
            RemoveResult::RoleMismatch
        );
        assert!(store
            .update_relay_member_role(community_a, &member, "admin")
            .await
            .expect("promote"));
        let record = store
            .get_relay_member(community_a, &member)
            .await
            .expect("member read")
            .expect("member exists");
        assert_eq!(record.role, "admin");
        assert_eq!(
            store
                .remove_relay_member_if_role(community_a, &member, "admin")
                .await
                .expect("remove"),
            RemoveResult::Removed
        );
        assert!(!store
            .has_join_policy_acceptance(community_a, &member, &policy)
            .await
            .expect("policy removed"));
        assert!(store
            .is_relay_member(community_b, &member)
            .await
            .expect("community B remains"));
    }

    #[tokio::test]
    async fn owners_cannot_be_removed_or_demoted() {
        let (_directory, store) = fixture().await;
        let owner = "a1".repeat(32);
        let community = match store
            .create_community_with_owner("owner.example.test", &owner)
            .await
            .expect("community")
        {
            CreateCommunityWithOwnerResult::Created(record) => record.id,
            result => panic!("unexpected create result: {result:?}"),
        };

        assert_eq!(
            store
                .remove_relay_member(community, &owner)
                .await
                .expect("owner removal"),
            RemoveResult::IsOwner
        );
        assert!(!store
            .update_relay_member_role(community, &owner, "admin")
            .await
            .expect("owner demotion"));
        assert_eq!(
            store
                .list_relay_members(community)
                .await
                .expect("member list")
                .len(),
            1
        );
    }
}
