//! SQLite community registry and relay-membership operations.

use chrono::{DateTime, Utc};
use sqlx::{Connection as _, Row as _};
use uuid::Uuid;

use super::SqliteStore;
use crate::relay_members::{RelayMember, RemoveResult, TransferResult};
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

    /// Ensure the configured owner is the sole owner of one community.
    pub async fn bootstrap_owner(&self, community: CommunityId, owner_pubkey: &str) -> Result<()> {
        let owner_pubkey = owner_pubkey.to_ascii_lowercase();
        let community = community.as_uuid().to_string();
        let timestamp = now_micros();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, added_by, created_at, updated_at) \
             VALUES (?, ?, 'owner', NULL, ?, ?) \
             ON CONFLICT (community_id, pubkey) DO UPDATE \
             SET role = 'owner', updated_at = excluded.updated_at",
        )
        .bind(&community)
        .bind(&owner_pubkey)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE relay_members SET role = 'admin', updated_at = ? \
             WHERE community_id = ? AND role = 'owner' AND pubkey <> ?",
        )
        .bind(timestamp)
        .bind(&community)
        .bind(&owner_pubkey)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Return whether one community has at least one administrator or owner.
    pub async fn has_admin_or_owner(&self, community: CommunityId) -> Result<bool> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM relay_members \
             WHERE community_id = ? AND role IN ('admin', 'owner'))",
        )
        .bind(community.as_uuid().to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }

    /// Atomically transfer community ownership after validating the expected owner.
    pub async fn transfer_ownership(
        &self,
        community: CommunityId,
        new_owner_pubkey: &str,
        expected_owner_pubkey: &str,
    ) -> Result<TransferResult> {
        let new_owner = new_owner_pubkey.to_ascii_lowercase();
        let expected_owner = expected_owner_pubkey.to_ascii_lowercase();
        let community = community.as_uuid().to_string();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let owners: Vec<String> = sqlx::query_scalar(
            "SELECT pubkey FROM relay_members \
             WHERE community_id = ? AND role = 'owner' ORDER BY pubkey",
        )
        .bind(&community)
        .fetch_all(&mut *transaction)
        .await?;

        if owners.is_empty() {
            transaction.rollback().await?;
            return Ok(TransferResult::NoOwner);
        }
        if !owners.iter().any(|owner| owner == &expected_owner) {
            transaction.rollback().await?;
            return Ok(TransferResult::OwnerConflict);
        }
        if owners.len() == 1 && owners[0] == new_owner {
            transaction.rollback().await?;
            return Ok(TransferResult::AlreadyOwner);
        }
        let previous_owner = if owners.len() == 1 {
            Some(owners[0].clone())
        } else {
            owners.iter().find(|owner| **owner != new_owner).cloned()
        };
        let owned_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM relay_members WHERE pubkey = ? AND role = 'owner'",
        )
        .bind(&new_owner)
        .fetch_one(&mut *transaction)
        .await?;
        if owned_count >= crate::relay_members::max_communities_per_owner() {
            transaction.rollback().await?;
            return Ok(TransferResult::LimitReached);
        }

        let timestamp = now_micros();
        sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, added_by, created_at, updated_at) \
             VALUES (?, ?, 'owner', NULL, ?, ?) \
             ON CONFLICT (community_id, pubkey) DO UPDATE \
             SET role = 'owner', updated_at = excluded.updated_at",
        )
        .bind(&community)
        .bind(&new_owner)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE relay_members SET role = 'member', updated_at = ? \
             WHERE community_id = ? AND role = 'owner' AND pubkey <> ?",
        )
        .bind(timestamp)
        .bind(&community)
        .bind(&new_owner)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(TransferResult::Transferred { previous_owner })
    }

    /// One-time conversion of a community's legacy allowlist to relay membership.
    pub async fn backfill_from_allowlist(&self, community: CommunityId) -> Result<u64> {
        let community = community.as_uuid().to_string();
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let has_members: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM relay_members WHERE community_id = ?)")
                .bind(&community)
                .fetch_one(&mut *transaction)
                .await?;
        if has_members != 0 {
            transaction.rollback().await?;
            return Ok(0);
        }
        let result = sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, added_by, created_at, updated_at) \
             SELECT ?, lower(hex(pubkey)), 'member', NULL, added_at, added_at \
             FROM pubkey_allowlist WHERE community_id = ? \
             ON CONFLICT (community_id, pubkey) DO NOTHING",
        )
        .bind(&community)
        .bind(&community)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result.rows_affected())
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

        for index in 2..=crate::relay_members::MAX_COMMUNITIES_PER_OWNER {
            let host = format!("{index}.example.test");
            assert!(matches!(
                store
                    .create_community_with_owner(&host, &owner)
                    .await
                    .expect("within owner limit"),
                CreateCommunityWithOwnerResult::Created(_)
            ));
        }
        assert_eq!(
            store
                .create_community_with_owner("over-limit.example.test", &owner)
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

    #[tokio::test]
    async fn bootstrap_rotates_only_the_selected_community_owner() {
        let (_directory, store) = fixture().await;
        let community_a = store
            .ensure_configured_community("bootstrap-a.example.test")
            .await
            .expect("community A")
            .id;
        let community_b = store
            .ensure_configured_community("bootstrap-b.example.test")
            .await
            .expect("community B")
            .id;
        let first_owner = "b1".repeat(32);
        let second_owner = "b2".repeat(32);
        store
            .bootstrap_owner(community_a, &first_owner)
            .await
            .expect("first owner A");
        store
            .bootstrap_owner(community_b, &first_owner)
            .await
            .expect("owner B");
        store
            .bootstrap_owner(community_a, &second_owner)
            .await
            .expect("rotated owner A");

        assert_eq!(
            store
                .get_relay_member(community_a, &first_owner)
                .await
                .expect("old owner A")
                .expect("member")
                .role,
            "admin"
        );
        assert_eq!(
            store
                .get_relay_member(community_a, &second_owner)
                .await
                .expect("new owner A")
                .expect("member")
                .role,
            "owner"
        );
        assert_eq!(
            store
                .get_relay_member(community_b, &first_owner)
                .await
                .expect("owner B")
                .expect("member")
                .role,
            "owner"
        );
    }

    #[tokio::test]
    async fn ownership_transfer_rejects_stale_owner_and_preserves_other_tenants() {
        let (_directory, store) = fixture().await;
        let original = "c1".repeat(32);
        let replacement = "c2".repeat(32);
        let community_a = match store
            .create_community_with_owner("transfer-a.example.test", &original)
            .await
            .expect("community A")
        {
            CreateCommunityWithOwnerResult::Created(record) => record.id,
            result => panic!("unexpected create result: {result:?}"),
        };
        let community_b = match store
            .create_community_with_owner("transfer-b.example.test", &original)
            .await
            .expect("community B")
        {
            CreateCommunityWithOwnerResult::Created(record) => record.id,
            result => panic!("unexpected create result: {result:?}"),
        };

        assert_eq!(
            store
                .transfer_ownership(community_a, &replacement, &original)
                .await
                .expect("transfer"),
            TransferResult::Transferred {
                previous_owner: Some(original.clone())
            }
        );
        assert_eq!(
            store
                .transfer_ownership(community_a, &original, &original)
                .await
                .expect("stale transfer"),
            TransferResult::OwnerConflict
        );
        assert_eq!(
            store
                .transfer_ownership(community_a, &replacement, &replacement)
                .await
                .expect("idempotent transfer"),
            TransferResult::AlreadyOwner
        );
        assert_eq!(
            store
                .get_relay_member(community_a, &original)
                .await
                .expect("old owner A")
                .expect("member")
                .role,
            "member"
        );
        assert_eq!(
            store
                .get_relay_member(community_b, &original)
                .await
                .expect("owner B")
                .expect("member")
                .role,
            "owner"
        );

        let ownerless = store
            .ensure_configured_community("ownerless.example.test")
            .await
            .expect("ownerless")
            .id;
        assert_eq!(
            store
                .transfer_ownership(ownerless, &replacement, &original)
                .await
                .expect("ownerless transfer"),
            TransferResult::NoOwner
        );
    }

    #[tokio::test]
    async fn allowlist_backfill_is_scoped_and_runs_only_before_membership_exists() {
        let (_directory, store) = fixture().await;
        let community_a = store
            .ensure_configured_community("backfill-a.example.test")
            .await
            .expect("community A")
            .id;
        let community_b = store
            .ensure_configured_community("backfill-b.example.test")
            .await
            .expect("community B")
            .id;
        let allowed_a = vec![0xd1; 32];
        let allowed_b = vec![0xd2; 32];
        let actor = vec![0xd3; 32];
        store
            .add_to_allowlist(community_a, &allowed_a, &actor, None)
            .await
            .expect("allow A");
        store
            .add_to_allowlist(community_b, &allowed_b, &actor, None)
            .await
            .expect("allow B");

        assert_eq!(
            store
                .backfill_from_allowlist(community_a)
                .await
                .expect("backfill A"),
            1
        );
        assert!(store
            .is_relay_member(community_a, &hex::encode(&allowed_a))
            .await
            .expect("member A"));
        assert!(!store
            .is_relay_member(community_a, &hex::encode(&allowed_b))
            .await
            .expect("foreign member"));
        assert_eq!(
            store
                .backfill_from_allowlist(community_a)
                .await
                .expect("repeat backfill"),
            0
        );
        assert!(!store
            .is_relay_member(community_b, &hex::encode(&allowed_b))
            .await
            .expect("B remains untouched"));
    }
}
