//! Shared community/membership behavior contract for relational backends.

use async_trait::async_trait;
use uuid::Uuid;

use super::{SqliteConfig, SqliteStore};
use crate::relay_members::{RelayMember, RemoveResult};
use crate::{CommunityId, CommunityRecord, Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait CommunityMembershipContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn lookup_community(&self, host: &str) -> Result<Option<CommunityRecord>>;
    async fn add_member(&self, community: CommunityId, pubkey: &str, role: &str) -> Result<bool>;
    async fn get_member(&self, community: CommunityId, pubkey: &str)
        -> Result<Option<RelayMember>>;
    async fn claim_member(
        &self,
        community: CommunityId,
        pubkey: &str,
        policy: &str,
    ) -> Result<bool>;
    async fn has_policy(&self, community: CommunityId, pubkey: &str, policy: &str) -> Result<bool>;
    async fn update_role(&self, community: CommunityId, pubkey: &str, role: &str) -> Result<bool>;
    async fn remove_member(
        &self,
        community: CommunityId,
        pubkey: &str,
        role: &str,
    ) -> Result<RemoveResult>;
}

#[async_trait]
impl CommunityMembershipContract for SqliteStore {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
        self.ensure_configured_community(host).await
    }

    async fn lookup_community(&self, host: &str) -> Result<Option<CommunityRecord>> {
        self.lookup_community_by_host(host).await
    }

    async fn add_member(&self, community: CommunityId, pubkey: &str, role: &str) -> Result<bool> {
        self.add_relay_member(community, pubkey, role, None).await
    }

    async fn get_member(
        &self,
        community: CommunityId,
        pubkey: &str,
    ) -> Result<Option<RelayMember>> {
        self.get_relay_member(community, pubkey).await
    }

    async fn claim_member(
        &self,
        community: CommunityId,
        pubkey: &str,
        policy: &str,
    ) -> Result<bool> {
        self.claim_relay_membership(community, pubkey, "member", Some(policy))
            .await
    }

    async fn has_policy(&self, community: CommunityId, pubkey: &str, policy: &str) -> Result<bool> {
        self.has_join_policy_acceptance(community, pubkey, policy)
            .await
    }

    async fn update_role(&self, community: CommunityId, pubkey: &str, role: &str) -> Result<bool> {
        self.update_relay_member_role(community, pubkey, role).await
    }

    async fn remove_member(
        &self,
        community: CommunityId,
        pubkey: &str,
        role: &str,
    ) -> Result<RemoveResult> {
        self.remove_relay_member_if_role(community, pubkey, role)
            .await
    }
}

#[async_trait]
impl CommunityMembershipContract for Db {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
        self.ensure_configured_community(host).await
    }

    async fn lookup_community(&self, host: &str) -> Result<Option<CommunityRecord>> {
        self.lookup_community_by_host(host).await
    }

    async fn add_member(&self, community: CommunityId, pubkey: &str, role: &str) -> Result<bool> {
        self.add_relay_member(community, pubkey, role, None).await
    }

    async fn get_member(
        &self,
        community: CommunityId,
        pubkey: &str,
    ) -> Result<Option<RelayMember>> {
        self.get_relay_member(community, pubkey).await
    }

    async fn claim_member(
        &self,
        community: CommunityId,
        pubkey: &str,
        policy: &str,
    ) -> Result<bool> {
        self.claim_relay_membership(community, pubkey, "member", Some(policy))
            .await
    }

    async fn has_policy(&self, community: CommunityId, pubkey: &str, policy: &str) -> Result<bool> {
        self.has_join_policy_acceptance(community, pubkey, policy)
            .await
    }

    async fn update_role(&self, community: CommunityId, pubkey: &str, role: &str) -> Result<bool> {
        self.update_relay_member_role(community, pubkey, role).await
    }

    async fn remove_member(
        &self,
        community: CommunityId,
        pubkey: &str,
        role: &str,
    ) -> Result<RemoveResult> {
        self.remove_relay_member_if_role(community, pubkey, role)
            .await
    }
}

async fn run_contract(store: &impl CommunityMembershipContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let host_a = format!("contract-a-{suffix}.example.test");
    let host_b = format!("contract-b-{suffix}.example.test");
    let first = store
        .ensure_community(&host_a)
        .await
        .expect("first community");
    assert!(first.created);
    let repeated = store
        .ensure_community(&host_a.to_ascii_uppercase())
        .await
        .expect("repeated community");
    assert!(!repeated.created);
    assert_eq!(repeated.id, first.id);
    let second = store
        .ensure_community(&host_b)
        .await
        .expect("second community");
    assert_eq!(
        store
            .lookup_community(&host_a)
            .await
            .expect("lookup")
            .expect("community")
            .id,
        first.id
    );

    let member = "e1".repeat(32);
    assert!(store
        .add_member(first.id, &member, "member")
        .await
        .expect("add first"));
    assert!(!store
        .add_member(first.id, &member, "admin")
        .await
        .expect("repeat first"));
    assert!(store
        .add_member(second.id, &member, "member")
        .await
        .expect("add second"));
    assert_eq!(
        store
            .get_member(first.id, &member)
            .await
            .expect("read first")
            .expect("member")
            .role,
        "member"
    );

    let invited = "e2".repeat(32);
    let policy = "e3".repeat(32);
    assert!(store
        .claim_member(first.id, &invited, &policy)
        .await
        .expect("claim"));
    assert!(store
        .has_policy(first.id, &invited, &policy)
        .await
        .expect("policy first"));
    assert!(!store
        .has_policy(second.id, &invited, &policy)
        .await
        .expect("policy second"));
    assert!(store
        .update_role(first.id, &invited, "admin")
        .await
        .expect("promote"));
    assert_eq!(
        store
            .remove_member(first.id, &invited, "member")
            .await
            .expect("stale remove"),
        RemoveResult::RoleMismatch
    );
    assert_eq!(
        store
            .remove_member(first.id, &invited, "admin")
            .await
            .expect("remove"),
        RemoveResult::Removed
    );
    assert!(!store
        .has_policy(first.id, &invited, &policy)
        .await
        .expect("policy cascade"));
    assert!(store
        .get_member(second.id, &member)
        .await
        .expect("second remains")
        .is_some());
}

#[tokio::test]
async fn sqlite_community_membership_contract() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = SqliteStore::connect(
        &directory.path().join("buzz.sqlite3"),
        &SqliteConfig::default(),
    )
    .await
    .expect("SQLite connection");
    store.migrate().await.expect("SQLite migrations");
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_community_membership_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
