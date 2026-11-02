//! Shared identity and API-token behavior contract for relational backends.

use async_trait::async_trait;
use uuid::Uuid;

use super::{SqliteConfig, SqliteStore};
use crate::user::{UserProfile, UserSearchProfile};
use crate::{ApiTokenRecord, CommunityId, Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait IdentityAuthContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn update_profile(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        display_name: &str,
        nip05: &str,
    ) -> Result<()>;
    async fn get_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<Option<UserProfile>>;
    async fn search_users(
        &self,
        community: CommunityId,
        query: &str,
    ) -> Result<Vec<UserSearchProfile>>;
    async fn create_token(
        &self,
        community: CommunityId,
        hash: &[u8],
        owner: &[u8],
        name: &str,
    ) -> Result<Uuid>;
    async fn get_token(
        &self,
        community: CommunityId,
        hash: &[u8],
        include_revoked: bool,
    ) -> Result<Option<ApiTokenRecord>>;
    async fn revoke_token(&self, community: CommunityId, id: Uuid, owner: &[u8]) -> Result<bool>;
    async fn add_allowlist(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        actor: &[u8],
    ) -> Result<bool>;
    async fn is_allowed(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn archive_identity(
        &self,
        community: CommunityId,
        pubkey: &str,
        actor: &str,
        event_id: &str,
    ) -> Result<bool>;
    async fn is_archived(&self, community: CommunityId, pubkey: &str) -> Result<bool>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl IdentityAuthContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
                self.ensure_user(community, pubkey).await
            }

            async fn update_profile(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                display_name: &str,
                nip05: &str,
            ) -> Result<()> {
                self.update_user_profile(
                    community,
                    pubkey,
                    Some(display_name),
                    None,
                    None,
                    Some(nip05),
                )
                .await
            }

            async fn get_user(
                &self,
                community: CommunityId,
                pubkey: &[u8],
            ) -> Result<Option<UserProfile>> {
                self.get_user(community, pubkey).await
            }

            async fn search_users(
                &self,
                community: CommunityId,
                query: &str,
            ) -> Result<Vec<UserSearchProfile>> {
                self.search_users(community, query, 20).await
            }

            async fn create_token(
                &self,
                community: CommunityId,
                hash: &[u8],
                owner: &[u8],
                name: &str,
            ) -> Result<Uuid> {
                self.create_api_token(
                    community,
                    hash,
                    owner,
                    name,
                    &["files:read".to_owned()],
                    None,
                    None,
                )
                .await
            }

            async fn get_token(
                &self,
                community: CommunityId,
                hash: &[u8],
                include_revoked: bool,
            ) -> Result<Option<ApiTokenRecord>> {
                if include_revoked {
                    self.get_api_token_by_hash_including_revoked(community, hash)
                        .await
                } else {
                    self.get_api_token_by_hash(community, hash).await
                }
            }

            async fn revoke_token(
                &self,
                community: CommunityId,
                id: Uuid,
                owner: &[u8],
            ) -> Result<bool> {
                self.revoke_token(community, id, owner, owner).await
            }

            async fn add_allowlist(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                actor: &[u8],
            ) -> Result<bool> {
                self.add_to_allowlist(community, pubkey, actor, Some("contract"))
                    .await
            }

            async fn is_allowed(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
                self.is_pubkey_allowed(community, pubkey).await
            }

            async fn archive_identity(
                &self,
                community: CommunityId,
                pubkey: &str,
                actor: &str,
                event_id: &str,
            ) -> Result<bool> {
                self.archive(
                    community,
                    pubkey,
                    "self",
                    actor,
                    Some("contract"),
                    None,
                    event_id,
                )
                .await
            }

            async fn is_archived(&self, community: CommunityId, pubkey: &str) -> Result<bool> {
                self.is_archived(community, pubkey).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl IdentityAuthContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("identity-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("identity-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let owner = vec![0xa5; 32];
    assert!(store
        .ensure_user(community_a, &owner)
        .await
        .expect("user A"));
    assert!(!store
        .ensure_user(community_a, &owner)
        .await
        .expect("user A repeat"));
    assert!(store
        .ensure_user(community_b, &owner)
        .await
        .expect("user B"));
    store
        .update_profile(community_a, &owner, "Contract Alice", "alice@contract.test")
        .await
        .expect("profile A");
    assert_eq!(
        store
            .get_user(community_a, &owner)
            .await
            .expect("read A")
            .expect("user A")
            .display_name
            .as_deref(),
        Some("Contract Alice")
    );
    assert_eq!(
        store
            .get_user(community_b, &owner)
            .await
            .expect("read B")
            .expect("user B")
            .display_name,
        None
    );
    assert_eq!(
        store
            .search_users(community_a, "contract alice")
            .await
            .expect("search A")
            .len(),
        1
    );
    assert!(store
        .search_users(community_b, "contract alice")
        .await
        .expect("search B")
        .is_empty());

    let hash = vec![0xb5; 32];
    let token_a = store
        .create_token(community_a, &hash, &owner, "token A")
        .await
        .expect("token A");
    store
        .create_token(community_b, &hash, &owner, "token B")
        .await
        .expect("token B");
    assert_eq!(
        store
            .get_token(community_a, &hash, false)
            .await
            .expect("lookup A")
            .expect("token A")
            .name,
        "token A"
    );
    assert_eq!(
        store
            .get_token(community_b, &hash, false)
            .await
            .expect("lookup B")
            .expect("token B")
            .name,
        "token B"
    );
    assert!(!store
        .revoke_token(community_b, token_a, &owner)
        .await
        .expect("foreign revoke"));
    assert!(store
        .revoke_token(community_a, token_a, &owner)
        .await
        .expect("revoke A"));
    assert!(store
        .get_token(community_a, &hash, false)
        .await
        .expect("active A")
        .is_none());
    assert!(store
        .get_token(community_a, &hash, true)
        .await
        .expect("history A")
        .expect("revoked A")
        .revoked_at
        .is_some());
    assert!(store
        .get_token(community_b, &hash, false)
        .await
        .expect("active B")
        .is_some());

    let allowed = vec![0xc5; 32];
    assert!(store
        .add_allowlist(community_a, &allowed, &owner)
        .await
        .expect("allow A"));
    assert!(!store
        .is_allowed(community_b, &allowed)
        .await
        .expect("allow B"));

    let archived = "d5".repeat(32);
    let actor = hex::encode(&owner);
    let event_id = "e5".repeat(32);
    assert!(store
        .archive_identity(community_a, &archived, &actor, &event_id)
        .await
        .expect("archive A"));
    assert!(store
        .is_archived(community_a, &archived)
        .await
        .expect("archived A"));
    assert!(!store
        .is_archived(community_b, &archived)
        .await
        .expect("archived B"));
}

async fn sqlite_fixture() -> (tempfile::TempDir, SqliteStore) {
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
async fn sqlite_identity_auth_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_identity_auth_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
