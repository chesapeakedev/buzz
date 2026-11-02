//! Shared NIP-34 repository-name registry contract.

use async_trait::async_trait;

use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::git_repo::ReserveOutcome;
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait GitRepoContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn owner(&self, community: CommunityId, repo: &str) -> Result<Option<String>>;
    async fn reserve(
        &self,
        community: CommunityId,
        repo: &str,
        owner: &str,
    ) -> Result<ReserveOutcome>;
    async fn count(&self, community: CommunityId, owner: &str) -> Result<i64>;
    async fn release(&self, community: CommunityId, repo: &str, owner: &str) -> Result<u64>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl GitRepoContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn owner(&self, community: CommunityId, repo: &str) -> Result<Option<String>> {
                self.repo_name_owner(community, repo).await
            }

            async fn reserve(
                &self,
                community: CommunityId,
                repo: &str,
                owner: &str,
            ) -> Result<ReserveOutcome> {
                self.reserve_repo_name(community, repo, owner).await
            }

            async fn count(&self, community: CommunityId, owner: &str) -> Result<i64> {
                self.count_repos_for_owner(community, owner).await
            }

            async fn release(
                &self,
                community: CommunityId,
                repo: &str,
                owner: &str,
            ) -> Result<u64> {
                self.release_repo_name(community, repo, owner).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl GitRepoContract) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("git-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("git-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let owner_a = format!("{:0>64}", format!("a{suffix}"));
    let owner_b = format!("{:0>64}", format!("b{suffix}"));
    let repo = format!("shared-{suffix}");

    assert_eq!(
        store.owner(community_a, &repo).await.expect("free name"),
        None
    );
    assert_eq!(
        store
            .reserve(community_a, &repo, &owner_a)
            .await
            .expect("reserve A"),
        ReserveOutcome::Reserved
    );
    assert_eq!(
        store
            .reserve(community_a, &repo, &owner_a)
            .await
            .expect("idempotent reserve"),
        ReserveOutcome::AlreadyOwned
    );
    assert_eq!(
        store
            .reserve(community_a, &repo, &owner_b)
            .await
            .expect("cross-owner collision"),
        ReserveOutcome::TakenByOther
    );
    assert_eq!(
        store.owner(community_a, &repo).await.expect("owner A"),
        Some(owner_a.clone())
    );
    assert_eq!(
        store
            .count(community_a, &owner_a)
            .await
            .expect("owner A count"),
        1
    );
    assert_eq!(
        store
            .count(community_a, &owner_b)
            .await
            .expect("loser count"),
        0
    );

    assert_eq!(
        store
            .reserve(community_b, &repo, &owner_b)
            .await
            .expect("same name in B"),
        ReserveOutcome::Reserved
    );
    assert_eq!(
        store.owner(community_b, &repo).await.expect("owner B"),
        Some(owner_b.clone())
    );
    assert_eq!(
        store
            .release(community_a, &repo, &owner_b)
            .await
            .expect("stranger release"),
        0
    );
    assert_eq!(
        store
            .release(community_a, &repo, &owner_a)
            .await
            .expect("owner release"),
        1
    );
    assert_eq!(
        store
            .reserve(community_a, &repo, &owner_b)
            .await
            .expect("reclaim"),
        ReserveOutcome::Reserved
    );
    assert_eq!(
        store
            .owner(community_b, &repo)
            .await
            .expect("B unaffected by A release"),
        Some(owner_b.clone())
    );

    let collision_repo = format!("collision-{suffix}");
    let collision_a = store.reserve(community_a, &collision_repo, &owner_a);
    let collision_b = store.reserve(community_a, &collision_repo, &owner_b);
    let (collision_a, collision_b) = tokio::join!(collision_a, collision_b);
    let collision_outcomes = [
        collision_a.expect("collision racer A"),
        collision_b.expect("collision racer B"),
    ];
    assert_eq!(
        collision_outcomes
            .iter()
            .filter(|outcome| **outcome == ReserveOutcome::Reserved)
            .count(),
        1
    );
    assert_eq!(
        collision_outcomes
            .iter()
            .filter(|outcome| **outcome == ReserveOutcome::TakenByOther)
            .count(),
        1
    );

    let idempotent_repo = format!("idempotent-{suffix}");
    let idempotent_a = store.reserve(community_b, &idempotent_repo, &owner_a);
    let idempotent_b = store.reserve(community_b, &idempotent_repo, &owner_a);
    let (idempotent_a, idempotent_b) = tokio::join!(idempotent_a, idempotent_b);
    let idempotent_outcomes = [
        idempotent_a.expect("idempotent racer A"),
        idempotent_b.expect("idempotent racer B"),
    ];
    assert_eq!(
        idempotent_outcomes
            .iter()
            .filter(|outcome| **outcome == ReserveOutcome::Reserved)
            .count(),
        1
    );
    assert_eq!(
        idempotent_outcomes
            .iter()
            .filter(|outcome| **outcome == ReserveOutcome::AlreadyOwned)
            .count(),
        1
    );
    assert_eq!(
        store
            .count(community_b, &owner_a)
            .await
            .expect("idempotent race count"),
        1
    );
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
async fn sqlite_git_repo_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_git_repo_contract() {
    let admin = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/postgres")
        .await
        .expect("PostgreSQL admin connection");
    let database = format!("buzz_git_contract_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
        .execute(&admin)
        .await
        .expect("create scratch database");
    let url = format!("postgres://buzz:buzz_dev@localhost:5432/{database}");
    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("scratch PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
    db.postgres_pool().close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database} WITH (FORCE)"
    )))
    .execute(&admin)
    .await
    .expect("drop scratch database");
}
