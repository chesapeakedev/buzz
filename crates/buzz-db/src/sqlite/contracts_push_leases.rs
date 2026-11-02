//! Shared effective push-lease ordering and tenancy contract.

use async_trait::async_trait;
use chrono::Utc;
use nostr::{Event, EventBuilder, Keys, Kind};

use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::push::{ActiveLease, LeaseVersion, MatchLease, ReplaceLeaseOutcome};
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait PushLeaseContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool>;
    async fn match_count(&self, community: CommunityId, event_id: &[u8]) -> Result<i64>;
    #[allow(clippy::too_many_arguments)]
    async fn activate(
        &self,
        community: CommunityId,
        author: &[u8],
        installation: &str,
        source_event_id: &[u8],
        source_created_at: i64,
        generation: i64,
        endpoint_hash: &[u8],
    ) -> Result<ReplaceLeaseOutcome>;
    async fn revoke(
        &self,
        community: CommunityId,
        author: &[u8],
        installation: &str,
        source_event_id: &[u8],
        source_created_at: i64,
        generation: i64,
    ) -> Result<ReplaceLeaseOutcome>;
    async fn active(&self, community: CommunityId) -> Result<Vec<MatchLease>>;
}

macro_rules! impl_sqlite_contract {
    () => {
        #[async_trait]
        impl PushLeaseContract for SqliteStore {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
                self.ensure_user(community, pubkey).await
            }

            async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool> {
                Ok(self.insert_event(community, event, None).await?.1)
            }

            async fn match_count(
                &self,
                community: CommunityId,
                event_id: &[u8],
            ) -> Result<i64> {
                sqlx::query_scalar(
                    "SELECT count(*) FROM push_match_queue \
                     WHERE community_id = ? AND event_id = ?",
                )
                .bind(community.as_uuid().to_string())
                .bind(event_id)
                .fetch_one(&self.adapter_pool())
                .await
                .map_err(Into::into)
            }

        async fn activate(
            &self,
            community: CommunityId,
            author: &[u8],
            installation: &str,
            source_event_id: &[u8],
            source_created_at: i64,
            generation: i64,
            endpoint_hash: &[u8],
        ) -> Result<ReplaceLeaseOutcome> {
            let subscriptions = serde_json::json!([{"kinds":[9]}]);
            self.replace_active_lease(
                community,
                author,
                installation,
                LeaseVersion {
                    source_event_id,
                    source_created_at,
                    generation,
                    expires_at: Utc::now().timestamp() + 3600,
                },
                ActiveLease {
                    app_profile: "ios-production",
                    endpoint_hash,
                    endpoint_grant: "opaque-contract-grant",
                    max_class: "default",
                    subscriptions: &subscriptions,
                },
            )
            .await
        }

        async fn revoke(
            &self,
            community: CommunityId,
            author: &[u8],
            installation: &str,
            source_event_id: &[u8],
            source_created_at: i64,
            generation: i64,
        ) -> Result<ReplaceLeaseOutcome> {
            self.revoke_lease(
                community,
                author,
                installation,
                LeaseVersion {
                    source_event_id,
                    source_created_at,
                    generation,
                    expires_at: Utc::now().timestamp() + 3600,
                },
            )
            .await
        }

        async fn active(&self, community: CommunityId) -> Result<Vec<MatchLease>> {
            self.active_push_match_leases(community).await
        }
        }
    };
}

impl_sqlite_contract!();

#[async_trait]
impl PushLeaseContract for Db {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
        self.ensure_configured_community(host).await
    }

    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
        self.ensure_user(community, pubkey).await
    }

    async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool> {
        Ok(self.insert_event(community, event, None).await?.1)
    }

    async fn match_count(&self, community: CommunityId, event_id: &[u8]) -> Result<i64> {
        sqlx::query_scalar(
            "SELECT count(*) FROM push_match_queue WHERE community_id = $1 AND event_id = $2",
        )
        .bind(community.as_uuid())
        .bind(event_id)
        .fetch_one(self.postgres_pool())
        .await
        .map_err(Into::into)
    }

    async fn activate(
        &self,
        community: CommunityId,
        author: &[u8],
        installation: &str,
        source_event_id: &[u8],
        source_created_at: i64,
        generation: i64,
        endpoint_hash: &[u8],
    ) -> Result<ReplaceLeaseOutcome> {
        let subscriptions = serde_json::json!([{"kinds":[9]}]);
        crate::push::replace_active_lease(
            self.postgres_pool(),
            community,
            author,
            installation,
            LeaseVersion {
                source_event_id,
                source_created_at,
                generation,
                expires_at: Utc::now().timestamp() + 3600,
            },
            ActiveLease {
                app_profile: "ios-production",
                endpoint_hash,
                endpoint_grant: "opaque-contract-grant",
                max_class: "default",
                subscriptions: &subscriptions,
            },
        )
        .await
    }

    async fn revoke(
        &self,
        community: CommunityId,
        author: &[u8],
        installation: &str,
        source_event_id: &[u8],
        source_created_at: i64,
        generation: i64,
    ) -> Result<ReplaceLeaseOutcome> {
        crate::push::revoke_lease(
            self.postgres_pool(),
            community,
            author,
            installation,
            LeaseVersion {
                source_event_id,
                source_created_at,
                generation,
                expires_at: Utc::now().timestamp() + 3600,
            },
        )
        .await
    }

    async fn active(&self, community: CommunityId) -> Result<Vec<MatchLease>> {
        self.active_push_match_leases(community).await
    }
}

async fn run_contract(store: &impl PushLeaseContract) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("push-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("push-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let author = [0x81; 32];
    let endpoint = [0x82; 32];
    let source_1 = [0x83; 32];
    let source_2 = [0x84; 32];
    let source_3 = [0x85; 32];
    let base = Utc::now().timestamp();
    let keys = Keys::generate();
    let event_author = keys.public_key().to_bytes();
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &event_author)
            .await
            .expect("event author");
    }

    let before_activation = EventBuilder::new(Kind::Custom(9), "before activation")
        .sign_with_keys(&keys)
        .expect("signed pre-activation event");
    assert!(store
        .insert_event(community_a, &before_activation)
        .await
        .expect("insert before activation"));
    assert_eq!(
        store
            .match_count(community_a, before_activation.id.as_bytes())
            .await
            .expect("pre-activation match count"),
        0,
        "the event trigger must skip communities without an eligible lease"
    );

    for community in [community_a, community_b] {
        assert_eq!(
            store
                .activate(community, &author, "primary", &source_1, base, 1, &endpoint,)
                .await
                .expect("initial activation"),
            ReplaceLeaseOutcome::Accepted
        );
    }
    assert_eq!(
        store
            .match_count(community_a, before_activation.id.as_bytes())
            .await
            .expect("activation backfill"),
        1,
        "activation must backfill recent events skipped by the lease gate"
    );
    let after_activation = EventBuilder::new(Kind::Custom(9), "after activation")
        .sign_with_keys(&keys)
        .expect("signed post-activation event");
    assert!(store
        .insert_event(community_a, &after_activation)
        .await
        .expect("insert after activation"));
    assert_eq!(
        store
            .match_count(community_a, after_activation.id.as_bytes())
            .await
            .expect("post-activation match count"),
        1,
        "the event transaction must durably enqueue matching work"
    );
    for community in [community_a, community_b] {
        let active = store.active(community).await.expect("active leases");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].generation, 1);
        assert_eq!(active[0].subscriptions, serde_json::json!([{"kinds":[9]}]));
    }

    assert_eq!(
        store
            .activate(
                community_a,
                &author,
                "primary",
                &source_2,
                base - 1,
                99,
                &endpoint,
            )
            .await
            .expect("stale event"),
        ReplaceLeaseOutcome::StaleEvent
    );
    assert_eq!(
        store
            .activate(
                community_a,
                &author,
                "primary",
                &source_2,
                base + 1,
                1,
                &endpoint,
            )
            .await
            .expect("stale generation"),
        ReplaceLeaseOutcome::StaleGeneration
    );
    assert_eq!(
        store
            .revoke(community_a, &author, "primary", &source_2, base + 2, 2,)
            .await
            .expect("revoke A"),
        ReplaceLeaseOutcome::Accepted
    );
    assert!(store
        .active(community_a)
        .await
        .expect("inactive A")
        .is_empty());
    assert_eq!(
        store.active(community_b).await.expect("active B").len(),
        1,
        "revoking A must not affect the same lease address in B"
    );

    assert_eq!(
        store
            .activate(
                community_a,
                &author,
                "primary",
                &source_3,
                base + 3,
                3,
                &endpoint,
            )
            .await
            .expect("reactivate A"),
        ReplaceLeaseOutcome::Accepted
    );
    assert_eq!(
        store.active(community_a).await.expect("reactivated A")[0].generation,
        3
    );

    assert!(
        store
            .activate(
                community_a,
                &author,
                "duplicate-endpoint",
                &[0x86; 32],
                base + 4,
                1,
                &endpoint,
            )
            .await
            .is_err(),
        "one author cannot actively lease the same endpoint tuple twice"
    );

    let race_a = store.activate(
        community_a,
        &author,
        "race",
        &[0x90; 32],
        base + 10,
        1,
        &[0x91; 32],
    );
    let race_b = store.activate(
        community_a,
        &author,
        "race",
        &[0x8f; 32],
        base + 10,
        1,
        &[0x92; 32],
    );
    let (race_a, race_b) = tokio::join!(race_a, race_b);
    assert_ne!(
        race_a.expect("race A"),
        race_b.expect("race B"),
        "one concurrent initial replacement must win"
    );
    assert_eq!(
        store
            .active(community_a)
            .await
            .expect("active after race")
            .iter()
            .filter(|lease| lease.installation_id == "race")
            .count(),
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
async fn sqlite_push_lease_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_push_lease_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
