//! Shared durable push wake-outbox contract.

use async_trait::async_trait;
use chrono::{Duration, Utc};
use nostr::{Event, EventBuilder, Keys, Kind};
use uuid::Uuid;

use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::push::{
    ActiveLease, ClaimedWake, EnqueueWakeOutcome, LeaseVersion, NewWake, ReplaceLeaseOutcome,
    RevalidateWakeOutcome, WakeRequest,
};
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait PushOutboxContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn activate(
        &self,
        community: CommunityId,
        author: &[u8],
        source: &[u8],
        endpoint: &[u8],
        generation: i64,
    ) -> Result<ReplaceLeaseOutcome>;
    async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool>;
    async fn enqueue_one(
        &self,
        community: CommunityId,
        author: &[u8],
        event: &[u8],
        generation: i64,
    ) -> Result<EnqueueWakeOutcome>;
    async fn enqueue_many(
        &self,
        community: CommunityId,
        requests: &[WakeRequest],
    ) -> Result<Vec<EnqueueWakeOutcome>>;
    async fn claim(&self, community: CommunityId) -> Result<Vec<ClaimedWake>>;
    async fn revalidate(
        &self,
        community: CommunityId,
        id: Uuid,
        claim: Uuid,
    ) -> Result<RevalidateWakeOutcome>;
    async fn complete(&self, community: CommunityId, id: Uuid, claim: Uuid) -> Result<bool>;
    async fn retry(&self, community: CommunityId, id: Uuid, claim: Uuid) -> Result<bool>;
    async fn fail(&self, community: CommunityId, id: Uuid, claim: Uuid) -> Result<bool>;
    async fn disable(&self, community: CommunityId, author: &[u8], generation: i64)
        -> Result<bool>;
    async fn clear_match(&self, community: CommunityId, event: &[u8]) -> Result<u64>;
    async fn prune(&self, community: CommunityId) -> Result<u64>;
}

macro_rules! impl_sqlite_contract {
    () => {
        #[async_trait]
        impl PushOutboxContract for SqliteStore {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn ensure_user(
                &self,
                community: CommunityId,
                pubkey: &[u8],
            ) -> Result<bool> {
                self.ensure_user(community, pubkey).await
            }

            async fn activate(
                &self,
                community: CommunityId,
                author: &[u8],
                source: &[u8],
                endpoint: &[u8],
                generation: i64,
            ) -> Result<ReplaceLeaseOutcome> {
                let subscriptions = serde_json::json!([{"kinds":[9]}]);
                self.replace_active_lease(
                    community,
                    author,
                    "outbox",
                    LeaseVersion {
                        source_event_id: source,
                        source_created_at: Utc::now().timestamp() + generation,
                        generation,
                        expires_at: Utc::now().timestamp() + 3600,
                    },
                    ActiveLease {
                        app_profile: "ios-production",
                        endpoint_hash: endpoint,
                        endpoint_grant: "outbox-contract-grant",
                        max_class: "default",
                        subscriptions: &subscriptions,
                    },
                )
                .await
            }

            async fn insert_event(
                &self,
                community: CommunityId,
                event: &Event,
            ) -> Result<bool> {
                Ok(self.insert_event(community, event, None).await?.1)
            }

            async fn enqueue_one(
                &self,
                community: CommunityId,
                author: &[u8],
                event: &[u8],
                generation: i64,
            ) -> Result<EnqueueWakeOutcome> {
                self.enqueue_push_wake(
                    community,
                    author,
                    "outbox",
                    NewWake {
                        lease_generation: generation,
                        event_id: event,
                        class: "default",
                        expires_at: Utc::now().timestamp() + 1800,
                    },
                )
                .await
            }

            async fn enqueue_many(
                &self,
                community: CommunityId,
                requests: &[WakeRequest],
            ) -> Result<Vec<EnqueueWakeOutcome>> {
                self.enqueue_push_wakes(community, requests).await
            }

            async fn claim(&self, community: CommunityId) -> Result<Vec<ClaimedWake>> {
                self.claim_due_push_wakes(community, 10, Utc::now() + Duration::minutes(5))
                    .await
            }

            async fn revalidate(
                &self,
                community: CommunityId,
                id: Uuid,
                claim: Uuid,
            ) -> Result<RevalidateWakeOutcome> {
                self.revalidate_push_wake(community, id, claim).await
            }

            async fn complete(
                &self,
                community: CommunityId,
                id: Uuid,
                claim: Uuid,
            ) -> Result<bool> {
                self.complete_push_wake(community, id, claim).await
            }

            async fn retry(
                &self,
                community: CommunityId,
                id: Uuid,
                claim: Uuid,
            ) -> Result<bool> {
                self.retry_push_wake(
                    community,
                    id,
                    claim,
                    Utc::now() - Duration::seconds(1),
                )
                .await
            }

            async fn fail(
                &self,
                community: CommunityId,
                id: Uuid,
                claim: Uuid,
            ) -> Result<bool> {
                self.fail_push_wake(community, id, claim).await
            }

            async fn disable(
                &self,
                community: CommunityId,
                author: &[u8],
                generation: i64,
            ) -> Result<bool> {
                self.disable_push_endpoint(community, author, "outbox", generation)
                    .await
            }

            async fn clear_match(&self, community: CommunityId, event: &[u8]) -> Result<u64> {
                Ok(sqlx::query(
                    "DELETE FROM push_match_queue WHERE community_id = ? AND event_id = ?",
                )
                .bind(community.as_uuid().to_string())
                .bind(event)
                .execute(&self.adapter_pool())
                .await?
                .rows_affected())
            }

            async fn prune(&self, community: CommunityId) -> Result<u64> {
                self.prune_push_wake_outbox(community, Utc::now() + Duration::seconds(1))
                    .await
            }
        }
    };
}

impl_sqlite_contract!();

#[async_trait]
impl PushOutboxContract for Db {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
        self.ensure_configured_community(host).await
    }

    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
        self.ensure_user(community, pubkey).await
    }

    async fn activate(
        &self,
        community: CommunityId,
        author: &[u8],
        source: &[u8],
        endpoint: &[u8],
        generation: i64,
    ) -> Result<ReplaceLeaseOutcome> {
        let subscriptions = serde_json::json!([{"kinds":[9]}]);
        crate::push::replace_active_lease(
            self.postgres_pool(),
            community,
            author,
            "outbox",
            LeaseVersion {
                source_event_id: source,
                source_created_at: Utc::now().timestamp() + generation,
                generation,
                expires_at: Utc::now().timestamp() + 3600,
            },
            ActiveLease {
                app_profile: "ios-production",
                endpoint_hash: endpoint,
                endpoint_grant: "outbox-contract-grant",
                max_class: "default",
                subscriptions: &subscriptions,
            },
        )
        .await
    }

    async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool> {
        Ok(self.insert_event(community, event, None).await?.1)
    }

    async fn enqueue_one(
        &self,
        community: CommunityId,
        author: &[u8],
        event: &[u8],
        generation: i64,
    ) -> Result<EnqueueWakeOutcome> {
        self.enqueue_push_wake(
            community,
            author,
            "outbox",
            NewWake {
                lease_generation: generation,
                event_id: event,
                class: "default",
                expires_at: Utc::now().timestamp() + 1800,
            },
        )
        .await
    }

    async fn enqueue_many(
        &self,
        community: CommunityId,
        requests: &[WakeRequest],
    ) -> Result<Vec<EnqueueWakeOutcome>> {
        self.enqueue_push_wakes(community, requests).await
    }

    async fn claim(&self, community: CommunityId) -> Result<Vec<ClaimedWake>> {
        self.claim_due_push_wakes(community, 10, Utc::now() + Duration::minutes(5))
            .await
    }

    async fn revalidate(
        &self,
        community: CommunityId,
        id: Uuid,
        claim: Uuid,
    ) -> Result<RevalidateWakeOutcome> {
        self.revalidate_push_wake(community, id, claim).await
    }

    async fn complete(&self, community: CommunityId, id: Uuid, claim: Uuid) -> Result<bool> {
        self.complete_push_wake(community, id, claim).await
    }

    async fn retry(&self, community: CommunityId, id: Uuid, claim: Uuid) -> Result<bool> {
        self.retry_push_wake(community, id, claim, Utc::now() - Duration::seconds(1))
            .await
    }

    async fn fail(&self, community: CommunityId, id: Uuid, claim: Uuid) -> Result<bool> {
        self.fail_push_wake(community, id, claim).await
    }

    async fn disable(
        &self,
        community: CommunityId,
        author: &[u8],
        generation: i64,
    ) -> Result<bool> {
        self.disable_push_endpoint(community, author, "outbox", generation)
            .await
    }

    async fn clear_match(&self, community: CommunityId, event: &[u8]) -> Result<u64> {
        Ok(
            sqlx::query("DELETE FROM push_match_queue WHERE community_id = $1 AND event_id = $2")
                .bind(community.as_uuid())
                .bind(event)
                .execute(self.postgres_pool())
                .await?
                .rows_affected(),
        )
    }

    async fn prune(&self, community: CommunityId) -> Result<u64> {
        crate::push::prune_wake_outbox(
            self.postgres_pool(),
            community,
            Utc::now() + Duration::seconds(1),
        )
        .await
    }
}

fn request(author: &[u8], event: &[u8], generation: i64) -> WakeRequest {
    WakeRequest {
        author: author.to_vec(),
        installation_id: "outbox".to_owned(),
        lease_generation: generation,
        event_id: event.to_vec(),
        class: "default".to_owned(),
        expires_at: Utc::now().timestamp() + 1800,
    }
}

async fn run_contract(store: &impl PushOutboxContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("outbox-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("outbox-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let lease_author = [0xc1; 32];
    let endpoint = [0xc2; 32];
    for community in [community_a, community_b] {
        assert_eq!(
            store
                .activate(community, &lease_author, &[0xc3; 32], &endpoint, 1)
                .await
                .expect("activate lease"),
            ReplaceLeaseOutcome::Accepted
        );
    }
    let keys = Keys::generate();
    let event_author = keys.public_key().to_bytes();
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &event_author)
            .await
            .expect("event author");
    }
    let event = EventBuilder::new(Kind::Custom(9), "outbox event")
        .sign_with_keys(&keys)
        .expect("signed event");
    for community in [community_a, community_b] {
        assert!(store
            .insert_event(community, &event)
            .await
            .expect("insert event"));
    }

    let outcomes = store
        .enqueue_many(
            community_a,
            &[
                request(&lease_author, event.id.as_bytes(), 1),
                request(&lease_author, event.id.as_bytes(), 1),
                request(&lease_author, &[0xc4; 32], 99),
            ],
        )
        .await
        .expect("batch enqueue");
    let id_a = match outcomes.as_slice() {
        [EnqueueWakeOutcome::Enqueued(id), EnqueueWakeOutcome::Duplicate(duplicate), EnqueueWakeOutcome::InactiveLease]
            if id == duplicate =>
        {
            *id
        }
        other => panic!("unexpected set-wise outcomes: {other:?}"),
    };
    assert_eq!(
        store
            .enqueue_one(community_a, &lease_author, event.id.as_bytes(), 1)
            .await
            .expect("single duplicate"),
        EnqueueWakeOutcome::Duplicate(id_a)
    );
    let id_b = match store
        .enqueue_one(community_b, &lease_author, event.id.as_bytes(), 1)
        .await
        .expect("enqueue B")
    {
        EnqueueWakeOutcome::Enqueued(id) => id,
        other => panic!("same dedup key must enqueue independently in B: {other:?}"),
    };
    assert_ne!(id_a, id_b);

    let wakes_a = store.claim(community_a).await.expect("claim A");
    assert_eq!(wakes_a.len(), 1);
    let wake_a = &wakes_a[0];
    assert_eq!(wake_a.id, id_a);
    assert_eq!(wake_a.attempt, 1);
    assert!(matches!(
        store
            .revalidate(community_b, wake_a.id, wake_a.claim_id)
            .await
            .expect("foreign revalidation"),
        RevalidateWakeOutcome::Suppressed
    ));
    assert!(matches!(
        store
            .revalidate(community_a, wake_a.id, Uuid::new_v4())
            .await
            .expect("wrong-fence revalidation"),
        RevalidateWakeOutcome::Suppressed
    ));
    assert!(matches!(
        store
            .revalidate(community_a, wake_a.id, wake_a.claim_id)
            .await
            .expect("valid revalidation"),
        RevalidateWakeOutcome::Deliver(_)
    ));
    assert!(!store
        .retry(community_a, wake_a.id, Uuid::new_v4())
        .await
        .expect("wrong-fence retry"));
    assert!(store
        .retry(community_a, wake_a.id, wake_a.claim_id)
        .await
        .expect("retry A"));
    let retried = store.claim(community_a).await.expect("reclaim A");
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].id, id_a);
    assert_eq!(retried[0].attempt, 2);
    assert!(store
        .complete(community_a, id_a, retried[0].claim_id)
        .await
        .expect("complete A"));
    assert!(!store
        .complete(community_a, id_a, retried[0].claim_id)
        .await
        .expect("repeat completion"));

    let wakes_b = store.claim(community_b).await.expect("claim B");
    assert_eq!(wakes_b.len(), 1);
    assert!(!store
        .disable(community_b, &lease_author, 99)
        .await
        .expect("stale disable"));
    assert!(store
        .disable(community_b, &lease_author, 1)
        .await
        .expect("disable B"));
    assert!(matches!(
        store
            .revalidate(community_b, id_b, wakes_b[0].claim_id)
            .await
            .expect("disabled revalidation"),
        RevalidateWakeOutcome::Suppressed
    ));
    assert!(store
        .fail(community_b, id_b, wakes_b[0].claim_id)
        .await
        .expect("fail B"));
    assert_eq!(
        store
            .activate(community_b, &lease_author, &[0xc5; 32], &endpoint, 2)
            .await
            .expect("reactivate B"),
        ReplaceLeaseOutcome::Accepted
    );

    for community in [community_a, community_b] {
        assert_eq!(
            store
                .clear_match(community, event.id.as_bytes())
                .await
                .expect("clear matcher job"),
            1
        );
    }
    assert_eq!(store.prune(community_b).await.expect("prune B"), 1);
    assert_eq!(store.prune(community_a).await.expect("prune A"), 1);
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
async fn sqlite_push_outbox_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_push_outbox_contract() {
    let admin = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/postgres")
        .await
        .expect("PostgreSQL admin connection");
    let database = format!("buzz_outbox_contract_{}", Uuid::new_v4().simple());
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
