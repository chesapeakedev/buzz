//! Shared durable push-matcher claim-fencing contract.

use async_trait::async_trait;
use chrono::{Duration, Utc};
use nostr::{Event, EventBuilder, Keys, Kind};
use uuid::Uuid;

use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::push::{
    ActiveLease, ClaimedMatchBatch, LeaseVersion, ReplaceLeaseOutcome, MAX_MATCH_ATTEMPTS,
};
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait PushMatcherContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn activate(
        &self,
        community: CommunityId,
        author: &[u8],
        source: &[u8],
        endpoint: &[u8],
    ) -> Result<ReplaceLeaseOutcome>;
    async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool>;
    async fn soft_delete(&self, community: CommunityId, event_id: &[u8]) -> Result<bool>;
    async fn claim(&self, limit: i64) -> Result<Option<ClaimedMatchBatch>>;
    async fn complete(
        &self,
        community: CommunityId,
        claim_id: Uuid,
        event_ids: &[Vec<u8>],
    ) -> Result<u64>;
    async fn retry(
        &self,
        community: CommunityId,
        claim_id: Uuid,
        event_ids: &[Vec<u8>],
    ) -> Result<u64>;
    async fn reap(&self) -> Result<u64>;
    async fn match_count(&self, community: CommunityId, event_id: &[u8]) -> Result<i64>;
}

macro_rules! impl_sqlite_contract {
    () => {
        #[async_trait]
        impl PushMatcherContract for SqliteStore {
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
            ) -> Result<ReplaceLeaseOutcome> {
                let subscriptions = serde_json::json!([{"kinds":[9]}]);
                self.replace_active_lease(
                    community,
                    author,
                    "matcher",
                    LeaseVersion {
                        source_event_id: source,
                        source_created_at: Utc::now().timestamp(),
                        generation: 1,
                        expires_at: Utc::now().timestamp() + 3600,
                    },
                    ActiveLease {
                        app_profile: "ios-production",
                        endpoint_hash: endpoint,
                        endpoint_grant: "matcher-contract-grant",
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

            async fn soft_delete(
                &self,
                community: CommunityId,
                event_id: &[u8],
            ) -> Result<bool> {
                self.soft_delete_event(community, event_id).await
            }

            async fn claim(&self, limit: i64) -> Result<Option<ClaimedMatchBatch>> {
                self.claim_due_push_match_batch(limit, Utc::now() + Duration::minutes(5))
                    .await
            }

            async fn complete(
                &self,
                community: CommunityId,
                claim_id: Uuid,
                event_ids: &[Vec<u8>],
            ) -> Result<u64> {
                self.complete_push_match_batch(community, claim_id, event_ids)
                    .await
            }

            async fn retry(
                &self,
                community: CommunityId,
                claim_id: Uuid,
                event_ids: &[Vec<u8>],
            ) -> Result<u64> {
                self.retry_push_match_batch(
                    community,
                    claim_id,
                    event_ids,
                    Utc::now() - Duration::seconds(1),
                )
                .await
            }

            async fn reap(&self) -> Result<u64> {
                self.reap_exhausted_push_matches().await
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
        }
    };
}

impl_sqlite_contract!();

#[async_trait]
impl PushMatcherContract for Db {
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
    ) -> Result<ReplaceLeaseOutcome> {
        let subscriptions = serde_json::json!([{"kinds":[9]}]);
        crate::push::replace_active_lease(
            self.postgres_pool(),
            community,
            author,
            "matcher",
            LeaseVersion {
                source_event_id: source,
                source_created_at: Utc::now().timestamp(),
                generation: 1,
                expires_at: Utc::now().timestamp() + 3600,
            },
            ActiveLease {
                app_profile: "ios-production",
                endpoint_hash: endpoint,
                endpoint_grant: "matcher-contract-grant",
                max_class: "default",
                subscriptions: &subscriptions,
            },
        )
        .await
    }

    async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool> {
        Ok(self.insert_event(community, event, None).await?.1)
    }

    async fn soft_delete(&self, community: CommunityId, event_id: &[u8]) -> Result<bool> {
        self.soft_delete_event(community, event_id).await
    }

    async fn claim(&self, limit: i64) -> Result<Option<ClaimedMatchBatch>> {
        self.claim_due_push_match_batch(limit, Utc::now() + Duration::minutes(5))
            .await
    }

    async fn complete(
        &self,
        community: CommunityId,
        claim_id: Uuid,
        event_ids: &[Vec<u8>],
    ) -> Result<u64> {
        self.complete_push_match_batch(community, claim_id, event_ids)
            .await
    }

    async fn retry(
        &self,
        community: CommunityId,
        claim_id: Uuid,
        event_ids: &[Vec<u8>],
    ) -> Result<u64> {
        self.retry_push_match_batch(
            community,
            claim_id,
            event_ids,
            Utc::now() - Duration::seconds(1),
        )
        .await
    }

    async fn reap(&self) -> Result<u64> {
        self.reap_exhausted_push_matches().await
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
}

async fn run_contract(store: &impl PushMatcherContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("matcher-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("matcher-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let keys = Keys::generate();
    let event_author = keys.public_key().to_bytes();
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &event_author)
            .await
            .expect("event author");
    }
    assert_eq!(
        store
            .activate(community_a, &[0xa1; 32], &[0xa2; 32], &[0xa3; 32])
            .await
            .expect("activate A"),
        ReplaceLeaseOutcome::Accepted
    );
    assert_eq!(
        store
            .activate(community_b, &[0xb1; 32], &[0xb2; 32], &[0xb3; 32])
            .await
            .expect("activate B"),
        ReplaceLeaseOutcome::Accepted
    );

    let event_a1 = EventBuilder::new(Kind::Custom(9), "matcher A1")
        .sign_with_keys(&keys)
        .expect("event A1");
    let event_a2 = EventBuilder::new(Kind::Custom(9), "matcher A2")
        .sign_with_keys(&keys)
        .expect("event A2");
    for event in [&event_a1, &event_a2] {
        assert!(store
            .insert_event(community_a, event)
            .await
            .expect("insert A event"));
    }

    let claim_one = store.claim(10);
    let claim_two = store.claim(10);
    let (claim_one, claim_two) = tokio::join!(claim_one, claim_two);
    let batch = match (
        claim_one.expect("first claim"),
        claim_two.expect("second claim"),
    ) {
        (Some(batch), None) | (None, Some(batch)) => batch,
        other => panic!("exactly one concurrent matcher claim must win: {other:?}"),
    };
    assert_eq!(batch.community, community_a);
    assert_eq!(batch.jobs.len(), 2);
    assert!(batch.jobs.iter().all(|job| job.attempt == 1));
    let event_ids: Vec<Vec<u8>> = batch
        .jobs
        .iter()
        .map(|job| job.event.event.id.as_bytes().to_vec())
        .collect();
    assert_eq!(
        store
            .complete(community_b, batch.claim_id, &event_ids)
            .await
            .expect("foreign completion"),
        0
    );
    assert_eq!(
        store
            .complete(community_a, Uuid::new_v4(), &event_ids)
            .await
            .expect("wrong-fence completion"),
        0
    );
    assert_eq!(
        store
            .retry(
                community_a,
                batch.claim_id,
                std::slice::from_ref(&event_ids[0]),
            )
            .await
            .expect("retry one"),
        1
    );
    assert_eq!(
        store
            .complete(
                community_a,
                batch.claim_id,
                std::slice::from_ref(&event_ids[1]),
            )
            .await
            .expect("complete one"),
        1
    );
    let retried = store
        .claim(10)
        .await
        .expect("retry claim")
        .expect("retried batch");
    assert_eq!(retried.community, community_a);
    assert_eq!(retried.jobs.len(), 1);
    assert_eq!(retried.jobs[0].attempt, 2);
    assert_eq!(
        store
            .complete(community_a, retried.claim_id, &event_ids[..1])
            .await
            .expect("complete retried"),
        1
    );

    let shared = EventBuilder::new(Kind::Custom(9), "same event in B")
        .sign_with_keys(&keys)
        .expect("shared event");
    assert!(store
        .insert_event(community_a, &shared)
        .await
        .expect("insert shared A"));
    assert!(store
        .insert_event(community_b, &shared)
        .await
        .expect("insert shared B"));
    let batch_a = store
        .claim(1)
        .await
        .expect("claim shared A")
        .expect("batch A");
    let batch_b = store
        .claim(1)
        .await
        .expect("claim shared B")
        .expect("batch B");
    assert_ne!(batch_a.community, batch_b.community);
    assert_eq!(
        store
            .complete(
                batch_a.community,
                batch_a.claim_id,
                &[shared.id.as_bytes().to_vec()],
            )
            .await
            .expect("complete shared A"),
        1
    );
    assert_eq!(
        store
            .complete(
                batch_b.community,
                batch_b.claim_id,
                &[shared.id.as_bytes().to_vec()],
            )
            .await
            .expect("complete shared B"),
        1
    );

    let deleted = EventBuilder::new(Kind::Custom(9), "deleted before matching")
        .sign_with_keys(&keys)
        .expect("deleted event");
    assert!(store
        .insert_event(community_a, &deleted)
        .await
        .expect("insert deleted event"));
    assert!(store
        .soft_delete(community_a, deleted.id.as_bytes())
        .await
        .expect("soft delete event"));
    assert!(store.claim(10).await.expect("claim deleted").is_none());
    assert_eq!(
        store
            .match_count(community_a, deleted.id.as_bytes())
            .await
            .expect("deleted job count"),
        0,
        "missing/deleted source jobs are terminally removed"
    );

    let exhausted = EventBuilder::new(Kind::Custom(9), "exhaust matcher")
        .sign_with_keys(&keys)
        .expect("exhausted event");
    assert!(store
        .insert_event(community_a, &exhausted)
        .await
        .expect("insert exhausted event"));
    for attempt in 1..=MAX_MATCH_ATTEMPTS {
        let batch = store
            .claim(1)
            .await
            .expect("exhaustion claim")
            .expect("exhaustion batch");
        assert_eq!(batch.jobs[0].attempt, attempt);
        assert_eq!(
            store
                .retry(
                    community_a,
                    batch.claim_id,
                    &[exhausted.id.as_bytes().to_vec()],
                )
                .await
                .expect("exhaustion retry"),
            1
        );
    }
    assert!(store.claim(1).await.expect("over-budget claim").is_none());
    assert!(
        store.reap().await.expect("reap exhausted") >= 1,
        "the exhausted contract job must be reaped"
    );
    assert_eq!(
        store
            .match_count(community_a, exhausted.id.as_bytes())
            .await
            .expect("exhausted count"),
        0
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
async fn sqlite_push_matcher_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_push_matcher_contract() {
    let admin = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/postgres")
        .await
        .expect("PostgreSQL admin connection");
    let database = format!("buzz_matcher_contract_{}", Uuid::new_v4().simple());
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
