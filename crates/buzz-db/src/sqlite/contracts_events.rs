//! Shared direct event-lifecycle contract for relational backends.

use async_trait::async_trait;
use nostr::{Event, EventBuilder, Keys, Kind};
use uuid::Uuid;

use buzz_core::{CommunityId, StoredEvent};

use super::{SqliteConfig, SqliteStore};
use crate::{Db, DbError, EnsuredCommunityRecord, Result};

#[async_trait]
trait EventLifecycleContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn insert(&self, community: CommunityId, event: &Event) -> Result<(StoredEvent, bool)>;
    async fn get(&self, community: CommunityId, id: &[u8]) -> Result<Option<StoredEvent>>;
    async fn get_including_deleted(
        &self,
        community: CommunityId,
        id: &[u8],
    ) -> Result<Option<StoredEvent>>;
    async fn soft_delete(&self, community: CommunityId, id: &[u8]) -> Result<bool>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl EventLifecycleContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn insert(
                &self,
                community: CommunityId,
                event: &Event,
            ) -> Result<(StoredEvent, bool)> {
                self.insert_event(community, event, None).await
            }

            async fn get(&self, community: CommunityId, id: &[u8]) -> Result<Option<StoredEvent>> {
                self.get_event_by_id(community, id).await
            }

            async fn get_including_deleted(
                &self,
                community: CommunityId,
                id: &[u8],
            ) -> Result<Option<StoredEvent>> {
                self.get_event_by_id_including_deleted(community, id).await
            }

            async fn soft_delete(&self, community: CommunityId, id: &[u8]) -> Result<bool> {
                self.soft_delete_event(community, id).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl EventLifecycleContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("events-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("events-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let event = EventBuilder::new(Kind::TextNote, "shared event")
        .sign_with_keys(&Keys::generate())
        .expect("signed event");

    assert!(store.insert(community_a, &event).await.expect("insert A").1);
    assert!(
        !store
            .insert(community_a, &event)
            .await
            .expect("duplicate A")
            .1
    );
    assert!(store.insert(community_b, &event).await.expect("insert B").1);

    let stored_a = store
        .get(community_a, event.id.as_bytes())
        .await
        .expect("read A")
        .expect("event A");
    assert_eq!(stored_a.event.id, event.id);
    assert_eq!(stored_a.event.content, "shared event");
    assert!(stored_a.is_verified());

    assert!(store
        .soft_delete(community_a, event.id.as_bytes())
        .await
        .expect("delete A"));
    assert!(!store
        .soft_delete(community_a, event.id.as_bytes())
        .await
        .expect("repeat delete A"));
    assert!(store
        .get(community_a, event.id.as_bytes())
        .await
        .expect("live A")
        .is_none());
    assert!(store
        .get_including_deleted(community_a, event.id.as_bytes())
        .await
        .expect("history A")
        .is_some());
    assert!(store
        .get(community_b, event.id.as_bytes())
        .await
        .expect("live B")
        .is_some());

    for (kind, expected) in [
        (Kind::Custom(22_242), DbError::AuthEventRejected),
        (
            Kind::Custom(20_000),
            DbError::EphemeralEventRejected(20_000),
        ),
    ] {
        let rejected = EventBuilder::new(kind, "must not persist")
            .sign_with_keys(&Keys::generate())
            .expect("signed rejected event");
        let error = store
            .insert(community_a, &rejected)
            .await
            .expect_err("event must be rejected");
        assert_eq!(error.to_string(), expected.to_string());
    }

    let raced = EventBuilder::new(Kind::TextNote, "raced event")
        .sign_with_keys(&Keys::generate())
        .expect("signed raced event");
    let (left, right) = tokio::join!(
        store.insert(community_a, &raced),
        store.insert(community_a, &raced)
    );
    let inserted =
        usize::from(left.expect("left insert").1) + usize::from(right.expect("right insert").1);
    assert_eq!(inserted, 1);
}

#[tokio::test]
async fn sqlite_event_lifecycle_contract() {
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
async fn postgres_event_lifecycle_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
