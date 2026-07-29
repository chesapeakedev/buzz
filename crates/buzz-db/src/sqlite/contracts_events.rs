//! Shared direct event-lifecycle contract for relational backends.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};
use uuid::Uuid;

use buzz_core::{CommunityId, StoredEvent};

use super::{SqliteConfig, SqliteStore};
use crate::event::EventQuery;
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
    async fn query(&self, query: &EventQuery) -> Result<Vec<StoredEvent>>;
    async fn replace(&self, community: CommunityId, event: &Event) -> Result<(StoredEvent, bool)>;
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

            async fn query(&self, query: &EventQuery) -> Result<Vec<StoredEvent>> {
                self.query_events(query).await
            }

            async fn replace(
                &self,
                community: CommunityId,
                event: &Event,
            ) -> Result<(StoredEvent, bool)> {
                self.replace_addressable_event(community, event, None).await
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

    let author = Keys::generate();
    let mentioned = Keys::generate().public_key().to_hex();
    let referenced = "ab".repeat(32);
    let base = 1_800_000_000_u64;
    let tagged = EventBuilder::new(Kind::TextNote, "tagged")
        .tags([
            Tag::parse(["p", mentioned.as_str()]).expect("p tag"),
            Tag::parse(["e", referenced.as_str()]).expect("e tag"),
        ])
        .custom_created_at(Timestamp::from(base))
        .sign_with_keys(&author)
        .expect("tagged event");
    let addressable = EventBuilder::new(Kind::Custom(30_023), "addressable")
        .tag(Tag::parse(["d", "contract-coordinate"]).expect("d tag"))
        .custom_created_at(Timestamp::from(base + 1))
        .sign_with_keys(&author)
        .expect("addressable event");
    assert!(
        store
            .insert(community_a, &tagged)
            .await
            .expect("tagged insert")
            .1
    );
    assert!(
        store
            .insert(community_a, &addressable)
            .await
            .expect("addressable insert")
            .1
    );

    let mut filtered = EventQuery::for_community(community_a);
    filtered.kinds = Some(vec![30_023]);
    filtered.authors = Some(vec![author.public_key().to_bytes().to_vec()]);
    filtered.d_tag = Some("contract-coordinate".to_owned());
    let rows = store.query(&filtered).await.expect("kind/author/d query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event.id, addressable.id);

    let mut mentions = EventQuery::for_community(community_a);
    mentions.p_tag_hex = Some(mentioned.to_ascii_uppercase());
    assert_eq!(
        store.query(&mentions).await.expect("p-tag query")[0]
            .event
            .id,
        tagged.id
    );

    let mut references = EventQuery::for_community(community_a);
    references.e_tags = Some(vec![referenced]);
    references.ids = Some(vec![tagged.id.as_bytes().to_vec()]);
    assert_eq!(
        store.query(&references).await.expect("e-tag/id query")[0]
            .event
            .id,
        tagged.id
    );

    let cursor_time =
        DateTime::<Utc>::from_timestamp(base as i64 + 1, 0).expect("contract timestamp");
    let mut cursor = EventQuery::for_community(community_a);
    cursor.until = Some(cursor_time);
    cursor.before_id = Some(addressable.id.as_bytes().to_vec());
    cursor.limit = Some(1);
    assert_eq!(
        store.query(&cursor).await.expect("cursor query")[0]
            .event
            .id,
        tagged.id
    );

    let mut foreign = EventQuery::for_community(community_b);
    foreign.ids = Some(vec![tagged.id.as_bytes().to_vec()]);
    assert!(store
        .query(&foreign)
        .await
        .expect("foreign query")
        .is_empty());

    let mut invalid = EventQuery::for_community(community_a);
    invalid.before_id = Some(tagged.id.as_bytes().to_vec());
    assert!(matches!(
        store.query(&invalid).await,
        Err(DbError::InvalidData(_))
    ));

    let replacement_author = Keys::generate();
    let old = EventBuilder::new(Kind::Custom(10_001), "old")
        .custom_created_at(Timestamp::from(base + 10))
        .sign_with_keys(&replacement_author)
        .expect("old replacement");
    let new = EventBuilder::new(Kind::Custom(10_001), "new")
        .custom_created_at(Timestamp::from(base + 11))
        .sign_with_keys(&replacement_author)
        .expect("new replacement");
    assert!(
        store
            .replace(community_a, &old)
            .await
            .expect("old replace")
            .1
    );
    assert!(
        store
            .replace(community_b, &old)
            .await
            .expect("foreign old replace")
            .1
    );
    assert!(
        store
            .replace(community_a, &new)
            .await
            .expect("new replace")
            .1
    );
    assert!(
        !store
            .replace(community_a, &old)
            .await
            .expect("stale replace")
            .1
    );

    let mut replacement_query = EventQuery::for_community(community_a);
    replacement_query.kinds = Some(vec![10_001]);
    replacement_query.pubkey = Some(replacement_author.public_key().to_bytes().to_vec());
    let live = store
        .query(&replacement_query)
        .await
        .expect("replacement query");
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].event.id, new.id);
    replacement_query.community_id = community_b;
    assert_eq!(
        store
            .query(&replacement_query)
            .await
            .expect("foreign replacement query")[0]
            .event
            .id,
        old.id
    );

    let tied_a = EventBuilder::new(Kind::Custom(10_002), "tie A")
        .custom_created_at(Timestamp::from(base + 20))
        .sign_with_keys(&replacement_author)
        .expect("tied A");
    let tied_b = EventBuilder::new(Kind::Custom(10_002), "tie B")
        .custom_created_at(Timestamp::from(base + 20))
        .sign_with_keys(&replacement_author)
        .expect("tied B");
    let expected_id = [tied_a.id, tied_b.id]
        .into_iter()
        .min()
        .expect("two tied IDs");
    let (left, right) = tokio::join!(
        store.replace(community_a, &tied_a),
        store.replace(community_a, &tied_b)
    );
    left.expect("left tied replace");
    right.expect("right tied replace");
    let mut tied_query = EventQuery::for_community(community_a);
    tied_query.kinds = Some(vec![10_002]);
    tied_query.pubkey = Some(replacement_author.public_key().to_bytes().to_vec());
    let tied_live = store.query(&tied_query).await.expect("tied query");
    assert_eq!(tied_live.len(), 1);
    assert_eq!(tied_live[0].event.id, expected_id);
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
