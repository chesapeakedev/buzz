//! Shared thread-metadata contract for relational backends.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nostr::{Event, EventBuilder, Keys, Kind, Timestamp};
use uuid::Uuid;

use buzz_core::{CommunityId, StoredEvent};

use super::{SqliteConfig, SqliteStore};
use crate::channel::{ChannelType, ChannelVisibility};
use crate::event::ThreadMetadataParams;
use crate::thread::{ThreadMetadataRecord, ThreadReply, ThreadSummary};
use crate::{Db, EnsuredCommunityRecord, Result};

fn event_created_at(event: &Event) -> DateTime<Utc> {
    let seconds = i64::try_from(event.created_at.as_secs()).expect("test timestamp in i64");
    DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
}

#[async_trait]
trait ThreadContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn create_channel(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        created_by: &[u8],
    ) -> Result<()>;
    #[allow(clippy::too_many_arguments)]
    async fn insert_thread_event(
        &self,
        community: CommunityId,
        event: &Event,
        channel_id: Uuid,
        parent: Option<&Event>,
        root: Option<&Event>,
        depth: i32,
        broadcast: bool,
    ) -> Result<(StoredEvent, bool)>;
    async fn summary(
        &self,
        community: CommunityId,
        event_id: &[u8],
    ) -> Result<Option<ThreadSummary>>;
    async fn metadata(
        &self,
        community: CommunityId,
        event_id: &[u8],
    ) -> Result<Option<ThreadMetadataRecord>>;
    async fn replies(
        &self,
        community: CommunityId,
        root_event_id: &[u8],
        depth_limit: Option<u32>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> Result<Vec<ThreadReply>>;
    async fn decrement(
        &self,
        community: CommunityId,
        parent_event_id: &[u8],
        root_event_id: Option<&[u8]>,
    ) -> Result<()>;
    async fn get_event(
        &self,
        community: CommunityId,
        event_id: &[u8],
    ) -> Result<Option<StoredEvent>>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl ThreadContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn create_channel(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                created_by: &[u8],
            ) -> Result<()> {
                self.create_channel_with_id(
                    community,
                    channel_id,
                    "thread-contract",
                    ChannelType::Stream,
                    ChannelVisibility::Open,
                    None,
                    created_by,
                    None,
                )
                .await
                .map(|_| ())
            }

            async fn insert_thread_event(
                &self,
                community: CommunityId,
                event: &Event,
                channel_id: Uuid,
                parent: Option<&Event>,
                root: Option<&Event>,
                depth: i32,
                broadcast: bool,
            ) -> Result<(StoredEvent, bool)> {
                let metadata = parent.map(|parent| ThreadMetadataParams {
                    event_id: event.id.as_bytes(),
                    event_created_at: event_created_at(event),
                    channel_id,
                    parent_event_id: Some(parent.id.as_bytes()),
                    parent_event_created_at: Some(event_created_at(parent)),
                    root_event_id: root.map(|root| root.id.as_bytes().as_slice()),
                    root_event_created_at: root.map(event_created_at),
                    depth,
                    broadcast,
                });
                self.insert_event_with_thread_metadata(community, event, Some(channel_id), metadata)
                    .await
            }

            async fn summary(
                &self,
                community: CommunityId,
                event_id: &[u8],
            ) -> Result<Option<ThreadSummary>> {
                self.get_thread_summary(community, event_id).await
            }

            async fn metadata(
                &self,
                community: CommunityId,
                event_id: &[u8],
            ) -> Result<Option<ThreadMetadataRecord>> {
                self.get_thread_metadata_by_event(community, event_id).await
            }

            async fn replies(
                &self,
                community: CommunityId,
                root_event_id: &[u8],
                depth_limit: Option<u32>,
                limit: u32,
                cursor: Option<&[u8]>,
            ) -> Result<Vec<ThreadReply>> {
                self.get_thread_replies(community, root_event_id, depth_limit, limit, cursor)
                    .await
            }

            async fn decrement(
                &self,
                community: CommunityId,
                parent_event_id: &[u8],
                root_event_id: Option<&[u8]>,
            ) -> Result<()> {
                self.decrement_reply_count(community, parent_event_id, root_event_id)
                    .await
            }

            async fn get_event(
                &self,
                community: CommunityId,
                event_id: &[u8],
            ) -> Result<Option<StoredEvent>> {
                self.get_event_by_id(community, event_id).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl ThreadContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("threads-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("threads-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let channel_id = Uuid::new_v4();
    let keys = Keys::generate();
    let owner = keys.public_key().to_bytes();
    store
        .create_channel(community_a, channel_id, &owner)
        .await
        .expect("channel A");
    store
        .create_channel(community_b, channel_id, &owner)
        .await
        .expect("same channel ID in community B");

    let base = u64::try_from(Utc::now().timestamp()).expect("current timestamp is positive");
    let timestamp = Timestamp::from(base);
    let root = EventBuilder::new(Kind::TextNote, "root")
        .custom_created_at(timestamp)
        .sign_with_keys(&keys)
        .expect("signed root");
    let direct = EventBuilder::new(Kind::TextNote, "direct")
        .custom_created_at(timestamp)
        .sign_with_keys(&Keys::generate())
        .expect("signed direct reply");
    let nested = EventBuilder::new(Kind::TextNote, "nested")
        .custom_created_at(timestamp)
        .sign_with_keys(&Keys::generate())
        .expect("signed nested reply");

    assert!(
        store
            .insert_thread_event(community_a, &root, channel_id, None, None, 0, false)
            .await
            .expect("root event")
            .1
    );
    assert!(
        store
            .insert_thread_event(
                community_a,
                &direct,
                channel_id,
                Some(&root),
                Some(&root),
                1,
                true,
            )
            .await
            .expect("direct reply")
            .1
    );
    assert!(
        store
            .insert_thread_event(
                community_a,
                &nested,
                channel_id,
                Some(&direct),
                Some(&root),
                2,
                false,
            )
            .await
            .expect("nested reply")
            .1
    );
    assert!(
        !store
            .insert_thread_event(
                community_a,
                &nested,
                channel_id,
                Some(&direct),
                Some(&root),
                2,
                false,
            )
            .await
            .expect("duplicate nested reply")
            .1
    );

    let root_summary = store
        .summary(community_a, root.id.as_bytes())
        .await
        .expect("root summary")
        .expect("root metadata stub");
    assert_eq!(root_summary.reply_count, 1);
    assert_eq!(root_summary.descendant_count, 2);
    assert!(root_summary.last_reply_at.is_some());
    assert_eq!(root_summary.participants.len(), 2);
    let direct_metadata = store
        .metadata(community_a, direct.id.as_bytes())
        .await
        .expect("direct metadata")
        .expect("direct metadata exists");
    assert_eq!(
        direct_metadata.parent_event_id.as_deref(),
        Some(root.id.as_bytes().as_slice())
    );
    assert_eq!(
        direct_metadata.root_event_id.as_deref(),
        Some(root.id.as_bytes().as_slice())
    );
    assert_eq!(direct_metadata.depth, 1);
    assert_eq!(direct_metadata.reply_count, 1);
    assert!(direct_metadata.broadcast);

    let replies = store
        .replies(community_a, root.id.as_bytes(), None, 10, None)
        .await
        .expect("all replies");
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0].created_at, replies[1].created_at);
    assert!(replies[0].event_id < replies[1].event_id);
    assert_eq!(
        store
            .replies(community_a, root.id.as_bytes(), Some(1), 10, None)
            .await
            .expect("depth-limited replies")
            .len(),
        1
    );
    let mut cursor = replies[0].created_at.timestamp().to_be_bytes().to_vec();
    cursor.extend_from_slice(&replies[0].event_id);
    let second_page = store
        .replies(community_a, root.id.as_bytes(), None, 10, Some(&cursor))
        .await
        .expect("composite cursor");
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].event_id, replies[1].event_id);

    assert!(
        store
            .insert_thread_event(
                community_b,
                &direct,
                channel_id,
                Some(&root),
                Some(&root),
                1,
                false,
            )
            .await
            .expect("same event in community B")
            .1
    );
    assert_eq!(
        store
            .summary(community_b, root.id.as_bytes())
            .await
            .expect("community B summary")
            .expect("community B root stub")
            .descendant_count,
        1
    );
    assert_eq!(
        store
            .summary(community_a, root.id.as_bytes())
            .await
            .expect("community A summary")
            .expect("community A root")
            .descendant_count,
        2
    );

    let concurrent_root = EventBuilder::new(Kind::TextNote, "concurrent root")
        .custom_created_at(Timestamp::from(base + 1))
        .sign_with_keys(&keys)
        .expect("signed concurrent root");
    let concurrent_a = EventBuilder::new(Kind::TextNote, "concurrent A")
        .custom_created_at(Timestamp::from(base + 2))
        .sign_with_keys(&Keys::generate())
        .expect("signed concurrent reply A");
    let concurrent_b = EventBuilder::new(Kind::TextNote, "concurrent B")
        .custom_created_at(Timestamp::from(base + 2))
        .sign_with_keys(&Keys::generate())
        .expect("signed concurrent reply B");
    store
        .insert_thread_event(
            community_a,
            &concurrent_root,
            channel_id,
            None,
            None,
            0,
            false,
        )
        .await
        .expect("concurrent root event");
    let (insert_a, insert_b) = tokio::join!(
        store.insert_thread_event(
            community_a,
            &concurrent_a,
            channel_id,
            Some(&concurrent_root),
            Some(&concurrent_root),
            1,
            false,
        ),
        store.insert_thread_event(
            community_a,
            &concurrent_b,
            channel_id,
            Some(&concurrent_root),
            Some(&concurrent_root),
            1,
            false,
        )
    );
    assert!(insert_a.expect("concurrent insert A").1);
    assert!(insert_b.expect("concurrent insert B").1);
    let concurrent_summary = store
        .summary(community_a, concurrent_root.id.as_bytes())
        .await
        .expect("concurrent summary")
        .expect("concurrent root metadata");
    assert_eq!(concurrent_summary.reply_count, 2);
    assert_eq!(concurrent_summary.descendant_count, 2);

    store
        .decrement(community_a, direct.id.as_bytes(), Some(root.id.as_bytes()))
        .await
        .expect("decrement nested counters");
    store
        .decrement(community_a, direct.id.as_bytes(), Some(root.id.as_bytes()))
        .await
        .expect("floor nested counters");
    assert_eq!(
        store
            .metadata(community_a, direct.id.as_bytes())
            .await
            .expect("direct after decrement")
            .expect("direct metadata")
            .reply_count,
        0
    );
    assert_eq!(
        store
            .summary(community_a, root.id.as_bytes())
            .await
            .expect("root after decrement")
            .expect("root summary")
            .descendant_count,
        0
    );

    let missing_channel_event = EventBuilder::new(Kind::TextNote, "rollback")
        .sign_with_keys(&Keys::generate())
        .expect("signed rollback event");
    assert!(store
        .insert_thread_event(
            community_a,
            &missing_channel_event,
            Uuid::new_v4(),
            Some(&root),
            Some(&root),
            1,
            false,
        )
        .await
        .is_err());
    assert!(
        store
            .get_event(community_a, missing_channel_event.id.as_bytes())
            .await
            .expect("rollback event lookup")
            .is_none(),
        "thread metadata failure must roll back the event"
    );
}

#[tokio::test]
async fn sqlite_thread_contract() {
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
async fn postgres_thread_contract() {
    let db = Db::new(&crate::DbConfig {
        database_url: "postgres://buzz:buzz_dev@localhost:5432/buzz".to_owned(),
        read_database_url: None,
        max_connections: 5,
        min_connections: 0,
        acquire_timeout_secs: 5,
        max_lifetime_secs: 300,
        idle_timeout_secs: 60,
    })
    .await
    .expect("PostgreSQL connection");
    run_contract(&db).await;
}
