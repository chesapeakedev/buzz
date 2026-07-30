//! Shared event batch and channel-window contract for relational backends.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nostr::{Event, EventBuilder, Keys, Kind, Timestamp};
use uuid::Uuid;

use buzz_core::{CommunityId, StoredEvent};

use super::{SqliteConfig, SqliteStore};
use crate::channel::{ChannelType, ChannelVisibility};
use crate::event::ThreadMetadataParams;
use crate::thread::ChannelWindow;
use crate::{Db, EnsuredCommunityRecord, Result};

fn event_created_at(event: &Event) -> DateTime<Utc> {
    let seconds = i64::try_from(event.created_at.as_secs()).expect("test timestamp in i64");
    DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
}

#[async_trait]
trait ChannelWindowContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn create_channel(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        owner: &[u8],
    ) -> Result<()>;
    #[allow(clippy::too_many_arguments)]
    async fn insert(
        &self,
        community: CommunityId,
        event: &Event,
        channel_id: Uuid,
        parent: Option<&Event>,
        root: Option<&Event>,
        depth: i32,
        broadcast: bool,
    ) -> Result<(StoredEvent, bool)>;
    async fn window(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        limit: u32,
        cursor: Option<(DateTime<Utc>, Vec<u8>)>,
        kinds: Option<&[u32]>,
    ) -> Result<ChannelWindow>;
    async fn batch(&self, community: CommunityId, event_ids: &[&[u8]]) -> Result<Vec<StoredEvent>>;
    async fn soft_delete(&self, community: CommunityId, event_id: &[u8]) -> Result<bool>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl ChannelWindowContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn create_channel(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                owner: &[u8],
            ) -> Result<()> {
                self.create_channel_with_id(
                    community,
                    channel_id,
                    "channel-window",
                    ChannelType::Stream,
                    ChannelVisibility::Open,
                    None,
                    owner,
                    None,
                )
                .await
                .map(|_| ())
            }

            async fn insert(
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

            async fn window(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                limit: u32,
                cursor: Option<(DateTime<Utc>, Vec<u8>)>,
                kinds: Option<&[u32]>,
            ) -> Result<ChannelWindow> {
                self.get_channel_window(community, channel_id, limit, cursor, kinds)
                    .await
            }

            async fn batch(
                &self,
                community: CommunityId,
                event_ids: &[&[u8]],
            ) -> Result<Vec<StoredEvent>> {
                self.get_events_by_ids(community, event_ids).await
            }

            async fn soft_delete(&self, community: CommunityId, event_id: &[u8]) -> Result<bool> {
                self.soft_delete_event(community, event_id).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

fn signed_event(keys: &Keys, kind: u16, content: &str, timestamp: u64) -> Event {
    EventBuilder::new(Kind::Custom(kind), content)
        .custom_created_at(Timestamp::from(timestamp))
        .sign_with_keys(keys)
        .expect("signed channel event")
}

async fn run_contract(store: &impl ChannelWindowContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("window-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("window-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let channel_id = Uuid::new_v4();
    let owner = Keys::generate();
    let owner_pubkey = owner.public_key().to_bytes();
    store
        .create_channel(community_a, channel_id, &owner_pubkey)
        .await
        .expect("channel A");
    store
        .create_channel(community_b, channel_id, &owner_pubkey)
        .await
        .expect("channel B");

    let base = u64::try_from(Utc::now().timestamp()).expect("positive current timestamp");
    let newest = signed_event(&owner, 9, "newest", base + 10);
    let root = signed_event(&owner, 9, "root", base + 9);
    let broadcast = signed_event(&Keys::generate(), 9, "broadcast", base + 8);
    let nested = signed_event(&Keys::generate(), 9, "nested", base + 7);
    let oldest = signed_event(&owner, 10, "oldest", base + 6);

    for event in [&newest, &root, &oldest] {
        assert!(
            store
                .insert(community_a, event, channel_id, None, None, 0, false)
                .await
                .expect("top-level event")
                .1
        );
    }
    assert!(
        store
            .insert(
                community_a,
                &broadcast,
                channel_id,
                Some(&root),
                Some(&root),
                1,
                true,
            )
            .await
            .expect("broadcast reply")
            .1
    );
    assert!(
        store
            .insert(
                community_a,
                &nested,
                channel_id,
                Some(&broadcast),
                Some(&root),
                2,
                false,
            )
            .await
            .expect("nested reply")
            .1
    );

    let first = store
        .window(community_a, channel_id, 2, None, None)
        .await
        .expect("first window");
    assert_eq!(first.rows.len(), 2);
    assert!(first.has_more);
    assert_eq!(first.rows[0].stored_event.event.id, newest.id);
    assert_eq!(first.rows[1].stored_event.event.id, root.id);
    let root_summary = first.rows[1]
        .thread_summary
        .as_ref()
        .expect("root thread summary");
    assert_eq!(root_summary.reply_count, 1);
    assert_eq!(root_summary.descendant_count, 2);
    assert_eq!(root_summary.participants.len(), 2);
    let cursor = first.next_cursor.clone().expect("first cursor");
    assert_eq!(cursor.0, event_created_at(&root));
    assert_eq!(cursor.1, root.id.as_bytes());

    let second = store
        .window(community_a, channel_id, 2, Some(cursor), None)
        .await
        .expect("second window");
    assert_eq!(second.rows.len(), 2);
    assert!(!second.has_more, "exact-multiple final page is exhausted");
    assert!(second.next_cursor.is_none());
    assert_eq!(second.rows[0].stored_event.event.id, broadcast.id);
    assert_eq!(second.rows[1].stored_event.event.id, oldest.id);
    let broadcast_summary = second.rows[0]
        .thread_summary
        .as_ref()
        .expect("broadcast reply has a direct child");
    assert_eq!(broadcast_summary.reply_count, 1);
    assert_eq!(broadcast_summary.descendant_count, 0);

    let kind_nine = store
        .window(community_a, channel_id, 10, None, Some(&[9]))
        .await
        .expect("kind-filtered window");
    assert_eq!(kind_nine.rows.len(), 3);
    assert!(kind_nine
        .rows
        .iter()
        .all(|row| row.stored_event.event.kind.as_u16() == 9));
    let empty_filter = store
        .window(community_a, channel_id, 10, None, Some(&[]))
        .await
        .expect("empty kind filter");
    assert_eq!(empty_filter.rows.len(), 4);

    assert!(
        store
            .insert(community_b, &newest, channel_id, None, None, 0, false,)
            .await
            .expect("same event in community B")
            .1
    );
    let foreign = store
        .window(community_b, channel_id, 10, None, None)
        .await
        .expect("community B window");
    assert_eq!(foreign.rows.len(), 1);
    assert_eq!(foreign.rows[0].stored_event.event.id, newest.id);

    let batch = store
        .batch(
            community_a,
            &[
                newest.id.as_bytes().as_slice(),
                nested.id.as_bytes().as_slice(),
                [0xff; 32].as_slice(),
            ],
        )
        .await
        .expect("event batch");
    assert_eq!(batch.len(), 2);
    assert!(batch.iter().any(|event| event.event.id == newest.id));
    assert!(batch.iter().any(|event| event.event.id == nested.id));
    assert!(store
        .batch(community_b, &[nested.id.as_bytes().as_slice()])
        .await
        .expect("cross-tenant event batch")
        .is_empty());
    assert!(store
        .batch(community_a, &[])
        .await
        .expect("empty event batch")
        .is_empty());

    assert!(store
        .soft_delete(community_a, newest.id.as_bytes())
        .await
        .expect("soft-delete newest"));
    assert!(store
        .batch(community_a, &[newest.id.as_bytes().as_slice()])
        .await
        .expect("deleted event batch")
        .is_empty());
    let after_delete = store
        .window(community_a, channel_id, 10, None, None)
        .await
        .expect("window after deletion");
    assert_eq!(after_delete.rows[0].stored_event.event.id, root.id);
}

#[tokio::test]
async fn sqlite_channel_window_contract() {
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
async fn sqlite_corrupt_window_row_advances_the_raw_cursor() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = SqliteStore::connect(
        &directory.path().join("buzz.sqlite3"),
        &SqliteConfig::default(),
    )
    .await
    .expect("SQLite connection");
    store.migrate().await.expect("SQLite migrations");
    let community = store
        .ensure_configured_community("corrupt-window.example.test")
        .await
        .expect("community")
        .id;
    let channel_id = Uuid::new_v4();
    let keys = Keys::generate();
    store
        .create_channel_with_id(
            community,
            channel_id,
            "corrupt-window",
            ChannelType::Stream,
            ChannelVisibility::Open,
            None,
            &keys.public_key().to_bytes(),
            None,
        )
        .await
        .expect("channel");
    let base = u64::try_from(Utc::now().timestamp()).expect("positive current timestamp");
    let older = signed_event(&keys, 9, "older valid row", base);
    let newer = signed_event(&keys, 9, "newer corrupt row", base + 1);
    store
        .insert_event(community, &older, Some(channel_id))
        .await
        .expect("older event");
    store
        .insert_event(community, &newer, Some(channel_id))
        .await
        .expect("newer event");
    sqlx::query("UPDATE events SET tags = ? WHERE community_id = ? AND id = ?")
        .bind("[1]")
        .bind(community.as_uuid().to_string())
        .bind(newer.id.as_bytes().as_slice())
        .execute(store.pool())
        .await
        .expect("corrupt newest event fixture");

    let first = store
        .get_channel_window(community, channel_id, 1, None, None)
        .await
        .expect("corrupt first page");
    assert!(first.rows.is_empty(), "corrupt retained rows are skipped");
    assert!(first.has_more);
    let cursor = first
        .next_cursor
        .expect("raw corrupt row still advances the cursor");
    assert_eq!(cursor.1, newer.id.as_bytes());

    let second = store
        .get_channel_window(community, channel_id, 1, Some(cursor), None)
        .await
        .expect("page after corrupt row");
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].stored_event.event.id, older.id);
    assert!(!second.has_more);
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_channel_window_contract() {
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
