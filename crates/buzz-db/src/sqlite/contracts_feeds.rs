//! Shared home-feed contract for relational backends.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};
use uuid::Uuid;

use buzz_core::kind::{
    KIND_FORUM_POST, KIND_JOB_PROGRESS, KIND_STREAM_MESSAGE, KIND_STREAM_REMINDER, KIND_TEXT_NOTE,
    KIND_WORKFLOW_APPROVAL_REQUESTED,
};
use buzz_core::{CommunityId, StoredEvent};

use super::{SqliteConfig, SqliteStore};
use crate::channel::{ChannelType, ChannelVisibility};
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait FeedContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn create_channel(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        owner: &[u8],
    ) -> Result<()>;
    async fn insert(
        &self,
        community: CommunityId,
        event: &Event,
        channel_id: Option<Uuid>,
    ) -> Result<()>;
    async fn soft_delete(&self, community: CommunityId, event_id: &[u8]) -> Result<bool>;
    async fn mentions(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        channels: &[Uuid],
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<StoredEvent>>;
    async fn needs_action(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        channels: &[Uuid],
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<StoredEvent>>;
    async fn activity(
        &self,
        community: CommunityId,
        channels: &[Uuid],
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<StoredEvent>>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl FeedContract for $backend {
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
                    "feed-contract",
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
                channel_id: Option<Uuid>,
            ) -> Result<()> {
                self.insert_event(community, event, channel_id)
                    .await
                    .map(|_| ())
            }

            async fn soft_delete(&self, community: CommunityId, event_id: &[u8]) -> Result<bool> {
                self.soft_delete_event(community, event_id).await
            }

            async fn mentions(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                channels: &[Uuid],
                since: Option<DateTime<Utc>>,
                limit: i64,
            ) -> Result<Vec<StoredEvent>> {
                self.query_feed_mentions(community, pubkey, channels, since, limit)
                    .await
            }

            async fn needs_action(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                channels: &[Uuid],
                since: Option<DateTime<Utc>>,
                limit: i64,
            ) -> Result<Vec<StoredEvent>> {
                self.query_feed_needs_action(community, pubkey, channels, since, limit)
                    .await
            }

            async fn activity(
                &self,
                community: CommunityId,
                channels: &[Uuid],
                since: Option<DateTime<Utc>>,
                limit: i64,
            ) -> Result<Vec<StoredEvent>> {
                self.query_feed_activity(community, channels, since, limit)
                    .await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

fn signed_event(
    keys: &Keys,
    kind: u32,
    content: &str,
    timestamp: u64,
    mentioned_pubkey: Option<&str>,
) -> Event {
    let builder = EventBuilder::new(
        Kind::Custom(u16::try_from(kind).expect("test kind in u16")),
        content,
    )
    .custom_created_at(Timestamp::from(timestamp));
    let builder = if let Some(pubkey) = mentioned_pubkey {
        builder.tags(vec![
            Tag::parse(["p", pubkey]).expect("valid mentioned pubkey tag")
        ])
    } else {
        builder
    };
    builder.sign_with_keys(keys).expect("signed feed event")
}

async fn run_contract(store: &impl FeedContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("feeds-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("feeds-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let accessible = Uuid::new_v4();
    let inaccessible = Uuid::new_v4();
    let keys = Keys::generate();
    let owner = keys.public_key().to_bytes();
    for channel in [accessible, inaccessible] {
        store
            .create_channel(community_a, channel, &owner)
            .await
            .expect("community A channel");
    }
    store
        .create_channel(community_b, accessible, &owner)
        .await
        .expect("community B same channel");

    let mentioned = [0x77; 32];
    let other = [0x88; 32];
    let mentioned_hex = hex::encode(mentioned);
    let other_hex = hex::encode(other);
    let base = u64::try_from(Utc::now().timestamp()).expect("positive current timestamp");

    let global_mention = signed_event(
        &keys,
        KIND_TEXT_NOTE,
        "global mention",
        base + 10,
        Some(&mentioned_hex),
    );
    let accessible_mention = signed_event(
        &keys,
        KIND_STREAM_MESSAGE,
        "accessible mention",
        base + 9,
        Some(&mentioned_hex),
    );
    let inaccessible_mention = signed_event(
        &keys,
        KIND_STREAM_MESSAGE,
        "inaccessible mention",
        base + 8,
        Some(&mentioned_hex),
    );
    let approval = signed_event(
        &keys,
        KIND_WORKFLOW_APPROVAL_REQUESTED,
        "accessible approval",
        base + 7,
        Some(&mentioned_hex),
    );
    let reminder = signed_event(
        &keys,
        KIND_STREAM_REMINDER,
        "global reminder",
        base + 6,
        Some(&mentioned_hex),
    );
    let inaccessible_approval = signed_event(
        &keys,
        KIND_WORKFLOW_APPROVAL_REQUESTED,
        "inaccessible approval",
        base + 5,
        Some(&mentioned_hex),
    );
    let other_mention = signed_event(
        &keys,
        KIND_STREAM_MESSAGE,
        "other user",
        base + 4,
        Some(&other_hex),
    );
    let global_activity = signed_event(&keys, KIND_FORUM_POST, "global activity", base + 3, None);
    let inaccessible_activity = signed_event(
        &keys,
        KIND_JOB_PROGRESS,
        "inaccessible activity",
        base + 2,
        None,
    );
    let foreign_mention = signed_event(
        &keys,
        KIND_STREAM_MESSAGE,
        "foreign mention",
        base + 11,
        Some(&mentioned_hex),
    );

    for (event, channel) in [
        (&global_mention, None),
        (&accessible_mention, Some(accessible)),
        (&inaccessible_mention, Some(inaccessible)),
        (&approval, Some(accessible)),
        (&reminder, None),
        (&inaccessible_approval, Some(inaccessible)),
        (&other_mention, Some(accessible)),
        (&global_activity, None),
        (&inaccessible_activity, Some(inaccessible)),
    ] {
        store
            .insert(community_a, event, channel)
            .await
            .expect("community A feed event");
    }
    store
        .insert(community_b, &foreign_mention, Some(accessible))
        .await
        .expect("community B feed event");

    let mentions = store
        .mentions(community_a, &mentioned, &[accessible], None, 100)
        .await
        .expect("mentions");
    assert_eq!(mentions.len(), 2);
    assert_eq!(mentions[0].event.id, global_mention.id);
    assert_eq!(mentions[1].event.id, accessible_mention.id);
    assert!(mentions
        .iter()
        .all(|event| event.event.id != foreign_mention.id));
    let global_mentions = store
        .mentions(community_a, &mentioned, &[], None, 100)
        .await
        .expect("global-only mentions");
    assert_eq!(global_mentions.len(), 1);
    assert_eq!(global_mentions[0].event.id, global_mention.id);
    let recent_mentions = store
        .mentions(
            community_a,
            &mentioned,
            &[accessible],
            Some(
                DateTime::from_timestamp(i64::try_from(base + 10).expect("timestamp"), 0)
                    .expect("recent timestamp"),
            ),
            100,
        )
        .await
        .expect("recent mentions");
    assert_eq!(recent_mentions.len(), 1);
    assert_eq!(recent_mentions[0].event.id, global_mention.id);
    assert_eq!(
        store
            .mentions(community_a, &mentioned, &[accessible], None, 1)
            .await
            .expect("limited mentions")
            .len(),
        1
    );

    let needs_action = store
        .needs_action(community_a, &mentioned, &[accessible], None, 100)
        .await
        .expect("needs-action feed");
    assert_eq!(needs_action.len(), 2);
    assert_eq!(needs_action[0].event.id, approval.id);
    assert_eq!(needs_action[1].event.id, reminder.id);
    let global_actions = store
        .needs_action(community_a, &mentioned, &[], None, 100)
        .await
        .expect("global-only needs-action feed");
    assert_eq!(global_actions.len(), 1);
    assert_eq!(global_actions[0].event.id, reminder.id);

    let activity = store
        .activity(community_a, &[accessible], None, 100)
        .await
        .expect("activity");
    assert!(activity
        .iter()
        .any(|event| event.event.id == accessible_mention.id));
    assert!(activity
        .iter()
        .any(|event| event.event.id == global_activity.id));
    assert!(activity
        .iter()
        .all(|event| event.event.id != inaccessible_activity.id));
    assert!(activity.iter().all(|event| event.event.id != approval.id));
    let global_activity_only = store
        .activity(community_a, &[], None, 100)
        .await
        .expect("global-only activity");
    assert_eq!(global_activity_only.len(), 1);
    assert_eq!(global_activity_only[0].event.id, global_activity.id);

    assert!(store
        .soft_delete(community_a, global_mention.id.as_bytes())
        .await
        .expect("delete global mention"));
    let after_delete = store
        .mentions(community_a, &mentioned, &[accessible], None, 100)
        .await
        .expect("mentions after deletion");
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].event.id, accessible_mention.id);

    for index in 0..105 {
        let event = signed_event(
            &keys,
            KIND_JOB_PROGRESS,
            &format!("bounded activity {index}"),
            base + 20,
            None,
        );
        store
            .insert(community_a, &event, None)
            .await
            .expect("bounded activity event");
    }
    assert_eq!(
        store
            .activity(community_a, &[accessible], None, 1_000)
            .await
            .expect("capped activity")
            .len(),
        crate::feed::FEED_MAX_LIMIT as usize
    );
}

#[tokio::test]
async fn sqlite_feed_contract() {
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
async fn postgres_feed_contract() {
    let db = Db::new(&crate::DbConfig {
        database_url: "postgres://buzz:buzz_dev@localhost:5432/buzz".to_owned(),
        read_database_url: None,
        read_max_connections: None,
        replica_read_max_age_ms: 0,
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
