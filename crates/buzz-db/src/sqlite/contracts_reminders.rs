//! Shared due-reminder query and delivery-claim contract.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};

use buzz_core::kind::KIND_EVENT_REMINDER;
use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::event::DueReminder;
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait ReminderContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool>;
    async fn query_due(&self, now_secs: i64, limit: i64) -> Result<Vec<DueReminder>>;
    async fn claim(
        &self,
        community: CommunityId,
        event_id: &[u8],
        created_at: DateTime<Utc>,
    ) -> Result<bool>;
    async fn claim_with_stamp(
        &self,
        community: CommunityId,
        event_id: &[u8],
        created_at: DateTime<Utc>,
        stamp: i64,
    ) -> Result<bool>;
    async fn release(
        &self,
        community: CommunityId,
        event_id: &[u8],
        created_at: DateTime<Utc>,
        stamp: i64,
    ) -> Result<bool>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl ReminderContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
                self.ensure_user(community, pubkey).await
            }

            async fn insert_event(&self, community: CommunityId, event: &Event) -> Result<bool> {
                Ok(self.insert_event(community, event, None).await?.1)
            }

            async fn query_due(&self, now_secs: i64, limit: i64) -> Result<Vec<DueReminder>> {
                self.query_due_reminders(now_secs, limit).await
            }

            async fn claim(
                &self,
                community: CommunityId,
                event_id: &[u8],
                created_at: DateTime<Utc>,
            ) -> Result<bool> {
                self.claim_due_reminder(community, event_id, created_at)
                    .await
            }

            async fn claim_with_stamp(
                &self,
                community: CommunityId,
                event_id: &[u8],
                created_at: DateTime<Utc>,
                stamp: i64,
            ) -> Result<bool> {
                self.claim_due_reminder_with_stamp(community, event_id, created_at, stamp)
                    .await
            }

            async fn release(
                &self,
                community: CommunityId,
                event_id: &[u8],
                created_at: DateTime<Utc>,
                stamp: i64,
            ) -> Result<bool> {
                self.release_due_reminder(community, event_id, created_at, stamp)
                    .await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

fn reminder(keys: &Keys, d_tag: &str, not_before: i64, created_at: u64, body: &str) -> Event {
    EventBuilder::new(Kind::Custom(KIND_EVENT_REMINDER as u16), body)
        .tags([
            Tag::parse(["d", d_tag]).expect("d tag"),
            Tag::parse(["not_before", &not_before.to_string()]).expect("not_before tag"),
        ])
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(keys)
        .expect("signed reminder")
}

fn event_created_at(event: &Event) -> DateTime<Utc> {
    DateTime::from_timestamp(event.created_at.as_secs() as i64, 0).expect("event timestamp")
}

async fn run_contract(store: &impl ReminderContract) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let host_a = format!("reminder-a-{suffix}.example.test");
    let host_b = format!("reminder-b-{suffix}.example.test");
    let community_a = store
        .ensure_community(&host_a)
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&host_b)
        .await
        .expect("community B")
        .id;
    let keys = Keys::generate();
    let pubkey = keys.public_key().to_bytes();
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &pubkey)
            .await
            .expect("reminder author");
    }

    let now = Utc::now().timestamp();
    let created = u64::try_from(now - 10).expect("positive timestamp");
    let shared = reminder(
        &keys,
        &format!("shared-{suffix}"),
        now - 1,
        created,
        "shared",
    );
    assert!(store
        .insert_event(community_a, &shared)
        .await
        .expect("insert A/X"));
    assert!(store
        .insert_event(community_b, &shared)
        .await
        .expect("insert B/X"));

    let older = reminder(
        &keys,
        &format!("head-{suffix}"),
        now - 1,
        created - 2,
        "older",
    );
    let newer = reminder(
        &keys,
        &format!("head-{suffix}"),
        now - 1,
        created - 1,
        "newer",
    );
    let future = reminder(
        &keys,
        &format!("future-{suffix}"),
        now + 3600,
        created,
        "future",
    );
    for event in [&older, &newer, &future] {
        assert!(store
            .insert_event(community_a, event)
            .await
            .expect("insert reminder"));
    }

    let due = store.query_due(now, 1000).await.expect("query due");
    assert!(due.iter().any(|row| {
        row.community_id == community_a
            && row.host == host_a
            && row.id == shared.id.as_bytes()
            && row.channel_id.is_none()
    }));
    assert!(due.iter().any(|row| {
        row.community_id == community_b && row.host == host_b && row.id == shared.id.as_bytes()
    }));
    assert!(!due.iter().any(|row| row.id == older.id.as_bytes()));
    assert!(due.iter().any(|row| row.id == newer.id.as_bytes()));
    assert!(!due.iter().any(|row| row.id == future.id.as_bytes()));

    let created_at = event_created_at(&shared);
    let stamp_a = 0x1111_2222_3333_4444;
    let stamp_b = 0x5555_6666_7777_1111;
    let claim_a = store.claim_with_stamp(community_a, shared.id.as_bytes(), created_at, stamp_a);
    let claim_a_race =
        store.claim_with_stamp(community_a, shared.id.as_bytes(), created_at, stamp_b);
    let (claim_a, claim_a_race) = tokio::join!(claim_a, claim_a_race);
    assert_ne!(
        claim_a.expect("claim A"),
        claim_a_race.expect("racing claim A"),
        "exactly one delivery claim must win"
    );
    assert!(store
        .claim_with_stamp(community_b, shared.id.as_bytes(), created_at, stamp_a,)
        .await
        .expect("same event remains claimable in B"));
    assert!(!store
        .release(community_b, shared.id.as_bytes(), created_at, stamp_b,)
        .await
        .expect("wrong-stamp release"));
    assert!(store
        .release(community_b, shared.id.as_bytes(), created_at, stamp_a,)
        .await
        .expect("matching-stamp release"));
    assert!(store
        .claim(community_b, shared.id.as_bytes(), created_at)
        .await
        .expect("default-stamp reclaim"));
    assert!(!store
        .claim(community_b, shared.id.as_bytes(), created_at)
        .await
        .expect("already claimed"));

    let due_after_claim = store.query_due(now, 1000).await.expect("query after claim");
    assert!(!due_after_claim
        .iter()
        .any(|row| row.id == shared.id.as_bytes()));
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
async fn sqlite_reminder_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_reminder_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
