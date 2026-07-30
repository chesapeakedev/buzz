//! Shared reaction lifecycle contract for relational backends.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nostr::{Event, EventBuilder, Keys, Kind, Tag};

use buzz_core::{CommunityId, StoredEvent};

use super::{SqliteConfig, SqliteStore};
use crate::event::ReactionEventInsertOutcome;
use crate::reaction::{ActiveReactionRecord, BulkReactionEntry, ReactionGroup};
use crate::{Db, DbError, EnsuredCommunityRecord, Result};

#[async_trait]
trait ReactionContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn insert_event(
        &self,
        community: CommunityId,
        event: &Event,
    ) -> Result<(StoredEvent, bool)>;
    async fn get_event(
        &self,
        community: CommunityId,
        event_id: &[u8],
    ) -> Result<Option<StoredEvent>>;
    async fn add(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
        reaction_event_id: Option<&[u8]>,
    ) -> Result<bool>;
    async fn remove(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
    ) -> Result<bool>;
    async fn remove_by_source(
        &self,
        community: CommunityId,
        reaction_event_id: &[u8],
    ) -> Result<bool>;
    async fn active(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
    ) -> Result<Option<ActiveReactionRecord>>;
    async fn set_source(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        pubkey: &[u8],
        emoji: &str,
        reaction_event_id: &[u8],
    ) -> Result<bool>;
    async fn grouped(
        &self,
        community: CommunityId,
        event_id: &[u8],
        event_created_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ReactionGroup>>;
    async fn bulk(
        &self,
        community: CommunityId,
        event_ids: &[(&[u8], DateTime<Utc>)],
    ) -> Result<Vec<BulkReactionEntry>>;
    async fn insert_atomic(
        &self,
        community: CommunityId,
        reaction_event: &Event,
        target_event_id: &[u8],
        actor_pubkey: &[u8],
        emoji: &str,
    ) -> Result<ReactionEventInsertOutcome>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl ReactionContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn insert_event(
                &self,
                community: CommunityId,
                event: &Event,
            ) -> Result<(StoredEvent, bool)> {
                self.insert_event(community, event, None).await
            }

            async fn get_event(
                &self,
                community: CommunityId,
                event_id: &[u8],
            ) -> Result<Option<StoredEvent>> {
                self.get_event_by_id(community, event_id).await
            }

            async fn add(
                &self,
                community: CommunityId,
                event_id: &[u8],
                event_created_at: DateTime<Utc>,
                pubkey: &[u8],
                emoji: &str,
                reaction_event_id: Option<&[u8]>,
            ) -> Result<bool> {
                self.add_reaction(
                    community,
                    event_id,
                    event_created_at,
                    pubkey,
                    emoji,
                    reaction_event_id,
                )
                .await
            }

            async fn remove(
                &self,
                community: CommunityId,
                event_id: &[u8],
                event_created_at: DateTime<Utc>,
                pubkey: &[u8],
                emoji: &str,
            ) -> Result<bool> {
                self.remove_reaction(community, event_id, event_created_at, pubkey, emoji)
                    .await
            }

            async fn remove_by_source(
                &self,
                community: CommunityId,
                reaction_event_id: &[u8],
            ) -> Result<bool> {
                self.remove_reaction_by_source_event_id(community, reaction_event_id)
                    .await
            }

            async fn active(
                &self,
                community: CommunityId,
                event_id: &[u8],
                event_created_at: DateTime<Utc>,
                pubkey: &[u8],
                emoji: &str,
            ) -> Result<Option<ActiveReactionRecord>> {
                self.get_active_reaction_record(
                    community,
                    event_id,
                    event_created_at,
                    pubkey,
                    emoji,
                )
                .await
            }

            async fn set_source(
                &self,
                community: CommunityId,
                event_id: &[u8],
                event_created_at: DateTime<Utc>,
                pubkey: &[u8],
                emoji: &str,
                reaction_event_id: &[u8],
            ) -> Result<bool> {
                self.set_reaction_event_id(
                    community,
                    event_id,
                    event_created_at,
                    pubkey,
                    emoji,
                    reaction_event_id,
                )
                .await
            }

            async fn grouped(
                &self,
                community: CommunityId,
                event_id: &[u8],
                event_created_at: DateTime<Utc>,
                limit: u32,
            ) -> Result<Vec<ReactionGroup>> {
                self.get_reactions(community, event_id, event_created_at, limit, None)
                    .await
            }

            async fn bulk(
                &self,
                community: CommunityId,
                event_ids: &[(&[u8], DateTime<Utc>)],
            ) -> Result<Vec<BulkReactionEntry>> {
                self.get_reactions_bulk(community, event_ids).await
            }

            async fn insert_atomic(
                &self,
                community: CommunityId,
                reaction_event: &Event,
                target_event_id: &[u8],
                actor_pubkey: &[u8],
                emoji: &str,
            ) -> Result<ReactionEventInsertOutcome> {
                self.insert_reaction_event_with_thread_metadata(
                    community,
                    reaction_event,
                    None,
                    None,
                    target_event_id,
                    actor_pubkey,
                    emoji,
                )
                .await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

fn reaction_event(keys: &Keys, target: &Event, emoji: &str) -> Event {
    let nonce = uuid::Uuid::new_v4().to_string();
    EventBuilder::new(Kind::Custom(7), emoji)
        .tags(vec![
            Tag::parse(["e", target.id.to_hex().as_str()]).expect("reaction target tag"),
            Tag::parse(["nonce", nonce.as_str()]).expect("reaction nonce tag"),
        ])
        .sign_with_keys(keys)
        .expect("signed reaction event")
}

async fn run_contract(store: &impl ReactionContract) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("reactions-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("reactions-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let target_id = [0x41; 32];
    let second_target_id = [0x42; 32];
    let target_created_at =
        DateTime::from_timestamp(Utc::now().timestamp(), 0).expect("current timestamp");
    let alice = [0xa1; 32];
    let bob = [0xb2; 32];
    let source_a = [0xc3; 32];
    let source_b = [0xd4; 32];

    assert!(store
        .add(
            community_a,
            &target_id,
            target_created_at,
            &alice,
            "👍",
            None,
        )
        .await
        .expect("add Alice thumbs-up"));
    assert!(!store
        .add(
            community_a,
            &target_id,
            target_created_at,
            &alice,
            "👍",
            None,
        )
        .await
        .expect("duplicate Alice thumbs-up"));
    assert!(store
        .add(
            community_a,
            &target_id,
            target_created_at,
            &bob,
            "👍",
            Some(&source_b),
        )
        .await
        .expect("add Bob thumbs-up"));
    assert!(store
        .add(
            community_a,
            &target_id,
            target_created_at,
            &alice,
            "🎉",
            None,
        )
        .await
        .expect("add Alice celebration"));
    assert!(store
        .add(community_a, &target_id, target_created_at, &bob, "🎉", None,)
        .await
        .expect("add Bob celebration"));
    assert!(store
        .set_source(
            community_a,
            &target_id,
            target_created_at,
            &alice,
            "👍",
            &source_a,
        )
        .await
        .expect("set Alice source event"));
    assert_eq!(
        store
            .active(community_a, &target_id, target_created_at, &alice, "👍",)
            .await
            .expect("active Alice thumbs-up")
            .expect("Alice reaction")
            .reaction_event_id
            .as_deref(),
        Some(source_a.as_slice())
    );

    let groups = store
        .grouped(community_a, &target_id, target_created_at, 100)
        .await
        .expect("grouped reactions");
    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups
            .iter()
            .find(|group| group.emoji == "👍")
            .expect("thumbs-up group")
            .count,
        2
    );
    assert_eq!(
        groups
            .iter()
            .find(|group| group.emoji == "🎉")
            .expect("celebration group")
            .count,
        2
    );
    let limited = store
        .grouped(community_a, &target_id, target_created_at, 1)
        .await
        .expect("limited grouped reactions");
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].count, 2, "limit applies to groups, not rows");

    let bulk = store
        .bulk(
            community_a,
            &[
                (target_id.as_slice(), target_created_at),
                (second_target_id.as_slice(), target_created_at),
            ],
        )
        .await
        .expect("bulk reactions");
    assert_eq!(bulk.len(), 1);
    assert_eq!(bulk[0].event_id, target_id);
    assert_eq!(bulk[0].reactions.len(), 2);

    assert!(store
        .grouped(community_b, &target_id, target_created_at, 100)
        .await
        .expect("community B grouped reactions")
        .is_empty());
    assert!(!store
        .remove(community_b, &target_id, target_created_at, &alice, "👍",)
        .await
        .expect("cross-tenant remove"));
    assert!(store
        .add(
            community_b,
            &target_id,
            target_created_at,
            &alice,
            "👍",
            None,
        )
        .await
        .expect("same tuple in community B"));

    assert!(store
        .remove_by_source(community_a, &source_a)
        .await
        .expect("remove Alice by source"));
    assert!(store
        .active(community_a, &target_id, target_created_at, &alice, "👍",)
        .await
        .expect("Alice after source removal")
        .is_none());
    assert!(store
        .add(
            community_a,
            &target_id,
            target_created_at,
            &alice,
            "👍",
            None,
        )
        .await
        .expect("reactivate Alice"));
    assert_eq!(
        store
            .active(community_a, &target_id, target_created_at, &alice, "👍",)
            .await
            .expect("reactivated Alice")
            .expect("active Alice")
            .reaction_event_id
            .as_deref(),
        Some(source_a.as_slice()),
        "reactivation without a new source preserves the prior event ID"
    );

    let race_target = [0x51; 32];
    let (race_a, race_b) = tokio::join!(
        store.add(
            community_a,
            &race_target,
            target_created_at,
            &alice,
            "✅",
            None,
        ),
        store.add(
            community_a,
            &race_target,
            target_created_at,
            &alice,
            "✅",
            None,
        )
    );
    assert_ne!(
        race_a.expect("concurrent add A"),
        race_b.expect("concurrent add B"),
        "exactly one concurrent add must win"
    );

    let target = EventBuilder::new(Kind::Custom(9), "atomic target")
        .sign_with_keys(&Keys::generate())
        .expect("signed target");
    store
        .insert_event(community_a, &target)
        .await
        .expect("insert atomic target");
    let actor = Keys::generate();
    let actor_pubkey = actor.public_key().to_bytes();
    let first = reaction_event(&actor, &target, "🔥");
    assert!(matches!(
        store
            .insert_atomic(
                community_a,
                &first,
                target.id.as_bytes(),
                &actor_pubkey,
                "🔥",
            )
            .await
            .expect("atomic reaction"),
        ReactionEventInsertOutcome::Inserted {
            was_inserted: true,
            ..
        }
    ));
    assert!(store
        .get_event(community_a, first.id.as_bytes())
        .await
        .expect("stored reaction event")
        .is_some());

    let duplicate = reaction_event(&actor, &target, "🔥");
    assert!(matches!(
        store
            .insert_atomic(
                community_a,
                &duplicate,
                target.id.as_bytes(),
                &actor_pubkey,
                "🔥",
            )
            .await
            .expect("atomic duplicate"),
        ReactionEventInsertOutcome::Duplicate
    ));
    assert!(store
        .get_event(community_a, duplicate.id.as_bytes())
        .await
        .expect("duplicate event lookup")
        .is_none());

    let missing = reaction_event(&actor, &target, "❓");
    assert!(matches!(
        store
            .insert_atomic(
                community_b,
                &missing,
                target.id.as_bytes(),
                &actor_pubkey,
                "❓",
            )
            .await
            .expect("missing cross-tenant target"),
        ReactionEventInsertOutcome::TargetMissing
    ));
    assert!(store
        .get_event(community_b, missing.id.as_bytes())
        .await
        .expect("missing-target event lookup")
        .is_none());

    let ephemeral = EventBuilder::new(Kind::Custom(20_000), "💥")
        .sign_with_keys(&Keys::generate())
        .expect("signed ephemeral event");
    let rollback_actor = ephemeral.pubkey.to_bytes();
    let error = store
        .insert_atomic(
            community_a,
            &ephemeral,
            target.id.as_bytes(),
            &rollback_actor,
            "💥",
        )
        .await
        .expect_err("ephemeral reaction event must fail");
    assert!(matches!(error, DbError::EphemeralEventRejected(20_000)));
    let target_timestamp = DateTime::from_timestamp(
        i64::try_from(target.created_at.as_secs()).expect("target timestamp in i64"),
        0,
    )
    .expect("target timestamp");
    assert!(store
        .active(
            community_a,
            target.id.as_bytes(),
            target_timestamp,
            &rollback_actor,
            "💥",
        )
        .await
        .expect("rolled-back reaction lookup")
        .is_none());
}

#[tokio::test]
async fn sqlite_reaction_contract() {
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
async fn postgres_reaction_contract() {
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
