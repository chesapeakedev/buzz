//! Shared direct-message contract for relational backends.

use async_trait::async_trait;
use uuid::Uuid;

use super::{SqliteConfig, SqliteStore};
use crate::channel::ChannelRecord;
use crate::dm::{compute_participant_hash, DmRecord};
use crate::{CommunityId, Db, DbError, EnsuredCommunityRecord, Result};

#[async_trait]
trait DmContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn find_dm(
        &self,
        community: CommunityId,
        participant_hash: &[u8],
    ) -> Result<Option<ChannelRecord>>;
    async fn create_dm(
        &self,
        community: CommunityId,
        participants: &[&[u8]],
        created_by: &[u8],
    ) -> Result<ChannelRecord>;
    async fn open_dm(
        &self,
        community: CommunityId,
        participants: &[&[u8]],
        created_by: &[u8],
    ) -> Result<(ChannelRecord, bool)>;
    async fn list_dms(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        limit: u32,
        cursor: Option<Uuid>,
    ) -> Result<Vec<DmRecord>>;
    async fn hide_dm(&self, community: CommunityId, channel_id: Uuid, pubkey: &[u8]) -> Result<()>;
    async fn unhide_dm(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<()>;
    async fn hidden_dms(&self, community: CommunityId, pubkey: &[u8]) -> Result<Vec<Uuid>>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl DmContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn find_dm(
                &self,
                community: CommunityId,
                participant_hash: &[u8],
            ) -> Result<Option<ChannelRecord>> {
                self.find_dm_by_participants(community, participant_hash)
                    .await
            }

            async fn create_dm(
                &self,
                community: CommunityId,
                participants: &[&[u8]],
                created_by: &[u8],
            ) -> Result<ChannelRecord> {
                self.create_dm(community, participants, created_by).await
            }

            async fn open_dm(
                &self,
                community: CommunityId,
                participants: &[&[u8]],
                created_by: &[u8],
            ) -> Result<(ChannelRecord, bool)> {
                self.open_dm(community, participants, created_by).await
            }

            async fn list_dms(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                limit: u32,
                cursor: Option<Uuid>,
            ) -> Result<Vec<DmRecord>> {
                self.list_dms_for_user(community, pubkey, limit, cursor)
                    .await
            }

            async fn hide_dm(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                pubkey: &[u8],
            ) -> Result<()> {
                self.hide_dm(community, channel_id, pubkey).await
            }

            async fn unhide_dm(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                pubkey: &[u8],
            ) -> Result<()> {
                self.unhide_dm(community, channel_id, pubkey).await
            }

            async fn hidden_dms(&self, community: CommunityId, pubkey: &[u8]) -> Result<Vec<Uuid>> {
                self.list_hidden_dms(community, pubkey).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl DmContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("dms-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("dms-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let alice = vec![0x11; 32];
    let bob = vec![0x22; 32];
    let carol = vec![0x33; 32];
    let participants = [alice.as_slice(), bob.as_slice()];
    let reverse = [bob.as_slice(), alice.as_slice()];

    let (first, first_created) = store
        .open_dm(community_a, &participants, &alice)
        .await
        .expect("open first DM");
    assert!(first_created);
    assert_eq!(first.channel_type, "dm");
    assert_eq!(first.visibility, "private");
    assert_eq!(first.name, "DM");

    let (same, same_created) = store
        .open_dm(community_a, &reverse, &alice)
        .await
        .expect("open reordered DM");
    assert!(!same_created);
    assert_eq!(same.id, first.id);
    let participant_hash = compute_participant_hash(&participants);
    assert_eq!(
        store
            .find_dm(community_a, participant_hash.as_slice())
            .await
            .expect("find DM")
            .expect("DM exists")
            .id,
        first.id
    );

    let foreign = store
        .create_dm(community_b, &participants, &bob)
        .await
        .expect("same participants in community B");
    assert_ne!(foreign.id, first.id);
    assert_eq!(
        store
            .find_dm(community_b, participant_hash.as_slice())
            .await
            .expect("find foreign DM")
            .expect("foreign DM exists")
            .id,
        foreign.id
    );

    let listed_alice = store
        .list_dms(community_a, &alice, 200, None)
        .await
        .expect("Alice DMs");
    assert_eq!(listed_alice.len(), 1);
    assert_eq!(listed_alice[0].channel_id, first.id);
    assert_eq!(listed_alice[0].participants.len(), 2);
    assert!(listed_alice[0]
        .participants
        .iter()
        .all(|participant| participant.role == "member"));
    assert!(store
        .list_dms(community_a, &carol, 200, None)
        .await
        .expect("Carol DMs")
        .is_empty());

    store
        .hide_dm(community_a, first.id, &alice)
        .await
        .expect("hide Alice DM");
    assert_eq!(
        store
            .hidden_dms(community_a, &alice)
            .await
            .expect("Alice hidden DMs"),
        vec![first.id]
    );
    assert!(store
        .list_dms(community_a, &alice, 200, None)
        .await
        .expect("hidden Alice DMs")
        .is_empty());
    assert_eq!(
        store
            .list_dms(community_a, &bob, 200, None)
            .await
            .expect("Bob still sees DM")
            .len(),
        1
    );
    let (_, reopened_created) = store
        .open_dm(community_a, &participants, &alice)
        .await
        .expect("reopen hidden DM");
    assert!(!reopened_created);
    assert!(store
        .hidden_dms(community_a, &alice)
        .await
        .expect("reopened hidden DMs")
        .is_empty());

    store
        .hide_dm(community_a, first.id, &alice)
        .await
        .expect("hide for explicit unhide");
    store
        .unhide_dm(community_a, first.id, &alice)
        .await
        .expect("explicit unhide");
    assert!(store
        .hidden_dms(community_a, &alice)
        .await
        .expect("explicitly unhidden")
        .is_empty());
    assert!(matches!(
        store
            .hide_dm(community_a, first.id, &carol)
            .await
            .expect_err("non-member cannot hide"),
        DbError::NotFound(_)
    ));

    let group_participants = [alice.as_slice(), bob.as_slice(), carol.as_slice()];
    let group = store
        .create_dm(community_a, &group_participants, &alice)
        .await
        .expect("group DM");
    assert_eq!(group.name, "Group DM (3)");
    let page = store
        .list_dms(community_a, &alice, 1, None)
        .await
        .expect("first DM page");
    assert_eq!(page.len(), 1);
    let next = store
        .list_dms(community_a, &alice, 200, Some(page[0].channel_id))
        .await
        .expect("next DM page");
    assert!(next
        .iter()
        .all(|record| record.channel_id != page[0].channel_id));

    let raced_participants = [alice.as_slice(), carol.as_slice()];
    let (left, right) = tokio::join!(
        store.open_dm(community_a, &raced_participants, &alice),
        store.open_dm(community_a, &raced_participants, &alice)
    );
    let left = left.expect("left concurrent open");
    let right = right.expect("right concurrent open");
    assert_eq!(usize::from(left.1) + usize::from(right.1), 1);
    assert_eq!(left.0.id, right.0.id);

    let short_pubkey = [0x44; 31];
    for invalid in [
        Vec::<&[u8]>::new(),
        vec![alice.as_slice()],
        vec![alice.as_slice(); 10],
        vec![alice.as_slice(), short_pubkey.as_slice()],
    ] {
        assert!(matches!(
            store.create_dm(community_a, &invalid, &alice).await,
            Err(DbError::InvalidData(_))
        ));
    }
}

#[tokio::test]
async fn sqlite_dm_contract() {
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
async fn postgres_dm_contract() {
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
