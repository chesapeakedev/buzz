//! Shared channel-foundation contract for relational backends.

use async_trait::async_trait;
use uuid::Uuid;

use super::{SqliteConfig, SqliteStore};
use crate::channel::{ChannelRecord, ChannelType, ChannelVisibility, MemberRecord, MemberRole};
use crate::{CommunityId, Db, DbError, EnsuredCommunityRecord, Result};

#[async_trait]
trait ChannelFoundationContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn create_with_id(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        name: &str,
        channel_type: ChannelType,
        visibility: ChannelVisibility,
        created_by: &[u8],
    ) -> Result<(ChannelRecord, bool)>;
    async fn get_channel(&self, community: CommunityId, channel_id: Uuid) -> Result<ChannelRecord>;
    async fn list_channels(
        &self,
        community: CommunityId,
        visibility: Option<&str>,
    ) -> Result<Vec<ChannelRecord>>;
    async fn get_members(
        &self,
        community: CommunityId,
        channel_id: Uuid,
    ) -> Result<Vec<MemberRecord>>;
    async fn membership_pairs(
        &self,
        community: CommunityId,
        channel_ids: &[Uuid],
        pubkeys: &[Vec<u8>],
    ) -> Result<Vec<(Uuid, Vec<u8>)>>;
    async fn get_members_bulk(
        &self,
        community: CommunityId,
        channel_ids: &[Uuid],
    ) -> Result<Vec<MemberRecord>>;
    async fn add_member(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
        role: MemberRole,
        invited_by: Option<&[u8]>,
    ) -> Result<MemberRecord>;
    async fn remove_member(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
        actor_pubkey: &[u8],
    ) -> Result<()>;
    async fn is_member(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<bool>;
    async fn get_member_role(
        &self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<Option<String>>;
    async fn get_member_count(&self, community: CommunityId, channel_id: Uuid) -> Result<i64>;
    async fn get_member_counts_bulk(
        &self,
        community: CommunityId,
        channel_ids: &[Uuid],
    ) -> Result<std::collections::HashMap<Uuid, i64>>;
    async fn get_accessible_channel_ids(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<Vec<Uuid>>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl ChannelFoundationContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn create_with_id(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                name: &str,
                channel_type: ChannelType,
                visibility: ChannelVisibility,
                created_by: &[u8],
            ) -> Result<(ChannelRecord, bool)> {
                self.create_channel_with_id(
                    community,
                    channel_id,
                    name,
                    channel_type,
                    visibility,
                    None,
                    created_by,
                    None,
                )
                .await
            }

            async fn get_channel(
                &self,
                community: CommunityId,
                channel_id: Uuid,
            ) -> Result<ChannelRecord> {
                self.get_channel(community, channel_id).await
            }

            async fn list_channels(
                &self,
                community: CommunityId,
                visibility: Option<&str>,
            ) -> Result<Vec<ChannelRecord>> {
                self.list_channels(community, visibility).await
            }

            async fn get_members(
                &self,
                community: CommunityId,
                channel_id: Uuid,
            ) -> Result<Vec<MemberRecord>> {
                self.get_members(community, channel_id).await
            }

            async fn membership_pairs(
                &self,
                community: CommunityId,
                channel_ids: &[Uuid],
                pubkeys: &[Vec<u8>],
            ) -> Result<Vec<(Uuid, Vec<u8>)>> {
                self.membership_pairs(community, channel_ids, pubkeys).await
            }

            async fn get_members_bulk(
                &self,
                community: CommunityId,
                channel_ids: &[Uuid],
            ) -> Result<Vec<MemberRecord>> {
                self.get_members_bulk(community, channel_ids).await
            }

            async fn add_member(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                pubkey: &[u8],
                role: MemberRole,
                invited_by: Option<&[u8]>,
            ) -> Result<MemberRecord> {
                self.add_member(community, channel_id, pubkey, role, invited_by)
                    .await
            }

            async fn remove_member(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                pubkey: &[u8],
                actor_pubkey: &[u8],
            ) -> Result<()> {
                self.remove_member(community, channel_id, pubkey, actor_pubkey)
                    .await
            }

            async fn is_member(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                pubkey: &[u8],
            ) -> Result<bool> {
                self.is_member(community, channel_id, pubkey).await
            }

            async fn get_member_role(
                &self,
                community: CommunityId,
                channel_id: Uuid,
                pubkey: &[u8],
            ) -> Result<Option<String>> {
                self.get_member_role(community, channel_id, pubkey).await
            }

            async fn get_member_count(
                &self,
                community: CommunityId,
                channel_id: Uuid,
            ) -> Result<i64> {
                self.get_member_count(community, channel_id).await
            }

            async fn get_member_counts_bulk(
                &self,
                community: CommunityId,
                channel_ids: &[Uuid],
            ) -> Result<std::collections::HashMap<Uuid, i64>> {
                self.get_member_counts_bulk(community, channel_ids).await
            }

            async fn get_accessible_channel_ids(
                &self,
                community: CommunityId,
                pubkey: &[u8],
            ) -> Result<Vec<Uuid>> {
                self.get_accessible_channel_ids(community, pubkey).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl ChannelFoundationContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("channels-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("channels-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let channel_id = Uuid::new_v4();
    let owner_a = vec![0xa1; 32];
    let owner_b = vec![0xb2; 32];

    let (channel_a, inserted_a) = store
        .create_with_id(
            community_a,
            channel_id,
            "  ##general  ",
            ChannelType::Stream,
            ChannelVisibility::Open,
            &owner_a,
        )
        .await
        .expect("create channel A");
    assert!(inserted_a);
    assert_eq!(channel_a.id, channel_id);
    assert_eq!(channel_a.name, "general");
    assert_eq!(channel_a.channel_type, ChannelType::Stream.as_str());
    assert_eq!(channel_a.visibility, ChannelVisibility::Open.as_str());
    assert_eq!(channel_a.created_by, owner_a);

    let (duplicate, duplicate_inserted) = store
        .create_with_id(
            community_a,
            channel_id,
            "replacement-name",
            ChannelType::Forum,
            ChannelVisibility::Private,
            &owner_b,
        )
        .await
        .expect("duplicate channel A");
    assert!(!duplicate_inserted);
    assert_eq!(duplicate.name, "general");
    assert_eq!(duplicate.created_by, owner_a);

    let (channel_b, inserted_b) = store
        .create_with_id(
            community_b,
            channel_id,
            "private-room",
            ChannelType::Forum,
            ChannelVisibility::Private,
            &owner_b,
        )
        .await
        .expect("same UUID in community B");
    assert!(inserted_b);
    assert_eq!(channel_b.created_by, owner_b);

    assert_eq!(
        store
            .get_channel(community_a, channel_id)
            .await
            .expect("read A")
            .name,
        "general"
    );
    assert_eq!(
        store
            .get_channel(community_b, channel_id)
            .await
            .expect("read B")
            .name,
        "private-room"
    );

    let members_a = store
        .get_members(community_a, channel_id)
        .await
        .expect("members A");
    assert_eq!(members_a.len(), 1);
    assert_eq!(members_a[0].pubkey, owner_a);
    assert_eq!(members_a[0].role, MemberRole::Owner.as_str());
    assert_eq!(members_a[0].invited_by.as_deref(), Some(owner_a.as_slice()));

    assert_eq!(
        store
            .list_channels(community_a, Some("open"))
            .await
            .expect("open A")
            .len(),
        1
    );
    assert!(store
        .list_channels(community_a, Some("private"))
        .await
        .expect("private A")
        .is_empty());
    assert_eq!(
        store
            .list_channels(community_b, Some("private"))
            .await
            .expect("private B")
            .len(),
        1
    );

    let missing = store
        .get_channel(CommunityId::from_uuid(Uuid::new_v4()), channel_id)
        .await
        .expect_err("unknown tenant cannot read channel");
    assert!(matches!(missing, DbError::ChannelNotFound(id) if id == channel_id));

    let raced_id = Uuid::new_v4();
    let (left, right) = tokio::join!(
        store.create_with_id(
            community_a,
            raced_id,
            "race",
            ChannelType::Stream,
            ChannelVisibility::Open,
            &owner_a,
        ),
        store.create_with_id(
            community_a,
            raced_id,
            "race",
            ChannelType::Stream,
            ChannelVisibility::Open,
            &owner_a,
        )
    );
    let inserted =
        usize::from(left.expect("left create").1) + usize::from(right.expect("right create").1);
    assert_eq!(inserted, 1);
    assert_eq!(
        store
            .get_members(community_a, raced_id)
            .await
            .expect("raced members")
            .len(),
        1
    );

    let private_id = Uuid::new_v4();
    store
        .create_with_id(
            community_a,
            private_id,
            "private-membership",
            ChannelType::Stream,
            ChannelVisibility::Private,
            &owner_a,
        )
        .await
        .expect("private channel");
    let member = vec![0xc3; 32];
    let outsider = vec![0xd4; 32];
    let no_invite = store
        .add_member(community_a, private_id, &member, MemberRole::Member, None)
        .await
        .expect_err("private join requires invite");
    assert!(matches!(no_invite, DbError::AccessDenied(_)));
    store
        .add_member(
            community_a,
            private_id,
            &member,
            MemberRole::Member,
            Some(&owner_a),
        )
        .await
        .expect("owner invite");
    assert!(store
        .is_member(community_a, private_id, &member)
        .await
        .expect("active member"));
    assert_eq!(
        store
            .get_member_role(community_a, private_id, &member)
            .await
            .expect("member role")
            .as_deref(),
        Some(MemberRole::Member.as_str())
    );
    assert_eq!(
        store
            .get_member_count(community_a, private_id)
            .await
            .expect("member count"),
        2
    );
    let pairs = store
        .membership_pairs(
            community_a,
            &[private_id, Uuid::new_v4()],
            &[owner_a.clone(), member.clone(), outsider.clone()],
        )
        .await
        .expect("membership pairs");
    assert_eq!(pairs.len(), 2);
    assert!(pairs.contains(&(private_id, owner_a.clone())));
    assert!(pairs.contains(&(private_id, member.clone())));
    assert!(store
        .membership_pairs(community_a, &[], std::slice::from_ref(&owner_a))
        .await
        .expect("empty channel pairs")
        .is_empty());
    assert_eq!(
        store
            .get_members_bulk(community_a, &[private_id, channel_id])
            .await
            .expect("bulk members")
            .len(),
        3
    );
    let counts = store
        .get_member_counts_bulk(community_a, &[private_id, channel_id, Uuid::new_v4()])
        .await
        .expect("bulk counts");
    assert_eq!(counts.get(&private_id), Some(&2));
    assert_eq!(counts.get(&channel_id), Some(&1));

    let unauthorized_elevation = store
        .add_member(
            community_a,
            private_id,
            &outsider,
            MemberRole::Admin,
            Some(&member),
        )
        .await
        .expect_err("ordinary member cannot grant admin");
    assert!(matches!(unauthorized_elevation, DbError::AccessDenied(_)));
    let unauthorized_remove = store
        .remove_member(community_a, private_id, &owner_a, &member)
        .await
        .expect_err("ordinary member cannot remove owner");
    assert!(matches!(unauthorized_remove, DbError::AccessDenied(_)));

    store
        .remove_member(community_a, private_id, &member, &owner_a)
        .await
        .expect("owner removes member");
    assert!(!store
        .is_member(community_a, private_id, &member)
        .await
        .expect("removed member"));
    store
        .add_member(
            community_a,
            private_id,
            &member,
            MemberRole::Member,
            Some(&owner_a),
        )
        .await
        .expect("reactivate member");
    assert_eq!(
        store
            .get_member_count(community_a, private_id)
            .await
            .expect("reactivated count"),
        2
    );

    let last_owner = store
        .remove_member(community_a, private_id, &owner_a, &owner_a)
        .await
        .expect_err("last owner is protected");
    assert!(matches!(last_owner, DbError::AccessDenied(_)));

    let accessible = store
        .get_accessible_channel_ids(community_a, &member)
        .await
        .expect("accessible channels");
    assert!(accessible.contains(&channel_id));
    assert!(accessible.contains(&raced_id));
    assert!(accessible.contains(&private_id));
    assert!(!store
        .is_member(community_b, private_id, &member)
        .await
        .expect("foreign membership"));

    let governance_id = Uuid::new_v4();
    store
        .create_with_id(
            community_a,
            governance_id,
            "governance-race",
            ChannelType::Stream,
            ChannelVisibility::Private,
            &owner_a,
        )
        .await
        .expect("governance channel");
    store
        .add_member(
            community_a,
            governance_id,
            &owner_b,
            MemberRole::Owner,
            Some(&owner_a),
        )
        .await
        .expect("second owner");
    let (demote_a, demote_b) = tokio::join!(
        store.add_member(
            community_a,
            governance_id,
            &owner_a,
            MemberRole::Member,
            Some(&owner_b),
        ),
        store.add_member(
            community_a,
            governance_id,
            &owner_b,
            MemberRole::Member,
            Some(&owner_a),
        )
    );
    assert_eq!(
        usize::from(demote_a.is_ok()) + usize::from(demote_b.is_ok()),
        1,
        "serialized demotions must preserve one owner"
    );
    let owners = store
        .get_members(community_a, governance_id)
        .await
        .expect("governance members")
        .into_iter()
        .filter(|member| member.role == MemberRole::Owner.as_str())
        .count();
    assert_eq!(owners, 1);
}

#[tokio::test]
async fn sqlite_channel_foundation_contract() {
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
async fn postgres_channel_foundation_contract() {
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
