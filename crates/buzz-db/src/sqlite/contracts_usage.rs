//! Shared usage-rollup contract.

use async_trait::async_trait;
use nostr::{Event, EventBuilder, Keys, Kind, Timestamp};
use uuid::Uuid;

use buzz_core::channel::{ChannelType, ChannelVisibility};
use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::usage::{
    CommunityActiveChannels, CommunityActiveUsers, CommunityChannelCount, CommunityGitRepoCount,
    CommunityHost, CommunityMemberCount, CommunityMessageCount, CommunityUserCounts,
    CommunityWorkflowCount,
};
use crate::workflow::WorkflowStatus;
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait UsageContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn mark_agent(&self, community: CommunityId, agent: &[u8], owner: &[u8]) -> Result<()>;
    async fn create_channel(
        &self,
        community: CommunityId,
        name: &str,
        channel_type: ChannelType,
        visibility: ChannelVisibility,
        owner: &[u8],
    ) -> Result<Uuid>;
    async fn delete_channel(&self, community: CommunityId, channel: Uuid) -> Result<bool>;
    async fn insert_event(
        &self,
        community: CommunityId,
        event: &Event,
        channel: Option<Uuid>,
    ) -> Result<bool>;
    async fn delete_event(&self, community: CommunityId, event: &[u8]) -> Result<bool>;
    async fn add_member(&self, community: CommunityId, pubkey: &str, role: &str) -> Result<bool>;
    async fn create_workflow(
        &self,
        community: CommunityId,
        owner: &[u8],
        name: &str,
    ) -> Result<Uuid>;
    async fn set_workflow_status(
        &self,
        community: CommunityId,
        workflow: Uuid,
        status: WorkflowStatus,
    ) -> Result<()>;
    async fn reserve_repo(&self, community: CommunityId, repo: &str, owner: &str) -> Result<()>;
    async fn community_count(&self) -> Result<i64>;
    async fn users(&self) -> Result<Vec<CommunityUserCounts>>;
    async fn channels(&self) -> Result<Vec<CommunityChannelCount>>;
    async fn messages(&self) -> Result<Vec<CommunityMessageCount>>;
    async fn members(&self) -> Result<Vec<CommunityMemberCount>>;
    async fn workflows(&self) -> Result<Vec<CommunityWorkflowCount>>;
    async fn repos(&self) -> Result<Vec<CommunityGitRepoCount>>;
    async fn active_users(&self) -> Result<Vec<CommunityActiveUsers>>;
    async fn active_channels(&self) -> Result<Vec<CommunityActiveChannels>>;
    async fn hosts(&self) -> Result<Vec<CommunityHost>>;
}

#[async_trait]
trait UsageAgentSeeder: Sync {
    async fn seed_agent(&self, community: CommunityId, agent: &[u8], owner: &[u8]) -> Result<()>;
}

#[async_trait]
impl UsageAgentSeeder for SqliteStore {
    async fn seed_agent(&self, community: CommunityId, agent: &[u8], owner: &[u8]) -> Result<()> {
        let _writer = self.acquire_writer().await;
        sqlx::query(
            "UPDATE users SET agent_owner_pubkey = ?, updated_at = ? \
             WHERE community_id = ? AND pubkey = ?",
        )
        .bind(owner)
        .bind(chrono::Utc::now().timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(agent)
        .execute(self.pool())
        .await?;
        Ok(())
    }
}

#[async_trait]
impl UsageAgentSeeder for Db {
    async fn seed_agent(&self, community: CommunityId, agent: &[u8], owner: &[u8]) -> Result<()> {
        let _ = self.set_agent_owner(community, agent, owner).await?;
        Ok(())
    }
}

macro_rules! impl_usage_contract {
    ($backend:ty) => {
        #[async_trait]
        impl UsageContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
                self.ensure_user(community, pubkey).await
            }

            async fn mark_agent(
                &self,
                community: CommunityId,
                agent: &[u8],
                owner: &[u8],
            ) -> Result<()> {
                UsageAgentSeeder::seed_agent(self, community, agent, owner).await
            }

            async fn create_channel(
                &self,
                community: CommunityId,
                name: &str,
                channel_type: ChannelType,
                visibility: ChannelVisibility,
                owner: &[u8],
            ) -> Result<Uuid> {
                Ok(self
                    .create_channel(community, name, channel_type, visibility, None, owner, None)
                    .await?
                    .id)
            }

            async fn delete_channel(&self, community: CommunityId, channel: Uuid) -> Result<bool> {
                self.soft_delete_channel(community, channel).await
            }

            async fn insert_event(
                &self,
                community: CommunityId,
                event: &Event,
                channel: Option<Uuid>,
            ) -> Result<bool> {
                Ok(self.insert_event(community, event, channel).await?.1)
            }

            async fn delete_event(&self, community: CommunityId, event: &[u8]) -> Result<bool> {
                self.soft_delete_event(community, event).await
            }

            async fn add_member(
                &self,
                community: CommunityId,
                pubkey: &str,
                role: &str,
            ) -> Result<bool> {
                self.add_relay_member(community, pubkey, role, None).await
            }

            async fn create_workflow(
                &self,
                community: CommunityId,
                owner: &[u8],
                name: &str,
            ) -> Result<Uuid> {
                self.create_workflow(community, None, owner, name, "{}", &[0x51; 32])
                    .await
            }

            async fn set_workflow_status(
                &self,
                community: CommunityId,
                workflow: Uuid,
                status: WorkflowStatus,
            ) -> Result<()> {
                self.update_workflow_status(community, workflow, status)
                    .await
            }

            async fn reserve_repo(
                &self,
                community: CommunityId,
                repo: &str,
                owner: &str,
            ) -> Result<()> {
                let _ = self.reserve_repo_name(community, repo, owner).await?;
                Ok(())
            }

            async fn community_count(&self) -> Result<i64> {
                self.usage_community_count().await
            }

            async fn users(&self) -> Result<Vec<CommunityUserCounts>> {
                self.usage_user_counts().await
            }

            async fn channels(&self) -> Result<Vec<CommunityChannelCount>> {
                self.usage_channel_counts().await
            }

            async fn messages(&self) -> Result<Vec<CommunityMessageCount>> {
                self.usage_message_counts().await
            }

            async fn members(&self) -> Result<Vec<CommunityMemberCount>> {
                self.usage_relay_member_counts().await
            }

            async fn workflows(&self) -> Result<Vec<CommunityWorkflowCount>> {
                self.usage_workflow_counts().await
            }

            async fn repos(&self) -> Result<Vec<CommunityGitRepoCount>> {
                self.usage_git_repo_counts().await
            }

            async fn active_users(&self) -> Result<Vec<CommunityActiveUsers>> {
                self.usage_active_user_counts("1 day").await
            }

            async fn active_channels(&self) -> Result<Vec<CommunityActiveChannels>> {
                self.usage_active_channel_counts("1 day").await
            }

            async fn hosts(&self) -> Result<Vec<CommunityHost>> {
                self.usage_community_hosts().await
            }
        }
    };
}

impl_usage_contract!(SqliteStore);
impl_usage_contract!(Db);

fn event(keys: &Keys, kind: u16, created_at: i64, body: &str) -> Event {
    EventBuilder::new(Kind::Custom(kind), body)
        .custom_created_at(Timestamp::from(
            u64::try_from(created_at).expect("positive event timestamp"),
        ))
        .sign_with_keys(keys)
        .expect("signed usage event")
}

fn count_for<T>(rows: &[T], community: CommunityId, select: impl Fn(&T) -> (Uuid, i64)) -> i64 {
    rows.iter()
        .find_map(|row| {
            let (id, count) = select(row);
            (id == *community.as_uuid()).then_some(count)
        })
        .unwrap_or_default()
}

async fn run_contract(store: &impl UsageContract) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let host_a = format!("usage-a-{suffix}.example.test");
    let host_b = format!("usage-b-{suffix}.example.test");
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
    let human = Keys::generate();
    let agent = Keys::generate();
    let unknown = Keys::generate();
    let human_pubkey = human.public_key().to_bytes();
    let agent_pubkey = agent.public_key().to_bytes();
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &human_pubkey)
            .await
            .expect("human");
    }
    store
        .ensure_user(community_a, &agent_pubkey)
        .await
        .expect("agent");
    store
        .mark_agent(community_a, &agent_pubkey, &human_pubkey)
        .await
        .expect("mark agent");

    let stream = store
        .create_channel(
            community_a,
            &format!("stream-{suffix}"),
            ChannelType::Stream,
            ChannelVisibility::Open,
            &human_pubkey,
        )
        .await
        .expect("stream");
    let deleted_dm = store
        .create_channel(
            community_a,
            &format!("dm-{suffix}"),
            ChannelType::Dm,
            ChannelVisibility::Private,
            &human_pubkey,
        )
        .await
        .expect("deleted DM");
    assert!(store
        .delete_channel(community_a, deleted_dm)
        .await
        .expect("delete DM"));
    let forum = store
        .create_channel(
            community_b,
            &format!("forum-{suffix}"),
            ChannelType::Forum,
            ChannelVisibility::Open,
            &human_pubkey,
        )
        .await
        .expect("forum");

    let now = chrono::Utc::now().timestamp();
    for (keys, body) in [(&human, "human"), (&agent, "agent"), (&unknown, "unknown")] {
        let event = event(keys, 9, now, body);
        assert!(store
            .insert_event(community_a, &event, Some(stream))
            .await
            .expect("insert A message"));
    }
    let old = event(&human, 9, now - 2 * 24 * 60 * 60, "old");
    assert!(store
        .insert_event(community_a, &old, Some(stream))
        .await
        .expect("insert old message"));
    let deleted = event(&human, 9, now, "deleted");
    assert!(store
        .insert_event(community_a, &deleted, Some(stream))
        .await
        .expect("insert deleted message"));
    assert!(store
        .delete_event(community_a, deleted.id.as_bytes())
        .await
        .expect("delete message"));
    let event_b = event(&human, 9, now, "community B");
    assert!(store
        .insert_event(community_b, &event_b, Some(forum))
        .await
        .expect("insert B message"));

    let human_hex = hex::encode(human_pubkey);
    let agent_hex = hex::encode(agent_pubkey);
    assert!(store
        .add_member(community_a, &human_hex, "owner")
        .await
        .expect("owner A"));
    assert!(store
        .add_member(community_a, &agent_hex, "member")
        .await
        .expect("member A"));
    assert!(store
        .add_member(community_b, &human_hex, "owner")
        .await
        .expect("owner B"));

    let _active = store
        .create_workflow(community_a, &human_pubkey, "active")
        .await
        .expect("active workflow");
    let disabled = store
        .create_workflow(community_a, &human_pubkey, "disabled")
        .await
        .expect("disabled workflow");
    store
        .set_workflow_status(community_a, disabled, WorkflowStatus::Disabled)
        .await
        .expect("disable workflow");
    store
        .create_workflow(community_b, &human_pubkey, "active B")
        .await
        .expect("workflow B");

    store
        .reserve_repo(community_a, &format!("repo-a1-{suffix}"), &human_hex)
        .await
        .expect("repo A1");
    store
        .reserve_repo(community_a, &format!("repo-a2-{suffix}"), &human_hex)
        .await
        .expect("repo A2");
    store
        .reserve_repo(community_b, &format!("repo-b-{suffix}"), &human_hex)
        .await
        .expect("repo B");

    assert_eq!(store.community_count().await.expect("community count"), 2);
    let hosts = store.hosts().await.expect("hosts");
    assert!(hosts
        .iter()
        .any(|row| row.id == *community_a.as_uuid() && row.host == host_a));
    assert!(hosts
        .iter()
        .any(|row| row.id == *community_b.as_uuid() && row.host == host_b));

    let users = store.users().await.expect("user counts");
    let users_a = users
        .iter()
        .find(|row| row.community_id == *community_a.as_uuid())
        .expect("users A");
    assert_eq!((users_a.human, users_a.agent), (1, 1));
    let users_b = users
        .iter()
        .find(|row| row.community_id == *community_b.as_uuid())
        .expect("users B");
    assert_eq!((users_b.human, users_b.agent), (1, 0));

    let channels = store.channels().await.expect("channel counts");
    assert!(channels.iter().any(|row| {
        row.community_id == *community_a.as_uuid() && row.channel_type == "stream" && row.count == 1
    }));
    assert!(!channels
        .iter()
        .any(|row| { row.community_id == *community_a.as_uuid() && row.channel_type == "dm" }));
    assert!(channels.iter().any(|row| {
        row.community_id == *community_b.as_uuid() && row.channel_type == "forum" && row.count == 1
    }));

    let messages = store.messages().await.expect("message counts");
    assert_eq!(
        count_for(&messages, community_a, |row| (row.community_id, row.count)),
        4
    );
    assert_eq!(
        count_for(&messages, community_b, |row| (row.community_id, row.count)),
        1
    );

    let members = store.members().await.expect("member counts");
    assert!(members.iter().any(|row| {
        row.community_id == *community_a.as_uuid() && row.role == "owner" && row.count == 1
    }));
    assert!(members.iter().any(|row| {
        row.community_id == *community_a.as_uuid() && row.role == "member" && row.count == 1
    }));

    let workflows = store.workflows().await.expect("workflow counts");
    assert!(workflows.iter().any(|row| {
        row.community_id == *community_a.as_uuid() && row.status == "active" && row.count == 1
    }));
    assert!(workflows.iter().any(|row| {
        row.community_id == *community_a.as_uuid() && row.status == "disabled" && row.count == 1
    }));

    let repos = store.repos().await.expect("repo counts");
    assert_eq!(
        count_for(&repos, community_a, |row| (row.community_id, row.count)),
        2
    );
    assert_eq!(
        count_for(&repos, community_b, |row| (row.community_id, row.count)),
        1
    );

    let active_users = store.active_users().await.expect("active users");
    let active_a = active_users
        .iter()
        .find(|row| row.community_id == *community_a.as_uuid())
        .expect("active users A");
    assert_eq!(
        (active_a.human, active_a.agent, active_a.unknown),
        (1, 1, 1)
    );
    let active_channels = store.active_channels().await.expect("active channels");
    assert_eq!(
        count_for(&active_channels, community_a, |row| (
            row.community_id,
            row.count
        )),
        1
    );
    assert_eq!(
        count_for(&active_channels, community_b, |row| (
            row.community_id,
            row.count
        )),
        1
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
async fn sqlite_usage_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_usage_contract() {
    let admin = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/postgres")
        .await
        .expect("PostgreSQL admin connection");
    let database = format!("buzz_usage_contract_{}", uuid::Uuid::new_v4().simple());
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
