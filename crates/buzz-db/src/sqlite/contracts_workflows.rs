//! Shared workflow-definition and execution-run contract.

use async_trait::async_trait;
use uuid::Uuid;

use buzz_core::channel::{ChannelType, ChannelVisibility};
use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::channel::ChannelRecord;
use crate::workflow::{RunStatus, WorkflowRecord, WorkflowRunRecord, WorkflowStatus};
use crate::{Db, DbError, EnsuredCommunityRecord, Result};

#[async_trait]
trait WorkflowContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn create_channel(
        &self,
        community: CommunityId,
        id: Uuid,
        name: &str,
        owner: &[u8],
    ) -> Result<(ChannelRecord, bool)>;
    async fn create_workflow(
        &self,
        community: CommunityId,
        channel: Option<Uuid>,
        owner: &[u8],
        name: &str,
        definition: &str,
        hash: &[u8],
    ) -> Result<Uuid>;
    #[allow(clippy::too_many_arguments)]
    async fn upsert_workflow(
        &self,
        community: CommunityId,
        id: Uuid,
        channel: Option<Uuid>,
        owner: &[u8],
        name: &str,
        definition: &str,
        hash: &[u8],
    ) -> Result<()>;
    async fn get_workflow(&self, community: CommunityId, id: Uuid) -> Result<WorkflowRecord>;
    async fn list_channel(
        &self,
        community: CommunityId,
        channel: Uuid,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<WorkflowRecord>>;
    async fn list_enabled_channel(
        &self,
        community: CommunityId,
        channel: Uuid,
    ) -> Result<Vec<WorkflowRecord>>;
    async fn list_schedules(&self) -> Result<Vec<WorkflowRecord>>;
    async fn update_workflow(
        &self,
        community: CommunityId,
        id: Uuid,
        name: &str,
        definition: &str,
        hash: &[u8],
    ) -> Result<()>;
    async fn set_status(
        &self,
        community: CommunityId,
        id: Uuid,
        status: WorkflowStatus,
    ) -> Result<()>;
    async fn set_enabled(&self, community: CommunityId, id: Uuid, enabled: bool) -> Result<()>;
    async fn disable_owner(
        &self,
        community: CommunityId,
        channel: Uuid,
        owner: &[u8],
    ) -> Result<u64>;
    async fn delete_workflow(&self, community: CommunityId, id: Uuid) -> Result<()>;
    async fn delete_for_owner(
        &self,
        community: CommunityId,
        id: Uuid,
        owner: &[u8],
    ) -> Result<Option<Uuid>>;
    async fn find_by_owner(
        &self,
        community: CommunityId,
        owner: &[u8],
        name: &str,
    ) -> Result<Option<WorkflowRecord>>;
    async fn create_run(
        &self,
        community: CommunityId,
        workflow: Uuid,
        event_id: Option<&[u8]>,
        context: Option<&serde_json::Value>,
    ) -> Result<Uuid>;
    async fn get_run(&self, community: CommunityId, id: Uuid) -> Result<WorkflowRunRecord>;
    async fn list_runs(
        &self,
        community: CommunityId,
        workflow: Uuid,
        limit: i64,
    ) -> Result<Vec<WorkflowRunRecord>>;
    async fn update_run(
        &self,
        community: CommunityId,
        id: Uuid,
        status: RunStatus,
        step: i32,
        trace: &serde_json::Value,
        error: Option<&str>,
    ) -> Result<()>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl WorkflowContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
                self.ensure_user(community, pubkey).await
            }

            async fn create_channel(
                &self,
                community: CommunityId,
                id: Uuid,
                name: &str,
                owner: &[u8],
            ) -> Result<(ChannelRecord, bool)> {
                self.create_channel_with_id(
                    community,
                    id,
                    name,
                    ChannelType::Stream,
                    ChannelVisibility::Open,
                    None,
                    owner,
                    None,
                )
                .await
            }

            async fn create_workflow(
                &self,
                community: CommunityId,
                channel: Option<Uuid>,
                owner: &[u8],
                name: &str,
                definition: &str,
                hash: &[u8],
            ) -> Result<Uuid> {
                self.create_workflow(community, channel, owner, name, definition, hash)
                    .await
            }

            async fn upsert_workflow(
                &self,
                community: CommunityId,
                id: Uuid,
                channel: Option<Uuid>,
                owner: &[u8],
                name: &str,
                definition: &str,
                hash: &[u8],
            ) -> Result<()> {
                self.upsert_workflow(community, id, channel, owner, name, definition, hash)
                    .await
            }

            async fn get_workflow(
                &self,
                community: CommunityId,
                id: Uuid,
            ) -> Result<WorkflowRecord> {
                self.get_workflow(community, id).await
            }

            async fn list_channel(
                &self,
                community: CommunityId,
                channel: Uuid,
                limit: Option<i64>,
                offset: Option<i64>,
            ) -> Result<Vec<WorkflowRecord>> {
                self.list_channel_workflows(community, channel, limit, offset)
                    .await
            }

            async fn list_enabled_channel(
                &self,
                community: CommunityId,
                channel: Uuid,
            ) -> Result<Vec<WorkflowRecord>> {
                self.list_enabled_channel_workflows(community, channel)
                    .await
            }

            async fn list_schedules(&self) -> Result<Vec<WorkflowRecord>> {
                self.list_all_enabled_workflows().await
            }

            async fn update_workflow(
                &self,
                community: CommunityId,
                id: Uuid,
                name: &str,
                definition: &str,
                hash: &[u8],
            ) -> Result<()> {
                self.update_workflow(community, id, name, definition, hash)
                    .await
            }

            async fn set_status(
                &self,
                community: CommunityId,
                id: Uuid,
                status: WorkflowStatus,
            ) -> Result<()> {
                self.update_workflow_status(community, id, status).await
            }

            async fn set_enabled(
                &self,
                community: CommunityId,
                id: Uuid,
                enabled: bool,
            ) -> Result<()> {
                self.set_workflow_enabled(community, id, enabled).await
            }

            async fn disable_owner(
                &self,
                community: CommunityId,
                channel: Uuid,
                owner: &[u8],
            ) -> Result<u64> {
                self.disable_workflows_for_owner_in_channel(community, channel, owner)
                    .await
            }

            async fn delete_workflow(&self, community: CommunityId, id: Uuid) -> Result<()> {
                self.delete_workflow(community, id).await
            }

            async fn delete_for_owner(
                &self,
                community: CommunityId,
                id: Uuid,
                owner: &[u8],
            ) -> Result<Option<Uuid>> {
                self.delete_workflow_for_owner(community, id, owner).await
            }

            async fn find_by_owner(
                &self,
                community: CommunityId,
                owner: &[u8],
                name: &str,
            ) -> Result<Option<WorkflowRecord>> {
                self.find_workflow_by_owner_and_name(community, owner, name)
                    .await
            }

            async fn create_run(
                &self,
                community: CommunityId,
                workflow: Uuid,
                event_id: Option<&[u8]>,
                context: Option<&serde_json::Value>,
            ) -> Result<Uuid> {
                self.create_workflow_run(community, workflow, event_id, context)
                    .await
            }

            async fn get_run(&self, community: CommunityId, id: Uuid) -> Result<WorkflowRunRecord> {
                self.get_workflow_run(community, id).await
            }

            async fn list_runs(
                &self,
                community: CommunityId,
                workflow: Uuid,
                limit: i64,
            ) -> Result<Vec<WorkflowRunRecord>> {
                self.list_workflow_runs(community, workflow, limit).await
            }

            async fn update_run(
                &self,
                community: CommunityId,
                id: Uuid,
                status: RunStatus,
                step: i32,
                trace: &serde_json::Value,
                error: Option<&str>,
            ) -> Result<()> {
                self.update_workflow_run(community, id, status, step, trace, error)
                    .await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl WorkflowContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("workflow-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("workflow-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let owner = [0x61; 32];
    let other_owner = [0x62; 32];
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &owner)
            .await
            .expect("owner user");
        store
            .ensure_user(community, &other_owner)
            .await
            .expect("other owner user");
    }
    let channel = Uuid::new_v4();
    for (community, name) in [(community_a, "workflow-a"), (community_b, "workflow-b")] {
        assert!(
            store
                .create_channel(community, channel, name, &owner)
                .await
                .expect("channel")
                .1
        );
    }

    let schedule = r#"{"trigger":{"on":"schedule","cron":"0 * * * *"},"steps":[]}"#;
    let event = r#"{"trigger":{"on":"event"},"steps":[]}"#;
    let schedule_hash = [0x63; 32];
    let event_hash = [0x64; 32];
    let shared_id = Uuid::new_v4();
    store
        .upsert_workflow(
            community_a,
            shared_id,
            Some(channel),
            &owner,
            "schedule A",
            schedule,
            &schedule_hash,
        )
        .await
        .expect("workflow A");
    store
        .upsert_workflow(
            community_b,
            shared_id,
            Some(channel),
            &owner,
            "schedule B",
            schedule,
            &schedule_hash,
        )
        .await
        .expect("workflow B");
    assert_eq!(
        store
            .get_workflow(community_a, shared_id)
            .await
            .expect("read A")
            .name,
        "schedule A"
    );
    assert!(store
        .upsert_workflow(
            community_a,
            Uuid::new_v4(),
            Some(channel),
            &owner,
            "invalid JSON",
            "not-json",
            &event_hash,
        )
        .await
        .is_err());
    assert_eq!(
        store
            .get_workflow(community_b, shared_id)
            .await
            .expect("read B")
            .name,
        "schedule B"
    );
    assert!(matches!(
        store
            .upsert_workflow(
                community_a,
                shared_id,
                Some(channel),
                &other_owner,
                "stolen",
                event,
                &event_hash,
            )
            .await,
        Err(DbError::AccessDenied(_))
    ));
    assert_eq!(
        store
            .get_workflow(community_a, shared_id)
            .await
            .expect("guarded workflow")
            .name,
        "schedule A"
    );

    let event_id = store
        .create_workflow(
            community_a,
            Some(channel),
            &owner,
            "event A",
            event,
            &event_hash,
        )
        .await
        .expect("create event workflow");
    store
        .update_workflow(community_a, event_id, "event A updated", event, &event_hash)
        .await
        .expect("update workflow");
    assert_eq!(
        store
            .find_by_owner(community_a, &owner, "event A updated")
            .await
            .expect("find workflow")
            .expect("found workflow")
            .id,
        event_id
    );
    assert_eq!(
        store
            .list_channel(community_a, channel, Some(1), Some(0))
            .await
            .expect("bounded channel list")
            .len(),
        1
    );

    let schedule_ids = store
        .list_schedules()
        .await
        .expect("global schedule scan")
        .into_iter()
        .filter(|workflow| workflow.id == shared_id)
        .map(|workflow| workflow.community_id)
        .collect::<Vec<_>>();
    assert!(schedule_ids.contains(&community_a));
    assert!(schedule_ids.contains(&community_b));
    assert!(!store
        .list_schedules()
        .await
        .expect("event workflow excluded")
        .iter()
        .any(|workflow| workflow.id == event_id));

    store
        .set_status(community_b, shared_id, WorkflowStatus::Disabled)
        .await
        .expect("disable status B");
    assert!(store
        .list_enabled_channel(community_b, channel)
        .await
        .expect("enabled B")
        .is_empty());
    assert!(store
        .list_enabled_channel(community_a, channel)
        .await
        .expect("enabled A")
        .iter()
        .any(|workflow| workflow.id == shared_id));
    store
        .set_enabled(community_a, event_id, false)
        .await
        .expect("disable event workflow");
    assert_eq!(
        store
            .disable_owner(community_a, channel, &owner)
            .await
            .expect("disable owner workflows"),
        1
    );
    assert!(
        store
            .get_workflow(community_b, shared_id)
            .await
            .expect("B unaffected")
            .enabled
    );

    let race_id = Uuid::new_v4();
    let race_a = store.upsert_workflow(
        community_a,
        race_id,
        Some(channel),
        &owner,
        "race A",
        schedule,
        &schedule_hash,
    );
    let race_b = store.upsert_workflow(
        community_a,
        race_id,
        Some(channel),
        &owner,
        "race B",
        event,
        &event_hash,
    );
    let (race_a, race_b) = tokio::join!(race_a, race_b);
    race_a.expect("concurrent upsert A");
    race_b.expect("concurrent upsert B");
    let raced = store
        .get_workflow(community_a, race_id)
        .await
        .expect("raced workflow");
    assert!(
        (raced.name == "race A" && raced.definition_hash == schedule_hash)
            || (raced.name == "race B" && raced.definition_hash == event_hash),
        "concurrent upsert must leave one coherent definition"
    );
    assert!(
        store
            .create_run(community_b, race_id, None, None)
            .await
            .is_err(),
        "a run cannot reference another community's workflow"
    );

    let trigger_event = [0x65; 32];
    let context = serde_json::json!({"channelId": channel, "source": "contract"});
    let run = store
        .create_run(community_a, race_id, Some(&trigger_event), Some(&context))
        .await
        .expect("create run");
    assert!(matches!(
        store.get_run(community_b, run).await,
        Err(DbError::NotFound(_))
    ));
    let pending = store.get_run(community_a, run).await.expect("pending run");
    assert_eq!(pending.status, RunStatus::Pending);
    assert_eq!(pending.trigger_context.as_ref(), Some(&context));
    store
        .update_run(
            community_a,
            run,
            RunStatus::Running,
            1,
            &serde_json::json!([{"step": 0, "ok": true}]),
            None,
        )
        .await
        .expect("start run");
    let running = store.get_run(community_a, run).await.expect("running run");
    assert!(running.started_at.is_some());
    assert!(running.completed_at.is_none());
    store
        .update_run(
            community_a,
            run,
            RunStatus::Completed,
            2,
            &serde_json::json!([{"step": 0}, {"step": 1}]),
            None,
        )
        .await
        .expect("complete run");
    let completed = store
        .get_run(community_a, run)
        .await
        .expect("completed run");
    assert_eq!(completed.started_at, running.started_at);
    assert!(completed.completed_at.is_some());
    assert_eq!(completed.current_step, 2);
    assert!(store
        .list_runs(community_b, race_id, 100)
        .await
        .expect("foreign runs")
        .is_empty());
    assert_eq!(
        store
            .list_runs(community_a, race_id, 100)
            .await
            .expect("workflow runs")
            .len(),
        1
    );

    assert!(matches!(
        store
            .delete_for_owner(community_a, race_id, &other_owner)
            .await,
        Err(DbError::NotFound(_))
    ));
    assert_eq!(
        store
            .delete_for_owner(community_a, race_id, &owner)
            .await
            .expect("owner delete"),
        Some(channel)
    );
    assert!(matches!(
        store.get_run(community_a, run).await,
        Err(DbError::NotFound(_))
    ));
    store
        .delete_workflow(community_a, event_id)
        .await
        .expect("plain delete");
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
async fn sqlite_workflow_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_workflow_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
