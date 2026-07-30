//! Shared workflow-approval and scheduled-fire contract.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use buzz_core::channel::{ChannelType, ChannelVisibility};
use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::channel::ChannelRecord;
use crate::workflow::{
    ApprovalRecord, ApprovalStatus, CreateApprovalParams, ScheduledWorkflowFireClaim,
};
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait WorkflowCoordinationContract: Sync {
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
        channel: Uuid,
        owner: &[u8],
        name: &str,
    ) -> Result<Uuid>;
    async fn create_run(&self, community: CommunityId, workflow: Uuid) -> Result<Uuid>;
    #[allow(clippy::too_many_arguments)]
    async fn create_approval(
        &self,
        community: CommunityId,
        token: &str,
        workflow: Uuid,
        run: Uuid,
        step_id: &str,
        step_index: i32,
        expires_at: DateTime<Utc>,
    ) -> Result<()>;
    async fn get_approval(&self, community: CommunityId, token: &str) -> Result<ApprovalRecord>;
    async fn get_approval_hash(
        &self,
        community: CommunityId,
        token_hash: &[u8],
    ) -> Result<ApprovalRecord>;
    async fn list_approvals(
        &self,
        community: CommunityId,
        workflow: Uuid,
        run: Uuid,
    ) -> Result<Vec<ApprovalRecord>>;
    async fn resolve_approval(
        &self,
        community: CommunityId,
        token_hash: &[u8],
        status: ApprovalStatus,
        approver: &[u8],
    ) -> Result<bool>;
    async fn resolve_approval_raw(
        &self,
        community: CommunityId,
        token: &str,
        status: ApprovalStatus,
        approver: &[u8],
    ) -> Result<bool>;
    async fn claim_fire(
        &self,
        community: CommunityId,
        workflow: Uuid,
        scheduled_for: DateTime<Utc>,
    ) -> Result<Option<ScheduledWorkflowFireClaim>>;
    async fn latest_fire(
        &self,
        community: CommunityId,
        workflow: Uuid,
    ) -> Result<Option<DateTime<Utc>>>;
    async fn attach_run(
        &self,
        community: CommunityId,
        workflow: Uuid,
        scheduled_for: DateTime<Utc>,
        run: Uuid,
    ) -> Result<bool>;
    async fn prune_fires(&self, older_than: DateTime<Utc>) -> Result<u64>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl WorkflowCoordinationContract for $backend {
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
                channel: Uuid,
                owner: &[u8],
                name: &str,
            ) -> Result<Uuid> {
                self.create_workflow(
                    community,
                    Some(channel),
                    owner,
                    name,
                    r#"{"trigger":{"on":"schedule"},"steps":[]}"#,
                    &[0x71; 32],
                )
                .await
            }

            async fn create_run(&self, community: CommunityId, workflow: Uuid) -> Result<Uuid> {
                self.create_workflow_run(community, workflow, None, None)
                    .await
            }

            async fn create_approval(
                &self,
                community: CommunityId,
                token: &str,
                workflow: Uuid,
                run: Uuid,
                step_id: &str,
                step_index: i32,
                expires_at: DateTime<Utc>,
            ) -> Result<()> {
                self.create_approval(CreateApprovalParams {
                    community_id: community,
                    token,
                    workflow_id: workflow,
                    run_id: run,
                    step_id,
                    step_index,
                    approver_spec: "role:owner",
                    expires_at,
                })
                .await
            }

            async fn get_approval(
                &self,
                community: CommunityId,
                token: &str,
            ) -> Result<ApprovalRecord> {
                self.get_approval(community, token).await
            }

            async fn get_approval_hash(
                &self,
                community: CommunityId,
                token_hash: &[u8],
            ) -> Result<ApprovalRecord> {
                self.get_approval_by_stored_hash(community, token_hash)
                    .await
            }

            async fn list_approvals(
                &self,
                community: CommunityId,
                workflow: Uuid,
                run: Uuid,
            ) -> Result<Vec<ApprovalRecord>> {
                self.get_run_approvals(community, workflow, run).await
            }

            async fn resolve_approval(
                &self,
                community: CommunityId,
                token_hash: &[u8],
                status: ApprovalStatus,
                approver: &[u8],
            ) -> Result<bool> {
                self.update_approval_by_stored_hash(
                    community,
                    token_hash,
                    status,
                    Some(approver),
                    Some("contract"),
                )
                .await
            }

            async fn resolve_approval_raw(
                &self,
                community: CommunityId,
                token: &str,
                status: ApprovalStatus,
                approver: &[u8],
            ) -> Result<bool> {
                self.update_approval(
                    community,
                    token,
                    status,
                    Some(approver),
                    Some("raw contract"),
                )
                .await
            }

            async fn claim_fire(
                &self,
                community: CommunityId,
                workflow: Uuid,
                scheduled_for: DateTime<Utc>,
            ) -> Result<Option<ScheduledWorkflowFireClaim>> {
                self.claim_scheduled_workflow_fire(community, workflow, scheduled_for)
                    .await
            }

            async fn latest_fire(
                &self,
                community: CommunityId,
                workflow: Uuid,
            ) -> Result<Option<DateTime<Utc>>> {
                self.latest_scheduled_workflow_fire(community, workflow)
                    .await
            }

            async fn attach_run(
                &self,
                community: CommunityId,
                workflow: Uuid,
                scheduled_for: DateTime<Utc>,
                run: Uuid,
            ) -> Result<bool> {
                self.attach_scheduled_workflow_run(community, workflow, scheduled_for, run)
                    .await
            }

            async fn prune_fires(&self, older_than: DateTime<Utc>) -> Result<u64> {
                self.prune_scheduled_workflow_fires_before(older_than).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

async fn run_contract(store: &impl WorkflowCoordinationContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("coord-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("coord-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let owner = [0x72; 32];
    let approver = [0x73; 32];
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &owner)
            .await
            .expect("owner user");
        store
            .ensure_user(community, &approver)
            .await
            .expect("approver user");
    }
    let channel = Uuid::new_v4();
    for (community, name) in [(community_a, "coord-a"), (community_b, "coord-b")] {
        store
            .create_channel(community, channel, name, &owner)
            .await
            .expect("channel");
    }
    let workflow_a = store
        .create_workflow(community_a, channel, &owner, "schedule A")
        .await
        .expect("workflow A");
    let workflow_b = store
        .create_workflow(community_b, channel, &owner, "schedule B")
        .await
        .expect("workflow B");
    let run_a = store
        .create_run(community_a, workflow_a)
        .await
        .expect("run A");
    let run_b = store
        .create_run(community_b, workflow_b)
        .await
        .expect("run B");

    let token = format!("approval-{suffix}");
    let expires = Utc::now() + Duration::hours(1);
    store
        .create_approval(community_a, &token, workflow_a, run_a, "deploy", 2, expires)
        .await
        .expect("approval A");
    store
        .create_approval(community_b, &token, workflow_b, run_b, "deploy", 2, expires)
        .await
        .expect("same raw token in B");
    assert!(store
        .create_approval(
            community_a,
            "cross-run",
            workflow_a,
            run_b,
            "bad",
            0,
            expires
        )
        .await
        .is_err());
    let approval_a = store
        .get_approval(community_a, &token)
        .await
        .expect("approval A");
    assert_eq!(approval_a.workflow_id, workflow_a);
    assert_eq!(
        store
            .get_approval_hash(community_a, &approval_a.token)
            .await
            .expect("approval by hash")
            .run_id,
        run_a
    );
    assert_eq!(
        store
            .list_approvals(community_a, workflow_a, run_a)
            .await
            .expect("approval list")
            .len(),
        1
    );
    assert!(store
        .list_approvals(community_b, workflow_a, run_a)
        .await
        .expect("foreign approval list")
        .is_empty());

    let grant = store.resolve_approval(
        community_a,
        &approval_a.token,
        ApprovalStatus::Granted,
        &approver,
    );
    let deny = store.resolve_approval(
        community_a,
        &approval_a.token,
        ApprovalStatus::Denied,
        &approver,
    );
    let (grant, deny) = tokio::join!(grant, deny);
    assert_ne!(
        grant.expect("grant result"),
        deny.expect("deny result"),
        "exactly one concurrent approval action must win"
    );
    assert_eq!(
        store
            .get_approval(community_b, &token)
            .await
            .expect("approval B remains pending")
            .status,
        ApprovalStatus::Pending
    );
    assert!(store
        .resolve_approval_raw(community_b, &token, ApprovalStatus::Expired, &approver,)
        .await
        .expect("raw-token resolution"));
    assert!(!store
        .resolve_approval_raw(community_b, &token, ApprovalStatus::Granted, &approver,)
        .await
        .expect("resolved approval cannot change again"));

    let scheduled_for =
        DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).expect("schedule instant");
    let first = store.claim_fire(community_a, workflow_a, scheduled_for);
    let second = store.claim_fire(community_a, workflow_a, scheduled_for);
    let (first, second) = tokio::join!(first, second);
    assert_ne!(
        first.expect("first claim").is_some(),
        second.expect("second claim").is_some(),
        "exactly one concurrent fire claim must win"
    );
    assert!(store
        .claim_fire(community_b, workflow_b, scheduled_for)
        .await
        .expect("same instant in B")
        .is_some());
    assert!(store
        .claim_fire(community_b, workflow_a, scheduled_for)
        .await
        .expect("foreign workflow claim")
        .is_none());
    assert_eq!(
        store
            .latest_fire(community_a, workflow_a)
            .await
            .expect("latest A"),
        Some(scheduled_for)
    );
    assert!(store
        .attach_run(community_a, workflow_a, scheduled_for, run_b)
        .await
        .is_err());
    assert!(store
        .attach_run(community_a, workflow_a, scheduled_for, run_a)
        .await
        .expect("attach run"));
    assert!(!store
        .attach_run(community_a, workflow_a, scheduled_for, run_a)
        .await
        .expect("repeat attach"));
    assert_eq!(
        store
            .prune_fires(DateTime::UNIX_EPOCH)
            .await
            .expect("safe old prune"),
        0
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
async fn sqlite_workflow_coordination_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_workflow_coordination_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
