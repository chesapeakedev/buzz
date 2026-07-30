//! Shared product-feedback and deployment-admin contract.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use nostr::{Event, EventBuilder, Keys, Kind};
use uuid::Uuid;

use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::admin_moderation::{AdminFeedback, AdminReport, AdminReportDetail};
use crate::moderation::{NewReport, ReportTarget};
use crate::product_feedback::{NewProductFeedback, ProductFeedbackRecord};
use crate::{Db, EnsuredCommunityRecord, Result, StoredEvent};

#[async_trait]
trait AdminContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn insert_event(
        &self,
        community: CommunityId,
        event: &Event,
    ) -> Result<(StoredEvent, bool)>;
    async fn delete_event(&self, community: CommunityId, id: &[u8]) -> Result<bool>;
    async fn insert_report(&self, community: CommunityId, report: NewReport<'_>) -> Result<Uuid>;
    async fn resolve_report(
        &self,
        community: CommunityId,
        id: Uuid,
        status: &str,
        actor: &[u8],
    ) -> Result<bool>;
    #[allow(clippy::too_many_arguments)]
    async fn admin_reports(
        &self,
        community: Option<Uuid>,
        status: Option<&str>,
        report_type: Option<&str>,
        target_kind: Option<&str>,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<AdminReport>>;
    async fn admin_report(&self, id: Uuid) -> Result<Option<AdminReportDetail>>;
    async fn insert_feedback(
        &self,
        community: CommunityId,
        feedback: NewProductFeedback<'_>,
    ) -> Result<Uuid>;
    async fn feedback(&self, limit: i64) -> Result<Vec<ProductFeedbackRecord>>;
    async fn admin_feedback(&self, limit: i64) -> Result<Vec<AdminFeedback>>;
    async fn admin_feedback_by_id(&self, id: Uuid) -> Result<Option<AdminFeedback>>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl AdminContract for $backend {
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

            async fn delete_event(&self, community: CommunityId, id: &[u8]) -> Result<bool> {
                self.soft_delete_event(community, id).await
            }

            async fn insert_report(
                &self,
                community: CommunityId,
                report: NewReport<'_>,
            ) -> Result<Uuid> {
                self.insert_moderation_report(community, report).await
            }

            async fn resolve_report(
                &self,
                community: CommunityId,
                id: Uuid,
                status: &str,
                actor: &[u8],
            ) -> Result<bool> {
                self.resolve_moderation_report(community, id, status, actor, None)
                    .await
            }

            async fn admin_reports(
                &self,
                community: Option<Uuid>,
                status: Option<&str>,
                report_type: Option<&str>,
                target_kind: Option<&str>,
                after: Option<DateTime<Utc>>,
                before: Option<DateTime<Utc>>,
                cursor: Option<(DateTime<Utc>, Uuid)>,
                limit: i64,
            ) -> Result<Vec<AdminReport>> {
                self.admin_list_reports(
                    community,
                    status,
                    report_type,
                    target_kind,
                    after,
                    before,
                    cursor,
                    limit,
                )
                .await
            }

            async fn admin_report(&self, id: Uuid) -> Result<Option<AdminReportDetail>> {
                self.admin_get_report(id).await
            }

            async fn insert_feedback(
                &self,
                community: CommunityId,
                feedback: NewProductFeedback<'_>,
            ) -> Result<Uuid> {
                self.insert_product_feedback(community, feedback).await
            }

            async fn feedback(&self, limit: i64) -> Result<Vec<ProductFeedbackRecord>> {
                self.list_product_feedback(limit).await
            }

            async fn admin_feedback(&self, limit: i64) -> Result<Vec<AdminFeedback>> {
                self.admin_list_feedback(limit).await
            }

            async fn admin_feedback_by_id(&self, id: Uuid) -> Result<Option<AdminFeedback>> {
                self.admin_get_feedback(id).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

fn new_report<'a>(
    event_id: &'a [u8],
    reporter: &'a [u8],
    target: ReportTarget,
    report_type: &'a str,
    note: &'a str,
) -> NewReport<'a> {
    NewReport {
        report_event_id: event_id,
        reporter_pubkey: reporter,
        target,
        channel_id: None,
        report_type,
        note: Some(note),
    }
}

async fn run_contract(store: &impl AdminContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let host_a = format!("admin-a-{suffix}.example.test");
    let host_b = format!("admin-b-{suffix}.example.test");
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

    let reported = EventBuilder::new(Kind::TextNote, "same-tenant reported message")
        .sign_with_keys(&Keys::generate())
        .expect("reported event");
    assert!(
        store
            .insert_event(community_a, &reported)
            .await
            .expect("insert reported event")
            .1
    );
    assert!(store
        .delete_event(community_a, reported.id.as_bytes())
        .await
        .expect("delete reported event"));

    let foreign = EventBuilder::new(Kind::TextNote, "foreign tenant message")
        .sign_with_keys(&Keys::generate())
        .expect("foreign event");
    assert!(
        store
            .insert_event(community_b, &foreign)
            .await
            .expect("insert foreign event")
            .1
    );

    let reporter = [0x41; 32];
    let report_event_a = [0x42; 32];
    let report_a = store
        .insert_report(
            community_a,
            new_report(
                &report_event_a,
                &reporter,
                ReportTarget::Event(reported.id.as_bytes().to_vec()),
                "spam",
                "same tenant",
            ),
        )
        .await
        .expect("report A");
    let report_event_missing = [0x43; 32];
    let missing_in_a = store
        .insert_report(
            community_a,
            new_report(
                &report_event_missing,
                &reporter,
                ReportTarget::Event(foreign.id.as_bytes().to_vec()),
                "malware",
                "foreign target must not join",
            ),
        )
        .await
        .expect("missing target report");
    let report_event_b = [0x44; 32];
    let report_b = store
        .insert_report(
            community_b,
            new_report(
                &report_event_b,
                &reporter,
                ReportTarget::Pubkey(vec![0x45; 32]),
                "spam",
                "pubkey report",
            ),
        )
        .await
        .expect("report B");
    assert!(store
        .resolve_report(community_b, report_b, "dismissed", &[0x46; 32])
        .await
        .expect("dismiss report B"));

    let detail = store
        .admin_report(report_a)
        .await
        .expect("admin detail")
        .expect("report A detail");
    assert_eq!(detail.report.community_id, *community_a.as_uuid());
    assert_eq!(detail.report.community_host, host_a);
    let message = detail.message.expect("same-tenant deleted message");
    assert_eq!(message.content, "same-tenant reported message");
    assert!(message.deleted_at.is_some());
    assert!(store
        .admin_report(missing_in_a)
        .await
        .expect("foreign target detail")
        .expect("foreign target report")
        .message
        .is_none());

    let reports_a = store
        .admin_reports(
            Some(*community_a.as_uuid()),
            Some("open"),
            None,
            Some("event"),
            None,
            None,
            None,
            200,
        )
        .await
        .expect("community A admin reports");
    assert_eq!(reports_a.len(), 2);
    assert!(reports_a
        .iter()
        .all(|report| report.community_id == *community_a.as_uuid()));
    assert!(reports_a.iter().any(|report| report.id == report_a));
    assert!(reports_a.iter().any(|report| report.id == missing_in_a));
    assert_eq!(
        store
            .admin_reports(
                Some(*community_a.as_uuid()),
                None,
                None,
                None,
                Some(DateTime::<Utc>::UNIX_EPOCH),
                Some(Utc::now() + Duration::days(1)),
                None,
                200,
            )
            .await
            .expect("bounded time window")
            .len(),
        2
    );
    assert!(store
        .admin_reports(
            Some(*community_a.as_uuid()),
            None,
            None,
            None,
            None,
            Some(DateTime::<Utc>::UNIX_EPOCH),
            None,
            200,
        )
        .await
        .expect("empty time window")
        .is_empty());

    let dismissed_b = store
        .admin_reports(
            Some(*community_b.as_uuid()),
            Some("dismissed"),
            Some("spam"),
            Some("pubkey"),
            None,
            None,
            None,
            0,
        )
        .await
        .expect("bounded report list");
    assert_eq!(dismissed_b.len(), 1);
    assert_eq!(dismissed_b[0].id, report_b);

    let first_page = store
        .admin_reports(
            Some(*community_a.as_uuid()),
            None,
            None,
            None,
            None,
            None,
            None,
            1,
        )
        .await
        .expect("first report page");
    assert_eq!(first_page.len(), 1);
    let cursor = (first_page[0].created_at, first_page[0].id);
    let second_page = store
        .admin_reports(
            Some(*community_a.as_uuid()),
            None,
            None,
            None,
            None,
            None,
            Some(cursor),
            1,
        )
        .await
        .expect("second report page");
    assert_eq!(second_page.len(), 1);
    assert_ne!(second_page[0].id, first_page[0].id);

    let mut feedback_event = [0_u8; 32];
    feedback_event[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    feedback_event[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    let submitter = [0x52; 32];
    let tags = serde_json::json!([["category", "bug"], ["client", "contract"]]);
    let event_created_at = Utc::now();
    let feedback = || NewProductFeedback {
        event_id: &feedback_event,
        submitter_pubkey: &submitter,
        category: Some("bug"),
        body: "shared admin contract",
        tags: &tags,
        event_created_at,
    };
    let feedback_id = store
        .insert_feedback(community_a, feedback())
        .await
        .expect("feedback");
    assert_eq!(
        store
            .insert_feedback(community_b, feedback())
            .await
            .expect("duplicate feedback"),
        feedback_id
    );

    let mut concurrent_event = [0_u8; 32];
    concurrent_event[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    concurrent_event[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    let concurrent_feedback = || NewProductFeedback {
        event_id: &concurrent_event,
        submitter_pubkey: &submitter,
        category: Some("needs-work"),
        body: "concurrent signed feedback",
        tags: &tags,
        event_created_at,
    };
    let (concurrent_a, concurrent_b) = tokio::join!(
        store.insert_feedback(community_a, concurrent_feedback()),
        store.insert_feedback(community_b, concurrent_feedback())
    );
    assert_eq!(
        concurrent_a.expect("concurrent feedback A"),
        concurrent_b.expect("concurrent feedback B"),
        "deployment-wide feedback idempotency must converge under a race"
    );

    let primary = store.feedback(200).await.expect("primary feedback list");
    let primary = primary
        .iter()
        .find(|feedback| feedback.id == feedback_id)
        .expect("feedback primary row");
    assert_eq!(primary.community_id, *community_a.as_uuid());
    assert_eq!(primary.body, "shared admin contract");

    let admin = store
        .admin_feedback_by_id(feedback_id)
        .await
        .expect("admin feedback")
        .expect("admin feedback row");
    assert_eq!(admin.community_id, *community_a.as_uuid());
    assert_eq!(admin.community_host, host_a);
    assert_eq!(admin.tags, tags);
    assert!(store
        .admin_feedback(200)
        .await
        .expect("admin feedback list")
        .iter()
        .any(|feedback| feedback.id == feedback_id));
    assert_eq!(
        store
            .admin_feedback(0)
            .await
            .expect("bounded admin feedback")
            .len(),
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
async fn sqlite_admin_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_admin_contract() {
    let pool = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/buzz")
        .await
        .expect("PostgreSQL connection");
    let db = Db::from_pool(pool);
    db.migrate().await.expect("PostgreSQL migrations");
    run_contract(&db).await;
}
