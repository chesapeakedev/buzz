//! Shared community-moderation contract for relational backends.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::moderation::{
    ActionRecord, BanRecord, NewAction, NewReport, ReportRecord, ReportTarget, RestrictionState,
};
use crate::{Db, EnsuredCommunityRecord, Result};

#[async_trait]
trait ModerationContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn insert_report(&self, community: CommunityId, report: NewReport<'_>) -> Result<Uuid>;
    async fn list_reports(
        &self,
        community: CommunityId,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ReportRecord>>;
    async fn get_report(
        &self,
        community: CommunityId,
        report_id: Uuid,
    ) -> Result<Option<ReportRecord>>;
    async fn get_report_by_event(
        &self,
        community: CommunityId,
        report_event_id: &[u8],
    ) -> Result<Option<ReportRecord>>;
    async fn resolve_report(
        &self,
        community: CommunityId,
        report_id: Uuid,
        status: &str,
        resolved_by: &[u8],
        action_id: Option<Uuid>,
    ) -> Result<bool>;
    async fn ban(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        actor: &[u8],
        reason: Option<&str>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<()>;
    async fn unban(&self, community: CommunityId, pubkey: &[u8], actor: &[u8]) -> Result<bool>;
    async fn timeout(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        actor: &[u8],
        muted_until: DateTime<Utc>,
        reason: Option<&str>,
    ) -> Result<()>;
    async fn untimeout(&self, community: CommunityId, pubkey: &[u8], actor: &[u8]) -> Result<bool>;
    async fn restriction(&self, community: CommunityId, pubkey: &[u8]) -> Result<RestrictionState>;
    async fn get_ban(&self, community: CommunityId, pubkey: &[u8]) -> Result<Option<BanRecord>>;
    async fn list_restricted(&self, community: CommunityId) -> Result<Vec<BanRecord>>;
    async fn insert_action(&self, community: CommunityId, action: NewAction<'_>) -> Result<Uuid>;
    async fn list_actions(&self, community: CommunityId, limit: i64) -> Result<Vec<ActionRecord>>;
}

macro_rules! impl_contract {
    ($backend:ty) => {
        #[async_trait]
        impl ModerationContract for $backend {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn insert_report(
                &self,
                community: CommunityId,
                report: NewReport<'_>,
            ) -> Result<Uuid> {
                self.insert_moderation_report(community, report).await
            }

            async fn list_reports(
                &self,
                community: CommunityId,
                status: Option<&str>,
                limit: i64,
            ) -> Result<Vec<ReportRecord>> {
                self.list_moderation_reports(community, status, limit).await
            }

            async fn get_report(
                &self,
                community: CommunityId,
                report_id: Uuid,
            ) -> Result<Option<ReportRecord>> {
                self.get_moderation_report(community, report_id).await
            }

            async fn get_report_by_event(
                &self,
                community: CommunityId,
                report_event_id: &[u8],
            ) -> Result<Option<ReportRecord>> {
                self.get_moderation_report_by_event(community, report_event_id)
                    .await
            }

            async fn resolve_report(
                &self,
                community: CommunityId,
                report_id: Uuid,
                status: &str,
                resolved_by: &[u8],
                action_id: Option<Uuid>,
            ) -> Result<bool> {
                self.resolve_moderation_report(community, report_id, status, resolved_by, action_id)
                    .await
            }

            async fn ban(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                actor: &[u8],
                reason: Option<&str>,
                expires_at: Option<DateTime<Utc>>,
            ) -> Result<()> {
                self.ban_community_member(community, pubkey, actor, reason, expires_at)
                    .await
            }

            async fn unban(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                actor: &[u8],
            ) -> Result<bool> {
                self.unban_community_member(community, pubkey, actor).await
            }

            async fn timeout(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                actor: &[u8],
                muted_until: DateTime<Utc>,
                reason: Option<&str>,
            ) -> Result<()> {
                self.timeout_community_member(community, pubkey, actor, muted_until, reason)
                    .await
            }

            async fn untimeout(
                &self,
                community: CommunityId,
                pubkey: &[u8],
                actor: &[u8],
            ) -> Result<bool> {
                self.untimeout_community_member(community, pubkey, actor)
                    .await
            }

            async fn restriction(
                &self,
                community: CommunityId,
                pubkey: &[u8],
            ) -> Result<RestrictionState> {
                self.moderation_restriction_state(community, pubkey).await
            }

            async fn get_ban(
                &self,
                community: CommunityId,
                pubkey: &[u8],
            ) -> Result<Option<BanRecord>> {
                self.get_community_ban(community, pubkey).await
            }

            async fn list_restricted(&self, community: CommunityId) -> Result<Vec<BanRecord>> {
                self.list_community_restrictions(community).await
            }

            async fn insert_action(
                &self,
                community: CommunityId,
                action: NewAction<'_>,
            ) -> Result<Uuid> {
                self.insert_moderation_action(community, action).await
            }

            async fn list_actions(
                &self,
                community: CommunityId,
                limit: i64,
            ) -> Result<Vec<ActionRecord>> {
                self.list_moderation_actions(community, limit).await
            }
        }
    };
}

impl_contract!(SqliteStore);
impl_contract!(Db);

fn report<'a>(
    event_id: &'a [u8],
    reporter: &'a [u8],
    target: ReportTarget,
    note: Option<&'a str>,
) -> NewReport<'a> {
    NewReport {
        report_event_id: event_id,
        reporter_pubkey: reporter,
        target,
        channel_id: None,
        report_type: "spam",
        note,
    }
}

async fn run_contract(store: &impl ModerationContract) {
    let suffix = Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("reports-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("reports-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let report_event_id = [0x11; 32];
    let pubkey_report_event_id = [0x12; 32];
    let blob_report_event_id = [0x13; 32];
    let reporter = [0x22; 32];
    let target_event = [0x33; 32];
    let target_pubkey = [0x44; 32];
    let target_blob = [0x55; 32];
    let resolver_a = [0x66; 32];
    let resolver_b = [0x77; 32];
    let restricted_pubkey = [0x88; 32];

    let (first, duplicate) = tokio::join!(
        store.insert_report(
            community_a,
            report(
                &report_event_id,
                &reporter,
                ReportTarget::Event(target_event.to_vec()),
                Some("first"),
            ),
        ),
        store.insert_report(
            community_a,
            report(
                &report_event_id,
                &reporter,
                ReportTarget::Event(target_event.to_vec()),
                Some("concurrent retry"),
            ),
        ),
    );
    let report_id = first.expect("first concurrent report");
    assert_eq!(
        duplicate.expect("duplicate concurrent report"),
        report_id,
        "concurrent retries must converge on one row"
    );

    let pubkey_report_id = store
        .insert_report(
            community_a,
            report(
                &pubkey_report_event_id,
                &reporter,
                ReportTarget::Pubkey(target_pubkey.to_vec()),
                None,
            ),
        )
        .await
        .expect("pubkey report");
    let blob_report_id = store
        .insert_report(
            community_a,
            report(
                &blob_report_event_id,
                &reporter,
                ReportTarget::Blob(target_blob.to_vec()),
                None,
            ),
        )
        .await
        .expect("blob report");

    assert_eq!(
        store
            .get_report_by_event(community_a, &report_event_id)
            .await
            .expect("event-id lookup")
            .expect("event report")
            .id,
        report_id
    );
    assert!(matches!(
        store
            .get_report(community_a, pubkey_report_id)
            .await
            .expect("pubkey lookup")
            .expect("pubkey report")
            .target,
        ReportTarget::Pubkey(value) if value == target_pubkey
    ));
    assert!(matches!(
        store
            .get_report(community_a, blob_report_id)
            .await
            .expect("blob lookup")
            .expect("blob report")
            .target,
        ReportTarget::Blob(value) if value == target_blob
    ));

    assert!(
        store
            .get_report(community_b, report_id)
            .await
            .expect("cross-tenant row lookup")
            .is_none(),
        "row IDs cannot cross the community fence"
    );
    assert!(
        store
            .get_report_by_event(community_b, &report_event_id)
            .await
            .expect("cross-tenant event lookup")
            .is_none(),
        "event IDs cannot cross the community fence"
    );
    assert!(
        !store
            .resolve_report(community_b, report_id, "resolved", &resolver_a, None)
            .await
            .expect("cross-tenant resolve"),
        "another community cannot resolve the report"
    );

    let foreign_id = store
        .insert_report(
            community_b,
            report(
                &report_event_id,
                &reporter,
                ReportTarget::Event(target_event.to_vec()),
                Some("same signed id, different tenant"),
            ),
        )
        .await
        .expect("same event id in community B");
    assert_ne!(foreign_id, report_id, "idempotency is scoped by community");

    let foreign_action = store
        .insert_action(
            community_b,
            NewAction {
                actor_pubkey: &resolver_b,
                action: "resolve:ban",
                target_pubkey: Some(&target_pubkey),
                target_event_id: None,
                channel_id: None,
                reason_code: Some("foreign"),
                public_reason: None,
                private_reason: None,
                matched_principal: None,
            },
        )
        .await
        .expect("foreign moderation action");
    store
        .resolve_report(
            community_a,
            pubkey_report_id,
            "resolved",
            &resolver_b,
            Some(foreign_action),
        )
        .await
        .expect_err("cross-community resolution action must violate provenance");
    assert_eq!(
        store
            .get_report(community_a, pubkey_report_id)
            .await
            .expect("report after rejected resolution")
            .expect("report after rejected resolution")
            .status,
        "open",
        "failed provenance check must leave the report open"
    );

    let action_a = store
        .insert_action(
            community_a,
            NewAction {
                actor_pubkey: &resolver_a,
                action: "resolve:ban",
                target_pubkey: Some(&restricted_pubkey),
                target_event_id: Some(&target_event),
                channel_id: None,
                reason_code: Some("spam"),
                public_reason: Some("community policy"),
                private_reason: Some("moderator context"),
                matched_principal: Some("self"),
            },
        )
        .await
        .expect("resolution action A");
    let action_b = store
        .insert_action(
            community_a,
            NewAction {
                actor_pubkey: &resolver_b,
                action: "dismiss_report",
                target_pubkey: None,
                target_event_id: Some(&target_event),
                channel_id: None,
                reason_code: None,
                public_reason: None,
                private_reason: None,
                matched_principal: None,
            },
        )
        .await
        .expect("resolution action B");

    let (resolved, dismissed) = tokio::join!(
        store.resolve_report(
            community_a,
            report_id,
            "resolved",
            &resolver_a,
            Some(action_a),
        ),
        store.resolve_report(
            community_a,
            report_id,
            "dismissed",
            &resolver_b,
            Some(action_b),
        ),
    );
    assert_eq!(
        usize::from(resolved.expect("concurrent resolve"))
            + usize::from(dismissed.expect("concurrent dismiss")),
        1,
        "only one transition out of open may win"
    );

    let closed = store
        .get_report(community_a, report_id)
        .await
        .expect("closed report lookup")
        .expect("closed report");
    assert!(matches!(closed.status.as_str(), "resolved" | "dismissed"));
    assert!(closed.resolved_at.is_some());
    assert!(matches!(
        closed.resolved_by.as_deref(),
        Some(value) if value == resolver_a || value == resolver_b
    ));
    assert!(matches!(closed.action_id, Some(id) if id == action_a || id == action_b));

    let reingested = store
        .insert_report(
            community_a,
            report(
                &report_event_id,
                &reporter,
                ReportTarget::Event(target_event.to_vec()),
                Some("retry after resolution"),
            ),
        )
        .await
        .expect("reingest closed report");
    assert_eq!(reingested, report_id);
    assert_eq!(
        store
            .get_report(community_a, report_id)
            .await
            .expect("reingested report lookup")
            .expect("reingested report")
            .status,
        closed.status,
        "idempotent ingest must not reopen a closed report"
    );

    let open = store
        .list_reports(community_a, Some("open"), 100)
        .await
        .expect("open report queue");
    assert_eq!(open.len(), 2);
    assert!(open.iter().all(|row| row.status == "open"));
    let limited = store
        .list_reports(community_a, None, 1)
        .await
        .expect("limited report queue");
    assert_eq!(limited.len(), 1);
    assert!(store
        .list_reports(community_b, None, 100)
        .await
        .expect("community B report queue")
        .iter()
        .all(|row| row.id == foreign_id));

    let actions = store
        .list_actions(community_a, 100)
        .await
        .expect("community A actions");
    assert_eq!(actions.len(), 2);
    let detailed = actions
        .iter()
        .find(|action| action.id == action_a)
        .expect("detailed action");
    assert_eq!(detailed.actor_pubkey, resolver_a);
    assert_eq!(
        detailed.target_pubkey.as_deref(),
        Some(restricted_pubkey.as_slice())
    );
    assert_eq!(
        detailed.target_event_id.as_deref(),
        Some(target_event.as_slice())
    );
    assert_eq!(detailed.reason_code.as_deref(), Some("spam"));
    assert_eq!(detailed.public_reason.as_deref(), Some("community policy"));
    assert_eq!(
        detailed.private_reason.as_deref(),
        Some("moderator context")
    );
    assert_eq!(detailed.matched_principal.as_deref(), Some("self"));
    let foreign_actions = store
        .list_actions(community_b, 100)
        .await
        .expect("community B actions");
    assert_eq!(foreign_actions.len(), 1);
    assert_eq!(foreign_actions[0].id, foreign_action);
    assert!(
        actions.iter().all(|action| action.id != foreign_action),
        "moderation actions cannot cross the community fence"
    );

    let active_until =
        DateTime::from_timestamp_micros((Utc::now() + Duration::hours(1)).timestamp_micros())
            .expect("active timeout timestamp");
    let (ban_result, timeout_result) = tokio::join!(
        store.ban(
            community_a,
            &restricted_pubkey,
            &resolver_a,
            Some("ban reason"),
            None,
        ),
        store.timeout(
            community_a,
            &restricted_pubkey,
            &resolver_b,
            active_until,
            Some("timeout reason"),
        ),
    );
    ban_result.expect("concurrent ban");
    timeout_result.expect("concurrent timeout");

    let restriction = store
        .restriction(community_a, &restricted_pubkey)
        .await
        .expect("community A restriction");
    assert!(restriction.banned);
    assert_eq!(restriction.muted_until, Some(active_until));
    assert_eq!(
        store
            .restriction(community_b, &restricted_pubkey)
            .await
            .expect("community B restriction"),
        RestrictionState::default()
    );
    let ban = store
        .get_ban(community_a, &restricted_pubkey)
        .await
        .expect("get restriction row")
        .expect("restriction row");
    assert!(ban.banned);
    assert_eq!(ban.muted_until, Some(active_until));
    assert_eq!(
        store
            .list_restricted(community_a)
            .await
            .expect("restricted list")
            .len(),
        1
    );
    assert!(store
        .list_restricted(community_b)
        .await
        .expect("foreign restricted list")
        .is_empty());

    assert!(store
        .unban(community_a, &restricted_pubkey, &resolver_a)
        .await
        .expect("unban"));
    assert!(!store
        .unban(community_a, &restricted_pubkey, &resolver_a)
        .await
        .expect("repeat unban"));
    assert!(store
        .untimeout(community_a, &restricted_pubkey, &resolver_b)
        .await
        .expect("untimeout"));
    assert!(!store
        .untimeout(community_a, &restricted_pubkey, &resolver_b)
        .await
        .expect("repeat untimeout"));
    assert_eq!(
        store
            .restriction(community_a, &restricted_pubkey)
            .await
            .expect("cleared restriction"),
        RestrictionState::default()
    );
}

#[tokio::test]
async fn sqlite_moderation_contract() {
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
async fn postgres_moderation_contract() {
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
