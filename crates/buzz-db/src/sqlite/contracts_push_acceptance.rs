//! Shared atomic signed push-lease acceptance contract.

use async_trait::async_trait;
use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};

use buzz_core::CommunityId;

use super::{SqliteConfig, SqliteStore};
use crate::push::{AcceptLeaseOutcome, ActiveLease, LeaseVersion, MatchLease, ReplaceLeaseOutcome};
use crate::{Db, EnsuredCommunityRecord, Result};

#[derive(Clone, Copy)]
struct Acceptance<'a> {
    installation: &'a str,
    generation: i64,
    endpoint: Option<&'a [u8]>,
    max_active: i64,
}

#[derive(Clone, Copy)]
struct SeedLease<'a> {
    installation: &'a str,
    source: &'a [u8],
    created_at: i64,
    endpoint: &'a [u8],
    expires_at: i64,
}

#[async_trait]
trait PushAcceptanceContract: Sync {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord>;
    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool>;
    async fn accept(
        &self,
        community: CommunityId,
        event: &Event,
        acceptance: Acceptance<'_>,
    ) -> Result<AcceptLeaseOutcome>;
    async fn seed_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        lease: SeedLease<'_>,
    ) -> Result<ReplaceLeaseOutcome>;
    async fn live_event(&self, community: CommunityId, id: &[u8]) -> Result<bool>;
    async fn active(&self, community: CommunityId) -> Result<Vec<MatchLease>>;
}

macro_rules! impl_sqlite_contract {
    () => {
        #[async_trait]
        impl PushAcceptanceContract for SqliteStore {
            async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
                self.ensure_configured_community(host).await
            }

            async fn ensure_user(
                &self,
                community: CommunityId,
                pubkey: &[u8],
            ) -> Result<bool> {
                self.ensure_user(community, pubkey).await
            }

            async fn accept(
                &self,
                community: CommunityId,
                event: &Event,
                acceptance: Acceptance<'_>,
            ) -> Result<AcceptLeaseOutcome> {
                let subscriptions = serde_json::json!([{"kinds":[9]}]);
                let active = acceptance.endpoint.map(|endpoint| ActiveLease {
                    app_profile: "ios-production",
                    endpoint_hash: endpoint,
                    endpoint_grant: "acceptance-contract-grant",
                    max_class: "default",
                    subscriptions: &subscriptions,
                });
                self.accept_push_lease_event(
                    community,
                    event,
                    acceptance.installation,
                    LeaseVersion {
                        source_event_id: event.id.as_bytes(),
                        source_created_at: event.created_at.as_secs() as i64,
                        generation: acceptance.generation,
                        expires_at: chrono::Utc::now().timestamp() + 3600,
                    },
                    active,
                    acceptance.max_active,
                )
                .await
            }

            async fn seed_lease(
                &self,
                community: CommunityId,
                author: &[u8],
                lease: SeedLease<'_>,
            ) -> Result<ReplaceLeaseOutcome> {
                let subscriptions = serde_json::json!([]);
                self.replace_active_lease(
                    community,
                    author,
                    lease.installation,
                    LeaseVersion {
                        source_event_id: lease.source,
                        source_created_at: lease.created_at,
                        generation: 1,
                        expires_at: lease.expires_at,
                    },
                    ActiveLease {
                        app_profile: "ios-production",
                        endpoint_hash: lease.endpoint,
                        endpoint_grant: "seed-contract-grant",
                        max_class: "default",
                        subscriptions: &subscriptions,
                    },
                )
                .await
            }

            async fn live_event(&self, community: CommunityId, id: &[u8]) -> Result<bool> {
                Ok(self.get_event_by_id(community, id).await?.is_some())
            }

            async fn active(&self, community: CommunityId) -> Result<Vec<MatchLease>> {
                self.active_push_match_leases(community).await
            }
        }
    };
}

impl_sqlite_contract!();

#[async_trait]
impl PushAcceptanceContract for Db {
    async fn ensure_community(&self, host: &str) -> Result<EnsuredCommunityRecord> {
        self.ensure_configured_community(host).await
    }

    async fn ensure_user(&self, community: CommunityId, pubkey: &[u8]) -> Result<bool> {
        self.ensure_user(community, pubkey).await
    }

    async fn accept(
        &self,
        community: CommunityId,
        event: &Event,
        acceptance: Acceptance<'_>,
    ) -> Result<AcceptLeaseOutcome> {
        let subscriptions = serde_json::json!([{"kinds":[9]}]);
        let active = acceptance.endpoint.map(|endpoint| ActiveLease {
            app_profile: "ios-production",
            endpoint_hash: endpoint,
            endpoint_grant: "acceptance-contract-grant",
            max_class: "default",
            subscriptions: &subscriptions,
        });
        self.accept_push_lease_event(
            community,
            event,
            acceptance.installation,
            LeaseVersion {
                source_event_id: event.id.as_bytes(),
                source_created_at: event.created_at.as_secs() as i64,
                generation: acceptance.generation,
                expires_at: chrono::Utc::now().timestamp() + 3600,
            },
            active,
            acceptance.max_active,
        )
        .await
    }

    async fn seed_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        lease: SeedLease<'_>,
    ) -> Result<ReplaceLeaseOutcome> {
        let subscriptions = serde_json::json!([]);
        crate::push::replace_active_lease(
            self.postgres_pool(),
            community,
            author,
            lease.installation,
            LeaseVersion {
                source_event_id: lease.source,
                source_created_at: lease.created_at,
                generation: 1,
                expires_at: lease.expires_at,
            },
            ActiveLease {
                app_profile: "ios-production",
                endpoint_hash: lease.endpoint,
                endpoint_grant: "seed-contract-grant",
                max_class: "default",
                subscriptions: &subscriptions,
            },
        )
        .await
    }

    async fn live_event(&self, community: CommunityId, id: &[u8]) -> Result<bool> {
        Ok(self.get_event_by_id(community, id).await?.is_some())
    }

    async fn active(&self, community: CommunityId) -> Result<Vec<MatchLease>> {
        self.active_push_match_leases(community).await
    }
}

fn lease_event(keys: &Keys, installation: &str, created_at: u64, body: &str) -> Event {
    EventBuilder::new(Kind::Custom(30350), body)
        .tags([Tag::parse(["d", installation]).expect("installation d tag")])
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(keys)
        .expect("signed lease event")
}

async fn run_contract(store: &impl PushAcceptanceContract) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let community_a = store
        .ensure_community(&format!("accept-a-{suffix}.example.test"))
        .await
        .expect("community A")
        .id;
    let community_b = store
        .ensure_community(&format!("accept-b-{suffix}.example.test"))
        .await
        .expect("community B")
        .id;
    let keys = Keys::generate();
    let author = keys.public_key().to_bytes();
    for community in [community_a, community_b] {
        store
            .ensure_user(community, &author)
            .await
            .expect("lease author");
    }
    let base = u64::try_from(chrono::Utc::now().timestamp()).expect("positive timestamp");
    let endpoint_a = [0xd1; 32];
    let primary = lease_event(&keys, "primary", base, "primary");
    for community in [community_a, community_b] {
        assert_eq!(
            store
                .accept(
                    community,
                    &primary,
                    Acceptance {
                        installation: "primary",
                        generation: 1,
                        endpoint: Some(&endpoint_a),
                        max_active: 4,
                    },
                )
                .await
                .expect("initial acceptance"),
            AcceptLeaseOutcome::Accepted
        );
        assert!(store
            .live_event(community, primary.id.as_bytes())
            .await
            .expect("source event"));
    }
    assert_eq!(
        store
            .accept(
                community_a,
                &primary,
                Acceptance {
                    installation: "primary",
                    generation: 1,
                    endpoint: Some(&endpoint_a),
                    max_active: 4,
                },
            )
            .await
            .expect("idempotent replay"),
        AcceptLeaseOutcome::StaleEvent
    );

    let stale_event = lease_event(&keys, "primary", base - 1, "stale event");
    assert_eq!(
        store
            .accept(
                community_a,
                &stale_event,
                Acceptance {
                    installation: "primary",
                    generation: 99,
                    endpoint: Some(&[0xd2; 32]),
                    max_active: 4,
                },
            )
            .await
            .expect("stale event outcome"),
        AcceptLeaseOutcome::StaleEvent
    );
    assert!(!store
        .live_event(community_a, stale_event.id.as_bytes())
        .await
        .expect("stale event rollback"));
    let stale_generation = lease_event(&keys, "primary", base + 1, "stale generation");
    assert_eq!(
        store
            .accept(
                community_a,
                &stale_generation,
                Acceptance {
                    installation: "primary",
                    generation: 1,
                    endpoint: Some(&[0xd3; 32]),
                    max_active: 4,
                },
            )
            .await
            .expect("stale generation outcome"),
        AcceptLeaseOutcome::StaleGeneration
    );
    assert!(!store
        .live_event(community_a, stale_generation.id.as_bytes())
        .await
        .expect("stale generation rollback"));

    let secondary = lease_event(&keys, "secondary", base + 2, "secondary");
    assert_eq!(
        store
            .accept(
                community_a,
                &secondary,
                Acceptance {
                    installation: "secondary",
                    generation: 1,
                    endpoint: Some(&[0xd4; 32]),
                    max_active: 4,
                },
            )
            .await
            .expect("secondary acceptance"),
        AcceptLeaseOutcome::Accepted
    );
    let duplicate_endpoint = lease_event(&keys, "duplicate", base + 3, "duplicate endpoint");
    assert_eq!(
        store
            .accept(
                community_a,
                &duplicate_endpoint,
                Acceptance {
                    installation: "duplicate",
                    generation: 1,
                    endpoint: Some(&endpoint_a),
                    max_active: 4,
                },
            )
            .await
            .expect("duplicate endpoint outcome"),
        AcceptLeaseOutcome::EndpointAlreadyLeased
    );
    assert!(!store
        .live_event(community_a, duplicate_endpoint.id.as_bytes())
        .await
        .expect("duplicate endpoint rollback"));

    let quota = lease_event(&keys, "quota", base + 4, "quota");
    assert_eq!(
        store
            .accept(
                community_a,
                &quota,
                Acceptance {
                    installation: "quota",
                    generation: 1,
                    endpoint: Some(&[0xd5; 32]),
                    max_active: 2,
                },
            )
            .await
            .expect("quota outcome"),
        AcceptLeaseOutcome::LeaseQuotaExceeded
    );
    assert!(!store
        .live_event(community_a, quota.id.as_bytes())
        .await
        .expect("quota rollback"));

    let conflicting_replacement =
        lease_event(&keys, "primary", base + 5, "conflicting replacement");
    assert_eq!(
        store
            .accept(
                community_a,
                &conflicting_replacement,
                Acceptance {
                    installation: "primary",
                    generation: 2,
                    endpoint: Some(&[0xd4; 32]),
                    max_active: 4,
                },
            )
            .await
            .expect("replacement collision"),
        AcceptLeaseOutcome::EndpointAlreadyLeased
    );
    assert!(
        store
            .live_event(community_a, primary.id.as_bytes())
            .await
            .expect("prior source survives rollback"),
        "a rejected effective-state update must roll back source tombstoning"
    );

    let collision_keys = Keys::generate();
    let collision_author = collision_keys.public_key().to_bytes();
    store
        .ensure_user(community_a, &collision_author)
        .await
        .expect("collision author");
    let collision_event = lease_event(&collision_keys, "incoming", base + 6, "collision");
    assert_eq!(
        store
            .seed_lease(
                community_a,
                &collision_author,
                SeedLease {
                    installation: "existing",
                    source: collision_event.id.as_bytes(),
                    created_at: base as i64,
                    endpoint: &[0xd6; 32],
                    expires_at: chrono::Utc::now().timestamp() + 3600,
                },
            )
            .await
            .expect("seed source collision"),
        ReplaceLeaseOutcome::Accepted
    );
    assert_eq!(
        store
            .accept(
                community_a,
                &collision_event,
                Acceptance {
                    installation: "incoming",
                    generation: 2,
                    endpoint: None,
                    max_active: 4,
                },
            )
            .await
            .expect("source collision outcome"),
        AcceptLeaseOutcome::SourceEventCollision
    );
    assert!(!store
        .live_event(community_a, collision_event.id.as_bytes())
        .await
        .expect("source collision rollback"));

    let expiry_keys = Keys::generate();
    let expiry_author = expiry_keys.public_key().to_bytes();
    store
        .ensure_user(community_a, &expiry_author)
        .await
        .expect("expiry author");
    assert_eq!(
        store
            .seed_lease(
                community_a,
                &expiry_author,
                SeedLease {
                    installation: "expired",
                    source: &[0xd7; 32],
                    created_at: base as i64,
                    endpoint: &[0xd8; 32],
                    expires_at: 1,
                },
            )
            .await
            .expect("seed expired lease"),
        ReplaceLeaseOutcome::Accepted
    );
    let after_expiry = lease_event(&expiry_keys, "after-expiry", base + 7, "after expiry");
    assert_eq!(
        store
            .accept(
                community_a,
                &after_expiry,
                Acceptance {
                    installation: "after-expiry",
                    generation: 1,
                    endpoint: Some(&[0xd9; 32]),
                    max_active: 1,
                },
            )
            .await
            .expect("expired lease no longer consumes quota"),
        AcceptLeaseOutcome::Accepted
    );

    let revoke = lease_event(&keys, "primary", base + 7, "revoke");
    assert_eq!(
        store
            .accept(
                community_a,
                &revoke,
                Acceptance {
                    installation: "primary",
                    generation: 2,
                    endpoint: None,
                    max_active: 4,
                },
            )
            .await
            .expect("inactive acceptance"),
        AcceptLeaseOutcome::Accepted
    );
    assert_eq!(
        store
            .active(community_a)
            .await
            .expect("remaining active leases")
            .iter()
            .filter(|lease| lease.installation_id == "primary")
            .count(),
        0
    );
    assert_eq!(
        store
            .active(community_b)
            .await
            .expect("B remains active")
            .iter()
            .filter(|lease| lease.installation_id == "primary")
            .count(),
        1
    );

    let race_keys = Keys::generate();
    let race_author = race_keys.public_key().to_bytes();
    store
        .ensure_user(community_b, &race_author)
        .await
        .expect("race author");
    let race_a = lease_event(&race_keys, "race-a", base + 10, "race A");
    let race_b = lease_event(&race_keys, "race-b", base + 10, "race B");
    let accept_a = store.accept(
        community_b,
        &race_a,
        Acceptance {
            installation: "race-a",
            generation: 1,
            endpoint: Some(&[0xe1; 32]),
            max_active: 1,
        },
    );
    let accept_b = store.accept(
        community_b,
        &race_b,
        Acceptance {
            installation: "race-b",
            generation: 1,
            endpoint: Some(&[0xe2; 32]),
            max_active: 1,
        },
    );
    let (accept_a, accept_b) = tokio::join!(accept_a, accept_b);
    let outcomes = [
        accept_a.expect("race acceptance A"),
        accept_b.expect("race acceptance B"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == AcceptLeaseOutcome::Accepted)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == AcceptLeaseOutcome::LeaseQuotaExceeded)
            .count(),
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
async fn sqlite_push_acceptance_contract() {
    let (_directory, store) = sqlite_fixture().await;
    run_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn postgres_push_acceptance_contract() {
    let admin = sqlx::PgPool::connect("postgres://buzz:buzz_dev@localhost:5432/postgres")
        .await
        .expect("PostgreSQL admin connection");
    let database = format!("buzz_accept_contract_{}", uuid::Uuid::new_v4().simple());
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
