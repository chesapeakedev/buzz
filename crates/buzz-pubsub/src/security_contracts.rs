//! Shared Redis/SQLite durable-security behavior.

use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use buzz_auth::{LimitType, Nip98ReplayGuard, RateLimiter};
use buzz_core::{CommunityId, TenantContext};
use nostr::{EventBuilder, Keys, Kind};
use uuid::Uuid;

async fn run_replay_contract(guard: &dyn Nip98ReplayGuard) {
    let event = EventBuilder::new(Kind::HttpAuth, "")
        .sign_with_keys(&Keys::generate())
        .expect("signed replay event")
        .id;
    let scope_a = Uuid::new_v4().to_string();
    let scope_b = Uuid::new_v4().to_string();
    assert!(guard
        .try_mark_in_scope(&scope_a, &event, 1)
        .await
        .expect("first scope A claim"));
    assert!(!guard
        .try_mark_in_scope(&scope_a, &event, 1)
        .await
        .expect("scope A replay"));
    assert!(guard
        .try_mark_in_scope(&scope_b, &event, 1)
        .await
        .expect("scope B claim"));

    let race_event = EventBuilder::new(Kind::HttpAuth, "")
        .sign_with_keys(&Keys::generate())
        .expect("signed race event")
        .id;
    let race_scope = Uuid::new_v4().to_string();
    let race_a = guard.try_mark_in_scope(&race_scope, &race_event, 120);
    let race_b = guard.try_mark_in_scope(&race_scope, &race_event, 120);
    let (race_a, race_b) = tokio::join!(race_a, race_b);
    assert_eq!(
        [race_a.expect("race A"), race_b.expect("race B")]
            .into_iter()
            .filter(|claimed| *claimed)
            .count(),
        1
    );
}

async fn run_rate_contract(limiter: &dyn RateLimiter) {
    let tenant_a = TenantContext::resolved(
        CommunityId::from_uuid(Uuid::new_v4()),
        "security-a.example.test",
    );
    let tenant_b = TenantContext::resolved(
        CommunityId::from_uuid(Uuid::new_v4()),
        "security-b.example.test",
    );
    let pubkey = Keys::generate().public_key();
    let first = limiter
        .check_and_increment(&tenant_a, &pubkey, LimitType::Messages, 60, 1)
        .await
        .expect("first principal increment");
    let denied = limiter
        .check_and_increment(&tenant_a, &pubkey, LimitType::Messages, 60, 1)
        .await
        .expect("second principal increment");
    let other = limiter
        .check_and_increment(&tenant_b, &pubkey, LimitType::Messages, 60, 1)
        .await
        .expect("other tenant increment");
    assert_eq!((first.allowed, first.current), (true, 1));
    assert_eq!((denied.allowed, denied.current), (false, 2));
    assert_eq!((other.allowed, other.current), (true, 1));

    let ip = IpAddr::V6(Ipv6Addr::from(Uuid::new_v4().as_u128()));
    assert!(
        limiter
            .check_ip_connection(&ip, 60, 1)
            .await
            .expect("first IP increment")
            .allowed
    );
    assert!(
        !limiter
            .check_ip_connection(&ip, 60, 1)
            .await
            .expect("second IP increment")
            .allowed
    );
}

#[tokio::test]
async fn sqlite_security_contract() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = buzz_db::sqlite::SqliteStore::connect(
        &directory.path().join("buzz.sqlite3"),
        &buzz_db::sqlite::SqliteConfig::default(),
    )
    .await
    .expect("SQLite connection");
    store.migrate().await.expect("SQLite migrations");
    let security = Arc::new(crate::SqliteSecurityStore::new(store));
    run_replay_contract(security.as_ref()).await;
    run_rate_contract(security.as_ref()).await;
}

#[tokio::test]
#[ignore = "requires Redis"]
async fn redis_security_contract() {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let pool = deadpool_redis::Config::from_url(url)
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("Redis pool");
    let replay = crate::RedisNip98ReplayGuard::new(pool.clone());
    let rate = crate::rate_limiter::RedisRateLimiter::new(pool);
    run_replay_contract(&replay).await;
    run_rate_contract(&rate).await;
}
