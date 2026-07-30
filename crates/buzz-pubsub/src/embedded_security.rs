//! SQLite-backed replay and fixed-window security adapters.

use std::net::IpAddr;

use buzz_auth::{
    error::AuthError,
    ip_rate_limit_key,
    nip98_replay::{Nip98ReplayGuard, DEFAULT_REPLAY_TTL_SECS, MAX_REPLAY_TTL_SECS},
    rate_limit::{rate_limit_key, LimitType, RateLimitResult, RateLimiter},
};
use buzz_core::TenantContext;
use buzz_db::sqlite::SqliteStore;
use nostr::{EventId, PublicKey};

/// Durable embedded security adapter backed by the relay's SQLite database.
#[derive(Clone)]
pub struct SqliteSecurityStore {
    store: SqliteStore,
}

impl SqliteSecurityStore {
    /// Wrap the embedded relational store.
    pub fn new(store: SqliteStore) -> Self {
        Self { store }
    }
}

fn unavailable(operation: &'static str) -> AuthError {
    AuthError::Internal(format!("embedded {operation} unavailable"))
}

impl Nip98ReplayGuard for SqliteSecurityStore {
    fn try_mark_in_scope<'a>(
        &'a self,
        scope: &'a str,
        event_id: &'a EventId,
        ttl_secs: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, AuthError>> + Send + 'a>>
    {
        Box::pin(async move {
            let ttl = ttl_secs.clamp(DEFAULT_REPLAY_TTL_SECS, MAX_REPLAY_TTL_SECS);
            let ttl_micros = i64::try_from(ttl)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1_000_000))
                .ok_or_else(|| unavailable("replay check"))?;
            let expires_at = chrono::Utc::now()
                .timestamp_micros()
                .checked_add(ttl_micros)
                .ok_or_else(|| unavailable("replay check"))?;
            self.store
                .try_claim_security_replay(scope, event_id.as_bytes(), expires_at)
                .await
                .map_err(|_| unavailable("replay check"))
        })
    }
}

#[async_trait::async_trait]
impl RateLimiter for SqliteSecurityStore {
    async fn check_and_increment(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
        limit_type: LimitType,
        window_secs: u64,
        limit: u64,
    ) -> Result<RateLimitResult, AuthError> {
        let key = rate_limit_key(ctx, pubkey, &limit_type);
        self.increment(&key, window_secs, limit).await
    }

    async fn check_ip_connection(
        &self,
        ip: &IpAddr,
        window_secs: u64,
        limit: u64,
    ) -> Result<RateLimitResult, AuthError> {
        let key = ip_rate_limit_key(ip);
        self.increment(&key, window_secs, limit).await
    }
}

impl SqliteSecurityStore {
    async fn increment(
        &self,
        key: &str,
        window_secs: u64,
        limit: u64,
    ) -> Result<RateLimitResult, AuthError> {
        let window = self
            .store
            .increment_security_rate_window(key, window_secs)
            .await
            .map_err(|_| unavailable("rate-limit check"))?;
        if window.current <= limit {
            Ok(RateLimitResult::allowed(
                window.current,
                limit,
                window.reset_in_secs,
            ))
        } else {
            Ok(RateLimitResult::denied(
                window.current,
                limit,
                window.reset_in_secs,
            ))
        }
    }
}
