use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use buzz_core::TenantContext;
use nostr::PublicKey;
use tokio::sync::broadcast;

use crate::cache_invalidation::{CacheInvalidation, ScopedCacheInvalidation};
use crate::conn_control::{ConnControl, ScopedConnControl};
use crate::{ChannelEvent, EventTopic, PubSubError, RedisCoordination};

/// Backend-neutral coordination operations used by the relay.
///
/// Implementations may coordinate multiple relay processes through an
/// external service or confine delivery to one process. Durable replay and
/// rate-window operations use separate fail-closed interfaces because they
/// have different persistence requirements.
#[async_trait]
pub trait Coordination: Send + Sync {
    /// Run the event subscriber until the backend stops it.
    async fn run_event_subscriber(self: Arc<Self>);

    /// Run the cache-invalidation subscriber until the backend stops it.
    async fn run_cache_invalidation_subscriber(self: Arc<Self>);

    /// Run the connection-control subscriber until the backend stops it.
    async fn run_conn_control_subscriber(self: Arc<Self>);

    /// Subscribe to events delivered to this relay process.
    fn subscribe_events(&self) -> broadcast::Receiver<ChannelEvent>;

    /// Subscribe to cache invalidations delivered to this relay process.
    fn subscribe_cache_invalidations(&self) -> broadcast::Receiver<ScopedCacheInvalidation>;

    /// Subscribe to connection-control commands delivered to this relay process.
    fn subscribe_conn_control(&self) -> broadcast::Receiver<ScopedConnControl>;

    /// Retain local interest in a tenant-scoped event topic.
    async fn retain_topic(&self, ctx: &TenantContext, topic: EventTopic);

    /// Release local interest in a tenant-scoped event topic.
    async fn release_topic(&self, ctx: &TenantContext, topic: EventTopic);

    /// Publish an event to interested relay processes.
    async fn publish_event(
        &self,
        ctx: &TenantContext,
        topic: EventTopic,
        event: &nostr::Event,
    ) -> Result<i64, PubSubError>;

    /// Publish a cache invalidation to interested relay processes.
    async fn publish_cache_invalidation(
        &self,
        ctx: &TenantContext,
        invalidation: &CacheInvalidation,
    ) -> Result<i64, PubSubError>;

    /// Publish a connection-control command to interested relay processes.
    async fn publish_conn_control(
        &self,
        ctx: &TenantContext,
        command: &ConnControl,
    ) -> Result<i64, PubSubError>;

    /// Record an expiring presence status.
    async fn set_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
        status: &str,
    ) -> Result<(), PubSubError>;

    /// Remove a presence status.
    async fn clear_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
    ) -> Result<(), PubSubError>;

    /// Read one presence status.
    async fn get_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
    ) -> Result<Option<String>, PubSubError>;

    /// Read presence statuses as a public-key-hex to status map.
    async fn get_presence_bulk(
        &self,
        ctx: &TenantContext,
        pubkeys: &[PublicKey],
    ) -> Result<HashMap<String, String>, PubSubError>;
}

#[async_trait]
impl Coordination for RedisCoordination {
    async fn run_event_subscriber(self: Arc<Self>) {
        self.run_subscriber().await;
    }

    async fn run_cache_invalidation_subscriber(self: Arc<Self>) {
        RedisCoordination::run_cache_invalidation_subscriber(self).await;
    }

    async fn run_conn_control_subscriber(self: Arc<Self>) {
        RedisCoordination::run_conn_control_subscriber(self).await;
    }

    fn subscribe_events(&self) -> broadcast::Receiver<ChannelEvent> {
        self.subscribe_local()
    }

    fn subscribe_cache_invalidations(&self) -> broadcast::Receiver<ScopedCacheInvalidation> {
        RedisCoordination::subscribe_cache_invalidations(self)
    }

    fn subscribe_conn_control(&self) -> broadcast::Receiver<ScopedConnControl> {
        RedisCoordination::subscribe_conn_control(self)
    }

    async fn retain_topic(&self, ctx: &TenantContext, topic: EventTopic) {
        RedisCoordination::retain_topic(self, ctx, topic).await;
    }

    async fn release_topic(&self, ctx: &TenantContext, topic: EventTopic) {
        RedisCoordination::release_topic(self, ctx, topic).await;
    }

    async fn publish_event(
        &self,
        ctx: &TenantContext,
        topic: EventTopic,
        event: &nostr::Event,
    ) -> Result<i64, PubSubError> {
        RedisCoordination::publish_event(self, ctx, topic, event).await
    }

    async fn publish_cache_invalidation(
        &self,
        ctx: &TenantContext,
        invalidation: &CacheInvalidation,
    ) -> Result<i64, PubSubError> {
        RedisCoordination::publish_cache_invalidation(self, ctx, invalidation).await
    }

    async fn publish_conn_control(
        &self,
        ctx: &TenantContext,
        command: &ConnControl,
    ) -> Result<i64, PubSubError> {
        RedisCoordination::publish_conn_control(self, ctx, command).await
    }

    async fn set_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
        status: &str,
    ) -> Result<(), PubSubError> {
        RedisCoordination::set_presence(self, ctx, pubkey, status).await
    }

    async fn clear_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
    ) -> Result<(), PubSubError> {
        RedisCoordination::clear_presence(self, ctx, pubkey).await
    }

    async fn get_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
    ) -> Result<Option<String>, PubSubError> {
        RedisCoordination::get_presence(self, ctx, pubkey).await
    }

    async fn get_presence_bulk(
        &self,
        ctx: &TenantContext,
        pubkeys: &[PublicKey],
    ) -> Result<HashMap<String, String>, PubSubError> {
        RedisCoordination::get_presence_bulk(self, ctx, pubkeys).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::{CommunityId, TenantContext};
    use uuid::Uuid;

    #[tokio::test]
    async fn redis_adapter_is_usable_through_coordination_boundary() {
        let pool = crate::test_util::make_test_pool();
        let backend: Arc<dyn Coordination> = Arc::new(
            crate::RedisCoordination::new("redis://127.0.0.1:6379", pool)
                .await
                .expect("construct Redis coordination"),
        );
        let tenant =
            TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(0xaaaa)), "a.example");
        let topic = EventTopic::Channel(Uuid::from_u128(0xbbbb));

        let _events = backend.subscribe_events();
        let _invalidations = backend.subscribe_cache_invalidations();
        let _control = backend.subscribe_conn_control();
        backend.retain_topic(&tenant, topic).await;
        backend.release_topic(&tenant, topic).await;
    }

    #[tokio::test]
    #[ignore = "requires Redis"]
    async fn redis_adapter_satisfies_presence_contract() {
        let pool = crate::test_util::make_test_pool();
        let backend: Arc<dyn Coordination> = Arc::new(
            crate::RedisCoordination::new("redis://127.0.0.1:6379", pool)
                .await
                .expect("construct Redis coordination"),
        );
        super::test_contract::assert_presence_contract(backend).await;
    }
}

#[cfg(test)]
pub(crate) mod test_contract {
    use super::*;
    use buzz_core::{CommunityId, TenantContext};
    use nostr::Keys;
    use uuid::Uuid;

    pub(crate) async fn assert_presence_contract(backend: Arc<dyn Coordination>) {
        let tenant_a = TenantContext::resolved(
            CommunityId::from_uuid(Uuid::from_u128(0xc0a1)),
            "contract-a.example",
        );
        let tenant_b = TenantContext::resolved(
            CommunityId::from_uuid(Uuid::from_u128(0xc0b2)),
            "contract-b.example",
        );
        let present = Keys::generate().public_key();
        let absent = Keys::generate().public_key();

        backend
            .set_presence(&tenant_a, &present, "online")
            .await
            .expect("set tenant A presence");
        assert_eq!(
            backend
                .get_presence(&tenant_a, &present)
                .await
                .expect("get tenant A presence")
                .as_deref(),
            Some("online")
        );
        assert_eq!(
            backend
                .get_presence(&tenant_b, &present)
                .await
                .expect("get tenant B presence"),
            None,
            "the same pubkey must be isolated between communities"
        );

        backend
            .set_presence(&tenant_b, &present, "away")
            .await
            .expect("set tenant B presence");
        let bulk = backend
            .get_presence_bulk(&tenant_a, &[present, absent])
            .await
            .expect("bulk presence");
        assert_eq!(bulk.len(), 1);
        assert_eq!(
            bulk.get(&present.to_hex()).map(String::as_str),
            Some("online")
        );

        backend
            .clear_presence(&tenant_a, &present)
            .await
            .expect("clear tenant A presence");
        assert_eq!(
            backend
                .get_presence(&tenant_a, &present)
                .await
                .expect("get cleared presence"),
            None
        );
        assert_eq!(
            backend
                .get_presence(&tenant_b, &present)
                .await
                .expect("tenant B remains present")
                .as_deref(),
            Some("away")
        );
        backend
            .clear_presence(&tenant_b, &present)
            .await
            .expect("clear tenant B presence");
    }
}
