//! Single-process coordination backed by Tokio broadcast channels and Moka.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use buzz_core::{CommunityId, TenantContext};
use moka::future::Cache;
use nostr::PublicKey;
use tokio::sync::{broadcast, Mutex};

use crate::cache_invalidation::{CacheInvalidation, ScopedCacheInvalidation};
use crate::conn_control::{ConnControl, ScopedConnControl};
use crate::coordination::Coordination;
use crate::{ChannelEvent, EventTopic, EventTopicKey, PubSubError};

type PresenceKey = (CommunityId, [u8; 32]);

/// Configuration for single-process coordination.
#[derive(Debug, Clone)]
pub struct LocalCoordinationConfig {
    /// Capacity of the local event fan-out channel.
    pub event_channel_capacity: usize,
    /// Capacity of each invalidation and connection-control channel.
    pub control_channel_capacity: usize,
    /// Maximum number of tenant-scoped presence entries.
    pub presence_capacity: u64,
    /// Time after which a presence entry expires.
    pub presence_ttl: Duration,
}

impl Default for LocalCoordinationConfig {
    fn default() -> Self {
        Self {
            event_channel_capacity: 4096,
            control_channel_capacity: 4096,
            presence_capacity: 100_000,
            presence_ttl: Duration::from_secs(60),
        }
    }
}

/// Single-process implementation of relay coordination.
///
/// All messages remain inside this process. Presence is intentionally
/// ephemeral and bounded; durable replay claims and security rate windows are
/// separate database-backed concerns.
pub struct LocalCoordination {
    desired_topics: Mutex<HashMap<EventTopicKey, usize>>,
    event_tx: broadcast::Sender<ChannelEvent>,
    cache_invalidation_tx: broadcast::Sender<ScopedCacheInvalidation>,
    conn_control_tx: broadcast::Sender<ScopedConnControl>,
    presence: Cache<PresenceKey, String>,
    presence_evictions: Arc<AtomicU64>,
}

impl LocalCoordination {
    /// Construct local coordination with production defaults.
    pub fn new() -> Self {
        Self::build(LocalCoordinationConfig::default())
    }

    /// Construct local coordination with explicit capacity and expiry limits.
    pub fn with_config(config: LocalCoordinationConfig) -> Result<Self, PubSubError> {
        if config.event_channel_capacity == 0 {
            return Err(PubSubError::InvalidConfiguration(
                "event_channel_capacity must be greater than zero".to_owned(),
            ));
        }
        if config.control_channel_capacity == 0 {
            return Err(PubSubError::InvalidConfiguration(
                "control_channel_capacity must be greater than zero".to_owned(),
            ));
        }
        if config.presence_capacity == 0 {
            return Err(PubSubError::InvalidConfiguration(
                "presence_capacity must be greater than zero".to_owned(),
            ));
        }
        if config.presence_ttl.is_zero() {
            return Err(PubSubError::InvalidConfiguration(
                "presence_ttl must be greater than zero".to_owned(),
            ));
        }
        Ok(Self::build(config))
    }

    fn build(config: LocalCoordinationConfig) -> Self {
        let (event_tx, _) = broadcast::channel(config.event_channel_capacity);
        let (cache_invalidation_tx, _) = broadcast::channel(config.control_channel_capacity);
        let (conn_control_tx, _) = broadcast::channel(config.control_channel_capacity);
        let presence_evictions = Arc::new(AtomicU64::new(0));
        let eviction_counter = Arc::clone(&presence_evictions);
        let presence = Cache::builder()
            .max_capacity(config.presence_capacity)
            .time_to_live(config.presence_ttl)
            .eviction_listener(move |_key, _value, cause| {
                if cause.was_evicted() {
                    eviction_counter.fetch_add(1, Ordering::Relaxed);
                }
            })
            .build();

        Self {
            desired_topics: Mutex::new(HashMap::new()),
            event_tx,
            cache_invalidation_tx,
            conn_control_tx,
            presence,
            presence_evictions,
        }
    }

    /// Return the approximate number of live presence entries.
    pub fn presence_entry_count(&self) -> u64 {
        self.presence.entry_count()
    }

    /// Return the number of presence entries evicted by capacity or expiry.
    pub fn presence_eviction_count(&self) -> u64 {
        self.presence_evictions.load(Ordering::Relaxed)
    }

    fn presence_key(ctx: &TenantContext, pubkey: &PublicKey) -> PresenceKey {
        (ctx.community(), pubkey.to_bytes())
    }

    async fn wait_for_shutdown() {
        std::future::pending::<()>().await;
    }

    fn subscriber_count<T>(result: Result<usize, broadcast::error::SendError<T>>) -> i64 {
        result.map_or(0, |count| i64::try_from(count).unwrap_or(i64::MAX))
    }
}

impl Default for LocalCoordination {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Coordination for LocalCoordination {
    async fn run_event_subscriber(self: Arc<Self>) {
        Self::wait_for_shutdown().await;
    }

    async fn run_cache_invalidation_subscriber(self: Arc<Self>) {
        Self::wait_for_shutdown().await;
    }

    async fn run_conn_control_subscriber(self: Arc<Self>) {
        Self::wait_for_shutdown().await;
    }

    fn subscribe_events(&self) -> broadcast::Receiver<ChannelEvent> {
        self.event_tx.subscribe()
    }

    fn subscribe_cache_invalidations(&self) -> broadcast::Receiver<ScopedCacheInvalidation> {
        self.cache_invalidation_tx.subscribe()
    }

    fn subscribe_conn_control(&self) -> broadcast::Receiver<ScopedConnControl> {
        self.conn_control_tx.subscribe()
    }

    async fn retain_topic(&self, ctx: &TenantContext, topic: EventTopic) {
        let topic = EventTopicKey::from_context(ctx, topic);
        let mut desired = self.desired_topics.lock().await;
        *desired.entry(topic).or_insert(0) += 1;
    }

    async fn release_topic(&self, ctx: &TenantContext, topic: EventTopic) {
        let topic = EventTopicKey::from_context(ctx, topic);
        let mut desired = self.desired_topics.lock().await;
        let Some(count) = desired.get_mut(&topic) else {
            tracing::warn!(?topic, "release_topic called for unretained local topic");
            return;
        };
        *count -= 1;
        if *count == 0 {
            desired.remove(&topic);
        }
    }

    async fn publish_event(
        &self,
        ctx: &TenantContext,
        topic: EventTopic,
        event: &nostr::Event,
    ) -> Result<i64, PubSubError> {
        let topic_key = EventTopicKey::from_context(ctx, topic);
        if !self.desired_topics.lock().await.contains_key(&topic_key) {
            return Ok(0);
        }
        let event = ChannelEvent {
            community_id: ctx.community(),
            topic,
            event: event.clone(),
        };
        Ok(Self::subscriber_count(self.event_tx.send(event)))
    }

    async fn publish_cache_invalidation(
        &self,
        ctx: &TenantContext,
        invalidation: &CacheInvalidation,
    ) -> Result<i64, PubSubError> {
        let scoped = ScopedCacheInvalidation {
            community_id: ctx.community(),
            invalidation: invalidation.clone(),
        };
        Ok(Self::subscriber_count(
            self.cache_invalidation_tx.send(scoped),
        ))
    }

    async fn publish_conn_control(
        &self,
        ctx: &TenantContext,
        command: &ConnControl,
    ) -> Result<i64, PubSubError> {
        let scoped = ScopedConnControl {
            community_id: ctx.community(),
            command: command.clone(),
        };
        Ok(Self::subscriber_count(self.conn_control_tx.send(scoped)))
    }

    async fn set_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
        status: &str,
    ) -> Result<(), PubSubError> {
        self.presence
            .insert(Self::presence_key(ctx, pubkey), status.to_owned())
            .await;
        Ok(())
    }

    async fn clear_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
    ) -> Result<(), PubSubError> {
        self.presence
            .invalidate(&Self::presence_key(ctx, pubkey))
            .await;
        Ok(())
    }

    async fn get_presence(
        &self,
        ctx: &TenantContext,
        pubkey: &PublicKey,
    ) -> Result<Option<String>, PubSubError> {
        Ok(self.presence.get(&Self::presence_key(ctx, pubkey)).await)
    }

    async fn get_presence_bulk(
        &self,
        ctx: &TenantContext,
        pubkeys: &[PublicKey],
    ) -> Result<HashMap<String, String>, PubSubError> {
        let mut result = HashMap::with_capacity(pubkeys.len());
        for pubkey in pubkeys {
            if let Some(status) = self.presence.get(&Self::presence_key(ctx, pubkey)).await {
                result.insert(pubkey.to_hex(), status);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::{CommunityId, TenantContext};
    use nostr::{EventBuilder, Keys, Kind};
    use uuid::Uuid;

    fn ctx(id: u128, host: &str) -> TenantContext {
        TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(id)), host)
    }

    fn event(content: &str) -> nostr::Event {
        EventBuilder::new(Kind::TextNote, content)
            .tags([])
            .sign_with_keys(&Keys::generate())
            .expect("sign test event")
    }

    #[tokio::test]
    async fn local_adapter_satisfies_presence_contract() {
        let backend: Arc<dyn Coordination> = Arc::new(LocalCoordination::new());
        crate::coordination::test_contract::assert_presence_contract(backend).await;
    }

    #[tokio::test]
    async fn events_require_tenant_scoped_topic_interest() {
        let backend = LocalCoordination::new();
        let mut receiver = backend.subscribe_events();
        let tenant_a = ctx(0xaaaa, "a.example");
        let tenant_b = ctx(0xbbbb, "b.example");
        let topic = EventTopic::Channel(Uuid::from_u128(0xcccc));

        backend.retain_topic(&tenant_a, topic).await;
        assert_eq!(
            backend
                .publish_event(&tenant_b, topic, &event("wrong tenant"))
                .await
                .expect("publish without interest"),
            0
        );
        assert!(
            receiver.try_recv().is_err(),
            "tenant A interest must not receive tenant B events"
        );

        let sent = event("right tenant");
        assert_eq!(
            backend
                .publish_event(&tenant_a, topic, &sent)
                .await
                .expect("publish retained topic"),
            1
        );
        let received = receiver.recv().await.expect("receive local event");
        assert_eq!(received.community_id, tenant_a.community());
        assert_eq!(received.topic, topic);
        assert_eq!(received.event.id, sent.id);

        backend.release_topic(&tenant_a, topic).await;
        assert_eq!(
            backend
                .publish_event(&tenant_a, topic, &event("released"))
                .await
                .expect("publish released topic"),
            0
        );
    }

    #[tokio::test]
    async fn refcounts_keep_topic_live_until_last_release() {
        let backend = LocalCoordination::new();
        let mut receiver = backend.subscribe_events();
        let tenant = ctx(0xaaaa, "a.example");
        let topic = EventTopic::Global;

        backend.retain_topic(&tenant, topic).await;
        backend.retain_topic(&tenant, topic).await;
        backend.release_topic(&tenant, topic).await;
        assert_eq!(
            backend
                .publish_event(&tenant, topic, &event("still retained"))
                .await
                .expect("publish retained topic"),
            1
        );
        receiver.recv().await.expect("receive retained event");

        backend.release_topic(&tenant, topic).await;
        assert_eq!(
            backend
                .publish_event(&tenant, topic, &event("fully released"))
                .await
                .expect("publish released topic"),
            0
        );
    }

    #[tokio::test]
    async fn invalidation_and_control_messages_preserve_tenant_scope() {
        let backend = LocalCoordination::new();
        let mut invalidations = backend.subscribe_cache_invalidations();
        let mut controls = backend.subscribe_conn_control();
        let tenant = ctx(0xaaaa, "a.example");
        let invalidation = CacheInvalidation::Visibility {
            channel_id: Uuid::from_u128(0xbbbb),
        };

        assert_eq!(
            backend
                .publish_cache_invalidation(&tenant, &invalidation)
                .await
                .expect("publish invalidation"),
            1
        );
        assert_eq!(
            invalidations.recv().await.expect("receive invalidation"),
            ScopedCacheInvalidation {
                community_id: tenant.community(),
                invalidation,
            }
        );

        assert_eq!(
            backend
                .publish_conn_control(&tenant, &ConnControl::DisconnectCommunity)
                .await
                .expect("publish control"),
            1
        );
        assert_eq!(
            controls.recv().await.expect("receive control"),
            ScopedConnControl {
                community_id: tenant.community(),
                command: ConnControl::DisconnectCommunity,
            }
        );
    }

    #[tokio::test]
    async fn presence_expires_and_is_bounded() {
        let backend = LocalCoordination::with_config(LocalCoordinationConfig {
            presence_capacity: 2,
            presence_ttl: Duration::from_millis(20),
            ..LocalCoordinationConfig::default()
        })
        .expect("valid local coordination config");
        let tenant = ctx(0xaaaa, "a.example");
        let pubkey = Keys::generate().public_key();
        let second = Keys::generate().public_key();
        let third = Keys::generate().public_key();

        backend
            .set_presence(&tenant, &pubkey, "online")
            .await
            .expect("set presence");
        backend
            .set_presence(&tenant, &second, "online")
            .await
            .expect("set second presence");
        backend
            .set_presence(&tenant, &third, "online")
            .await
            .expect("set third presence");
        backend.presence.run_pending_tasks().await;
        assert!(backend.presence_entry_count() <= 2);
        assert!(backend.presence_eviction_count() >= 1);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            backend
                .get_presence(&tenant, &pubkey)
                .await
                .expect("get expired presence"),
            None
        );
    }

    #[test]
    fn zero_capacity_is_rejected_without_panicking() {
        let result = LocalCoordination::with_config(LocalCoordinationConfig {
            event_channel_capacity: 0,
            ..LocalCoordinationConfig::default()
        });
        assert!(matches!(
            result,
            Err(PubSubError::InvalidConfiguration(message))
                if message.contains("event_channel_capacity")
        ));
    }
}
