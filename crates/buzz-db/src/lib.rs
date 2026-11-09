#![deny(unsafe_code)]
#![warn(missing_docs)]
//! buzz-db — Postgres event store for Buzz.
//!
//! ## Design invariants
//! - AUTH events (kind 22242) are never stored — they carry bearer tokens.
//! - Ephemeral events (20000–29999) are never stored — Redis pub/sub only.
//! - Events table is partitioned by month on `created_at`.
//! - No FK references to partitioned tables.
//! - Uses `sqlx::query()` (runtime) not `sqlx::query!()` (compile-time).
//!
//! ## Runtime and store ownership
//! Database runtime infrastructure and domain persistence are physically
//! separated behind this crate-root compatibility facade:
//!
//! - Runtime concerns own pool construction, writer/replica routing,
//!   transactions, sessions, metrics, health support, and migrations.
//! - Store concerns own domain-specific SQL, row mapping, locking, mutation
//!   rules, indexes, and focused persistence tests.
//!
//! Existing crate-root modules, records, and [`Db`] methods remain the public
//! API. The internal `runtime` and `store` namespaces are not public APIs.

/// Wrap `$retire`, an `UPDATE`/`DELETE` on `events` that retires a
/// coordinate's previous head, so the same statement drops those rows' mention
/// index entries. Mention-index reads (`#p` filters and feeds) never return a
/// retired row, so its mention rows are dead weight they must walk and
/// discard: one per member for every kind:39002 roster republish. `$1` must
/// bind the community.
///
/// The three replacement paths that index mentions (NIP-33 parameterized,
/// addressable, and the relay's member snapshot) go through this, so a head
/// and its mention rows leave together on those paths.
macro_rules! superseded_with_mentions_sql {
    ($retire:literal) => {
        concat!(
            "WITH superseded AS (",
            $retire,
            " RETURNING id) \
             DELETE FROM event_mentions mention USING superseded \
             WHERE mention.community_id = $1 AND mention.event_id = superseded.id"
        )
    };
}

mod runtime;
mod store;

/// Concurrency caps whose limit can change at runtime.
pub mod concurrency_gate;

/// Database error types.
pub mod error;
/// SQLite connection policy and backend resources.
pub mod sqlite;

#[cfg(test)]
mod test_support;

pub use runtime::{
    insert_mentions, migration, replica_fence, AdmittedTx, ColdStartError, DatabaseBackendKind, Db,
    DbConfig, DbConnectionOutcome, DbConnectionStep, DbPoolRole, DbPoolStats, DbReadinessOutcome,
    ReadSession,
};

/// Valid low-cardinality `(pool_role, operation)` pairs for pool-acquisition telemetry.
pub const DB_POOL_ACQUIRE_VALID_PAIRS: [(&str, &str); 11] =
    runtime::observability::POOL_ACQUIRE_VALID_PAIRS;

/// Raw Prometheus series ceiling per relay pod for the operation-aware contract.
pub const DB_POOL_ACQUIRE_RAW_SERIES_PER_POD: usize =
    runtime::observability::POOL_ACQUIRE_RAW_SERIES_PER_POD;

/// Valid database connection role/step pairs with a start counter.
pub const DB_CONNECTION_STARTED_STEPS: [(DbPoolRole, DbConnectionStep); 4] =
    runtime::CONNECTION_STARTED_STEPS;

/// Valid database connection role/step pairs with a duration histogram.
pub const DB_CONNECTION_DURATION_STEPS: [(DbPoolRole, DbConnectionStep); 4] =
    runtime::CONNECTION_DURATION_STEPS;

/// Valid database connection role/step/outcome terminal combinations.
pub const DB_CONNECTION_TERMINALS: [(DbPoolRole, DbConnectionStep, DbConnectionOutcome); 15] =
    runtime::CONNECTION_TERMINALS;

/// Raw Prometheus series ceiling per pod for connection-step telemetry.
pub const DB_CONNECTION_RAW_SERIES_PER_POD: usize = runtime::CONNECTION_RAW_SERIES_PER_POD;
pub(crate) use runtime::{
    begin_community_event_write_transaction,
    begin_community_event_write_transaction_with_legacy_metrics, begin_community_sweep_transaction,
    begin_community_write_transaction, insert_mentions_in_transaction, observability, route_proof,
    sweep_admitted_communities, ReadSessionInner, RouteDecision, RoutePredicate,
};
pub use store::{
    admin_moderation, allowlist, api_token, archived_identities, artifact, artifact_sync, channel,
    channel_members, community, community_auth, deletion, dm, entity_deletion, event, feed,
    git_repo, leader_lock, media, membership_snapshot, moderation, nip_fi_binding,
    operator_listener, partition, personal_read, product_feedback, push, reaction, read_state,
    relay_admin_actions, relay_invite, relay_members, relay_operators, reminder, replaceable,
    storage_accounting, thread, thread_window, user, user_deletion, user_deletion_fences, workflow,
};

pub use allowlist::AllowlistEntry;
pub use api_token::{ApiTokenRecord, TokenSummary};
pub use community::{
    ArchivedCommunityRecord, CommunityRecord, CreateCommunityWithOwnerResult,
    CreatedCommunityRecord, EnsuredCommunityRecord, OwnedCommunityRecord, UnarchiveCommunityResult,
    UnarchivedCommunityRecord,
};
pub use concurrency_gate::{ConcurrencyGate, GatePermit};
pub use error::{DbError, Result};
pub use event::{ChannelHeadPrecondition, ChannelHeadWriteStatus, DeletionTarget};
pub use event::{EventQuery, DEFAULT_MAX_PAGE_LIMIT};
pub use leader_lock::SessionLeader;
pub use nip_fi_binding::{NipFiBindingRecord, NipFiUnbind, RowCapExceeded};
pub use reaction::ReactionEventInsertOutcome;
pub use reminder::DueReminder;
pub use sqlite::git_pointers::{
    cas_swap_pointer, get_pointer_metadata, GitPointerRow, PointerCasOutcome,
};

// Read-at compatibility adapters used by relay caches. The embedded backend
// has no replica session, so its authoritative read completes at the adapter's
// timestamp; PostgreSQL callers retain the same backend-neutral contract.
impl Db {
    /// Creates a community with its owner and optional issuer bindings.
    ///
    /// The embedded backend currently derives NIP-FI enforcement from the
    /// relay policy layer; the boolean is accepted here to keep the relay's
    /// upstream provisioning seam backend-neutral.
    pub async fn create_community_with_owner_and_nip_fi_policy(
        &self,
        host: &str,
        owner: &str,
        issuers: &[String],
        _requires_nip_fi: bool,
        relay_keypair: &nostr::Keys,
    ) -> Result<relay_members::MembershipWrite<CreateCommunityWithOwnerResult>> {
        self.create_community_with_owner_and_nip_fi_issuers(host, owner, issuers, relay_keypair)
            .await
    }

    /// Looks up a community and records the authoritative read timestamp.
    pub async fn lookup_community_by_host_read_at(
        &self,
        host: &str,
    ) -> Result<ReadAt<Option<CommunityRecord>>> {
        let value = self.lookup_community_by_host(host).await?;
        Ok(ReadAt {
            value,
            read_at: std::time::Instant::now(),
        })
    }

    /// Reads moderation state with the backend-neutral read timestamp contract.
    pub async fn moderation_restriction_state_read_at(
        &self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<ReadAt<moderation::RestrictionState>> {
        let value = self.moderation_restriction_state(community, pubkey).await?;
        Ok(ReadAt {
            value,
            read_at: std::time::Instant::now(),
        })
    }

    /// Reads listed open channels with the backend-neutral read timestamp contract.
    pub async fn get_listed_open_channel_ids_read_at(
        &self,
        community: CommunityId,
    ) -> Result<ReadAt<Vec<uuid::Uuid>>> {
        let value = self.get_listed_open_channel_ids(community).await?;
        Ok(ReadAt {
            value,
            read_at: std::time::Instant::now(),
        })
    }

    /// Reads community activity with the backend-neutral read timestamp contract.
    pub async fn is_community_active_read_at(
        &self,
        community: CommunityId,
    ) -> Result<ReadAt<bool>> {
        let value = self.is_community_active(community).await?;
        Ok(ReadAt {
            value,
            read_at: std::time::Instant::now(),
        })
    }

    /// Reads a member's accessible channels with the backend-neutral timestamp.
    pub async fn get_active_membership_ids_read_at(
        &self,
        community: CommunityId,
        pubkey: &[u8],
        _limit: u32,
    ) -> Result<ReadAt<Vec<uuid::Uuid>>> {
        let value = self.get_accessible_channel_ids(community, pubkey).await?;
        Ok(ReadAt {
            value,
            read_at: std::time::Instant::now(),
        })
    }

    /// Reads accessible channels for multiple members with read provenance.
    pub async fn get_active_membership_ids_bulk_read_at(
        &self,
        community: CommunityId,
        pubkeys: &[Vec<u8>],
        _limit: u32,
    ) -> Result<ReadAt<channel_members::PubkeyChannels>> {
        let mut value = Vec::new();
        for pubkey in pubkeys {
            for channel in self.get_accessible_channel_ids(community, pubkey).await? {
                value.push((pubkey.clone(), channel));
            }
        }
        Ok(ReadAt {
            value,
            read_at: std::time::Instant::now(),
        })
    }
}
pub use sqlite::media_objects::SqliteBlobMetadata;
pub use store::{ArtifactSnapshot, SnapshotDenied};

/// A value read from the database, with when its reading connection was
/// checked out of the pool.
///
/// The statement that produced `value` started after `read_at`, so its
/// snapshot is no older than `read_at`. A cache that serves `value` for a
/// bound after `read_at` is never staler than that bound, however long the
/// read waited for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadAt<T> {
    /// The value as read.
    pub value: T,
    /// When the connection that read `value` was checked out.
    pub read_at: std::time::Instant,
}

use buzz_core::CommunityId;

/// Durable NIP-FI verified-profile projection state.
pub use store::verified_profiles;

/// Result of an atomic command event and domain mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandExecution<T> {
    /// The event was already stored, so its mutation was not executed again.
    Duplicate,
    /// The event and domain mutation committed together.
    Applied(T),
}
