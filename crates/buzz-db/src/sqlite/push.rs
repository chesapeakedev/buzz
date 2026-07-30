//! SQLite NIP-PL effective lease and matcher persistence.

use chrono::Utc;
use sqlx::{QueryBuilder, Row as _};

use buzz_core::CommunityId;

use super::SqliteStore;
use crate::push::{
    AcceptLeaseOutcome, ActiveLease, BatchedMatch, ClaimedMatchBatch, ClaimedWake,
    EnqueueWakeOutcome, LeaseVersion, MatchLease, NewWake, ReplaceLeaseOutcome,
    RevalidateWakeOutcome, WakeRequest, MAX_MATCH_ATTEMPTS, PUSH_GATE_BACKFILL_SECS,
};
use crate::Result;

async fn backfill_push_match_jobs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    community: CommunityId,
    now_micros: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO push_match_queue ( \
            community_id, event_id, next_attempt_at, created_at \
         ) SELECT community_id, id, ?, ? FROM events \
           WHERE community_id = ? \
             AND kind IN (7, 9, 1059, 40007, 46010) \
             AND deleted_at IS NULL AND received_at > ? \
         ON CONFLICT DO NOTHING",
    )
    .bind(now_micros)
    .bind(now_micros)
    .bind(community.as_uuid().to_string())
    .bind(now_micros - PUSH_GATE_BACKFILL_SECS * 1_000_000)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn parse_uuid(value: String, column: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(&value)
        .map_err(|error| crate::DbError::InvalidData(format!("invalid {column} UUID: {error}")))
}

fn row_to_claimed_wake(row: sqlx::sqlite::SqliteRow) -> Result<ClaimedWake> {
    let community = parse_uuid(row.try_get("community_id")?, "community")?;
    let channel_id = row
        .try_get::<Option<String>, _>("channel_id")?
        .map(|value| parse_uuid(value, "channel"))
        .transpose()?;
    Ok(ClaimedWake {
        community: CommunityId::from_uuid(community),
        id: parse_uuid(row.try_get("id")?, "wake")?,
        claim_id: parse_uuid(row.try_get("claim_id")?, "claim")?,
        event_id: row.try_get("event_id")?,
        channel_id,
        author: row.try_get("author")?,
        installation_id: row.try_get("installation_id")?,
        lease_generation: row.try_get("lease_generation")?,
        endpoint_grant: row.try_get("endpoint_grant")?,
        class: row.try_get("class")?,
        expires_at: row.try_get("expires_at")?,
        attempt: row.try_get("attempts")?,
    })
}

fn acceptance_constraint_outcome(error: &sqlx::Error) -> Option<AcceptLeaseOutcome> {
    let database = error.as_database_error()?;
    let message = database.message();
    if message.contains(
        "push_leases.community_id, push_leases.author, \
         push_leases.app_profile, push_leases.endpoint_hash",
    ) {
        Some(AcceptLeaseOutcome::EndpointAlreadyLeased)
    } else if message.contains("push_leases.community_id, push_leases.source_event_id") {
        Some(AcceptLeaseOutcome::SourceEventCollision)
    } else if database.is_unique_violation()
        || database.is_check_violation()
        || database.is_foreign_key_violation()
    {
        Some(AcceptLeaseOutcome::ConstraintViolation)
    } else {
        None
    }
}

impl SqliteStore {
    /// Atomically persist a validated signed lease event and effective state.
    #[allow(clippy::too_many_arguments)]
    pub async fn accept_push_lease_event(
        &self,
        community: CommunityId,
        event: &nostr::Event,
        installation_id: &str,
        version: LeaseVersion<'_>,
        active: Option<ActiveLease<'_>>,
        max_active_leases: i64,
    ) -> Result<AcceptLeaseOutcome> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let author = event.pubkey.to_bytes();

        if let Some(row) = sqlx::query(
            "SELECT author, installation_id FROM push_leases \
             WHERE community_id = ? AND source_event_id = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(version.source_event_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let existing_author: Vec<u8> = row.try_get("author")?;
            let existing_installation: String = row.try_get("installation_id")?;
            transaction.rollback().await?;
            return Ok(
                if existing_author.as_slice() == author && existing_installation == installation_id
                {
                    AcceptLeaseOutcome::StaleEvent
                } else {
                    AcceptLeaseOutcome::SourceEventCollision
                },
            );
        }

        if let Some(row) = sqlx::query(
            "SELECT source_event_id, source_created_at, generation \
             FROM push_leases \
             WHERE community_id = ? AND author = ? AND installation_id = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(author.as_slice())
        .bind(installation_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let current_created_at: i64 = row.try_get("source_created_at")?;
            let current_event_id: Vec<u8> = row.try_get("source_event_id")?;
            let current_generation: i64 = row.try_get("generation")?;
            let wins_event = version.source_created_at > current_created_at
                || (version.source_created_at == current_created_at
                    && version.source_event_id < current_event_id.as_slice());
            if !wins_event {
                transaction.rollback().await?;
                return Ok(AcceptLeaseOutcome::StaleEvent);
            }
            if version.generation <= current_generation {
                transaction.rollback().await?;
                return Ok(AcceptLeaseOutcome::StaleGeneration);
            }
        }

        let now_micros = Utc::now().timestamp_micros();
        sqlx::query(
            "UPDATE push_leases SET active = 0, endpoint_enabled = 0, \
                app_profile = NULL, endpoint_hash = NULL, endpoint_grant = NULL, \
                max_class = NULL, subscriptions = NULL, updated_at = ? \
             WHERE community_id = ? AND author = ? AND active = 1 \
               AND expires_at <= ?",
        )
        .bind(now_micros)
        .bind(community.as_uuid().to_string())
        .bind(author.as_slice())
        .bind(Utc::now().timestamp())
        .execute(&mut *transaction)
        .await?;

        if let Some(active) = active {
            let active_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM push_leases \
                 WHERE community_id = ? AND author = ? AND active = 1 \
                   AND installation_id <> ?",
            )
            .bind(community.as_uuid().to_string())
            .bind(author.as_slice())
            .bind(installation_id)
            .fetch_one(&mut *transaction)
            .await?;
            if active_count >= max_active_leases {
                transaction.rollback().await?;
                return Ok(AcceptLeaseOutcome::LeaseQuotaExceeded);
            }
            let duplicate: bool = sqlx::query_scalar(
                "SELECT EXISTS( \
                    SELECT 1 FROM push_leases \
                    WHERE community_id = ? AND author = ? \
                      AND installation_id <> ? AND active = 1 \
                      AND app_profile = ? AND endpoint_hash = ? \
                 )",
            )
            .bind(community.as_uuid().to_string())
            .bind(author.as_slice())
            .bind(installation_id)
            .bind(active.app_profile)
            .bind(active.endpoint_hash)
            .fetch_one(&mut *transaction)
            .await?;
            if duplicate {
                transaction.rollback().await?;
                return Ok(AcceptLeaseOutcome::EndpointAlreadyLeased);
            }
        }

        sqlx::query(
            "UPDATE events SET deleted_at = ? \
             WHERE community_id = ? AND kind = 30350 AND pubkey = ? \
               AND d_tag = ? AND deleted_at IS NULL",
        )
        .bind(now_micros)
        .bind(community.as_uuid().to_string())
        .bind(author.as_slice())
        .bind(installation_id)
        .execute(&mut *transaction)
        .await?;
        let (_, inserted) =
            super::events::insert_event_transaction(&mut transaction, community, event, None)
                .await?;
        if !inserted {
            transaction.rollback().await?;
            return Ok(AcceptLeaseOutcome::ConstraintViolation);
        }

        let (is_active, app_profile, endpoint_hash, endpoint_grant, max_class, subscriptions) =
            match active {
                Some(active) => (
                    1_i64,
                    Some(active.app_profile),
                    Some(active.endpoint_hash),
                    Some(active.endpoint_grant),
                    Some(active.max_class),
                    Some(serde_json::to_string(active.subscriptions)?),
                ),
                None => (0, None, None, None, None, None),
            };
        let lease = sqlx::query(
            "INSERT INTO push_leases ( \
                community_id, author, installation_id, source_event_id, \
                source_created_at, generation, active, app_profile, \
                endpoint_hash, endpoint_grant, max_class, subscriptions, \
                expires_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, author, installation_id) DO UPDATE SET \
                source_event_id = excluded.source_event_id, \
                source_created_at = excluded.source_created_at, \
                generation = excluded.generation, active = excluded.active, \
                endpoint_enabled = 1, app_profile = excluded.app_profile, \
                endpoint_hash = excluded.endpoint_hash, \
                endpoint_grant = excluded.endpoint_grant, \
                max_class = excluded.max_class, \
                subscriptions = excluded.subscriptions, \
                expires_at = excluded.expires_at, updated_at = excluded.updated_at",
        )
        .bind(community.as_uuid().to_string())
        .bind(author.as_slice())
        .bind(installation_id)
        .bind(version.source_event_id)
        .bind(version.source_created_at)
        .bind(version.generation)
        .bind(is_active)
        .bind(app_profile)
        .bind(endpoint_hash)
        .bind(endpoint_grant)
        .bind(max_class)
        .bind(subscriptions)
        .bind(version.expires_at)
        .bind(now_micros)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = lease {
            let outcome = acceptance_constraint_outcome(&error);
            transaction.rollback().await?;
            if let Some(outcome) = outcome {
                return Ok(outcome);
            }
            return Err(error.into());
        }
        if is_active == 1 {
            backfill_push_match_jobs(&mut transaction, community, now_micros).await?;
        }
        transaction.commit().await?;
        Ok(AcceptLeaseOutcome::Accepted)
    }

    /// Create or rotate an active lease when event and generation ordering win.
    #[allow(clippy::too_many_arguments)]
    pub async fn replace_active_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        version: LeaseVersion<'_>,
        active: ActiveLease<'_>,
    ) -> Result<ReplaceLeaseOutcome> {
        self.replace_lease(community, author, installation_id, version, Some(active))
            .await
    }

    /// Revoke one installation with a higher-generation inactive replacement.
    pub async fn revoke_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        version: LeaseVersion<'_>,
    ) -> Result<ReplaceLeaseOutcome> {
        self.replace_lease(community, author, installation_id, version, None)
            .await
    }

    async fn replace_lease(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        version: LeaseVersion<'_>,
        active: Option<ActiveLease<'_>>,
    ) -> Result<ReplaceLeaseOutcome> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let now_micros = Utc::now().timestamp_micros();
        let (is_active, app_profile, endpoint_hash, endpoint_grant, max_class, subscriptions) =
            match active {
                Some(active) => (
                    1_i64,
                    Some(active.app_profile),
                    Some(active.endpoint_hash),
                    Some(active.endpoint_grant),
                    Some(active.max_class),
                    Some(serde_json::to_string(active.subscriptions)?),
                ),
                None => (0, None, None, None, None, None),
            };

        let accepted = sqlx::query(
            "INSERT INTO push_leases ( \
                community_id, author, installation_id, source_event_id, \
                source_created_at, generation, active, app_profile, \
                endpoint_hash, endpoint_grant, max_class, subscriptions, \
                expires_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (community_id, author, installation_id) DO UPDATE SET \
                source_event_id = excluded.source_event_id, \
                source_created_at = excluded.source_created_at, \
                generation = excluded.generation, active = excluded.active, \
                endpoint_enabled = 1, app_profile = excluded.app_profile, \
                endpoint_hash = excluded.endpoint_hash, \
                endpoint_grant = excluded.endpoint_grant, \
                max_class = excluded.max_class, \
                subscriptions = excluded.subscriptions, \
                expires_at = excluded.expires_at, updated_at = excluded.updated_at \
             WHERE ( \
                    excluded.source_created_at > push_leases.source_created_at \
                    OR ( \
                        excluded.source_created_at = push_leases.source_created_at \
                        AND excluded.source_event_id < push_leases.source_event_id \
                    ) \
                   ) \
               AND excluded.generation > push_leases.generation \
             RETURNING generation",
        )
        .bind(community.as_uuid().to_string())
        .bind(author)
        .bind(installation_id)
        .bind(version.source_event_id)
        .bind(version.source_created_at)
        .bind(version.generation)
        .bind(is_active)
        .bind(app_profile)
        .bind(endpoint_hash)
        .bind(endpoint_grant)
        .bind(max_class)
        .bind(subscriptions)
        .bind(version.expires_at)
        .bind(now_micros)
        .fetch_optional(&mut *transaction)
        .await?;

        if accepted.is_some() {
            if is_active == 1 {
                backfill_push_match_jobs(&mut transaction, community, now_micros).await?;
            }
            transaction.commit().await?;
            return Ok(ReplaceLeaseOutcome::Accepted);
        }

        let current = sqlx::query(
            "SELECT source_event_id, source_created_at, generation \
             FROM push_leases \
             WHERE community_id = ? AND author = ? AND installation_id = ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(author)
        .bind(installation_id)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        let current_created_at: i64 = current.try_get("source_created_at")?;
        let current_event_id: Vec<u8> = current.try_get("source_event_id")?;
        let wins_event_order = version.source_created_at > current_created_at
            || (version.source_created_at == current_created_at
                && version.source_event_id < current_event_id.as_slice());
        if wins_event_order {
            Ok(ReplaceLeaseOutcome::StaleGeneration)
        } else {
            Ok(ReplaceLeaseOutcome::StaleEvent)
        }
    }

    /// Load active endpoint-enabled, unexpired leases for one community.
    pub async fn active_push_match_leases(
        &self,
        community: CommunityId,
    ) -> Result<Vec<MatchLease>> {
        let rows = sqlx::query(
            "SELECT author, installation_id, generation, subscriptions, expires_at \
             FROM push_leases \
             WHERE community_id = ? AND active = 1 AND endpoint_enabled = 1 \
               AND expires_at > ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(Utc::now().timestamp())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MatchLease {
                    author: row.try_get("author")?,
                    installation_id: row.try_get("installation_id")?,
                    generation: row.try_get("generation")?,
                    subscriptions: serde_json::from_str(
                        row.try_get::<String, _>("subscriptions")?.as_str(),
                    )?,
                    expires_at: row.try_get("expires_at")?,
                })
            })
            .collect()
    }

    /// Idempotently enqueue one wake from the current effective lease.
    pub async fn enqueue_push_wake(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        wake: NewWake<'_>,
    ) -> Result<EnqueueWakeOutcome> {
        let outcomes = self
            .enqueue_push_wakes(
                community,
                &[WakeRequest {
                    author: author.to_vec(),
                    installation_id: installation_id.to_owned(),
                    lease_generation: wake.lease_generation,
                    event_id: wake.event_id.to_vec(),
                    class: wake.class.to_owned(),
                    expires_at: wake.expires_at,
                }],
            )
            .await?;
        outcomes.into_iter().next().ok_or_else(|| {
            crate::DbError::InvalidData("wake enqueue returned no outcome".to_owned())
        })
    }

    /// Set-wise enqueue wakes after resolving current lease generations.
    pub async fn enqueue_push_wakes(
        &self,
        community: CommunityId,
        requests: &[WakeRequest],
    ) -> Result<Vec<EnqueueWakeOutcome>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let mut pairs: Vec<(Vec<u8>, String)> = requests
            .iter()
            .map(|request| (request.author.clone(), request.installation_id.clone()))
            .collect();
        pairs.sort_unstable();
        pairs.dedup();

        let mut lease_query: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT author, installation_id, generation, endpoint_hash \
             FROM push_leases WHERE community_id = ",
        );
        lease_query
            .push_bind(community.as_uuid().to_string())
            .push(" AND active = 1 AND endpoint_enabled = 1 AND expires_at > ")
            .push_bind(Utc::now().timestamp())
            .push(" AND (author, installation_id) IN (");
        lease_query.push_values(&pairs, |mut values, (author, installation)| {
            values.push_bind(author).push_bind(installation);
        });
        lease_query.push(")");
        let rows = lease_query.build().fetch_all(&mut *transaction).await?;
        let mut leases = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            leases.insert(
                (
                    row.try_get::<Vec<u8>, _>("author")?,
                    row.try_get::<String, _>("installation_id")?,
                ),
                (
                    row.try_get::<i64, _>("generation")?,
                    row.try_get::<Vec<u8>, _>("endpoint_hash")?,
                ),
            );
        }
        let resolved: Vec<Option<Vec<u8>>> = requests
            .iter()
            .map(|request| {
                leases
                    .get(&(request.author.clone(), request.installation_id.clone()))
                    .filter(|(generation, _)| *generation == request.lease_generation)
                    .map(|(_, endpoint)| endpoint.clone())
            })
            .collect();
        let eligible: Vec<(usize, uuid::Uuid, Vec<u8>)> = resolved
            .iter()
            .enumerate()
            .filter_map(|(index, endpoint)| {
                endpoint
                    .as_ref()
                    .map(|endpoint| (index, uuid::Uuid::new_v4(), endpoint.clone()))
            })
            .collect();
        let now = Utc::now().timestamp_micros();
        let mut inserted_keys = std::collections::HashSet::new();
        let mut job_ids = std::collections::HashMap::new();
        if !eligible.is_empty() {
            let mut insert: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
                "INSERT INTO push_wake_outbox ( \
                    community_id, id, author, installation_id, lease_generation, \
                    endpoint_hash, event_id, class, expires_at, next_attempt_at, \
                    created_at \
                 ) ",
            );
            insert.push_values(&eligible, |mut values, (index, id, endpoint_hash)| {
                let request = &requests[*index];
                values
                    .push_bind(community.as_uuid().to_string())
                    .push_bind(id.to_string())
                    .push_bind(&request.author)
                    .push_bind(&request.installation_id)
                    .push_bind(request.lease_generation)
                    .push_bind(endpoint_hash)
                    .push_bind(&request.event_id)
                    .push_bind(&request.class)
                    .push_bind(request.expires_at)
                    .push_bind(now)
                    .push_bind(now);
            });
            insert.push(
                " ON CONFLICT (community_id, endpoint_hash, event_id) DO NOTHING \
                  RETURNING endpoint_hash, event_id, id",
            );
            for row in insert.build().fetch_all(&mut *transaction).await? {
                let key = (
                    row.try_get::<Vec<u8>, _>("endpoint_hash")?,
                    row.try_get::<Vec<u8>, _>("event_id")?,
                );
                let id = parse_uuid(row.try_get("id")?, "wake")?;
                inserted_keys.insert(key.clone());
                job_ids.insert(key, id);
            }

            let mut lookup: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
                "SELECT endpoint_hash, event_id, id FROM push_wake_outbox \
                 WHERE community_id = ",
            );
            lookup
                .push_bind(community.as_uuid().to_string())
                .push(" AND (endpoint_hash, event_id) IN (");
            lookup.push_values(&eligible, |mut values, (index, _, endpoint_hash)| {
                values
                    .push_bind(endpoint_hash)
                    .push_bind(&requests[*index].event_id);
            });
            lookup.push(")");
            for row in lookup.build().fetch_all(&mut *transaction).await? {
                let key = (
                    row.try_get::<Vec<u8>, _>("endpoint_hash")?,
                    row.try_get::<Vec<u8>, _>("event_id")?,
                );
                job_ids
                    .entry(key)
                    .or_insert(parse_uuid(row.try_get("id")?, "wake")?);
            }
        }
        transaction.commit().await?;

        let mut reported = std::collections::HashSet::new();
        requests
            .iter()
            .zip(resolved)
            .map(|(request, endpoint)| {
                let Some(endpoint) = endpoint else {
                    return Ok(EnqueueWakeOutcome::InactiveLease);
                };
                let key = (endpoint, request.event_id.clone());
                let id = *job_ids.get(&key).ok_or_else(|| {
                    crate::DbError::InvalidData(
                        "wake enqueue resolved neither insert nor duplicate".to_owned(),
                    )
                })?;
                Ok(if inserted_keys.contains(&key) && reported.insert(key) {
                    EnqueueWakeOutcome::Enqueued(id)
                } else {
                    EnqueueWakeOutcome::Duplicate(id)
                })
            })
            .collect()
    }

    /// Claim due wakes for one community and fence the worker generation.
    pub async fn claim_due_push_wakes(
        &self,
        community: CommunityId,
        limit: i64,
        lease_until: chrono::DateTime<Utc>,
    ) -> Result<Vec<ClaimedWake>> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let claim_id = uuid::Uuid::new_v4();
        let now_micros = Utc::now().timestamp_micros();
        let now_secs = Utc::now().timestamp();
        sqlx::query(
            "UPDATE push_wake_outbox SET state = 'sending', claim_id = ?, \
                lease_until = ?, attempts = attempts + 1 \
             WHERE rowid IN ( \
                SELECT o.rowid FROM push_wake_outbox o \
                JOIN push_leases l \
                  ON l.community_id = o.community_id \
                 AND l.author = o.author \
                 AND l.installation_id = o.installation_id \
                 AND l.generation = o.lease_generation \
                 AND l.endpoint_hash = o.endpoint_hash \
                JOIN events e \
                  ON e.community_id = o.community_id \
                 AND e.id = o.event_id AND e.deleted_at IS NULL \
                WHERE o.community_id = ? AND o.expires_at > ? \
                  AND o.next_attempt_at <= ? \
                  AND (o.state = 'pending' OR ( \
                      o.state = 'sending' AND o.lease_until < ? \
                  )) \
                  AND l.active = 1 AND l.endpoint_enabled = 1 \
                  AND l.expires_at > ? \
                ORDER BY o.next_attempt_at, o.created_at, o.id LIMIT ? \
             )",
        )
        .bind(claim_id.to_string())
        .bind(lease_until.timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(now_secs)
        .bind(now_micros)
        .bind(now_micros)
        .bind(now_secs)
        .bind(limit)
        .execute(&mut *transaction)
        .await?;
        let rows = sqlx::query(
            "SELECT o.community_id, o.id, o.claim_id, o.event_id, e.channel_id, \
                    o.author, o.installation_id, o.lease_generation, \
                    l.endpoint_grant, o.class, o.expires_at, o.attempts \
             FROM push_wake_outbox o \
             JOIN push_leases l \
               ON l.community_id = o.community_id AND l.author = o.author \
              AND l.installation_id = o.installation_id \
              AND l.generation = o.lease_generation \
              AND l.endpoint_hash = o.endpoint_hash \
             JOIN events e ON e.community_id = o.community_id \
              AND e.id = o.event_id AND e.deleted_at IS NULL \
             WHERE o.community_id = ? AND o.claim_id = ? AND o.state = 'sending'",
        )
        .bind(community.as_uuid().to_string())
        .bind(claim_id.to_string())
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        rows.into_iter().map(row_to_claimed_wake).collect()
    }

    /// Revalidate the current lease, event, and unexpired worker fence.
    pub async fn revalidate_push_wake(
        &self,
        community: CommunityId,
        id: uuid::Uuid,
        claim_id: uuid::Uuid,
    ) -> Result<RevalidateWakeOutcome> {
        let now_micros = Utc::now().timestamp_micros();
        let now_secs = Utc::now().timestamp();
        let row = sqlx::query(
            "SELECT o.community_id, o.id, o.claim_id, o.event_id, e.channel_id, \
                    o.author, o.installation_id, o.lease_generation, \
                    l.endpoint_grant, o.class, o.expires_at, o.attempts \
             FROM push_wake_outbox o \
             JOIN push_leases l \
               ON l.community_id = o.community_id AND l.author = o.author \
              AND l.installation_id = o.installation_id \
              AND l.generation = o.lease_generation \
              AND l.endpoint_hash = o.endpoint_hash \
             JOIN events e ON e.community_id = o.community_id \
              AND e.id = o.event_id AND e.deleted_at IS NULL \
             WHERE o.community_id = ? AND o.id = ? AND o.claim_id = ? \
               AND o.state = 'sending' AND o.lease_until >= ? \
               AND o.expires_at > ? AND l.active = 1 \
               AND l.endpoint_enabled = 1 AND l.expires_at > ?",
        )
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(claim_id.to_string())
        .bind(now_micros)
        .bind(now_secs)
        .bind(now_secs)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_claimed_wake)
            .transpose()?
            .map_or(Ok(RevalidateWakeOutcome::Suppressed), |wake| {
                Ok(RevalidateWakeOutcome::Deliver(Box::new(wake)))
            })
    }

    async fn transition_push_wake(
        &self,
        community: CommunityId,
        id: uuid::Uuid,
        claim_id: uuid::Uuid,
        state: &str,
        next_attempt_at: Option<chrono::DateTime<Utc>>,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE push_wake_outbox SET state = ?, \
                next_attempt_at = COALESCE(?, next_attempt_at), \
                claim_id = NULL, lease_until = NULL \
             WHERE community_id = ? AND id = ? AND claim_id = ? \
               AND state = 'sending'",
        )
        .bind(state)
        .bind(next_attempt_at.map(|next| next.timestamp_micros()))
        .bind(community.as_uuid().to_string())
        .bind(id.to_string())
        .bind(claim_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Mark a fenced wake delivered.
    pub async fn complete_push_wake(
        &self,
        community: CommunityId,
        id: uuid::Uuid,
        claim_id: uuid::Uuid,
    ) -> Result<bool> {
        self.transition_push_wake(community, id, claim_id, "delivered", None)
            .await
    }

    /// Release a fenced wake for retry.
    pub async fn retry_push_wake(
        &self,
        community: CommunityId,
        id: uuid::Uuid,
        claim_id: uuid::Uuid,
        next: chrono::DateTime<Utc>,
    ) -> Result<bool> {
        self.transition_push_wake(community, id, claim_id, "pending", Some(next))
            .await
    }

    /// Permanently fail one fenced wake.
    pub async fn fail_push_wake(
        &self,
        community: CommunityId,
        id: uuid::Uuid,
        claim_id: uuid::Uuid,
    ) -> Result<bool> {
        self.transition_push_wake(community, id, claim_id, "failed", None)
            .await
    }

    /// Disable an endpoint only when the supplied generation is current.
    pub async fn disable_push_endpoint(
        &self,
        community: CommunityId,
        author: &[u8],
        installation_id: &str,
        generation: i64,
    ) -> Result<bool> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "UPDATE push_leases SET endpoint_enabled = 0, updated_at = ? \
             WHERE community_id = ? AND author = ? AND installation_id = ? \
               AND generation = ? AND active = 1 AND endpoint_enabled = 1",
        )
        .bind(Utc::now().timestamp_micros())
        .bind(community.as_uuid().to_string())
        .bind(author)
        .bind(installation_id)
        .bind(generation)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Delete terminal or expired wakes outside the retention window.
    pub async fn prune_push_wake_outbox(
        &self,
        community: CommunityId,
        before: chrono::DateTime<Utc>,
    ) -> Result<u64> {
        let _writer = self.acquire_writer().await;
        let result = sqlx::query(
            "DELETE FROM push_wake_outbox AS o \
             WHERE o.community_id = ? AND o.created_at < ? \
               AND (o.state IN ('delivered', 'failed') OR o.expires_at <= ?) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM push_match_queue q \
                   WHERE q.community_id = o.community_id \
                     AND q.event_id = o.event_id \
               )",
        )
        .bind(community.as_uuid().to_string())
        .bind(before.timestamp_micros())
        .bind(Utc::now().timestamp())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Claim a due matcher batch from exactly one community.
    pub async fn claim_due_push_match_batch(
        &self,
        limit: i64,
        lease_until: chrono::DateTime<Utc>,
    ) -> Result<Option<ClaimedMatchBatch>> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut transaction =
            sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let now = Utc::now().timestamp_micros();
        let claim_id = uuid::Uuid::new_v4();
        let rows = sqlx::query(
            "UPDATE push_match_queue SET \
                state = 'matching', claim_id = ?, lease_until = ?, \
                attempts = attempts + 1 \
             WHERE rowid IN ( \
                SELECT q.rowid FROM push_match_queue q \
                WHERE q.community_id = ( \
                    SELECT community_id FROM push_match_queue \
                    WHERE attempts < ? AND next_attempt_at <= ? \
                      AND (state = 'pending' OR ( \
                          state = 'matching' AND lease_until < ? \
                      )) \
                    ORDER BY next_attempt_at, created_at LIMIT 1 \
                ) \
                  AND q.attempts < ? AND q.next_attempt_at <= ? \
                  AND (q.state = 'pending' OR ( \
                      q.state = 'matching' AND q.lease_until < ? \
                  )) \
                ORDER BY q.next_attempt_at, q.created_at LIMIT ? \
             ) \
             RETURNING community_id, event_id, attempts",
        )
        .bind(claim_id.to_string())
        .bind(lease_until.timestamp_micros())
        .bind(MAX_MATCH_ATTEMPTS)
        .bind(now)
        .bind(now)
        .bind(MAX_MATCH_ATTEMPTS)
        .bind(now)
        .bind(now)
        .bind(limit)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        drop(_writer);
        if rows.is_empty() {
            return Ok(None);
        }

        let community: String = rows[0].try_get("community_id")?;
        let community = uuid::Uuid::parse_str(&community).map_err(|error| {
            crate::DbError::InvalidData(format!("invalid match community UUID: {error}"))
        })?;
        let community = CommunityId::from_uuid(community);
        let mut attempts = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            attempts.insert(
                row.try_get::<Vec<u8>, _>("event_id")?,
                row.try_get::<i32, _>("attempts")?,
            );
        }
        let ids: Vec<&[u8]> = attempts.keys().map(Vec::as_slice).collect();
        let events = self.get_events_by_ids(community, &ids).await?;
        let mut jobs = Vec::with_capacity(events.len());
        for event in events {
            let attempt = attempts
                .remove(event.event.id.as_bytes().as_slice())
                .unwrap_or(1);
            jobs.push(BatchedMatch { event, attempt });
        }
        let gone: Vec<Vec<u8>> = attempts.into_keys().collect();
        if !gone.is_empty() {
            self.complete_push_match_batch(community, claim_id, &gone)
                .await?;
        }
        if jobs.is_empty() {
            return Ok(None);
        }
        Ok(Some(ClaimedMatchBatch {
            community,
            claim_id,
            jobs,
        }))
    }

    /// Complete fenced matcher jobs in one set-wise delete.
    pub async fn complete_push_match_batch(
        &self,
        community: CommunityId,
        claim_id: uuid::Uuid,
        event_ids: &[Vec<u8>],
    ) -> Result<u64> {
        if event_ids.is_empty() {
            return Ok(0);
        }
        let _writer = self.acquire_writer().await;
        let mut builder: QueryBuilder<sqlx::Sqlite> =
            QueryBuilder::new("DELETE FROM push_match_queue WHERE community_id = ");
        builder
            .push_bind(community.as_uuid().to_string())
            .push(" AND claim_id = ")
            .push_bind(claim_id.to_string())
            .push(" AND state = 'matching' AND event_id IN (");
        let mut separated = builder.separated(", ");
        for event_id in event_ids {
            separated.push_bind(event_id);
        }
        builder.push(")");
        Ok(builder.build().execute(&self.pool).await?.rows_affected())
    }

    /// Release fenced matcher jobs for retry in one set-wise update.
    pub async fn retry_push_match_batch(
        &self,
        community: CommunityId,
        claim_id: uuid::Uuid,
        event_ids: &[Vec<u8>],
        next: chrono::DateTime<Utc>,
    ) -> Result<u64> {
        if event_ids.is_empty() {
            return Ok(0);
        }
        let _writer = self.acquire_writer().await;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "UPDATE push_match_queue SET state = 'pending', claim_id = NULL, \
             lease_until = NULL, next_attempt_at = ",
        );
        builder
            .push_bind(next.timestamp_micros())
            .push(" WHERE community_id = ")
            .push_bind(community.as_uuid().to_string())
            .push(" AND claim_id = ")
            .push_bind(claim_id.to_string())
            .push(" AND state = 'matching' AND event_id IN (");
        let mut separated = builder.separated(", ");
        for event_id in event_ids {
            separated.push_bind(event_id);
        }
        builder.push(")");
        Ok(builder.build().execute(&self.pool).await?.rows_affected())
    }

    /// Delete matcher jobs whose retry budget is exhausted.
    pub async fn reap_exhausted_push_matches(&self) -> Result<u64> {
        let _writer = self.acquire_writer().await;
        let now = Utc::now().timestamp_micros();
        Ok(sqlx::query(
            "DELETE FROM push_match_queue WHERE attempts >= ? \
             AND (state = 'pending' OR (state = 'matching' AND lease_until < ?))",
        )
        .bind(MAX_MATCH_ATTEMPTS)
        .bind(now)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }
}
