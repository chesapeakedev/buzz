//! SQLite persistence for tenant-scoped, use-limited relay invites.

use buzz_core::invite::{
    encode_v2_code, hash_v2_code, MAX_INVITE_TTL_SECS, MAX_INVITE_USES, MIN_INVITE_TTL_SECS,
    V2_SECRET_LEN,
};
use chrono::{DateTime, Utc};
use sqlx::Row as _;
use uuid::Uuid;

use super::SqliteStore;
use crate::relay_invite::{ClaimOutcome, MintedInvite};
use crate::{CommunityId, DbError, Result};

const RETENTION_SWEEP_BATCH_SIZE: i64 = 1_000;

fn validate_inputs(ttl_secs: u64, max_uses: Option<i32>) -> Result<()> {
    if !(MIN_INVITE_TTL_SECS..=MAX_INVITE_TTL_SECS).contains(&ttl_secs) {
        return Err(DbError::InvalidData(format!(
            "ttl_secs must be between {MIN_INVITE_TTL_SECS} and {MAX_INVITE_TTL_SECS}"
        )));
    }
    if let Some(max_uses) = max_uses {
        if !(1..=MAX_INVITE_USES).contains(&max_uses) {
            return Err(DbError::InvalidData(format!(
                "max_uses must be between 1 and {MAX_INVITE_USES}"
            )));
        }
    }
    Ok(())
}

fn parse_timestamp(value: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or(DbError::InvalidTimestamp(value))
}

impl SqliteStore {
    /// Mint a v2 invite, storing only its token hash.
    pub async fn mint_relay_invite(
        &self,
        community: CommunityId,
        created_by: &str,
        ttl_secs: u64,
        max_uses: Option<i32>,
    ) -> Result<MintedInvite> {
        validate_inputs(ttl_secs, max_uses)?;
        let secret: [u8; V2_SECRET_LEN] = rand::random();
        let code = encode_v2_code(&secret);
        let token_hash = hash_v2_code(&code);
        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(ttl_secs as i64);
        let invite_id = Uuid::new_v4();
        let _writer = self.acquire_writer().await;
        sqlx::query(
            "INSERT INTO relay_invites \
             (community_id, id, token_hash, max_uses, expires_at, created_by, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(community.as_uuid().to_string())
        .bind(invite_id.to_string())
        .bind(token_hash.as_slice())
        .bind(max_uses)
        .bind(expires_at.timestamp_micros())
        .bind(created_by)
        .bind(now.timestamp_micros())
        .execute(&self.pool)
        .await?;
        Ok(MintedInvite {
            code,
            expires_at,
            max_uses,
            uses_remaining: max_uses,
            invite_id,
        })
    }

    /// Delete one bounded batch of expired invites.
    pub async fn reap_expired_relay_invites(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let _writer = self.acquire_writer().await;
        Ok(sqlx::query(
            "DELETE FROM relay_invites WHERE id IN (\
                 SELECT id FROM relay_invites WHERE expires_at < ?\
                 ORDER BY expires_at LIMIT ?\
             )",
        )
        .bind(cutoff.timestamp_micros())
        .bind(RETENTION_SWEEP_BATCH_SIZE)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Atomically claim an invite, membership row, policy evidence, and use count.
    pub async fn claim_relay_invite(
        &self,
        community: CommunityId,
        token_hash: &[u8; 32],
        claimer_pubkey: &str,
        policy_version: Option<&str>,
    ) -> Result<ClaimOutcome> {
        let _writer = self.acquire_writer().await;
        let mut connection = self.pool.acquire().await?;
        let mut tx = sqlx::Connection::begin_with(&mut *connection, "BEGIN IMMEDIATE").await?;
        let community_id = community.as_uuid().to_string();
        let row = sqlx::query(
            "SELECT id, max_uses, use_count, expires_at FROM relay_invites \
             WHERE community_id = ? AND token_hash = ?",
        )
        .bind(&community_id)
        .bind(token_hash.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Invalid);
        };
        let invite_id = Uuid::parse_str(row.try_get::<String, _>("id")?.as_str())
            .map_err(|error| DbError::InvalidData(format!("invite id: {error}")))?;
        let max_uses: Option<i32> = row.try_get("max_uses")?;
        let use_count: i32 = row.try_get("use_count")?;
        let expires_at = parse_timestamp(row.try_get("expires_at")?)?;
        let remaining = || max_uses.map(|limit| limit - use_count);
        if expires_at <= Utc::now() {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Expired);
        }

        let existing =
            sqlx::query("SELECT 1 FROM relay_members WHERE community_id = ? AND pubkey = ?")
                .bind(&community_id)
                .bind(claimer_pubkey)
                .fetch_optional(&mut *tx)
                .await?
                .is_some();
        if existing {
            if let Some(version) = policy_version {
                sqlx::query(
                    "INSERT INTO join_policy_acceptances \
                     (community_id, pubkey, policy_version, accepted_at) VALUES (?, ?, ?, ?) \
                     ON CONFLICT DO NOTHING",
                )
                .bind(&community_id)
                .bind(claimer_pubkey)
                .bind(version)
                .bind(Utc::now().timestamp_micros())
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            return Ok(ClaimOutcome::AlreadyMember {
                use_count,
                uses_remaining: remaining(),
            });
        }
        if max_uses.is_some_and(|limit| use_count >= limit) {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Exhausted);
        }

        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "INSERT INTO relay_members \
             (community_id, pubkey, role, added_by, created_at, updated_at) \
             VALUES (?, ?, 'member', 'invite', ?, ?)",
        )
        .bind(&community_id)
        .bind(claimer_pubkey)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if let Some(version) = policy_version {
            sqlx::query(
                "INSERT INTO join_policy_acceptances \
                 (community_id, pubkey, policy_version, accepted_at) VALUES (?, ?, ?, ?) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(&community_id)
            .bind(claimer_pubkey)
            .bind(version)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        let use_count = use_count + 1;
        sqlx::query("UPDATE relay_invites SET use_count = ? WHERE community_id = ? AND id = ?")
            .bind(use_count)
            .bind(&community_id)
            .bind(invite_id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(ClaimOutcome::Joined {
            use_count,
            uses_remaining: max_uses.map(|limit| limit - use_count),
        })
    }
}
