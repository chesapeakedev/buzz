//! SQLite usage rollups for relay metrics.

use chrono::Utc;
use sqlx::Row as _;
use uuid::Uuid;

use super::SqliteStore;
use crate::usage::{
    CommunityActiveChannels, CommunityActiveUsers, CommunityChannelCount, CommunityGitRepoCount,
    CommunityHost, CommunityMemberCount, CommunityMessageCount, CommunityUserCounts,
    CommunityWorkflowCount,
};
use crate::{DbError, Result};

fn parse_community(value: String) -> Result<Uuid> {
    Uuid::parse_str(&value)
        .map_err(|error| DbError::InvalidData(format!("usage community UUID: {error}")))
}

fn interval_cutoff_micros(interval: &'static str) -> Result<i64> {
    let mut parts = interval.split_ascii_whitespace();
    let amount = parts
        .next()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(|| DbError::InvalidData(format!("invalid usage interval: {interval}")))?;
    let unit_seconds = match (parts.next(), parts.next()) {
        (Some("minute" | "minutes"), None) => 60_i64,
        (Some("hour" | "hours"), None) => 60 * 60,
        (Some("day" | "days"), None) => 24 * 60 * 60,
        _ => {
            return Err(DbError::InvalidData(format!(
                "unsupported usage interval: {interval}"
            )));
        }
    };
    let window = amount
        .checked_mul(unit_seconds)
        .and_then(|seconds| seconds.checked_mul(1_000_000))
        .ok_or_else(|| DbError::InvalidData(format!("usage interval overflow: {interval}")))?;
    Utc::now()
        .timestamp_micros()
        .checked_sub(window)
        .ok_or_else(|| DbError::InvalidData(format!("usage interval underflow: {interval}")))
}

impl SqliteStore {
    /// Return the total number of configured communities.
    pub async fn usage_community_count(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT count(*) FROM communities")
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    /// Return active user counts grouped by community and identity type.
    pub async fn usage_user_counts(&self) -> Result<Vec<CommunityUserCounts>> {
        let rows = sqlx::query(
            "SELECT community_id, \
                SUM(CASE WHEN agent_owner_pubkey IS NULL THEN 1 ELSE 0 END) AS human, \
                SUM(CASE WHEN agent_owner_pubkey IS NOT NULL THEN 1 ELSE 0 END) AS agent \
             FROM users WHERE deactivated_at IS NULL GROUP BY community_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityUserCounts {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    human: row.try_get("human")?,
                    agent: row.try_get("agent")?,
                })
            })
            .collect()
    }

    /// Return non-deleted channel counts grouped by community and type.
    pub async fn usage_channel_counts(&self) -> Result<Vec<CommunityChannelCount>> {
        let rows = sqlx::query(
            "SELECT community_id, channel_type, count(*) AS count \
             FROM channels WHERE deleted_at IS NULL \
             GROUP BY community_id, channel_type",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityChannelCount {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    channel_type: row.try_get("channel_type")?,
                    count: row.try_get("count")?,
                })
            })
            .collect()
    }

    /// Return non-deleted kind-nine event counts per community.
    pub async fn usage_message_counts(&self) -> Result<Vec<CommunityMessageCount>> {
        let rows = sqlx::query(
            "SELECT community_id, count(*) AS count FROM events \
             WHERE kind = 9 AND deleted_at IS NULL GROUP BY community_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityMessageCount {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    count: row.try_get("count")?,
                })
            })
            .collect()
    }

    /// Return relay membership counts grouped by community and role.
    pub async fn usage_relay_member_counts(&self) -> Result<Vec<CommunityMemberCount>> {
        let rows = sqlx::query(
            "SELECT community_id, role, count(*) AS count FROM relay_members \
             GROUP BY community_id, role",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityMemberCount {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    role: row.try_get("role")?,
                    count: row.try_get("count")?,
                })
            })
            .collect()
    }

    /// Return workflow counts grouped by community and status.
    pub async fn usage_workflow_counts(&self) -> Result<Vec<CommunityWorkflowCount>> {
        let rows = sqlx::query(
            "SELECT community_id, status, count(*) AS count FROM workflows \
             GROUP BY community_id, status",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityWorkflowCount {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    status: row.try_get("status")?,
                    count: row.try_get("count")?,
                })
            })
            .collect()
    }

    /// Return registered git repository counts per community.
    pub async fn usage_git_repo_counts(&self) -> Result<Vec<CommunityGitRepoCount>> {
        let rows = sqlx::query(
            "SELECT community_id, count(*) AS count FROM git_repo_names \
             GROUP BY community_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityGitRepoCount {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    count: row.try_get("count")?,
                })
            })
            .collect()
    }

    /// Return distinct recent publishers grouped by identity type.
    pub async fn usage_active_user_counts(
        &self,
        interval: &'static str,
    ) -> Result<Vec<CommunityActiveUsers>> {
        let rows = sqlx::query(
            "SELECT e.community_id, \
                COUNT(DISTINCT CASE \
                    WHEN u.pubkey IS NOT NULL AND u.agent_owner_pubkey IS NULL \
                    THEN e.pubkey END) AS human, \
                COUNT(DISTINCT CASE \
                    WHEN u.pubkey IS NOT NULL AND u.agent_owner_pubkey IS NOT NULL \
                    THEN e.pubkey END) AS agent, \
                COUNT(DISTINCT CASE WHEN u.pubkey IS NULL THEN e.pubkey END) AS unknown \
             FROM events e LEFT JOIN users u \
               ON u.community_id = e.community_id AND u.pubkey = e.pubkey \
             WHERE e.created_at >= ? AND e.deleted_at IS NULL \
             GROUP BY e.community_id",
        )
        .bind(interval_cutoff_micros(interval)?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityActiveUsers {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    human: row.try_get("human")?,
                    agent: row.try_get("agent")?,
                    unknown: row.try_get("unknown")?,
                })
            })
            .collect()
    }

    /// Return distinct channels with recent non-deleted kind-nine messages.
    pub async fn usage_active_channel_counts(
        &self,
        interval: &'static str,
    ) -> Result<Vec<CommunityActiveChannels>> {
        let rows = sqlx::query(
            "SELECT community_id, count(DISTINCT channel_id) AS count \
             FROM events WHERE kind = 9 AND channel_id IS NOT NULL \
               AND created_at >= ? AND deleted_at IS NULL \
             GROUP BY community_id",
        )
        .bind(interval_cutoff_micros(interval)?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityActiveChannels {
                    community_id: parse_community(row.try_get("community_id")?)?,
                    count: row.try_get("count")?,
                })
            })
            .collect()
    }

    /// Return all community identifiers and canonical hosts.
    pub async fn usage_community_hosts(&self) -> Result<Vec<CommunityHost>> {
        let rows = sqlx::query("SELECT id, host FROM communities")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CommunityHost {
                    id: parse_community(row.try_get("id")?)?,
                    host: row.try_get("host")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::interval_cutoff_micros;

    #[test]
    fn usage_intervals_are_bounded_and_explicit() {
        assert!(interval_cutoff_micros("1 day").is_ok());
        assert!(interval_cutoff_micros("30 days").is_ok());
        assert!(interval_cutoff_micros("1 hour").is_ok());
        assert!(interval_cutoff_micros("-1 day").is_err());
        assert!(interval_cutoff_micros("1 month").is_err());
        assert!(interval_cutoff_micros("1 day; DROP TABLE events").is_err());
    }
}
