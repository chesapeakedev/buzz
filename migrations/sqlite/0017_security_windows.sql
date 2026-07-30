-- Durable embedded security windows.
--
-- Replay scopes are either a community UUID or an explicit operator scope.
-- Rate keys retain the existing backend-neutral key format, including
-- operator-global IP windows. Both tables are intentionally deployment-global
-- security fences rather than tenant-visible domain data.

CREATE TABLE security_replay_claims (
    scope      TEXT NOT NULL CHECK (length(scope) BETWEEN 1 AND 255),
    event_id   BLOB NOT NULL CHECK (length(event_id) = 32),
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (scope, event_id)
) STRICT;

CREATE INDEX security_replay_claims_expiry
    ON security_replay_claims (expires_at);

CREATE TABLE security_rate_windows (
    window_key TEXT PRIMARY KEY CHECK (
                   length(CAST(window_key AS BLOB)) BETWEEN 1 AND 1024
               ),
    count      INTEGER NOT NULL CHECK (count > 0),
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE INDEX security_rate_windows_expiry
    ON security_rate_windows (expires_at);
