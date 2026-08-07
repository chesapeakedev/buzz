-- Durable, tenant-scoped v2 invite records for the embedded backend.
-- Only the SHA-256 token hash is persisted; plaintext codes never reach disk.

CREATE TABLE relay_invites (
    community_id TEXT NOT NULL REFERENCES communities(id),
    id           TEXT NOT NULL,
    token_hash   BLOB NOT NULL CHECK (length(token_hash) = 32),
    role         TEXT NOT NULL DEFAULT 'member' CHECK (role = 'member'),
    max_uses     INTEGER CHECK (max_uses BETWEEN 1 AND 10000),
    use_count    INTEGER NOT NULL DEFAULT 0 CHECK (use_count >= 0),
    expires_at   INTEGER NOT NULL,
    created_by   TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (community_id, id),
    UNIQUE (community_id, token_hash),
    CHECK (max_uses IS NULL OR use_count <= max_uses)
) STRICT;

CREATE INDEX idx_relay_invites_expires
    ON relay_invites (expires_at);
