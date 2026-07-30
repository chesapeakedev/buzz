-- Per-community NIP-34 repository-name registry for fresh SQLite installs.
--
-- The composite primary key is the atomic name reservation boundary. Owner
-- lookup distinguishes idempotent re-announces from cross-owner collisions.

CREATE TABLE git_repo_names (
    community_id TEXT NOT NULL REFERENCES communities(id),
    repo_id      TEXT NOT NULL,
    owner_pubkey TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (community_id, repo_id)
) STRICT;

CREATE INDEX idx_git_repo_names_owner
    ON git_repo_names (community_id, owner_pubkey);
