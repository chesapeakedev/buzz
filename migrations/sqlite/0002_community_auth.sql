-- Fresh-install SQLite community and authentication baseline.
--
-- UUIDs are canonical lowercase text, binary identifiers remain BLOBs, JSON is
-- validated text, booleans are 0/1 integers, and timestamps are UTC
-- microseconds supplied by the application. Every tenant-owned key and foreign
-- key leads with community_id.

CREATE TABLE communities (
    id          TEXT NOT NULL PRIMARY KEY
                CHECK (
                    length(id) = 36
                    AND id = lower(id)
                    AND substr(id, 9, 1) = '-'
                    AND substr(id, 14, 1) = '-'
                    AND substr(id, 19, 1) = '-'
                    AND substr(id, 24, 1) = '-'
                    AND length(replace(id, '-', '')) = 32
                    AND replace(id, '-', '') NOT GLOB '*[^0-9a-f]*'
                    AND id <> '00000000-0000-0000-0000-000000000000'
                ),
    host        TEXT NOT NULL CHECK (length(host) BETWEEN 1 AND 255),
    signing_key BLOB,
    created_at  INTEGER NOT NULL,
    icon        TEXT,
    archived_at INTEGER
) STRICT;

CREATE UNIQUE INDEX idx_communities_host ON communities (lower(host));

CREATE TABLE users (
    community_id       TEXT NOT NULL REFERENCES communities(id),
    pubkey             BLOB NOT NULL CHECK (length(pubkey) = 32),
    nip05_handle       TEXT CHECK (nip05_handle IS NULL OR length(nip05_handle) <= 255),
    display_name       TEXT CHECK (display_name IS NULL OR length(display_name) <= 255),
    avatar_url         TEXT,
    about              TEXT,
    agent_type         TEXT CHECK (agent_type IS NULL OR length(agent_type) <= 255),
    capabilities       TEXT CHECK (capabilities IS NULL OR json_valid(capabilities)),
    okta_user_id       TEXT CHECK (okta_user_id IS NULL OR length(okta_user_id) <= 255),
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    deactivated_at     INTEGER,
    metadata_event_id  BLOB,
    agent_owner_pubkey BLOB CHECK (
        agent_owner_pubkey IS NULL OR length(agent_owner_pubkey) = 32
    ),
    channel_add_policy TEXT NOT NULL DEFAULT 'anyone'
                       CHECK (channel_add_policy IN ('anyone', 'owner_only', 'nobody')),
    PRIMARY KEY (community_id, pubkey),
    FOREIGN KEY (community_id, agent_owner_pubkey)
        REFERENCES users (community_id, pubkey) ON DELETE SET NULL
) STRICT;

CREATE UNIQUE INDEX idx_users_nip05
    ON users (community_id, lower(nip05_handle))
    WHERE nip05_handle IS NOT NULL;
CREATE UNIQUE INDEX idx_users_okta
    ON users (community_id, okta_user_id)
    WHERE okta_user_id IS NOT NULL;

CREATE TABLE api_tokens (
    community_id        TEXT NOT NULL REFERENCES communities(id),
    id                  TEXT NOT NULL CHECK (
                            length(id) = 36
                            AND id = lower(id)
                            AND substr(id, 9, 1) = '-'
                            AND substr(id, 14, 1) = '-'
                            AND substr(id, 19, 1) = '-'
                            AND substr(id, 24, 1) = '-'
                            AND length(replace(id, '-', '')) = 32
                            AND replace(id, '-', '') NOT GLOB '*[^0-9a-f]*'
                            AND id <> '00000000-0000-0000-0000-000000000000'
                        ),
    token_hash          BLOB NOT NULL CHECK (length(token_hash) = 32),
    owner_pubkey        BLOB NOT NULL CHECK (length(owner_pubkey) = 32),
    name                TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 255),
    scopes              TEXT NOT NULL CHECK (json_valid(scopes)),
    channel_ids         TEXT CHECK (channel_ids IS NULL OR json_valid(channel_ids)),
    created_at          INTEGER NOT NULL,
    expires_at          INTEGER,
    last_used_at        INTEGER,
    revoked_at          INTEGER,
    revoked_by          BLOB,
    created_by_self_mint INTEGER NOT NULL DEFAULT 0
                         CHECK (created_by_self_mint IN (0, 1)),
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, owner_pubkey)
        REFERENCES users (community_id, pubkey)
) STRICT;

CREATE UNIQUE INDEX idx_api_tokens_hash
    ON api_tokens (community_id, token_hash);
CREATE INDEX idx_api_tokens_owner
    ON api_tokens (community_id, owner_pubkey, created_at DESC);

CREATE TABLE pubkey_allowlist (
    community_id TEXT NOT NULL REFERENCES communities(id),
    pubkey        BLOB NOT NULL CHECK (length(pubkey) = 32),
    added_by      BLOB,
    added_at      INTEGER NOT NULL,
    note          TEXT,
    PRIMARY KEY (community_id, pubkey)
) STRICT;

CREATE TABLE relay_members (
    community_id TEXT NOT NULL REFERENCES communities(id),
    pubkey        TEXT NOT NULL CHECK (
                      length(pubkey) = 64
                      AND pubkey = lower(pubkey)
                      AND pubkey NOT GLOB '*[^0-9a-f]*'
                  ),
    role          TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'member')),
    added_by      TEXT CHECK (
                      added_by IS NULL
                      OR added_by = 'invite'
                      OR (
                          length(added_by) = 64
                          AND added_by = lower(added_by)
                          AND added_by NOT GLOB '*[^0-9a-f]*'
                      )
                  ),
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (community_id, pubkey)
) STRICT;

CREATE INDEX idx_relay_members_role
    ON relay_members (community_id, role);

CREATE TABLE join_policy_acceptances (
    community_id  TEXT NOT NULL,
    pubkey         TEXT NOT NULL,
    policy_version TEXT NOT NULL CHECK (
                       length(policy_version) = 64
                       AND policy_version = lower(policy_version)
                       AND policy_version NOT GLOB '*[^0-9a-f]*'
                   ),
    accepted_at    INTEGER NOT NULL,
    PRIMARY KEY (community_id, pubkey, policy_version),
    FOREIGN KEY (community_id, pubkey)
        REFERENCES relay_members (community_id, pubkey) ON DELETE CASCADE
) STRICT;

CREATE TABLE archived_identities (
    community_id     TEXT NOT NULL REFERENCES communities(id),
    pubkey           TEXT NOT NULL CHECK (
                         length(pubkey) = 64
                         AND pubkey = lower(pubkey)
                         AND pubkey NOT GLOB '*[^0-9a-f]*'
                     ),
    consent_path     TEXT NOT NULL CHECK (consent_path IN ('self', 'owner', 'admin')),
    actor            TEXT NOT NULL,
    reason           TEXT,
    replaced_by      TEXT,
    request_event_id TEXT NOT NULL,
    archived_at      INTEGER NOT NULL,
    PRIMARY KEY (community_id, pubkey)
) STRICT;
