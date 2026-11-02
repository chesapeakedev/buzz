-- Fresh-install SQLite channel and membership baseline.
--
-- Channel UUIDs are tenant-local wire identifiers, so every key and foreign
-- key leads with community_id. The process-wide SQLite writer gate serializes
-- membership authorization and last-owner checks that PostgreSQL protects with
-- per-channel advisory locks.

CREATE TABLE channels (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    id              TEXT NOT NULL CHECK (
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
    name            TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 255),
    channel_type    TEXT NOT NULL DEFAULT 'stream'
                    CHECK (channel_type IN ('stream', 'forum', 'dm', 'workflow')),
    visibility      TEXT NOT NULL DEFAULT 'open'
                    CHECK (visibility IN ('open', 'private')),
    description     TEXT,
    canvas          TEXT,
    created_by      BLOB NOT NULL CHECK (length(created_by) = 32),
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    archived_at     INTEGER,
    deleted_at      INTEGER,
    nip29_group_id  TEXT CHECK (
                        nip29_group_id IS NULL OR length(nip29_group_id) <= 255
                    ),
    topic_required  INTEGER NOT NULL DEFAULT 0
                    CHECK (topic_required IN (0, 1)),
    max_members     INTEGER,
    topic           TEXT,
    topic_set_by    BLOB CHECK (
                        topic_set_by IS NULL OR length(topic_set_by) = 32
                    ),
    topic_set_at    INTEGER,
    purpose         TEXT,
    purpose_set_by  BLOB CHECK (
                        purpose_set_by IS NULL OR length(purpose_set_by) = 32
                    ),
    purpose_set_at  INTEGER,
    participant_hash BLOB,
    ttl_seconds     INTEGER,
    ttl_deadline    INTEGER,
    PRIMARY KEY (community_id, id)
) STRICT;

CREATE UNIQUE INDEX idx_channels_nip29_group
    ON channels (community_id, nip29_group_id)
    WHERE nip29_group_id IS NOT NULL;
CREATE UNIQUE INDEX idx_channels_dm_hash
    ON channels (community_id, participant_hash)
    WHERE participant_hash IS NOT NULL;
CREATE INDEX idx_channels_community_type
    ON channels (community_id, channel_type);
CREATE INDEX idx_channels_community_visibility
    ON channels (community_id, visibility);
CREATE INDEX idx_channels_created_by
    ON channels (community_id, created_by);
CREATE INDEX idx_channels_ttl_expiry
    ON channels (ttl_deadline)
    WHERE ttl_seconds IS NOT NULL AND archived_at IS NULL AND deleted_at IS NULL;

CREATE TABLE channel_members (
    community_id TEXT NOT NULL REFERENCES communities(id),
    channel_id   TEXT NOT NULL,
    pubkey       BLOB NOT NULL CHECK (length(pubkey) = 32),
    role         TEXT NOT NULL DEFAULT 'member'
                 CHECK (role IN ('owner', 'admin', 'member', 'guest', 'bot')),
    joined_at    INTEGER NOT NULL,
    invited_by   BLOB CHECK (invited_by IS NULL OR length(invited_by) = 32),
    removed_at   INTEGER,
    removed_by   BLOB CHECK (removed_by IS NULL OR length(removed_by) = 32),
    hidden_at    INTEGER,
    PRIMARY KEY (community_id, channel_id, pubkey),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_channel_members_pubkey
    ON channel_members (community_id, pubkey)
    WHERE removed_at IS NULL;
