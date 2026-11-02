-- Fresh-install SQLite event-storage foundation.
--
-- PostgreSQL keeps its partitioned event tables and generated search vector.
-- Embedded SQLite uses one ordinary table; FTS5 is added by the later
-- search/feed slice. Timestamps are UTC microseconds supplied by the
-- application, and every tenant-owned key and index leads with community_id.

CREATE TABLE events (
    community_id TEXT NOT NULL REFERENCES communities(id),
    id           BLOB NOT NULL CHECK (length(id) = 32),
    pubkey       BLOB NOT NULL CHECK (length(pubkey) = 32),
    created_at   INTEGER NOT NULL,
    kind         INTEGER NOT NULL CHECK (kind BETWEEN 0 AND 65535),
    tags         TEXT NOT NULL CHECK (json_valid(tags) AND json_type(tags) = 'array'),
    content      TEXT NOT NULL,
    sig          BLOB NOT NULL CHECK (length(sig) = 64),
    received_at  INTEGER NOT NULL,
    channel_id   TEXT CHECK (
                     channel_id IS NULL
                     OR (
                         length(channel_id) = 36
                         AND channel_id = lower(channel_id)
                         AND substr(channel_id, 9, 1) = '-'
                         AND substr(channel_id, 14, 1) = '-'
                         AND substr(channel_id, 19, 1) = '-'
                         AND substr(channel_id, 24, 1) = '-'
                         AND length(replace(channel_id, '-', '')) = 32
                         AND replace(channel_id, '-', '') NOT GLOB '*[^0-9a-f]*'
                         AND channel_id <> '00000000-0000-0000-0000-000000000000'
                     )
                 ),
    deleted_at   INTEGER,
    d_tag        TEXT CHECK (d_tag IS NULL OR length(CAST(d_tag AS BLOB)) <= 1024),
    not_before   INTEGER,
    delivered_at INTEGER,
    PRIMARY KEY (community_id, id)
) STRICT;

CREATE INDEX idx_events_community_channel_created
    ON events (community_id, channel_id, created_at DESC, id);
CREATE INDEX idx_events_community_pubkey_kind_created
    ON events (community_id, pubkey, kind, created_at DESC, id);
CREATE INDEX idx_events_community_kind_created
    ON events (community_id, kind, created_at DESC, id);
CREATE INDEX idx_events_community_deleted
    ON events (community_id, deleted_at);
CREATE INDEX idx_events_addressable
    ON events (community_id, kind, pubkey, channel_id, deleted_at);
CREATE INDEX idx_events_parameterized
    ON events (community_id, kind, pubkey, d_tag, created_at DESC, id)
    WHERE d_tag IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX idx_events_not_before
    ON events (community_id, not_before)
    WHERE not_before IS NOT NULL AND deleted_at IS NULL AND delivered_at IS NULL;

CREATE TABLE event_mentions (
    community_id     TEXT NOT NULL REFERENCES communities(id),
    pubkey_hex       TEXT NOT NULL CHECK (
                         length(pubkey_hex) = 64
                         AND pubkey_hex = lower(pubkey_hex)
                         AND pubkey_hex NOT GLOB '*[^0-9a-f]*'
                     ),
    event_id         BLOB NOT NULL CHECK (length(event_id) = 32),
    event_created_at INTEGER NOT NULL,
    channel_id       TEXT,
    event_kind       INTEGER CHECK (event_kind IS NULL OR event_kind BETWEEN 0 AND 65535),
    PRIMARY KEY (community_id, pubkey_hex, event_id),
    FOREIGN KEY (community_id, event_id)
        REFERENCES events (community_id, id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_event_mentions_pubkey_created
    ON event_mentions (community_id, pubkey_hex, event_created_at DESC);
CREATE INDEX idx_event_mentions_pubkey_kind_created
    ON event_mentions (community_id, pubkey_hex, event_kind, event_created_at DESC);
