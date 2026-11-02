-- Fresh-install SQLite thread metadata and materialized counters.
--
-- Metadata intentionally has no foreign key to events, matching PostgreSQL:
-- parent/root stubs may be created before their signed event receives its own
-- metadata row. Channel ownership remains tenant-scoped through the composite
-- foreign key.

CREATE TABLE thread_metadata (
    community_id            TEXT NOT NULL REFERENCES communities(id),
    event_created_at        INTEGER NOT NULL,
    event_id                BLOB NOT NULL CHECK (length(event_id) = 32),
    channel_id              TEXT NOT NULL,
    parent_event_id         BLOB CHECK (
                                parent_event_id IS NULL
                                OR length(parent_event_id) = 32
                            ),
    parent_event_created_at INTEGER,
    root_event_id           BLOB CHECK (
                                root_event_id IS NULL
                                OR length(root_event_id) = 32
                            ),
    root_event_created_at   INTEGER,
    depth                   INTEGER NOT NULL DEFAULT 0,
    reply_count             INTEGER NOT NULL DEFAULT 0,
    descendant_count        INTEGER NOT NULL DEFAULT 0,
    last_reply_at           INTEGER,
    broadcast               INTEGER NOT NULL DEFAULT 0
                            CHECK (broadcast IN (0, 1)),
    PRIMARY KEY (community_id, event_created_at, event_id),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id)
) STRICT;

CREATE INDEX idx_thread_metadata_parent
    ON thread_metadata (community_id, parent_event_id);
CREATE INDEX idx_thread_metadata_root
    ON thread_metadata (community_id, root_event_id);
CREATE INDEX idx_thread_metadata_channel_depth
    ON thread_metadata (community_id, channel_id, depth, event_created_at);
CREATE INDEX idx_thread_metadata_event_id
    ON thread_metadata (community_id, event_id);
