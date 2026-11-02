-- Fresh-install SQLite community moderation schema.
--
-- Reports, restrictions, and moderation actions share the relational
-- consistency boundary with events and membership. UUIDs are canonical text,
-- binary identifiers are fixed-width BLOBs, and timestamps are UTC
-- microseconds supplied by the application.

CREATE TABLE moderation_actions (
    community_id      TEXT NOT NULL REFERENCES communities(id),
    id                TEXT NOT NULL CHECK (
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
    actor_pubkey      BLOB NOT NULL CHECK (length(actor_pubkey) = 32),
    action            TEXT NOT NULL CHECK (action IN (
                          'delete_message', 'kick', 'ban', 'unban',
                          'timeout', 'untimeout', 'dismiss_report', 'escalate',
                          'resolve:delete', 'resolve:kick', 'resolve:ban',
                          'resolve:timeout'
                      )),
    target_pubkey     BLOB CHECK (
                          target_pubkey IS NULL OR length(target_pubkey) = 32
                      ),
    target_event_id   BLOB CHECK (
                          target_event_id IS NULL OR length(target_event_id) = 32
                      ),
    channel_id        TEXT,
    reason_code       TEXT,
    public_reason     TEXT,
    private_reason    TEXT,
    matched_principal TEXT CHECK (
                          matched_principal IS NULL
                          OR matched_principal IN ('self', 'owner')
                      ),
    created_at        INTEGER NOT NULL,
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id)
) STRICT;

CREATE INDEX idx_moderation_actions_created
    ON moderation_actions (community_id, created_at DESC);
CREATE INDEX idx_moderation_actions_target_pubkey
    ON moderation_actions (community_id, target_pubkey)
    WHERE target_pubkey IS NOT NULL;

CREATE TABLE moderation_reports (
    community_id       TEXT NOT NULL REFERENCES communities(id),
    id                 TEXT NOT NULL CHECK (
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
    report_event_id    BLOB NOT NULL CHECK (length(report_event_id) = 32),
    reporter_pubkey    BLOB NOT NULL CHECK (length(reporter_pubkey) = 32),
    target_kind        TEXT NOT NULL CHECK (
                           target_kind IN ('event', 'pubkey', 'blob')
                       ),
    target_event_id    BLOB CHECK (
                           target_event_id IS NULL OR length(target_event_id) = 32
                       ),
    target_pubkey      BLOB CHECK (
                           target_pubkey IS NULL OR length(target_pubkey) = 32
                       ),
    target_blob_sha256 BLOB CHECK (
                           target_blob_sha256 IS NULL
                           OR length(target_blob_sha256) = 32
                       ),
    channel_id         TEXT,
    report_type        TEXT NOT NULL,
    note               TEXT,
    status             TEXT NOT NULL DEFAULT 'open'
                       CHECK (
                           status IN ('open', 'resolved', 'dismissed', 'escalated')
                       ),
    resolved_by        BLOB CHECK (
                           resolved_by IS NULL OR length(resolved_by) = 32
                       ),
    resolved_at        INTEGER,
    action_id          TEXT,
    created_at         INTEGER NOT NULL,
    PRIMARY KEY (community_id, id),
    UNIQUE (community_id, report_event_id),
    CHECK (
        (
            target_kind = 'event'
            AND target_event_id IS NOT NULL
            AND target_pubkey IS NULL
            AND target_blob_sha256 IS NULL
        ) OR (
            target_kind = 'pubkey'
            AND target_event_id IS NULL
            AND target_pubkey IS NOT NULL
            AND target_blob_sha256 IS NULL
        ) OR (
            target_kind = 'blob'
            AND target_event_id IS NULL
            AND target_pubkey IS NULL
            AND target_blob_sha256 IS NOT NULL
        )
    ),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id),
    FOREIGN KEY (community_id, action_id)
        REFERENCES moderation_actions (community_id, id)
) STRICT;

CREATE INDEX idx_moderation_reports_status
    ON moderation_reports (community_id, status, created_at DESC);
CREATE INDEX idx_moderation_reports_target_event
    ON moderation_reports (community_id, target_event_id)
    WHERE target_event_id IS NOT NULL;
CREATE INDEX idx_moderation_reports_target_pubkey
    ON moderation_reports (community_id, target_pubkey)
    WHERE target_pubkey IS NOT NULL;

CREATE TABLE community_bans (
    community_id   TEXT NOT NULL REFERENCES communities(id),
    pubkey         BLOB NOT NULL CHECK (length(pubkey) = 32),
    banned         INTEGER NOT NULL DEFAULT 0 CHECK (banned IN (0, 1)),
    ban_expires_at INTEGER,
    ban_reason     TEXT,
    muted_until    INTEGER,
    mute_reason    TEXT,
    actor_pubkey   BLOB NOT NULL CHECK (length(actor_pubkey) = 32),
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    PRIMARY KEY (community_id, pubkey)
) STRICT;
