-- Fresh-install SQLite tamper-evident audit chains.
--
-- Each community owns an independent sequence and hash namespace. Timestamps
-- are UTC microseconds supplied by the application, matching the precision
-- hashed by buzz-audit.

CREATE TABLE audit_log (
    community_id TEXT NOT NULL REFERENCES communities(id),
    seq          INTEGER NOT NULL CHECK (seq > 0),
    hash         BLOB NOT NULL CHECK (length(hash) = 32),
    prev_hash    BLOB CHECK (prev_hash IS NULL OR length(prev_hash) = 32),
    action       TEXT NOT NULL,
    actor_pubkey BLOB CHECK (
                     actor_pubkey IS NULL OR length(actor_pubkey) = 32
                 ),
    object_id    TEXT,
    detail       TEXT NOT NULL CHECK (json_valid(detail)),
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (community_id, seq),
    UNIQUE (community_id, hash)
) STRICT;
