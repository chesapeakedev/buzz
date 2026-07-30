-- Fresh-install SQLite durable push wake outbox.

CREATE TABLE push_wake_outbox (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    id              TEXT NOT NULL,
    author          BLOB NOT NULL CHECK (length(author) = 32),
    installation_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL CHECK (lease_generation > 0),
    endpoint_hash   BLOB NOT NULL CHECK (length(endpoint_hash) = 32),
    event_id        BLOB NOT NULL CHECK (length(event_id) = 32),
    class           TEXT NOT NULL CHECK (
                        class IN (
                            'silent', 'default', 'time_sensitive', 'urgent'
                        )
                    ),
    expires_at      INTEGER NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'sending', 'delivered', 'failed')),
    attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at INTEGER NOT NULL,
    lease_until     INTEGER,
    claim_id        TEXT,
    created_at      INTEGER NOT NULL,
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, author, installation_id)
        REFERENCES push_leases (community_id, author, installation_id),
    UNIQUE (community_id, endpoint_hash, event_id)
) STRICT;

CREATE INDEX push_wake_outbox_due
    ON push_wake_outbox (community_id, next_attempt_at)
    WHERE state = 'pending';
CREATE INDEX push_wake_outbox_recovery
    ON push_wake_outbox (community_id, lease_until)
    WHERE state = 'sending';
