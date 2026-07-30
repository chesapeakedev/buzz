-- Fresh-install SQLite NIP-PL effective leases and durable match queue.
--
-- Embedded mode serializes activation and event insertion through one writer
-- gate. The trigger remains the crash-safe event-to-match boundary; activation
-- also backfills the short recovery window in the same transaction.

CREATE TABLE push_leases (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    author          BLOB NOT NULL CHECK (length(author) = 32),
    installation_id TEXT NOT NULL CHECK (
                        length(CAST(installation_id AS BLOB)) BETWEEN 1 AND 64
                    ),
    source_event_id BLOB NOT NULL CHECK (length(source_event_id) = 32),
    source_created_at INTEGER NOT NULL,
    generation      INTEGER NOT NULL CHECK (generation > 0),
    active          INTEGER NOT NULL CHECK (active IN (0, 1)),
    endpoint_enabled INTEGER NOT NULL DEFAULT 1 CHECK (endpoint_enabled IN (0, 1)),
    app_profile     TEXT,
    endpoint_hash   BLOB CHECK (
                        endpoint_hash IS NULL
                        OR length(endpoint_hash) = 32
                    ),
    endpoint_grant  TEXT,
    max_class       TEXT CHECK (
                        max_class IS NULL
                        OR max_class IN (
                            'silent', 'default', 'time_sensitive', 'urgent'
                        )
                    ),
    subscriptions   TEXT CHECK (
                        subscriptions IS NULL
                        OR json_valid(subscriptions)
                    ),
    expires_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (community_id, author, installation_id),
    UNIQUE (community_id, source_event_id),
    CHECK (
        (
            active = 1
            AND app_profile IS NOT NULL
            AND endpoint_hash IS NOT NULL
            AND endpoint_grant IS NOT NULL
            AND max_class IS NOT NULL
            AND subscriptions IS NOT NULL
        )
        OR (
            active = 0
            AND app_profile IS NULL
            AND endpoint_hash IS NULL
            AND endpoint_grant IS NULL
            AND max_class IS NULL
            AND subscriptions IS NULL
        )
    )
) STRICT;

CREATE UNIQUE INDEX push_leases_endpoint_unique
    ON push_leases (community_id, author, app_profile, endpoint_hash)
    WHERE active = 1;
CREATE INDEX push_leases_expiry
    ON push_leases (community_id, expires_at)
    WHERE active = 1;

CREATE TABLE push_match_queue (
    community_id  TEXT NOT NULL REFERENCES communities(id),
    event_id      BLOB NOT NULL CHECK (length(event_id) = 32),
    state         TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending', 'matching')),
    attempts      INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at INTEGER NOT NULL,
    lease_until   INTEGER,
    claim_id      TEXT,
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (community_id, event_id)
) STRICT;

CREATE INDEX push_match_queue_due
    ON push_match_queue (next_attempt_at, created_at)
    WHERE state = 'pending';
CREATE INDEX push_match_queue_recovery
    ON push_match_queue (lease_until)
    WHERE state = 'matching';

CREATE TRIGGER events_enqueue_push_match
AFTER INSERT ON events
WHEN NEW.kind IN (7, 9, 1059, 40007, 46010)
 AND EXISTS (
    SELECT 1
    FROM push_leases
    WHERE community_id = NEW.community_id
      AND active = 1
      AND endpoint_enabled = 1
      AND expires_at > unixepoch()
 )
BEGIN
    INSERT INTO push_match_queue (
        community_id, event_id, next_attempt_at, created_at
    ) VALUES (
        NEW.community_id, NEW.id, unixepoch('subsec') * 1000000,
        unixepoch('subsec') * 1000000
    )
    ON CONFLICT DO NOTHING;
END;
