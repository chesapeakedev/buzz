-- Durable ordering state for privacy-sensitive parameterized events.
--
-- Ordinary NIP-33 rows retain soft-deleted history. Conforming NIP-RS
-- read-state rows and Buzz mesh heartbeat rows hard-delete superseded payloads;
-- this compact watermark prevents a deleted signed payload from being replayed.

CREATE TABLE parameterized_event_watermarks (
    community_id TEXT NOT NULL REFERENCES communities(id),
    kind         INTEGER NOT NULL CHECK (kind BETWEEN 30000 AND 39999),
    pubkey       BLOB NOT NULL CHECK (length(pubkey) = 32),
    d_tag        TEXT NOT NULL CHECK (length(CAST(d_tag AS BLOB)) <= 1024),
    created_at   INTEGER NOT NULL,
    event_id     BLOB NOT NULL CHECK (length(event_id) = 32),
    PRIMARY KEY (community_id, kind, pubkey, d_tag)
) STRICT;

CREATE INDEX idx_event_mentions_community_event
    ON event_mentions (community_id, event_id);
