-- Fresh-install SQLite reaction state.
--
-- The composite primary key preserves one active-or-removed row per
-- tenant/target/actor/emoji tuple. Source reaction event IDs are independently
-- unique within a tenant so signed deletion events resolve unambiguously.

CREATE TABLE reactions (
    community_id      TEXT NOT NULL REFERENCES communities(id),
    event_created_at  INTEGER NOT NULL,
    event_id           BLOB NOT NULL CHECK (length(event_id) = 32),
    pubkey             BLOB NOT NULL CHECK (length(pubkey) = 32),
    emoji              TEXT NOT NULL CHECK (length(emoji) BETWEEN 1 AND 64),
    created_at         INTEGER NOT NULL,
    removed_at         INTEGER,
    reaction_event_id  BLOB CHECK (
                           reaction_event_id IS NULL
                           OR length(reaction_event_id) = 32
                       ),
    PRIMARY KEY (
        community_id,
        event_created_at,
        event_id,
        pubkey,
        emoji
    )
) STRICT;

CREATE INDEX idx_reactions_event
    ON reactions (community_id, event_id, event_created_at);
CREATE INDEX idx_reactions_pubkey
    ON reactions (community_id, pubkey);
CREATE UNIQUE INDEX idx_reactions_source_event
    ON reactions (community_id, reaction_event_id)
    WHERE reaction_event_id IS NOT NULL;
