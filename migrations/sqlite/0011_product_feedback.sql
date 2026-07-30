-- Deployment-global product-feedback inbox for fresh SQLite installs.
--
-- community_id records source provenance, but signed event ids are
-- deployment-wide idempotency keys because this is an operator-global inbox.

CREATE TABLE product_feedback (
    id               TEXT PRIMARY KEY CHECK (
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
    community_id     TEXT NOT NULL REFERENCES communities(id),
    event_id         BLOB NOT NULL UNIQUE CHECK (length(event_id) = 32),
    submitter_pubkey BLOB NOT NULL CHECK (length(submitter_pubkey) = 32),
    category         TEXT CHECK (
                         category IS NULL
                         OR category IN ('bug', 'praise', 'needs-work')
                     ),
    body             TEXT NOT NULL CHECK (length(trim(body)) > 0),
    tags             TEXT NOT NULL DEFAULT '[]' CHECK (
                         json_valid(tags) AND json_type(tags) = 'array'
                     ),
    event_created_at INTEGER NOT NULL,
    received_at      INTEGER NOT NULL
) STRICT;

CREATE INDEX idx_product_feedback_received
    ON product_feedback (received_at DESC, id);
CREATE INDEX idx_product_feedback_community_received
    ON product_feedback (community_id, received_at DESC, id);
