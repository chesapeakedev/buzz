-- Add the version discriminator used by the shared audit hash contract to
-- databases created before the field was introduced.
ALTER TABLE audit_log
    ADD COLUMN hash_version INTEGER NOT NULL DEFAULT 1
    CHECK (hash_version IN (1, 2));
