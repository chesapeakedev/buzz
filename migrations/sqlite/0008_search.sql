-- Fresh-install SQLite full-text index.
--
-- FTS5 uses the events rowid as an external-content key. Triggers update the
-- index in the same transaction as the signed event row, so search has no
-- asynchronous indexing window. Tombstones remain indexed but are filtered by
-- the canonical events row at query time, matching PostgreSQL's access shape.
--
-- Fresh PostgreSQL installations use the same positive allowlist. Keeping
-- non-public, encrypted, and control kinds out of the index is defense in
-- depth: a future query-filter regression cannot make their content searchable.
-- Kind values are frozen migration data and correspond to the registry in
-- buzz_core::kind. Additive migrations must update both backend indexes when
-- that allowlist changes.

CREATE VIRTUAL TABLE events_fts USING fts5(
    content,
    content = 'events',
    content_rowid = 'rowid',
    tokenize = 'unicode61'
);

CREATE TRIGGER events_fts_after_insert
AFTER INSERT ON events
WHEN new.kind IN (0, 9, 40002, 45001, 45003) BEGIN
    INSERT INTO events_fts(rowid, content)
    VALUES (new.rowid, new.content);
END;

CREATE TRIGGER events_fts_after_delete
AFTER DELETE ON events
WHEN old.kind IN (0, 9, 40002, 45001, 45003) BEGIN
    INSERT INTO events_fts(events_fts, rowid, content)
    VALUES ('delete', old.rowid, old.content);
END;

CREATE TRIGGER events_fts_after_searchable_update
AFTER UPDATE OF content, kind ON events
WHEN old.kind IN (0, 9, 40002, 45001, 45003)
  OR new.kind IN (0, 9, 40002, 45001, 45003) BEGIN
    INSERT INTO events_fts(events_fts, rowid, content)
    SELECT 'delete', old.rowid, old.content
    WHERE old.kind IN (0, 9, 40002, 45001, 45003);
    INSERT INTO events_fts(rowid, content)
    SELECT new.rowid, new.content
    WHERE new.kind IN (0, 9, 40002, 45001, 45003);
END;

INSERT INTO events_fts(rowid, content)
SELECT rowid, content
FROM events
WHERE kind IN (0, 9, 40002, 45001, 45003);
