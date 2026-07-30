-- Content-addressed blob metadata for embedded media and git storage.
--
-- The filesystem media/git backends store immutable blob bytes on disk under
-- `/data/objects`; this migration owns the matching metadata in SQLite so a
-- single row is the atomic publication gate for a blob write, replacing the
-- sidecar-JSON approach used by the S3-backed implementation. Object keys stay
-- wire-compatible with S3 so a future migration tool can copy objects between
-- filesystem and S3 by transferring key/content pairs without rewriting content.
--
-- UUIDs are canonical lowercase text, binary identifiers are BLOBs, and
-- timestamps are UTC microseconds supplied by the application. Every tenant
-- key leads with community_id, mirroring the rest of the SQLite schema.

CREATE TABLE media_objects (
    community_id     TEXT NOT NULL REFERENCES communities(id),
    -- Bare SHA-256 hex digest of the stored blob (the content-addressed key
    -- suffix). Two communities may reference the same bytes through distinct
    -- rows; the (community_id, sha256) pair is the read-authorization boundary.
    sha256           TEXT NOT NULL CHECK (
                         length(sha256) = 64
                         AND sha256 = lower(sha256)
                         AND sha256 NOT GLOB '*[^0-9a-f]*'
                     ),
    mime_type        TEXT NOT NULL CHECK (length(mime_type) BETWEEN 1 AND 255),
    size             INTEGER NOT NULL CHECK (size >= 0),
    -- Optional derived metadata, mirroring `buzz_media::storage::BlobMeta`.
    ext              TEXT CHECK (ext IS NULL OR length(ext) BETWEEN 1 AND 64),
    dim              TEXT CHECK (dim IS NULL OR length(dim) <= 32),
    blurhash         TEXT CHECK (blurhash IS NULL OR length(blurhash) <= 255),
    thumb_url        TEXT,
    duration_secs    REAL CHECK (duration_secs IS NULL OR duration_secs >= 0),
    -- Uploader provenance. `uploader_pubkey` is the 32-byte nostr pubkey.
    uploader_pubkey  BLOB CHECK (
                         uploader_pubkey IS NULL
                         OR length(uploader_pubkey) = 32
                     ),
    uploaded_at      INTEGER NOT NULL,
    PRIMARY KEY (community_id, sha256)
) STRICT;

CREATE INDEX idx_media_objects_uploaded
    ON media_objects (community_id, uploaded_at DESC);

-- Git repository CAS pointers. The pointer body itself is an immutable object
-- on the filesystem (`objects/git/pointers/<community>/<owner>/<repo>/pointer`);
-- this row records the manifest digest the pointer currently resolves to, the
-- CAS version token (ETag equivalent), and provenance so the metadata row is
-- the atomic publish gate for a pointer swap. The deployment-wide exclusive
-- lock makes process-local serialization of per-repository CAS sufficient.
CREATE TABLE git_pointers (
    community_id     TEXT NOT NULL REFERENCES communities(id),
    owner            TEXT NOT NULL CHECK (length(owner) BETWEEN 1 AND 255),
    repo             TEXT NOT NULL CHECK (length(repo) BETWEEN 1 AND 255),
    -- Manifest digest (hex SHA-256) the pointer currently resolves to.
    content_digest   TEXT NOT NULL CHECK (
                         length(content_digest) = 64
                         AND content_digest = lower(content_digest)
                         AND content_digest NOT GLOB '*[^0-9a-f]*'
                     ),
    size             INTEGER NOT NULL CHECK (size >= 0),
    -- Opaque CAS version token returned by the previous successful swap; used
    -- as the precondition for the next compare-and-swap. NULL on first push.
    etag             TEXT,
    uploader_pubkey  BLOB CHECK (
                         uploader_pubkey IS NULL
                         OR length(uploader_pubkey) = 32
                     ),
    updated_at       INTEGER NOT NULL,
    PRIMARY KEY (community_id, owner, repo)
) STRICT;