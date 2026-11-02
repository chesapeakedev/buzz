-- Fresh-install SQLite workflow approvals and durable scheduled-fire claims.
--
-- Approval tokens are SHA-256 hashes before they reach this schema. Schedule
-- claims use the authoritative schedule instant as part of their tenant-scoped
-- primary key so restarts and concurrent workers cannot duplicate a fire.

CREATE TABLE workflow_approvals (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    token           BLOB NOT NULL CHECK (length(token) = 32),
    workflow_id     TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    step_id         TEXT NOT NULL,
    step_index      INTEGER NOT NULL,
    approver_spec   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'granted', 'denied', 'expired')),
    approver_pubkey BLOB CHECK (
                        approver_pubkey IS NULL
                        OR length(approver_pubkey) = 32
                    ),
    note            TEXT,
    granted_at      INTEGER,
    denied_at       INTEGER,
    expires_at      INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    PRIMARY KEY (community_id, token),
    FOREIGN KEY (community_id, workflow_id)
        REFERENCES workflows (community_id, id) ON DELETE CASCADE,
    FOREIGN KEY (community_id, run_id)
        REFERENCES workflow_runs (community_id, id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_workflow_approvals_workflow
    ON workflow_approvals (community_id, workflow_id);
CREATE INDEX idx_workflow_approvals_run
    ON workflow_approvals (community_id, run_id);
CREATE INDEX idx_workflow_approvals_status
    ON workflow_approvals (community_id, status);

CREATE TABLE scheduled_workflow_fires (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    workflow_id     TEXT NOT NULL,
    scheduled_for   INTEGER NOT NULL,
    claimed_at      INTEGER NOT NULL,
    workflow_run_id TEXT,
    PRIMARY KEY (community_id, workflow_id, scheduled_for),
    FOREIGN KEY (community_id, workflow_id)
        REFERENCES workflows (community_id, id) ON DELETE CASCADE,
    FOREIGN KEY (community_id, workflow_run_id)
        REFERENCES workflow_runs (community_id, id) ON DELETE NO ACTION
) STRICT;

CREATE INDEX idx_scheduled_fires_claimed_at
    ON scheduled_workflow_fires (claimed_at);
