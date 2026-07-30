-- Fresh-install SQLite workflow definitions and execution runs.
--
-- Approval gates and scheduled-fire claims follow in later migrations once
-- these tenant-scoped parent rows exist.

CREATE TABLE workflows (
    community_id   TEXT NOT NULL REFERENCES communities(id),
    id             TEXT NOT NULL CHECK (
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
    name           TEXT NOT NULL,
    owner_pubkey   BLOB NOT NULL CHECK (length(owner_pubkey) = 32),
    channel_id     TEXT,
    definition     TEXT NOT NULL CHECK (json_valid(definition)),
    definition_hash BLOB NOT NULL CHECK (length(definition_hash) = 32),
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'disabled', 'archived')),
    enabled        INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, owner_pubkey)
        REFERENCES users (community_id, pubkey),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id)
) STRICT;

CREATE INDEX idx_workflows_channel_active
    ON workflows (community_id, channel_id, status, enabled);
CREATE INDEX idx_workflows_enabled
    ON workflows (enabled, status) WHERE enabled = 1;

CREATE TABLE workflow_runs (
    community_id    TEXT NOT NULL REFERENCES communities(id),
    id              TEXT NOT NULL CHECK (
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
    workflow_id     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (
                        status IN (
                            'pending', 'running', 'waiting_approval',
                            'completed', 'failed', 'cancelled'
                        )
                    ),
    trigger_event_id BLOB CHECK (
                         trigger_event_id IS NULL
                         OR length(trigger_event_id) = 32
                     ),
    current_step    INTEGER NOT NULL DEFAULT 0,
    execution_trace TEXT NOT NULL DEFAULT '[]' CHECK (
                         json_valid(execution_trace)
                     ),
    trigger_context TEXT CHECK (
                        trigger_context IS NULL
                        OR json_valid(trigger_context)
                    ),
    started_at      INTEGER,
    completed_at    INTEGER,
    error_message   TEXT,
    created_at      INTEGER NOT NULL,
    PRIMARY KEY (community_id, id),
    FOREIGN KEY (community_id, workflow_id)
        REFERENCES workflows (community_id, id) ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_workflow_runs_workflow
    ON workflow_runs (community_id, workflow_id);
CREATE INDEX idx_workflow_runs_status
    ON workflow_runs (community_id, status);
