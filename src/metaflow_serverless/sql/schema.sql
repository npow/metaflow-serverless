-- Metaflow ephemeral service schema.
-- Matches the OSS metaflow-service (netflix/metaflow-service) table layout.

-- ---------------------------------------------------------------------------
-- flows_v3
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flows_v3 (
    flow_id       VARCHAR(255) NOT NULL,
    user_name     VARCHAR(255),
    ts_epoch      BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    tags          JSONB DEFAULT '[]',
    system_tags   JSONB DEFAULT '[]',
    CONSTRAINT flows_v3_pkey PRIMARY KEY (flow_id)
);

CREATE INDEX IF NOT EXISTS flows_v3_tags_gin
    ON flows_v3 USING GIN ((tags || system_tags));

-- ---------------------------------------------------------------------------
-- runs_v3
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS runs_v3 (
    flow_id           VARCHAR(255) NOT NULL,
    run_number        BIGSERIAL    NOT NULL,
    run_id            VARCHAR(255),
    user_name         VARCHAR(255),
    ts_epoch          BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    last_heartbeat_ts BIGINT,
    tags              JSONB DEFAULT '[]',
    system_tags       JSONB DEFAULT '[]',
    CONSTRAINT runs_v3_pkey            PRIMARY KEY (flow_id, run_number),
    CONSTRAINT runs_v3_flow_id_fkey    FOREIGN KEY (flow_id) REFERENCES flows_v3 (flow_id)
);

-- Unique run_id per flow (partial: only when run_id is not null)
CREATE UNIQUE INDEX IF NOT EXISTS runs_v3_flow_id_run_id_unique
    ON runs_v3 (flow_id, run_id)
    WHERE run_id IS NOT NULL;

-- GIN index for tag lookups
CREATE INDEX IF NOT EXISTS runs_v3_tags_gin
    ON runs_v3 USING GIN ((tags || system_tags));

-- Time-ordering indexes
CREATE INDEX IF NOT EXISTS runs_v3_ts_epoch_desc
    ON runs_v3 (ts_epoch DESC);

CREATE INDEX IF NOT EXISTS runs_v3_flow_id_ts_epoch_desc
    ON runs_v3 (flow_id, ts_epoch DESC);

-- ---------------------------------------------------------------------------
-- steps_v3
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS steps_v3 (
    flow_id     VARCHAR(255) NOT NULL,
    run_number  BIGINT       NOT NULL,
    run_id      VARCHAR(255),
    step_name   VARCHAR(255) NOT NULL,
    user_name   VARCHAR(255),
    ts_epoch    BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    tags        JSONB DEFAULT '[]',
    system_tags JSONB DEFAULT '[]',
    CONSTRAINT steps_v3_pkey PRIMARY KEY (flow_id, run_number, step_name),
    CONSTRAINT steps_v3_run_fkey FOREIGN KEY (flow_id, run_number)
        REFERENCES runs_v3 (flow_id, run_number)
);

-- ---------------------------------------------------------------------------
-- tasks_v3
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tasks_v3 (
    task_id           BIGSERIAL    NOT NULL,
    flow_id           VARCHAR(255) NOT NULL,
    run_number        BIGINT       NOT NULL,
    run_id            VARCHAR(255),
    step_name         VARCHAR(255) NOT NULL,
    task_name         VARCHAR(255) NOT NULL,
    user_name         VARCHAR(255),
    ts_epoch          BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    last_heartbeat_ts BIGINT,
    tags              JSONB DEFAULT '[]',
    system_tags       JSONB DEFAULT '[]',
    CONSTRAINT tasks_v3_pkey PRIMARY KEY (task_id),
    CONSTRAINT tasks_v3_step_fkey FOREIGN KEY (flow_id, run_number, step_name)
        REFERENCES steps_v3 (flow_id, run_number, step_name),
    CONSTRAINT tasks_v3_unique_name UNIQUE (flow_id, run_number, step_name, task_name)
);

-- Partial index for run_id-based task lookups
CREATE INDEX IF NOT EXISTS tasks_v3_flow_run_id_step_task
    ON tasks_v3 (flow_id, run_id, step_name, task_name)
    WHERE run_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- metadata_v3
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metadata_v3 (
    id          BIGSERIAL    NOT NULL,
    flow_id     VARCHAR(255) NOT NULL,
    run_number  BIGINT       NOT NULL,
    run_id      VARCHAR(255),
    step_name   VARCHAR(255) NOT NULL,
    task_id     BIGINT       NOT NULL,
    task_name   VARCHAR(255),
    field_name  VARCHAR(255) NOT NULL,
    value       TEXT         NOT NULL,
    type        VARCHAR(255),
    user_name   VARCHAR(255),
    ts_epoch    BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    tags        JSONB DEFAULT '[]',
    system_tags JSONB DEFAULT '[]',
    CONSTRAINT metadata_v3_pkey PRIMARY KEY (id, flow_id, run_number, step_name, task_id, field_name)
);

-- ---------------------------------------------------------------------------
-- artifact_v3
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS artifact_v3 (
    flow_id      VARCHAR(255) NOT NULL,
    run_number   BIGINT       NOT NULL,
    run_id       VARCHAR(255),
    step_name    VARCHAR(255) NOT NULL,
    task_id      BIGINT       NOT NULL,
    task_name    VARCHAR(255),
    name         VARCHAR(255) NOT NULL,
    location     VARCHAR(255),
    ds_type      VARCHAR(255),
    sha          VARCHAR(255),
    type         VARCHAR(255),
    content_type VARCHAR(255),
    attempt_id   SMALLINT     NOT NULL DEFAULT 0,
    user_name    VARCHAR(255),
    ts_epoch     BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    tags         JSONB DEFAULT '[]',
    system_tags  JSONB DEFAULT '[]',
    CONSTRAINT artifact_v3_pkey PRIMARY KEY (flow_id, run_number, step_name, task_id, attempt_id, name)
);

-- ---------------------------------------------------------------------------
-- pg_notify triggers (broadcasts on channel 'notify')
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION notify_on_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify(
        'notify',
        row_to_json(NEW)::text
    );
    RETURN NEW;
END;
$$;

-- metadata_v3 insert trigger
DROP TRIGGER IF EXISTS metadata_v3_notify ON metadata_v3;
CREATE TRIGGER metadata_v3_notify
    AFTER INSERT ON metadata_v3
    FOR EACH ROW
    EXECUTE FUNCTION notify_on_insert();

-- artifact_v3 insert trigger
DROP TRIGGER IF EXISTS artifact_v3_notify ON artifact_v3;
CREATE TRIGGER artifact_v3_notify
    AFTER INSERT ON artifact_v3
    FOR EACH ROW
    EXECUTE FUNCTION notify_on_insert();
