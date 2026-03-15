-- Metaflow ephemeral service stored procedures.
-- All procedures match the semantics of the OSS metaflow-service API layer.

-- ---------------------------------------------------------------------------
-- Helper: resolve run_number from a run_id string.
-- If the string is purely numeric it is treated as a run_number directly;
-- otherwise it is matched against the run_id column.
-- Returns NULL if not found.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION _resolve_run_number(p_flow_id text, p_run_id text)
RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_run_number BIGINT;
BEGIN
    IF p_run_id ~ '^[0-9]+$' THEN
        v_run_number := p_run_id::BIGINT;
    ELSE
        SELECT run_number
          INTO v_run_number
          FROM runs_v3
         WHERE flow_id  = p_flow_id
           AND run_id   = p_run_id
         LIMIT 1;
    END IF;
    RETURN v_run_number;
END;
$$;

-- ---------------------------------------------------------------------------
-- Helper: reserve one request against the current UTC month quota bucket.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION enforce_monthly_quota(
    p_scope          TEXT,
    p_request_limit  BIGINT,
    p_egress_limit   BIGINT
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_scope         TEXT := COALESCE(NULLIF(p_scope, ''), 'global');
    v_period_start  DATE := DATE_TRUNC('month', TIMEZONE('utc', NOW()))::DATE;
    v_row           service_quota_monthly%ROWTYPE;
BEGIN
    IF p_request_limit IS NULL OR p_request_limit <= 0 THEN
        RAISE EXCEPTION 'enforce_monthly_quota: p_request_limit must be > 0';
    END IF;
    IF p_egress_limit IS NULL OR p_egress_limit <= 0 THEN
        RAISE EXCEPTION 'enforce_monthly_quota: p_egress_limit must be > 0';
    END IF;

    INSERT INTO service_quota_monthly (scope, period_start)
    VALUES (v_scope, v_period_start)
    ON CONFLICT (scope, period_start) DO NOTHING;

    SELECT *
      INTO v_row
      FROM service_quota_monthly
     WHERE scope = v_scope
       AND period_start = v_period_start
     FOR UPDATE;

    IF v_row.request_count >= p_request_limit THEN
        RETURN json_build_object(
            'allowed', false,
            'reason', 'request_limit_exceeded',
            'scope', v_scope,
            'period_start', v_period_start,
            'request_count', v_row.request_count,
            'request_limit', p_request_limit,
            'egress_bytes', v_row.egress_bytes,
            'egress_limit', p_egress_limit
        );
    END IF;

    IF v_row.egress_bytes >= p_egress_limit THEN
        RETURN json_build_object(
            'allowed', false,
            'reason', 'egress_limit_exceeded',
            'scope', v_scope,
            'period_start', v_period_start,
            'request_count', v_row.request_count,
            'request_limit', p_request_limit,
            'egress_bytes', v_row.egress_bytes,
            'egress_limit', p_egress_limit
        );
    END IF;

    UPDATE service_quota_monthly
       SET request_count = request_count + 1,
           updated_at = NOW()
     WHERE scope = v_scope
       AND period_start = v_period_start
     RETURNING *
      INTO v_row;

    RETURN json_build_object(
        'allowed', true,
        'scope', v_scope,
        'period_start', v_period_start,
        'request_count', v_row.request_count,
        'request_limit', p_request_limit,
        'egress_bytes', v_row.egress_bytes,
        'egress_limit', p_egress_limit
    );
END;
$$;

-- ---------------------------------------------------------------------------
-- Helper: record response egress bytes against the current UTC month bucket.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION record_monthly_egress(
    p_scope         TEXT,
    p_bytes         BIGINT
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_scope         TEXT := COALESCE(NULLIF(p_scope, ''), 'global');
    v_period_start  DATE := DATE_TRUNC('month', TIMEZONE('utc', NOW()))::DATE;
    v_row           service_quota_monthly%ROWTYPE;
BEGIN
    IF p_bytes IS NULL OR p_bytes < 0 THEN
        RAISE EXCEPTION 'record_monthly_egress: p_bytes must be >= 0';
    END IF;

    INSERT INTO service_quota_monthly (scope, period_start)
    VALUES (v_scope, v_period_start)
    ON CONFLICT (scope, period_start) DO NOTHING;

    UPDATE service_quota_monthly
       SET egress_bytes = egress_bytes + p_bytes,
           updated_at = NOW()
     WHERE scope = v_scope
       AND period_start = v_period_start
     RETURNING *
      INTO v_row;

    RETURN json_build_object(
        'scope', v_scope,
        'period_start', v_period_start,
        'request_count', v_row.request_count,
        'egress_bytes', v_row.egress_bytes
    );
END;
$$;

-- ---------------------------------------------------------------------------
-- 1. heartbeat_run
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION heartbeat_run(
    p_flow_id TEXT,
    p_run_id  TEXT,
    p_ts      BIGINT
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_number BIGINT;
BEGIN
    IF p_flow_id IS NULL OR p_run_id IS NULL OR p_ts IS NULL THEN
        RAISE EXCEPTION 'heartbeat_run: p_flow_id, p_run_id, and p_ts must not be NULL';
    END IF;

    v_run_number := _resolve_run_number(p_flow_id, p_run_id);

    IF v_run_number IS NULL THEN
        RAISE EXCEPTION 'heartbeat_run: run not found for flow=% run_id=%', p_flow_id, p_run_id;
    END IF;

    UPDATE runs_v3
       SET last_heartbeat_ts = p_ts
     WHERE flow_id    = p_flow_id
       AND run_number = v_run_number
       AND (last_heartbeat_ts IS NULL OR last_heartbeat_ts <= p_ts - 10);

    RETURN '{"wait_time_in_seconds": 10}'::JSON;
END;
$$;

-- ---------------------------------------------------------------------------
-- 2. heartbeat_task
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION heartbeat_task(
    p_flow_id   TEXT,
    p_run_id    TEXT,
    p_step_name TEXT,
    p_task_id   TEXT,
    p_ts        BIGINT
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_number BIGINT;
    v_task_id    BIGINT;
BEGIN
    IF p_flow_id IS NULL OR p_run_id IS NULL OR p_step_name IS NULL
       OR p_task_id IS NULL OR p_ts IS NULL THEN
        RAISE EXCEPTION 'heartbeat_task: all parameters must not be NULL';
    END IF;

    v_run_number := _resolve_run_number(p_flow_id, p_run_id);

    IF v_run_number IS NULL THEN
        RAISE EXCEPTION 'heartbeat_task: run not found for flow=% run_id=%', p_flow_id, p_run_id;
    END IF;

    -- Resolve task: numeric string → task_id column, otherwise → task_name column
    IF p_task_id ~ '^[0-9]+$' THEN
        v_task_id := p_task_id::BIGINT;
    ELSE
        SELECT task_id
          INTO v_task_id
          FROM tasks_v3
         WHERE flow_id    = p_flow_id
           AND run_number = v_run_number
           AND step_name  = p_step_name
           AND task_name  = p_task_id
         LIMIT 1;
    END IF;

    IF v_task_id IS NULL THEN
        RAISE EXCEPTION 'heartbeat_task: task not found for flow=% run=% step=% task=%',
            p_flow_id, p_run_id, p_step_name, p_task_id;
    END IF;

    UPDATE tasks_v3
       SET last_heartbeat_ts = p_ts
     WHERE flow_id    = p_flow_id
       AND run_number = v_run_number
       AND step_name  = p_step_name
       AND task_id    = v_task_id
       AND (last_heartbeat_ts IS NULL OR last_heartbeat_ts <= p_ts - 10);

    RETURN '{"wait_time_in_seconds": 10}'::JSON;
END;
$$;

-- ---------------------------------------------------------------------------
-- 3. mutate_run_tags
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION mutate_run_tags(
    p_flow_id        TEXT,
    p_run_id         TEXT,
    p_tags_to_add    TEXT[],
    p_tags_to_remove TEXT[]
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_number  BIGINT;
    v_tags        TEXT[];
    v_system_tags TEXT[];
    v_next_tags   TEXT[];
    v_overlap     TEXT[];
BEGIN
    IF p_flow_id IS NULL OR p_run_id IS NULL THEN
        RAISE EXCEPTION 'mutate_run_tags: p_flow_id and p_run_id must not be NULL';
    END IF;

    -- Use SERIALIZABLE isolation for this transaction block.
    -- (The caller is responsible for wrapping in a transaction; we set the
    --  level here to be safe when called outside an explicit transaction.)
    SET LOCAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    v_run_number := _resolve_run_number(p_flow_id, p_run_id);

    IF v_run_number IS NULL THEN
        RAISE EXCEPTION 'mutate_run_tags: run not found for flow=% run_id=%', p_flow_id, p_run_id;
    END IF;

    -- Fetch current tags as plain text arrays
    SELECT
        ARRAY(SELECT jsonb_array_elements_text(tags)),
        ARRAY(SELECT jsonb_array_elements_text(system_tags))
    INTO v_tags, v_system_tags
    FROM runs_v3
    WHERE flow_id    = p_flow_id
      AND run_number = v_run_number;

    -- Validate: tags_to_remove must not overlap with system_tags
    SELECT ARRAY(
        SELECT unnest(COALESCE(p_tags_to_remove, ARRAY[]::TEXT[]))
        INTERSECT
        SELECT unnest(COALESCE(v_system_tags, ARRAY[]::TEXT[]))
    ) INTO v_overlap;

    IF array_length(v_overlap, 1) > 0 THEN
        RAISE EXCEPTION
            'mutate_run_tags: cannot remove system tags: %',
            array_to_string(v_overlap, ', ')
            USING ERRCODE = '23514';
    END IF;

    -- Compute next_tags:
    --   start with current tags
    --   remove p_tags_to_remove
    --   add p_tags_to_add that are not in system_tags
    SELECT ARRAY(
        SELECT unnest(
            -- (current - to_remove) union (to_add - system_tags)
            ARRAY(
                SELECT unnest(COALESCE(v_tags, ARRAY[]::TEXT[]))
                EXCEPT
                SELECT unnest(COALESCE(p_tags_to_remove, ARRAY[]::TEXT[]))
            )
            ||
            ARRAY(
                SELECT unnest(COALESCE(p_tags_to_add, ARRAY[]::TEXT[]))
                EXCEPT
                SELECT unnest(COALESCE(v_system_tags, ARRAY[]::TEXT[]))
            )
        )
        -- deduplicate
        GROUP BY 1
        ORDER BY 1
    ) INTO v_next_tags;

    -- Normalise current tags for comparison (sorted, deduplicated)
    SELECT ARRAY(
        SELECT unnest(COALESCE(v_tags, ARRAY[]::TEXT[]))
        GROUP BY 1
        ORDER BY 1
    ) INTO v_tags;

    IF v_next_tags IS NOT DISTINCT FROM v_tags THEN
        RETURN '{"status": "unchanged"}'::JSON;
    END IF;

    UPDATE runs_v3
       SET tags = to_jsonb(v_next_tags)
     WHERE flow_id    = p_flow_id
       AND run_number = v_run_number;

    RETURN '{"status": "ok"}'::JSON;
END;
$$;

-- ---------------------------------------------------------------------------
-- 4. get_artifacts_latest
-- Returns artifacts for the task at the maximum attempt_id per artifact name.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_artifacts_latest(
    p_flow_id   TEXT,
    p_run_id    TEXT,
    p_step_name TEXT,
    p_task_id   TEXT
)
RETURNS SETOF artifact_v3
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_run_number BIGINT;
    v_task_id    BIGINT;
BEGIN
    IF p_flow_id IS NULL OR p_run_id IS NULL OR p_step_name IS NULL OR p_task_id IS NULL THEN
        RAISE EXCEPTION 'get_artifacts_latest: all parameters must not be NULL';
    END IF;

    v_run_number := _resolve_run_number(p_flow_id, p_run_id);

    IF v_run_number IS NULL THEN
        RAISE EXCEPTION 'get_artifacts_latest: run not found for flow=% run_id=%', p_flow_id, p_run_id;
    END IF;

    IF p_task_id ~ '^[0-9]+$' THEN
        v_task_id := p_task_id::BIGINT;
    ELSE
        SELECT task_id
          INTO v_task_id
          FROM tasks_v3
         WHERE flow_id    = p_flow_id
           AND run_number = v_run_number
           AND step_name  = p_step_name
           AND task_name  = p_task_id
         LIMIT 1;
    END IF;

    IF v_task_id IS NULL THEN
        RAISE EXCEPTION 'get_artifacts_latest: task not found for flow=% run=% step=% task=%',
            p_flow_id, p_run_id, p_step_name, p_task_id;
    END IF;

    RETURN QUERY
        SELECT a.*
          FROM artifact_v3 a
          JOIN (
              SELECT name, MAX(attempt_id) AS max_attempt
                FROM artifact_v3
               WHERE flow_id    = p_flow_id
                 AND run_number = v_run_number
                 AND step_name  = p_step_name
                 AND task_id    = v_task_id
               GROUP BY name
          ) latest
            ON  a.name       = latest.name
            AND a.attempt_id = latest.max_attempt
         WHERE a.flow_id    = p_flow_id
           AND a.run_number = v_run_number
           AND a.step_name  = p_step_name
           AND a.task_id    = v_task_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- 5. filter_tasks_by_metadata
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION filter_tasks_by_metadata(
    p_flow_id   TEXT,
    p_run_id    TEXT,
    p_step_name TEXT,
    p_field_name TEXT,
    p_pattern   TEXT
)
RETURNS TABLE(
    flow_id    TEXT,
    run_number BIGINT,
    run_id     TEXT,
    step_name  TEXT,
    task_id    BIGINT,
    task_name  TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_run_number BIGINT;
BEGIN
    IF p_flow_id IS NULL OR p_run_id IS NULL OR p_step_name IS NULL
       OR p_field_name IS NULL OR p_pattern IS NULL THEN
        RAISE EXCEPTION 'filter_tasks_by_metadata: all parameters must not be NULL';
    END IF;

    v_run_number := _resolve_run_number(p_flow_id, p_run_id);

    IF v_run_number IS NULL THEN
        RAISE EXCEPTION 'filter_tasks_by_metadata: run not found for flow=% run_id=%', p_flow_id, p_run_id;
    END IF;

    RETURN QUERY
        SELECT DISTINCT
               m.flow_id::TEXT,
               m.run_number,
               m.run_id::TEXT,
               m.step_name::TEXT,
               m.task_id,
               m.task_name::TEXT
          FROM metadata_v3 m
         WHERE m.flow_id    = p_flow_id
           AND m.run_number = v_run_number
           AND m.step_name  = p_step_name
           AND m.field_name = p_field_name
           AND regexp_match(m.value, p_pattern) IS NOT NULL;
END;
$$;

-- ---------------------------------------------------------------------------
-- 6. create_flow
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION create_flow(
    p_flow_id     TEXT,
    p_user_name   TEXT,
    p_tags        JSONB,
    p_system_tags JSONB
)
RETURNS flows_v3
LANGUAGE plpgsql
AS $$
DECLARE
    v_row flows_v3;
BEGIN
    IF p_flow_id IS NULL THEN
        RAISE EXCEPTION 'create_flow: p_flow_id must not be NULL';
    END IF;

    INSERT INTO flows_v3 (flow_id, user_name, ts_epoch, tags, system_tags)
    VALUES (
        p_flow_id,
        p_user_name,
        (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
        COALESCE(p_tags,        '[]'::JSONB),
        COALESCE(p_system_tags, '[]'::JSONB)
    )
    ON CONFLICT (flow_id) DO NOTHING;

    SELECT * INTO v_row FROM flows_v3 WHERE flow_id = p_flow_id;
    RETURN v_row;
END;
$$;

-- ---------------------------------------------------------------------------
-- 7. create_run
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION create_run(
    p_flow_id     TEXT,
    p_run_id      TEXT,
    p_user_name   TEXT,
    p_ts_epoch    BIGINT,
    p_tags        JSONB,
    p_system_tags JSONB
)
RETURNS runs_v3
LANGUAGE plpgsql
AS $$
DECLARE
    v_row runs_v3;
BEGIN
    IF p_flow_id IS NULL THEN
        RAISE EXCEPTION 'create_run: p_flow_id must not be NULL';
    END IF;

    INSERT INTO runs_v3 (flow_id, run_id, user_name, ts_epoch, last_heartbeat_ts, tags, system_tags)
    VALUES (
        p_flow_id,
        p_run_id,
        p_user_name,
        COALESCE(p_ts_epoch, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT),
        COALESCE(p_ts_epoch, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT),
        COALESCE(p_tags,        '[]'::JSONB),
        COALESCE(p_system_tags, '[]'::JSONB)
    )
    RETURNING * INTO v_row;

    RETURN v_row;
END;
$$;

-- ---------------------------------------------------------------------------
-- 8. create_step
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION create_step(
    p_flow_id     TEXT,
    p_run_id      TEXT,
    p_step_name   TEXT,
    p_user_name   TEXT,
    p_ts_epoch    BIGINT,
    p_tags        JSONB,
    p_system_tags JSONB
)
RETURNS steps_v3
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_number BIGINT;
    v_row        steps_v3;
BEGIN
    IF p_flow_id IS NULL OR p_run_id IS NULL OR p_step_name IS NULL THEN
        RAISE EXCEPTION 'create_step: p_flow_id, p_run_id, and p_step_name must not be NULL';
    END IF;

    v_run_number := _resolve_run_number(p_flow_id, p_run_id);

    IF v_run_number IS NULL THEN
        RAISE EXCEPTION 'create_step: run not found for flow=% run_id=%', p_flow_id, p_run_id;
    END IF;

    -- Propagate run_id (string form) for denormalised storage
    INSERT INTO steps_v3 (flow_id, run_number, run_id, step_name, user_name, ts_epoch, tags, system_tags)
    VALUES (
        p_flow_id,
        v_run_number,
        CASE WHEN p_run_id ~ '^[0-9]+$' THEN NULL ELSE p_run_id END,
        p_step_name,
        p_user_name,
        COALESCE(p_ts_epoch, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT),
        COALESCE(p_tags,        '[]'::JSONB),
        COALESCE(p_system_tags, '[]'::JSONB)
    )
    RETURNING * INTO v_row;

    RETURN v_row;
END;
$$;

-- ---------------------------------------------------------------------------
-- 9. create_task
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION create_task(
    p_flow_id     TEXT,
    p_run_id      TEXT,
    p_step_name   TEXT,
    p_task_name   TEXT,
    p_user_name   TEXT,
    p_ts_epoch    BIGINT,
    p_tags        JSONB,
    p_system_tags JSONB
)
RETURNS tasks_v3
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_number BIGINT;
    v_row        tasks_v3;
BEGIN
    IF p_flow_id IS NULL OR p_run_id IS NULL OR p_step_name IS NULL OR p_task_name IS NULL THEN
        RAISE EXCEPTION 'create_task: p_flow_id, p_run_id, p_step_name, and p_task_name must not be NULL';
    END IF;

    v_run_number := _resolve_run_number(p_flow_id, p_run_id);

    IF v_run_number IS NULL THEN
        RAISE EXCEPTION 'create_task: run not found for flow=% run_id=%', p_flow_id, p_run_id;
    END IF;

    INSERT INTO tasks_v3 (
        flow_id, run_number, run_id, step_name, task_name,
        user_name, ts_epoch, last_heartbeat_ts, tags, system_tags
    )
    VALUES (
        p_flow_id,
        v_run_number,
        CASE WHEN p_run_id ~ '^[0-9]+$' THEN NULL ELSE p_run_id END,
        p_step_name,
        p_task_name,
        p_user_name,
        COALESCE(p_ts_epoch, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT),
        COALESCE(p_ts_epoch, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT),
        COALESCE(p_tags,        '[]'::JSONB),
        COALESCE(p_system_tags, '[]'::JSONB)
    )
    RETURNING * INTO v_row;

    RETURN v_row;
END;
$$;
