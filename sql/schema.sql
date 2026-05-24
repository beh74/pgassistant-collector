CREATE TABLE IF NOT EXISTS pga_collection_run (
    run_id uuid PRIMARY KEY,
    parent_job_id uuid,
    target_id text NOT NULL,
    trigger_type text NOT NULL,
    status text NOT NULL,
    environment text,
    target_group text,
    metadata jsonb NOT NULL DEFAULT '{}',
    jobs_requested text[] NOT NULL DEFAULT '{}',
    started_at timestamptz,
    finished_at timestamptz,
    error_message text
);

CREATE INDEX IF NOT EXISTS idx_pga_collection_run_target_time
    ON pga_collection_run (target_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_collection_run_parent_job
    ON pga_collection_run (parent_job_id);

CREATE TABLE IF NOT EXISTS pga_collection_job_result (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    job_type text NOT NULL,
    status text NOT NULL,
    response_time_ms integer,
    error_message text,
    payload_summary jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pga_collection_job_result_run
    ON pga_collection_job_result (run_id);

CREATE TABLE IF NOT EXISTS pga_collection_raw_payload (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    target_id text NOT NULL,
    job_type text NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    raw_payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pga_collection_raw_payload_target_time
    ON pga_collection_raw_payload (target_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_collection_raw_payload_gin
    ON pga_collection_raw_payload USING gin (raw_payload);

CREATE TABLE IF NOT EXISTS pga_ranked_query_snapshot (
    id bigserial PRIMARY KEY,

    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id),
    target_id text NOT NULL,

    collected_at timestamptz NOT NULL DEFAULT now(),

    rank_position integer NOT NULL,
    queryid text,

    priority_score numeric,
    priority_level text,
    reason text,

    calls bigint,
    rows bigint,
    rows_per_call numeric,

    total_exec_time_ms numeric,
    mean_exec_time_ms numeric,
    min_exec_time_ms numeric,
    max_exec_time_ms numeric,
    stddev_exec_time_ms numeric,

    share_calls numeric,
    share_total_time numeric,
    share_io numeric,

    cache_hit_ratio numeric,
    cache_miss_share numeric,

    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_written bigint,

    total_blks_read bigint,
    total_blks_written bigint,

    temp_blks_read bigint,
    temp_blks_written bigint,

    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_written bigint,

    wal_bytes numeric,
    wal_records bigint,
    wal_fpi bigint,

    query text,

    signals jsonb,
    raw_payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pga_ranked_query_target_time
    ON pga_ranked_query_snapshot (target_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_ranked_query_queryid_time
    ON pga_ranked_query_snapshot (queryid, collected_at DESC);

DROP TABLE IF EXISTS pga_global_advisor_snapshot;

CREATE TABLE IF NOT EXISTS pga_global_advisor_snapshot (
    id bigserial PRIMARY KEY,

    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id),
    target_id text NOT NULL,

    collected_at timestamptz NOT NULL DEFAULT now(),

    rank_position integer,

    recommendation_id text,
    title text,
    label text,
    description text,
    recommendation_note text,
    why_it_matters text,
    expected_benefit text,
    fix_strategy text,
    improvement_sql text,

    advisor_group text,
    category_id text,
    outcome_id text,
    source text,

    priority text,
    risk_level text,
    action_type text,
    action_safety text,

    confidence numeric,
    impact numeric,
    effort numeric,
    estimated_rows bigint,

    can_auto_apply boolean,
    can_generate_sql boolean,
    manual_review_required boolean,
    requires_lock boolean,
    requires_maintenance_window boolean,

    object_type text,
    object_id bigint,
    object_name text,

    schema_name text,
    schema_id bigint,
    table_name text,
    table_id bigint,
    column_name text,
    index_name text,

    query_id text,

    tags jsonb,

    finding_fingerprint text NOT NULL,
    raw_payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pga_global_advisor_target_time
    ON pga_global_advisor_snapshot (target_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_global_advisor_fingerprint_time
    ON pga_global_advisor_snapshot (finding_fingerprint, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_global_advisor_raw_payload_gin
    ON pga_global_advisor_snapshot USING gin (raw_payload);
