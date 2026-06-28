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
    id bigserial,
    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    job_type text NOT NULL,
    status text NOT NULL,
    response_time_ms integer,
    error_message text,
    payload_summary jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS idx_pga_collection_job_result_run
    ON pga_collection_job_result (run_id);

CREATE TABLE IF NOT EXISTS pga_collection_raw_payload (
    id bigserial,
    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    target_id text NOT NULL,
    job_type text NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE INDEX IF NOT EXISTS idx_pga_collection_raw_payload_target_time
    ON pga_collection_raw_payload (target_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_collection_raw_payload_gin
    ON pga_collection_raw_payload USING gin (raw_payload);

CREATE TABLE IF NOT EXISTS pga_ranked_query_snapshot (
    id bigserial,

    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
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
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE INDEX IF NOT EXISTS idx_pga_ranked_query_target_time
    ON pga_ranked_query_snapshot (target_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_ranked_query_queryid_time
    ON pga_ranked_query_snapshot (queryid, collected_at DESC);

CREATE TABLE IF NOT EXISTS pga_global_advisor_snapshot (
    id bigserial,

    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
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
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE INDEX IF NOT EXISTS idx_pga_global_advisor_target_time
    ON pga_global_advisor_snapshot (target_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_global_advisor_fingerprint_time
    ON pga_global_advisor_snapshot (finding_fingerprint, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_pga_global_advisor_raw_payload_gin
    ON pga_global_advisor_snapshot USING gin (raw_payload);

CREATE TABLE IF NOT EXISTS pga_partition_registry (
    parent_table regclass NOT NULL,
    partition_table regclass NOT NULL,
    range_start timestamptz NOT NULL,
    range_end timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_table, partition_table)
);

CREATE OR REPLACE FUNCTION pga_create_weekly_partitions(
    from_date date DEFAULT CURRENT_DATE,
    weeks_ahead integer DEFAULT 8,
    weeks_back integer DEFAULT 1
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    parent_table_name text;
    start_week date;
    range_start timestamptz;
    range_end timestamptz;
    partition_name text;
    week_offset integer;
BEGIN
    IF weeks_ahead < 0 OR weeks_back < 0 THEN
        RAISE EXCEPTION 'weeks_ahead and weeks_back must be non-negative';
    END IF;

    start_week := date_trunc('week', from_date)::date - (weeks_back * 7);

    FOREACH parent_table_name IN ARRAY ARRAY[
        'pga_collection_job_result',
        'pga_collection_raw_payload',
        'pga_ranked_query_snapshot',
        'pga_global_advisor_snapshot'
    ]
    LOOP
        FOR week_offset IN 0..(weeks_back + weeks_ahead)
        LOOP
            range_start := start_week + (week_offset * interval '1 week');
            range_end := range_start + interval '1 week';
            partition_name := parent_table_name || '_' || to_char(range_start, 'YYYYMMDD');

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                parent_table_name,
                range_start,
                range_end
            );

            INSERT INTO pga_partition_registry (
                parent_table,
                partition_table,
                range_start,
                range_end
            )
            VALUES (
                parent_table_name::regclass,
                partition_name::regclass,
                range_start,
                range_end
            )
            ON CONFLICT (parent_table, partition_table) DO UPDATE SET
                range_start = EXCLUDED.range_start,
                range_end = EXCLUDED.range_end;
        END LOOP;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pga_drop_partitions_older_than(
    retain_weeks integer
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    cutoff timestamptz;
    partition_record record;
    dropped_count integer := 0;
BEGIN
    IF retain_weeks < 1 THEN
        RAISE EXCEPTION 'retain_weeks must be greater than 0';
    END IF;

    cutoff := date_trunc('week', now()) - (retain_weeks * interval '1 week');

    FOR partition_record IN
        SELECT parent_table, partition_table
        FROM pga_partition_registry
        WHERE range_end <= cutoff
        ORDER BY range_end
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %s', partition_record.partition_table);

        DELETE FROM pga_partition_registry
        WHERE parent_table = partition_record.parent_table
          AND partition_table = partition_record.partition_table;

        dropped_count := dropped_count + 1;
    END LOOP;

    DELETE FROM pga_collection_run
    WHERE COALESCE(finished_at, started_at) < cutoff;

    RETURN dropped_count;
END;
$$;

SELECT pga_create_weekly_partitions();
