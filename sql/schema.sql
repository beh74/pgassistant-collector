CREATE TABLE pga_collection_run (
    run_id uuid PRIMARY KEY,
    parent_job_id uuid,
    target_id varchar(255) NOT NULL,
    target_name varchar(255),
    trigger_type varchar(32) NOT NULL,
    status varchar(16) NOT NULL,
    environment varchar(128),
    target_group varchar(128),
    db_host varchar(253),
    db_port integer CHECK (db_port IS NULL OR db_port BETWEEN 1 AND 65535),
    db_name varchar(63),
    db_user varchar(63),
    metadata jsonb NOT NULL DEFAULT '{}',
    jobs_requested text[] NOT NULL DEFAULT '{}',
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    error_message text
);

CREATE INDEX idx_pga_collection_run_target_time
    ON pga_collection_run (target_id, started_at DESC);
CREATE INDEX idx_pga_collection_run_database_identity
    ON pga_collection_run (db_name, db_host, db_port, db_user, started_at DESC);
CREATE INDEX idx_pga_collection_run_parent_job
    ON pga_collection_run (parent_job_id);

CREATE TABLE pga_collection_job_result (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    job_type varchar(64) NOT NULL,
    status varchar(16) NOT NULL,
    response_time_ms integer,
    error_message text,
    payload_summary jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, job_type)
);

CREATE INDEX idx_pga_collection_job_result_run
    ON pga_collection_job_result (run_id);

CREATE TABLE pga_collection_payload (
    id bigint GENERATED ALWAYS AS IDENTITY,
    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    target_id varchar(255) NOT NULL,
    job_type varchar(64) NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE INDEX idx_pga_collection_payload_target_time
    ON pga_collection_payload (target_id, collected_at DESC);
CREATE INDEX idx_pga_collection_payload_run_job
    ON pga_collection_payload (run_id, job_type);
CREATE INDEX idx_pga_collection_payload_gin
    ON pga_collection_payload USING gin (raw_payload);

CREATE TABLE pga_ranked_query_snapshot (
    id bigint GENERATED ALWAYS AS IDENTITY,
    run_id uuid NOT NULL REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    target_id varchar(255) NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    rank_position integer NOT NULL,
    queryid text,
    priority_score numeric,
    priority_level varchar(32),
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
    signals jsonb NOT NULL DEFAULT '[]',
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE INDEX idx_pga_ranked_query_target_time
    ON pga_ranked_query_snapshot (target_id, collected_at DESC);
CREATE INDEX idx_pga_ranked_query_queryid_time
    ON pga_ranked_query_snapshot (queryid, collected_at DESC);
CREATE INDEX idx_pga_ranked_query_run_rank
    ON pga_ranked_query_snapshot (run_id, rank_position);

CREATE TABLE pga_executive_plan_snapshot (
    run_id uuid PRIMARY KEY REFERENCES pga_collection_run(run_id) ON DELETE CASCADE,
    target_id varchar(255) NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    database_name varchar(63),
    status varchar(16) NOT NULL,
    recommendations_collected integer NOT NULL DEFAULT 0,
    recommendations_after_deduplication integer NOT NULL DEFAULT 0,
    task_count integer NOT NULL DEFAULT 0,
    phase_count integer NOT NULL DEFAULT 0,
    dev_task_count integer NOT NULL DEFAULT 0,
    ops_task_count integer NOT NULL DEFAULT 0,
    dev_ops_task_count integer NOT NULL DEFAULT 0,
    advisor_errors jsonb NOT NULL DEFAULT '[]',
    summary jsonb NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_pga_executive_plan_target_time
    ON pga_executive_plan_snapshot (target_id, collected_at DESC);

CREATE TABLE pga_executive_plan_phase_snapshot (
    run_id uuid NOT NULL REFERENCES pga_executive_plan_snapshot(run_id) ON DELETE CASCADE,
    phase_number integer NOT NULL,
    phase_order integer NOT NULL,
    name text NOT NULL,
    rationale text,
    team varchar(16),
    teams jsonb NOT NULL DEFAULT '[]',
    task_count integer NOT NULL DEFAULT 0,
    sql_count integer NOT NULL DEFAULT 0,
    requires_restart boolean NOT NULL DEFAULT false,
    requires_maintenance_window boolean NOT NULL DEFAULT false,
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (run_id, phase_number)
);

CREATE TABLE pga_executive_plan_task_snapshot (
    run_id uuid NOT NULL,
    task_id varchar(64) NOT NULL,
    target_id varchar(255) NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    phase_number integer NOT NULL,
    task_order integer NOT NULL,
    title text NOT NULL,
    team varchar(16),
    priority varchar(16),
    score numeric,
    workstream varchar(64),
    scope_name text,
    recommendation_count integer NOT NULL DEFAULT 0,
    sql_count integer NOT NULL DEFAULT 0,
    query_ids jsonb NOT NULL DEFAULT '[]',
    sources jsonb NOT NULL DEFAULT '[]',
    requires_restart boolean NOT NULL DEFAULT false,
    requires_maintenance_window boolean NOT NULL DEFAULT false,
    raw_payload jsonb NOT NULL,
    PRIMARY KEY (run_id, task_id),
    FOREIGN KEY (run_id, phase_number)
        REFERENCES pga_executive_plan_phase_snapshot(run_id, phase_number)
        ON DELETE CASCADE
);

CREATE INDEX idx_pga_executive_plan_task_target_time
    ON pga_executive_plan_task_snapshot (target_id, collected_at DESC);
CREATE INDEX idx_pga_executive_plan_task_phase
    ON pga_executive_plan_task_snapshot (run_id, phase_number, task_order);

CREATE TABLE pga_executive_plan_recommendation_snapshot (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL,
    task_id varchar(64) NOT NULL,
    target_id varchar(255) NOT NULL,
    collected_at timestamptz NOT NULL DEFAULT now(),
    recommendation_order integer NOT NULL,
    finding_fingerprint char(64) NOT NULL,
    action_fingerprint char(64) NOT NULL,
    source varchar(64),
    sources jsonb NOT NULL DEFAULT '[]',
    advisor_id text,
    planning_rule varchar(128),
    category_id varchar(64),
    action_type varchar(64),
    team varchar(16),
    priority varchar(16),
    impact smallint CHECK (impact IS NULL OR impact BETWEEN 0 AND 100),
    confidence smallint CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 100),
    effort smallint CHECK (effort IS NULL OR effort BETWEEN 0 AND 100),
    scope varchar(32),
    scope_name text,
    urgency varchar(32),
    risk_level varchar(32),
    database_name varchar(63),
    schema_name text,
    table_name text,
    object_name text,
    title text NOT NULL,
    description text,
    recommendation_sql text,
    query_ids jsonb NOT NULL DEFAULT '[]',
    evidence jsonb NOT NULL DEFAULT '[]',
    requires_lock boolean NOT NULL DEFAULT false,
    requires_restart boolean NOT NULL DEFAULT false,
    requires_maintenance_window boolean NOT NULL DEFAULT false,
    raw_payload jsonb NOT NULL,
    FOREIGN KEY (run_id, task_id)
        REFERENCES pga_executive_plan_task_snapshot(run_id, task_id)
        ON DELETE CASCADE,
    UNIQUE (run_id, task_id, recommendation_order)
);

CREATE INDEX idx_pga_executive_plan_recommendation_target_time
    ON pga_executive_plan_recommendation_snapshot (target_id, collected_at DESC);
CREATE INDEX idx_pga_executive_plan_recommendation_finding_time
    ON pga_executive_plan_recommendation_snapshot
       (target_id, finding_fingerprint, collected_at DESC);
CREATE INDEX idx_pga_executive_plan_recommendation_action_time
    ON pga_executive_plan_recommendation_snapshot
       (target_id, action_fingerprint, collected_at DESC);
CREATE INDEX idx_pga_executive_plan_recommendation_team_priority
    ON pga_executive_plan_recommendation_snapshot
       (target_id, team, priority, collected_at DESC);

CREATE TABLE pga_partition_registry (
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
        'pga_collection_payload',
        'pga_ranked_query_snapshot'
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
