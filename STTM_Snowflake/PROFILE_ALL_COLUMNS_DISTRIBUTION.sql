CREATE OR REPLACE TABLE sep_column_distribution (
    -- Identification
    table_schema            VARCHAR         NOT NULL,
    table_name              VARCHAR         NOT NULL,
    column_name             VARCHAR         NOT NULL,

    -- Type & role inference
    inferred_data_type      VARCHAR,        -- NUMERIC | STRING | BOOLEAN | DATE | etc
    primary_role            VARCHAR,        -- ID | MEASURE | CODE | FLAG | TEXT
    secondary_roles         ARRAY,           -- e.g. ['FOREIGN_KEY','SURROGATE_KEY']

    -- Cardinality & completeness
    row_count               NUMBER,
    non_null_count          NUMBER,
    distinct_count          NUMBER,
    distinct_ratio          FLOAT,           -- distinct / non-null

    -- Distribution summaries
    top_values              VARIANT,         -- array of {value, pct}
    modal_value_pct         FLOAT,
    entropy_score           FLOAT,
    skewness                FLOAT,

    -- Numeric-only stats (NULL for non-numeric)
    min_value               FLOAT,
    max_value               FLOAT,
    mean_value              FLOAT,
    stddev_value            FLOAT,

    -- Temporal behavior
    temporal_density        VARCHAR,         -- e.g. "124.6 per day"

    -- Metadata
    profiling_run_id        VARCHAR,         -- allows grouping multiple runs
    profiling_mode          VARCHAR,         -- FULL | SAMPLE | INCREMENTAL
    created_at              TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP,

    -- Convenience
    CONSTRAINT pk_sep_column_distribution
        PRIMARY KEY (table_schema, table_name, column_name, created_at)
);


-- =====================================================================
-- STM UNPIVOT PROFILER (ZERO-PROCEDURE, METADATA-DRIVEN)
-- Author intent: Single-pass column profiling + role classification
-- Target platform: Snowflake
-- =====================================================================

-- =====================
-- CONFIGURATION
-- =====================
-- Change these three values to test on a new table

SET TARGET_SCHEMA = CURRENT_SCHEMA();
SET TARGET_TABLE  = 'PROCEDURE_FACT';
SET TEMPORAL_COLUMN = 'PROCEDURE_DATE';

-- Output table must already exist
-- Expected columns (minimum):
-- table_name, column_name, data_type, column_class,
-- primary_role, secondary_roles,
-- top_values, entropy_score, skewness, modal_value_pct,
-- mean_val, stddev_val, min_val, max_val,
-- temporal_density, created_at

-- =====================
-- BUILD UNPIVOT COLUMN LIST
-- =====================

DECLARE
    v_unpivot_list STRING;
    v_sql          STRING;
BEGIN

    SELECT
        LISTAGG(column_name, ',\n            ') 
            WITHIN GROUP (ORDER BY ordinal_position)
    INTO :v_unpivot_list
    FROM information_schema.columns
    WHERE table_schema = $TARGET_SCHEMA
      AND table_name   = $TARGET_TABLE
      AND data_type NOT IN ('VARIANT', 'ARRAY', 'OBJECT')
      AND column_name <> $TEMPORAL_COLUMN;

-- =====================
-- MAIN PROFILER QUERY
-- =====================

    v_sql := '
WITH column_metadata AS (
    SELECT
        column_name,
        data_type,
        CASE
            WHEN data_type IN (
                ''NUMBER'', ''DECIMAL'', ''NUMERIC'',
                ''FLOAT'', ''FLOAT4'', ''FLOAT8'',
                ''DOUBLE'', ''DOUBLE PRECISION'',
                ''INT'', ''INTEGER'', ''BIGINT'',
                ''SMALLINT'', ''TINYINT'', ''BYTEINT''
            ) THEN ''NUMERIC''
            ELSE ''CATEGORICAL''
        END AS column_class
    FROM information_schema.columns
    WHERE table_schema = ''' || $TARGET_SCHEMA || '''
      AND table_name   = ''' || $TARGET_TABLE || '''
),

temporal_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT DATE(' || $TEMPORAL_COLUMN || ')) AS active_days
    FROM ' || $TARGET_TABLE || '
    WHERE ' || $TEMPORAL_COLUMN || ' IS NOT NULL
),

unpivoted AS (
    SELECT
        u.column_name,
        cm.column_class,
        u.value::VARCHAR AS val,
        CASE
            WHEN cm.column_class = ''NUMERIC''
            THEN TRY_TO_NUMBER(u.value)
            ELSE NULL
        END AS num_val,
        ' || $TEMPORAL_COLUMN || ' AS temporal_value
    FROM ' || $TARGET_TABLE || '
    UNPIVOT (
        value FOR column_name IN (
            ' || v_unpivot_list || '
        )
    ) u
    JOIN column_metadata cm
        ON u.column_name = cm.column_name
    WHERE u.value IS NOT NULL
),

value_counts AS (
    SELECT
        column_name,
        column_class,
        val,
        COUNT(*) AS cnt
    FROM unpivoted
    GROUP BY column_name, column_class, val
),

column_totals AS (
    SELECT
        column_name,
        SUM(cnt) AS total_cnt
    FROM value_counts
    GROUP BY column_name
),

ranked AS (
    SELECT
        vc.column_name,
        vc.column_class,
        vc.val,
        vc.cnt,
        vc.cnt / ct.total_cnt AS pct,
        ROW_NUMBER() OVER (
            PARTITION BY vc.column_name
            ORDER BY vc.cnt DESC
        ) AS rn
    FROM value_counts vc
    JOIN column_totals ct
        ON vc.column_name = ct.column_name
),

top_values AS (
    SELECT
        column_name,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                ''value'', val,
                ''pct'', ROUND(pct, 6)
            ) ORDER BY pct DESC
        ) AS top_values,
        MAX(pct) AS modal_value_pct
    FROM ranked
    WHERE rn <= 10
    GROUP BY column_name
),

entropy AS (
    SELECT
        column_name,
        -SUM(CASE WHEN pct > 0 THEN pct * LOG(2, pct) ELSE 0 END) AS entropy_score
    FROM ranked
    GROUP BY column_name
),

numeric_stats AS (
    SELECT
        column_name,
        COUNT(num_val)       AS n,
        AVG(num_val)         AS mean_val,
        STDDEV_SAMP(num_val) AS stddev_val,
        MIN(num_val)         AS min_val,
        MAX(num_val)         AS max_val
    FROM unpivoted
    WHERE column_class = ''NUMERIC''
      AND num_val IS NOT NULL
    GROUP BY column_name
),

categorical_skew AS (
    SELECT
        column_name,
        MAX(cnt)::FLOAT / NULLIF(AVG(cnt), 0) AS skewness
    FROM value_counts
    WHERE column_class = ''CATEGORICAL''
    GROUP BY column_name
),

numeric_skew AS (
    SELECT
        u.column_name,
        CASE
            WHEN ns.stddev_val > 0 AND ns.n > 2 THEN
                SUM(POWER((u.num_val - ns.mean_val) / ns.stddev_val, 3))
                * ns.n / ((ns.n - 1) * (ns.n - 2))
            ELSE NULL
        END AS skewness
    FROM unpivoted u
    JOIN numeric_stats ns
        ON u.column_name = ns.column_name
    WHERE u.column_class = ''NUMERIC''
      AND u.num_val IS NOT NULL
    GROUP BY u.column_name, ns.mean_val, ns.stddev_val, ns.n
),

temporal_density AS (
    SELECT
        u.column_name,
        ROUND(COUNT(*) / NULLIF(ts.active_days, 0), 2)::VARCHAR || '' per day'' AS temporal_density
    FROM unpivoted u
    CROSS JOIN temporal_stats ts
    WHERE u.temporal_value IS NOT NULL
    GROUP BY u.column_name, ts.active_days
),

final_metrics AS (
    SELECT
        tv.column_name,
        cm.data_type,
        cm.column_class,
        tv.top_values,
        e.entropy_score,
        COALESCE(ns2.skewness, cs.skewness) AS skewness,
        tv.modal_value_pct,
        td.temporal_density,
        ns.mean_val,
        ns.stddev_val,
        ns.min_val,
        ns.max_val
    FROM top_values tv
    JOIN column_metadata cm ON tv.column_name = cm.column_name
    JOIN entropy e          ON tv.column_name = e.column_name
    LEFT JOIN categorical_skew cs ON tv.column_name = cs.column_name
    LEFT JOIN numeric_skew ns2    ON tv.column_name = ns2.column_name
    LEFT JOIN numeric_stats ns    ON tv.column_name = ns.column_name
    LEFT JOIN temporal_density td ON tv.column_name = td.column_name
),

column_roles AS (
    SELECT
        fm.*,
        CASE
            WHEN fm.column_class = ''NUMERIC''
             AND fm.min_val IN (0,1)
             AND fm.max_val IN (0,1)
             AND fm.modal_value_pct > 0.8
                THEN ''FLAG''
            WHEN fm.column_name ILIKE ''%_ID''
              OR fm.column_name ILIKE ''%_KEY''
                THEN ''ID''
            WHEN fm.column_class = ''NUMERIC''
                THEN ''MEASURE''
            ELSE ''CODE''
        END AS primary_role
    FROM final_metrics fm
),

secondary_roles AS (
    SELECT
        cr.*,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(cr.primary_role = ''ID'' AND cr.column_name ILIKE ''%_KEY'', ''SURROGATE_KEY'', NULL),
            IFF(cr.primary_role = ''ID'' AND cr.column_name ILIKE ''%_ID'',  ''FOREIGN_KEY'',   NULL),
            IFF(cr.primary_role = ''CODE'' AND cr.entropy_score < 3,       ''ENUM'',          NULL)
        )) AS secondary_roles
    FROM column_roles cr
)

INSERT INTO sep_column_distribution
SELECT
    ''' || $TARGET_TABLE || ''' AS table_name,
    column_name,
    data_type,
    column_class,
    primary_role,
    secondary_roles,
    top_values,
    entropy_score,
    skewness,
    modal_value_pct,
    mean_val,
    stddev_val,
    min_val,
    max_val,
    temporal_density,
    CURRENT_TIMESTAMP
FROM secondary_roles;
';

    EXECUTE IMMEDIATE v_sql;

    RETURN 'STM profiler completed for table ' || $TARGET_TABLE;
END;
