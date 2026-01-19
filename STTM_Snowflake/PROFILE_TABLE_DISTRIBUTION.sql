USE DATABASE STAR_DEV;
USE SCHEMA SEMANTIC_MAPPING;

CREATE OR REPLACE PROCEDURE SP_TABLE_DISTRIBUTION(
    PROFILE_DB VARCHAR,
    PROFILE_SCHEMA VARCHAR,
    PROFILE_TABLE VARCHAR,
    TEMPORAL_COLUMN VARCHAR,
    DISTRIBUTION_TABLE VARCHAR DEFAULT 'STAR_DEV.SEMANTIC_MAPPING.COLUMN_DISTRIBUTIONS'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_unpivot_list STRING;
    v_unpivot_cast STRING;
    v_sql STRING;
    info_schema_path STRING := PROFILE_DB || '.INFORMATION_SCHEMA.COLUMNS';
    v_error_msg STRING;
BEGIN
    -- Validate table exists
    LET v_table_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO :v_table_count
        FROM IDENTIFIER(:info_schema_path)
        WHERE table_schema = :PROFILE_SCHEMA
          AND table_name = :PROFILE_TABLE;
        
        IF (v_table_count = 0) THEN
            RETURN 'ERROR: Table ' || PROFILE_DB || '.' || PROFILE_SCHEMA || '.' || PROFILE_TABLE || ' does not exist';
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: Unable to access table metadata for ' || PROFILE_DB || '.' || PROFILE_SCHEMA || '.' || PROFILE_TABLE || ' - ' || SQLERRM;
    END;
    
    -- Validate temporal column exists
    LET v_col_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO :v_col_count
        FROM IDENTIFIER(:info_schema_path)
        WHERE table_schema = :PROFILE_SCHEMA
          AND table_name = :PROFILE_TABLE
          AND column_name = :TEMPORAL_COLUMN;
        
        IF (v_col_count = 0) THEN
            RETURN 'ERROR: Temporal column ' || TEMPORAL_COLUMN || ' does not exist in table ' || PROFILE_TABLE;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: Unable to validate temporal column - ' || SQLERRM;
    END;
    
    -- Build unpivot column list
    BEGIN
        SELECT
            LISTAGG(
                column_name || '::VARCHAR AS ' || column_name || '_V',
                ',\n            '
            ) WITHIN GROUP (ORDER BY ordinal_position),
            LISTAGG(column_name || '_V', ',\n            ') WITHIN GROUP (ORDER BY ordinal_position)
        INTO :v_unpivot_cast, :v_unpivot_list
        FROM IDENTIFIER(:info_schema_path)
        WHERE table_schema = :PROFILE_SCHEMA
          AND table_name = :PROFILE_TABLE
          AND data_type NOT IN ('VARIANT', 'ARRAY', 'OBJECT')
          AND column_name <> :TEMPORAL_COLUMN
          AND column_name NOT IN ('ETL_CREATED_DATE', 'ETL_UPDATED_DATE', 'ETL_CREATED_BY', 'ETL_UPDATED_BY');
        
        IF (v_unpivot_list IS NULL OR v_unpivot_list = '') THEN
            RETURN 'ERROR: No valid columns found to profile in table ' || PROFILE_TABLE;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: Failed to build column list - ' || SQLERRM;
    END;
    
    -- Build and execute main profiler query
    BEGIN
        v_sql := '
INSERT INTO ' || DISTRIBUTION_TABLE || '   
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
    FROM ' || PROFILE_DB || '.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_schema = ''' || PROFILE_SCHEMA || '''
      AND table_name   = ''' || PROFILE_TABLE || '''
),

base_projected AS (
    SELECT
        t.*,
        ' || v_unpivot_cast || '
    FROM ' || PROFILE_DB || '.' || PROFILE_SCHEMA || '.' || PROFILE_TABLE || ' AS t
),

unpivoted AS (
    SELECT
        REPLACE(column_name, ''_V'', '''') AS column_name,
        value
    FROM base_projected
    UNPIVOT (
        value FOR column_name IN (
            ' || v_unpivot_list || '
        )
    )
),

unpivoted_with_type AS (
    SELECT
        u.column_name,
        cm.column_class,
        u.value::VARCHAR AS val,
        CASE
            WHEN cm.column_class = ''NUMERIC'' THEN TRY_TO_NUMBER(u.value)
            ELSE NULL
        END AS num_val,
        ''' || TEMPORAL_COLUMN || ''' AS temporal_value
    FROM unpivoted u
    JOIN column_metadata cm
      ON u.column_name = cm.column_name
    WHERE u.value IS NOT NULL
),

value_counts AS (
    SELECT column_name, column_class, val, COUNT(*) AS cnt
    FROM unpivoted_with_type
    GROUP BY column_name, column_class, val
),

column_totals AS (
    SELECT column_name, SUM(cnt) AS total_cnt
    FROM value_counts
    GROUP BY column_name
),

ranked AS (
   SELECT
        vc.column_name,
        vc.column_class,
        vc.val,
        vc.cnt,
        vc.cnt / NULLIF(ct.total_cnt, 0) AS pct,
        ROW_NUMBER() OVER (
            PARTITION BY vc.column_name
            ORDER BY vc.cnt DESC
        ) AS rn
    FROM value_counts vc
    JOIN column_totals ct ON vc.column_name = ct.column_name
),

top_values AS (
    SELECT
        column_name,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                ''value'', val,
                ''pct'', ROUND(pct, 6)
            )
        ) WITHIN GROUP (ORDER BY pct DESC) AS top_values,
        MAX(pct) AS modal_value_pct
    FROM ranked
    WHERE rn <= 10
    GROUP BY column_name
),

entropy AS (
    SELECT column_name, -SUM(CASE WHEN pct > 0 THEN pct * LOG(2,pct) ELSE 0 END) AS entropy_score
    FROM ranked
    GROUP BY column_name
),

numeric_stats AS (
    SELECT column_name, COUNT(num_val) AS n, AVG(num_val) AS mean_val,
           STDDEV_SAMP(num_val) AS stddev_val, MIN(num_val) AS min_val, MAX(num_val) AS max_val
    FROM unpivoted_with_type
    WHERE column_class = ''NUMERIC'' AND num_val IS NOT NULL
    GROUP BY column_name
),

categorical_skew AS (
    SELECT column_name, MAX(cnt)::FLOAT / NULLIF(AVG(cnt),0) AS skewness
    FROM value_counts
    WHERE column_class = ''CATEGORICAL''
    GROUP BY column_name
),

numeric_skew AS (
    SELECT u.column_name,
           CASE WHEN ns.stddev_val > 0 AND ns.n > 2 THEN
                SUM(POWER((u.num_val - ns.mean_val)/NULLIF(ns.stddev_val,0),3)) * ns.n / NULLIF((ns.n-1)*(ns.n-2), 0)
           ELSE NULL END AS skewness
    FROM unpivoted_with_type u
    JOIN numeric_stats ns ON u.column_name = ns.column_name
    WHERE u.column_class = ''NUMERIC'' AND u.num_val IS NOT NULL
    GROUP BY u.column_name, ns.mean_val, ns.stddev_val, ns.n
),

temporal_density AS (
    SELECT u.column_name, ROUND(COUNT(*)/NULLIF(ts.active_days,0),2)::VARCHAR || '' per day'' AS temporal_density
    FROM unpivoted_with_type u
    CROSS JOIN (SELECT COUNT(DISTINCT ' || TEMPORAL_COLUMN || ') AS active_days FROM ' || PROFILE_DB || '.' || PROFILE_SCHEMA || '.' || PROFILE_TABLE || ') ts
    WHERE u.temporal_value IS NOT NULL
    GROUP BY u.column_name, ts.active_days
),

final_metrics AS (
    SELECT tv.column_name, cm.data_type, cm.column_class, tv.top_values, e.entropy_score,
           COALESCE(ns2.skewness, cs.skewness) AS skewness,
           tv.modal_value_pct, td.temporal_density, ns.mean_val, ns.stddev_val, ns.min_val, ns.max_val
    FROM top_values tv
    JOIN column_metadata cm ON tv.column_name = cm.column_name
    JOIN entropy e ON tv.column_name = e.column_name
    LEFT JOIN categorical_skew cs ON tv.column_name = cs.column_name
    LEFT JOIN numeric_skew ns2 ON tv.column_name = ns2.column_name
    LEFT JOIN numeric_stats ns ON tv.column_name = ns.column_name
    LEFT JOIN temporal_density td ON tv.column_name = td.column_name
),

column_roles AS (
    SELECT fm.*, CASE
        WHEN fm.column_class = ''NUMERIC'' AND fm.min_val IN (0,1) AND fm.max_val IN (0,1) AND fm.modal_value_pct > 0.8 THEN ''FLAG''
        WHEN fm.column_name ILIKE ''%_ID'' OR fm.column_name ILIKE ''%_KEY'' THEN ''ID''
        WHEN fm.column_class = ''NUMERIC'' THEN ''MEASURE''
        ELSE ''CODE''
    END AS primary_role
    FROM final_metrics fm
),

secondary_roles AS (
    SELECT cr.*, ARRAY_COMPACT(ARRAY_CONSTRUCT(
        IFF(cr.primary_role=''ID'' AND cr.column_name ILIKE ''%_KEY'',''SURROGATE_KEY'',NULL),
        IFF(cr.primary_role=''ID'' AND cr.column_name ILIKE ''%_ID'',''FOREIGN_KEY'',NULL),
        IFF(cr.primary_role=''CODE'' AND cr.entropy_score<3,''ENUM'',NULL)
    )) AS secondary_roles
    FROM column_roles cr
)

SELECT ''' || PROFILE_TABLE || ''' AS table_name, column_name, data_type, column_class, primary_role,
       secondary_roles, top_values, entropy_score, skewness, modal_value_pct, mean_val, stddev_val,
       min_val, max_val, temporal_density, CURRENT_TIMESTAMP
FROM secondary_roles;
';

        EXECUTE IMMEDIATE v_sql;
        RETURN 'STM profiler completed for table ' || PROFILE_TABLE;
        
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: Failed to execute profiler query - ' || SQLERRM;
    END;
END;
$$;

-- Example usage:
-- CALL SP_TABLE_DISTRIBUTION('STAR_DEV', 'DIM_MODEL', 'FACT_ENCOUNTER_PROCEDURE', 'DIM_DATE_KEY');
-- CALL SP_TABLE_DISTRIBUTION('STAR_DEV', 'DIM_MODEL', 'FACT_ENCOUNTER_PROCEDURE', 'DIM_DATE_KEY', 'STAR_DEV.SEMANTIC_MAPPING.COLUMN_DISTRIBUTIONS');