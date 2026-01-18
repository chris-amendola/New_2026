CREATE OR REPLACE PROCEDURE compute_column_distributions(
    p_table_name VARCHAR,
    p_temporal_column VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_column_name VARCHAR;
    v_data_type VARCHAR;
    v_sql VARCHAR;
    v_result_count INTEGER DEFAULT 0;
    
    -- Cursor to get all eligible columns
    c_columns CURSOR FOR
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = CURRENT_SCHEMA()
          AND table_name = p_table_name
          AND data_type NOT IN ('VARIANT', 'ARRAY', 'OBJECT')
        ORDER BY ordinal_position;
BEGIN
    -- Open cursor and loop through columns
    OPEN c_columns;
    FOR record IN c_columns DO
        v_column_name := record.column_name;
        v_data_type := record.data_type;
        
        -- Build dynamic SQL based on data type
        IF v_data_type IN ('NUMBER', 'FLOAT', 'DOUBLE', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 'DECIMAL', 'NUMERIC') THEN
            -- Numeric columns: use statistical skewness
            v_sql := '
            WITH base AS (
                SELECT
                    ' || v_column_name || '::VARCHAR AS val,
                    ' || v_column_name || ' AS num_val,
                    COUNT(*) AS cnt
                FROM ' || p_table_name || '
                WHERE ' || v_column_name || ' IS NOT NULL
                GROUP BY ' || v_column_name || '
            ),
            totals AS (
                SELECT SUM(cnt) AS total_cnt
                FROM base
            ),
            ranked AS (
                SELECT
                    val,
                    num_val,
                    cnt,
                    cnt / total_cnt AS pct,
                    ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn
                FROM base
                CROSS JOIN totals
            ),
            top_vals AS (
                SELECT
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            ''value'', val,
                            ''pct'', ROUND(pct, 6)
                        )
                        ORDER BY pct DESC
                    ) AS top_values,
                    MAX(pct) AS modal_value_pct
                FROM ranked
                WHERE rn <= 10
            ),
            entropy AS (
                SELECT
                    -SUM(CASE WHEN pct > 0 THEN pct * LOG(2, pct) ELSE 0 END) AS entropy_score
                FROM ranked
            ),
            stats AS (
                SELECT
                    AVG(num_val) AS mean_val,
                    STDDEV(num_val) AS stddev_val,
                    COUNT(*) AS n
                FROM ranked
            ),
            skew AS (
                SELECT
                    CASE 
                        WHEN s.stddev_val > 0 AND s.n > 2 THEN
                            SUM(POWER((r.num_val - s.mean_val) / s.stddev_val, 3)) * s.n / ((s.n - 1) * (s.n - 2))
                        ELSE 0
                    END AS skewness
                FROM ranked r
                CROSS JOIN stats s
            ),
            temporal AS (
                SELECT
                    CASE
                        WHEN COUNT(DISTINCT ' || p_temporal_column || ') > 0
                            THEN ROUND(COUNT(*) / COUNT(DISTINCT ' || p_temporal_column || '), 2)::VARCHAR || '' per day''
                        ELSE NULL
                    END AS temporal_density
                FROM ' || p_table_name || '
                WHERE ' || v_column_name || ' IS NOT NULL
                  AND ' || p_temporal_column || ' IS NOT NULL
            )
            INSERT INTO sep_column_distribution
            SELECT
                ''' || p_table_name || ''' AS table_name,
                ''' || v_column_name || ''' AS column_name,
                tv.top_values,
                e.entropy_score,
                sk.skewness,
                tv.modal_value_pct,
                t.temporal_density,
                CURRENT_TIMESTAMP AS created_at
            FROM top_vals tv
            CROSS JOIN entropy e
            CROSS JOIN skew sk
            CROSS JOIN temporal t';
            
        ELSE
            -- String, Boolean, and other non-numeric types: use frequency-based skewness
            v_sql := '
            WITH base AS (
                SELECT
                    ' || v_column_name || '::VARCHAR AS val,
                    COUNT(*) AS cnt
                FROM ' || p_table_name || '
                WHERE ' || v_column_name || ' IS NOT NULL
                GROUP BY ' || v_column_name || '
            ),
            totals AS (
                SELECT SUM(cnt) AS total_cnt
                FROM base
            ),
            ranked AS (
                SELECT
                    val,
                    cnt,
                    cnt / total_cnt AS pct,
                    ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn
                FROM base
                CROSS JOIN totals
            ),
            top_vals AS (
                SELECT
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            ''value'', val,
                            ''pct'', ROUND(pct, 6)
                        )
                        ORDER BY pct DESC
                    ) AS top_values,
                    MAX(pct) AS modal_value_pct
                FROM ranked
                WHERE rn <= 10
            ),
            entropy AS (
                SELECT
                    -SUM(CASE WHEN pct > 0 THEN pct * LOG(2, pct) ELSE 0 END) AS entropy_score
                FROM ranked
            ),
            skew AS (
                SELECT
                    CASE 
                        WHEN AVG(cnt) > 0 THEN MAX(cnt)::FLOAT / AVG(cnt)
                        ELSE 0
                    END AS skewness
                FROM base
            ),
            temporal AS (
                SELECT
                    CASE
                        WHEN COUNT(DISTINCT ' || p_temporal_column || ') > 0
                            THEN ROUND(COUNT(*) / COUNT(DISTINCT ' || p_temporal_column || '), 2)::VARCHAR || '' per day''
                        ELSE NULL
                    END AS temporal_density
                FROM ' || p_table_name || '
                WHERE ' || v_column_name || ' IS NOT NULL
                  AND ' || p_temporal_column || ' IS NOT NULL
            )
            INSERT INTO sep_column_distribution
            SELECT
                ''' || p_table_name || ''' AS table_name,
                ''' || v_column_name || ''' AS column_name,
                tv.top_values,
                e.entropy_score,
                sk.skewness,
                tv.modal_value_pct,
                t.temporal_density,
                CURRENT_TIMESTAMP AS created_at
            FROM top_vals tv
            CROSS JOIN entropy e
            CROSS JOIN skew sk
            CROSS JOIN temporal t';
        END IF;
        
        -- Execute the dynamic SQL
        EXECUTE IMMEDIATE v_sql;
        v_result_count := v_result_count + 1;
        
    END FOR;
    CLOSE c_columns;
    
    RETURN 'Successfully computed distribution metrics for ' || v_result_count || ' columns in table ' || p_table_name;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error: ' || SQLERRM || ' - Column: ' || IFNULL(v_column_name, 'N/A');
END;
$$;

-- Example usage:
-- CALL compute_column_distributions('procedure_fact', 'procedure_date');