-- =====================================================================
-- OPTIMIZED DYNAMIC TABLE PROFILING PROCEDURE
-- Healthcare Data Engineering - Snowflake Platform
-- =====================================================================

CREATE OR REPLACE PROCEDURE PROFILE_TABLE(
    TABLE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR DEFAULT CURRENT_SCHEMA(),
    DATABASE_NAME VARCHAR DEFAULT CURRENT_DATABASE(),
    SAMPLE_SIZE NUMBER DEFAULT 100000
)
RETURNS TABLE (
    METRIC_CATEGORY VARCHAR,
    METRIC_NAME VARCHAR,
    METRIC_VALUE VARIANT
)
LANGUAGE SQL
AS
$$
DECLARE
    full_table_name VARCHAR;
    source_table VARCHAR;
    col_cursor CURSOR FOR 
        SELECT column_name, data_type 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = :SCHEMA_NAME 
        AND table_name = :TABLE_NAME
        AND table_catalog = :DATABASE_NAME
        ORDER BY ordinal_position;
    
    sql_stmt VARCHAR;
    res RESULTSET;
BEGIN
    full_table_name := '"' || DATABASE_NAME || '"."' || SCHEMA_NAME || '"."' || TABLE_NAME || '"';
    
    -- Create a sampling CTE to use throughout the session if SAMPLE_SIZE > 0
    -- This ensures we only scan the data once per column
    source_table := '(SELECT * FROM ' || full_table_name || ' LIMIT ' || SAMPLE_SIZE || ')';

    -- Create temporary results table to collect all metrics
    CREATE OR REPLACE TEMPORARY TABLE TEMP_PROFILER_OUTPUT (
        metric_category VARCHAR,
        metric_name VARCHAR,
        metric_value VARIANT
    );
    
    -- 1. TABLE-LEVEL METRICS (Single Scan)
    INSERT INTO TEMP_PROFILER_OUTPUT
    SELECT 'Table Overview', 'Total Rows', TO_VARIANT(COUNT(*)) FROM IDENTIFIER(:full_table_name);
    
    INSERT INTO TEMP_PROFILER_OUTPUT
    SELECT 'Table Overview', 'Table Size (MB)', TO_VARIANT(ROUND(bytes/1024/1024, 2))
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_catalog = :DATABASE_NAME AND table_schema = :SCHEMA_NAME AND table_name = :TABLE_NAME;

    -- 2. COLUMN-LEVEL PROFILING
    FOR record IN col_cursor DO
        LET col VARCHAR := '"' || record.column_name || '"';
        LET col_raw VARCHAR := record.column_name;
        LET dtype VARCHAR := record.data_type;
        
        -- Basic Metrics (Nulls, Distinct) - Done in ONE scan per column
        sql_stmt := 'INSERT INTO TEMP_PROFILER_OUTPUT
            SELECT 
                ''' || col_raw || ''', 
                metric_name, 
                val 
            FROM (
                SELECT 
                    TO_VARIANT(''' || dtype || ''') as "Data Type",
                    TO_VARIANT(COUNT(*) - COUNT(' || col || ')) as "Null Count",
                    TO_VARIANT(ROUND((COUNT(*) - COUNT(' || col || ')) * 100.0 / NULLIF(COUNT(*), 0), 2)) as "Null Percentage",
                    TO_VARIANT(COUNT(DISTINCT ' || col || ')) as "Distinct Count",
                    TO_VARIANT(ROUND(COUNT(DISTINCT ' || col || ') * 100.0 / NULLIF(COUNT(*), 0), 2)) as "Cardinality Ratio"
                FROM ' || source_table || '
            ) UNPIVOT(val FOR metric_name IN ("Data Type", "Null Count", "Null Percentage", "Distinct Count", "Cardinality Ratio"))';
        EXECUTE IMMEDIATE :sql_stmt;

        -- Numeric Specifics (Single Scan)
        IF (dtype IN ('NUMBER', 'INTEGER', 'FLOAT', 'DOUBLE', 'DECIMAL')) THEN
            sql_stmt := 'INSERT INTO TEMP_PROFILER_OUTPUT
                SELECT ''' || col_raw || ''', metric_name, val
                FROM (
                    SELECT 
                        TO_VARIANT(MIN(' || col || ')) as "Min",
                        TO_VARIANT(MAX(' || col || ')) as "Max",
                        TO_VARIANT(AVG(' || col || ')) as "Mean",
                        TO_VARIANT(MEDIAN(' || col || ')) as "Median",
                        TO_VARIANT(STDDEV(' || col || ')) as "Std Dev"
                    FROM ' || source_table || '
                ) UNPIVOT(val FOR metric_name IN ("Min", "Max", "Mean", "Median", "Std Dev"))';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;

        -- String Specifics (Single Scan)
        IF (dtype IN ('TEXT', 'VARCHAR', 'STRING', 'CHAR')) THEN
            sql_stmt := 'INSERT INTO TEMP_PROFILER_OUTPUT
                SELECT ''' || col_raw || ''', metric_name, val
                FROM (
                    SELECT 
                        TO_VARIANT(MIN(LENGTH(' || col || '))) as "Min Length",
                        TO_VARIANT(MAX(LENGTH(' || col || '))) as "Max Length",
                        TO_VARIANT(AVG(LENGTH(' || col || '))) as "Avg Length"
                    FROM ' || source_table || '
                ) UNPIVOT(val FOR metric_name IN ("Min Length", "Max Length", "Avg Length"))';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;

    END FOR;

    res := (SELECT * FROM TEMP_PROFILER_OUTPUT ORDER BY 
                CASE WHEN metric_category = 'Table Overview' THEN 0 ELSE 1 END, 
                metric_category, metric_name);
    RETURN TABLE(res);
END;
$$;