-- =====================================================================
-- FINAL STABLE VERSION: DYNAMIC TABLE PROFILER
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
    source_query VARCHAR;
    sql_stmt VARCHAR;
    final_res RESULTSET;
BEGIN
    -- 1. Setup Identifiers
    full_table_name := '"' || :DATABASE_NAME || '"."' || :SCHEMA_NAME || '"."' || :TABLE_NAME || '"';
    source_query := '(SELECT * FROM ' || :full_table_name || ' LIMIT ' || :SAMPLE_SIZE || ')';

    -- 2. Create Temporary Results Table
    CREATE OR REPLACE TEMPORARY TABLE TEMP_PROFILE_RESULTS (
        metric_category VARCHAR,
        metric_name VARCHAR,
        metric_value VARIANT
    );
    
    -- 3. Table-Level Metrics
    sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS 
                 SELECT ''Table Overview'', ''Total Rows'', TO_VARIANT(COUNT(*)) FROM ' || :full_table_name;
    EXECUTE IMMEDIATE :sql_stmt;
    
    INSERT INTO TEMP_PROFILE_RESULTS
    SELECT 'Table Overview', 'Table Size (MB)', TO_VARIANT(ROUND(bytes/1024/1024, 2))
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_catalog = :DATABASE_NAME 
      AND table_schema = :SCHEMA_NAME 
      AND table_name = :TABLE_NAME;

    -- 4. Column-Level Profiling using RESULTSET (Fixes the Bind Variable Error)
    LET col_res RESULTSET := (
        SELECT column_name, data_type 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = :SCHEMA_NAME 
          AND table_name = :TABLE_NAME
          AND table_catalog = :DATABASE_NAME
        ORDER BY ordinal_position
    );

    -- Iterate through the columns found in the resultset
    FOR record IN col_res DO
        LET col VARCHAR := '"' || record.column_name || '"';
        LET col_raw VARCHAR := record.column_name;
        LET dtype VARCHAR := record.data_type;
        
        -- Basic Profiling
        sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS
            SELECT ''' || :col_raw || ''', metric_name, val 
            FROM (
                SELECT 
                    TO_VARIANT(''' || :dtype || ''') as "Data Type",
                    TO_VARIANT(COUNT(*) - COUNT(' || :col || ')) as "Null Count",
                    TO_VARIANT(ROUND((COUNT(*) - COUNT(' || :col || ')) * 100.0 / NULLIF(COUNT(*), 0), 2)) as "Null %",
                    TO_VARIANT(COUNT(DISTINCT ' || :col || ')) as "Distinct Count"
                FROM ' || :source_query || '
            ) UNPIVOT(val FOR metric_name IN ("Data Type", "Null Count", "Null %", "Distinct Count"))';
        EXECUTE IMMEDIATE :sql_stmt;

        -- Numeric Stats
        IF (dtype IN ('NUMBER', 'INTEGER', 'FLOAT', 'DOUBLE', 'DECIMAL')) THEN
            sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS
                SELECT ''' || :col_raw || ''', metric_name, val
                FROM (
                    SELECT 
                        TO_VARIANT(MIN(' || :col || ')) as "Min Value",
                        TO_VARIANT(MAX(' || :col || ')) as "Max Value",
                        TO_VARIANT(ROUND(AVG(' || :col || '), 2)) as "Mean"
                    FROM ' || :source_query || '
                ) UNPIVOT(val FOR metric_name IN ("Min Value", "Max Value", "Mean"))';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;

        -- String Stats
        IF (dtype IN ('TEXT', 'VARCHAR', 'STRING', 'CHAR')) THEN
            sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS
                SELECT ''' || :col_raw || ''', metric_name, val
                FROM (
                    SELECT 
                        TO_VARIANT(MIN(LENGTH(' || :col || '))) as "Min Length",
                        TO_VARIANT(MAX(LENGTH(' || :col || '))) as "Max Length"
                    FROM ' || :source_query || '
                ) UNPIVOT(val FOR metric_name IN ("Min Length", "Max Length"))';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;
    END FOR;
    
    -- 5. Return Results
    final_res := (SELECT * FROM TEMP_PROFILE_RESULTS 
            ORDER BY CASE WHEN metric_category = 'Table Overview' THEN 0 ELSE 1 END, 
            metric_category, metric_name);
    RETURN TABLE(final_res);
END;
$$;