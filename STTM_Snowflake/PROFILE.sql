-- =====================================================================
-- VERSION 3: ADDING CATEGORICAL FREQUENCY DISTRIBUTIONS
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
    full_table_name := '"' || :DATABASE_NAME || '"."' || :SCHEMA_NAME || '"."' || :TABLE_NAME || '"';
    source_query := '(SELECT * FROM ' || :full_table_name || ' LIMIT ' || :SAMPLE_SIZE || ')';

    CREATE OR REPLACE TEMPORARY TABLE TEMP_PROFILE_RESULTS (
        metric_category VARCHAR,
        metric_name VARCHAR,
        metric_value VARIANT
    );
    
    -- 1. Table-Level Metrics
    sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS 
                 SELECT ''Table Overview'', ''Total Rows'', TO_VARIANT(COUNT(*)) FROM ' || :full_table_name;
    EXECUTE IMMEDIATE :sql_stmt;
    
    -- 2. Column-Level Discovery
    LET col_res RESULTSET := (
        SELECT column_name, data_type 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = :SCHEMA_NAME 
          AND table_name = :TABLE_NAME
          AND table_catalog = :DATABASE_NAME
        ORDER BY ordinal_position
    );

    FOR record IN col_res DO
        LET col VARCHAR := '"' || record.column_name || '"';
        LET col_raw VARCHAR := record.column_name;
        LET dtype VARCHAR := record.data_type;
        
        -- Basic Profiling (Standard for all types)
        sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS
            SELECT ''' || :col_raw || ''', metric_name, val 
            FROM (
                SELECT 
                    TO_VARIANT(''' || :dtype || ''') as "Data Type",
                    TO_VARIANT(COUNT(*) - COUNT(' || :col || ')) as "Null Count",
                    TO_VARIANT(COUNT(DISTINCT ' || :col || ')) as "Distinct Count"
                FROM ' || :source_query || '
            ) UNPIVOT(val FOR metric_name IN ("Data Type", "Null Count", "Distinct Count"))';
        EXECUTE IMMEDIATE :sql_stmt;

        -- Categorical Frequency Distribution (For Strings and Booleans)
        IF (dtype IN ('TEXT', 'VARCHAR', 'STRING', 'CHAR', 'BOOLEAN')) THEN
            sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS
                SELECT ''' || :col_raw || ''', ''Top 5 Frequencies'', TO_VARIANT(freq_map)
                FROM (
                    SELECT OBJECT_AGG(' || :col || '::STRING, TO_VARIANT(cnt)) as freq_map
                    FROM (
                        SELECT ' || :col || ', COUNT(*) as cnt
                        FROM ' || :source_query || '
                        WHERE ' || :col || ' IS NOT NULL
                        GROUP BY 1
                        ORDER BY 2 DESC
                        LIMIT 5
                    )
                )';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;

        -- Numeric Stats
        IF (dtype IN ('NUMBER', 'INTEGER', 'FLOAT', 'DOUBLE', 'DECIMAL')) THEN
            sql_stmt := 'INSERT INTO TEMP_PROFILE_RESULTS
                SELECT ''' || :col_raw || ''', metric_name, val
                FROM (
                    SELECT 
                        TO_VARIANT(MIN(' || :col || ')) as "Min",
                        TO_VARIANT(MAX(' || :col || ')) as "Max",
                        TO_VARIANT(ROUND(AVG(' || :col || '), 2)) as "Mean"
                    FROM ' || :source_query || '
                ) UNPIVOT(val FOR metric_name IN ("Min", "Max", "Mean"))';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;

    END FOR;
    
    final_res := (SELECT * FROM TEMP_PROFILE_RESULTS 
            ORDER BY CASE WHEN metric_category = 'Table Overview' THEN 0 ELSE 1 END, 
            metric_category, metric_name);
    RETURN TABLE(final_res);
END;
$$;