-- =====================================================================
-- DYNAMIC TABLE PROFILING PROCEDURE FOR EXPLORATORY DATA ANALYSIS
-- Healthcare Data Engineering - Snowflake Platform
-- =====================================================================

-- Create a stored procedure for dynamic table profiling
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
    col_cursor CURSOR FOR 
        SELECT column_name, data_type 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = SCHEMA_NAME 
        AND table_name = TABLE_NAME
        AND table_catalog = DATABASE_NAME
        ORDER BY ordinal_position;
    
    col_name VARCHAR;
    col_type VARCHAR;
    sql_stmt VARCHAR;
    result_table VARCHAR DEFAULT 'TEMP_PROFILE_RESULTS_' || ABS(HASH(CURRENT_TIMESTAMP()::VARCHAR));
BEGIN
    full_table_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME;
    
    -- Create temporary results table
    CREATE TEMPORARY TABLE IDENTIFIER(:result_table) (
        metric_category VARCHAR,
        metric_name VARCHAR,
        metric_value VARIANT
    );
    
    -- ============ TABLE-LEVEL METRICS ============
    sql_stmt := '
        INSERT INTO IDENTIFIER(''' || result_table || ''')
        SELECT 
            ''Table Overview'' AS metric_category,
            ''Total Rows'' AS metric_name,
            TO_VARIANT(COUNT(*)) AS metric_value
        FROM ' || full_table_name;
    EXECUTE IMMEDIATE :sql_stmt;
    
    sql_stmt := '
        INSERT INTO IDENTIFIER(''' || result_table || ''')
        SELECT 
            ''Table Overview'' AS metric_category,
            ''Total Columns'' AS metric_name,
            TO_VARIANT(COUNT(*)) AS metric_value
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_catalog = ''' || DATABASE_NAME || '''
        AND table_schema = ''' || SCHEMA_NAME || '''
        AND table_name = ''' || TABLE_NAME || '''';
    EXECUTE IMMEDIATE :sql_stmt;
    
    sql_stmt := '
        INSERT INTO IDENTIFIER(''' || result_table || ''')
        SELECT 
            ''Table Overview'' AS metric_category,
            ''Table Size (MB)'' AS metric_name,
            TO_VARIANT(ROUND(bytes/1024/1024, 2)) AS metric_value
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_catalog = ''' || DATABASE_NAME || '''
        AND table_schema = ''' || SCHEMA_NAME || '''
        AND table_name = ''' || TABLE_NAME || '''';
    EXECUTE IMMEDIATE :sql_stmt;
    
    -- ============ COLUMN-LEVEL PROFILING ============
    FOR record IN col_cursor DO
        col_name := record.column_name;
        col_type := record.data_type;
        
        -- Data Type
        sql_stmt := '
            INSERT INTO IDENTIFIER(''' || result_table || ''')
            SELECT 
                ''' || col_name || ''' AS metric_category,
                ''Data Type'' AS metric_name,
                TO_VARIANT(''' || col_type || ''') AS metric_value';
        EXECUTE IMMEDIATE :sql_stmt;
        
        -- Null Count and Percentage
        sql_stmt := '
            INSERT INTO IDENTIFIER(''' || result_table || ''')
            SELECT 
                ''' || col_name || ''' AS metric_category,
                metric_name,
                metric_value
            FROM (
                SELECT 
                    ''Null Count'' AS metric_name,
                    TO_VARIANT(COUNT(*) - COUNT("' || col_name || '")) AS metric_value
                FROM ' || full_table_name || '
                UNION ALL
                SELECT 
                    ''Null Percentage'' AS metric_name,
                    TO_VARIANT(ROUND((COUNT(*) - COUNT("' || col_name || '")) * 100.0 / NULLIF(COUNT(*), 0), 2)) AS metric_value
                FROM ' || full_table_name || '
                UNION ALL
                SELECT 
                    ''Distinct Count'' AS metric_name,
                    TO_VARIANT(COUNT(DISTINCT "' || col_name || '")) AS metric_value
                FROM ' || full_table_name || '
                UNION ALL
                SELECT 
                    ''Cardinality Ratio'' AS metric_name,
                    TO_VARIANT(ROUND(COUNT(DISTINCT "' || col_name || '") * 100.0 / NULLIF(COUNT(*), 0), 2)) AS metric_value
                FROM ' || full_table_name || '
            )';
        EXECUTE IMMEDIATE :sql_stmt;
        
        -- Numeric column statistics
        IF (col_type LIKE '%NUMBER%' OR col_type LIKE '%INT%' OR col_type LIKE '%FLOAT%' OR col_type LIKE '%DECIMAL%') THEN
            sql_stmt := '
                INSERT INTO IDENTIFIER(''' || result_table || ''')
                SELECT 
                    ''' || col_name || ''' AS metric_category,
                    metric_name,
                    metric_value
                FROM (
                    SELECT ''Min Value'' AS metric_name, TO_VARIANT(MIN("' || col_name || '")) AS metric_value FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Max Value'', TO_VARIANT(MAX("' || col_name || '")) FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Mean'', TO_VARIANT(ROUND(AVG("' || col_name || '"), 2)) FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Median'', TO_VARIANT(ROUND(MEDIAN("' || col_name || '"), 2)) FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Std Dev'', TO_VARIANT(ROUND(STDDEV("' || col_name || '"), 2)) FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Zero Count'', TO_VARIANT(SUM(IFF("' || col_name || '" = 0, 1, 0))) FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Negative Count'', TO_VARIANT(SUM(IFF("' || col_name || '" < 0, 1, 0))) FROM ' || full_table_name || '
                )';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;
        
        -- String column statistics
        IF (col_type LIKE '%VARCHAR%' OR col_type LIKE '%CHAR%' OR col_type LIKE '%STRING%' OR col_type LIKE '%TEXT%') THEN
            sql_stmt := '
                INSERT INTO IDENTIFIER(''' || result_table || ''')
                SELECT 
                    ''' || col_name || ''' AS metric_category,
                    metric_name,
                    metric_value
                FROM (
                    SELECT ''Min Length'' AS metric_name, TO_VARIANT(MIN(LENGTH("' || col_name || '"))) AS metric_value FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Max Length'', TO_VARIANT(MAX(LENGTH("' || col_name || '"))) AS metric_value FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Avg Length'', TO_VARIANT(ROUND(AVG(LENGTH("' || col_name || '")), 2)) AS metric_value FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Empty String Count'', TO_VARIANT(SUM(IFF(LENGTH(TRIM("' || col_name || '")) = 0, 1, 0))) AS metric_value FROM ' || full_table_name || '
                )';
            EXECUTE IMMEDIATE :sql_stmt;
            
            -- Top 10 most frequent values (for low cardinality strings)
            sql_stmt := '
                INSERT INTO IDENTIFIER(''' || result_table || ''')
                SELECT 
                    ''' || col_name || ''' AS metric_category,
                    ''Top 10 Values'' AS metric_name,
                    TO_VARIANT(OBJECT_AGG(value_rank, value_info)) AS metric_value
                FROM (
                    SELECT 
                        ROW_NUMBER() OVER (ORDER BY cnt DESC) AS value_rank,
                        OBJECT_CONSTRUCT(
                            ''value'', "' || col_name || '",
                            ''count'', cnt,
                            ''percentage'', ROUND(cnt * 100.0 / SUM(cnt) OVER (), 2)
                        ) AS value_info
                    FROM (
                        SELECT "' || col_name || '", COUNT(*) AS cnt
                        FROM ' || full_table_name || '
                        WHERE "' || col_name || '" IS NOT NULL
                        GROUP BY "' || col_name || '"
                        ORDER BY cnt DESC
                        LIMIT 10
                    )
                )';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;
        
        -- Date/Timestamp column statistics
        IF (col_type LIKE '%DATE%' OR col_type LIKE '%TIME%') THEN
            sql_stmt := '
                INSERT INTO IDENTIFIER(''' || result_table || ''')
                SELECT 
                    ''' || col_name || ''' AS metric_category,
                    metric_name,
                    metric_value
                FROM (
                    SELECT ''Min Date'' AS metric_name, TO_VARIANT(MIN("' || col_name || '")) AS metric_value FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Max Date'', TO_VARIANT(MAX("' || col_name || '")) AS metric_value FROM ' || full_table_name || '
                    UNION ALL
                    SELECT ''Date Range (Days)'', TO_VARIANT(DATEDIFF(DAY, MIN("' || col_name || '"), MAX("' || col_name || '"))) AS metric_value FROM ' || full_table_name || '
                )';
            EXECUTE IMMEDIATE :sql_stmt;
        END IF;
        
    END FOR;
    
    -- Return results
    RETURN TABLE(SELECT * FROM IDENTIFIER(:result_table) ORDER BY metric_category, metric_name);
END;
$$;

-- =====================================================================
-- USAGE EXAMPLES
-- =====================================================================

/*
-- Example 1: Profile a table in the current schema and database
CALL PROFILE_TABLE('PATIENT_ENCOUNTERS');

-- Example 2: Profile a table in a specific schema
CALL PROFILE_TABLE('PATIENT_ENCOUNTERS', 'HEALTHCARE_STAGING');

-- Example 3: Profile a table with full qualification
CALL PROFILE_TABLE('PATIENT_ENCOUNTERS', 'HEALTHCARE_STAGING', 'EDW_PROD');

-- Example 4: View results in a formatted way
WITH profile_results AS (
    CALL PROFILE_TABLE('PATIENT_ENCOUNTERS')
)
SELECT 
    metric_category,
    metric_name,
    metric_value
FROM TABLE(profile_results)
ORDER BY 
    CASE 
        WHEN metric_category = 'Table Overview' THEN 0
        ELSE 1
    END,
    metric_category,
    metric_name;

-- Example 5: Export profile to a permanent table for documentation
CREATE OR REPLACE TABLE TABLE_PROFILE_HISTORY AS
SELECT 
    CURRENT_TIMESTAMP() AS profile_timestamp,
    'PATIENT_ENCOUNTERS' AS table_name,
    metric_category,
    metric_name,
    metric_value
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE CURRENT_STATEMENT() LIKE '%CALL PROFILE_TABLE%';
*/

-- =====================================================================
-- ALTERNATIVE: SIMPLIFIED QUICK PROFILE VIEW
-- =====================================================================

-- Create a quick profile macro for ad-hoc analysis
CREATE OR REPLACE PROCEDURE QUICK_PROFILE(
    TABLE_NAME VARCHAR,
    COLUMN_NAME VARCHAR DEFAULT NULL
)
RETURNS TABLE (metric VARCHAR, value VARIANT)
LANGUAGE SQL
AS
$$
BEGIN
    IF (COLUMN_NAME IS NULL) THEN
        -- Table-level quick stats
        RETURN TABLE(
            SELECT 
                'Total Rows' AS metric,
                TO_VARIANT(COUNT(*)) AS value
            FROM IDENTIFIER(:TABLE_NAME)
            UNION ALL
            SELECT 
                'Approx Size (MB)',
                TO_VARIANT(ROUND(COUNT(*) * 1024 / 1024.0 / 1000, 2))
            FROM IDENTIFIER(:TABLE_NAME)
        );
    ELSE
        -- Column-level quick stats
        LET query := '
            SELECT 
                ''Null %'' AS metric, TO_VARIANT(ROUND((COUNT(*) - COUNT("' || COLUMN_NAME || '")) * 100.0 / COUNT(*), 2)) AS value
            FROM ' || TABLE_NAME || '
            UNION ALL
            SELECT ''Distinct'', TO_VARIANT(COUNT(DISTINCT "' || COLUMN_NAME || '")) FROM ' || TABLE_NAME || '
            UNION ALL
            SELECT ''Min'', TO_VARIANT(MIN("' || COLUMN_NAME || '")) FROM ' || TABLE_NAME || '
            UNION ALL
            SELECT ''Max'', TO_VARIANT(MAX("' || COLUMN_NAME || '")) FROM ' || TABLE_NAME;
        RETURN TABLE(RESULTSET(query));
    END IF;
END;
$$;