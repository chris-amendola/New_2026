-- =====================================================================
-- COMPREHENSIVE TABLE PROFILING FOR HEALTHCARE DATA INTEGRATION
-- Purpose: Deep profile analysis for source-to-target mapping
-- Author: Analytics Engineering Team
-- =====================================================================

-- CONFIGURATION: Update these variables for your table
SET target_database = 'YOUR_DATABASE';
SET target_schema = 'YOUR_SCHEMA';
SET target_table = 'YOUR_TABLE';
SET sample_size = 1000; -- For value distribution analysis

-- =====================================================================
-- SECTION 1: TABLE METADATA & STRUCTURE
-- =====================================================================

-- 1.1 Basic Table Information
SELECT 
    CURRENT_TIMESTAMP() AS profiling_timestamp,
    '$target_database' AS database_name,
    '$target_schema' AS schema_name,
    '$target_table' AS table_name,
    COUNT(*) AS total_row_count,
    COUNT(DISTINCT *) AS distinct_row_count,
    (COUNT(*) - COUNT(DISTINCT *)) AS duplicate_row_count
FROM IDENTIFIER('$target_database.$target_schema.$target_table');

-- 1.2 Column Inventory with Data Types
SELECT 
    ORDINAL_POSITION,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    COMMENT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = '$target_database'
  AND TABLE_SCHEMA = '$target_schema'
  AND TABLE_NAME = '$target_table'
ORDER BY ORDINAL_POSITION;

-- =====================================================================
-- SECTION 2: COLUMN-LEVEL PROFILING
-- =====================================================================

-- 2.1 Comprehensive Column Statistics
-- This dynamic SQL generates statistics for ALL columns
SET column_stats_sql = (
    SELECT 
        'SELECT ' || CHR(10) ||
        '    ''' || '$target_table' || ''' AS table_name,' || CHR(10) ||
        LISTAGG(
            '    OBJECT_CONSTRUCT(' || CHR(10) ||
            '        ''column_name'', ''' || COLUMN_NAME || ''',' || CHR(10) ||
            '        ''data_type'', ''' || DATA_TYPE || ''',' || CHR(10) ||
            '        ''total_count'', COUNT(*),' || CHR(10) ||
            '        ''null_count'', COUNT(*) - COUNT(' || COLUMN_NAME || '),' || CHR(10) ||
            '        ''null_percentage'', ROUND(100.0 * (COUNT(*) - COUNT(' || COLUMN_NAME || ')) / NULLIF(COUNT(*), 0), 2),' || CHR(10) ||
            '        ''distinct_count'', COUNT(DISTINCT ' || COLUMN_NAME || '),' || CHR(10) ||
            '        ''cardinality_ratio'', ROUND(COUNT(DISTINCT ' || COLUMN_NAME || ')::FLOAT / NULLIF(COUNT(' || COLUMN_NAME || '), 0), 4),' || CHR(10) ||
            CASE 
                WHEN DATA_TYPE IN ('NUMBER', 'FLOAT', 'INTEGER', 'DECIMAL') THEN
                    '        ''min_value'', MIN(' || COLUMN_NAME || '),' || CHR(10) ||
                    '        ''max_value'', MAX(' || COLUMN_NAME || '),' || CHR(10) ||
                    '        ''avg_value'', ROUND(AVG(' || COLUMN_NAME || '), 2),' || CHR(10) ||
                    '        ''median_value'', ROUND(MEDIAN(' || COLUMN_NAME || '), 2),' || CHR(10) ||
                    '        ''stddev_value'', ROUND(STDDEV(' || COLUMN_NAME || '), 2),' || CHR(10)
                WHEN DATA_TYPE IN ('VARCHAR', 'STRING', 'TEXT', 'CHAR') THEN
                    '        ''min_length'', MIN(LENGTH(' || COLUMN_NAME || ')),' || CHR(10) ||
                    '        ''max_length'', MAX(LENGTH(' || COLUMN_NAME || ')),' || CHR(10) ||
                    '        ''avg_length'', ROUND(AVG(LENGTH(' || COLUMN_NAME || ')), 2),' || CHR(10) ||
                    '        ''empty_string_count'', SUM(CASE WHEN ' || COLUMN_NAME || ' = '''' THEN 1 ELSE 0 END),' || CHR(10) ||
                    '        ''whitespace_only_count'', SUM(CASE WHEN TRIM(' || COLUMN_NAME || ') = '''' AND ' || COLUMN_NAME || ' != '''' THEN 1 ELSE 0 END),' || CHR(10)
                WHEN DATA_TYPE IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ') THEN
                    '        ''min_date'', MIN(' || COLUMN_NAME || '),' || CHR(10) ||
                    '        ''max_date'', MAX(' || COLUMN_NAME || '),' || CHR(10) ||
                    '        ''date_range_days'', DATEDIFF(day, MIN(' || COLUMN_NAME || '), MAX(' || COLUMN_NAME || ')),' || CHR(10)
                ELSE ''
            END ||
            '        ''sample_values'', ARRAY_AGG(DISTINCT ' || COLUMN_NAME || ') WITHIN GROUP (ORDER BY ' || COLUMN_NAME || ') LIMIT 5' || CHR(10) ||
            '    ) AS ' || COLUMN_NAME || '_profile',
            ',' || CHR(10)
        ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) || CHR(10) ||
        'FROM IDENTIFIER(''$target_database.$target_schema.$target_table'');'
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = '$target_database'
      AND TABLE_SCHEMA = '$target_schema'
      AND TABLE_NAME = '$target_table'
);

-- Execute the dynamic SQL (uncomment to run)
-- EXECUTE IMMEDIATE $column_stats_sql;

-- =====================================================================
-- SECTION 3: DATA QUALITY CHECKS
-- =====================================================================

-- 3.1 Identify Columns with High Null Rates (>20%)
SET null_analysis_sql = (
    SELECT 
        'SELECT ' || CHR(10) ||
        '    ''' || '$target_table' || ''' AS table_name,' || CHR(10) ||
        '    ''High Null Rate Columns'' AS check_type,' || CHR(10) ||
        '    ARRAY_CONSTRUCT(' || CHR(10) ||
        LISTAGG(
            '        OBJECT_CONSTRUCT(' || CHR(10) ||
            '            ''column'', ''' || COLUMN_NAME || ''',' || CHR(10) ||
            '            ''null_count'', COUNT(*) - COUNT(' || COLUMN_NAME || '),' || CHR(10) ||
            '            ''null_pct'', ROUND(100.0 * (COUNT(*) - COUNT(' || COLUMN_NAME || ')) / COUNT(*), 2)' || CHR(10) ||
            '        )',
            ',' || CHR(10)
        ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) || CHR(10) ||
        '    ) AS null_analysis' || CHR(10) ||
        'FROM IDENTIFIER(''$target_database.$target_schema.$target_table'');'
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = '$target_database'
      AND TABLE_SCHEMA = '$target_schema'
      AND TABLE_NAME = '$target_table'
);

-- 3.2 Check for Duplicate Records
-- Assumes you want to check full row duplicates; modify for specific key columns
SELECT 
    'Duplicate Analysis' AS check_type,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT *) AS unique_rows,
    COUNT(*) - COUNT(DISTINCT *) AS duplicate_rows,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT *)) / COUNT(*), 2) AS duplicate_percentage
FROM IDENTIFIER('$target_database.$target_schema.$target_table');

-- 3.3 Identify Potential Key Columns (high cardinality, low nulls)
SET key_candidate_sql = (
    SELECT 
        'SELECT ' || CHR(10) ||
        '    ''' || COLUMN_NAME || ''' AS column_name,' || CHR(10) ||
        '    ''' || DATA_TYPE || ''' AS data_type,' || CHR(10) ||
        '    COUNT(*) AS total_count,' || CHR(10) ||
        '    COUNT(DISTINCT ' || COLUMN_NAME || ') AS distinct_count,' || CHR(10) ||
        '    COUNT(*) - COUNT(' || COLUMN_NAME || ') AS null_count,' || CHR(10) ||
        '    ROUND(100.0 * COUNT(DISTINCT ' || COLUMN_NAME || ') / NULLIF(COUNT(' || COLUMN_NAME || '), 0), 2) AS cardinality_pct,' || CHR(10) ||
        '    CASE ' || CHR(10) ||
        '        WHEN COUNT(DISTINCT ' || COLUMN_NAME || ') = COUNT(' || COLUMN_NAME || ') THEN ''Potential Primary Key''' || CHR(10) ||
        '        WHEN COUNT(DISTINCT ' || COLUMN_NAME || ')::FLOAT / NULLIF(COUNT(' || COLUMN_NAME || '), 0) > 0.95 THEN ''High Cardinality''' || CHR(10) ||
        '        WHEN COUNT(DISTINCT ' || COLUMN_NAME || ')::FLOAT / NULLIF(COUNT(' || COLUMN_NAME || '), 0) BETWEEN 0.10 AND 0.95 THEN ''Medium Cardinality''' || CHR(10) ||
        '        ELSE ''Low Cardinality''' || CHR(10) ||
        '    END AS key_candidate_type' || CHR(10) ||
        'FROM IDENTIFIER(''$target_database.$target_schema.$target_table'')' ||
        CASE WHEN ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) < 
            (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
             WHERE TABLE_CATALOG = '$target_database'
               AND TABLE_SCHEMA = '$target_schema'
               AND TABLE_NAME = '$target_table')
        THEN CHR(10) || 'UNION ALL' || CHR(10)
        ELSE ';'
        END
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = '$target_database'
      AND TABLE_SCHEMA = '$target_schema'
      AND TABLE_NAME = '$target_table'
    ORDER BY ORDINAL_POSITION
);

-- =====================================================================
-- SECTION 4: HEALTHCARE-SPECIFIC PATTERN DETECTION
-- =====================================================================

-- 4.1 Detect Common Healthcare Identifiers
-- This checks for patterns commonly found in healthcare data
WITH column_samples AS (
    SELECT 
        column_name,
        data_type,
        sample_value
    FROM (
        SELECT DISTINCT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            -- Dynamic sampling would go here in production
            NULL AS sample_value
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_CATALOG = '$target_database'
          AND c.TABLE_SCHEMA = '$target_schema'
          AND c.TABLE_NAME = '$target_table'
    )
)
SELECT 
    column_name,
    data_type,
    CASE 
        -- MRN patterns
        WHEN UPPER(column_name) LIKE '%MRN%' 
          OR UPPER(column_name) LIKE '%MEDICAL_RECORD%'
          OR UPPER(column_name) LIKE '%PATIENT_ID%' THEN 'MRN/Patient Identifier'
        
        -- Encounter/Visit patterns
        WHEN UPPER(column_name) LIKE '%ENCOUNTER%'
          OR UPPER(column_name) LIKE '%VISIT%'
          OR UPPER(column_name) LIKE '%ADMISSION%' THEN 'Encounter Identifier'
        
        -- Provider patterns
        WHEN UPPER(column_name) LIKE '%PROVIDER%'
          OR UPPER(column_name) LIKE '%PHYSICIAN%'
          OR UPPER(column_name) LIKE '%NPI%' THEN 'Provider Identifier'
        
        -- Date patterns
        WHEN UPPER(column_name) LIKE '%DATE%'
          OR UPPER(column_name) LIKE '%TIME%'
          OR UPPER(column_name) LIKE '%DT%' THEN 'Temporal Field'
        
        -- Diagnosis patterns
        WHEN UPPER(column_name) LIKE '%DIAG%'
          OR UPPER(column_name) LIKE '%ICD%'
          OR UPPER(column_name) LIKE '%DX%' THEN 'Diagnosis Code'
        
        -- Procedure patterns
        WHEN UPPER(column_name) LIKE '%PROC%'
          OR UPPER(column_name) LIKE '%CPT%'
          OR UPPER(column_name) LIKE '%PX%' THEN 'Procedure Code'
        
        -- Drug/Pharmacy patterns
        WHEN UPPER(column_name) LIKE '%MED%'
          OR UPPER(column_name) LIKE '%DRUG%'
          OR UPPER(column_name) LIKE '%RX%'
          OR UPPER(column_name) LIKE '%NDC%' THEN 'Medication Field'
        
        -- Demographics
        WHEN UPPER(column_name) LIKE '%GENDER%'
          OR UPPER(column_name) LIKE '%SEX%'
          OR UPPER(column_name) LIKE '%RACE%'
          OR UPPER(column_name) LIKE '%ETHNIC%'
          OR UPPER(column_name) LIKE '%DOB%'
          OR UPPER(column_name) LIKE '%BIRTH%' THEN 'Demographics'
        
        -- Financial
        WHEN UPPER(column_name) LIKE '%CHARGE%'
          OR UPPER(column_name) LIKE '%COST%'
          OR UPPER(column_name) LIKE '%PAYMENT%'
          OR UPPER(column_name) LIKE '%PRICE%'
          OR UPPER(column_name) LIKE '%AMOUNT%' THEN 'Financial'
        
        ELSE 'Other'
    END AS suspected_field_type,
    CASE
        WHEN data_type IN ('VARCHAR', 'STRING', 'TEXT') THEN 'Check for standard code formats'
        WHEN data_type IN ('DATE', 'TIMESTAMP') THEN 'Validate date ranges and formats'
        WHEN data_type IN ('NUMBER', 'INTEGER') THEN 'Check for valid ID ranges'
        ELSE 'Review data type appropriateness'
    END AS integration_consideration
FROM column_samples
ORDER BY 
    CASE 
        WHEN UPPER(column_name) LIKE '%MRN%' OR UPPER(column_name) LIKE '%PATIENT%' THEN 1
        WHEN UPPER(column_name) LIKE '%ENCOUNTER%' OR UPPER(column_name) LIKE '%VISIT%' THEN 2
        WHEN UPPER(column_name) LIKE '%DATE%' OR UPPER(column_name) LIKE '%TIME%' THEN 3
        ELSE 4
    END,
    column_name;

-- =====================================================================
-- SECTION 5: VALUE DISTRIBUTION ANALYSIS
-- =====================================================================

-- 5.1 Top Value Frequencies (for low-medium cardinality columns)
-- Generate this dynamically for columns with <100 distinct values
SET value_dist_sql = (
    SELECT 
        LISTAGG(
            'SELECT ' || CHR(10) ||
            '    ''' || COLUMN_NAME || ''' AS column_name,' || CHR(10) ||
            '    ' || COLUMN_NAME || '::STRING AS value,' || CHR(10) ||
            '    COUNT(*) AS frequency,' || CHR(10) ||
            '    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage,' || CHR(10) ||
            '    ROUND(100.0 * SUM(COUNT(*)) OVER (ORDER BY COUNT(*) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COUNT(*)) OVER (), 2) AS cumulative_pct' || CHR(10) ||
            'FROM IDENTIFIER(''$target_database.$target_schema.$target_table'')' || CHR(10) ||
            'WHERE ' || COLUMN_NAME || ' IS NOT NULL' || CHR(10) ||
            'GROUP BY ' || COLUMN_NAME || CHR(10) ||
            'ORDER BY frequency DESC' || CHR(10) ||
            'LIMIT 50',
            CHR(10) || 'UNION ALL' || CHR(10)
        ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) || ';'
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = '$target_database'
      AND TABLE_SCHEMA = '$target_schema'
      AND TABLE_NAME = '$target_table'
      AND DATA_TYPE IN ('VARCHAR', 'STRING', 'TEXT', 'CHAR', 'NUMBER', 'INTEGER', 'BOOLEAN')
    LIMIT 5  -- Limit to first 5 columns for demonstration
);

-- =====================================================================
-- SECTION 6: RELATIONSHIPS & DEPENDENCIES
-- =====================================================================

-- 6.1 Check for potential foreign key relationships
-- This looks for columns that might reference other tables based on naming
SELECT 
    c1.TABLE_NAME AS source_table,
    c1.COLUMN_NAME AS source_column,
    c2.TABLE_NAME AS potential_reference_table,
    c2.COLUMN_NAME AS potential_reference_column,
    'Name-based match' AS relationship_basis
FROM INFORMATION_SCHEMA.COLUMNS c1
JOIN INFORMATION_SCHEMA.COLUMNS c2
    ON c1.COLUMN_NAME = c2.COLUMN_NAME
    AND c1.TABLE_NAME != c2.TABLE_NAME
WHERE c1.TABLE_CATALOG = '$target_database'
  AND c1.TABLE_SCHEMA = '$target_schema'
  AND c1.TABLE_NAME = '$target_table'
  AND c2.TABLE_CATALOG = '$target_database'
  AND c2.TABLE_SCHEMA = '$target_schema'
ORDER BY c1.COLUMN_NAME;

-- =====================================================================
-- SECTION 7: DATA QUALITY SCORE SUMMARY
-- =====================================================================

-- 7.1 Overall Data Quality Scorecard
-- This provides a high-level assessment for source-to-target mapping readiness
SELECT 
    '$target_table' AS table_name,
    CURRENT_TIMESTAMP() AS assessment_date,
    COUNT(DISTINCT column_name) AS total_columns,
    
    -- Completeness Score (based on null rates)
    ROUND(AVG(
        CASE 
            WHEN IS_NULLABLE = 'YES' THEN 70  -- Nullable columns get base score
            ELSE 100  -- Non-nullable columns get full score
        END
    ), 2) AS completeness_score,
    
    -- Structure Score (based on data type appropriateness)
    ROUND(AVG(
        CASE 
            WHEN DATA_TYPE IN ('VARCHAR', 'STRING') THEN 80
            WHEN DATA_TYPE IN ('NUMBER', 'INTEGER', 'FLOAT') THEN 90
            WHEN DATA_TYPE IN ('DATE', 'TIMESTAMP') THEN 95
            ELSE 70
        END
    ), 2) AS structure_score,
    
    -- Documentation Score
    ROUND(100.0 * COUNT(CASE WHEN COMMENT IS NOT NULL AND COMMENT != '' THEN 1 END) / COUNT(*), 2) AS documentation_score,
    
    -- Overall Readiness Assessment
    CASE 
        WHEN COUNT(*) > 100 THEN 'Large table - consider column subsetting'
        WHEN COUNT(CASE WHEN IS_NULLABLE = 'YES' THEN 1 END)::FLOAT / COUNT(*) > 0.5 THEN 'Many nullable columns - validate data quality'
        WHEN COUNT(CASE WHEN COMMENT IS NOT NULL THEN 1 END) = 0 THEN 'No documentation - SME interviews needed'
        ELSE 'Ready for detailed mapping'
    END AS readiness_assessment
    
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = '$target_database'
  AND TABLE_SCHEMA = '$target_schema'
  AND TABLE_NAME = '$target_table';

-- =====================================================================
-- SECTION 8: EXPORT PROFILING RESULTS
-- =====================================================================

-- 8.1 Create a persistent profiling results table
CREATE OR REPLACE TABLE profile_results_$target_table AS
SELECT 
    CURRENT_TIMESTAMP() AS profile_date,
    '$target_database' AS database_name,
    '$target_schema' AS schema_name,
    '$target_table' AS table_name,
    c.ORDINAL_POSITION,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.IS_NULLABLE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.COMMENT AS column_comment
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_CATALOG = '$target_database'
  AND c.TABLE_SCHEMA = '$target_schema'
  AND c.TABLE_NAME = '$target_table'
ORDER BY c.ORDINAL_POSITION;

-- =====================================================================
-- EXECUTION NOTES
-- =====================================================================
-- 1. Set the configuration variables at the top before running
-- 2. Run sections sequentially or select specific sections needed
-- 3. Some dynamic SQL is stored in variables and needs EXECUTE IMMEDIATE
-- 4. Results can be exported to Excel/CSV for source-to-target mapping docs
-- 5. For very large tables (>10M rows), consider sampling strategies
-- 6. Schedule this as a regular job for ongoing data quality monitoring
-- =====================================================================