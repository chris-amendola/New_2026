-- =====================================================================
-- OPTIMIZED TABLE PROFILING FOR HIGH-VOLUME HEALTHCARE DATA
-- Purpose: Performance-tuned profiling using approximation and single-scan logic
-- =====================================================================

-- CONFIGURATION
SET target_database = 'YOUR_DATABASE';
SET target_schema = 'YOUR_SCHEMA';
SET target_table = 'YOUR_TABLE';
SET sampling_percent = 10; -- Percentage for heavy distribution analysis (1-100)

-- =====================================================================
-- SECTION 1: INSTANT METADATA & STRUCTURE
-- =====================================================================

-- 1.1 Fast Row Counts (Metadata-based, no table scan)
SELECT 
    TABLE_CATALOG, 
    TABLE_SCHEMA, 
    TABLE_NAME, 
    ROW_COUNT AS total_row_count,
    BYTES / POWER(1024, 3) AS size_gb,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = $target_database
  AND TABLE_SCHEMA = $target_schema
  AND TABLE_NAME = $target_table;

-- 1.2 Column Inventory
CREATE OR REPLACE TEMPORARY TABLE col_inventory AS
SELECT 
    ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = $target_database
  AND TABLE_SCHEMA = $target_schema
  AND TABLE_NAME = $target_table;

-- =====================================================================
-- SECTION 2: SINGLE-SCAN COLUMN PROFILING (Dynamic Wide Query)
-- =====================================================================

-- This block generates a single SELECT statement that profiles all columns at once.
-- We use APPROX functions to handle high cardinality efficiently.
SET profiling_sql = (
    SELECT 
        'CREATE OR REPLACE TRANSIENT TABLE profile_stats_raw AS SELECT ' || 
        LISTAGG(
            'COUNT(*) - COUNT(' || COLUMN_NAME || ') AS ' || COLUMN_NAME || '_nulls, ' ||
            'APPROX_COUNT_DISTINCT(' || COLUMN_NAME || ') AS ' || COLUMN_NAME || '_distinct, ' ||
            CASE 
                WHEN DATA_TYPE IN ('NUMBER', 'FLOAT', 'INTEGER', 'DECIMAL') 
                    THEN 'MIN(' || COLUMN_NAME || ') AS ' || COLUMN_NAME || '_min, ' ||
                         'MAX(' || COLUMN_NAME || ') AS ' || COLUMN_NAME || '_max, ' ||
                         'APPROX_PERCENTILE(' || COLUMN_NAME || ', 0.5) AS ' || COLUMN_NAME || '_median'
                WHEN DATA_TYPE IN ('VARCHAR', 'STRING', 'TEXT') 
                    THEN 'MIN(LENGTH(' || COLUMN_NAME || ')) AS ' || COLUMN_NAME || '_min, ' ||
                         'MAX(LENGTH(' || COLUMN_NAME || ')) AS ' || COLUMN_NAME || '_max, ' ||
                         'NULL AS ' || COLUMN_NAME || '_median'
                WHEN DATA_TYPE IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ') 
                    THEN 'MIN(' || COLUMN_NAME || ')::STRING AS ' || COLUMN_NAME || '_min, ' ||
                         'MAX(' || COLUMN_NAME || ')::STRING AS ' || COLUMN_NAME || '_max, ' ||
                         'NULL AS ' || COLUMN_NAME || '_median'
                ELSE 'NULL AS ' || COLUMN_NAME || '_min, NULL AS ' || COLUMN_NAME || '_max, NULL AS ' || COLUMN_NAME || '_median'
            END,
            ', '
        ) || 
        ' FROM IDENTIFIER(''$target_database.$target_schema.$target_table'');'
    FROM col_inventory
);

EXECUTE IMMEDIATE $profiling_sql;

-- =====================================================================
-- SECTION 3: DATA QUALITY & KEY CANDIDATES (Unpivoted for reporting)
-- =====================================================================

-- Transform the wide stats table into a usable long format
CREATE OR REPLACE TABLE profile_summary_report AS
WITH total_rows AS (
    SELECT ROW_COUNT FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_CATALOG = $target_database AND TABLE_NAME = $target_table
)
SELECT 
    f.key AS column_name,
    -- Logic to split the unpivoted keys into metrics
    SPLIT_PART(f.key, '_', -1) AS metric_type,
    f.value AS metric_value
FROM profile_stats_raw
UNPIVOT(value FOR key IN (SELECT column_name FROM col_inventory)) f;

-- =====================================================================
-- SECTION 4: HEALTHCARE PATTERN DETECTION (CTE Optimized)
-- =====================================================================

WITH pattern_config AS (
    SELECT * FROM (VALUES 
        ('MRN|PATIENT|SUBJECT', 'Patient Identifier'),
        ('ENCOUNTER|VISIT|ADM', 'Encounter Identifier'),
        ('PROVIDER|PHYSICIAN|NPI', 'Provider Info'),
        ('DIAG|ICD|DX', 'Clinical Code: Diagnosis'),
        ('PROC|CPT|PX', 'Clinical Code: Procedure'),
        ('DRUG|MED|RX|NDC', 'Pharmacy/Medication'),
        ('GENDER|SEX|RACE|ETHNIC|DOB', 'Demographics'),
        ('DATE|TIME|DTTM', 'Temporal Field')
    ) AS t (regex, category)
)
SELECT 
    c.COLUMN_NAME,
    c.DATA_TYPE,
    COALESCE(p.category, 'Other/Attribute') AS suspected_healthcare_type,
    CASE 
        WHEN c.DATA_TYPE = 'VARCHAR' AND p.category LIKE 'Clinical%' THEN 'Validate against Terminology Server (LOINC/ICD)'
        WHEN c.DATA_TYPE = 'VARCHAR' AND p.category = 'Patient Identifier' THEN 'Check for PII/PHI masking'
        ELSE 'Standard Integration'
    END AS integration_notes
FROM col_inventory c
LEFT JOIN pattern_config p 
    ON REGEXP_LIKE(c.COLUMN_NAME, p.regex, 'i');

-- =====================================================================
-- SECTION 5: VALUE DISTRIBUTION (Sampled for Performance)
-- =====================================================================

-- Use TABLESAMPLE to get distribution without scanning billions of rows
SET dist_sql = (
    SELECT 
        'SELECT ''' || COLUMN_NAME || ''' AS col, ' || COLUMN_NAME || '::STRING AS val, COUNT(*) AS freq ' ||
        'FROM IDENTIFIER(''$target_database.$target_schema.$target_table'') ' ||
        'TABLESAMPLE (' || $sampling_percent || ' PERCENT) ' ||
        'GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10'
    FROM col_inventory
    WHERE DATA_TYPE IN ('VARCHAR', 'BOOLEAN')
    LIMIT 3 -- Top 3 columns for sample
);

-- EXECUTE IMMEDIATE $dist_sql;

-- =====================================================================
-- SECTION 6: READINESS SCORECARD
-- =====================================================================

SELECT 
    '$target_table' AS table_name,
    COUNT(*) AS col_count,
    ROUND(AVG(CASE WHEN IS_NULLABLE = 'NO' THEN 100 ELSE 80 END), 2) AS structural_integrity_score,
    COUNT(CASE WHEN COMMENT IS NOT NULL THEN 1 END) AS documented_cols,
    CASE 
        WHEN COUNT(CASE WHEN COMMENT IS NOT NULL THEN 1 END) = 0 THEN 'ACTION REQUIRED: Missing Metadata'
        ELSE 'Proceed to Mapping'
    END AS status
FROM col_inventory;