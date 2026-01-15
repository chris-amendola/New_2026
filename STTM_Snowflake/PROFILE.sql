SELECT
    'SELECT
        ''' || column_name || ''' AS column_name,
        ''' || data_type   || ''' AS data_type,
        ''' || is_nullable || ''' AS is_nullable,
        COUNT(*) AS rows_scanned,
        COUNT(' || column_name || ') AS non_null_count,
        COUNT(*) - COUNT(' || column_name || ') AS null_count,
        ROUND(COUNT(' || column_name || ') / NULLIF(COUNT(*),0) * 100, 2)
            AS pct_populated,
        COUNT(DISTINCT ' || column_name || ') AS distinct_count,
        ROUND(
            COUNT(DISTINCT ' || column_name || ') /
            NULLIF(COUNT(' || column_name || '),0) * 100, 2
        ) AS pct_distinct_non_null,
        CAST(MIN(' || column_name || ') AS STRING) AS min_value,
        CAST(MAX(' || column_name || ') AS STRING) AS max_value
     FROM SRC_DB.CLINICAL.ENCOUNTER
     UNION ALL'
AS profiling_sql
FROM SRC_DB.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'CLINICAL'
  AND table_name   = 'ENCOUNTER'
ORDER BY ordinal_position;

CREATE OR REPLACE TABLE STM_SOURCE_PROFILE AS
<PASTED GENERATED SQL>;

SELECT
    column_name,
    data_type,
    is_nullable,
    pct_populated,
    pct_distinct_non_null,
    min_value,
    max_value,

    CASE
        WHEN pct_distinct_non_null >= 99 THEN 'Identifier candidate'
        WHEN pct_distinct_non_null BETWEEN 5 AND 95 THEN 'Code / reference'
        ELSE 'Low signal'
    END AS inferred_role

FROM STM_SOURCE_PROFILE
ORDER BY column_name;