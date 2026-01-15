CREATE OR REPLACE PROCEDURE PROFILE_SOURCE_TABLE(
    P_DATABASE      STRING,
    P_SCHEMA        STRING,
    P_TABLE         STRING,
    P_SAMPLE_PCT    NUMBER DEFAULT 100   -- 100 = full scan
)
RETURNS TABLE (
    column_name                STRING,
    data_type                  STRING,
    is_nullable                STRING,
    rows_scanned               NUMBER,
    non_null_count             NUMBER,
    null_count                 NUMBER,
    pct_populated              NUMBER,
    distinct_count             NUMBER,
    pct_distinct_non_null      NUMBER,
    min_value                  STRING,
    max_value                  STRING
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    sql_text STRING;
BEGIN

    SELECT LISTAGG(stmt, ' UNION ALL ')
    INTO :sql_text
    FROM (
        SELECT
            'SELECT
                ''' || c.column_name || '''                        AS column_name,
                ''' || c.data_type   || '''                        AS data_type,
                ''' || c.is_nullable || '''                        AS is_nullable,
                COUNT(*)                                           AS rows_scanned,
                COUNT(' || c.column_name || ')                     AS non_null_count,
                COUNT(*) - COUNT(' || c.column_name || ')          AS null_count,
                ROUND(COUNT(' || c.column_name || ') / NULLIF(COUNT(*),0) * 100, 2)
                                                                  AS pct_populated,
                COUNT(DISTINCT ' || c.column_name || ')             AS distinct_count,
                ROUND(
                    COUNT(DISTINCT ' || c.column_name || ') /
                    NULLIF(COUNT(' || c.column_name || '),0) * 100, 2
                )                                                   AS pct_distinct_non_null,
                CAST(MIN(' || c.column_name || ') AS STRING)       AS min_value,
                CAST(MAX(' || c.column_name || ') AS STRING)       AS max_value
            FROM ' || P_DATABASE || '.' || P_SCHEMA || '.' || P_TABLE || '
            ' ||
            CASE
                WHEN P_SAMPLE_PCT < 100
                    THEN ' SAMPLE (' || P_SAMPLE_PCT || ')'
                ELSE ''
            END
            AS stmt
        FROM IDENTIFIER(P_DATABASE || '.INFORMATION_SCHEMA.COLUMNS') c
        WHERE c.table_schema = P_SCHEMA
          AND c.table_name   = P_TABLE
        ORDER BY c.ordinal_position
    );

    RETURN TABLE(
        EXECUTE IMMEDIATE :sql_text
    );

END;
$$;

CREATE OR REPLACE VIEW STM_SOURCE_PROFILE AS
SELECT
    column_name,

    data_type,
    is_nullable,

    rows_scanned,
    pct_populated,

    distinct_count,
    pct_distinct_non_null,

    min_value,
    max_value,

    /* Analyst-facing interpretation hints */
    CASE
        WHEN pct_distinct_non_null >= 99 THEN 'Likely Identifier'
        WHEN pct_distinct_non_null BETWEEN 5 AND 95 THEN 'Reference / Code'
        ELSE 'Low Information'
    END AS inferred_role,

    CASE
        WHEN data_type ILIKE '%DATE%' OR data_type ILIKE '%TIME%'
            THEN 'Temporal'
        WHEN data_type ILIKE '%CHAR%' AND pct_distinct_non_null < 5
            THEN 'Enum / Flag'
        ELSE 'Review'
    END AS mapping_hint

FROM TABLE(
    PROFILE_SOURCE_TABLE(
        'SRC_DB',
        'CLINICAL',
        'ENCOUNTER',
        10
    )
);