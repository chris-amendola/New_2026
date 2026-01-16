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
     FROM STAR_DEV.GGASTRO.STG_ORDER
     UNION ALL' AS profiling_sql
FROM STAR.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'GGASTRO'
  AND table_name   = 'STG_ORDER'
ORDER BY ordinal_position;


CREATE OR REPLACE TEMPORARY TABLE STAR_DEV.SEMANTIC_MAPPING.COLUMN_SUMMARY_RESULTS (
        column_name STRING,
        data_type STRING,
        is_nullable STRING,
        rows_scanned NUMBER,
        non_null_count NUMBER,
        null_count NUMBER,
        pct_populated FLOAT,
        distinct_count NUMBER,
        pct_distinct_non_null FLOAT,
        min_value STRING,
        max_value STRING
    );
    
CREATE OR REPLACE PROCEDURE PROFILE(
    DB_NAME STRING, 
    SCHEMA_NAME STRING, 
    TABLE_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    col_cursor CURSOR FOR 
        SELECT column_name, data_type, is_nullable 
        FROM STAR_DEV.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = ? 
          AND table_schema = ?
        ORDER BY ORDINAL_POSITION;
    insert_sql STRING;
    --full_table_path STRING:= :DB_NAME || '.' || :SCHEMA_NAME || '.' || :TABLE_NAME;
    full_table_path STRING DEFAULT 'STAR_DEV.GGASTRO.STG_ORDER';
BEGIN
    -- Loop through each column and execute the summary query
    OPEN col_cursor USING (:TABLE_NAME, :SCHEMA_NAME);
    FOR record IN col_cursor DO
        insert_sql := 'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.COLUMN_SUMMARY_RESULTS ' ||
                      'SELECT 
                       '''|| record.column_name ||''' as column_name, 
                      ''' || record.data_type || ''' as data_type,
                      ''' || record.is_nullable ||''' as is_nullable,
                       COUNT(*) as rows_scanned, ' ||
                      'COUNT("' || record.column_name || '") as non_null_count, ' ||
                      'COUNT(*) - COUNT("' || record.column_name || '") as null_count, ' ||
                      'ROUND(COUNT("' || record.column_name || '") / NULLIF(COUNT(*), 0) * 100, 2) as pct_populated, ' ||
                      'COUNT(DISTINCT "' || record.column_name || '") as distinct_count, ' ||
                      'ROUND(COUNT(DISTINCT "' || record.column_name || '") / NULLIF(COUNT("' || record.column_name || '"), 0)*100, 2) as pct_distinct_non_null, ' ||
                      'CAST(MIN("' || record.column_name || '") AS STRING) as min_value, ' ||
                      'CAST(MAX("' || record.column_name || '") AS STRING) as max_value ' ||
                      'FROM ' || :full_table_path;
        
        EXECUTE IMMEDIATE :insert_sql;
    END FOR;

    RETURN 'Summary completed for table: ' || :full_table_path;
END;

CALL PROFILE('STAR_DEV', 'GGASTRO', 'STG_ORDER');

