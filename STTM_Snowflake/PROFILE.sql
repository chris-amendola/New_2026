USE DATABASE STAR_DEV;
USE SCHEMA SEMANTIC_MAPPING;

DROP TABLE IF EXISTS STAR_DEV.SEMANTIC_MAPPING.COLUMN_SUMMARY_RESULTS;
CREATE OR REPLACE TABLE STAR_DEV.SEMANTIC_MAPPING.COLUMN_SUMMARY_RESULTS (
        table_name STRING,
        column_name STRING,
        data_type STRING,
        is_nullable STRING,
        rows_scanned NUMBER,
        non_null_count NUMBER,
        null_count NUMBER,
        pct_populated FLOAT,
        distinct_count NUMBER,
        uniqueness_pct NUMBER,
        pct_distinct_non_null FLOAT,
        min_value STRING,
        max_value STRING
    );

DROP TABLE IF EXISTS STAR_DEV.SEMANTIC_MAPPING.COLUMN_DISTRIBUTIONS;
CREATE OR REPLACE TABLE STAR_DEV.SEMANTIC_MAPPING.COLUMN_DISTRIBUTIONS
(
  table_name           STRING,
  column_name          STRING,
  top_values           ARRAY,   -- array of {value, pct}
  entropy_score        NUMBER,
  skewness             NUMBER,
  modal_value_pct      NUMBER,
  unit_density     STRING
);

----    
CREATE OR REPLACE PROCEDURE PROFILE_COLUMNS(
    DB_NAME STRING, 
    SCHEMA_NAME STRING, 
    TABLE_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    col_cursor CURSOR FOR 
        SELECT table_name,column_name, data_type, is_nullable 
        FROM STAR_DEV.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = ? 
          AND table_schema = ?
        ORDER BY ORDINAL_POSITION;
    insert_sql STRING;
    full_table_path STRING:= :DB_NAME || '.' || :SCHEMA_NAME || '.' || :TABLE_NAME;
BEGIN
    -- Loop through each column and execute the summary query
    OPEN col_cursor USING (:TABLE_NAME, :SCHEMA_NAME);
    FOR record IN col_cursor DO
        insert_sql := 'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.COLUMN_SUMMARY_RESULTS ' ||
                      'SELECT 
                      '''|| record.table_name ||''' as table_name, 
                      '''|| record.column_name ||''' as column_name, 
                      ''' || record.data_type || ''' as data_type,
                      ''' || record.is_nullable ||''' as is_nullable,
                       COUNT(*) as rows_scanned, ' ||
                      'COUNT("' || record.column_name || '") as non_null_count, ' ||
                      'COUNT(*) - COUNT("' || record.column_name || '") as null_count, ' ||
                      'ROUND(COUNT("' || record.column_name || '") / NULLIF(COUNT(*), 0) * 100, 2) as pct_populated, ' ||
                      'COUNT(DISTINCT "' || record.column_name || '") as distinct_count, ' ||
                      'ROUND((COUNT(DISTINCT"' || record.column_name || '")'||' / COUNT(*))*100,2) as uniqueness_pct,' ||
                      'ROUND(COUNT(DISTINCT "' || record.column_name || '") / NULLIF(COUNT("' || record.column_name || '"), 0)*100, 2) as pct_distinct_non_null, ' ||
                      'CAST(MIN("' || record.column_name || '") AS STRING) as min_value, ' ||
                      'CAST(MAX("' || record.column_name || '") AS STRING) as max_value ' ||
                      'FROM ' || :full_table_path;
        
        EXECUTE IMMEDIATE :insert_sql;
    END FOR;

    RETURN 'Summary completed for table: ' || :full_table_path;
END;

CALL PROFILE_COLUMNS('STAR_DEV','DIM_MODEL','FACT_ENCOUNTER_PROCEDURE');
SELECT * FROM STAR_DEV.SEMANTIC_MAPPING.COLUMN_SUMMARY_RESULTS;

CREATE OR REPLACE PROCEDURE PROFILE_TABLES(SCHEMA_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var result_log = [];

    // Get all table names from INFORMATION_SCHEMA
    var get_tables_sql = `
        SELECT TABLE_NAME 
        FROM STAR_DEV.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = UPPER('${SCHEMA_NAME}')
          AND TABLE_TYPE = 'BASE TABLE'
          -- Profile for Incoming data only
          AND STARTSWITH(TABLE_NAME, 'STG_')
        ORDER BY TABLE_NAME
    `;
    var stmt = snowflake.createStatement({ sqlText: get_tables_sql });
    var rs = stmt.execute();

    // Loop through each table and call the processing procedure
    while (rs.next()) {
        //var tbl_name = SCHEMA_NAME + '.' + rs.getColumnValue(1);
        var tbl_name = rs.getColumnValue(1);

        try {
            var call_sql = `CALL PROFILE_COLUMNS('STAR_DEV', 'GGASTRO','${tbl_name}');`;
            snowflake.execute({ sqlText: call_sql });
            result_log.push(`✅ Success: ${tbl_name}`);
        } catch (err) {
            result_log.push(`❌ Failed: ${tbl_name} - ${err.message}`);
        }
    }

    return result_log.join('\n');
$$;

CALL PROFILE_TABLES('GGASTRO');

SELECT table_name,COUNT(DISTINCT table_name)  
FROM STAR_DEV.SEMANTIC_MAPPING.COLUMN_SUMMARY_RESULTS
GROUP BY table_name;

----
CREATE OR REPLACE PROCEDURE PROFILE_ALL_COLUMNS_DISTRIBUTION(
    DB_NAME STRING, 
    SCHEMA_NAME STRING, 
    TABLE_NAME STRING,
    DATE_COLUMN STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    dynamic_query STRING;
    FULL_TABLE_PATH STRING := :DB_NAME || '.' || :SCHEMA_NAME || '.' || :TABLE_NAME;
    INFO_SCHEMA STRING := :DB_NAME || '.INFORMATION_SCHEMA.COLUMNS';
    column_cursor CURSOR FOR 
        SELECT column_name 
        FROM IDENTIFIER(?) 
        WHERE table_schema = ? 
          AND table_name = ?
          AND column_name NOT IN ('ETL_UPDATED_DATE')
          AND data_type NOT IN ('VARIANT', 'ARRAY', 'OBJECT'); -- Exclude semi-structured to prevent errors
BEGIN
    -- Open the cursor using the session's information schema
    -- Note: We use the DB_NAME provided to point to the correct Information Schema
    OPEN column_cursor USING (:INFO_SCHEMA, :SCHEMA_NAME, :TABLE_NAME);

    FOR row_variable IN column_cursor DO
        LET col_name STRING := row_variable.column_name;

        dynamic_query := 'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.COLUMN_DISTRIBUTIONS (
                            table_name, column_name, top_values, entropy_score, skewness, modal_value_pct, unit_density) ' ||
            'WITH base_stats AS (
                SELECT 
                    ''' || col_name || ''' AS val,
                    COUNT(*)::FLOAT AS val_cnt,
                    SUM(COUNT(*)) OVER()::FLOAT AS total_cnt
                FROM IDENTIFIER(''' || FULL_TABLE_PATH || ''')
                GROUP BY 1
            ),
            entropy AS (
                SELECT 
                    COALESCE(SUM(-1 * (val_cnt / total_cnt) * LOG(2, (val_cnt / total_cnt))), 0)::FLOAT AS entropy_score
                FROM base_stats
            ),
            top_vals AS (
                SELECT 
                    ARRAY_AGG(val) WITHIN GROUP (ORDER BY val_cnt DESC) AS top_values,
                    MAX(val_cnt) / NULLIF(MAX(total_cnt), 0) AS modal_value_pct
                FROM base_stats
            ),
            skew AS (
                -- Note: SKEW may fail on non-numeric strings; logic kept from original
                SELECT SKEW(TRY_CAST(' || col_name || ' AS FLOAT)) AS skewness 
                FROM IDENTIFIER(''' || FULL_TABLE_PATH || ''')
            ),
            temporal AS (
                SELECT (COUNT(*)::FLOAT / NULLIF(COUNT(DISTINCT ' || DATE_COLUMN || '), 0))::STRING AS unit_density
                FROM IDENTIFIER(''' || FULL_TABLE_PATH || ''')
            )
            SELECT
                ''' || FULL_TABLE_PATH || ''',
                ''' || col_name || ''',
                tv.top_values,
                e.entropy_score,
                s.skewness,
                tv.modal_value_pct,
                t.unit_density
            FROM top_vals tv
            CROSS JOIN entropy e
            CROSS JOIN skew s
            CROSS JOIN temporal t';

        EXECUTE IMMEDIATE :dynamic_query;
    END FOR;

    RETURN 'Statistics successfully updated for all columns in ' || FULL_TABLE_PATH;
END;

CALL PROFILE_ALL_COLUMNS_DISTRIBUTION('STAR_DEV','DIM_MODEL','FACT_ENCOUNTER_PROCEDURE','DIM_DATE_KEY');
