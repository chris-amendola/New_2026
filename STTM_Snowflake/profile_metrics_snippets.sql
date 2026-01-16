DROP TABLE IF EXISTS STAR_DEV.SEMANTIC_MAPPING.sep_column_distribution;
CREATE OR REPLACE TABLE STAR_DEV.SEMANTIC_MAPPING.sep_column_distribution
(
  table_name           STRING,
  column_name          STRING,
  top_values           ARRAY,   -- array of {value, pct}
  entropy_score        NUMBER,
  skewness             NUMBER,
  modal_value_pct      NUMBER,
  unit_density     STRING
);

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
    column_cursor CURSOR FOR 
        SELECT column_name 
        FROM IDENTIFIER(?) 
        WHERE table_schema = ? 
          AND table_name = ?
          AND data_type NOT IN ('VARIANT', 'ARRAY', 'OBJECT'); -- Exclude semi-structured to prevent errors
BEGIN
    -- Open the cursor using the session's information schema
    -- Note: We use the DB_NAME provided to point to the correct Information Schema
    OPEN column_cursor USING (:DB_NAME || '.INFORMATION_SCHEMA.COLUMNS'), :SCHEMA_NAME, :TABLE_NAME;

    FOR row_variable IN column_cursor DO
        LET col_name STRING := row_variable.column_name;

        dynamic_query := 'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.sep_column_distribution (
                            table_name, column_name, top_values, entropy_score, skewness, modal_value_pct, unit_density) ' ||
            'WITH base_stats AS (
                SELECT 
                    ' || col_name || ' AS val,
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
CALL PROFILE_COLUMN_DISTRIBUTION('STAR_DEV','DIM_MODEL','FACT_ENCOUNTER_PROCEDURE','DIM_CPTCODE_KEY','DIM_DATE_KEY');
