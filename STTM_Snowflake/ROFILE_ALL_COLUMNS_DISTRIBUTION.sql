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
          AND column_name NOT IN ('ETL_CREATED_DATE','ETL_UPDATED_DATE','ETL_CREATED_BY','ETL_UPDATED_BY')
          AND data_type NOT IN ('VARIANT', 'ARRAY', 'OBJECT','BOOLEAN'); -- Exclude semi-structured to prevent errors
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
                    ' || col_name || ' AS val,
                    COUNT(*) AS val_cnt,
                    SUM(COUNT(*)) OVER() AS total_cnt
                FROM IDENTIFIER(''' || FULL_TABLE_PATH || ''')
                GROUP BY 1 
            ),
            entropy AS (
               SELECT 
                   COALESCE( 
                            SUM( 
                                CASE 
                                  WHEN total_cnt > 0 AND val_cnt > 0 
                                  THEN -1 * DIV0(val_cnt,total_cnt) * (LN(NULLIF(DIV0(val_cnt, total_cnt), 0)) / LN(2)) 
                                  ELSE 0 
                                END ), 0)
                                AS entropy_score FROM base_stats
            ),
            ranked_stats AS (
                SELECT 
                    val, 
                    val_cnt,
                    total_cnt,
                    ROW_NUMBER() OVER (ORDER BY val_cnt DESC) as rnk
                FROM base_stats
            ),
            top_vals AS (
                SELECT 
                    ARRAY_AGG(val) WITHIN GROUP (ORDER BY val_cnt DESC) AS top_values,
                    MAX(val_cnt) / NULLIF(MAX(total_cnt), 0) AS modal_value_pct
                FROM ranked_stats
                WHERE rnk <= 5
            ),
            skew AS (
                SELECT 
                    SKEW(
                         COALESCE(
                                  -- 1. Try to treat it as a number (Legacy numeric strings)
                                  TRY_TO_DOUBLE(' || col_name ||'::VARCHAR),
                                  -- 2. Try to treat it as a Boolean (Native or "true"/"false" strings)
                                  -- Maps TRUE/TRUE-like to 1.0, FALSE/FALSE-like to 0.0
                                  IFF(TRY_TO_BOOLEAN(' || col_name || '::VARCHAR), 1.0, 0.0))
                                 ) AS skewness
                            FROM IDENTIFIER(''' || FULL_TABLE_PATH ||''')
            ),
            temporal AS (
                SELECT COUNT(*)/COUNT(DISTINCT ' || DATE_COLUMN || ') AS unit_density
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
            CROSS JOIN temporal t
            ';

        EXECUTE IMMEDIATE :dynamic_query;
    END FOR;

    //RETURN 'Statistics successfully updated for all columns in ' || FULL_TABLE_PATH;
    RETURN DYNAMIC_QUERY;
END;

CALL PROFILE_ALL_COLUMNS_DISTRIBUTION('STAR_DEV','DIM_MODEL','FACT_ENCOUNTER_PROCEDURE','DIM_DATE_KEY');

--SELECT * FROM STAR_DEV.SEMANTIC_MAPPING.COLUMN_DISTRIBUTIONS; 
