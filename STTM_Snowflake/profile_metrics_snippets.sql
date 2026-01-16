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

CREATE OR REPLACE PROCEDURE PROFILE_COLUMN_DISTRIBUTION(
    DB_NAME STRING, 
    SCHEMA_NAME STRING, 
    TABLE_NAME STRING,
    COLUMN_NAME STRING,
    DATE_COLUMN STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    dynamic_query STRING;
    FULL_TABLE_PATH STRING := :DB_NAME || '.' || :SCHEMA_NAME || '.' || :TABLE_NAME;
BEGIN
    -- Using FLOAT casts to prevent NUMBER(38,12) overflow during statistical math
    dynamic_query := 'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.sep_column_distribution (
                    table_name, column_name, top_values, entropy_score, skewness, modal_value_pct, unit_density) ' ||
        'WITH base_stats AS (
            SELECT 
                ' || COLUMN_NAME || ' AS val,
                COUNT(*)::FLOAT AS val_cnt,
                SUM(COUNT(*)) OVER()::FLOAT AS total_cnt
            FROM IDENTIFIER(''' || FULL_TABLE_PATH || ''')
            GROUP BY 1
        ),
        entropy AS (
            -- Shannon Entropy: -Sum(p * log2(p)) cast to FLOAT
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
            -- Cast column to FLOAT before cubing to avoid 38-digit overflow
            SELECT SKEW(IDENTIFIER(''' || COLUMN_NAME || ''')::FLOAT) AS skewness 
            FROM IDENTIFIER(''' || FULL_TABLE_PATH || ''')
        ),
        temporal AS (
            SELECT (COUNT(*)::FLOAT / NULLIF(COUNT(DISTINCT IDENTIFIER(''' || DATE_COLUMN || ''')), 0))::STRING AS unit_density
            FROM IDENTIFIER(''' || FULL_TABLE_PATH || ''')
        )
        SELECT
            ''' || FULL_TABLE_PATH || ''',
            ''' || COLUMN_NAME || ''',
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

    RETURN 'Statistics successfully updated for ' || FULL_TABLE_PATH;
END;

CALL PROFILE_COLUMN_DISTRIBUTION('STAR_DEV','DIM_MODEL','FACT_ENCOUNTER_PROCEDURE','DIM_CPTCODE_KEY','DIM_DATE_KEY');
