CREATE OR REPLACE PROCEDURE insert_sep_column_distribution(
    TABLE_NAME_IN STRING,
    COLUMN_NAME_IN STRING,
    DATE_COLUMN_IN STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    dynamic_query STRING;
BEGIN
    dynamic_query := '
        INSERT INTO sep_column_distribution (
            table_name, column_name, top_values, entropy_score, 
            skewness, modal_value_pct, temporal_density, created_at
        )
        WITH base_stats AS (
            -- Calculate counts for entropy and modal value
            SELECT 
                ' || COLUMN_NAME_IN || ' AS val,
                COUNT(*) AS val_cnt,
                SUM(COUNT(*)) OVER() AS total_cnt
            FROM IDENTIFIER(''' || TABLE_NAME_IN || ''')
            GROUP BY 1
        ),
        entropy AS (
            -- Shannon Entropy calculation: -Sum(p * log2(p))
            SELECT 
                -SUM((val_cnt / total_cnt) * (LN(val_cnt / total_cnt) / LN(2))) AS entropy_score
            FROM base_stats
        ),
        top_vals AS (
            -- Calculate Modal Value % and aggregate top values
            SELECT 
                ARRAY_AGG(val) WITHIN GROUP (ORDER BY val_cnt DESC) AS top_values,
                MAX(val_cnt) / MAX(total_cnt) AS modal_value_pct
            FROM base_stats
        ),
        skew AS (
            -- Built-in Snowflake Skewness
            SELECT SKEW(' || COLUMN_NAME_IN || ') AS skewness 
            FROM IDENTIFIER(''' || TABLE_NAME_IN || ''')
        ),
        temporal AS (
            -- Density: Total rows divided by distinct days
            SELECT COUNT(*) / NULLIF(COUNT(DISTINCT ' || DATE_COLUMN_IN || '), 0) AS temporal_density
            FROM IDENTIFIER(''' || TABLE_NAME_IN || ''')
        )
        SELECT
            ' || QUOTE_LITERAL(TABLE_NAME_IN) || ',
            ' || QUOTE_LITERAL(COLUMN_NAME_IN) || ',
            tv.top_values,
            e.entropy_score,
            s.skewness,
            tv.modal_value_pct,
            t.temporal_density,
            CURRENT_TIMESTAMP
        FROM top_vals tv
        CROSS JOIN entropy e
        CROSS JOIN skew s
        CROSS JOIN temporal t';

    EXECUTE IMMEDIATE :dynamic_query;

    RETURN 'Statistics successfully updated for ' || TABLE_NAME_IN;
END;
$$;
