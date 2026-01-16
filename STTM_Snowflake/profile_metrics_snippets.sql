--STAT Table
sep_column_distribution
(
  table_name           STRING,
  column_name          STRING,
  top_values           ARRAY,   -- array of {value, pct}
  entropy_score        NUMBER,
  skewness             NUMBER,
  modal_value_pct      NUMBER,
  temporal_density     STRING,
  created_at           TIMESTAMP
);

WITH base AS (
    SELECT
        procedure_cpt_code AS val,
        COUNT(*)           AS cnt
    FROM procedure_fact
    WHERE procedure_cpt_code IS NOT NULL
    GROUP BY procedure_cpt_code
),
totals AS (
    SELECT SUM(cnt) AS total_cnt
    FROM base
),
ranked AS (
    SELECT
        val,
        cnt,
        cnt / total_cnt AS pct,
        ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rn
    FROM base
    CROSS JOIN totals
)

, top_vals AS (
    SELECT
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'value', val,
                'pct', ROUND(pct, 6)
            )
            ORDER BY pct DESC
        ) AS top_values,
        MAX(pct) AS modal_value_pct
    FROM ranked
    WHERE rn <= 10
)

, entropy AS (
    SELECT
        -SUM(pct * LOG(2, pct)) AS entropy_score
    FROM ranked
)

, skew AS (
    SELECT
        (MAX(cnt)::FLOAT / AVG(cnt)) AS skewness
    FROM base
)

, temporal AS (
    SELECT
        CASE
            WHEN COUNT(DISTINCT procedure_date) > 0
                THEN ROUND(COUNT(*) / COUNT(DISTINCT procedure_date), 2)::STRING || ' per day'
            ELSE NULL
        END AS temporal_density
    FROM procedure_fact
    WHERE procedure_cpt_code IS NOT NULL
)

INSERT INTO sep_column_distribution
SELECT
    'procedure_fact'                AS table_name,
    'procedure_cpt_code'             AS column_name,
    tv.top_values                    AS top_values,
    e.entropy_score                  AS entropy_score,
    s.skewness                       AS skewness,
    tv.modal_value_pct               AS modal_value_pct,
    t.temporal_density               AS temporal_density,
    CURRENT_TIMESTAMP                AS created_at
FROM top_vals tv
CROSS JOIN entropy e
CROSS JOIN skew s
CROSS JOIN temporal t;


