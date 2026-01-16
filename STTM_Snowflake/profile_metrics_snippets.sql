--STAT Table
CREATE OR REPLACE TABLE STAR_DEV.SEMANTIC_MAPPING.sep_column_distribution
(
  table_name           STRING,
  column_name          STRING,
  top_values           ARRAY,   -- array of {value, pct}
  entropy_score        NUMBER,
  skewness             NUMBER,
  modal_value_pct      NUMBER,
  temporal_density     STRING
);

INSERT INTO sep_column_distribution
WITH base AS (
    SELECT
        DIM_CPTCODE_KEY AS val,
        COUNT(*)        AS cnt
    FROM STAR_DEV.DIM_MODEL.FACT_ENCOUNTER_PROCEDURE
    WHERE DIM_CPTCODE_KEY IS NOT NULL
    GROUP BY DIM_CPTCODE_KEY
),
totals AS (
    SELECT SUM(cnt) AS total_cnt
    FROM base
),
ranked AS (
    SELECT
        val,
        cnt,
        (cnt / total_cnt)*100 AS pct,
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
    ) WITHIN GROUP (ORDER BY pct DESC) AS top_values, -- Corrected Syntax
    MAX(pct) AS modal_value_pct
FROM ranked
WHERE rn <= 10
)
, entropy AS (
    SELECT
        -SUM(CASE WHEN pct > 0 THEN pct * LOG(2, pct) ELSE 0 END) AS entropy_score
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
            WHEN COUNT(DISTINCT DIM_DATE_KEY) > 0
                THEN ROUND(COUNT(*) / COUNT(DISTINCT DIM_DATE_KEY), 2)::STRING || ' per day'
            ELSE NULL
        END AS temporal_density
    FROM STAR_DEV.DIM_MODEL.FACT_ENCOUNTER_PROCEDURE
    WHERE DIM_CPTCODE_KEY IS NOT NULL
)

SELECT
    'FACT_ENCOUNTER_PROCEDURE'                AS table_name,
    'DIM_CPTCODE_KEY'            AS column_name,
    top.top_values                  AS top_values,
    ent.entropy_score               AS entropy_score,
    skw.skewness                    AS skewness,
    top.modal_value_pct              AS modal_value_pct,
    tmp.temporal_density            AS temporal_density
FROM top_vals AS top
CROSS JOIN entropy ent
CROSS JOIN skew AS skw
CROSS JOIN temporal AS tmp;

SELECT * FROM sep_column_distribution;

--SELECT * FROM STAR.DIM_MODEL.DIM_CPTCODE WHERE DIM_CPTCODE_KEY=345697;
