--
-- What is it *really* (type, max length, numeric scale, true nullability)?
-- How populated is it?
-- How unique is it (identifier candidate)?
-- What values actually appear (domains, min/max, outliers)?
-- Is it stable or volatile?
-- Does it *smell* like PHI, a key, a code, or a timestamp?

-- Single pass per column

SET PROFILE_DB     = 'SOURCE_DB';
SET PROFILE_SCHEMA = 'SOURCE_SCHEMA';
SET PROFILE_TABLE  = 'SOURCE_TABLE';


---

-- 1 Column metadata + physical characteristics

--This anchors the profile and catches upstream schema lies.

WITH column_metadata AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable
    FROM IDENTIFIER($PROFILE_DB || '.INFORMATION_SCHEMA.COLUMNS')
    WHERE table_schema = $PROFILE_SCHEMA
      AND table_name   = $PROFILE_TABLE
)
SELECT *
FROM column_metadata
ORDER BY ordinal_position;


-- 2 Core data profiling metrics (population, uniqueness, sparsity)

WITH base AS (
    SELECT *
    FROM IDENTIFIER($PROFILE_DB || '.' || $PROFILE_SCHEMA || '.' || $PROFILE_TABLE)
),
row_counts AS (
    SELECT COUNT(*) AS total_rows FROM base
),
column_profile AS (
    SELECT
        column_name,

        COUNT(*)                                    AS rows_scanned,
        COUNT(column_value)                         AS non_null_count,
        COUNT(*) - COUNT(column_value)              AS null_count,

        COUNT(DISTINCT column_value)                AS distinct_count,

        MIN(column_value)                           AS min_value,
        MAX(column_value)                           AS max_value

    FROM base
    UNPIVOT(column_value FOR column_name IN (*))
)
SELECT
    p.column_name,
    r.total_rows,

    p.non_null_count,
    ROUND(p.non_null_count / r.total_rows * 100, 2) AS pct_populated,

    p.null_count,
    ROUND(p.null_count / r.total_rows * 100, 2)     AS pct_null,

    p.distinct_count,
    ROUND(p.distinct_count / NULLIF(p.non_null_count,0) * 100, 2)
                                                    AS pct_distinct_non_null,

    p.min_value,
    p.max_value

FROM column_profile p
CROSS JOIN row_counts r
ORDER BY p.column_name;

-- For STM
-- `pct_populated < 95%` → nullable target or late-arriving data
-- `pct_distinct_non_null ≈ 100%` → key candidate
-- min/max → bad dates, sentinel values (`1900-01-01`, `9999-12-31`)

---

-- 3 Length & format profiling (text fields)

-- Catches **free-text pretending to be codes**.

WITH base AS (
    SELECT *
    FROM IDENTIFIER($PROFILE_DB || '.' || $PROFILE_SCHEMA || '.' || $PROFILE_TABLE)
),
text_profile AS (
    SELECT
        column_name,
        MIN(LENGTH(column_value)) AS min_length,
        MAX(LENGTH(column_value)) AS max_length,
        AVG(LENGTH(column_value)) AS avg_length
    FROM base
    UNPIVOT(column_value FOR column_name IN (*))
    WHERE TYPEOF(column_value) = 'VARCHAR'
      AND column_value IS NOT NULL
    GROUP BY column_name
)
SELECT *
FROM text_profile
ORDER BY column_name;


-- Red flags this exposes**

- `avg_length ≈ max_length` → fixed-width codes
- `max_length >> expected` → comments, concatenations, HL7 garbage
- `min_length = 0` → empty strings masquerading as NULLs

---

-- 4 Domain frequency (top values per column)

-- Essential for **code mappings, enums, flags, and dirty booleans**.

WITH base AS (
    SELECT *
    FROM IDENTIFIER($PROFILE_DB || '.' || $PROFILE_SCHEMA || '.' || $PROFILE_TABLE)
),
domain_counts AS (
    SELECT
        column_name,
        column_value,
        COUNT(*) AS value_count
    FROM base
    UNPIVOT(column_value FOR column_name IN (*))
    WHERE column_value IS NOT NULL
    GROUP BY column_name, column_value
),
ranked_domains AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY column_name
            ORDER BY value_count DESC
        ) AS value_rank
    FROM domain_counts
)
SELECT
    column_name,
    column_value,
    value_count
FROM ranked_domains
WHERE value_rank <= 10
ORDER BY column_name, value_rank;

-- STM payoff**

- Exposes `Y/N/Yes/No/1/0/True/False`
- Reveals ICD/CPT/LOINC shape *before* formal mapping
- Flags junk placeholders (`UNKNOWN`, `ZZZ`, `TEST`)

---

--5 Identifier & PHI heuristics (practical, not perfect)

--You *want* these signals early.

WITH base AS (
    SELECT *
    FROM IDENTIFIER($PROFILE_DB || '.' || $PROFILE_SCHEMA || '.' || $PROFILE_TABLE)
),
id_signals AS (
    SELECT
        column_name,

        COUNT(DISTINCT column_value)        AS distinct_count,
        COUNT(column_value)                 AS non_null_count,

        MAX(LENGTH(column_value))           AS max_length,

        SUM(
            CASE
                WHEN column_value RLIKE '^[0-9]{8,}$' THEN 1
                ELSE 0
            END
        ) AS numeric_id_like_count

    FROM base
    UNPIVOT(column_value FOR column_name IN (*))
    WHERE column_value IS NOT NULL
    GROUP BY column_name
)
SELECT
    column_name,

    ROUND(distinct_count / NULLIF(non_null_count,0) * 100, 2)
        AS pct_unique,

    max_length,

    ROUND(numeric_id_like_count / NULLIF(non_null_count,0) * 100, 2)
        AS pct_numeric_id_like

FROM id_signals
ORDER BY pct_unique DESC;

--Interpretation

-- `pct_unique ~100%` → primary / natural key candidate
-- high numeric-ID-like → MRN, encounter_id, claim_id
-- combine with name patterns (`NAME`, `DOB`, `SSN`) for PHI tagging

---

-- 6 STM-ready summary view (what I hand to architects)

CREATE OR REPLACE VIEW PROFILE_SOURCE_TABLE_SUMMARY AS
SELECT
    m.column_name,
    m.data_type,
    m.is_nullable,

    p.pct_populated,
    p.pct_distinct_non_null,

    t.avg_length,
    t.max_length,

    d.pct_unique

FROM column_metadata m
LEFT JOIN population_stats p ON m.column_name = p.column_name
LEFT JOIN text_profile     t ON m.column_name = t.column_name
LEFT JOIN id_signals       d ON m.column_name = d.column_name;

--This becomes:

--STM worksheet input
--Automated mapping hints
--Governance artifact
--Evidence for “why this target field is nullable”