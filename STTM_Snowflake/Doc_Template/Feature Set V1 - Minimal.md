
**Minimal** feature set requirements:

- Strong enough to support meaningful Cortex reasoning
- Cheap to compute
- Defensible in design review
- Stable enough to freeze early

AKA the **irreducible core**.

---

# STM Assistant — Feature Set v1

## Design Rules for v1

1. **Derived only from data + schema** (no business guesses)
2. **Deterministic and reproducible**
3. **Explainable to a human**
4. **Healthcare-safe by default**
5. **Cheap enough to run repeatedly**

Anything else is v2.

---
## 1. Column Identity Features (Who am I?)

These are table-stakes and very stable.

|Feature|Type|Notes|
|---|---|---|
|source_schema|string||
|source_table|string||
|source_column|string||
|ordinal_position|integer|Sometimes useful|
|declared_data_type|string|As declared|
|nullable|boolean||
|max_length|integer|If applicable|

> These anchor the column in reality and help Cortex avoid hallucinated joins.

---
## 2. Observed Data Type Reality (What am I _really_?)

Healthcare systems lie in schemas. Reality matters.

|Feature|Type|Notes|
|---|---|---|
|observed_type|enum|string / integer / decimal / date / timestamp|
|cast_success_rate|float|% of values that cast cleanly|
|mixed_type_flag|boolean|Strong ambiguity signal|

> NOTE: Cast success rate is _surprisingly powerful_ in mapping.

---
## 3. Completeness & Cardinality (How populated am I?)

These directly inform role classification.

|Feature|Type|
|---|---|
|row_count|integer|
|non_null_pct|float|
|distinct_count|integer|
|distinct_ratio|float|
|uniqueness_flag|boolean (≈1.0 ratio)|

> IDs, flags, and measures separate cleanly here.

---

## 4. Value Shape & Length (What do values look like?)

Cheap but high signal.

|Feature|Type|
|---|---|
|min_length|integer|
|max_length|integer|
|avg_length|float|
|whitespace_pct|float|
|empty_string_pct|float|
---
## 5. Pattern & Format Signals (Do I match known shapes?)

These feed PHI detection _and_ semantic role inference.

|Feature|Type|Examples|
|---|---|---|
|date_pattern_pct|float|YYYY-MM-DD|
|numeric_only_pct|float||
|alpha_only_pct|float||
|alphanumeric_pct|float||
|hyphenated_pct|float|MRNs, codes|
|email_like_pct|float||
---
## 6. Domain Concentration (Am I categorical?)

Essential for mapping to codes vs measures.

|Feature|Type|
|---|---|
|top_10_value_pct|float|
|top_3_value_pct|float|
|entropy_score|float (optional)|

> High concentration + low cardinality → codes, flags.
---
## 7. Foreign-Key-Like Behavior (Do I reference something?)

Very powerful when it hits.
|Feature|Type|
|---|---|
|appears_in_other_tables|boolean|
|referenced_table_count|integer|
|join_success_pct|float (sampled)|

> Even a weak signal here helps Cortex enormously.
---
## 8. PHI / Sensitivity Heuristics (Safety First)

These are _heuristics_, not labels.

|Feature|Type|
|---|---|
|name_like_pct|float|
|dob_like_pct|float|
|ssn_like_pct|float|
|address_like_pct|float|
|free_text_pct|float|

> You never say “this **is** PHI”—you say “this **resembles** PHI.”
---
## 9. Initial Column Role Classification (Deterministic)

Before Cortex ever reasons.

|Feature|Type|
|---|---|
|role_candidate|enum|
|role_confidence|float|
|role_rules_fired|array|

> Cortex _inherits_ this as prior knowledge.

---
## What’s Explicitly Deferred to v2

On purpose:
- Semantic embeddings
- Business glossary alignment
- Cross-system historical mapping reuse
- NLP over free text
- Learned transformation patterns

You don’t need them yet—and they complicate trust.
---
## Why This v1 Is “Freeze-Worthy”

- ~90% of features won’t change over time
- They align with how senior analysts already think
- They support _both_ STM and general profiling
- They degrade gracefully when data is messy
- They’re easy to explain in one slide

If leadership asks:
> “What is the AI actually looking at?”

You can answer clearly.
---

## My Strong Recommendation

Before adding anything else:
1. Implement **this exact feature set**
2. Run it on 3–5 very different source systems
3. See where analysts _disagree with it_
    
Disagreement reveals v2 requirements.

---

1. **Snowflake SQL patterns** to compute Feature Set v1 (modular, reusable)
2. **Deterministic role classification rules** that sit _between_ features and Cortex

I’ll keep everything **Snowflake-native**, production-realistic, and architect-defensible.
---
# 1. Snowflake SQL Patterns for Feature Set v1

## Core Design Pattern

You want **one row per column**, produced via:

- `INFORMATION_SCHEMA.COLUMNS` (declared metadata)
- A **single-pass UNPIVOT** of sampled data
- Aggregations grouped by `(schema, table, column)`
    
I’ll show this as **CTE blocks** so you can lift pieces.

---
## A. Column Identity & Declared Metadata

```sql
WITH column_metadata AS (
    SELECT
        table_schema      AS source_schema,
        table_name        AS source_table,
        column_name       AS source_column,
        ordinal_position,
        data_type         AS declared_data_type,
        is_nullable       AS nullable,
        character_maximum_length AS max_length
    FROM information_schema.columns
    WHERE table_schema = :source_schema
      AND table_name   = :source_table
)
```

This is your **stable anchor**.

---
## B. Sample & UNPIVOT (Single Pass)

Snowflake does _not_ support `UNPIVOT(*)`, so we generate the column list dynamically upstream (you already know this pattern).

Assume a generated statement like:

```sql
, unpivoted AS (
    SELECT
        t.$1 AS row_id,          -- optional
        u.column_name,
        u.value::STRING AS value
    FROM (
        SELECT * 
        FROM IDENTIFIER(:qualified_table)
        SAMPLE (100000 ROWS)
    ) t
    UNPIVOT(value FOR column_name IN (
        col1, col2, col3, ...
    )) u
)
```

Everything downstream hangs off `unpivoted`.

---
## C. Observed Type Reality

```sql
, observed_type AS (
    SELECT
        column_name,
        COUNT(*) AS row_count,
        AVG(IFF(TRY_TO_NUMBER(value) IS NOT NULL, 1, 0)) AS numeric_cast_rate,
        AVG(IFF(TRY_TO_DATE(value)   IS NOT NULL, 1, 0)) AS date_cast_rate
    FROM unpivoted
    GROUP BY column_name
)
```

Later:

```sql
CASE
    WHEN numeric_cast_rate > 0.95 THEN 'NUMERIC'
    WHEN date_cast_rate    > 0.95 THEN 'DATE'
    ELSE 'STRING'
END AS observed_type
```

Mixed-type flag:

```sql
numeric_cast_rate BETWEEN 0.2 AND 0.8
```

---

## D. Completeness & Cardinality

```sql
, cardinality AS (
    SELECT
        column_name,
        COUNT(*) AS row_count,
        COUNT(value) / COUNT(*) AS non_null_pct,
        COUNT(DISTINCT value) AS distinct_count,
        COUNT(DISTINCT value) / COUNT(*) AS distinct_ratio
    FROM unpivoted
    GROUP BY column_name
)
```

Uniqueness flag:

```sql
distinct_ratio > 0.98 AND non_null_pct > 0.95
```

---
## E. Value Shape & Length

```sql
, length_stats AS (
    SELECT
        column_name,
        MIN(LENGTH(value)) AS min_length,
        MAX(LENGTH(value)) AS max_length,
        AVG(LENGTH(value)) AS avg_length,
        AVG(IFF(value LIKE '% %', 1, 0)) AS whitespace_pct,
        AVG(IFF(value = '', 1, 0)) AS empty_string_pct
    FROM unpivoted
    GROUP BY column_name
)
```

---
## F. Pattern & Format Signals

```sql
, pattern_stats AS (
    SELECT
        column_name,
        AVG(IFF(REGEXP_LIKE(value, '^\d{4}-\d{2}-\d{2}$'), 1, 0)) AS date_pattern_pct,
        AVG(IFF(REGEXP_LIKE(value, '^\d+$'), 1, 0)) AS numeric_only_pct,
        AVG(IFF(REGEXP_LIKE(value, '^[A-Za-z]+$'), 1, 0)) AS alpha_only_pct,
        AVG(IFF(REGEXP_LIKE(value, '^[A-Za-z0-9]+$'), 1, 0)) AS alphanumeric_pct,
        AVG(IFF(REGEXP_LIKE(value, '-'), 1, 0)) AS hyphenated_pct,
        AVG(IFF(REGEXP_LIKE(value, '@'), 1, 0)) AS email_like_pct
    FROM unpivoted
    GROUP BY column_name
)
```

---
## G. Domain Concentration

```sql
, domain_concentration AS (
    SELECT
        column_name,
        MAX(cnt) / SUM(cnt) AS top_1_value_pct
    FROM (
        SELECT
            column_name,
            value,
            COUNT(*) AS cnt
        FROM unpivoted
        GROUP BY column_name, value
    )
    GROUP BY column_name
)
```

(Top-3/10 can be added later—v1 can live with top-1.)

---
## H. PHI Heuristics (Lightweight)

```sql
, phi_signals AS (
    SELECT
        column_name,
        AVG(IFF(REGEXP_LIKE(value, '^[A-Z][a-z]+$'), 1, 0)) AS name_like_pct,
        AVG(IFF(REGEXP_LIKE(value, '^\d{3}-\d{2}-\d{4}$'), 1, 0)) AS ssn_like_pct,
        AVG(IFF(REGEXP_LIKE(value, '\d{1,2}/\d{1,2}/\d{4}'), 1, 0)) AS dob_like_pct
    FROM unpivoted
    GROUP BY column_name
)
```

---
## I. Final Feature Assembly

```sql
SELECT
    m.*,
    o.observed_type,
    c.non_null_pct,
    c.distinct_ratio,
    l.avg_length,
    p.numeric_only_pct,
    d.top_1_value_pct,
    phi.name_like_pct,
    phi.ssn_like_pct
FROM column_metadata m
LEFT JOIN observed_type o USING (source_column)
LEFT JOIN cardinality c USING (source_column)
LEFT JOIN length_stats l USING (source_column)
LEFT JOIN pattern_stats p USING (source_column)
LEFT JOIN domain_concentration d USING (source_column)
LEFT JOIN phi_signals phi USING (source_column);
```

That table is your **Feature Set v1**.

---
# 2. Deterministic Role Classification Rules (Pre-Cortex)

This is _critical_. Cortex must **inherit priors**, not invent them.

---
## Role Taxonomy (v1)

```
ID
MEASURE
CODE
FLAG
TEXT
UNKNOWN
```

---
## Rule Evaluation Order (Important)

Rules are evaluated **top-down**, first match wins.

---
## Rule Set

### Rule 1 — ID

```sql
IF distinct_ratio > 0.98
AND non_null_pct > 0.95
AND avg_length BETWEEN 5 AND 50
AND numeric_only_pct < 0.95
THEN role = 'ID'
```

Confidence:

```sql
LEAST(1.0, distinct_ratio + non_null_pct) / 2
```

---

### Rule 2 — FLAG

```sql
IF distinct_ratio <= 0.02
AND top_1_value_pct > 0.6
AND avg_length <= 5
THEN role = 'FLAG'
```

---

### Rule 3 — MEASURE

```sql
IF observed_type = 'NUMERIC'
AND distinct_ratio > 0.1
AND distinct_ratio < 0.9
THEN role = 'MEASURE'
```

---

### Rule 4 — CODE

```sql
IF distinct_ratio BETWEEN 0.01 AND 0.2
AND avg_length BETWEEN 2 AND 20
AND alpha_only_pct + alphanumeric_pct > 0.7
THEN role = 'CODE'
```

---

### Rule 5 — TEXT

```sql
IF avg_length > 30
AND distinct_ratio > 0.5
THEN role = 'TEXT'
```

---

### Else — UNKNOWN

Low confidence, Cortex leans heavily here.

---

## Persist These Too

For every column:

- `role_candidate`
    
- `role_confidence`
    
- `rules_fired` (array of rule IDs)
    

This lets you say:

> “The model didn’t invent this classification—we did.”

---

## How Cortex Uses This

In prompts, you pass:

```json
{
  "role_candidate": "CODE",
  "role_confidence": 0.87,
  "features": { ... }
}
```

And explicitly instruct:

> “If your reasoning conflicts with the role_candidate, explain why.”

That single instruction massively reduces hallucination.

---

## Why This Is the Right Level of Rigor

- Deterministic where it should be  
- Probabilistic where it adds value
- Fully auditable
- Easy to explain to architects
- Easy to evolve to v2
---

