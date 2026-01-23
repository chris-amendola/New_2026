
### 1.1 Purpose & Design Intent

This SQL implements a **column-level data profiling and semantic inference table** designed to support:

- Source system exploration
- Automated or assisted **source-to-target mapping**
- Data quality and risk assessment
- Early semantic classification (numeric-like, date-like, categorical, identifier-like, free-text)

The table is intended to be **machine-generated, human-interpretable**, and stable enough to support downstream automation (LLM or rule-based).

---

### 1.2 Functional Requirements

The system must:

1. **Profile every column** in selected source tables at runtime
2. Produce **one row per source column**
3. Compute:
    - Structural metadata (schema, table, ordinal position, declared type)
    - Completeness and sparsity
    - Cardinality and uniqueness
    - Cast-ability into numeric and date domains
    - Distributional shape (min/max, quantiles)
    - String morphology (lengths, pattern entropy)
4. Be **data-type agnostic** (operate via VARIANT / VARCHAR coercion)
5. Be safe to run repeatedly and transient by default
6. Support **semantic inference**, not just raw statistics

---
### 1.3 Non-Functional Requirements

- **Snowflake-native SQL only**
- No permanent UDFs
- Scalable across wide schemas
- Tolerant of malformed values
- Deterministic output schema
- Explainable features (each metric has semantic meaning)
    
---

### 1.4 Output Contract

The output table `INT_PROFILE` must:

- Contain **only derived column-level features**
- Avoid intermediate or diagnostic columns
- Be suitable for:
    - Feature stores
    - STM auto-drafting
    - Column classification models
    - Human review workflows
        
---

## 2) INT_PROFILE Feature Dictionary

Below is a **complete, column-by-column feature specification**.

> **Legend — Feature Categories**
> - **Metadata**: structural descriptors
> - **Completeness**: null / presence signals
> - **Cardinality**: uniqueness and repetition
> - **Type Affinity**: how values behave under casting
> - **Distributional**: numeric shape and spread
> - **String Morphology**: text length & complexity
>     
---
### 2.1 Metadata Features

| Column               | Category | What It Measures                                 | How to Interpret                                      |
| -------------------- | -------- | ------------------------------------------------ | ----------------------------------------------------- |
| `PROFILE_COLUMN`     | Metadata | Fully-qualified or synthesized column identifier | Stable identity key for joins and downstream modeling |
| `SOURCE_SCHEMA`      | Metadata | Origin schema name                               | Grouping, lineage, and access context                 |
| `SOURCE_TABLE`       | Metadata | Origin table name                                | Dataset boundary                                      |
| `ORDINAL_POSITION`   | Metadata | Column position in source table                  | Helpful for lineage diffing, not semantic meaning     |
| `DECLARED_DATA_TYPE` | Metadata | Source system declared data type                 | Often misleading; used only as a prior                |
| `NULLABLE`           | Metadata | Schema-level nullability flag                    | May differ from observed null behavior                |
| `SCHEMA_MAX_LENGTH`  | Metadata | Declared max length (strings)                    | Upper bound, not observed distribution                |

---
### 2.2 Volume & Completeness

| Column         | Category     | What It Measures          | How to Interpret                                    |
| -------------- | ------------ | ------------------------- | --------------------------------------------------- |
| `ROW_COUNT`    | Volume       | Total rows scanned        | Denominator for most ratios                         |
| `NON_NULL_PCT` | Completeness | Fraction of non-null rows | Low values → optional or sparse fields              |
| `NULL_PCT`     | Completeness | Fraction of null rows     | Redundant with NON_NULL_PCT but easier thresholding |

---

### 2.3 Cardinality & Uniqueness

| Column           | Category    | What It Measures                | How to Interpret                      |
| ---------------- | ----------- | ------------------------------- | ------------------------------------- |
| `DISTINCT_COUNT` | Cardinality | Count of unique non-null values | Low → categorical; high → identifiers |
| `DISTINCT_PCT`   | Cardinality | DISTINCT_COUNT / ROW_COUNT      | Near 1.0 → key-like                   |
| `DUPLICATE_PCT`  | Cardinality | Repetition rate                 | High → dimensions / enums             |

---

### 2.4 Type Affinity (Semantic Castability)

| Column                             | Category      | What It Measures               | How to Interpret                                |
| ---------------------------------- | ------------- | ------------------------------ | ----------------------------------------------- |
| `NUMERIC_CAST_RATE`                | Type Affinity | % of values castable to NUMBER | High → numeric semantics even if stored as text |
| `DATE_CAST_RATE`                   | Type Affinity | % castable to DATE/TIMESTAMP   | High → temporal meaning                         |
| `BOOLEAN_CAST_RATE` _(if present)_ | Type Affinity | % castable to boolean          | Flags / indicators                              |

> **NOTE :**  
> These features intentionally **override declared data type** when reality disagrees.

---

### 2.5 Numeric Distributional Features

(Computed only when numeric cast succeeds)

| Column           | Category       | What It Measures      | How to Interpret                 |
| ---------------- | -------------- | --------------------- | -------------------------------- |
| `NUMERIC_MIN`    | Distributional | Minimum numeric value | Range checks, sentinel detection |
| `NUMERIC_MAX`    | Distributional | Maximum numeric value | Outlier detection                |
| `NUMERIC_MEAN`   | Distributional | Average               | Central tendency                 |
| `NUMERIC_STDDEV` | Distributional | Spread                | Stability / volatility           |
| `PERCENTILE_05`  | Distributional | 5th percentile        | Lower tail behavior              |
| `PERCENTILE_50`  | Distributional | Median                | Robust central value             |
| `PERCENTILE_95`  | Distributional | Upper tail behavior   | Skew / extremes                  |

---

### 2.6 String Morphology & Complexity

| Column             | Category          | What It Measures               | How to Interpret          |
| ------------------ | ----------------- | ------------------------------ | ------------------------- |
| `MIN_LENGTH`       | String Morphology | Shortest observed string       | Empty / code detection    |
| `MAX_LENGTH`       | String Morphology | Longest observed string        | Free-text vs code         |
| `AVG_LENGTH`       | String Morphology | Mean string length             | Description vs identifier |
| `STDDEV_LENGTH`    | String Morphology | Length variability             | High → free text          |
| `ALPHA_PCT`        | String Morphology | % alphabetic characters        | Names, descriptions       |
| `NUMERIC_CHAR_PCT` | String Morphology | % digits                       | IDs, codes                |
| `SPECIAL_CHAR_PCT` | String Morphology | Symbols / punctuation          | Encoded fields            |
| `ENTROPY_SCORE`    | String Morphology | Character distribution entropy | High → hashes / GUIDs     |

---

### 2.7 Temporal Distribution (If Date-Like)

| Column               | Category | What It Measures | How to Interpret            |
| -------------------- | -------- | ---------------- | --------------------------- |
| `MIN_DATE`           | Temporal | Earliest date    | Epoch checks                |
| `MAX_DATE`           | Temporal | Latest date      | Freshness                   |
| `TEMPORAL_SPAN_DAYS` | Temporal | Range width      | Event vs reference dates    |
| `TEMPORAL_DENSITY`   | Temporal | Coverage vs span | Burst vs continuous logging |

---

## 3) What This Table Is Optimized For (Implicitly)

This design strongly suggests:

- **LLM-assisted STM generation**
- Column classification (identifier, measure, dimension, timestamp)
- Data quality early warning
- Risk scoring before ingestion
- Human-in-the-loop semantic mapping

It is _not_ optimized for:

- Row-level anomaly detection
- Real-time validation
- Business rule enforcement

---

