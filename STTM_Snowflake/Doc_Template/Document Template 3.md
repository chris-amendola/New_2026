# **📘 Multi‑Sheet Excel Layout for Healthcare STM**

Each sheet below includes:

- A recommended sheet name
- Column definitions
- Purpose and usage notes

---

## **1. Sheet: `Metadata`**

**Purpose:** Capture document‑level metadata, governance, and versioning.

|Column|Description|
|---|---|
|STM Document Name|Human‑readable name|
|Version|Semantic version|
|Author(s)|Names, roles|
|Date Created|YYYY‑MM‑DD|
|Last Updated|YYYY‑MM‑DD|
|Source System|EHR, claims, FHIR, etc.|
|Target Model|OMOP, warehouse schema, FHIR, etc.|
|Target Table|Table/resource this STM applies to|
|Status|Draft / In Review / Approved|
|Approval Signatures|Governance workflow|

---

## **2. Sheet: `Terminology`**

**Purpose:** Capture all terminology normalization and code‑system mapping rules.

|Column|Description|
|---|---|
|Source Code System|ICD‑10, CPT, SNOMED, LOINC, local codes|
|Target Code System|Standardized terminology|
|Mapping Method|Direct, lookup, crosswalk, algorithmic|
|Mapping Table Name|Name of lookup table|
|Versioning|Code system version|
|Fallback Logic|Behavior for unmapped codes|
|Semantic Notes|Clinical nuance, ambiguity|

---

## **3. Sheet: `Field_Mappings`**

**Purpose:** The core STM sheet—field‑level mapping from source to target.

|Column|Description|
|---|---|
|Target Field|Name of target column|
|Target Datatype|Datatype in target schema|
|Target Constraints|PK, FK, NOT NULL, etc.|
|Source Field(s)|One or more source fields|
|Source Datatype|Datatype in source system|
|Transformation Logic|SQL, pseudocode, algorithm|
|Terminology Mapping|Code system mapping logic|
|Default Value|If no source value|
|Null Handling|Reject, default, allow|
|Data Quality Rules|Field‑level DQ checks|
|Semantic Match Score|0–1 score for schema matching|
|Notes|SME comments, caveats|

---

## **4. Sheet: `DQ_Rules`**

**Purpose:** Centralized data quality rules for validation and monitoring.

|Column|Description|
|---|---|
|Rule ID|Unique identifier|
|Rule Type|Field‑level, row‑level, referential, SPC|
|Field(s) Affected|Target fields|
|Rule Description|What the rule checks|
|Severity|Error / Warning / Info|
|Action|Reject, nullify, flag|
|Threshold|Numeric or logical threshold|
|Example Failure|Sample bad value|
|Notes|Additional context|

---

## **5. Sheet: `Lineage`**

**Purpose:** Capture provenance, audit trail, and transformation lineage.

|Column|Description|
|---|---|
|Source Extract|File/table name|
|Source Version|Extract version or timestamp|
|Load Batch ID|Pipeline batch identifier|
|Transformation Step|Raw → Standardized → Target|
|Timestamp|When transformation occurred|
|Responsible Process|Job name, pipeline ID|
|Checksum|Optional hash for compliance|
|Notes|Additional lineage details|

---

## **6. Sheet: `Exceptions`**

**Purpose:** Track unmapped codes, rejected rows, fallback logic, and overrides.

|Column|Description|
|---|---|
|Exception Type|Unmapped code, rejected row, override|
|Source Value|Original value|
|Target Value|Mapped or fallback value|
|Reason|Why exception occurred|
|Count|Frequency (optional)|
|Resolution|How it was handled|
|Approver|SME or governance approver|
|Timestamp|When exception was logged|
|Notes|Additional detail|

---

- Store multiple STM documents
- Version them cleanly
- Capture field mappings, terminology rules, DQ rules, lineage, and exceptions
- Maintain auditability and referential integrity
- Support automated STM generation or manual population

---

# **1. Table: `stm_document`**

**Purpose:** Document‑level metadata and governance.

```sql
CREATE TABLE stm_document (
    stm_id              INTEGER PRIMARY KEY,
    stm_document_name   TEXT NOT NULL,
    version             TEXT NOT NULL,
    author              TEXT,
    date_created        TEXT,
    last_updated        TEXT,
    source_system       TEXT NOT NULL,
    target_model        TEXT NOT NULL,
    target_table        TEXT NOT NULL,
    status              TEXT,
    approval_signatures TEXT
);
```

---

# **2. Table: `stm_terminology`**

**Purpose:** Terminology normalization rules.

```sql
CREATE TABLE stm_terminology (
    terminology_id      INTEGER PRIMARY KEY,
    stm_id              INTEGER NOT NULL,
    source_code_system  TEXT,
    target_code_system  TEXT,
    mapping_method      TEXT,
    mapping_table_name  TEXT,
    versioning          TEXT,
    fallback_logic      TEXT,
    semantic_notes      TEXT,
    FOREIGN KEY (stm_id) REFERENCES stm_document(stm_id)
);
```

---

# **3. Table: `stm_field_mapping`**

**Purpose:** Core field‑level source‑to‑target mappings.

```sql
CREATE TABLE stm_field_mapping (
    field_mapping_id        INTEGER PRIMARY KEY,
    stm_id                  INTEGER NOT NULL,
    target_field            TEXT NOT NULL,
    target_datatype         TEXT,
    target_constraints      TEXT,
    source_fields           TEXT,   -- comma‑separated or JSON
    source_datatype         TEXT,
    transformation_logic    TEXT,
    terminology_mapping     TEXT,
    default_value           TEXT,
    null_handling           TEXT,
    data_quality_rules      TEXT,
    semantic_match_score    REAL,
    notes                   TEXT,
    FOREIGN KEY (stm_id) REFERENCES stm_document(stm_id)
);
```

---

# **4. Table: `stm_dq_rule`**

**Purpose:** Centralized data quality rules.

```sql
CREATE TABLE stm_dq_rule (
    dq_rule_id         INTEGER PRIMARY KEY,
    stm_id             INTEGER NOT NULL,
    rule_type          TEXT,      -- field-level, row-level, referential, SPC
    fields_affected    TEXT,
    rule_description   TEXT,
    severity           TEXT,      -- error, warning, info
    action             TEXT,      -- reject, nullify, flag
    threshold          TEXT,
    example_failure    TEXT,
    notes              TEXT,
    FOREIGN KEY (stm_id) REFERENCES stm_document(stm_id)
);
```

---

# **5. Table: `stm_lineage`**

**Purpose:** Provenance, audit trail, and transformation lineage.

```sql
CREATE TABLE stm_lineage (
    lineage_id         INTEGER PRIMARY KEY,
    stm_id             INTEGER NOT NULL,
    source_extract     TEXT,
    source_version     TEXT,
    load_batch_id      TEXT,
    transformation_step TEXT,
    timestamp          TEXT,
    responsible_process TEXT,
    checksum           TEXT,
    notes              TEXT,
    FOREIGN KEY (stm_id) REFERENCES stm_document(stm_id)
);
```

---

# **6. Table: `stm_exception`**

**Purpose:** Track unmapped codes, rejected rows, overrides.

```sql
CREATE TABLE stm_exception (
    exception_id       INTEGER PRIMARY KEY,
    stm_id             INTEGER NOT NULL,
    exception_type     TEXT,   -- unmapped code, rejected row, override
    source_value       TEXT,
    target_value       TEXT,
    reason             TEXT,
    count              INTEGER,
    resolution         TEXT,
    approver           TEXT,
    timestamp          TEXT,
    notes              TEXT,
    FOREIGN KEY (stm_id) REFERENCES stm_document(stm_id)
);
```

---

# **7. Optional: `stm_change_log`**

**Purpose:** Version history and governance tracking.

```sql
CREATE TABLE stm_change_log (
    change_id          INTEGER PRIMARY KEY,
    stm_id             INTEGER NOT NULL,
    version            TEXT,
    date               TEXT,
    author             TEXT,
    change_description TEXT,
    FOREIGN KEY (stm_id) REFERENCES stm_document(stm_id)
);
```

---

# **8. Optional: `stm_appendix`**

**Purpose:** Glossary, code system references, sample mappings.

```sql
CREATE TABLE stm_appendix (
    appendix_id        INTEGER PRIMARY KEY,
    stm_id             INTEGER NOT NULL,
    glossary           TEXT,
    code_system_refs   TEXT,
    sample_mappings    TEXT,
    FOREIGN KEY (stm_id) REFERENCES stm_document(stm_id)
);
```
---

# 1) JSON Schema Version of the STM Template

This schema is written in **JSON Schema Draft‑07** .

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Healthcare Source-to-Target Mapping (STM) Document",
  "type": "object",
  "properties": {
    "document_metadata": {
      "type": "object",
      "properties": {
        "stm_document_name": { "type": "string" },
        "version": { "type": "string" },
        "author": { "type": "string" },
        "date_created": { "type": "string", "format": "date" },
        "last_updated": { "type": "string", "format": "date" },
        "source_system": { "type": "string" },
        "target_model": { "type": "string" },
        "target_table": { "type": "string" },
        "status": { "type": "string" },
        "approval_signatures": { "type": "string" }
      },
      "required": ["stm_document_name", "version", "source_system", "target_model"]
    },

    "source_dataset_overview": {
      "type": "object",
      "properties": {
        "source_table": { "type": "string" },
        "source_schema": { "type": "string" },
        "primary_keys": { "type": "array", "items": { "type": "string" } },
        "natural_keys": { "type": "array", "items": { "type": "string" } },
        "record_volume_estimate": { "type": "string" },
        "refresh_cadence": { "type": "string" },
        "known_data_quality_issues": { "type": "string" },
        "source_steward": { "type": "string" }
      }
    },

    "target_dataset_overview": {
      "type": "object",
      "properties": {
        "target_table": { "type": "string" },
        "target_schema": { "type": "string" },
        "primary_keys": { "type": "array", "items": { "type": "string" } },
        "foreign_keys": { "type": "array", "items": { "type": "string" } },
        "constraints": { "type": "string" },
        "target_steward": { "type": "string" }
      }
    },

    "terminology_normalization": {
      "type": "object",
      "properties": {
        "source_code_system": { "type": "string" },
        "target_code_system": { "type": "string" },
        "mapping_method": { "type": "string" },
        "mapping_table_name": { "type": "string" },
        "versioning": { "type": "string" },
        "fallback_logic": { "type": "string" },
        "semantic_notes": { "type": "string" }
      }
    },

    "row_filtering": {
      "type": "object",
      "properties": {
        "inclusion_criteria": { "type": "string" },
        "exclusion_criteria": { "type": "string" },
        "filter_logic": { "type": "string" },
        "expected_row_reduction_percent": { "type": "number" }
      }
    },

    "field_mappings": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "target_field": { "type": "string" },
          "target_datatype": { "type": "string" },
          "target_constraints": { "type": "string" },
          "source_fields": { "type": "array", "items": { "type": "string" } },
          "source_datatype": { "type": "string" },
          "transformation_logic": { "type": "string" },
          "terminology_mapping": { "type": "string" },
          "default_value": { "type": ["string", "number", "null"] },
          "null_handling": { "type": "string" },
          "data_quality_rules": { "type": "string" },
          "semantic_match_score": { "type": "number" },
          "notes": { "type": "string" }
        },
        "required": ["target_field"]
      }
    },

    "detailed_transformation_logic": {
      "type": "object",
      "properties": {
        "sql_logic": { "type": "string" },
        "algorithmic_logic": { "type": "string" },
        "multi_field_derivations": { "type": "string" }
      }
    },

    "data_quality_validation": {
      "type": "object",
      "properties": {
        "field_level_rules": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "field": { "type": "string" },
              "rule_type": { "type": "string" },
              "rule_description": { "type": "string" },
              "severity": { "type": "string" },
              "action": { "type": "string" }
            }
          }
        },
        "row_level_rules": { "type": "string" },
        "spc_expectations": { "type": "string" }
      }
    },

    "lineage_provenance": {
      "type": "object",
      "properties": {
        "source_extract": { "type": "string" },
        "load_batch_id": { "type": "string" },
        "transformation_step": { "type": "string" },
        "timestamp": { "type": "string" },
        "responsible_process": { "type": "string" },
        "checksum": { "type": "string" }
      }
    },

    "exceptions": {
      "type": "object",
      "properties": {
        "unmapped_codes": { "type": "string" },
        "rejected_rows": { "type": "string" },
        "fallback_logic_applied": { "type": "string" },
        "manual_overrides": { "type": "string" }
      }
    },

    "testing_validation": {
      "type": "object",
      "properties": {
        "unit_tests": { "type": "string" },
        "integration_tests": { "type": "string" },
        "uat_scenarios": { "type": "string" }
      }
    },

    "change_log": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "version": { "type": "string" },
          "date": { "type": "string" },
          "author": { "type": "string" },
          "change_description": { "type": "string" }
        }
      }
    },

    "appendices": {
      "type": "object",
      "properties": {
        "glossary": { "type": "string" },
        "code_system_references": { "type": "string" },
        "sample_mappings": { "type": "string" }
      }
    }
  }
}
```

---

# 2) Excel‑Ready Tabular Version


|Target Field|Target Datatype|Target Constraints|Source Field(s)|Source Datatype|Transformation Logic|Terminology Mapping|Default Value|Null Handling|Data Quality Rules|Semantic Match Score|Notes|
|---|---|---|---|---|---|---|---|---|---|---|---|
|||||||||||||
|||||||||||||
|||||||||||||

