
---
## Source-to-Target Mapping Template (Full Structure)

### SECTION 0 — Document Metadata

| Field                  | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| Mapping Document ID    | Unique, immutable identifier                               |
| Mapping Name           | Human-readable name                                        |
| Domain                 | Clinical / Revenue Cycle / Operations / Quality / Research |
| Source System(s)       | e.g., Epic, Athena, NextGen, Clearinghouse                 |
| Target System          | EDW, Data Mart, Analytics Layer                            |
| Target Model           | e.g., Star schema, OMOP, FHIR-derived, Custom              |
| Author(s)              | Name(s), role(s)                                           |
| Reviewer(s)            | Data owner, SME, compliance                                |
| Effective Date         | Date mapping becomes active                                |
| Version                | Semantic version (e.g., 2.1.0)                             |
| Change Type            | New / Enhancement / Bug Fix                                |
| Regulatory Sensitivity | PHI / HIPAA / De-identified                                |
| Approval Status        | Draft / Approved / Deprecated                              |

---

### SECTION 1 — Business Context & Intent

|Field|Description|
|---|---|
|Business Purpose|Why this data exists|
|Primary Use Cases|Quality reporting, ops, billing, research|
|Downstream Consumers|Dashboards, models, extracts|
|Decision Impact|What decisions depend on this data|
|Materiality|High / Medium / Low business risk|
|Known Biases|Known systematic distortions|
|Non-Goals|Explicitly out-of-scope use cases|

---

### SECTION 2 — Source System Profile

#### 2.1 Source System Overview

|Field|Description|
|---|---|
|Source System Name||
|Vendor||
|Module / Subsystem||
|Source Data Owner||
|Source Update Frequency||
|Data Latency|Typical / worst case|
|Correction Behavior|Overwrite / Append / Void-Replace|
|Historical Availability||
|Known Data Quality Issues||

---

#### 2.2 Source Table Inventory

|Source Schema|Source Table|Description|Row Grain|Change Capture Method|Volatility|
|---|---|---|---|---|---|

---

### SECTION 3 — Target Model Profile

|Field|Description|
|---|---|
|Target Schema||
|Target Table||
|Target Grain|(e.g., one row per encounter per day)|
|Slowly Changing Behavior|Type 1 / 2 / Hybrid|
|Retention Policy||
|Consumer Expectations|e.g., “clinically accurate, not billing-final”|

---

## SECTION 4 — Column-Level Source-to-Target Mapping (Core)

### 4.1 Core Mapping Table

|Category|Field|
|---|---|
|Mapping ID|Unique per target column|
|Target Schema||
|Target Table||
|Target Column||
|Target Data Type||
|Target Nullable||
|Target Description||
|Target Grain Context|How it behaves at target grain|
|Source System||
|Source Schema||
|Source Table||
|Source Column||
|Source Data Type||
|Source Nullable||
|Source Grain||
|Source Filter Logic||
|Join Logic|Explicit join keys|
|Transformation Type|Direct / Derived / Lookup / Aggregated|
|Transformation Logic|SQL / Pseudocode|
|Business Rule Description||
|Default Value Logic||
|Null Handling Strategy||
|Invalid Value Handling||
|Unit of Measure||
|Code System|ICD-10, CPT, SNOMED, LOINC|
|Code Version||
|Value Set Reference||
|Time Semantics|Event time / Record time|
|Effective Dating Logic||
|Late Arriving Data Handling||
|Correction Handling||
|Lineage Confidence|High / Medium / Low|
|Data Quality Expectations||
|Validation Rules||
|Reconciliation Strategy||
|PHI Classification||
|Security Classification||
|Usage Restrictions||
|Known Limitations||
|Open Questions||

---

### 4.2 Transformation Detail (Optional Deep Dive)

For complex fields, use a child section:

|Field|Description|
|---|---|
|Transformation Name||
|Input Fields||
|Algorithm Description||
|Edge Cases||
|Performance Considerations||
|Determinism|Deterministic / Probabilistic|
|Reprocess Safety||
|Idempotency||
|Example Input||
|Example Output||

---

## SECTION 5 — Aggregations & Derived Metrics

| Metric Name |  
| Target Table |  
| Grain |  
| Numerator Logic |  
| Denominator Logic |  
| Inclusion Criteria |  
| Exclusion Criteria |  
| Risk Adjustment |  
| Attribution Logic |  
| Time Window |  
| Clinical Review Status |  
| Financial Reconciliation Status |

---

## SECTION 6 — Data Quality & Validation Framework

### 6.1 Column-Level Checks

|Target Column|Check Type|Rule|Threshold|Failure Severity|
|---|---|---|---|---|

Examples:

- Completeness
    
- Valid range
    
- Referential integrity
    
- Distribution stability (SPC-friendly 😉)
    

---

### 6.2 End-to-End Reconciliation

|Check|Source Metric|Target Metric|Expected Relationship|
|---|---|---|---|

---

## SECTION 7 — Error Handling & Exceptions

|Scenario|Detection Logic|Mitigation|Reporting Path|
|---|---|---|---|

---

## SECTION 8 — Change Management

| Change ID |  
| Description |  
| Reason |  
| Impacted Fields |  
| Backfill Required |  
| Historical Restatement |  
| Consumer Notification |  
| Effective Date |

---

## SECTION 9 — Governance, Ownership & Stewardship

|Role|Name / Team|
|---|---|
|Data Owner||
|Clinical Owner||
|Financial Owner||
|Technical Owner||
|Steward||
|Escalation Path||

---

## SECTION 10 — Compliance & Regulatory Notes

|Regulation|Applicability|Notes|
|---|---|---|
|HIPAA|||
|CMS|||
|NCQA|||
|HEDIS|||
|State-Specific|||

---

## SECTION 11 — Appendices

### A. Code Sets & Value Sets

### B. Sample Records (De-identified)

### C. Known Source System Quirks

### D. Deprecated Fields

---
