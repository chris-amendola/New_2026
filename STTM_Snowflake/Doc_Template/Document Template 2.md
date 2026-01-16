---

## 1. Document metadata

These fields describe the mapping specification as an artifact (useful for governance, approvals, and versioning).

- Document ID / Name
    
- Program / Initiative (e.g., “EDW Modernization”, “Quality Reporting”)
    
- Domain / Subject Area (e.g., Encounters, Claims, Orders, Medications)
    
- Owning Team (e.g., Enterprise Data Engineering, Analytics)
    
- Business Owner
    
- Technical Owner
    
- Target Platform / Environment (e.g., “Enterprise Data Warehouse”, “FHIR Store”)
    
- Status (Draft / In Review / Approved / Deprecated)
    
- Version
    
- Effective Start Date / End Date
    
- Change Log (Date, Author, Summary of Change, Version, Approval Reference)
    
- Regulatory / Contractual Drivers (e.g., HEDIS, CMS, HIPAA, local quality program)
    

---

## 2. Scope and context

This section or tab anchors the mapping to its business and technical purpose.​

- Business Purpose / Use Cases
    
    - Brief description of why data is being integrated (e.g., risk adjustment, quality measures, operational dashboards).
        
- In-Scope Entities
    
    - High‑level list (e.g., Patient, Encounter, Provider, Claim, Lab Result, Medication Order).
        
- Out-of-Scope Items
    
    - Explicitly list excluded entities / edge cases (e.g., non‑covered benefits, external lab PDFs).
        
- Assumptions
    
    - E.g., “Only adjudicated claims are loaded”, “Only primary coverage payers included”.
        
- Constraints
    
    - E.g., “No PHI in lower environments”, “Load is daily incremental only”.
        
- Related Specifications / Artifacts
    
    - Links/IDs for ERDs, interface specs (e.g., HL7, FHIR), value set catalogs, business rule docs.
        

---

## 3. Source system inventory

Capture each contributing system and feed in a structured way so field‑level lines can reference them via keys.[[adeptia](https://www.adeptia.com/blog/data-mapping-healthcare)]​

Per source system:

- Source System ID
    
- Source System Name (e.g., Epic Clarity, Cerner, Facets, OptumRx, LabCorp feed)
    
- System Type (EHR, claims adjudicator, PBM, registry, CRM, etc.)
    
- Data Feed / Interface Name (e.g., “ADT HL7 v2 feed”, “837P file”, “FHIR R4 bulk export”)
    
- Environment (Prod, Test, etc.)
    
- Connection / Access Pattern (batch file, API, message queue)
    
- Source Data Model Reference
    
    - Schema names, main tables, relevant HL7 message types, FHIR resource types.
        
- Data Currency / Latency (e.g., real‑time, hourly, daily, T+1, monthly)
    
- Data Ownership and SMEs (business and technical contacts)
    
- Known Data Quality Issues and Peculiarities
    
    - E.g., “Historical encounters missing discharge time”, “Race codes inconsistent pre‑2018”.
        

---

## 4. Target model overview

This section describes the target structures so the field‑level sheet can reference canonical entities and fields consistently.[[adeptia](https://www.adeptia.com/blog/data-mapping-document-template)]​

Per target subject area:

- Target Model Name / Version (e.g., “EDW v3 Encounter Mart”, “OMOP v5.4”, “FHIR R4”)
    
- Target Schema / Dataset Names
    
- Key Target Entities (tables/resources)
    
    - Name, brief description, primary key(s), natural key(s).
        
- General Modeling Conventions
    
    - Surrogate key patterns, SCD type (for dimensions), partitioning, soft‑deletion flags.
        
- Target Coding / Terminology Standards
    
    - E.g., SNOMED CT for problems, LOINC for labs, RxNorm/NDC for meds, ICD‑10‑CM/PCS, HCPCS, CPT.
        

---

## 5. Field‑level mapping (core template)

This is the primary grid, typically implemented as a table/tab. Each row is a target field; each row can support multiple sources via structured subfields or repeated sections.[[projectresources.cdt.ca](https://projectresources.cdt.ca.gov/wp-content/uploads/sites/50/2017/08/DataMappingTemplate.xlsx)]​

## 5.1 Recommended columns (per target field)

- Target Entity Name
    
- Target Field Name
    
- Target Field Description (business definition)
    
- Target Data Type
    
- Target Field Length / Precision / Scale
    
- Target Field Nullable? (Y/N)
    
- Target Default Value / Behavior (e.g., NULL, 0, ‘UNKNOWN’, derived, auto‑generated)
    
- Target Domain / Value Set (name of value set reference)
    
- Target Coding System / Standard (e.g., SNOMED, LOINC, ICD‑10, local code)
    
- Target Key Role
    
    - Indicator: primary key, foreign key, natural key, degenerate key, SCD attribute type.
        
- Sensitivity / PHI Classification (e.g., direct identifier, quasi‑identifier, de‑identified, non‑PHI)
    
- Security / Masking Rules (e.g., encrypt at rest, tokenization, masking in non‑prod)
    
- Lineage Category
    
    - Direct copy / Transformed / Derived / Calculated / Lookup / Aggregated.
        

---

## 6. Source field details and transformations

Because healthcare mappings often involve multiple sources feeding the same target field (for example, problem lists from EHR and diagnoses from claims), structure the template to handle one‑to‑many mappings per target field.[[bryteflow](https://bryteflow.com/source-to-target-mapping-guide-what-why-how/)]​

You can implement this as either:

- A single wide table with repeated column groups (Source1__, Source2__), or
    
- A separate “Source Details” sheet keyed by a Target Field Mapping ID.
    

## 6.1 Core source columns (for each source contributing to the target field)

- Target Field Mapping ID (FK back to the main mapping row)
    
- Source System ID / Name
    
- Source Feed / Interface Name
    
- Source Entity / Table / Resource Name
    
- Source Field / Element Name
    
- Source Field Description
    
- Source Data Type / Length
    
- Source Coding System / Value Set (e.g., local code set, HL7 table, FHIR CodeSystem)
    
- Source Primary Key(s) / Join Keys
    
- Source Filter Conditions
    
    - E.g., “only ORC‑1 = ‘NW’ for new orders”, “Encounter type in (’INPATIENT’, ’OBS’)”.
        

## 6.2 Transformation logic and rules

This is where all transformation rules are documented at a level that a data engineer can implement and a reviewer can validate.[[linkedin](https://www.linkedin.com/pulse/how-prepare-effective-source-to-target-mapping-excel-bernacki-tzfbf)]​

- Transformation Category
    
    - e.g., Reformat, Standardize, Map codes, Derive, Aggregate, De‑duplicate, Lookup.
        
- Business Rule Description (plain language)
    
    - E.g., “For inpatient stays, encounter_end is discharge datetime; for observation, use check‑out time.”
        
- Technical Rule / Expression
    
    - Pseudocode, SQL, or expression, e.g., `CASE WHEN src.discharge_dt IS NOT NULL THEN src.discharge_dt ELSE src.last_contact_dt END`.
        
- Data Type / Format Conversions
    
    - E.g., date formats (HL7 TS → target datetime), numeric casts, string trimming.
        
- Code Mapping / Normalization
    
    - Reference to a separate lookup table or value set (see section 7).
        
- Unit Conversion Rules
    
    - E.g., lb → kg, mg/dL → mmol/L, with formula.
        
- Multi‑source Resolution / Priority Rules
    
    - E.g., “Prefer EHR vitals over claims if both exist for same patient/date.”
        
- Default / Imputation Rules
    
    - E.g., “If smoking status missing, set to ‘UNKNOWN’, do not infer.”
        
- De‑duplication / Consolidation Logic
    
    - E.g., rules to consolidate multiple HL7 messages into a single encounter record.
        
- Effective Dating and History Handling
    
    - E.g., SCD type logic, “valid_from/valid_to” derivation from source timestamps.
        

## 6.3 Validation and quality rules (per target field)

Embedding validation expectations directly in the mapping document supports test planning and continuous monitoring.[[adeptia](https://www.adeptia.com/blog/data-mapping-document-template)]​

- Required Field Rule
    
    - E.g., “Must be non‑null for all admitted encounters.”
        
- Acceptable Range / Thresholds
    
    - E.g., systolic BP between 60 and 260; age between 0 and 115.
        
- Pattern / Format Constraints
    
    - E.g., MRN pattern, phone number, ZIP code, NPI format.
        
- Referential Integrity Rules
    
    - E.g., “encounter.patient_id must exist in patient entity.”
        
- Cross‑Field Consistency Rules
    
    - E.g., “Discharge date must be ≥ admit date.”
        
- Duplicate / Uniqueness Rules
    
    - E.g., constraints on natural keys (e.g., “one active enrollment per member and product at a time”).
        
- Known Data Quality Exceptions and Tolerances
    
    - E.g., “Legacy system may have 0.1% of encounters with missing discharge date; log but do not reject.”
        
- Validation Method / Automation Notes
    
    - E.g., referenced to profiling scripts, automated test harnesses, or data quality dashboards.
        

---

## 7. Value set and code mapping sections

Healthcare mappings almost always require explicit code translations between local systems and canonical standards.[[kms-technology](https://kms-technology.com/blog/data-mapping-in-healthcare/)]​

Create dedicated sheets/tables to keep these maintainable:

## 7.1 Value set definition

- Value Set ID / Name
    
- Domain (e.g., race, ethnicity, marital status, encounter type)
    
- Description / Business Context
    
- Governing Standard / Source (e.g., CDC race categories, HL7 table, local governance group)
    
- Version / Effective Dates
    
- Steward / Owner
    
- Reference to External Specification (URL/ID if applicable)
    

## 7.2 Code translation table

- Mapping ID
    
- Source System ID
    
- Source Code
    
- Source Code Description
    
- Source Code System (if applicable)
    
- Target Canonical Code
    
- Target Canonical Description
    
- Target Code System (e.g., SNOMED, LOINC, RxNorm)
    
- Mapping Type (1:1, 1:many, many:1, approximate)
    
- Mapping Priority / Confidence (if multiple possible targets)
    
- Status (Active / Deprecated)
    
- Notes / Edge Cases
    

---

## 8. Record‑level lineage and joins

Document how entities in different systems relate and how joins are constructed, especially for patient and encounter identities.[[adeptia](https://www.adeptia.com/blog/data-mapping-healthcare)]​

- Master Identity Strategy
    
    - E.g., MPI details, member ID strategy, enterprise provider ID strategy.
        
- Key Join Paths
    
    - E.g., “EHR patient → MPI → member table”, “Claim header → detail → line‑level diagnosis.”
        
- Surrogate Key Logic
    
    - How surrogate keys are generated for patients, encounters, providers, payers, etc.
        
- Merge / Split Rules
    
    - E.g., merging multiple source encounters into one target stay; splitting bundled claims into encounters.
        
- Source Priority for Conflicting Records
    
    - E.g., for provider demographics, “credentialing system supersedes others.”
        

---

## 9. Operational and load behavior

This section supports implementation and run‑time operations.[[bryteflow](https://bryteflow.com/source-to-target-mapping-guide-what-why-how/)]​

- Load Type
    
    - Initial full load vs incremental vs change data capture.
        
- Incremental Logic / Watermarks
    
    - E.g., “Use encounter_last_updated_ts and load only records where ts > last watermark.”
        
- Load Frequency and Scheduling
    
    - E.g., “Daily at 02:00 Central; retry on failure up to 3 times.”
        
- Error Handling Strategy
    
    - Reject vs quarantine, error queues, partial‑fail behavior.
        
- Retry / Recovery Procedures
    
    - How to re‑run failed loads, re‑process files, or re‑play messages.
        
- Performance Considerations
    
    - Expected volume, partitioning keys, indexes, target SLAs.
        
- Monitoring and Logging Requirements
    
    - Metrics and logs required to validate mapping (e.g., record counts, DQ alerts).
        

---

## 10. Security, privacy, and compliance

Healthcare mappings must clearly capture PHI handling and regulatory constraints.[[kms-technology](https://kms-technology.com/blog/data-mapping-in-healthcare/)]​

- PHI / PII Classification Matrix
    
    - For each entity/field category: classification and handling requirements.
        
- De‑identification / Pseudonymization Rules
    
    - E.g., Safe Harbor rules, limited dataset rules, hashing of identifiers.
        
- Environment‑Specific Handling
    
    - What gets masked or excluded in dev/test/stage vs prod.
        
- Access Control Requirements
    
    - Role‑based access notes related to sensitive fields or tables.
        
- Retention and Purge Rules
    
    - How long mapped data is retained and how purges propagate from source to target.
        
- Audit / Traceability Requirements
    
    - Fields or structures needed to trace target values back to specific source records and loads.
        

---

## 11. Testing, sign‑off, and governance

Finally, embed testing and approval information in the same artifact so the mapping document is the single source of truth.[[linkedin](https://www.linkedin.com/pulse/how-prepare-effective-source-to-target-mapping-excel-bernacki-tzfbf)]​

- Test Scenarios / Cases per Entity
    
    - Functional, edge cases, negative tests, regression tests.
        
- Sample Data References
    
    - IDs of test patients/encounters/claims or synthetic datasets.
        
- Reconciliation Rules
    
    - Expected source vs target counts, sums, aggregate comparisons.
        
- UAT Sign‑off
    
    - Stakeholder names, dates, and conclusions.
        
- Ongoing Change Management
    
    - Process for proposing, reviewing, and approving mapping changes.
        

---

## 12. Suggested physical layout (for Excel/CSV/DB)

To make this usable both by hand and as an automated output format, a common pattern is:

- One “Document_Metadata” section or table.
    
- One “Source_Systems” table.
    
- One “Target_Entities” table.
    
- One “Field_Mapping” table (one row per target field + high‑level attributes).
    
- One “Field_Mapping_Source_Details” table (one row per target field per source).
    
- One “Code_Value_Sets” table.
    
- One “Code_Translations” table.
    
- One “Rules_and_Validations” table (optionally separate, keyed by mapping ID).
    
- One “Operational_Profile” table (load and monitoring details).
    

With that structure, you can:

- Populate by hand (e.g., Excel workbook with the above tabs).
    
- Generate from tools (e.g., export as CSV/JSON using the same logical schema).
    

This template is intentionally broad so that you can trim or extend sections for specific projects (e.g., a pure claims→OMOP migration vs a mixed EHR+claims FHIR build) without changing the overall structure.
