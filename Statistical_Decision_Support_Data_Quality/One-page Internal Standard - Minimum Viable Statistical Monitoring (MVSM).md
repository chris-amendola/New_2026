**Audience:** Analytics, data engineering, analytics engineering  
**Purpose:** Operational clarity and enforcement  
**Tone:** Precise, neutral, non-academic

# Minimum Viable Statistical Monitoring (MVSM)

**Internal Standard — Provider Analytics**

## Purpose

To ensure that data used for clinical, operational, financial, or provider compensation decisions is continuously monitored for silent failures that could materially impact decisions.

This standard defines the _minimum required statistical controls_ for production data assets.

---

## Scope

This standard applies to **all production datasets** that are:

1. Used in executive, clinical, financial, quality, or provider compensation reporting
    
2. Loaded on a recurring cadence (daily or weekly)
    
3. Capable of influencing decisions that cannot be fully corrected retrospectively
    

Exploratory, ad hoc, and research datasets are explicitly out of scope.

---

## Required Failure Coverage

Each in-scope dataset **must** be monitored for the following failure classes:

|Failure Class|Required|
|---|---|
|Volume & completeness|Yes|
|Distributional drift|Yes|
|Referential integrity|Yes (where joins exist)|
|Timeliness & latency|Yes (for daily feeds)|
|Semantic coherence|Yes (at least one check per domain)|

---

## Required Statistical Controls

### 1. Volume & Completeness

- Daily (or load-level) row count tracking
    
- Historical baseline adjusted for day-of-week
    
- Control chart (Shewhart or EWMA)
    

**Alert condition:**

- Outside 3σ, or two consecutive outside 2σ
    

---

### 2. Distributional Drift

- At least one critical business dimension (e.g., payer, service line, location)
    
- PSI or Chi-square comparison vs historical baseline
    

**Alert condition:**

- PSI ≥ 0.2 (review)
    
- PSI ≥ 0.3 (escalation)
    

---

### 3. Referential Integrity

- Orphan rate monitoring
    
- Join cardinality ratio tracking
    

**Alert condition:**

- Orphan rate exceeds historical 99th percentile
    
- Cardinality ratio deviates >2× baseline
    

---

### 4. Timeliness

- Load completion time tracking
    
- P95 latency monitoring
    

**Alert condition:**

- P95 exceeds SLA tolerance
    
- Two late loads within five business days
    

---

### 5. Semantic Coherence

- At least one cross-metric relationship per domain
    
- Correlation or regression-residual monitoring
    

**Alert condition:**

- Relationship deviates beyond historical control limits
    

---

## Alert Hygiene

- Alerts must include:
    
    - What changed
        
    - Degree of abnormality
        
    - Likely failure class
        
    - Known benign explanations
        
- Alert volume target: **≤2 alerts per week per domain**
    

---

## Ownership

Each monitored dataset must have:

- A named **data owner** (business meaning)
    
- A named **technical steward** (pipeline health)
    
- A documented **escalation path**
    

---

## Non-Goals

This standard does **not** require:

- Machine learning–based anomaly detection
    
- Zero false positives
    
- Automated remediation
    

---

**Compliance with this standard is required for all decision-bearing production data assets.**