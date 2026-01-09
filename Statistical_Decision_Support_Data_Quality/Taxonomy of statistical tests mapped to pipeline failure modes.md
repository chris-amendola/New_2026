Alright, let’s get concrete and operational. What follows is a **practitioner-grade taxonomy**, not a stats textbook. This is the mental model I’ve used (and taught) inside provider organizations to connect _failure modes people actually experience_ with _statistical tests that surface them early and consistently_.

I’ll organize this in five layers:

1. **Canonical data pipeline failure modes**
    
2. **Observable statistical symptoms**
    
3. **Appropriate statistical tests / methods**
    
4. **When the test is most useful**
    
5. **Healthcare-specific nuances**
    

Think of this as a _differential diagnosis_ for data pipelines.

---

## 1. Volume & Completeness Failures

_(“Are we getting the right amount of data?”)_

### Failure modes

- Dropped feeds (partial or full)
    
- Upstream filter changes
    
- Late-arriving data not backfilled
    
- Source system downtime
    
- Accidental inner joins
    
- Incremental load window misalignment
    

### Statistical symptoms

- Mean shift in row counts
    
- Increased zero inflation
    
- Day-of-week pattern breaks
    
- Sudden variance collapse
    

### Tests & methods

|Test / Method|What it detects|Notes|
|---|---|---|
|**Statistical Process Control (SPC)** (Shewhart, EWMA)|Mean and variance shifts|Primary workhorse|
|**Z-score vs historical baseline**|Abrupt volume changes|Simple, interpretable|
|**Poisson / Negative Binomial tests**|Count anomalies|Better than normal for claims|
|**Change-point detection** (CUSUM, Bayesian)|Structural breaks|Excellent for silent failures|
|**Run tests**|Non-random patterns|Flags systematic issues|

### When most useful

- Daily loads
    
- High-volume fact tables (encounters, claims, charges)
    

### Healthcare nuance

- Claims lag requires _as-of-date_ baselining
    
- Seasonality must be modeled (holidays, flu season)
    
- Don’t compare raw counts across calendar shifts
    

---

## 2. Distributional Drift

_(“The rows are there, but are they the same kind of rows?”)_

### Failure modes

- Code system changes (CPT, ICD, DRG)
    
- Mapping regressions
    
- Vendor updates
    
- Dimension table corruption
    
- Semantic reinterpretation (same code, new meaning)
    

### Statistical symptoms

- Category frequency shifts
    
- Tail behavior changes
    
- Entropy increase or collapse
    
- Subpopulation imbalance
    

### Tests & methods

|Test / Method|What it detects|Notes|
|---|---|---|
|**Chi-square test**|Categorical distribution shifts|High power, low nuance|
|**Population Stability Index (PSI)**|Practical drift magnitude|Exec-friendly|
|**Kolmogorov–Smirnov test**|Continuous distribution change|Nonparametric|
|**Jensen–Shannon divergence**|Distribution similarity|Stable, bounded|
|**Entropy monitoring**|Coding richness collapse|Underrated|

### When most useful

- Dimension-heavy marts
    
- Clinical coding layers
    
- Risk and quality inputs
    

### Healthcare nuance

- Some drift is real care change—context matters
    
- Use _pairwise_ comparisons (payer, specialty, location)
    
- PSI thresholds should vary by domain
    

---

## 3. Referential Integrity & Relational Failures

_(“Do the relationships still make sense?”)_

### Failure modes

- Dimension key rekeys
    
- Late dimension updates
    
- Surrogate key regeneration
    
- Fact–dimension mismatch
    
- Orphaned facts
    

### Statistical symptoms

- Rising orphan rates
    
- Join cardinality shifts
    
- Unexpected many-to-many patterns
    
- Sudden dimensional sparsity
    

### Tests & methods

|Test / Method|What it detects|Notes|
|---|---|---|
|**Proportion tests** (binomial)|Orphan rate increases|Simple, powerful|
|**Cardinality ratio tracking**|Relationship drift|Great early signal|
|**Benford’s Law (limited use)**|Synthetic ID anomalies|Use cautiously|
|**Graph connectivity metrics**|Structural changes|Advanced, high value|

### When most useful

- Star schemas
    
- Slowly changing dimensions
    
- Provider / location hierarchies
    

### Healthcare nuance

- Provider attribution changes can be legitimate
    
- Watch for _correlated_ orphan spikes across facts
    
- Late-binding dims need separate baselines
    

---

## 4. Timeliness & Latency Failures

_(“Is the data arriving when it should?”)_

### Failure modes

- Interface delays
    
- Partial upstream processing
    
- Job dependency drift
    
- SLA regressions
    
- Silent retries masking issues
    

### Statistical symptoms

- Load-time variance increase
    
- Right-tail latency growth
    
- Partial-day completeness
    
- Intermittent failures
    

### Tests & methods

|Test / Method|What it detects|Notes|
|---|---|---|
|**Control charts on latency**|SLA degradation|EWMA works well|
|**Survival analysis**|Time-to-arrival drift|Very underused|
|**Quantile monitoring (P95/P99)**|Tail risk|Mean hides pain|
|**Autocorrelation analysis**|Systemic delays|Flags cascading failures|

### When most useful

- Near-real-time feeds
    
- Operational dashboards
    
- Executive morning reports
    

### Healthcare nuance

- Clinical vs claims feeds have different tolerance
    
- SLA violations often cluster by vendor
    
- Timeliness errors cause _decision latency_, not obvious errors
    

---

## 5. Value Integrity & Numerical Corruption

_(“Are the numbers plausible?”)_

### Failure modes

- Unit changes (cents ↔ dollars)
    
- Sign flips
    
- Truncation
    
- Overflow / precision loss
    
- Double-counting
    

### Statistical symptoms

- Mean magnitude jumps
    
- Variance explosion
    
- Rounding artifacts
    
- Correlation breakdowns
    

### Tests & methods

|Test / Method|What it detects|Notes|
|---|---|---|
|**Outlier detection** (IQR, robust Z)|Extreme values|Use robust stats|
|**Ratio monitoring**|Unit errors|High signal-to-noise|
|**Correlation tracking**|Broken relationships|Underrated|
|**Digit frequency analysis**|Rounding/truncation|Complementary|

### When most useful

- Financial metrics
    
- Utilization rates
    
- Derived KPIs
    

### Healthcare nuance

- Charges ≠ revenue—know the domain
    
- Ratios outperform raw dollar checks
    
- Always anchor to clinical volume
    

---

## 6. Semantic & Logical Consistency Failures

_(“Does the data still mean what we think it means?”)_

### Failure modes

- Business rule changes
    
- Measure definition drift
    
- Inclusion/exclusion logic edits
    
- Vendor “enhancements”
    
- Metric reuse in new contexts
    

### Statistical symptoms

- KPI relationships break
    
- Cross-metric incoherence
    
- Unexpected independence
    
- Sudden stability (false calm)
    

### Tests & methods

|Test / Method|What it detects|Notes|
|---|---|---|
|**Cross-metric correlation tests**|Semantic breakage|Critical for trust|
|**Constraint violation rates**|Logic drift|Binary but powerful|
|**Regression residual monitoring**|Model breakdown|Very strong signal|
|**Invariance testing**|Definition drift|Advanced, high payoff|

### When most useful

- Executive scorecards
    
- Quality metrics
    
- Provider compensation
    

### Healthcare nuance

- Definitions change quietly
    
- Semantic drift causes political pain
    
- Statistical flags depersonalize conflict
    

---

## 7. Putting it together: a diagnostic mindset

Think like a clinician:

|Symptom|Likely Failure Class|
|---|---|
|Volume drop, stable mix|Completeness|
|Stable volume, mix shift|Distribution drift|
|Broken joins|Referential integrity|
|Late dashboards|Timeliness|
|Implausible KPIs|Value integrity|
|Metrics disagree|Semantic failure|

No single test is sufficient. **Redundancy is a feature**, not a flaw—just like medicine.

---

## 8. The meta-lesson

Statistical tests are not “alerts.”  
They are **structured skepticism**.

They:

- Reduce judgment noise
    
- Externalize institutional memory
    
- Create shared evidence
    
- Shift debates from _who’s right_ to _what changed_
    

If you want, next steps could be:

- A reference architecture for implementing this taxonomy in dbt / Airflow
    
- Recommended default thresholds (and when to override them)
    
- A “minimum viable statistical monitoring” standard for provider orgs
    
- How to socialize this with non-technical leadership
    

Say where you want to go next.