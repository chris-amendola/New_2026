Below is a **practical, GI-Alliance–specific Data Quality Early-Warning Playbook**—the kind you can actually run in production without buying tools or boiling the ocean. This is written from the standpoint of a GI MSO with **multiple EMRs, heterogeneous billing systems, heavy vendor feeds, and executive sensitivity to revenue and provider trust**.

Think of this as **“SPC for healthcare data, disguised as common sense.”**

---

# 📕 Data Quality Early-Warning Playbook

**GI Alliance | Principal Data Analyst**

---

## 1️⃣ Purpose & Philosophy

### **Why this exists**

At GI Alliance, _bad data doesn’t just break dashboards_—it:

- Misstates provider productivity
- Delays revenue recognition
- Undermines quality reporting
- Erodes physician trust (which is hard to get back)

This playbook focuses on **early detection**, not forensic cleanup.

> **Goal:** Detect data degradation _before_ it impacts decisions, revenue, or clinical credibility.

---

## 2️⃣ Guiding Principles (Non-Negotiable)

1. **Stability > Perfection**  
    We monitor _change_, not correctness in the abstract.
2. **Signal > Volume**  
    10 strong checks beat 200 noisy ones.
3. **Domain-Aware Monitoring**  
    Clinical, financial, and operational data fail differently.
4. **Actionable Alerts Only**  
    If no one knows what to do when it fires, don’t alert.
5. **Analytics Owns Detection; Engineering Owns Fixes**  
    Clear contract, no finger-pointing.

---

## 3️⃣ GI-Specific Failure Modes (What Actually Breaks)

### **Clinical Data**

- Encounter drops due to EMR workflow changes
- CPT under-coding after template updates
- Provider attribution drift after acquisitions
- Service dates misaligned with posting dates

### **Revenue & Claims**

- Charge → payment mismatch by period
- Denials spike after payer rule changes
- 835 ingestion partial failures
- Legacy billing cutovers breaking continuity

### **Operational**

- Appointment volumes suddenly flatline
- Location codes change without notice
- Provider FTE logic breaks after HR updates

---

## 4️⃣ Early-Warning Signal Framework

Every monitored metric must define:

|Element|Required|
|---|---|
|Metric|What is being measured|
|Baseline|Historical expectation|
|Tolerance|Acceptable variation|
|Rule|What constitutes “abnormal”|
|Owner|Who investigates|
|Severity|How bad if wrong|
|Action|What to do next|

---

## 5️⃣ Tiered Monitoring Model (Critical for GI Alliance)

### 🟥 **Tier 1 — Revenue & Compliance Critical**

_Failure = financial or regulatory risk_

Examples:

- Total charges by service date
- Payments by payer
- Encounter counts
- CPT volume for high-revenue procedures (e.g., colonoscopy, EGD)

**Monitoring cadence:** Daily  
**Tolerance:** Tight  
**Alert channel:** Engineering + Analytics + Finance

---

### 🟨 **Tier 2 — Operational Decision Support**

_Failure = poor decisions_

Examples:

- No-show rates
- Provider productivity
- Appointment lead times
- Site throughput

**Monitoring cadence:** Daily / Weekly  
**Tolerance:** Moderate  
**Alert channel:** Analytics first

---

### 🟩 **Tier 3 — Analytical Enrichment**

_Failure = degraded insight, not crisis_

Examples:

- Diagnosis distributions
- Risk stratification inputs
- Comorbidity capture rates

**Monitoring cadence:** Weekly  
**Tolerance:** Loose  
**Alert channel:** Analytics backlog

---

## 6️⃣ Core Early-Warning Checks (GI-Optimized)

### **A. Volume Stability Checks**

> “Did something stop flowing?”

- Encounter count by EMR × site
- Charges by service date
- Claims received vs expected
- 835 remits received

**Method:**

- Rolling 7-day mean
- Alert on >3σ deviation or >20% drop

---

### **B. Distribution Drift Checks**

> “Same volume, wrong shape”

- CPT mix by site
- Payer mix by location
- Diagnosis code distribution
- Place of service codes

**Method:**

- PSI or KS test vs baseline
- Alert on sustained drift

---

### **C. Revenue Reconciliation Checks**

> “Do dollars still tie?”

- Charges → payments → adjustments by period
- Net revenue vs historical ratios
- Refund/void rates

**Method:**

- Period balancing logic
- Variance thresholds by payer

---

### **D. Timeliness & Latency Checks**

> “Is data arriving too late to be useful?”

- EMR → warehouse lag
- Claims adjudication lag
- Vendor feed arrival SLAs

**Method:**

- Max allowed lag per source
- Alert on SLA breach

---

### **E. Referential Integrity & Attribution**

> “Are joins still safe?”

- Encounter → provider
- Encounter → location
- Claim → encounter
- CPT → charge

**Method:**

- Orphan rate thresholds
- Sudden increases flagged

---

## 7️⃣ Run Rules (How We Detect Real Problems)

Avoid alert fatigue by requiring **patterns**, not one-offs.

Examples:

- 3 consecutive days below lower control limit
- 7 points trending downward
- Step change >25% sustained for 2 days
- Variance exceeds historical max

> If it fires once, log it.  
> If it fires twice, watch it.  
> If it fires thrice, escalate.

---

## 8️⃣ Severity Scoring (What Gets Attention)

|Score|Meaning|Example|
|---|---|---|
|1|Informational|Minor code drift|
|2|Investigate|Appointment counts off|
|3|High|CPT volume drop|
|4|Critical|Charges missing|
|5|Executive|Revenue misstated|

Severity is based on:

- Revenue impact
- Clinical reporting impact
- Executive visibility
- Compliance risk

---

## 9️⃣ Incident Response Workflow

### **Step 1: Detection**

Automated check fires → logged with:

- Metric
- Baseline
- Deviation
- Suspected domain

### **Step 2: Triage (Analytics)**

Within 24 hours:

- Is this real?
- Is this expected (holiday, cutover)?
- Who owns the source?

### **Step 3: Escalation (Engineering / Vendor)**

- Attach data evidence
- Identify break point
- Assign owner

### **Step 4: Resolution**

- Fix pipeline or mapping
- Backfill if needed
- Document root cause

### **Step 5: Learning**

- Was this detectable earlier?
- Do we need a new check?

---

## 🔁 10️⃣ Re-Baselining Rules (Critical in Acquisitions)

GI Alliance is acquisition-heavy. Re-baselining must be explicit.

Re-baseline only when:

- New site onboarded
- EMR conversion
- Billing system cutover
- Payer contract change

**Requirements:**

- Written approval (Analytics + Business)
- Baseline window defined
- Old baseline archived

---

## 11️⃣ Ownership Model (Avoids Blame)

|Area|Owns Detection|Owns Fix|
|---|---|---|
|Clinical metrics|Analytics|Engineering / EMR|
|Claims ingestion|Analytics|Engineering / Vendor|
|Revenue logic|Analytics|Engineering|
|Vendor feeds|Analytics|Vendor / IT|

Analytics is the **canary**, not the plumber.

---

## 12️⃣ What This Looks Like in Practice (Example)

> **Alert:** Colonoscopy CPT volume dropped 32% at 4 sites  
> **Detection:** Volume SPC breach × 3 days  
> **Root cause:** EMR template update removed CPT default  
> **Impact:** ~$450k delayed charges  
> **Outcome:** Fixed in 48 hours instead of end-of-month discovery

This is the difference between **early warning** and **post-mortem analytics**.

---

## 13️⃣ How You Roll This Out Without Resistance

1. Start with **5 Tier-1 checks**
2. Keep alerts private at first
3. Prove value with one prevented incident
4. Socialize success stories
5. Expand carefully

---

## 🧠 Principal-Level Insight

If leadership trusts _data stability_, they will tolerate _analytic uncertainty_.  
If data feels unreliable, **nothing else matters**.

This playbook positions you not as:

> “The person who checks data”

…but as:

> **“The person who prevents bad decisions before they happen.”**

---
