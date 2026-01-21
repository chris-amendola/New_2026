Snowflake Cortex AI STTM: Architecture and Intent
---
## Design Philosophy 

1. **Assist, not decide** (human is the authority)
2. Be **deterministic-first, probabilistic-second**
3. Leave a **full audit trail**
4. Live **entirely inside Snowflake**
5. Produce artifacts analysts already understood (STMs, profiles, notes)

---
## High-Level Architecture (Three Planes)

```
┌───────────────────────────────────────────┐
│  Plane 1: Deterministic Data Intelligence │
└───────────────────────────────────────────┘
        ↓ (features, evidence)
┌───────────────────────────────────────────┐
│  Plane 2: Cortex Reasoning Layer          │
└───────────────────────────────────────────┘
        ↓ (suggestions + confidence)
┌───────────────────────────────────────────┐
│  Plane 3: Human STM Workflow              │ 
└───────────────────────────────────────────┘
```

Each plane is independently valuable.

---

## Plane 1 — Deterministic Evidence Layer (The Backbone)

 (Analytics engineering based)
### Inputs/Features
**Source column features**:
- Data type
- Cardinality
- Null %
- Distinct ratio
- Value length stats
- Regex hits (dates, codes, IDs)
- Domain frequency (top N values)
- Foreign-key-like behavior
- PHI heuristics (names, DOB patterns, MRN shapes)

 ***Target column features:**:
- Business definition 
- Expected domain / type
- Required vs optional
- Historical usage (if available)

**Key design choice**

> These features are stored as **structured evidence**, not free text.

So:

> “The model is reasoning _from_ facts we control.”

---

## Plane 2 — Cortex Reasoning Layer (For the Assist)

Harness Cortex

### 1. Prompt Strategy: Evidence-In, Suggestion-Out

Not:

> “Map this source to target.”

But:

> “Given this evidence, what are the _top 3 plausible mappings_, and why?”

**Prompt inputs (structured):**

- Source column evidence (JSON-like)
- Target column metadata
- Optional: Similar past mappings
- Guardrails (no PHI guessing, no transformations beyond X)

**Prompt outputs (strict schema):**

- Suggested target column(s)
- Confidence score (model-estimated)
- Evidence references (which features mattered)
- Risks / ambiguities

---
### 2. Cortex Is Not a Single Call

Series of successive calls - not a one-shot

|Call|Purpose|
|---|---|
|Column role classifier|ID / measure / code / flag|
|Domain matcher|Which target domains resemble this|
|Transformation suggester|Cast, trim, normalize (only simple ops)|
|PHI risk narrator|“This looks like patient-identifying data because…”|

Each produces **narrow, testable output**.

---
## Plane 3 — Human STM Workflow 
### The STM Becomes a **Conversation Log**

For each mapping row:
- Source column
- Target column
- AI suggestion (collapsed by default)
- Confidence score
- “Why” (expandable)
- Analyst decision:
    - Accept
    - Modify
    - Reject
- Analyst comment (required if override)

**Critical insight**

> Overrides are informational _gold_, not failures.

They become training data and governance evidence.

---
## What This Architecture Deliberately Avoids

❌ End-to-end auto-mapping  
❌ Black-box embeddings without evidence  
❌ External vector DBs  
❌ “Let the model figure it out” logic  
❌ Anything that can’t be explained in a design review

---
## Unplaced thoughts

- layered, auditable, and Snowflake-native
- saves time but respects judgment
- visible, safe, produces tangible artifacts

> Even if Cortex disappeared tomorrow, Plane 1 still improves STM quality.

Value add no matter what happens.

---
