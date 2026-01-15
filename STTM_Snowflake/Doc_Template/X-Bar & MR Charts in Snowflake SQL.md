Great choice. **X̄ / MR charts are exactly the right SPC primitive for GI Alliance**: low ceremony, robust to messy healthcare data, and interpretable when something _actually_ breaks.

Below is a **Snowflake-native implementation** you can lift directly. I’ll keep it opinionated and GI-specific.

---

# 📈 X̄ / MR Charts in Snowflake SQL

**(Individual Values + Moving Range)**

This pattern assumes:

- Daily grain
    
- One value per day per site/system/metric
    
- No sub-grouping (perfect for encounter counts, charges, claims, etc.)
    

---

## 1️⃣ Define the Monitoring Series (X values)

Example: **daily encounter volume by site + EMR**

```sql
WITH x_values AS (
    SELECT
        CAST(service_date AS DATE) AS obs_date,
        site_id,
        emr_system,
        COUNT(*) AS x_value
    FROM fact_encounter
    WHERE service_date >= DATEADD(day, -120, CURRENT_DATE)
    GROUP BY 1,2,3
)
```

This `x_value` is what your **X̄ chart monitors**.

---

## 2️⃣ Compute Moving Range (MR)

Moving Range = |Xₙ − Xₙ₋₁|

```sql
WITH mr_values AS (
    SELECT
        obs_date,
        site_id,
        emr_system,
        x_value,
        ABS(
            x_value
            - LAG(x_value) OVER (
                PARTITION BY site_id, emr_system
                ORDER BY obs_date
            )
        ) AS mr_value
    FROM x_values
)
```

> GI note: MR is _excellent_ at catching step changes caused by EMR template edits or feed truncation.

---

## 3️⃣ Establish Baseline (First 30 Stable Days)

You **must baseline explicitly**. No silent drift.

```sql
WITH baseline AS (
    SELECT
        site_id,
        emr_system,
        AVG(x_value) AS xbar,
        AVG(mr_value) AS mrbar
    FROM mr_values
    WHERE obs_date BETWEEN
          DATEADD(day, -60, CURRENT_DATE)
      AND DATEADD(day, -30, CURRENT_DATE)
    GROUP BY 1,2
)
```

---

## 4️⃣ Control Limits (X̄ / MR Constants)

For **individuals charts**, use SPC constants:

|Constant|Value|
|---|---|
|d₂|1.128|
|D₄|3.267|

---

### **X̄ Chart Limits**

[  
UCL_X = \bar{X} + 3 \times \frac{\bar{MR}}{1.128}  
]  
[  
LCL_X = \bar{X} - 3 \times \frac{\bar{MR}}{1.128}  
]

```sql
SELECT
    m.obs_date,
    m.site_id,
    m.emr_system,
    m.x_value,
    b.xbar,
    b.xbar + 3 * (b.mrbar / 1.128) AS ucl_x,
    b.xbar - 3 * (b.mrbar / 1.128) AS lcl_x
FROM mr_values m
JOIN baseline b
  ON m.site_id = b.site_id
 AND m.emr_system = b.emr_system;
```

---

### **MR Chart Limits**

[  
UCL_{MR} = 3.267 \times \bar{MR}  
]  
[  
LCL_{MR} = 0  
]

```sql
SELECT
    m.obs_date,
    m.site_id,
    m.emr_system,
    m.mr_value,
    b.mrbar,
    3.267 * b.mrbar AS ucl_mr
FROM mr_values m
JOIN baseline b
  ON m.site_id = b.site_id
 AND m.emr_system = b.emr_system;
```

---

## 5️⃣ Signal Detection (X̄ + MR)

### **Rule 1: Point Outside Control Limits**

```sql
CASE
    WHEN x_value > ucl_x OR x_value < lcl_x THEN 1
    ELSE 0
END AS x_limit_breach
```

```sql
CASE
    WHEN mr_value > ucl_mr THEN 1
    ELSE 0
END AS mr_limit_breach
```

---

## 6️⃣ Run Rules (Critical for GI Noise)

### **Rule: 3 Consecutive X̄ Breaches**

```sql
SUM(x_limit_breach) OVER (
    PARTITION BY site_id, emr_system
    ORDER BY obs_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
) AS rolling_x_breaches
```

---

### **Rule: Sustained Shift (8 Points One Side of Mean)**

```sql
CASE
    WHEN x_value > xbar THEN 1
    ELSE -1
END AS side_flag
```

```sql
SUM(side_flag) OVER (
    PARTITION BY site_id, emr_system
    ORDER BY obs_date
    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
) AS sustained_shift_flag
```

Trigger when `ABS(sustained_shift_flag) = 8`.

---

## 7️⃣ Combined Alert Logic (Recommended)

```sql
CASE
    WHEN mr_limit_breach = 1 THEN 'STEP_CHANGE'
    WHEN rolling_x_breaches >= 3 THEN 'VOLUME_DROP'
    WHEN ABS(sustained_shift_flag) = 8 THEN 'MEAN_SHIFT'
    ELSE NULL
END AS signal_type
```

---

## 8️⃣ Severity Mapping (GI-Tuned)

```sql
CASE
    WHEN signal_type = 'STEP_CHANGE' THEN 5   -- feed or EMR break
    WHEN signal_type = 'VOLUME_DROP' THEN 4   -- operational impact
    WHEN signal_type = 'MEAN_SHIFT' THEN 3    -- workflow change
    ELSE 1
END AS severity
```

---

## 9️⃣ Persist Signals (Operationalize It)

```sql
INSERT INTO data_quality_alerts
SELECT
    obs_date,
    'CLINICAL',
    'ENCOUNTER_VOLUME',
    site_id,
    severity,
    x_value,
    xbar,
    (x_value - xbar) / xbar,
    'OPEN',
    CURRENT_TIMESTAMP
FROM xbar_mr_signals
WHERE signal_type IS NOT NULL;
```

---

## 🔍 Why X̄/MR Is Perfect for GI Alliance

|Problem|X̄/MR Strength|
|---|---|
|EMR template changes|MR spikes instantly|
|Vendor feed truncation|X̄ breach + MR|
|Gradual workflow drift|8-point rule|
|Seasonal GI volume|Stable MR despite swings|
|Acquisition onboarding|Re-baselining friendly|

---

## 🧠 Principal-Level Advice

- **MR catches breaks faster than X̄**
    
- **X̄ tells leadership how bad it is**
    
- **Run rules prevent pager fatigue**
    
- **Baseline governance matters more than math**
    

If you want next, I can:

- Add **automatic re-baselining with approvals**
    
- Extend this to **charges, payments, CPT volumes**
    
- Convert this to **dbt models + tests**
    
- Create **executive-friendly SPC visuals**
    
- Map **signal → ETL failure taxonomy**
    

Just say the word.