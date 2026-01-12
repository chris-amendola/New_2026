-- =====================================================================
-- STATISTICAL PROCESS CONTROL FOR HEALTHCARE DATA QUALITY
-- A Practical Guide for Data Engineers and DBAs
-- =====================================================================

-- INTRODUCTION
-- This guide demonstrates how to monitor data quality using Statistical 
-- Process Control (SPC). Think of SPC as an "early warning system" that
-- alerts you when something unusual is happening with your data.

-- Key Concept: Instead of arbitrary thresholds (like "alert if < 95%"),
-- SPC uses the data's own natural variation to detect real problems.

-- =====================================================================
-- SECTION 1: GENERATE EXAMPLE DATA
-- =====================================================================

-- We'll create 60 days of data for three quality measures, each showing
-- a different scenario that could happen in real life.

-- Create a date dimension table
CREATE OR REPLACE TEMPORARY TABLE date_spine AS
SELECT DATEADD(day, SEQ4(), '2024-01-01')::DATE AS measure_date
FROM TABLE(GENERATOR(ROWCOUNT => 60));

-- ---------------------------------------------------------------------
-- MEASURE 1: COMPLETENESS (% of records with diagnosis codes)
-- Scenario: STABLE PROCESS - normal day-to-day variation
-- ---------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE completeness_data AS
SELECT 
    measure_date,
    -- Simulate stable process around 94% with natural variation
    -- Using UNIFORM for random variation (returns 0 to 1)
    94 + (UNIFORM(1, 3, RANDOM()) - 0.5) * 2.5 AS completeness_pct,
    500 + FLOOR(UNIFORM(1, 50, RANDOM()) * 100) AS total_records
FROM date_spine;

-- ---------------------------------------------------------------------
-- MEASURE 2: TIMELINESS (Average days from encounter to data load)
-- Scenario: PROCESS SHIFT - sudden change on day 30
-- ---------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE timeliness_data AS
SELECT 
    measure_date,
    DATEDIFF(day, '2024-01-01', measure_date) AS day_number,
    -- Before day 30: average 2.5 days lag
    -- After day 30: average 4.5 days lag (system change or staffing issue)
    CASE 
        WHEN DATEDIFF(day, '2024-01-01', measure_date) < 30 
        THEN 2.5 + (UNIFORM(1, 4, RANDOM()) - 0.5) * 1.2
        ELSE 4.5 + (UNIFORM(1, 5, RANDOM()) - 0.5) * 1.5
    END AS avg_lag_days,
    450 + FLOOR(UNIFORM(1, 60, RANDOM()) * 80) AS total_records
FROM date_spine;

-- ---------------------------------------------------------------------
-- MEASURE 3: ACCURACY (% of records passing validation rules)
-- Scenario: INCREASED VARIABILITY - becomes less predictable after day 35
-- ---------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE accuracy_data AS
SELECT 
    measure_date,
    DATEDIFF(day, '2024-01-01', measure_date) AS day_number,
    -- Before day 35: stable around 97% with low variation
    -- After day 35: same average but much more variation (inconsistent quality)
    CASE 
        WHEN DATEDIFF(day, '2024-01-01', measure_date) < 35 
        THEN 97 + (UNIFORM(1, 6, RANDOM()) - 0.5) * 1.5
        ELSE 97 + (UNIFORM(1, 7, RANDOM()) - 0.5) * 6
    END AS validation_pass_pct,
    600 + FLOOR(UNIFORM(1, 70, RANDOM()) * 120) AS total_records
FROM date_spine;

-- =====================================================================
-- SECTION 2: SPC TECHNIQUE #1 - CONTROL CHARTS (SHEWHART CHARTS)
-- =====================================================================

-- INTUITION: A control chart is like knowing the "normal range" for your
-- vital signs. If your temperature is usually 98.6°F ± 0.5°F, then 101°F
-- means something is wrong - even though 101°F might be normal for someone else.

-- Control charts calculate:
-- - Center Line (CL): The average when things are normal
-- - Upper Control Limit (UCL): Average + 3 standard deviations
-- - Lower Control Limit (LCL): Average - 3 standard deviations

-- Why 3 standard deviations? In a normal process, 99.7% of points fall
-- within this range. So if a point is outside, it's very unlikely to be
-- random chance - something probably changed.

-- COMPLETENESS CONTROL CHART (Stable Process Example)
CREATE OR REPLACE TEMPORARY TABLE completeness_control_chart AS
WITH baseline AS (
    -- Use first 20 days to establish "normal" baseline
    SELECT 
        AVG(completeness_pct) AS center_line,
        STDDEV(completeness_pct) AS std_dev
    FROM completeness_data
    WHERE measure_date <= DATEADD(day, 20, '2024-01-01')
)
SELECT 
    cd.measure_date,
    cd.completeness_pct AS value,
    b.center_line,
    b.center_line + (3 * b.std_dev) AS ucl,
    b.center_line - (3 * b.std_dev) AS lcl,
    -- Flag points outside control limits
    CASE 
        WHEN cd.completeness_pct > b.center_line + (3 * b.std_dev) THEN 'Above UCL'
        WHEN cd.completeness_pct < b.center_line - (3 * b.std_dev) THEN 'Below LCL'
        ELSE 'In Control'
    END AS status
FROM completeness_data cd
CROSS JOIN baseline b
ORDER BY cd.measure_date;

-- View the results
SELECT * FROM completeness_control_chart;

-- TIMELINESS CONTROL CHART (Process Shift Example)
CREATE OR REPLACE TEMPORARY TABLE timeliness_control_chart AS
WITH baseline AS (
    SELECT 
        AVG(avg_lag_days) AS center_line,
        STDDEV(avg_lag_days) AS std_dev
    FROM timeliness_data
    WHERE day_number < 30
)
SELECT 
    td.measure_date,
    td.avg_lag_days AS value,
    b.center_line,
    b.center_line + (3 * b.std_dev) AS ucl,
    b.center_line - (3 * b.std_dev) AS lcl,
    CASE 
        WHEN td.avg_lag_days > b.center_line + (3 * b.std_dev) THEN 'Above UCL'
        WHEN td.avg_lag_days < b.center_line - (3 * b.std_dev) THEN 'Below LCL'
        ELSE 'In Control'
    END AS status
FROM timeliness_data td
CROSS JOIN baseline b
ORDER BY td.measure_date;

-- View the results


-- =====================================================================
-- SECTION 3: SPC TECHNIQUE #2 - EWMA (Exponentially Weighted Moving Average)
-- =====================================================================

-- INTUITION: EWMA is like a "smart average" that gives more weight to
-- recent data. Think of checking your car's "average MPG" - the display
-- reacts to recent driving but doesn't jump around wildly with every
-- acceleration. EWMA is better than control charts at detecting small,
-- gradual shifts.

-- Lambda (λ) controls how much weight recent data gets:
-- - λ = 0.2 (typical): Reacts smoothly, good for gradual changes
-- - λ = 0.4: Reacts faster to changes
-- - λ = 1.0: Would be the same as just using the current value

-- ACCURACY EWMA (Increased Variability Example)
CREATE OR REPLACE TEMPORARY TABLE accuracy_ewma AS
WITH RECURSIVE ewma_calc AS (
    -- Start: first observation
    SELECT 
        measure_date,
        validation_pass_pct AS value,
        1 AS row_num,
        validation_pass_pct AS ewma
    FROM accuracy_data
    WHERE measure_date = '2024-01-01'
    
    UNION ALL
    
    -- Recursive: calculate EWMA for each subsequent day
    SELECT 
        ad.measure_date,
        ad.validation_pass_pct AS value,
        ec.row_num + 1 AS row_num,
        -- EWMA formula: λ × current_value + (1-λ) × previous_ewma
        -- Using λ = 0.2 (weights last 5 days roughly equally)
        (0.2 * ad.validation_pass_pct) + (0.8 * ec.ewma) AS ewma
    FROM accuracy_data ad
    INNER JOIN ewma_calc ec 
        ON ad.measure_date = DATEADD(day, 1, ec.measure_date)
    WHERE ec.row_num < 60
),
baseline AS (
    -- Calculate control limits from first 20 days
    SELECT 
        AVG(validation_pass_pct) AS mu,
        STDDEV(validation_pass_pct) AS sigma
    FROM accuracy_data
    WHERE day_number < 20
)
SELECT 
    e.measure_date,
    e.value,
    e.ewma,
    b.mu AS center_line,
    -- EWMA control limits are narrower than regular control limits
    -- They account for the smoothing effect
    b.mu + (3 * b.sigma * SQRT(0.2/(2-0.2))) AS ucl,
    b.mu - (3 * b.sigma * SQRT(0.2/(2-0.2))) AS lcl,
    CASE 
        WHEN e.ewma > b.mu + (3 * b.sigma * SQRT(0.2/(2-0.2))) THEN 'Above UCL'
        WHEN e.ewma < b.mu - (3 * b.sigma * SQRT(0.2/(2-0.2))) THEN 'Below LCL'
        ELSE 'In Control'
    END AS status
FROM ewma_calc e
CROSS JOIN baseline b
ORDER BY e.measure_date;

SELECT * FROM accuracy_ewma;

-- =====================================================================
-- SECTION 4: SPC TECHNIQUE #3 - CUSUM (Cumulative Sum)
-- =====================================================================

-- INTUITION: CUSUM detects persistent small shifts. Imagine you're trying
-- to detect if someone is slowly stealing money from the cash register.
-- Each individual shortage might be small ($1-2), but if it happens every
-- day, the cumulative shortage grows. CUSUM accumulates these small
-- deviations to make them visible.

-- CUSUM keeps two running totals:
-- - C+ (upper CUSUM): Accumulates points above target
-- - C- (lower CUSUM): Accumulates points below target
-- When either crosses a threshold (H), we have a shift

-- TIMELINESS CUSUM (Process Shift Example)
CREATE OR REPLACE TEMPORARY TABLE timeliness_cusum AS
WITH RECURSIVE cusum_calc AS (
    -- Calculate target and variation from baseline period
    SELECT 
        td.measure_date,
        td.avg_lag_days AS value,
        1 AS row_num,
        -- Target (μ) from baseline
        (SELECT AVG(avg_lag_days) FROM timeliness_data WHERE day_number < 30) AS target,
        -- Standard deviation from baseline
        (SELECT STDDEV(avg_lag_days) FROM timeliness_data WHERE day_number < 30) AS sigma,
        -- Initial CUSUM values - explicitly cast to FLOAT to match recursive term
        CAST(0.0 AS FLOAT) AS cusum_upper,
        CAST(0.0 AS FLOAT) AS cusum_lower
    FROM timeliness_data td
    WHERE td.measure_date = '2024-01-01'
    
    UNION ALL
    
    SELECT 
        td.measure_date,
        td.avg_lag_days AS value,
        cc.row_num + 1 AS row_num,
        cc.target,
        cc.sigma,
        -- Upper CUSUM: accumulates deviations above target
        -- K = sigma/2 is the "slack" value (allowable deviation)
        GREATEST(0, cc.cusum_upper + (td.avg_lag_days - cc.target - cc.sigma/2))::FLOAT AS cusum_upper,
        -- Lower CUSUM: accumulates deviations below target
        GREATEST(0, cc.cusum_lower + (cc.target - td.avg_lag_days - cc.sigma/2))::FLOAT AS cusum_lower
    FROM timeliness_data td
    INNER JOIN cusum_calc cc 
        ON td.measure_date = DATEADD(day, 1, cc.measure_date)
    WHERE cc.row_num < 60
)
SELECT 
    measure_date,
    value,
    target,
    cusum_upper,
    cusum_lower,
    -- Decision threshold H = 4 × sigma (typical value)
    4 * sigma AS threshold,
    CASE 
        WHEN cusum_upper > 4 * sigma THEN 'Shift Upward Detected'
        WHEN cusum_lower > 4 * sigma THEN 'Shift Downward Detected'
        ELSE 'In Control'
    END AS status
FROM cusum_calc
ORDER BY measure_date;

SELECT * FROM timeliness_cusum;

-- =====================================================================
-- SECTION 5: COMPARISON AND INTERPRETATION
-- =====================================================================

-- WHAT EACH TECHNIQUE DETECTS BEST:

-- 1. CONTROL CHARTS (Shewhart)
--    Best for: Large, sudden changes (like a system outage)
--    Limitation: Slow to detect small, gradual shifts
--    Use when: You want simple, easy-to-explain monitoring

-- 2. EWMA
--    Best for: Small to moderate shifts that persist
--    Limitation: Can be slow to reset after a shift
--    Use when: Changes tend to be gradual (staff turnover, seasonal effects)

-- 3. CUSUM
--    Best for: Small, persistent shifts (very sensitive)
--    Limitation: More complex to explain and interpret
--    Use when: You need to detect subtle changes quickly (fraud, degradation)

-- PRACTICAL INTERPRETATION GUIDE:

SELECT 'COMPLETENESS (Stable Process)' AS scenario,
       'Control Chart' AS technique,
       'Should show all points within control limits - this is what normal looks like' AS expected_result
UNION ALL
SELECT 'TIMELINESS (Process Shift)',
       'Control Chart',
       'Should show points above UCL after day 30 - clear signal of increased lag'
UNION ALL
SELECT 'TIMELINESS (Process Shift)',
       'CUSUM',
       'CUSUM should start climbing around day 30 and cross threshold - earlier detection than control chart'
UNION ALL
SELECT 'ACCURACY (Increased Variability)',
       'EWMA',
       'EWMA should show increased fluctuation after day 35 - variation is the problem, not the average'
UNION ALL
SELECT 'ACCURACY (Increased Variability)',
       'Control Chart',
       'May miss this issue - individual points might stay in limits even though process is less stable';

-- =====================================================================
-- SECTION 6: PRODUCTION IMPLEMENTATION TEMPLATE
-- =====================================================================

-- Here's how you'd implement this for real monitoring:

CREATE OR REPLACE TABLE dq_monitoring_log (
    measure_date DATE,
    measure_name VARCHAR,
    measure_value FLOAT,
    technique VARCHAR,
    center_line FLOAT,
    ucl FLOAT,
    lcl FLOAT,
    status VARCHAR,
    alert_flag BOOLEAN,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Example: Daily job to monitor completeness
CREATE OR REPLACE PROCEDURE monitor_completeness_daily()
RETURNS VARCHAR
LANGUAGE SQL
AS
$
DECLARE
    today_value FLOAT;
    center_line FLOAT;
    std_dev FLOAT;
BEGIN
    -- Calculate today's completeness
    SELECT 
        (COUNT(*) FILTER (WHERE diagnosis_code IS NOT NULL) * 100.0 / COUNT(*))
    INTO :today_value
    FROM encounters
    WHERE encounter_date = CURRENT_DATE - 1;
    
    -- Get baseline statistics (rolling 30-day window, excluding weekends)
    SELECT AVG(measure_value), STDDEV(measure_value)
    INTO :center_line, :std_dev
    FROM dq_monitoring_log
    WHERE measure_name = 'completeness_pct'
      AND measure_date >= CURRENT_DATE - 30
      AND measure_date < CURRENT_DATE - 1;
    
    -- Insert with control limits
    INSERT INTO dq_monitoring_log (
        measure_date, measure_name, measure_value, technique,
        center_line, ucl, lcl, status, alert_flag
    )
    SELECT 
        CURRENT_DATE - 1,
        'completeness_pct',
        :today_value,
        'control_chart',
        :center_line,
        :center_line + (3 * :std_dev),
        :center_line - (3 * :std_dev),
        CASE 
            WHEN :today_value > :center_line + (3 * :std_dev) THEN 'Above UCL'
            WHEN :today_value < :center_line - (3 * :std_dev) THEN 'Below LCL'
            ELSE 'In Control'
        END,
        :today_value NOT BETWEEN (:center_line - 3 * :std_dev) AND (:center_line + 3 * :std_dev);
    
    RETURN 'Monitoring complete for ' || (CURRENT_DATE - 1)::VARCHAR;
END;
$;

-- Schedule it to run daily
-- CREATE TASK monitor_completeness_task
--   WAREHOUSE = dq_monitoring_wh
--   SCHEDULE = 'USING CRON 0 6 * * * America/New_York'
-- AS
--   CALL monitor_completeness_daily();

-- =====================================================================
-- SECTION 7: ALERTING QUERIES
-- =====================================================================

-- Query to check for active alerts
SELECT 
    measure_date,
    measure_name,
    measure_value,
    status,
    center_line,
    ROUND(ABS(measure_value - center_line) / (ucl - center_line) * 3, 2) AS sigma_distance
FROM dq_monitoring_log
WHERE alert_flag = TRUE
  AND measure_date >= CURRENT_DATE - 7
ORDER BY measure_date DESC, sigma_distance DESC;

-- Summary dashboard query
SELECT 
    measure_name,
    COUNT(*) AS total_days,
    SUM(CASE WHEN alert_flag THEN 1 ELSE 0 END) AS alert_days,
    ROUND(AVG(measure_value), 2) AS avg_value,
    MIN(measure_value) AS min_value,
    MAX(measure_value) AS max_value
FROM dq_monitoring_log
WHERE measure_date >= CURRENT_DATE - 30
GROUP BY measure_name
ORDER BY alert_days DESC;

-- =====================================================================
-- KEY TAKEAWAYS FOR DATA ENGINEERS
-- =====================================================================

-- 1. SPC is about understanding NORMAL variation vs. SPECIAL CAUSE variation
--    - Normal: Random ups and downs that are part of any process
--    - Special: Something actually changed (system issue, process change, etc.)

-- 2. Don't use arbitrary thresholds (like "alert if < 95%")
--    - Your process might normally run at 93% - that's not a problem
--    - Or it might normally run at 99% - then 96% IS a problem
--    - Let the data tell you what's normal for YOUR system

-- 3. Use the right technique for what you're trying to detect:
--    - Big obvious problems → Control Charts
--    - Gradual degradation → EWMA
--    - Subtle persistent issues → CUSUM

-- 4. Always establish a baseline period when the process was known to be good
--    - Don't include known bad days in your baseline
--    - Re-baseline when you make intentional process changes

-- 5. SPC is not "set it and forget it"
--    - Review your baselines periodically
--    - Investigate special causes when they occur
--    - Update limits after process improvements

-- =====================================================================
-- END OF GUIDE
-- =====================================================================