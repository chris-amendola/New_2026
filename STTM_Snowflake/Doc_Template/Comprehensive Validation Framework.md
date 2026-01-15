## Foundational Data Quality Dimensions

**Completeness Validation**

- Check null rates for critical fields. For GI procedures, flag any colonoscopy records missing procedure indication (screening vs. surveillance vs. diagnostic) as this drives quality metrics like adenoma detection rates.
- Validate required field population: CPT codes, diagnosis codes, provider NPI, facility identifiers, and procedure dates must exceed 98% completeness.
- Example: If bowel prep quality scores are missing in >5% of colonoscopy records, the data source fails integration criteria—this field is essential for adenoma detection rate adjustment.

**Accuracy & Conformance**

- Cross-reference against authoritative sources. Provider NPIs should validate against NPPES registry; facility identifiers against CMS provider files.
- Value domain checks: Colonoscopy withdrawal times should fall within 2-45 minutes (flag outliers for investigation). Cecal intubation rates by provider should be 90-98% for screening cases.
- Diagnosis code validation: ICD-10 codes should align with GI specialty (K00-K95 range primarily). Flag records with unrelated primary diagnoses.
- Example: A data feed showing average procedure times of 3 minutes for colonoscopies indicates timestamp formatting issues or incomplete data capture.

**Consistency Checks**

_Temporal consistency_

- Procedure date should precede or equal pathology result date
- Surveillance colonoscopy intervals should align with guideline recommendations (e.g., 10 years for normal screening, 3-5 years for low-risk adenomas)
- Example: If 40% of "screening" colonoscopies show previous colonoscopy within 3 years, either the indication coding is wrong or duplicate records exist.

_Cross-field logical validation_

- Polypectomy biopsy records must have corresponding polyp detection in procedure documentation
- ASA classification should correlate with documented comorbidities
- Sedation type and dosing should align with procedure complexity and duration
- Example: Records showing moderate sedation for ERCP procedures would be flagged—these typically require anesthesia support.

_Cross-source reconciliation_

- Procedure counts from endoscopy system should reconcile with billing system within 2%
- Pathology results should match 1:1 with tissue biopsies documented in procedure reports
- Example: I once discovered a 15% discrepancy where pathology specimens were received but no corresponding endoscopy procedure was recorded—revealed incomplete EMR interface.

## Statistical Profile Analysis

**Volume & Distribution Patterns**

- Compare monthly procedure volumes against 12-month rolling average (±20% tolerance)
- Analyze case mix distribution: In a mature GI practice, expect approximately 60-65% colonoscopy, 15-20% EGD, 5-10% ERCP, with remaining distributed among other procedures
- Provider-level volume analysis: Flag providers with volumes >3 standard deviations from peer mean
- Example: A sudden 40% drop in EGD volumes likely indicates incomplete data transmission rather than true clinical change.

**Outlier Detection**

- Calculate adenoma detection rates by provider—should cluster between 25-50% for screening colonoscopies
- Sedation dosing by BMI and age categories—detect systematic over/under-dosing patterns
- Complication rates should be <1% for routine colonoscopy
- Example: One provider showing 5% perforation rate immediately triggers investigation—usually reveals miscoded complications or data quality issues.

## Referential Integrity & Linkage Quality

**Master Data Management**

- Provider matching: New source provider identifiers must resolve to existing provider master with >95% match rate
- Patient matching: Utilize probabilistic matching on demographic elements; target >98% successful linkage
- Facility/location mapping: Ensure endoscopy suite identifiers map correctly to facility hierarchy
- Example: When integrating ASC data, I've required exact matching of procedure rooms to specific locations to enable room-level efficiency metrics.

**Longitudinal Integrity**

- Validate patient care continuity: Same patient should not have multiple "first" colonoscopies
- Surveillance tracking: Identify patients with appropriate follow-up procedures based on previous findings
- Episode construction: Ensure pre-procedure, procedure, and post-procedure encounters link properly
- Example: For polyp surveillance metrics, I validate that 85-90% of patients with advanced adenomas have documented follow-up scheduling within the appropriate timeframe.

## Clinical Logic Validation

**Guideline Concordance**

- Appropriate indication for procedure (validate using USPSTF, ACG, ASGE guidelines)
- Interval appropriateness for surveillance (3, 5, 10-year intervals based on findings)
- Age-appropriate screening (typically 45-75 for average risk)
- Example: Flag all colonoscopies in patients <40 without documented high-risk indication—helps identify coding errors or inappropriate utilization.

**Quality Metric Feasibility**

- Calculate preliminary adenoma detection rates, cecal intubation rates, bowel prep adequacy—compare against national benchmarks
- If metrics fall outside expected ranges (ADR 15-55%, CIR 85-98%), investigate before proceeding
- Example: ADR of 8% signals either documentation problems, pathology linkage failures, or serious quality issues requiring immediate attention.

## Temporal Validation

**Latency & Freshness**

- Establish expected data lag: procedure data within 24 hours, pathology within 72 hours, billing within 5 days
- Monitor daily load completeness—missing or partial batches trigger investigation
- Validate "as-of" dates align with clinical workflow reality
- Example: If pathology results appear in the feed before the procedure date, indicates source system timestamp issues.

**Historical Validation**

- When backloading historical data, validate volume patterns against known organizational changes (new providers, facility openings, EMR implementations)
- Check for temporal gaps that might affect longitudinal metrics
- Example: I always validate whether a new EMR go-live date corresponds with sudden changes in documentation patterns or metric values.

## Operational & Technical Validation

**Record Count Reconciliation**

- Source system extract count = staging table count = transformed record count (account for legitimate exclusions)
- Maintain audit tables tracking every record's disposition (loaded, rejected, duplicate)
- Example: Implement daily reconciliation reports showing extracts vs. loads with variance explanations required when >1% discrepancy.

**Uniqueness & Duplication**

- Define natural keys (patient ID + procedure date + procedure type + provider) and enforce uniqueness
- Identify near-duplicates using fuzzy matching on timestamps (±1 day) and procedure details
- Example: Discovered 12% duplication rate when both hospital and ASC systems were sending data for physicians with privileges at both—required source prioritization rules.

**Data Type & Format Conformance**

- Validate dates parse correctly (watch for ambiguous MM/DD/YYYY vs. DD/MM/YYYY)
- Numeric fields contain valid numbers (procedure times, medication doses)
- Code fields match expected formats (CPT codes are 5 digits, ICD-10 follows proper structure)
- Example: European date format from vendor system caused months of data to fail loading until transformation rules were corrected.

## Business Rule Validation

**Denominator Definition**

- For ADR calculations, confirm the denominator includes only appropriate screening/surveillance colonoscopies, excludes diagnostic procedures
- Validate age filters, procedural exclusions align with quality program definitions
- Example: Including all colonoscopies in ADR calculation inflates the denominator inappropriately—must exclude incomplete procedures, poor prep, and diagnostic indications.

**Attribution Logic**

- Validate performing provider is correctly identified (not supervising or ordering provider)
- Facility attribution for multi-site organizations requires explicit mapping
- Example: ERCP procedures often involve multiple providers—establish clear rules for attributing outcomes to primary endoscopist vs. interventionalist.

## Pre-Integration Sign-off Process

**Validation Report Package** I require a comprehensive validation report before any new source goes live:

1. **Data Profiling Summary**: Row counts, completeness percentages, value distributions for top 50 fields
2. **Quality Metrics Dashboard**: Initial calculation of 10-15 key GI metrics with comparison to existing EDW values and national benchmarks
3. **Exception Report**: All records failing validation rules with business owner review and disposition
4. **Reconciliation Statement**: Signed attestation from source system owner confirming extract completeness
5. **Lineage Documentation**: Complete data flow from source to consumption layer with transformation logic

**Parallel Run Requirement**

- Run new source in parallel with existing data for 30-90 days
- Compare metrics between old and new methodology—investigate variances >5%
- Example: When replacing legacy endoscopy system data, I've maintained parallel processing for a full quarter to ensure no metric disruption for quality reporting.

## Ongoing Monitoring Post-Integration

**Automated Data Quality Dashboards**

- Daily monitoring of completeness, timeliness, volume patterns
- Weekly trending of key quality metrics (ADR, CIR, complication rates)
- Monthly deep-dive comparing current to historical patterns
- Example: Set up alerts when daily colonoscopy counts fall below 80% of 30-day average—enables rapid response to interface failures.

**Feedback Loop to Source Systems**

- Establish monthly data quality scorecards shared with source system owners
- Create continuous improvement process for addressing systematic issues
- Example: Pathology system was missing specimen adequacy fields—worked with vendor and lab to modify reporting template, improving data quality from 60% to 95% completeness.

---

**Critical!!** : **Never compromise on data quality at the point of entry.** The cost of cleaning data downstream, and the loss of trust when leadership makes decisions on flawed analytics, far exceeds the effort required for rigorous upfront validation. For a GI-focused EDW, your reputation depends on the precision of adenoma detection rates and other quality metrics—there's zero tolerance for "close enough" when those numbers determine reimbursement, accreditation, and clinical reputation.