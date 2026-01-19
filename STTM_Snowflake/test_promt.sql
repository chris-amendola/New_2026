Absolutely! Here's the revised prompt with clear input placeholders:

---

## Database Schema Mapping Prompt

You are an expert database architect specializing in healthcare data systems. Your task is to analyze source and target database schemas provided as DDL scripts and create a comprehensive mapping document.

**Input:**

```
SOURCE DATABASE DDL:
[PASTE SOURCE DDL SCRIPT HERE]
```

```
TARGET DATABASE DDL:
[PASTE TARGET DDL SCRIPT HERE]
```

---

**Your Task:**
Analyze both schemas and produce a detailed mapping document in Markdown format that maps source tables and columns to their target equivalents.

**Mapping Approach:**

1. **Matching Criteria:**
   - Use table names and column names as primary matching factors
   - Ignore case sensitivity when comparing names
   - Handle common prefixes/suffixes (e.g., "tbl_", "ID" vs "id", "pk_", etc.)
   - Consider database constraints (primary keys, foreign keys, unique constraints, NOT NULL, check constraints) in your analysis
   - DO NOT infer relationships between tables that aren't explicitly defined in the DDL

2. **For Each Mapping:**
   - Provide the top 5 mapping choices with confidence scores (0-100%)
   - Present all choices even when there's an obvious match
   - For each choice, provide reasoning for why it's a candidate
   - Flag any unmapped fields clearly
   - Propose calculated/derived mappings where appropriate

3. **Required Documentation Elements:**
   - Table-to-table mappings
   - Column-to-column mappings
   - Data type transformations with specific conversion recommendations
   - Business rules (especially relevant to healthcare data and CPT codes)
   - Default value handling
   - NULL handling strategies
   - Transformation logic required for each mapping

4. **Data Type Mismatches:**
   - Provide specific transformation recommendations
   - Include casting/conversion functions where needed
   - Document any potential precision loss or truncation
   - Warn about incompatible type conversions

5. **Multiple Target Possibilities:**
   - When a source column could map to multiple target columns, list all possibilities
   - Provide detailed reasoning for each possibility
   - Highlight the most likely candidate

6. **Healthcare Context:**
   - Be aware this is healthcare data
   - Pay special attention to CPT code fields
   - Apply healthcare domain knowledge when making mapping suggestions

7. **Special Scenarios:**
   - Calculated/derived target fields: Document as requiring custom logic AND suggest potential source field combinations
   - Structural differences: Identify and document normalized vs denormalized structures, missing tables, additional tables

8. **Migration Warnings:**
   Include recommendations and warnings about:
   - Possible data loss due to size constraints
   - Orphaned records due to missing foreign key targets
   - Data quality concerns
   - Constraint violations

9. **Summary Statistics:**
   Provide the following metrics:
   - Total number of tables mapped (source → target)
   - Total number of columns mapped
   - Percentage of coverage (mapped vs unmapped)
   - Count of unmapped fields (by table and total)
   - Data type conversion summary

**Output Format:**
- Generate a well-structured Markdown document
- Determine the best organizational structure for clarity
- Treat all data types equally (no special handling for BLOBs, CLOBs, etc.)
- Use tables, sections, and clear headings for readability
- Include all confidence scores, reasoning, and warnings inline

**Document Structure (you may determine the best organization, but include these elements):**
- Summary statistics and overview
- Structural differences between schemas
- Detailed table-by-table mappings
- Column mappings with top 5 choices and confidence scores
- Data type transformations and conversion logic
- Unmapped fields section
- Migration warnings and recommendations
- Business rules and transformation requirements

Begin your analysis and produce the complete mapping document.

---

Is this clearer for where to provide the inputs?