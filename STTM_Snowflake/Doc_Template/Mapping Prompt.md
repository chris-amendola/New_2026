
### Role
You are a Senior Data Engineer specializing in ETL and data warehousing.

### Task
Create a source-to-target mapping (STM) between the following two schemas. 
Analyze table names, field names and data types to suggest the most logical matches and transformations.

### Schemas
- **Source Schema (System A)**: [Paste Source Fields/DDL here, e.g., cust_id (INT), f_name (VARCHAR)]
- **Target Schema (System B)**: [Paste Target Fields/DDL here, e.g., CustomerNumber (STRING), FullName (STRING)]

### Rules & Transformations
1. Prioritize column mappings based on sematic meanings of the tables in the source and target schemas, so that incompatible tables don't have their columns compared e.g., source table 'Provider' should not have any of its columns compared to target table 'Patients'
2. Map fields based on semantic similarity (e.g., 'f_name' and 'l_name' should combine into 'FullName').
3. Identify data type mismatches and suggest conversion logic (e.g., casting INT to STRING).
4. If no match is found for a target field, mark it as "NULL" or "TBD".
5. Add a "Transformation Logic" column for any necessary business rules.
 
### Output Format
Provide the mapping in a Markdown table with the following columns:| Source Field | Source Type | Target Field | Target Type | Transformation Logic | Confidence Score |

### Tips for Better Results
*   **Provide Sample Values**: AI maps more accurately when it sees actual data (e.g., "Field: `zip_code`, Value: `90210`") rather than just names.
*   **Few-Shot Examples**: Give 1 or 2 manual examples of how you want the mapping to look; this helps the AI learn your preferred style.
*   **Request Rationale**: Ask the AI to "explain its reasoning" for complex mappings to ensure they align with your business logic.
*   **CSV Export**: If you need the results in Excel, add a final instruction: *"Return the final mapping as a raw CSV-formatted block"*. 