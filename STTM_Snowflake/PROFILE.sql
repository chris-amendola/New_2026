CREATE OR REPLACE PROCEDURE PROFILE_SOURCE_TABLE_JS(
    P_DATABASE   STRING,
    P_SCHEMA     STRING,
    P_TABLE      STRING,
    P_SAMPLE_PCT NUMBER DEFAULT 100
)
RETURNS TABLE (
    column_name           STRING,
    data_type             STRING,
    is_nullable            STRING,
    rows_scanned          NUMBER,
    non_null_count        NUMBER,
    null_count            NUMBER,
    pct_populated         NUMBER,
    distinct_count        NUMBER,
    pct_distinct_non_null NUMBER,
    min_value             STRING,
    max_value             STRING
)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var sqlText;
var stmt;
var rs;

/* 1️⃣ Create temp results table */
sqlText = `
CREATE OR REPLACE TEMP TABLE PROFILE_RESULTS (
    column_name           STRING,
    data_type             STRING,
    is_nullable            STRING,
    rows_scanned          NUMBER,
    non_null_count        NUMBER,
    null_count            NUMBER,
    pct_populated         NUMBER,
    distinct_count        NUMBER,
    pct_distinct_non_null NUMBER,
    min_value             STRING,
    max_value             STRING
)
`;
snowflake.execute({ sqlText });

/* 2️⃣ Get column metadata */
sqlText = `
SELECT column_name, data_type, is_nullable
FROM ${P_DATABASE}.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = '${P_SCHEMA}'
  AND table_name   = '${P_TABLE}'
ORDER BY ordinal_position
`;
stmt = snowflake.createStatement({ sqlText });
rs = stmt.execute();

/* 3️⃣ Profile each column */
while (rs.next()) {

    var colName     = rs.getColumnValue(1);
    var dataType    = rs.getColumnValue(2);
    var isNullable  = rs.getColumnValue(3);

    var sampleClause = (P_SAMPLE_PCT < 100)
        ? ` SAMPLE (${P_SAMPLE_PCT})`
        : ``;

    var profileSql = `
        INSERT INTO PROFILE_RESULTS
        SELECT
            '${colName}' AS column_name,
            '${dataType}' AS data_type,
            '${isNullable}' AS is_nullable,
            COUNT(*) AS rows_scanned,
            COUNT(${colName}) AS non_null_count,
            COUNT(*) - COUNT(${colName}) AS null_count,
            ROUND(COUNT(${colName}) / NULLIF(COUNT(*),0) * 100, 2)
                AS pct_populated,
            COUNT(DISTINCT ${colName}) AS distinct_count,
            ROUND(
                COUNT(DISTINCT ${colName}) /
                NULLIF(COUNT(${colName}),0) * 100, 2
            ) AS pct_distinct_non_null,
            CAST(MIN(${colName}) AS STRING) AS min_value,
            CAST(MAX(${colName}) AS STRING) AS max_value
        FROM ${P_DATABASE}.${P_SCHEMA}.${P_TABLE}
        ${sampleClause}
    `;

    snowflake.execute({ sqlText: profileSql });
}

/* 4️⃣ Return results */
return snowflake.execute({
    sqlText: `SELECT * FROM PROFILE_RESULTS ORDER BY column_name`
});
$$;
