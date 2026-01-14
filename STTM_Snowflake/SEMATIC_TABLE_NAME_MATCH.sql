
USE DATABASE STAR_DEV;
--EMBED TABLE NAME PLUS COLUMNS
SELECT 
    table_name,
    LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS column_names
FROM 
    STAR_DEV.INFORMATION_SCHEMA.COLUMNS
WHERE 
    table_schema = 'GGASTRO'
GROUP BY 
    table_name
ORDER BY 
    table_name;






WITH t1 AS (
    SELECT table_schema, table_name
    FROM STAR_DEV.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'GGASTRO'
      AND table_type = 'BASE TABLE'         -- optional but recommended
),
t2 AS (
    SELECT table_schema, table_name
    FROM STAR_DEV.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'DIM_MODEL'
      AND table_type = 'BASE TABLE'         -- optional but recommended
),
t1_emb AS (
    SELECT
        table_schema AS schema1,
        table_name   AS table1,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', table_name::STRING) AS vec1
    FROM t1
),
t2_emb AS (
    SELECT
        table_schema AS schema2,
        table_name   AS table2,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', table_name::STRING) AS vec2
    FROM t2
)
SELECT
    e1.schema1,
    e1.table1,
    e2.schema2,
    e2.table2,
    JAROWINKLER_SIMILARITY(e1.table1, e2.table2)/100 as chk1,
    VECTOR_COSINE_SIMILARITY(e1.vec1, e2.vec2) as ck2,
    ( (VECTOR_COSINE_SIMILARITY(e1.vec1, e2.vec2)*0.7)+ 
      (JAROWINKLER_SIMILARITY(e1.table1, e2.table2)/100)*0.3) as score
FROM t1_emb AS e1
CROSS JOIN t2_emb AS e2
ORDER BY table2,score DESC;
