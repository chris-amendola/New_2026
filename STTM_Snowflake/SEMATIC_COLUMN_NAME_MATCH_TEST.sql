
USE DATABASE GGASTRO;


CREATE OR REPLACE TABLE COLUMN_MATCH5 AS
WITH SOURCE_RAW AS(
    SELECT
        'TEST'       AS tab_schema,
        $1           AS table_name,
        $2  AS column_name,
        LOWER(REPLACE($2,'_',' ' )) as col_toke,
        LOWER(REPLACE($1,'_',' ' )) as tab_toke
    FROM '@"STAR_DEV"."STAGING"."TEST_SCHEMA_MAP_AI"/TableandColumns.csv'
      (file_format =>STAR_DEV.STAGING.COMMASEP_DBLQUOT_ONEHEADROW )
),
TARGET_RAW AS(
    SELECT 
        table_schema AS tab_schema,
        table_name,
        column_name,
        LOWER(REPLACE(column_name,'_',' ' )) as col_toke,
        LOWER(REPLACE(table_name,'_',' ' )) as tab_toke
    FROM 
        STAR.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'DIM_MODEL'
      AND table_name NOT IN ('FACT_APPOINTMENT_BKP_1010',
                             'FACT_BILLING_CHARGES_BKP_1009',
                             'FACT_BILLING_CHARGES_BK_1010',
                             'FACT_BILLING_LEDGER_BACKUP20260106',
                             'FACT_BILLING_LEDGER_BKP_1009',
                             'FACT_BILLING_LEDGER_BKP_1010',
                             'FACT_ENCOUNTER_PROCEDURE_BACKUP20260106')
      AND NOT ENDSWITH(table_name, '_VW')                       
      AND column_name NOT IN ('IDENTIFIER','ETL_CREATED_DATE', 'ETL_UPDATED_DATE', 'ETL_CREATED_BY', 'ETL_UPDATED_BY')
),
SOURCE_ENC AS(
    SELECT
            tab_schema AS schema,
            table_name,
            column_name,
         --'e5-base-v2'
         --snowflake-arctic-embed-m-v1.5   
         AI_EMBED('snowflake-arctic-embed-m-v1.5', tab_toke) AS tab_enc,
         AI_EMBED('snowflake-arctic-embed-m-v1.5', col_toke) AS col_enc,
         AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(tab_toke,',',col_toke)) AS tab_col_enc,
         AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(col_toke,',',tab_toke)) AS col_tab_enc
        FROM SOURCE_RAW
),
TARGET_ENC AS(
    SELECT
        tab_schema AS schema,
        table_name,
        column_name,
        --'e5-base-v2'
        --snowflake-arctic-embed-m-v1.5
        AI_EMBED('snowflake-arctic-embed-m-v1.5', tab_toke) AS tab_enc,
        AI_EMBED('snowflake-arctic-embed-m-v1.5', col_toke) AS col_enc,
        AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(tab_toke,',',col_toke)) AS tab_col_enc,
        AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(col_toke,',',tab_toke)) AS col_tab_enc
     FROM TARGET_RAW
)
SELECT
    src.schema AS SOURCE_SCHEMA,
    src.table_name AS SOURCE_TABLE,
    src.column_name AS SOURCE_COLUMN,
    tar.schema AS TARGET_SCHEMA,
    tar.table_name AS TARGET_TABLE,
    tar.column_name AS TARGET_COLUMN,
    VECTOR_COSINE_SIMILARITY(src.tab_enc, tar.tab_enc) AS TAB_MATCH,
    VECTOR_COSINE_SIMILARITY(src.col_enc, tar.col_enc) AS COL_MATCH,
    VECTOR_COSINE_SIMILARITY(src.tab_col_enc, tar.tab_col_enc) AS TAB_COL_MATCH,
    VECTOR_COSINE_SIMILARITY(src.col_tab_enc, tar.col_tab_enc) AS COL_TAB_MATCH,
    JAROWINKLER_SIMILARITY(src.column_name,tar.column_name)/100 as JW_SCORE,
    VECTOR_COSINE_SIMILARITY(src.col_tab_enc, tar.col_tab_enc)+VECTOR_COSINE_SIMILARITY(src.tab_col_enc, tar.tab_col_enc)+JAROWINKLER_SIMILARITY(src.column_name,tar.column_name)/100 as SUM   
FROM 
    TARGET_ENC AS tar
CROSS JOIN 
    SOURCE_ENC AS src  
WHERE SUM>2
  --AND TARGET_TABLE='FACT_ENCOUNTER_PROCEDURE'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TARGET_TABLE,TARGET_COLUMN
    ORDER BY SUM DESC
) <= 5  --TOP 5
ORDER BY TARGET_TABLE,TARGET_COLUMN,SUM DESC;

--How to decide which matches are most informative?

--JAROWINKLER_SIMILARITY(e1.table1, e2.table2)/100 as chk1,
--VECTOR_COSINE_SIMILARITY(e1.vec1, e2.vec2) as ck2,
--( (VECTOR_COSINE_SIMILARITY(e1.vec1, e2.vec2)*0.7)+ 
--(JAROWINKLER_SIMILARITY(e1.table1, e2.table2)/100)*0.3) as score

