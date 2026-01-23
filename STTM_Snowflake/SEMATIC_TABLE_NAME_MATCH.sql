
USE DATABASE STAR_DEV;
--EMBED TABLE NAME PLUS COLUMNS
WITH SOURCE_RAW AS(
    SELECT
        table_schema AS tab_schema,
        table_name,
        LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS column_names
    FROM 
        STAR_DEV.INFORMATION_SCHEMA.COLUMNS
    WHERE --WHAT IS TRUE GGASTRO SOURCE TABLE SET?
          --STG_{*} are point of entry
          table_schema = 'DIM_MODEL'
      AND STARTSWITH(table_name, 'DIM_')
      AND column_name NOT IN ('ETL_CREATED_DATE', 'ETL_UPDATED_DATE', 'ETL_CREATED_BY', 'ETL_UPDATED_BY')
    GROUP BY 
        table_schema,table_name
),
TARGET_RAW AS(
    SELECT 
        table_schema AS tab_schema,
        table_name,
        LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS column_names
    FROM 
        STAR_DEV.INFORMATION_SCHEMA.COLUMNS
    WHERE 
          table_schema = 'DIM_MODEL'
      AND STARTSWITH(table_name, 'FACT_')   
      AND column_name NOT IN ('ETL_CREATED_DATE', 'ETL_UPDATED_DATE', 'ETL_CREATED_BY', 'ETL_UPDATED_BY')
    GROUP BY 
        table_schema,table_name
),
SOURCE_ENC AS(
    SELECT
            tab_schema AS schema,
            table_name,
            AI_EMBED('snowflake-arctic-embed-m-v1.5', table_name::STRING) AS table_enc,
            AI_EMBED('snowflake-arctic-embed-m-v1.5', column_names::STRING) AS cols_enc,
            AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(table_name,',',column_names)) AS tab_cols_enc,
            AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(column_names,',',table_name)) AS cols_tab_enc
        FROM SOURCE_RAW
),
TARGET_ENC AS(
    SELECT
        tab_schema AS schema,
        table_name,
        AI_EMBED('snowflake-arctic-embed-m-v1.5', table_name::STRING) AS table_enc,
        AI_EMBED('snowflake-arctic-embed-m-v1.5', column_names::STRING) AS cols_enc,
        AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(table_name,',',column_names)) AS tab_cols_enc,
        AI_EMBED('snowflake-arctic-embed-m-v1.5', CONCAT(column_names,',',table_name)) AS cols_tab_enc
     FROM TARGET_RAW
)
SELECT
    src.schema AS SOURCE_SCHEMA,
    src.table_name AS SOURCE_TABLE,
    tar.schema AS TARGET_SCHEMA,
    tar.table_name AS TARGET_TABLE,
    VECTOR_COSINE_SIMILARITY(src.table_enc, tar.table_enc) AS TAB_MATCH,
    VECTOR_COSINE_SIMILARITY(src.cols_enc, tar.cols_enc) AS COLS_MATCH,
    VECTOR_COSINE_SIMILARITY(src.tab_cols_enc, tar.tab_cols_enc) AS TAB_COLS_MATCH,
    VECTOR_COSINE_SIMILARITY(src.cols_tab_enc, tar.cols_tab_enc) AS COLS_TAB_MATCH,
    (TAB_MATCH+COLS_MATCH+TAB_COLS_MATCH+COLS_TAB_MATCH) AS COMPOSITE_SCORE
FROM 
    TARGET_ENC AS tar
CROSS JOIN 
    SOURCE_ENC AS src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TARGET_TABLE
    ORDER BY COMPOSITE_SCORE DESC
) <= 10  --TOP 10
ORDER BY TARGET_TABLE,COMPOSITE_SCORE DESC
;