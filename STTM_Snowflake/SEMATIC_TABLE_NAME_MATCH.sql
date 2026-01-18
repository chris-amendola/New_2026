
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
          table_schema = 'GGASTRO'
      AND STARTSWITH(table_name, 'STG_')
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
    GROUP BY 
        table_schema,table_name
),
SOURCE_ENC AS(
    SELECT
            tab_schema AS schema,
            table_name,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', table_name::STRING) AS table_enc,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', column_names::STRING) AS cols_enc,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', CONCAT(table_name,',',column_names)) AS tab_cols_enc,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', CONCAT(column_names,',',table_name)) AS cols_tab_enc
        FROM SOURCE_RAW
),
TARGET_ENC AS(
    SELECT
        tab_schema AS schema,
        table_name,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', table_name::STRING) AS table_enc,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', column_names::STRING) AS cols_enc,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', CONCAT(table_name,',',column_names)) AS tab_cols_enc,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', CONCAT(column_names,',',table_name)) AS cols_tab_enc
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
--TEMP
ORDER BY TARGET_TABLE,TAB_COLS_MATCH DESC;
