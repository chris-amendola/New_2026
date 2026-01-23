USE DATABASE STAR_DEV;

--Create raw file read format to be ceratin of deffintion
DROP FILE FORMAT IF EXISTS STAR_DEV.STAGING.COMMASEP_DBLQUOT_ONEHEADROW;
create file format STAR_DEV.STAGING.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = ',' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;

--Table Row counts
select $1, count(*)
from '@"STAR_DEV"."STAGING"."TEST_SCHEMA_MAP_AI"/TableandColumns.csv'
(file_format =>STAR_DEV.STAGING.COMMASEP_DBLQUOT_ONEHEADROW )
group by $1;

CREATE OR REPLACE TABLE TABLE_MATCH10 AS
WITH SOURCE_RAW AS(
    SELECT
        'TEST' AS tab_schema,
        $1 as table_name,
        LISTAGG($2, ', ') AS column_names
    FROM '@"STAR_DEV"."STAGING"."TEST_SCHEMA_MAP_AI"/TableandColumns.csv'
      (file_format =>STAR_DEV.STAGING.COMMASEP_DBLQUOT_ONEHEADROW )
    GROUP BY 
        tab_schema,table_name
),
TARGET_RAW AS(
    SELECT 
        table_schema AS tab_schema,
        table_name,
        LISTAGG(column_name, ', ') AS column_names
    FROM 
        STAR.INFORMATION_SCHEMA.COLUMNS
    WHERE 
          table_schema = 'DIM_MODEL' 
      AND table_name NOT in ('FACT_BILLING_CHARGES_BKP_1009',
                             'DIM_CPTCODE_BACKUP20260106',
                             'DIM_PROVIDER_VW_BKP10152025',
                             'FACT_APPOINTMENT_BKP_1010')    
      AND column_name NOT IN ('ETL_CREATED_DATE', 'ETL_UPDATED_DATE', 'ETL_CREATED_BY', 'ETL_UPDATED_BY')
    GROUP BY 
        table_schema,table_name
),
SOURCE_ENC AS(
    SELECT
            tab_schema AS schema,
            table_name,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,LOWER(REPLACE(table_name::STRING,'_',' '))) AS table_enc,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,LOWER(REPLACE(column_names::STRING,'_',' '))) AS cols_enc,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,CONCAT(LOWER(REPLACE(table_name::STRING,'_',' ')),',',LOWER(REPLACE(column_names::STRING,'_',' ')))) AS tab_cols_enc,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,CONCAT(LOWER(REPLACE(column_names::STRING,'_',' ')),',',LOWER(REPLACE(table_name::STRING,'_',' ')))) AS cols_tab_enc
        FROM SOURCE_RAW
),
TARGET_ENC AS(
    SELECT
            tab_schema AS schema,
            table_name,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,LOWER(REPLACE(table_name::STRING,'_',' '))) AS table_enc,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,LOWER(REPLACE(column_names::STRING,'_',' '))) AS cols_enc,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,CONCAT(LOWER(REPLACE(table_name::STRING,'_',' ')),',',LOWER(REPLACE(column_names::STRING,'_',' ')))) AS tab_cols_enc,
            AI_EMBED( 'snowflake-arctic-embed-m-v1.5'
                     ,CONCAT(LOWER(REPLACE(column_names::STRING,'_',' ')),',',LOWER(REPLACE(table_name::STRING,'_',' ')))) AS cols_tab_enc
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
--Target top 10 matches
--SELECT * FROM MATCH10;

SELECT DISTINCT SOURCE_TABLE
FROM MATCH10;