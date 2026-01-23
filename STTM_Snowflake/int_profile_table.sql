USE DATABASE STAR_DEV;
USE SCHEMA SEMANTIC_MAPPING;

create or replace TRANSIENT TABLE STAR_DEV.SEMANTIC_MAPPING.INT_PROFILE (
	profile_column VARCHAR(10000),
	SOURCE_SCHEMA VARCHAR(10000),
	SOURCE_TABLE VARCHAR(10000),
	ORDINAL_POSITION NUMBER(38,0),
	DECLARED_DATA_TYPE VARCHAR(10000),
	NULLABLE VARCHAR(3),
	SCHEMA_MAX_LENGTH NUMBER(38,0),
	NUMERIC_CAST_RATE NUMBER(19,6),
	DATE_CAST_RATE NUMBER(19,6),
    ROW_COUNT NUMBER(18,0),
	NON_NULL_PCT NUMBER(24,6),
	DISTINCT_COUNT NUMBER(18,0),
	DISTINCT_RATIO NUMBER(24,6),
	MIN_LENGTH NUMBER(18,0),
	MAX_LENGTH NUMBER(18,0),
	AVG_LENGTH NUMBER(36,6),
	WHITESPACE_PCT NUMBER(19,6),
	EMPTY_STRING_PCT NUMBER(19,6),
	HYPHENATED_PCT NUMBER(19,6),
	EMAIL_LIKE_PCT NUMBER(19,6),
	NUMERIC_ONLY_PCT NUMBER(19,6),
	ALPHA_ONLY_PCT NUMBER(19,6),
	ALPHANUMERIC_PCT NUMBER(19,6),
	ISO_DATE_PATTERN_PCT NUMBER(19,6),
	TOP_1_VALUE_PCT NUMBER(24,6),
	NAME_LIKE_PCT NUMBER(19,6),
	SSN_LIKE_PCT NUMBER(19,6),
	DOB_SLASH_LIKE_PCT NUMBER(19,6)
);



CREATE OR REPLACE PROCEDURE INT_TABLE_PROFILE(
    PROFILE_DB VARCHAR,
    PROFILE_SCHEMA VARCHAR,
    PROFILE_TABLE VARCHAR,
    DISTRIBUTION_TABLE VARCHAR DEFAULT 'STAR_DEV.SEMANTIC_MAPPING.INT_PROFILE'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_unpivot_list STRING;
    v_unpivot_cast STRING;
    v_sql STRING;
    info_schema_path STRING := :PROFILE_DB || '.INFORMATION_SCHEMA.COLUMNS';
    v_return_msg STRING;
    v_full_table_name STRING := :PROFILE_DB || '.' || :PROFILE_SCHEMA || '.' || :PROFILE_TABLE;
    v_log_sql STRING;

BEGIN
    -- Validate table exists
    LET v_table_count NUMBER;
    
    SELECT COUNT(*) INTO :v_table_count
    FROM IDENTIFIER(:info_schema_path)
    WHERE table_schema = :PROFILE_SCHEMA
      AND table_name = :PROFILE_TABLE;
    
    IF (v_table_count = 0) THEN
        v_return_msg := 'ERROR: Table ' || v_full_table_name || ' does not exist';
        v_log_sql := 'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.PROFILER_DISTRIBUTION_LOG VALUES (''' || 
                     REPLACE(v_return_msg, '''', '''''') || ''', CURRENT_TIMESTAMP, ''' || 
                     v_full_table_name || ''')';
        EXECUTE IMMEDIATE v_log_sql;
        RETURN v_return_msg;
    END IF;

    -- Build unpivot column list
    SELECT
        LISTAGG(
            '"' || column_name || '"::VARCHAR AS "' || column_name || '"',
            ',\n            '
        ) WITHIN GROUP (ORDER BY ordinal_position),
        LISTAGG('"' || column_name || '"', ',\n            ') WITHIN GROUP (ORDER BY ordinal_position)
    INTO :v_unpivot_cast, :v_unpivot_list
    FROM IDENTIFIER(:info_schema_path)
    WHERE table_schema = :PROFILE_SCHEMA
      AND table_name = :PROFILE_TABLE
      AND data_type NOT IN ('VARIANT', 'ARRAY', 'OBJECT')
      AND column_name NOT IN ('ETL_CREATED_DATE', 'ETL_UPDATED_DATE', 'ETL_CREATED_BY', 'ETL_UPDATED_BY');
    
    IF (v_unpivot_list IS NULL OR v_unpivot_list = '') THEN
        v_return_msg := 'ERROR: No valid columns found to profile in table ' || :PROFILE_TABLE;
         v_log_sql := 'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.PROFILER_DISTRIBUTION_LOG VALUES (''' || 
                     REPLACE(v_return_msg, '''', '''''') || ''', CURRENT_TIMESTAMP, ''' || 
                     v_full_table_name || ''')';
        EXECUTE IMMEDIATE v_log_sql;
        RETURN v_return_msg;
    END IF;

    -- Build and execute main profiler query
    v_sql := //'INSERT INTO STAR_DEV.SEMANTIC_MAPPING.INT_PROFILE
        'WITH column_metadata AS (
            SELECT
                table_schema             AS source_schema,
                table_name               AS source_table,
                column_name              AS profile_column,
                ordinal_position,
                data_type                AS declared_data_type,
                is_nullable              AS nullable,
                character_maximum_length AS max_length,
                CASE
                    WHEN data_type IN (
                        ''NUMBER'', ''DECIMAL'', ''NUMERIC'',
                        ''FLOAT'', ''FLOAT4'', ''FLOAT8'',
                        ''DOUBLE'', ''DOUBLE PRECISION'',
                        ''INT'', ''INTEGER'', ''BIGINT'',
                        ''SMALLINT'', ''TINYINT'', ''BYTEINT''
                    ) THEN ''NUMERIC''
                    ELSE ''CATEGORICAL''
                END AS column_class
            FROM ' || PROFILE_DB || '.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_schema = ''' || PROFILE_SCHEMA || '''
              AND table_name   = ''' || PROFILE_TABLE || '''
        ),
        base_projected AS (
            SELECT
                ' || v_unpivot_cast || '
            FROM ' || PROFILE_DB || '.' || PROFILE_SCHEMA || '.' || PROFILE_TABLE || ' AS t
            TABLESAMPLE (1000 ROWS)
        ),      
        unpivoted AS (
            SELECT
                column_name AS profile_column,
                value::STRING as value,
            FROM (base_projected)
            UNPIVOT (value FOR column_name IN (' || v_unpivot_list || '))
        ),
        unpivoted_cast AS (
            SELECT 
                *,
                TRY_TO_NUMBER(value) as num_value,
                TRY_TO_DATE(value) as date_value
            FROM unpivoted        
        ),
        cardinality AS (
            SELECT
                profile_column,
                (SELECT COUNT(*) FROM base_projected) AS num_rows,
                COUNT(value) AS non_null_count,
                COUNT(value) / (SELECT COUNT(*) FROM base_projected) AS non_null_rate,
                COUNT(DISTINCT value) AS distinct_count,
                COUNT(DISTINCT value) / COUNT(*) AS distinct_ratio
            FROM unpivoted
            GROUP BY profile_column
       ),
       len_stats AS (
            SELECT
                profile_column,
                MIN(LENGTH(value)) AS min_length,
                MAX(LENGTH(value)) AS max_length,
                AVG(LENGTH(value)) AS avg_length
            FROM unpivoted
            GROUP BY profile_column
        ), 
        pattern_stats AS (
            SELECT
                profile_column,
                AVG(IFF(REGEXP_LIKE(value, ''^\\\\d+$''), 1, 0)) AS numeric_only_pct,
                AVG(IFF(REGEXP_LIKE(value, ''^[A-Za-z]+$''), 1, 0)) AS alpha_only_pct,
                AVG(IFF(REGEXP_LIKE(value, ''^[A-Za-z0-9]+$''), 1, 0)) AS alphanumeric_pct,
                AVG(IFF(REGEXP_LIKE(value, ''-''), 1, 0)) AS hyphenated_pct,
                AVG(IFF(value LIKE ''% %'', 1, 0)) AS whitespace_pct,
                AVG(IFF(value = '''', 1, 0)) AS empty_string_pct,
                AVG(IFF(REGEXP_LIKE(value, ''@''), 1, 0)) AS email_like_pct
            FROM unpivoted
            GROUP BY profile_column
       ), 
       value_counts AS (
            SELECT profile_column, value, COUNT(*) AS cnt
            FROM unpivoted
            GROUP BY profile_column, value
        ),
       ranked AS (
           SELECT
                vc.profile_column,
                vc.value,
                vc.cnt,
                cnt/cd.non_null_count as rate,
                ROW_NUMBER() OVER (
                    PARTITION BY vc.profile_column
                    ORDER BY vc.cnt DESC
                ) AS rn
            FROM value_counts as vc
            JOIN cardinality cd 
              ON vc.profile_column = cd.profile_column
      ),
      top_values AS (
            SELECT
                profile_column,
                ARRAY_AGG(
                    OBJECT_CONSTRUCT(
                        ''value'', value,
                        ''rate'', ROUND(rate, 6)
                    )
                ) WITHIN GROUP (ORDER BY rate DESC) AS top_values,
                MAX(rate) AS modal_value_rate
            FROM ranked
            WHERE rn <= 10
            GROUP BY profile_column
      ),
      numeric_stats AS (
        SELECT 
            profile_column, 
            COUNT(num_value) AS n, 
            AVG(num_value) AS mean_val,
            STDDEV_SAMP(num_value) AS stddev_val, 
            MIN(num_value) AS min_value_num, 
            MAX(num_value) AS max_value_num
        FROM (unpivoted_cast)
        WHERE num_value IS NOT NULL
        GROUP BY profile_column
      ),
      numeric_skew AS (
        SELECT 
            u.profile_column,
            CASE WHEN ns.stddev_val > 0 AND ns.n > 2 THEN
            SUM(POWER((u.num_value - ns.mean_val)/NULLIF(ns.stddev_val,0),3)) * ns.n / NULLIF((ns.n-1)*(ns.n-2), 0)
            ELSE NULL END AS skewness
        FROM unpivoted_cast as u
        JOIN numeric_stats as ns 
          ON u.profile_column = ns.profile_column
        WHERE u.num_value IS NOT NULL
        GROUP BY u.profile_column, ns.mean_val, ns.stddev_val, ns.n
      ),
      categorical_skew AS (
          SELECT profile_column, MAX(cnt)/NULLIF(AVG(cnt),0) AS skewness
          FROM value_counts
          GROUP BY profile_column
      ),
      entropy AS (
          SELECT profile_column,
          -SUM(CASE WHEN rate > 0 
                    THEN 
                        rate * LOG(2,rate) ELSE 0 END) AS entropy_score
          FROM ranked
          GROUP BY profile_column
      )       
    SELECT
        *
    FROM column_metadata as met
    LEFT JOIN cardinality as crd USING (met.profile_column)
    LEFT JOIN len_stats as len USING (met.profile_column)
    LEFT JOIN pattern_stats as pat USING (met.profile_column)
    LEFT JOIN top_values as top USING (met.profile_column)
    LEFT JOIN numeric_stats as num USING (met.profile_column)
    LEFT JOIN numeric_skew as nsk USING (met.profile_column)
    LEFT JOIN categorical_skew as cat USING (met.profile_column)
    LEFT JOIN entropy as ent USING (met.profile_column);  
    ';
    
    EXECUTE IMMEDIATE v_sql;

    --RETURN 'Success: Profiled ' || :v_full_table_name;
    RETURN v_sql; 

    EXCEPTION
        WHEN OTHER THEN
            v_return_msg := 'ERROR: ' || SQLERRM;
            RETURN v_sql; 
            --RETURN v_return_msg;
END;
$$;

CALL INT_TABLE_PROFILE('GGASTRO','PROD','STG_PATIENTVIEW');

--SELECT * FROM STAR_DEV.SEMANTIC_MAPPING.INT_PROFILE;

WITH column_metadata AS (
            SELECT
                table_schema             AS source_schema,
                table_name               AS source_table,
                column_name              AS profile_column,
                ordinal_position,
                data_type                AS declared_data_type,
                is_nullable              AS nullable,
                character_maximum_length AS max_length,
                CASE
                    WHEN data_type IN (
                        'NUMBER', 'DECIMAL', 'NUMERIC',
                        'FLOAT', 'FLOAT4', 'FLOAT8',
                        'DOUBLE', 'DOUBLE PRECISION',
                        'INT', 'INTEGER', 'BIGINT',
                        'SMALLINT', 'TINYINT', 'BYTEINT'
                    ) THEN 'NUMERIC'
                    ELSE 'CATEGORICAL'
                END AS column_class
            FROM GGASTRO.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_schema = 'PROD'
              AND table_name   = 'STG_PATIENTVIEW'
        ),
        base_projected AS (
            SELECT
                "ID"::VARCHAR AS "ID",
            "FULL NAME"::VARCHAR AS "FULL NAME",
            "LAST NAME"::VARCHAR AS "LAST NAME",
            "FIRST NAME"::VARCHAR AS "FIRST NAME",
            "MIDDLE NAME"::VARCHAR AS "MIDDLE NAME",
            "DOB"::VARCHAR AS "DOB",
            "AGE"::VARCHAR AS "AGE",
            "MRN"::VARCHAR AS "MRN",
            "HOME PHONE NUMBER"::VARCHAR AS "HOME PHONE NUMBER",
            "MOBILE PHONE NUMBER"::VARCHAR AS "MOBILE PHONE NUMBER",
            "BUSINESS PHONE NUMBER"::VARCHAR AS "BUSINESS PHONE NUMBER",
            "GENDER"::VARCHAR AS "GENDER",
            "RACE"::VARCHAR AS "RACE",
            "MARITAL STATUS"::VARCHAR AS "MARITAL STATUS",
            "EXCLUDE FROM REPORT"::VARCHAR AS "EXCLUDE FROM REPORT",
            "IDENTIFIER"::VARCHAR AS "IDENTIFIER"
            FROM GGASTRO.PROD.STG_PATIENTVIEW AS t
            TABLESAMPLE (1000 ROWS)
        ),      
        unpivoted AS (
            SELECT
                column_name AS profile_column,
                value::STRING as value,
            FROM (base_projected)
            UNPIVOT (value FOR column_name IN ("ID",
            "FULL NAME",
            "LAST NAME",
            "FIRST NAME",
            "MIDDLE NAME",
            "DOB",
            "AGE",
            "MRN",
            "HOME PHONE NUMBER",
            "MOBILE PHONE NUMBER",
            "BUSINESS PHONE NUMBER",
            "GENDER",
            "RACE",
            "MARITAL STATUS",
            "EXCLUDE FROM REPORT",
            "IDENTIFIER"))
        ),
        unpivoted_cast AS (
            SELECT 
                *,
                TRY_TO_NUMBER(value) as num_value,
                TRY_TO_DATE(value) as date_value
            FROM unpivoted        
        ),
        cardinality AS (
            SELECT
                profile_column,
                (SELECT COUNT(*) FROM base_projected) AS num_rows,
                COUNT(value) AS non_null_count,
                COUNT(value) / (SELECT COUNT(*) FROM base_projected) AS non_null_rate,
                COUNT(DISTINCT value) AS distinct_count,
                COUNT(DISTINCT value) / COUNT(*) AS distinct_ratio
            FROM unpivoted
            GROUP BY profile_column
       ),
       len_stats AS (
            SELECT
                profile_column,
                MIN(LENGTH(value)) AS min_length,
                MAX(LENGTH(value)) AS max_length,
                AVG(LENGTH(value)) AS avg_length
            FROM unpivoted
            GROUP BY profile_column
        ), 
        pattern_stats AS (
            SELECT
                profile_column,
                AVG(IFF(REGEXP_LIKE(value, '^\\d+$'), 1, 0)) AS numeric_only_pct,
                AVG(IFF(REGEXP_LIKE(value, '^[A-Za-z]+$'), 1, 0)) AS alpha_only_pct,
                AVG(IFF(REGEXP_LIKE(value, '^[A-Za-z0-9]+$'), 1, 0)) AS alphanumeric_pct,
                AVG(IFF(REGEXP_LIKE(value, '-'), 1, 0)) AS hyphenated_pct,
                AVG(IFF(value LIKE '% %', 1, 0)) AS whitespace_pct,
                AVG(IFF(value = '', 1, 0)) AS empty_string_pct,
                AVG(IFF(REGEXP_LIKE(value, '@'), 1, 0)) AS email_like_pct
            FROM unpivoted
            GROUP BY profile_column
       ), 
       value_counts AS (
            SELECT profile_column, value, COUNT(*) AS cnt
            FROM unpivoted
            GROUP BY profile_column, value
        ),
       ranked AS (
           SELECT
                vc.profile_column,
                vc.value,
                vc.cnt,
                cnt/cd.non_null_count as rate,
                ROW_NUMBER() OVER (
                    PARTITION BY vc.profile_column
                    ORDER BY vc.cnt DESC
                ) AS rn
            FROM value_counts as vc
            JOIN cardinality cd 
              ON vc.profile_column = cd.profile_column
      ),
      top_values AS (
            SELECT
                profile_column,
                ARRAY_AGG(
                    OBJECT_CONSTRUCT(
                        'value', value,
                        'rate', ROUND(rate, 6)
                    )
                ) WITHIN GROUP (ORDER BY rate DESC) AS top_values,
                MAX(rate) AS modal_value_rate
            FROM ranked
            WHERE rn <= 10
            GROUP BY profile_column
      ),
      numeric_stats AS (
        SELECT 
            profile_column, 
            COUNT(num_value) AS n, 
            AVG(num_value) AS mean_val,
            STDDEV_SAMP(num_value) AS stddev_val, 
            MIN(num_value) AS min_value_num, 
            MAX(num_value) AS max_value_num
        FROM (unpivoted_cast)
        WHERE num_value IS NOT NULL
        GROUP BY profile_column
      ),
      numeric_skew AS (
        SELECT 
            u.profile_column,
            CASE WHEN ns.stddev_val > 0 AND ns.n > 2 THEN
            SUM(POWER((u.num_value - ns.mean_val)/NULLIF(ns.stddev_val,0),3)) * ns.n / NULLIF((ns.n-1)*(ns.n-2), 0)
            ELSE NULL END AS skewness
        FROM unpivoted_cast as u
        JOIN numeric_stats as ns 
          ON u.profile_column = ns.profile_column
        WHERE u.num_value IS NOT NULL
        GROUP BY u.profile_column, ns.mean_val, ns.stddev_val, ns.n
      ),
      categorical_skew AS (
          SELECT profile_column, MAX(cnt)/NULLIF(AVG(cnt),0) AS skewness
          FROM value_counts
          GROUP BY profile_column
      ),
      entropy AS (
          SELECT profile_column,
          -SUM(CASE WHEN rate > 0 
                    THEN 
                        rate * LOG(2,rate) ELSE 0 END) AS entropy_score
          FROM ranked
          GROUP BY profile_column
      )       
    SELECT
        *
    FROM column_metadata as met
    LEFT JOIN cardinality as crd USING (met.profile_column)
    LEFT JOIN len_stats as len USING (met.profile_column)
    LEFT JOIN pattern_stats as pat USING (met.profile_column)
    LEFT JOIN top_values as top USING (met.profile_column)
    LEFT JOIN numeric_stats as num USING (met.profile_column)
    LEFT JOIN numeric_skew as nsk USING (met.profile_column)
    LEFT JOIN categorical_skew as cat USING (met.profile_column)
    LEFT JOIN entropy as ent USING (met.profile_column);  
    


--TAXONOMY
--
