DECLARE
    target_table STRING := 'YOUR_TABLE_NAME';
    target_schema STRING := 'YOUR_SCHEMA';
    fully_qualified_name STRING := :target_schema || '.' || :target_table;
    res RESULTSET;
    col_name STRING;
    col_type STRING;
    sql_stmt STRING;
BEGIN
    -- 1. Create a results table to hold the profile
    CREATE OR REPLACE TEMPORARY TABLE TABLE_PROFILE_RESULTS (
        column_name STRING,
        data_type STRING,
        total_count NUMBER,
        null_count NUMBER,
        null_pct NUMBER(5,2),
        unique_count NUMBER,
        uniqueness_pct NUMBER(5,2),
        min_val STRING,
        max_val STRING,
        avg_len NUMBER(10,2),
        blank_string_count NUMBER -- Common in healthcare flat files
    );

    -- 2. Cursor to loop through columns from Information Schema
    let cols_cursor cursor for 
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = :target_table 
          AND table_schema = :target_schema
        ORDER BY ordinal_position;

    FOR row_variable IN cols_cursor DO
        col_name := row_variable.column_name;
        col_type := row_variable.data_type;

        -- 3. Dynamic SQL to profile each column
        -- We use APPROX_COUNT_DISTINCT for performance on large healthcare datasets
        sql_stmt := 'INSERT INTO TABLE_PROFILE_RESULTS 
                     SELECT 
                        ''' || col_name || ''',
                        ''' || col_type || ''',
                        COUNT(*),
                        COUNT_IF(' || col_name || ' IS NULL),
                        ROUND(COUNT_IF(' || col_name || ' IS NULL) / COUNT(*) * 100, 2),
                        APPROX_COUNT_DISTINCT(' || col_name || '),
                        ROUND(APPROX_COUNT_DISTINCT(' || col_name || ') / COUNT(*) * 100, 2),
                        MIN(CAST(' || col_name || ' AS STRING)),
                        MAX(CAST(' || col_name || ' AS STRING)),
                        AVG(LEN(CAST(' || col_name || ' AS STRING))),
                        COUNT_IF(CAST(' || col_name || ' AS STRING) = '''')
                     FROM ' || :fully_qualified_name;

        EXECUTE IMMEDIATE :sql_stmt;
    END FOR;

    -- 4. Return the final profile
    res := (SELECT * FROM TABLE_PROFILE_RESULTS);
    RETURN TABLE(res);
END;